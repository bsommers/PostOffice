# Distributed Control Plane & Data Plane

[← Back to User Guide](README.md) | [← Back to Project README](../../README.md)

PostOffice scales horizontally by separating administrative configuration management (**Control Plane**) from real-time message routing workers (**Data Plane**), synchronized via Redis.

---

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Running Data Plane Workers](#running-data-plane-workers)
- [Administering the Control Plane](#administering-the-control-plane)
  - [Registering Brokers & Routes](#registering-brokers--routes)
  - [Broadcasting Dynamic Updates](#broadcasting-dynamic-updates)
  - [Clean State Resets](#clean-state-resets)
  - [Distributing Ingress Rules](#distributing-ingress-rules)
- [Securing the Control Plane](#securing-the-control-plane)
  - [Signed Events and State](#signed-events-and-state)
  - [Signing Modes and Rolling Out](#signing-modes-and-rolling-out)
  - [Key Rotation](#key-rotation)
  - [Event Limits and the Action Allow-List](#event-limits-and-the-action-allow-list)
  - [Payload Validation](#payload-validation)
  - [Schema Definition Caps](#schema-definition-caps)
  - [Environment Variable Reference](#environment-variable-reference)
- [Resiliency & Connection Loss Recovery](#resiliency--connection-loss-recovery)

---

## Architecture Overview

```
                      +-----------------------------+
                      |   Control Plane (Admin)     |
                      |   scripts/admin.py or API   |
                      +--------------+--------------+
                                     |
                           (Writes state & PubSub)
                                     v
                      +-----------------------------+
                      |         Redis Hash          |
                      |  postoffice:brokers         |
                      |  postoffice:routes          |
                      |  postoffice:subscriptions   |
                      |  postoffice:ingress         |
                      +--------------+--------------+
                                     |
                           (PubSub config_updates)
             +-----------------------+-----------------------+
             |                                               |
             v                                               v
+-------------------------+                     +-------------------------+
|   Data Plane Worker 1   |                     |   Data Plane Worker 2   |
|   src/postoffice/       |                     |   src/postoffice/       |
|   worker.py             |                     |   worker.py             |
+-------------------------+                     +-------------------------+
```

1. **State Persistence:** Brokers, routes, subscriptions, and ingress rules are persisted in Redis hashes.
2. **Real-Time Notification:** When configuration changes occur, the Control Plane publishes an update event on the Redis PubSub channel `postoffice:config_updates`.
3. **Stateless Workers:** Data Plane workers subscribe to `postoffice:config_updates`, dynamically updating their local in-memory `PostOffice` and `Router` instances without restarting.

---

## Running Data Plane Workers

Start any number of worker processes across containers or virtual machines:

```bash
# Set PYTHONPATH and launch worker
PYTHONPATH=src .venv/bin/python src/postoffice/worker.py
```

Workers immediately:
1. Connect to Redis (default: `localhost:6379`).
2. Sync all existing brokers, routes, and subscriptions from Redis hashes into their local `PostOffice` instance.
3. Start all broker network connections.
4. Listen on `postoffice:config_updates` for real-time changes.

---

## Administering the Control Plane

You can inject configuration programmatically using the `ControlPlane` Python API:

```python
from postoffice.control_plane import ControlPlane
from postoffice.security import SecurityConfig

# Connect to Redis control cluster
cp = ControlPlane(redis_host="localhost", redis_port=6379)

# 1. Register Brokers
cp.register_broker("mqtt_edge", "mqtt", host="10.0.0.1", port=1883)
cp.register_broker("kafka_main", "kafka", bootstrap_servers="10.0.0.2:9092")

# 2. Register Routes
cp.register_route(
    source_broker="mqtt_edge",
    source_topic="telemetry/+",
    target_broker="kafka_main",
    target_topic="telemetry.raw",
)

# 3. Register Subscriptions
cp.register_subscription("mqtt_edge", "telemetry/+", qos=1)

# 4. Hot-Reload Broker Security Credentials Live Across Workers
cp.update_broker_security(
    "kafka_main",
    SecurityConfig(
        tls_enabled=True,
        ca_cert="/etc/ssl/certs/ca.pem",
        username="rotated_user",
        password="rotated_password",
        sasl_mechanism="SCRAM-SHA-256",
    ),
)
```

Every method call automatically writes the configuration to Redis and broadcasts an update message, causing all active Data Plane workers to reconfigure dynamically.

### Executing the Bundled Admin Script
```bash
PYTHONPATH=src .venv/bin/python scripts/admin.py
```

### Clean State Resets
To completely wipe cluster state and reconfigure all workers from scratch:

```python
# Clears Redis hashes and broadcasts 'clear_state' signal
cp.clear_state()
```

Workers receiving `clear_state` invoke `PostOffice.reset()`, disconnecting all active brokers and clearing all routing tables cleanly in memory. The ingress rules (below) are removed as well.

> [!WARNING]
> Workers **ignore `clear_state` unless they run with `ALLOW_CLEAR_STATE=1`**, because it disconnects every broker. Each ignored event is counted as `forbidden_action` (see [Event Limits](#event-limits-and-the-action-allow-list)). `scripts/admin.py` prints a note about this. Set the variable only on workers that you intend to be wipeable.

### Distributing Ingress Rules

`cp.set_ingress(config)` replaces the ingress firewall (`postoffice.ingress.Firewall`, the rule chain evaluated before routing) on every worker. `config` has the same shape as `Firewall.from_dict`, plus an optional `inflight` section with per-broker in-flight caps. Rule types and their arguments are documented in the [Ingress Firewall guide](ingress-firewall.md).

```python
cp.set_ingress({
    "rules": [
        {"type": "max_payload_bytes", "max_bytes": 65536},
        {"type": "topic_shape", "max_depth": 8},
        {"type": "rate_limit", "rate": 500, "burst": 1000},
    ],
    "on_rule_error": "reject",
    "ledger": {},                                  # optional sender ledger; {} uses its defaults
    "inflight": {"mqtt_edge": {"max_inflight": 200, "low_water": 100}},
})
```

- **Rule types:** `max_payload_bytes`, `topic_shape`, `rate_limit`, `content_guard`. At most 64 rules per config. `inflight` limits take `max_inflight` (positive integer), optional `low_water` (`0 <= low_water < max_inflight`) and optional `lease_timeout`.
- **The whole config is replaced**, not merged: send the complete rule set each time. The hash `postoffice:ingress` holds a single `rules` field, so there is no per-broker partial update.
- **Safe swap:** a worker builds the new firewall first. If a rule is unknown or has bad arguments, the worker keeps its current firewall and counts the event as `invalid`. Otherwise the new firewall is installed with a single reference assignment, so a message being routed at that moment sees either the old rules or the new ones, never a mix. In-flight caps for brokers missing from the new config are removed.
- **Identical configs are skipped**, so a resync does not reset rate-limit buckets or sender-ledger state.
- Workers restore the rules from Redis at start-up and on every resync.

---

## Securing the Control Plane

Redis authentication and TLS (`REDIS_PASSWORD`, `REDIS_TLS=1`, plus Redis ACLs) are the primary controls. The features below are defence in depth for the case where Redis is shared, mis-exposed, or a worker is compromised. Without them, anyone who can publish to `postoffice:config_updates` or write the `postoffice:*` hashes can add a broker that points at their own host, change routes, or replace credentials.

### Signed Events and State

Set `POSTOFFICE_EVENT_KEYS` on the control plane **and** on every worker:

```bash
export POSTOFFICE_EVENT_KEYS="2026a:$(openssl rand -hex 32)"
```

The format is `kid:secret[,kid2:secret2...]`. Each secret must be at least 16 bytes; key ids may not contain `:` or `,`.

What gets signed (HMAC-SHA256 over the exact bytes sent, with a separate domain prefix for each kind, so a signature for one purpose cannot be replayed for another):

| What | How |
|---|---|
| Live events | Wire format becomes `{"kid", "sig", "payload"}`, where `payload` is the JSON string `{"action","data","version"}`. |
| Stored state | Each value in `postoffice:brokers`, `:routes`, `:subscriptions` and `:ingress` is wrapped the same way and bound to its hash name and field, so an entry cannot be moved to another field. |
| Version counter | Each counter value gets a signature in `postoffice:version_sigs` (last 1000 kept). A counter without a valid signature is not trusted: the worker stops gap detection but never silently skips events as stale, and counts `bad_version`. |

Because the signature covers the transmitted bytes, a verifier in any language needs only HMAC-SHA256, with no JSON canonicalisation. Old events cannot be replayed: the monotonic version check rejects anything at or below the worker's current version, including after a restart.

> [!NOTE]
> Signing gives **integrity and authenticity, not confidentiality.** Secrets inside `security_config` are still readable by anyone who can read the Redis hashes, and a compromised holder of the key can still sign anything. Keep Redis private and restrict read access with ACLs.

### Signing Modes and Rolling Out

| Mode | Behaviour |
|---|---|
| `enforce` | Unsigned or badly signed events and state entries are rejected. **Default when a key is configured.** |
| `observe` | Verifies and counts failures (`unsigned`, `bad_signature`) but still applies the message. Use it to confirm the control plane signs everything before switching to `enforce`. |
| `off` | No verification; both signed and unsigned messages are accepted. **Default without a key**, with a startup WARNING that events are not authenticated. |

Override with `POSTOFFICE_EVENT_SIGNING_MODE` (or `DataPlane(signing_mode=...)`). A mode other than `off` without a key is a startup error.

Rolling upgrade from an unsigned cluster:

1. Upgrade workers with the key and `POSTOFFICE_EVENT_SIGNING_MODE=off` (they accept either wire format).
2. Switch the control plane to the key. All new events and state are signed.
3. Move workers to `observe`; watch `postoffice_control_events_total{result=~"unsigned|bad_signature"}` stay at zero.
4. Move workers to `enforce` (unset the mode override).

State written before step 2 is unsigned; re-send it (re-run `register_broker`, `add_route`, ... or `scripts/admin.py`) before step 4, otherwise workers in `enforce` skip those entries on the next resync.

### Key Rotation

The **first** key in the list signs; **all** keys verify. Keys are read at process start, so each step is a restart:

1. Add the new key first on every worker: `POSTOFFICE_EVENT_KEYS="2026b:NEW,2026a:OLD"`.
2. Switch the control plane to `"2026b:NEW,2026a:OLD"` and re-send state so stored entries are re-signed.
3. Once nothing signed with `2026a` remains, remove it everywhere.

### Event Limits and the Action Allow-List

Every live event passes a pipeline of checks, cheapest first: **size** (before any JSON parsing) → **rate** → **signature and structural scan** (depth, key and array counts) → **version order** → **action allow-list** → **payload validation** → apply.

| Setting | Environment variable | Default | Effect |
|---|---|---|---|
| `max_event_bytes` | `POSTOFFICE_MAX_EVENT_BYTES` | 262144 (256 KiB) | Larger events (and stored entries) are dropped unparsed. |
| `event_rate` | `POSTOFFICE_EVENT_RATE` | 20 per second | Token-bucket rate at which a worker processes events. |
| `event_burst` | `POSTOFFICE_EVENT_BURST` | 100 | Bucket size. |
| `allowed_actions` | `POSTOFFICE_ALLOWED_ACTIONS` | all actions | Comma-separated subset of `add_broker, add_route, add_subscription, update_broker_security, clear_state, set_ingress`. Unknown names are a startup error. |
| `allow_clear_state` | `ALLOW_CLEAR_STATE=1` | off | Extra opt-in for `clear_state`, on top of the allow-list. |

Constructor arguments win over environment variables, which win over defaults. A worker that only needs to route could run with `POSTOFFICE_ALLOWED_ACTIONS=add_route,add_subscription`.

**Shed events do not lose state.** An event dropped by the rate limit leaves a gap in the version sequence, so the worker re-reads the snapshot from Redis. Those resyncs are spaced with exponential backoff (1 s doubling up to 30 s, reset after 60 s without one), so an event flood cannot become a snapshot storm. A deferred resync is retried when the next event arrives, so a worker whose last event of a burst was shed catches up on the next message. Likewise, a legitimate burst of more than `event_burst` queued events (for example the ones buffered while a snapshot loads) can trip the limiter and recover through a resync; raise `POSTOFFICE_EVENT_BURST` if you bulk-load configuration.

Outcomes are counted in `postoffice_control_events_total{result}`:

| `result` | Meaning |
|---|---|
| `applied` | Event accepted and applied. |
| `stale` | Version at or below the worker's current one (replay or duplicate). |
| `gap` | Version skipped ahead; a resync follows. |
| `unsigned` / `bad_signature` / `bad_version` | Signature problems (counted but still applied in `observe` mode). |
| `oversize` / `rate_limited` | Dropped by the size cap or rate limit. |
| `forbidden_action` | Action not in the allow-list, or `clear_state` without `ALLOW_CLEAR_STATE=1`. |
| `invalid` | Failed payload validation or could not be built (for example a bad ingress rule). |

**Cost of the checks.** `tests/load_control_plane.py` (in-memory Redis stand-in, so it measures only the worker's pipeline, not Redis or broker latency) processed about 64,000 small events/s unsigned and 44,000/s with signature verification on one development machine, about 23 µs per signed event. Events shed by the rate limiter cost about 2 µs, and oversize events about 1 µs, because both are rejected before any parsing or HMAC work. Applying large configs is dominated by building them (50-rule ingress configs: about 4,500/s), not by signing. Run it yourself: `PYTHONPATH=src .venv/bin/python tests/load_control_plane.py [events]`.

Rejection warnings are throttled to one per kind per 10 seconds and name keys and types only, never payload values, because payloads can carry credentials.

### Payload Validation

Events and stored state both come from Redis, so both are validated before they reach plugin constructors:

- Broker, route and subscription names match `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}`.
- Topics are non-empty, contain no NUL byte, and are at most 256 characters and 16 levels deep (the `TopicShape` defaults).
- `protocol` must be a registered plugin.
- Broker `kwargs` may contain only keys the plugin constructor accepts (plus `security_config`, `circuit_breaker_threshold`, `circuit_breaker_timeout`), with matching scalar types: `port` must be an integer, and `True` is not accepted for it. Route and subscription options must be objects with identifier-like keys.
- During a snapshot, one invalid entry is logged, counted as `invalid` and skipped; the rest still loads.

An event refused here still advances the worker's version, so it does not trigger a spurious resync.

> [!NOTE]
> Before this validation existed, extra keys passed through `**kwargs` to the plugin. If a worker rejects a broker with an `invalid` result after an upgrade, check the key against the plugin's constructor parameters.

### Schema Definition Caps

Dynamic Protobuf and Avro definitions are compiled in-process, and they can arrive from Redis (`postoffice:schemas:*`), so they are bounded before compilation. The same check runs on `register_schema` and on the Redis load path; an unacceptable definition from Redis is logged and treated as "no such schema" (never cached).

| Setting | Environment variable | Default |
|---|---|---|
| `max_bytes` | `POSTOFFICE_SCHEMA_MAX_BYTES` | 65536 |
| `max_fields` (Protobuf, nested included) | `POSTOFFICE_SCHEMA_MAX_FIELDS` | 256 |
| `max_depth` (JSON nesting; an Avro nested record costs three levels, so about 10 records deep) | `POSTOFFICE_SCHEMA_MAX_DEPTH` | 32 |

Protobuf dictionaries are also checked for identifier names, unique names and numbers, numbers in `1..536870911` excluding the reserved range `19000-19999`, labels `optional`/`repeated`, and known type names. Schema names are 1-128 characters.

> [!WARNING]
> **Behaviour change:** a Protobuf field with an unknown type name (for example a typo such as `"strng"`) used to compile silently as `string`; it is now rejected.

Schema changes do not propagate to running workers through the event channel; workers load a schema from Redis the first time it is used.

### Environment Variable Reference

| Variable | Used by | Purpose |
|---|---|---|
| `REDIS_HOST`, `REDIS_PORT` | worker | Redis address (default `localhost:6379`). |
| `REDIS_PASSWORD`, `REDIS_TLS=1` | worker | Redis authentication and TLS. |
| `POSTOFFICE_EVENT_KEYS` | control plane, worker | Signing keys (`kid:secret,...`). |
| `POSTOFFICE_EVENT_SIGNING_MODE` | worker | `off`, `observe` or `enforce`. |
| `POSTOFFICE_MAX_EVENT_BYTES`, `POSTOFFICE_EVENT_RATE`, `POSTOFFICE_EVENT_BURST` | worker | Event limits. |
| `POSTOFFICE_ALLOWED_ACTIONS` | worker | Action allow-list. |
| `ALLOW_CLEAR_STATE=1` | worker | Honour `clear_state`. |
| `POSTOFFICE_SCHEMA_MAX_BYTES`, `_MAX_FIELDS`, `_MAX_DEPTH` | any process with a schema registry | Schema definition caps. |

---

## Resiliency & Connection Loss Recovery

In production environments, network partitions or Redis restarts can disrupt the worker's connection to the Control Plane.

`DataPlane` incorporates an automated reconnection loop:
- When the Redis connection drops, the worker catches the error, logs a diagnostic warning, and enters a retry loop.
- It attempts to reconnect with backoff delays, preventing thrashing.
- Once Redis becomes reachable again, the worker automatically resynchronizes cluster state and resumes real-time Pub/Sub listening.
