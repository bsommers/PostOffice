# Ingress Firewall, Sender Ledger & In-Flight Limits

[← Back to User Guide](README.md) | [← Back to Project README](../../README.md)

The ingress firewall evaluates every incoming message **before routing**. It rejects oversized, malformed or abusive traffic at the edge so that a stuck device or a hostile sender cannot exhaust brokers, schemas or DLQs. It is disabled until you configure it.

---

## Table of Contents
- [How Evaluation Works](#how-evaluation-works)
- [Configuring the Firewall](#configuring-the-firewall)
- [Rule Reference](#rule-reference)
  - [`max_payload_bytes`](#max_payload_bytes)
  - [`topic_shape`](#topic_shape)
  - [`rate_limit`](#rate_limit)
  - [`content_guard`](#content_guard)
- [Sender Ledger](#sender-ledger)
- [In-Flight Limits (Back-Pressure)](#in-flight-limits-back-pressure)
- [Retry Cap](#retry-cap)
- [Distributing Rules to Workers](#distributing-rules-to-workers)
- [Metrics](#metrics)

---

## How Evaluation Works

Rules form an ordered chain and **the first non-allow verdict wins**. Each rule yields one of three actions:

| Action | Effect on the ingress message |
|---|---|
| `allow` | Continue to the next rule, then routing. |
| `drop` | Acknowledge (`ack_fn`) and discard. Use for traffic you want to silently shed. |
| `reject` | Negative-acknowledge without requeue (`nack_fn(requeue=False)`), so the source broker can dead-letter it. **Default for every rule.** |

Before the chain runs, a sender that the [ledger](#sender-ledger) has put into cool-down is dropped immediately. A rule that itself raises an exception **fails closed** by default (`on_rule_error: "reject"`); set it to `"allow"` to skip a broken rule instead, or `"drop"`.

Each rule can be limited to some source brokers with `brokers: ["mqtt_edge", ...]`; for other brokers it costs one set lookup.

---

## Configuring the Firewall

Pass a dictionary (the same shape works in a rules file, over the [Control Plane](distributed-plane.md#distributing-ingress-rules), or through `Firewall.from_dict`):

```python
from postoffice.app import PostOffice

po = PostOffice()
po.configure_ingress({
    "rules": [
        {"type": "max_payload_bytes", "max_bytes": 65536},
        {"type": "topic_shape", "max_depth": 8, "deny": ["$SYS/#", "admin/+/secret"]},
        {"type": "rate_limit", "rate": 500, "burst": 1000, "key": "broker"},
        {"type": "content_guard", "topic": "telemetry/+", "format": "json",
         "required_fields": ["device_id", "temp"],
         "field_ranges": {"temp": {"min": -60, "max": 150}}},
    ],
    "on_rule_error": "reject",
    "ledger": {},                                    # optional; {} uses the defaults
    "inflight": {"mqtt_edge": {"max_inflight": 200}},  # optional, see In-Flight Limits
})
```

`configure_ingress(None)` disables the firewall. You can also build `Firewall([...rules])` yourself and pass the object. An unknown rule `type` or invalid rule arguments raise `ValueError` and nothing is installed.

---

## Rule Reference

All rules also accept `action` (`"reject"` default, `"drop"`, `"allow"`) and `brokers`.

### `max_payload_bytes`

| Argument | Default | Meaning |
|---|---|---|
| `max_bytes` | required, `> 0` | Reject messages larger than this. Uses the declared size only: the payload is never decoded or copied. |

Category: `oversize`.

### `topic_shape`

| Argument | Default | Meaning |
|---|---|---|
| `max_length` | 256 | Maximum topic length in characters. |
| `max_depth` | 16 | Maximum number of `/`-separated levels. |
| `deny` | `[]` | MQTT-style patterns (`+` one level, `#` the rest) that are forbidden. |

Also rejects empty topics and topics containing a NUL byte. Categories: `malformed`, `forbidden_topic`.

### `rate_limit`

Token-bucket limiter, keyed so one noisy sender cannot starve the others.

| Argument | Default | Meaning |
|---|---|---|
| `rate` | required | Tokens per second. |
| `burst` | `rate` | Bucket size. |
| `key` | `"broker"` | `"broker"` (one bucket per source broker), `"topic"` (per broker and topic) or `"identity"` (per sender identity taken from the topic). |
| `template` | none | Required for `key: "identity"`. A topic template such as `devices/{device_id}/telemetry`; the first `{name}` segment is the identity. Topics that do not match the template fall back to the full topic. |
| `cost` | `"messages"` | `"messages"` (1 per message) or `"bytes"` (message size, so `rate` is bytes/second). |
| `max_keys`, `ttl` | 10000, 300 s | Bound on tracked keys and their idle lifetime, so key-cardinality floods cannot grow memory. |

Category: `rate_exceeded`.

### `content_guard`

Application-aware checks for **one topic pattern**; messages on other topics only cost a pattern match.

| Argument | Default | Meaning |
|---|---|---|
| `topic` | required | MQTT-style pattern this guard applies to. |
| `format` | `"json"` | `"json"`, `"avro"` or `"protobuf"`. |
| `schema` | none | Registered schema name; **required** for Avro/Protobuf, **not allowed** for JSON. The payload is decoded through the router's schema converter. |
| `max_bytes` | none | Payload size cap for this topic. |
| `max_depth`, `max_keys`, `max_array_length`, `max_string_length` | 32, 1000, 10000, 65536 | Structural bounds for JSON, checked by a linear byte scan **before** `json.loads`, so hostile nesting cannot cause `RecursionError` or memory blow-up. |
| `required_fields` | `[]` | Dotted paths that must exist (`"device.id"`). |
| `field_ranges` | `{}` | `{"temp": {"min": 0, "max": 100}}`; the value must be a number (booleans are not). Absent fields are skipped, so combine with `required_fields`. |
| `enums` | `{}` | `{"status": ["ok", "fault"]}`. |

Categories: `oversize`, `malformed` (structure, invalid UTF-8 or JSON) and `schema_mismatch` (schema decode or business-rule violations).

---

## Sender Ledger

Oversized, malformed or rate-exceeding traffic is as often a misconfigured or stuck device as an attack, so the ledger **observes first**: it counts problems per sender and tells you what looks wrong. Enable it with `"ledger": {...}`.

Sender identity is, in order: the explicit sender the plugin supplies (AMQP `user_id`, Kafka key), the first `{placeholder}` of `identity_template`, or the topic.

```python
for row in po.sender_report(top=5):
    print(row["broker"], row["sender"], row["total"], row["categories"],
          row["rising"], row["likely_cause"])
```

`likely_cause` is a hint: `widespread` (many senders at once, probably not one device), `repeated_identical_error` (stuck client), `mixed_high_rate`, `varied_errors`, or `insufficient_data`. A warning is logged once per window when a sender's rate rises quickly.

| Argument | Default | Meaning |
|---|---|---|
| `max_keys`, `ttl` | 1000, 3600 s | Tracked senders and idle lifetime. Beyond `max_keys`, further senders are aggregated into one `other` row rather than evicting tracked offenders. |
| `window` | 60 s | Window for rate comparisons. |
| `identity_template` | none | Topic template that names the sender. |
| `enforce` | `false` | When `true`, a sender with `strike_threshold` problem events inside one window is dropped for an exponentially growing cool-down. |
| `strike_threshold` | 10 | Events per window that trigger a cool-down. |
| `cooldown_base`, `cooldown_factor`, `cooldown_cap` | 30 s, 2.0, 900 s | Cool-down growth. `ttl` must be at least `cooldown_cap` when `enforce` is set. |
| `decay_after` | 600 s | A quiet period that forgives earlier strikes. |

Roll out with `enforce: false`, read `sender_report()` and the `postoffice_sender_events_total` metric, then switch enforcement on. Unroutable messages are recorded in the ledger too (category `unroutable`).

---

## In-Flight Limits (Back-Pressure)

`inflight` caps how many messages from a source broker are accepted but not yet acknowledged, and pauses the source client instead of letting its queue overrun the workers.

```python
po.set_inflight_limit("mqtt_edge", max_inflight=200, low_water=100, lease_timeout=60.0)
po.set_inflight_limit("mqtt_edge", None)      # remove
```

- The source is paused when `max_inflight` is reached and resumed when in-flight falls to `low_water` (default `max_inflight // 2`; `0 <= low_water < max_inflight`), so it does not flap per message.
- A slot is held until the message is acked or nacked. Sources without confirmation callbacks (for example plain MQTT) only hold a slot for the time spent inside `route()`.
- A lease whose confirmation never arrives is reclaimed after `lease_timeout`, so one lost callback cannot shrink capacity forever.
- Messages over the cap are shed and counted as `postoffice_routing_errors_total{error_type="inflight_limit"}`.

---

## Retry Cap

A message whose delivery keeps failing and keeps being requeued cannot loop forever:

```python
po.set_retry_policy(max_attempts=5, base_delay=0.5)   # default; None = unbounded immediate requeue
```

Each requeue is delayed with exponential backoff. After `max_attempts` failed deliveries the message is nacked without requeue and counted as `retry_exhausted`.

---

## Distributing Rules to Workers

With a Control Plane deployment, send the whole config with `cp.set_ingress(config)`. Workers validate the shape, build the firewall first, and swap it atomically; a bad rule leaves the previous firewall in place and counts the event as `invalid`. See [Distributing Ingress Rules](distributed-plane.md#distributing-ingress-rules).

---

## Metrics

| Metric | Labels | Meaning |
|---|---|---|
| `postoffice_ingress_total` | `source_broker`, `rule`, `action` | Messages stopped by the firewall (`rule` is the rule type, `cooldown` for cooled senders). |
| `postoffice_sender_events_total` | `source_broker`, `category` | Problem events by category (`oversize`, `malformed`, `rate_exceeded`, `forbidden_topic`, `schema_mismatch`, `cooled`, `unroutable`). No sender label, to keep cardinality bounded; use `sender_report()` for per-sender detail. |
| `postoffice_inflight_messages` | `source_broker` | Accepted messages not yet acknowledged. |
| `postoffice_routing_errors_total` | `source_broker`, `error_type` | Includes `inflight_limit` and `retry_exhausted`. |
