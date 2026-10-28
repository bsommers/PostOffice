# D9 Scoping: Control-plane hardening

Status: scoped 2026-10-25. **Decisions 1-6 accepted as recommended** (user: "go with recommendations"). D9a, D9b, D9c DONE; D9d open. See section 7.
Prerequisites now done: T5 (Redis password/TLS, loopback metrics, skip-verify warnings; `6f4e118`), T7 (subscribe-before-snapshot, idempotent sync, versioned events, resync; `830fcd0`). Threat I in `DOS_PROTECTION_PLAN.md`.

## 1. What an attacker with Redis access can do today

| # | Path | Effect |
|---|---|---|
| 1 | Publish to `postoffice:config_updates` | Add brokers (worker connects to an attacker host), add routes, `clear_state`, hot-swap credentials |
| 2 | Write the `postoffice:*` hashes | Same, applied on **every resync/restart**. T7 made this path matter more: every listener reconnect re-reads the hashes |
| 3 | Write `postoffice:version` | Set it high and the worker ignores all genuine events (`version <= current`); set it low or erratic and workers resync in a loop |
| 4 | Write `postoffice:schemas:*` | Untrusted Protobuf/Avro definitions are compiled dynamically (`schema/registry.py:_build_dynamic_proto_message_class`, no size, field-count or number validation) |
| 5 | Flood the channel | Unbounded `json.loads`, unbounded handler work, resync storms |
| 6 | Legitimate-but-malformed events | `**data["kwargs"]` is splatted into plugin constructors with no key or type validation |

Redis auth/TLS (T5) is the primary control for 1–5. D9 is defence in depth for when Redis is shared, mis-exposed, or a worker is compromised.

## 2. Proposed scope (four commits)

### D9a: Signed events and signed state
- HMAC-SHA256 over canonical JSON (`sort_keys`, no whitespace) of `{action, data, version}`; envelope gains `kid` and `sig`. Constant-time compare (`hmac.compare_digest`).
- Key from `POSTOFFICE_EVENT_KEYS` (`kid:secret,kid2:secret2`; first is the signing key, all verify) so rotation is: add new key to workers, switch the control plane, retire the old one.
- **State is signed too**: each hash value is stored as `{"config": ..., "sig": ...}` and verified at snapshot time (closes path 2). The `version` key gets a companion `postoffice:version_sig` (HMAC of the counter value) so a forged counter is detected (path 3).
- Replay: the T7 version check already rejects replays of old events (`version <= current`), including after a restart, because the snapshot version is current. No timestamp or nonce needed.
- Modes: `off` | `observe` | `enforce`. `observe` verifies and counts failures but still applies (mirrors the D4 ledger's observe-first principle). No key configured means `off` plus a startup WARNING.
- Metric `postoffice_control_events_total{result}`, `result` in `applied|bad_signature|unsigned|oversize|rate_limited|forbidden_action|invalid|stale|gap`. Worker log lines never include payloads (credentials).

### D9b: Event limits and payload validation
- **Size cap** on the raw message before `json.loads` (default 256 KiB). Parse depth/keys limited by reusing the D5 byte scan (extract `ContentGuard._scan` into a shared helper).
- **Rate limit** on incoming events with the existing `TokenBucket` (default 20/s, burst 100). Shed events leave a version gap, so T7 already triggers a resync; add a **minimum resync interval with `ExponentialBackoff`** (D1) so a flood cannot turn into a snapshot storm.
- **Action allow-list** per worker (`POSTOFFICE_ALLOWED_ACTIONS`); `clear_state` additionally requires `ALLOW_CLEAR_STATE=1`. **Behaviour change:** `scripts/admin.py` (which calls `clear_state`) stops affecting workers unless they set that flag.
- **Payload validation before `**kwargs`**: per-action required keys and types; broker/topic names against a pattern and `TopicShape` limits; `protocol` must be a registered plugin; broker `kwargs` restricted to the keys each plugin's `__init__` accepts (derived with `inspect.signature`, so no hand-maintained lists) and JSON-scalar/dict values only.

### D9c: Schema definition caps
- Before compiling a dynamic Protobuf definition: max serialized size (default 64 KiB), max fields (256), unique field numbers in 1..2^29-1 excluding 19000–19999, identifier pattern for names, allowed type names only (currently unknown types silently become `string`).
- Avro: size cap and a nesting-depth cap before `fastavro.parse_schema`.
- Applied on `register_schema` **and** on the Redis load path in `get_schema` (the untrusted one).

### D9d: Ingress rules over the Control Plane
- New action `set_ingress` carrying the full `Firewall.from_dict` config (plus `inflight`), stored as one hash entry `postoffice:ingress`, included in the snapshot and signed like everything else.
- Worker builds the new `Firewall` first; on validation error it keeps the old one and counts `invalid`. Success swaps via `PostOffice.configure_ingress`, which is a single reference assignment (atomic for in-flight `route()` calls) and rebinds the content-guard converter.
- `ControlPlane.set_ingress(config)`; also gives D10's "rules file in the worker" a shared loader.

## 3. Out of scope (noted, not done)
- Authenticating **who** may publish (per-publisher identity, ACLs): Redis ACLs (T5) are the right layer.
- Encrypting event payloads (TLS to Redis covers transport; credentials in `security_config` remain readable by anyone who can read the hashes. Worth a follow-up: encrypt secrets at rest or move to a secret store).
- Removing entities deleted from Redis on resync (no delete API exists).
- Asymmetric signatures (see decision 4).

## 4. Test plan (about 40 tests, all hermetic)
- Signing: valid event applies; tampered data/action/version rejected; unsigned rejected in `enforce`, applied-but-counted in `observe`; unknown `kid` rejected; rotation (old and new key both verify, control plane signs with the first); signed state verified at snapshot, tampered hash entry skipped and counted; forged `version` detected; no key means `off` + warning; payloads never logged.
- Limits: oversize message dropped before parsing; rate limit sheds excess then a gap triggers one (not many) resyncs; resync interval backs off; `clear_state` ignored unless enabled; disallowed action ignored.
- Validation: bad names/topics rejected; unknown protocol rejected; unexpected broker kwarg rejected; wrong types rejected; valid events unchanged (regression against the T7 suite).
- Schemas: oversize/over-field/duplicate-number/reserved-number Protobuf rejected on register and on Redis load; deep Avro rejected; existing schema tests unchanged.
- Ingress: rules event swaps atomically (a concurrent `route()` sees old or new, never partial); invalid config keeps the old firewall; snapshot restores rules after restart; `inflight` section applied.

## 5. Decisions needed

| # | Question | Recommendation |
|---|---|---|
| 1 | Sign the stored hashes and the version counter too, or events only? | **Both.** Events-only leaves the resync path (paths 2 and 3) open, and T7 made resync routine |
| 2 | Default mode when a key is configured | **`enforce`** (a configured key means the operator wants it); `observe` available for rollout. No key: `off` with a loud startup warning |
| 3 | `clear_state` disabled on workers by default (breaks `scripts/admin.py` clearing live workers) | **Yes, disabled by default**; `admin.py` prints a note, or workers opt in with `ALLOW_CLEAR_STATE=1` |
| 4 | Shared-secret HMAC now, or Ed25519 (control plane holds the private key, workers only the public key, so a compromised worker cannot forge events)? | **HMAC now** (stdlib, no new dependency). Ed25519 needs the `cryptography` package and fits the native-rewrite plan better; leave a `Signer` interface so it can be swapped in |
| 5 | Defaults: event size 256 KiB, 20 events/s (burst 100), Protobuf 64 KiB / 256 fields | **Accept**, all configurable |
| 6 | Include D9d (ingress rules over the Control Plane) here, or defer to D10 with the rules file? | **Include as the last commit**; it is the only part that adds new capability rather than hardening |

## 6. Risks
- D9a changes the wire format (`kid`, `sig`, wrapped hash values). Mixed old/new workers during a rolling upgrade: `off` mode must keep accepting unsigned events and must read both the old and the wrapped hash formats.
- D9b's kwargs allow-list derived from plugin signatures breaks any worker that relied on passing extra keys through `**kwargs` to `BaseClient`. The plugin tests will show which keys are in real use.
- Everything here assumes Redis is the only channel. A compromised **control plane** holding the key can still do anything; signing protects the transport and storage, not the signer.

## 7. Progress log

### D9a: Signed events and state (DONE)
Implemented: `postoffice/signing.py` (`HmacSigner`, `POSTOFFICE_EVENT_KEYS="kid:secret,..."`, first key signs, all verify, keys must be >= 16 bytes), `ControlPlane(signer=)` seals events and stored hash entries, `DataPlane(signer=, signing_mode=)` verifies (`POSTOFFICE_EVENT_SIGNING_MODE` overrides; default `enforce` with a key, `off` without plus a startup WARNING), metric `postoffice_control_events_total{result}` with `applied|stale|gap|unsigned|bad_signature|bad_version|invalid`, rejection warnings throttled to one per kind per 10 s and never containing payloads.

Design changes from the scoping text above:
- **Signatures cover the exact payload bytes as transmitted**, not canonical JSON. Envelope is `{"kid","sig","payload"}` where `payload` is the JSON string. Any language verifies with plain HMAC-SHA256 over the received bytes, which matters for the native/polyglot plan (no cross-language JSON canonicalisation).
- **Domain separation**: `event\0`, `state\0<hash>\0<field>\0` and `version\0` prefixes. A stored entry cannot be replayed as an event, nor moved to another field or hash.
- **Version counter** is protected by a signature per counter value in `postoffice:version_sigs` (last 1000 kept). A counter without a valid signature is not trusted: the worker keeps `_version = None` (no gap detection, but events are never silently skipped as stale) and counts `bad_version`. A short grace (3 x 50 ms) covers the writer's INCR-then-store window.
- Sync is now tolerant per entry: one rejected or unusable broker/route/subscription is logged and skipped instead of aborting the whole snapshot.
- `off` mode accepts signed and unsigned messages alike, so a rolling upgrade can start with workers on `off`.

Tests: `tests/test_control_signing.py` (35, in-memory) plus `TestSignedControlPlane` in `tests/integration/` (5, real Redis: signed round trip, state written straight into Redis rejected, forged version counter, forged vs genuine live events, wrong-key worker).

Known gaps / follow-ups: no key hot-reload (restart to rotate); `ControlPlane` still reads keys from the environment implicitly when no signer is passed; secrets inside `security_config` remain readable at rest by anyone who can read Redis (signing gives integrity, not confidentiality).

### D9b: Event limits and payload validation (DONE)
Implemented: `postoffice/validation.py` (`validate_action`, `PayloadError`, `plugin_kwargs`), `ingress.content.scan_json` (the D5 scan extracted so both use one implementation), and in `DataPlane`: size cap, token-bucket rate limit, action allow-list, validation, resync spacing.

Pipeline per live event, cheapest first: size (before any parse) -> rate -> signature + structural scan + parse -> version order -> allow-list -> validation -> apply. New `postoffice_control_events_total{result}` values: `oversize`, `rate_limited`, `forbidden_action`, `invalid`.
- Config (argument, else env, else default): `max_event_bytes`/`POSTOFFICE_MAX_EVENT_BYTES` 256 KiB; `event_rate`/`POSTOFFICE_EVENT_RATE` 20 per s; `event_burst`/`POSTOFFICE_EVENT_BURST` 100; `allowed_actions`/`POSTOFFICE_ALLOWED_ACTIONS` (comma list, unknown names are a startup error); `allow_clear_state`/`ALLOW_CLEAR_STATE=1`.
- **Behaviour change:** workers ignore `clear_state` unless `ALLOW_CLEAR_STATE=1`. `scripts/admin.py` prints a note. The two existing clear_state test modules opt in explicitly.
- Stored state gets the same size cap and validation (a bad entry is skipped and counted `invalid`, the rest of the snapshot still loads).
- Broker kwargs are checked against the plugin constructor signatures along the class hierarchy (plus `security_config`, `circuit_breaker_*`); scalar types come from annotations or defaults, so `port` must be an int and `True` is rejected. Topics: non-empty, no NUL, <= 256 chars, <= 16 levels (same defaults as `TopicShape`). Names: `[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}`. Error messages and logs name keys and types, never values.
- Events refused for `forbidden_action` or `invalid` still advance the version, so they do not cause a spurious gap resync.
- Shed events set `_needs_resync`; resyncs triggered from the listener are spaced by `ExponentialBackoff` (1 s doubling to 30 s, reset after 60 s quiet). Direct `resync()` calls from `_converge` are not delayed.

Known gaps: a deferred resync is retried on the *next* message, so if the last event of a burst is shed and nothing follows, the worker stays behind until the next event (a timer thread would fix this but needs locking around apply; not done). The rate limit also applies to the buffered events replayed after a snapshot, which a legitimate burst over 100 will trip; it recovers by resync. Key-name length of `target_kwargs` is bounded (64) but values are not type-checked.

Tests: `tests/test_control_limits.py` (43, hermetic). Suite 448 OK; integration tier 12 OK against real Redis.

### D9c: Schema definition caps (DONE)
Implemented: `postoffice/schema/limits.py` (`SchemaLimits`, `SchemaDefinitionError`, `validate_schema_definition`). `scan_json` moved to the neutral `postoffice/jsonscan.py` (importing it from `ingress.content` would have been circular through `schema.converters`); `ContentGuard` and `DataPlane` both use it.
- `InMemorySchemaRegistry.register_schema` validates before compiling, so one check covers local registration **and** the Redis load path. `RedisSchemaRegistry.register_schema` validates before any Redis write. `get_schema` checks the raw string length before `json.loads`, and an unacceptable definition from Redis is logged and treated as "no such schema" (returns None, never cached).
- Defaults, all configurable (`SchemaLimits(...)` or `POSTOFFICE_SCHEMA_MAX_BYTES` / `_MAX_FIELDS` / `_MAX_DEPTH`): 64 KiB, 256 fields, depth 32. Depth is JSON nesting; a nested Avro record costs three levels, so 32 is about 10 records deep.
- Protobuf dict: identifier patterns for message and field names, dotted package pattern, unique names and numbers, numbers in 1..2^29-1 excluding 19000-19999 (bool rejected), label `optional`/`repeated`, and **unknown type names are now rejected** (before: silently `string`). `FileDescriptorProto`: byte size, at least one message, total fields (nested included), field numbers. Compiled `Message` classes are trusted (code, not data).
- Avro: JSON strings are size-checked and structurally scanned before parsing; dicts get an iterative depth walk with a node budget (a self-referencing Python structure terminates) and a serialized-size check.
- Schema names: 1-128 characters, no NUL.

**Behaviour change:** a Protobuf definition with a misspelled type (`"strng"`) used to compile as `string`; it is now an error.

Known gaps: the Redis write still happens before fastavro/protobuf compile, so a definition within the caps that fails to compile is stored in Redis and fails on each worker (pre-existing; reorder if it matters). Schema changes still do not propagate to running workers via the event channel; they load lazily on first use.

Tests: `tests/test_schema_limits.py` (34, hermetic). Suite 482 OK.

### D9d: Ingress rules over the Control Plane (DONE)
Implemented: action `set_ingress` (data = `Firewall.from_dict` config plus `inflight`), `ControlPlane.set_ingress(config)` storing it signed under `postoffice:ingress` field `rules`, loaded in `DataPlane.sync_state`. The validator checks shape only (keys, rule count <= 64, inflight bounds); the worker builds the `Firewall` before touching anything, so an unknown rule type or bad rule arguments keep the old firewall and count `invalid`. Identical configs are skipped (a resync does not reset rate-limit or ledger state). Swap is `PostOffice.configure_ingress(firewall)`; inflight caps dropped from the new config are removed. `clear_state` also deletes the hash and disables the firewall.

Known gaps: the hash holds a single `rules` field, so there is no per-broker partial update; docs for the new action are left to D10.

Tests: `tests/test_control_ingress.py` (20, hermetic). Suite 502 OK; integration tier not rerun.

### D10: docs and load test (DONE, quick 261028-d10)
Documented the whole D9 arc in `docs/user-guide/distributed-plane.md` (plus cross-references) and added `tests/load_control_plane.py`. Follow-up done in quick 261028-d11: firewall guide written, integration tier rerun (12 OK).
