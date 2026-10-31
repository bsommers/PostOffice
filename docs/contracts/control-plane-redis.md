# Control Plane & Redis State Contract (v1)

## 1. Redis Keys & Data Structures

| Redis Key | Type | Purpose |
|---|---|---|
| `postoffice:brokers` | Hash (`field = broker_name`) | JSON-encoded broker registration (`name`, `type`, `kwargs`). |
| `postoffice:routes` | Hash (`field = route_id`) | JSON-encoded route entry (`source_broker`, `source_topic`, `target_broker`, `target_topic`, `target_kwargs`). |
| `postoffice:subscriptions` | Hash (`field = "{broker}:{topic}"`) | JSON-encoded ingress subscription (`broker_name`, `topic`, `kwargs`). |
| `postoffice:ingress` | Hash (`field = "rules"`) | JSON-encoded ingress firewall configuration (`rules`). |
| `postoffice:version` | String (integer counter) | Monotonically increasing control-plane mutation version (`INCR`). |
| `postoffice:version_sigs` | Hash (`field = str(version)`) | D9 HMAC-SHA256 signature envelope for `version` (retains latest 1,000 versions). |
| `postoffice:schemas:{id}` | String | JSON-encoded schema registration (`schema_type`, `schema_str`). |
| `postoffice:config_updates` | Pub/Sub Channel | Live mutation broadcast channel. |

## 2. Pub/Sub Event Wire Format

Events published on `postoffice:config_updates` have the JSON structure:

```json
{
  "action": "add_broker | add_route | add_subscription | update_broker_security | set_ingress | clear_state",
  "data": {},
  "version": 42,
  "schema_version": 1
}
```

- `schema_version`: Integer `1`. When omitted by legacy writers, DataPlane workers treat the event as `schema_version = 1`. Any event with a non-integer `schema_version` or `schema_version != 1` MUST be rejected with control-plane metric outcome `invalid`.
- Allowed `action` values (`ACTIONS`):
  - `add_broker`
  - `add_route`
  - `add_subscription`
  - `update_broker_security`
  - `set_ingress`
  - `clear_state` (requires explicit `allow_clear_state=True` / `ALLOW_CLEAR_STATE=1` on DataPlane).

## 3. D9 HMAC-SHA256 Authentication & Domain Separation

When `POSTOFFICE_EVENT_KEYS` (`kid1:secret1,kid2:secret2`) is configured:

1. **Signed Envelope JSON:**
   ```json
   {
     "v": 1,
     "kid": "k1",
     "sig": "<64-char lowercase hex HMAC-SHA256>",
     "payload": "<exact inner JSON string>"
   }
   ```
2. **Domain Separation Prefixes (prevent cross-context replay):**
   - **Pub/Sub Event (`sign_event` / `verify_event`):**
     `HMAC-SHA256(key, b"event\x00" + payload_utf8)`
   - **Redis Hash Entry (`sign_state` / `verify_state`):**
     `HMAC-SHA256(key, b"state\x00" + hash_name_utf8 + b"\x00" + field_utf8 + b"\x00" + payload_utf8)`
   - **Version Counter (`sign_version` / `verify_version`):**
     `HMAC-SHA256(key, b"version\x00" + str(int(version)).encode("utf-8"))`
     stored as `{"v": 1, "kid": "<kid>", "sig": "<hex>"}`.
3. **Signing Modes (`POSTOFFICE_EVENT_SIGNING_MODE`):**
   - `off`: Accepts signed and unsigned messages without verification.
   - `observe`: Verifies signatures and increments `unsigned` / `bad_signature` / `bad_version` counters on failure, but still applies the payload.
   - `enforce`: Drops unsigned or invalidly signed payloads and version counters.

## 4. Convergence Protocol

1. **Subscribe-Before-Snapshot:** A DataPlane worker MUST open and confirm its `SUBSCRIBE postoffice:config_updates` before reading `postoffice:version` and the state hashes (`postoffice:brokers`, `postoffice:routes`, `postoffice:subscriptions`, `postoffice:ingress`), buffering live messages during the snapshot read.
2. **Monotonic Version Tracking:**
   - Events with `version <= local_version` are ignored (`stale`).
   - Events with `version == local_version + 1` are applied in-order (`applied`).
   - Events with `version > local_version + 1` or non-integer `version` trigger a full snapshot resync (`gap`).
