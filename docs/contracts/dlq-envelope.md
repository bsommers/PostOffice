# Dead-Letter Queue (DLQ) Envelope & Budget Contract (v1)

## 1. DLQ JSON Envelope Schema

Every message published to a Dead-Letter Queue broker/topic via `format_dlq_payload` MUST serialize to a UTF-8 JSON object with the following fields:

```json
{
  "source_broker": "mqtt_edge",
  "source_topic": "sensors/temperature",
  "timestamp": "2026-10-31T00:00:00+00:00",
  "error": "Target broker disconnected",
  "payload": "{\"temp\": 22.5}",
  "is_base64": false,
  "metadata": {}
}
```

### Field Rules
- `source_broker` (`string`): Name of the ingress broker where the message originated.
- `source_topic` (`string`): Concrete topic on the ingress broker.
- `timestamp` (`string`): ISO-8601 UTC timestamp (`YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00`).
- `error` (`string`): Human-readable failure reason.
- `payload` (`string` or `null`):
  - If the original `bytes` payload is valid UTF-8, `payload` contains the decoded UTF-8 string and `is_base64` is `false`.
  - If the original `bytes` payload is not valid UTF-8, `payload` contains the standard ASCII Base64 encoding and `is_base64` is `true`.
  - If truncated by `BudgetedDlq` (`max_payload_bytes` exceeded), `payload` is `null` and `is_base64` is `false`.
- `metadata` (`object`): Optional dictionary of additional diagnostic fields:
  - `repeat_count` (`int`): Added when prior identical failures were suppressed by deduplication within the dedupe window.
  - `payload_truncated` (`bool`): `true` when `len(original_payload) > max_payload_bytes`.
  - `original_size` (`int`): Byte length of the truncated original payload.
  - `sha256` (`string`): 64-char hex SHA-256 digest of the truncated original payload.

## 2. DLQ Deduplication Fingerprint (`dlq_fingerprint`)

To deduplicate identical failure storms in bounded memory, `dlq_fingerprint` computes a 16-byte (32-hex-character) BLAKE2b digest (`digest_size=16`):

```
BLAKE2b-128(
    source_broker.encode("utf-8") + b"\x00" +
    source_topic.encode("utf-8")  + b"\x00" +
    error.encode("utf-8")         + b"\x00" +
    payload[:4096]
).hexdigest()
```

Only the first `4096` bytes (`FINGERPRINT_PAYLOAD_SAMPLE`) of `payload` are hashed so large messages do not stall hashing.

## 3. `BudgetedDlq` Admission Order

When a DLQ write is evaluated by `BudgetedDlq.send(source_broker, source_topic, payload, error, metadata)`:

1. **Oversize Check (`len(payload) > max_payload_bytes`):**
   - Does NOT drop the failure notification; instead replaces the body with a metadata-only envelope (`payload: null`, `metadata.payload_truncated: true`, `metadata.original_size`, `metadata.sha256`) and increments `oversize_truncated`.
2. **Deduplication Window (`dedupe_window > 0`):**
   - Computes `fp = dlq_fingerprint(source_broker, source_topic, error, payload)`.
   - Evicts expired entries older than `now - dedupe_window` from the head of the `OrderedDict`.
   - If `fp` is still active in the window, increments its `suppressed` counter, increments `suppressed_duplicate`, and drops the write (`Admission(refusal="duplicate")`).
   - If `fp` was not active (or its previous window expired), records any prior `suppressed` count in `metadata["repeat_count"]` (when `> 0`) without inserting `fp` into the dedupe window until rate and byte token buckets succeed.
3. **Message Rate Limit (`max_rate` msgs/sec):**
   - If the message token bucket rejects the write, increments `dropped_rate` and returns `Admission(refusal="rate_limited")`.
4. **Byte Rate Limit (`max_bytes_per_sec` bytes/sec):**
   - Evaluates the byte length of the formatted JSON envelope (`cost = max(len(envelope_bytes), 1)`).
   - If the byte token bucket rejects the write, increments `dropped_bytes` and returns `Admission(refusal="byte_limited")`.
5. **Commit & Publish:**
   - Records `fp` in the bounded LRU `OrderedDict` (evicting oldest if `len > max_dedupe_entries`), publishes the envelope to the underlying DLQ broker, and increments `sent` on success.
