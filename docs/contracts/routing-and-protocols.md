# Routing, Topic Matching, Wire Framing & Protocol Semantics Contract (v1)

## 1. Topic Matching & Wildcard Rules (`TopicTrie` & `topic_matches`)

1. **Segment Delimiter:** Forward slash `/` splits a topic or pattern into segments (`topic.split("/")`). Leading and trailing slashes produce an empty-string segment `""` at the start or end.
2. **Single-Level Wildcard (`+`):**
   - Must occupy the entire segment (`"a/+/c"` is valid; `"a/+b/c"` raises `ValueError`).
   - Matches exactly one segment at that depth (including an empty segment `""`).
3. **Multi-Level Wildcard (`#`):**
   - Must occupy the entire final segment (`"a/#"` or `"#"` is valid; `"a/#/b"` or `"a/#b"` raises `ValueError`).
   - Matches zero or more remaining segments (e.g., `"sensors/#"` matches `"sensors"`, `"sensors/temp"`, and `"sensors/room1/temp"`).
4. **MQTT §4.7.2 `$`-Prefix Isolation Rule:**
   - Topics starting with `$` (such as `$SYS/broker/load`) MUST NOT be matched by patterns whose first segment is `+` or `#` (e.g., `"#"` or `"+/broker/load"`).
   - Patterns whose first segment explicitly matches the `$` segment (such as `"$SYS/#"` or `"$SYS/+/load"`) DO match `$SYS/broker/load`.
5. **Idempotent Route Registration (`Router.add_route`):**
   - Duplicate registration of an identical `(source_broker, source_topic, targets, target_kwargs)` route is a no-op and MUST NOT cause duplicate message deliveries.

## 2. Firewall Verdict & Unroutable Policy Semantics

### Firewall Verdicts (`Firewall.check`)
- `allow`: Message proceeds to `TopicTrie` lookup and egress delivery.
- `drop`: Message is silently discarded; router returns `True` (acknowledged to the ingress broker so it is not redelivered) and does NOT write to the DLQ.
- `reject`: Message is rejected; router returns `False` (`nack(requeue=False)` to the ingress broker) and does NOT write to the DLQ.

### Unroutable Policies (`Router.set_unroutable_policy`)
When a message passes the firewall (`allow`) but matches zero routes in `TopicTrie`:
- `nack` (default): Records `unroutable_message` metric and returns `False` (`nack`).
- `dlq`: Records `unroutable_message` metric, forwards the message to the configured DLQ (`error="Unroutable message: no matching routes"`), and returns `True` iff the DLQ write succeeded (or `False` if no DLQ is configured).
- `drop`: Records `unroutable_message` metric and returns `True` (silent ack).

## 3. Semantic Translation (`translate_semantics`)

When `semantic_translation=True` is set on a route and `metadata` is present:
- **MQTT (`qos` in `0, 1, 2`) -> AMQP (`delivery_mode`):**
  - `qos > 0` (`1` or `2`) -> `delivery_mode = 2` (Persistent)
  - `qos == 0` -> `delivery_mode = 1` (Transient)
- **AMQP (`delivery_mode` in `1, 2`) -> MQTT (`qos`):**
  - `delivery_mode == 2` -> `qos = 1` (At least once)
  - `delivery_mode == 1` -> `qos = 0` (At most once)

## 4. Confluent 5-Byte Wire Framing (`SchemaRegistry`)

Schema-encoded payloads use the 5-byte Confluent Schema Registry wire header (`>bI` big-endian struct):

```
+-------------------+-----------------------------------+-----------------------+
| Magic Byte (1 B)  | Schema ID (4 B, big-endian uint32)| Encoded Payload (N B) |
|       0x00        |            0x00000001             |         ...           |
+-------------------+-----------------------------------+-----------------------+
```

- Framing smaller than 5 bytes raises `ValueError("Payload is too short to contain Confluent wire format header.")`.
- First byte `!= 0x00` raises `ValueError("Invalid magic byte: ... Expected 0x00.")`.

## 5. Per-Protocol Ingress Ack/Nack & Requeue Rules

| Verdict / Outcome | AMQP (`basic_ack` / `basic_nack`) | Kafka (`commit`) | MQTT / NanoMQ |
|---|---|---|---|
| Routed (`True`) | `basic_ack(delivery_tag)` | `commit(message=msg, asynchronous=False)` | Automatic / PUBACK |
| Firewall `drop` (`True`) | `basic_ack(delivery_tag)` | `commit(message=msg, asynchronous=False)` | Automatic / PUBACK |
| Firewall `reject` (`False`) | `basic_nack(delivery_tag, requeue=False)` | Not committed (or DLQ/skip per policy) | Disconnect / drop |
| Transient Egress Error (`False`, no DLQ) | `basic_nack(delivery_tag, requeue=True)` | Not committed (redelivered on poll) | QoS retry |
| Egress Error Handled by DLQ (`True`) | `basic_ack(delivery_tag)` | `commit(message=msg, asynchronous=False)` | Automatic / PUBACK |
