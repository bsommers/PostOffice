# Phase 2: Semantic Translation & Delivery Guarantees - Context

**Gathered:** 2026-10-06
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 2 coordinates delivery guarantee semantics and acknowledgement propagation across protocols:
1. Designing and implementing a callback-based async confirmation seam (`ack_fn`, `nack_fn`, `on_confirm`, `on_error`) in `BaseClient` and `Router`.
2. Implementing automatic semantic parameter translation (mapping MQTT QoS to AMQP delivery_mode and Kafka delivery callback wait) with explicit route overrides.
3. Enabling publisher confirms and ACK propagation in client plugins (`MqttClient`, `AmqpClient`, `KafkaClient`).
4. Implementing the "All-Succeed" multi-target fanout acknowledgement policy.

</domain>

<decisions>
## Implementation Decisions

### Ack Propagation Mechanism
- **D-01:** Implement a callback-based async confirmation seam. Ingress client calls `self.on_message(topic, message, ack_fn=..., nack_fn=..., **kwargs)`. `Router.route()` attaches completion callbacks to egress `client.publish(..., on_confirm=..., on_error=...)`. Lightweight, zero-dependency, thread-safe.
- **D-02:** Backward compatibility: `ack_fn` and `nack_fn` are optional. If not provided (fire-and-forget mode), routing behaves as before.

### Egress Failure Behavior
- **D-03:** When an egress publish fails (or times out), if `nack_fn` was supplied by the ingress client, invoke `nack_fn(requeue=True)`.
- **D-04:** For protocols with explicit NACK (AMQP), invoke `channel.basic_nack(delivery_tag, requeue=True)`. For protocols without explicit NACK (MQTT), withhold PUBACK so broker state is retained or retried.

### Semantic Parameter Translation
- **D-05:** Automatic translation mappings:
  - MQTT QoS 0 -> AMQP `delivery_mode=1` (transient)
  - MQTT QoS 1/2 -> AMQP `delivery_mode=2` (persistent)
  - AMQP `delivery_mode=2` -> MQTT `qos=1`
  - AMQP `delivery_mode=1` -> MQTT `qos=0`
- **D-06:** Explicit kwargs defined on `add_route` always override automatic defaults.

### Multi-Target Fanout Policy
- **D-07:** "All-Succeed" policy: When a message routes to multiple destinations, track pending confirmations with an atomic countdown latch. Call `ack_fn()` only when all targets report `on_confirm`. If any target reports `on_error`, invoke `nack_fn()`.

### Claude's Discretion
- Implementation specifics of the countdown tracker helper, callback signatures, and test mocks.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture Documentation
- `docs/ARCHITECTURE.md` — Section "The Challenge: Semantic Mismatches" & "Semantic Translation Mapping" (MQTT -> AMQP, MQTT -> Kafka, AMQP -> MQTT, Kafka -> MQTT)
- `docs/USER_GUIDE.md` — Section "Supported Publish Route Parameters"

### Source Files
- `src/postoffice/base_client.py` — `BaseClient` abstract method signatures (`publish`, `on_message`)
- `src/postoffice/router.py` — `Router.route()` dispatch logic
- `src/postoffice/plugins/mqtt_client.py` — MQTT callback and publish implementation
- `src/postoffice/plugins/amqp_client.py` — AMQP consume loop, basic_ack/nack, and publisher confirms
- `src/postoffice/plugins/kafka_client.py` — Kafka producer delivery_report callbacks and consumer commit

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tests/test_postoffice.py` — Integration mock patterns for client publish interception.
- `tests/test_data_plane.py` / `tests/test_control_plane.py` — Hermetic testing standards.

### Established Patterns
- Backward-compatible kwargs passing in `Router.route()` and `BaseClient.publish()`.
- Daemon worker threads and thread-safe callbacks (`add_callback_threadsafe` in `pika`).

</code_context>

<specifics>
## Specific Ideas

- Ensure Kafka delivery report callback `def delivery_report(err, msg)` triggers `on_confirm()` on success and `on_error(err)` on failure.
- In AMQP consumer, allow setting `auto_ack=False` when routes require delivery guarantees so `basic_ack` is only sent after egress confirmation.

</specifics>

<deferred>
## Deferred Ideas

- None — discussion stayed strictly within Phase 2 scope.

</deferred>

---

*Phase: 2-Semantic Translation & Delivery Guarantees*
*Context gathered: 2026-10-06*
