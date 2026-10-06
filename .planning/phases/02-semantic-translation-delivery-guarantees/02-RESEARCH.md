# Phase 2: Semantic Translation & Delivery Guarantees - Research

**Researched:** 2026-10-06
**Domain:** Cross-Protocol Acknowledgements, Publisher Confirms, Delivery Guarantees
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Implement a callback-based async confirmation seam. Ingress client calls `self.on_message(topic, message, ack_fn=..., nack_fn=..., **kwargs)`. `Router.route()` attaches completion callbacks to egress `client.publish(..., on_confirm=..., on_error=...)`. Lightweight, zero-dependency, thread-safe.
- **D-02:** Backward compatibility: `ack_fn` and `nack_fn` are optional. If not provided (fire-and-forget mode), routing behaves as before.
- **D-03:** When an egress publish fails (or times out), if `nack_fn` was supplied by the ingress client, invoke `nack_fn(requeue=True)`.
- **D-04:** For protocols with explicit NACK (AMQP), invoke `channel.basic_nack(delivery_tag, requeue=True)`. For protocols without explicit NACK (MQTT), withhold PUBACK so broker state is retained or retried.
- **D-05:** Automatic translation mappings:
  - MQTT QoS 0 -> AMQP `delivery_mode=1` (transient)
  - MQTT QoS 1/2 -> AMQP `delivery_mode=2` (persistent)
  - AMQP `delivery_mode=2` -> MQTT `qos=1`
  - AMQP `delivery_mode=1` -> MQTT `qos=0`
- **D-06:** Explicit kwargs defined on `add_route` always override automatic defaults.
- **D-07:** "All-Succeed" policy: When a message routes to multiple destinations, track pending confirmations with an atomic countdown latch. Call `ack_fn()` only when all targets report `on_confirm`. If any target reports `on_error`, invoke `nack_fn()`.

### Claude's Discretion
- Implementation specifics of the countdown tracker helper, callback signatures, and test mocks.

### Deferred Ideas (OUT OF SCOPE)
- None.
</user_constraints>

<architectural_responsibility_map>
## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Async Confirmation Seam (`ack_fn`, `nack_fn`) | Base Interface (`BaseClient`, `Router`) | Pipeline Seam | Standardizes ACK propagation across all protocol adapters |
| Multi-Target Fanout Coordinator | Router (`src/postoffice/router.py`) | Core Routing | Coordinates 1-to-N target publish confirmations before ACK |
| QoS / Delivery Mode Auto-Translator | Router (`src/postoffice/router.py`) | Semantic Mapping | Maps QoS 0/1/2 <-> delivery_mode 1/2 transparently |
| Kafka Delivery Confirmation Wiring | Kafka Client (`src/postoffice/plugins/kafka_client.py`) | Producer Adapter | Bridges librdkafka delivery report callback to on_confirm/on_error |
| AMQP Confirm & Ack Wiring | AMQP Client (`src/postoffice/plugins/amqp_client.py`) | AMQP Adapter | Bridges basic_ack/nack and publisher confirm callbacks |
| MQTT QoS 1/2 Confirm Wiring | MQTT Client (`src/postoffice/plugins/mqtt_client.py`) | MQTT Adapter | Bridges publish message_info to on_confirm/on_error |

</architectural_responsibility_map>

<research_summary>
## Summary

Phase 2 establishes end-to-end delivery guarantees between disparate messaging protocols.
When an ingress message arrives with delivery guarantees (e.g. MQTT QoS 1/2 or AMQP persistent message), PostOffice must not acknowledge the ingress broker until all downstream egress brokers have acknowledged receipt.

Key findings:
1. **Thread-Safe Latch Pattern:** For fanout routes, a thread-safe coordinator `_FanoutCoordinator` encapsulates `remaining_count`, `lock`, `ack_fn`, and `nack_fn`. As each target client completes its publish asynchronously, `on_confirm` decrements the count. When count reaches zero, `ack_fn()` fires.
2. **Semantic Translation Rules:** In `Router.route()`, `source_kwargs` provides context (e.g. `qos` or `delivery_mode`). When target kwargs lack explicit `delivery_mode` or `qos`, `Router` computes the translation defaults:
   - `qos in (1, 2)` -> `delivery_mode=2`
   - `qos == 0` -> `delivery_mode=1`
   - `delivery_mode == 2` -> `qos=1`
   - `delivery_mode == 1` -> `qos=0`
3. **Driver Integration:**
   - `KafkaClient.publish`: The `confluent_kafka.Producer.produce` callback already receives `(err, msg)`. Wiring `on_confirm` and `on_error` into `delivery_report` is immediate and native.
   - `AmqpClient.publish`: When `on_confirm` is provided, invoke it on successful publish, and pass `nack_fn` when handling ingress message consumption.
   - `MqttClient.publish`: Paho's `publish()` returns `MQTTMessageInfo`. Checking return code and calling `on_confirm()` completes the chain.

</research_summary>

<verification_strategy>
## Verification Strategy

1. **Unit Tests in `tests/test_postoffice.py` & new tests:**
   - Verify `Router.route()` with single target: `ack_fn` called when `on_confirm` fires.
   - Verify `Router.route()` with multi-target fanout: `ack_fn` called only after ALL targets confirm.
   - Verify `Router.route()` failure propagation: `nack_fn` called if any target reports error.
   - Verify automatic semantic translation: MQTT QoS 1 -> AMQP `delivery_mode=2`, AMQP persistent -> MQTT `qos=1`.
   - Verify explicit kwargs in `add_route` override automatic defaults.
   - Verify Kafka `delivery_report` triggers `on_confirm` / `on_error`.

2. **Zero Regressions:**
   - All 11 existing unit tests continue to pass.

</verification_strategy>

---
*Research completed: 2026-10-06*
