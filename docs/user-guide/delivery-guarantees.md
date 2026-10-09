# Delivery Guarantees & Semantic Translation

[← Back to User Guide](README.md) | [← Back to Project README](../../README.md)

Bridging messaging systems requires translating delivery contracts across protocols. For example, MQTT defines Quality of Service levels (QoS 0, 1, 2), AMQP uses transient or persistent `delivery_mode` (1 or 2) with publisher confirms, and Apache Kafka relies on producer acknowledgment policies (`acks`) and consumer offset commits.

PostOffice guarantees **at-least-once delivery with zero message loss** through asynchronous confirmation coordination and automatic semantic translation.

---

## Table of Contents
- [End-to-End Acknowledgment Coordination](#end-to-end-acknowledgment-coordination)
  - [The Fanout Coordinator Latch](#the-fanout-coordinator-latch)
- [Automatic Semantic Mapping](#automatic-semantic-mapping)
  - [MQTT to AMQP](#mqtt-to-amqp)
  - [AMQP to MQTT](#amqp-to-mqtt)
- [Explicit Overrides](#explicit-overrides)
- [Example: Guaranteed MQTT-to-AMQP Bridge](#example-guaranteed-mqtt-to-amqp-bridge)

---

## End-to-End Acknowledgment Coordination

When an ingress protocol receives a message requiring delivery confirmation (such as an MQTT QoS 1 publish), acknowledging the ingress broker prematurely risks data loss if the downstream broker is unreachable or crashes.

PostOffice solves this with an asynchronous confirmation seam:

```
[ MQTT Device ] ---> (QoS 1 PUBLISH) ---> [ PostOffice Router ]
                                                 |
                                     (Translate & Publish)
                                                 v
                                        [ AMQP Broker / Kafka ]
                                                 |
                                     (Publisher Confirm / ACK)
                                                 v
[ MQTT Device ] <--- (PUBACK Acknowledge) <------+
```

1. Ingress client supplies `ack_fn` and `nack_fn` callbacks when invoking `Router.route()`.
2. Egress publish attaches `on_confirm` and `on_error` completion handlers.
3. The upstream ingress broker only receives `ack_fn()` (e.g. MQTT `PUBACK` or AMQP `basic_ack`) after all downstream brokers successfully confirm reception.
4. If downstream delivery fails, `_route_to_dlq` captures the payload and issues `ack_fn()` (quarantining the message), or `nack_fn(requeue=True)` is issued if no DLQ is configured.

### The Fanout Coordinator Latch

When one ingress message routes to $M$ downstream targets (fanout), PostOffice uses an atomic latch (`_FanoutCoordinator`). The latch tracks remaining target confirms thread-safely:
- `ack_fn()` is fired **only once**, when all $M$ targets have called `on_confirm`.
- `on_error()` halts confirmation and triggers Dead Letter Queue routing or upstream NACK immediately upon the first failure.

---

## Automatic Semantic Mapping

PostOffice automatically translates persistence semantics between protocols when explicit target parameters are omitted:

### MQTT to AMQP
- **MQTT QoS 1 or 2** $\rightarrow$ AMQP `delivery_mode=2` (Persistent, written to disk)
- **MQTT QoS 0** $\rightarrow$ AMQP `delivery_mode=1` (Transient, memory-only)

### AMQP to MQTT
- **AMQP `delivery_mode=2`** $\rightarrow$ MQTT `qos=1` (At-least-once delivery)
- **AMQP `delivery_mode=1`** $\rightarrow$ MQTT `qos=0` (At-most-once delivery)

---

## Explicit Overrides

Automatic semantic translation provides safe defaults, but explicit parameters in `add_route(...)` always take precedence:

```python
# Force QoS 0 on target despite source being QoS 2
po.add_route(
    source_broker="mqtt_edge",
    source_topic="telemetry/streaming",
    target_broker="rabbit_mq",
    target_topic="telemetry.ephemeral",
    delivery_mode=1  # Explicit override to transient
)
```

---

## Example: Guaranteed MQTT-to-AMQP Bridge

```python
from postoffice.app import PostOffice

po = PostOffice()

po.add_broker("mqtt_edge", "mqtt", host="localhost", port=1883)
po.add_broker("rabbit_mq", "amqp", host="localhost", port=5672)

# End-to-end guaranteed route:
# Incoming QoS 1 messages automatically generate persistent AMQP messages.
# Upstream MQTT client will only receive PUBACK after RabbitMQ confirms basic.ack.
po.add_route(
    source_broker="mqtt_edge",
    source_topic="orders/credit_card",
    target_broker="rabbit_mq",
    target_topic="orders.billing",
    exchange="billing_exchange"
)

po.subscribe("mqtt_edge", "orders/credit_card", qos=1)
po.start()
```
