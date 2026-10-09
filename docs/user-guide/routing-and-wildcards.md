# Topic Routing & Wildcards

[← Back to User Guide](README.md) | [← Back to Project README](../../README.md)

PostOffice features an $O(k)$ hierarchical Radix/Prefix topic trie (`TopicTrie`) capable of matching topic paths in microseconds regardless of whether you have 10 or 10,000 active routing rules.

---

## Table of Contents
- [MQTT Wildcard Semantics](#mqtt-wildcard-semantics)
  - [Single-Level Wildcard (`+`)](#single-level-wildcard-)
  - [Multi-Level Wildcard (`#`)](#multi-level-wildcard-)
- [Configuring Wildcard Routes](#configuring-wildcard-routes)
- [Multi-Target Fanout](#multi-target-fanout)
- [Performance & Benchmark Validation](#performance--benchmark-validation)

---

## MQTT Wildcard Semantics

PostOffice uses standard MQTT topic delimiter syntax (`/`) and wildcard tokens across all messaging protocols.

### Single-Level Wildcard (`+`)
Matches exactly one topic segment between slashes.

- **Route Pattern:** `sensors/+/temperature`
- **Matches:**
  - `sensors/room1/temperature`
  - `sensors/room2/temperature`
  - `sensors/chiller/temperature`
- **Does Not Match:**
  - `sensors/temperature` (missing segment)
  - `sensors/buildingA/room1/temperature` (two segments)

### Multi-Level Wildcard (`#`)
Matches any number of topic levels. When present, `#` must appear as the final segment of the route pattern.

- **Route Pattern:** `facilities/campus1/#`
- **Matches:**
  - `facilities/campus1/bldgA/floor2/temp`
  - `facilities/campus1/power/meter1`
  - `facilities/campus1` (trailing match)
- **Does Not Match:**
  - `facilities/campus2/power`

---

## Configuring Wildcard Routes

```python
from postoffice.app import PostOffice

po = PostOffice()

# Register MQTT edge and RabbitMQ enterprise brokers
po.add_broker("mqtt_edge", "mqtt", host="localhost", port=1883)
po.add_broker("rabbit_mq", "amqp", host="localhost", port=5672)

# Route all temperature sensors across any zone to a common AMQP exchange
po.add_route(
    source_broker="mqtt_edge",
    source_topic="zones/+/sensors/temperature",
    target_broker="rabbit_mq",
    target_topic="telemetry.temperatures",
    exchange="amq.topic"
)

# Route all critical alarm topics across the entire namespace
po.add_route(
    source_broker="mqtt_edge",
    source_topic="alarms/#",
    target_broker="rabbit_mq",
    target_topic="emergency.alarms",
    exchange="alarms.direct"
)

po.start()
```

---

## Multi-Target Fanout

You can register multiple routes for the same source topic. When a message arrives matching multiple routes, PostOffice fans out the message to all registered targets concurrently:

```python
# Fanout: Single MQTT ingress -> Kafka analytics AND RabbitMQ processing
po.add_route("mqtt_edge", "orders/new", "kafka_cluster", "orders.stream", key=b"order_event")
po.add_route("mqtt_edge", "orders/new", "rabbit_mq", "orders.fulfillment", delivery_mode=2)
```

Both downstream deliveries are executed, and if delivery confirmations are requested, upstream acknowledgement occurs only after all targets confirm reception.

---

## Performance & Benchmark Validation

Prior to Phase 3, PostOffice used a linear $O(N)$ route scan. With the `TopicTrie` engine, routing time scales strictly with the topic depth $k$ ($O(k)$) rather than the total number of registered routes $N$.

Benchmark results from 5,000 lookups per scale:

| Registered Routes ($N$) | Linear Scan Time | TopicTrie Time | Speedup Factor | TopicTrie Throughput |
|---|---|---|---|---|
| **10** | 13.96 ms | 4.25 ms | **3.3x** | 1,175,090 ops/sec |
| **100** | 136.50 ms | 3.92 ms | **34.9x** | 1,277,067 ops/sec |
| **1,000** | 1,357.02 ms | 4.84 ms | **280.2x** | 1,032,462 ops/sec |
| **10,000** | 17,386.84 ms | 26.80 ms | **648.9x** | 186,601 ops/sec |

To run the trie benchmark locally:
```bash
PYTHONPATH=src .venv/bin/python tests/benchmark_trie.py
```
