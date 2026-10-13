# PostOffice

PostOffice is a high-throughput, multi-protocol messaging system and semantic translation bridge routing data between MQTT, AMQP (RabbitMQ), Apache Kafka, and NanoMQ edge brokers with verified delivery guarantees.

---

## Key Features

- **Multi-Protocol Bridging:** Seamless routing across MQTT, NanoMQ, RabbitMQ (AMQP), and Apache Kafka.
- **$O(k)$ TopicTrie Routing:** High-performance hierarchical topic matching with `+` (single-level) and `#` (multi-level) wildcards, scaling over 648x faster than linear scans.
- **End-to-End Delivery Guarantees:** Asynchronous publisher confirmation coordination (`_FanoutCoordinator`) and automatic semantic translation (MQTT QoS ↔ AMQP persistent delivery mode).
- **Dynamic Schema Registry & Transformation:** Runtime bidirectional payload conversion between JSON, Apache Avro (`fastavro`), and Protocol Buffers (`protobuf`) with Confluent 5-byte wire format support.
- **Production Security & Authentication:** TLS/mTLS encryption, strict certificate verification, and SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512) across all adapters.
- **Observability & Dead Letter Queues:** Built-in Prometheus metrics exporter (`/metrics` HTTP server) and Dead Letter Queue (DLQ) forensic envelopes with poison-pill isolation and anti-recursion protection.
- **Distributed Architecture:** Redis-synchronized Control Plane and horizontally scalable Data Plane workers.

---

## Quickstart

```python
from postoffice.app import PostOffice

# Initialize PostOffice with Prometheus metrics and Dead Letter Queue
po = PostOffice(metrics_port=9100, dlq_broker="kafka_bus", dlq_topic="deadletter.events")

# Register broker connections
po.add_broker("mqtt_edge", "mqtt", host="localhost", port=1883)
po.add_broker("kafka_bus", "kafka", bootstrap_servers="localhost:9092")

# Add a wildcard route with dynamic schema conversion
po.add_route(
    source_broker="mqtt_edge",
    source_topic="sensors/+/telemetry",
    target_broker="kafka_bus",
    target_topic="analytics.readings",
    input_format="json",
    output_format="avro",
    output_schema="SensorReading",
    wire_format=True
)

# Start connections
po.start()
```

---

## Documentation

Comprehensive documentation, tutorials, recipes, and API references are located in the [User Guide](docs/user-guide/README.md):

- 📖 **[User Guide Entrypoint](docs/user-guide/README.md)** — Installation, quickstart, and full API facade reference.
- 🔀 **[Topic Routing & Wildcards](docs/user-guide/routing-and-wildcards.md)** — MQTT `+` / `#` wildcards and $O(k)$ TopicTrie benchmarks.
- 🛡️ **[Delivery Guarantees & Semantic Translation](docs/user-guide/delivery-guarantees.md)** — QoS 1/2 coordination and persistent delivery modes.
- 🧬 **[Dynamic Schema Registry & Serialization](docs/user-guide/schema-transformation.md)** — JSON, Avro, Protobuf, and Confluent wire framing.
- 🔒 **[Production Security, TLS/mTLS & Authentication](docs/user-guide/security-and-auth.md)** — TLS/mTLS, SASL, and unified `SecurityConfig` across brokers.
- 📊 **[Observability & Dead Letter Queues](docs/user-guide/observability-and-dlq.md)** — Prometheus metrics scraping and DLQ poison-pill quarantine.
- 🌐 **[Distributed Control & Data Plane](docs/user-guide/distributed-plane.md)** — Multi-worker horizontal scaling via Redis Pub/Sub.
- 🔧 **[Troubleshooting & FAQ](docs/user-guide/troubleshooting.md)** — Common error diagnostics, recovery steps, and solutions.
- 🏛️ **[Architecture & Protocol Mapping](docs/ARCHITECTURE.md)** — Deep architectural overview and semantic translation tables.

---

## Running Tests

Execute the hermetic unit test suite (no external broker services required):

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover tests/
```
