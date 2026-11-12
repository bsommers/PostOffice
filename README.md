<p align="center"><img src="docs/assets/logo.svg" alt="PostOffice" width="520"></p>

# PostOffice

Site: https://bsommers.github.io/PostOffice/

PostOffice is a high-throughput, hybrid Rust/Python multi-protocol messaging system and semantic translation bridge routing data between MQTT, AMQP (RabbitMQ), Apache Kafka, and NanoMQ edge brokers with verified delivery guarantees.

---

## Key Features

- **Hybrid Native Rust + Python Architecture:** High-speed Rust Data Plane core (`libpostoffice` C ABI + `NativeEngine`) achieving 24x–253x micro-benchmark speedups and sub-millisecond p99 bridge latency, paired with an ergonomic Python `PostOffice` facade, Control Plane, and reference conformance oracle (`71/71` vectors in `--both` mode).
- **Multi-Protocol Bridging:** Seamless routing across MQTT, NanoMQ, RabbitMQ (AMQP), and Apache Kafka.
- **$O(k)$ TopicTrie Routing:** Hierarchical topic matching with `+` (single-level) and `#` (multi-level) wildcards and MQTT `$`-prefix isolation (`§5.0`), scaling over 648x faster than linear scans.
- **Ingress Firewall & DoS Protection:** Pre-routing payload size caps, topic shape constraints, token-bucket rate limits, streaming JSON/Avro/Protobuf `ContentGuard`, `SenderLedger` with escalating cool-downs, `InFlightGate` concurrency limits, `RetryLimiter` poison backoff, and cgroup-aware `MemoryGovernor`.
- **End-to-End Delivery Guarantees:** Asynchronous publisher confirmation coordination (`_FanoutCoordinator`), contiguous Kafka offset watermarks, AMQP publisher confirms, MQTT QoS 1 `PUBACK` tracking, and automatic semantic translation.
- **Dynamic Schema Registry & Transformation:** Runtime bidirectional payload conversion between JSON, Apache Avro (`fastavro`), and Protocol Buffers (`protobuf`) with Confluent 5-byte wire format framing and schema complexity caps.
- **Production Security & Hot-Reload:** TLS/mTLS encryption, strict certificate verification, SASL authentication (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), HMAC-SHA256 signed Control Plane events, and zero-downtime in-flight credential rotation (`reload_broker_security`).
- **Operational Resiliency & Edge Store-and-Forward:** Offline subscription replay, per-target bounded publish queues (`max_messages`, `max_bytes`, `max_age_sec`), opt-in edge disk-backed store-and-forward (`DiskStoreAndForward` with wear-aware batching and CRC32 crash recovery), and egress `CircuitBreaker` protection.
- **Mixed-Fleet Canary Rollout & Observability:** Route-level engine tagging (`engine="python" | "native"`), automated `CanaryRolloutController` (`5% -> 25% -> 100%` with SLO auto-rollback), shadow mode differential testing, Prometheus `/metrics`, `/healthz`, `/readyz` probes, structured JSON logs, and budgeted Dead Letter Queues.
- **Distributed Control & Data Plane:** Race-free subscribe-then-snapshot Redis synchronization with Pub/Sub (`postoffice:config_updates`) and durable Redis Streams (`postoffice:config_stream`) replay.

---

## Quickstart

```python
from postoffice.app import PostOffice

# Initialize PostOffice (enable Native Rust Engine with use_native_engine=True)
po = PostOffice(
    metrics_port=9100,
    dlq_broker="kafka_bus",
    dlq_topic="deadletter.events",
    use_native_engine=True,
)

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
    wire_format=True,
)

# Start connections
po.start()
```

---

## Documentation

Comprehensive documentation, tutorials, contracts, and operational runbooks:

- 📖 **[User Guide Entrypoint](docs/user-guide/README.md)** — Installation, quickstart, and full API facade reference.
- 🔀 **[Topic Routing & Wildcards](docs/user-guide/routing-and-wildcards.md)** — MQTT `+` / `#` wildcards, `$`-prefix isolation, and $O(k)$ TopicTrie benchmarks.
- 🛡️ **[Delivery Guarantees & Semantic Translation](docs/user-guide/delivery-guarantees.md)** — QoS 1/2 coordination, contiguous Kafka offsets, and persistent delivery modes.
- 🧱 **[Ingress Firewall, Sender Ledger & In-Flight Limits](docs/user-guide/ingress-firewall.md)** — Pre-routing DoS rules, content guard, sender cool-down, and retry caps.
- 🧬 **[Dynamic Schema Registry & Serialization](docs/user-guide/schema-transformation.md)** — JSON, Avro, Protobuf, and Confluent wire framing.
- 🔒 **[Production Security, TLS/mTLS & Authentication](docs/user-guide/security-and-auth.md)** — TLS/mTLS, SASL, unified `SecurityConfig`, and live credential hot-reload.
- 🛟 **[Operational Resiliency, Queuing & Circuit Breakers](docs/user-guide/resiliency-and-circuit-breakers.md)** — Offline subscription replay, bounded publish queues, edge store-and-forward, and circuit breakers.
- 📊 **[Observability & Dead Letter Queues](docs/user-guide/observability-and-dlq.md)** — Prometheus metrics, `/healthz` & `/readyz` probes, structured JSON logs, and budgeted DLQ quarantine.
- 🌐 **[Distributed Control & Data Plane](docs/user-guide/distributed-plane.md)** — Multi-worker scaling via Redis Pub/Sub & Streams, HMAC event signing, and canary rollouts.
- 📜 **[Polyglot v1 Contracts & Conformance](docs/contracts/README.md)** — Protobuf IDL (`proto/postoffice/v1/postoffice.proto`), OpenAPI 3.1 (`api/openapi.v1.yaml`), C ABI (`include/postoffice.h`), and 71-vector conformance suite.
- 🚀 **[Mixed-Fleet Canary Rollout Runbook](docs/runbooks/mixed-fleet-rollout.md)** — Helm/Compose deployment, canary ramp (`5% -> 25% -> 100%`), and emergency rollback procedures.
- ⚡ **[Benchmarks & Native Bake-Off](docs/BENCHMARKS.md)** — Micro-benchmark comparisons, bridge-added latency histograms, and memory budgets.
- 📐 **[Architecture Decision Records (ADRs)](docs/adr/0001-native-data-plane-language.md)** — [ADR-0001 (Rust Data Plane)](docs/adr/0001-native-data-plane-language.md) and [ADR-0002 (Phase 5 Consolidation)](docs/adr/0002-phase5-consolidation.md).
- 💡 **[Use Cases & Purpose](docs/use-cases/README.md)** — How PostOffice fits in IIoT, IoT/edge, data systems, and enterprise messaging.
- 🔧 **[Troubleshooting & FAQ](docs/user-guide/troubleshooting.md)** — Common error diagnostics, recovery steps, and solutions.
- 🏛️ **[Architecture & Protocol Mapping](docs/ARCHITECTURE.md)** — Deep architectural overview and semantic translation tables.
- 🧪 **[Test Suite & Coverage Report](docs/TEST_COVERAGE.md)** — Module coverage breakdown (`92%` combined branch + statement coverage across `586` Python tests + `7` Rust tests + `71` conformance vectors).

---

## Running Tests, Conformance & Coverage Gates

Execute the hermetic Python unit test suite (`586` tests, no external broker services required):

```bash
PYTHONPATH=src .venv/bin/python -m unittest discover tests/
```

Execute the dual-engine conformance suite (`71/71` vectors verified across both `python` and `native` Rust engines):

```bash
PYTHONPATH=src .venv/bin/python conformance/run.py --both
```

Execute the native Rust unit and fuzz test suite:

```bash
cargo test --release --manifest-path native/libpostoffice/Cargo.toml
```

Run the coverage gate (enforces `>= 90%` combined statement + branch coverage via `.coveragerc`):

```bash
PYTHONPATH=src .venv/bin/coverage run -m unittest discover tests/ && .venv/bin/coverage report
```
