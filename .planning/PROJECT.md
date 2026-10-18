# PostOffice

## What This Is

PostOffice is a hardened, multi-protocol messaging system and translation bridge that routes data between MQTT, AMQP (RabbitMQ), Apache Kafka, and NanoMQ edge brokers. It provides a uniform programmatic interface for local applications alongside a distributed architecture with a Redis-backed Control Plane, horizontally scalable Data Plane workers, Prometheus metrics, and Dead Letter Queue poison-pill quarantine.

## Core Value

Seamlessly route and translate messaging payloads and delivery semantics across heterogeneous pub/sub, streaming, and queueing protocols with zero message loss.

## Requirements

### Validated

- [x] Pluggable broker adapter architecture (`BaseClient`, `ClientRegistry`) — v1.0
- [x] Client implementations for MQTT, NanoMQ, AMQP, and Kafka — v1.0
- [x] Topic routing engine with single-level (`+`) and multi-level (`#`) wildcard matching — v1.0
- [x] Uniform programmatic facade (`PostOffice`) in `src/postoffice/app.py` — v1.0
- [x] Distributed Control Plane managing broker, route, and subscription state in Redis — v1.0
- [x] Horizontally scalable Data Plane worker syncing state and listening for real-time config updates — v1.0
- [x] Multi-broker local development orchestration via Docker Compose — v1.0
- [x] **TEST-01, TEST-02, TEST-03**: Automated hermetic tests for `ControlPlane`, `DataPlane`, reconnect resiliency, and `reset()` — v1.1
- [x] **SEM-01, SEM-02**: End-to-end semantic translation and delivery guarantee coordination (`_FanoutCoordinator`, MQTT QoS <-> AMQP delivery mode) — v1.1
- [x] **ROUT-01**: High-throughput $O(k)$ TopicTrie matching engine replacing linear route scans (648.9x faster at 10,000 routes) — v1.1
- [x] **OBS-01**: Observability and metrics instrumentation (`MetricsManager`, Prometheus counters/histogram, HTTP exporter) — v1.1
- [x] **DLQ-01**: Dead Letter Queue (DLQ) support for unroutable or failed messages with JSON envelope and anti-recursion protection — v1.1
- [x] **SCH-01**: Dynamic payload schema conversion and schema registry integration (`BaseSchemaRegistry`, JSON ↔ Avro ↔ Protobuf, Confluent wire format) — v2.0
- [x] **SEC-01**: TLS/mTLS and token authentication enforcement for production broker connections — v2.0
- [x] **RES-01**: Standardized offline subscription queuing and thread-safe in-flight publish buffers across all adapters (`MqttClient`, `NanoMqClient`, `AmqpClient`, `KafkaClient`) — v2.1
- [x] **RES-02**: Egress backpressure, bounded ring-buffer failover, and per-broker circuit breaker protection before DLQ drop — v2.1
- [x] **RES-03**: Dynamic in-flight TLS certificate and credential hot-reload without worker process restart — v2.1

### Future Considerations (v3.0 Candidates)

- **FLT-01**: Dynamic content-based filtering (JSONPath/CEL expressions) in topic routes
- **INT-01**: Live multi-broker integration test suite running against Docker Compose stack
- **ADPT-01**: Additional protocol adapters (HTTP/Webhooks, gRPC, WebSocket)

### Out of Scope

- Monolithic protocol rewrites — keep adapters isolated in `src/postoffice/plugins/`
- Direct broker-to-broker binary tunnels without application-level routing inspection
- Custom message broker storage engine — PostOffice bridges existing brokers, does not store broker state

## Context

- Messaging protocol diversity: IoT devices produce MQTT, enterprise queueing uses RabbitMQ, and analytics backbones consume Kafka.
- Horizontal scaling: Multiple Data Plane workers run against edge brokers using native features (MQTT 5 shared subscriptions, Kafka consumer groups, AMQP shared queues) to prevent message duplication.
- Shipped v2.1 with 94 unit tests passing in 0.55s hermetic execution.

## Constraints

- Runtime: Python 3.12+
- Dependencies: `paho-mqtt`, `pika`, `confluent-kafka`, `redis`, `prometheus-client`, `fastavro`, `protobuf`
- Hermetic testability: Unit tests must execute cleanly without external network broker dependencies via mocking.

---
*Last updated: 2026-10-18 after v2.1 milestone completion*

