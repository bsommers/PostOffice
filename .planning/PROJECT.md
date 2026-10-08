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

### Active (v2.0 Roadmap)

- [ ] **SCH-01**: Dynamic payload schema conversion and schema registry integration (JSON to Avro/Protobuf)
- [ ] **SEC-01**: TLS/mTLS and token authentication enforcement for production broker connections

### Out of Scope

- Monolithic protocol rewrites — keep adapters isolated in `src/postoffice/plugins/`
- Direct broker-to-broker binary tunnels without application-level routing inspection
- Custom message broker storage engine — PostOffice bridges existing brokers, does not store broker state

## Context

- Messaging protocol diversity: IoT devices produce MQTT, enterprise queueing uses RabbitMQ, and analytics backbones consume Kafka.
- Horizontal scaling: Multiple Data Plane workers run against edge brokers using native features (MQTT 5 shared subscriptions, Kafka consumer groups, AMQP shared queues) to prevent message duplication.
- Shipped v1.1 with 43 unit tests passing in 0.5s hermetic execution.

## Constraints

- Runtime: Python 3.12+
- Dependencies: `paho-mqtt`, `pika`, `confluent-kafka`, `redis`, `prometheus-client`
- Hermetic testability: Unit tests must execute cleanly without external network broker dependencies via mocking.

---
*Last updated: 2026-10-07 after v1.1 milestone*
