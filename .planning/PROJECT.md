# PostOffice

## What This Is

PostOffice is a multi-protocol messaging system and translation bridge that routes data between MQTT, AMQP (RabbitMQ), Apache Kafka, and NanoMQ edge brokers. It provides a uniform programmatic interface for local applications alongside a distributed architecture with a Redis-backed Control Plane and horizontally scalable Data Plane workers.

## Core Value

Seamlessly route and translate messaging payloads and delivery semantics across heterogeneous pub/sub, streaming, and queueing protocols with zero message loss.

## Requirements

### Validated

- [x] Pluggable broker adapter architecture (`BaseClient`, `ClientRegistry`)
- [x] Client implementations for MQTT (`MqttClient`), NanoMQ (`NanoMqClient`), AMQP (`AmqpClient`), and Kafka (`KafkaClient`)
- [x] Topic routing engine with single-level (`+`) and multi-level (`#`) wildcard matching
- [x] Uniform programmatic facade (`PostOffice`) in `src/postoffice/app.py`
- [x] Distributed Control Plane managing broker, route, and subscription state in Redis
- [x] Horizontally scalable Data Plane worker syncing state and listening for real-time config updates
- [x] Multi-broker local development orchestration via Docker Compose

### Active

- [ ] **SEM-01**: End-to-end semantic translation and delivery guarantee coordination (acknowledgement propagation between MQTT QoS, AMQP publisher confirms, and Kafka offset commits)
- [ ] **ROUT-01**: High-throughput Radix/Trie topic matching engine replacing linear route scans
- [ ] **TEST-01**: Comprehensive automated tests for `ControlPlane`, `DataPlane`, and dynamic configuration synchronization using mock/fake Redis
- [ ] **OBS-01**: Observability and metrics instrumentation (Prometheus metrics and OpenTelemetry tracing)
- [ ] **DLQ-01**: Dead Letter Queue (DLQ) support for unroutable or failed messages

### Out of Scope

- Monolithic protocol rewrites — keep adapters isolated in `src/postoffice/plugins/`
- Direct broker-to-broker binary tunnels without application-level routing inspection

## Context

- Messaging protocol diversity: IoT devices produce MQTT, enterprise queueing uses RabbitMQ, and analytics backbones consume Kafka.
- Horizontal scaling: Multiple Data Plane workers run against edge brokers using native features (MQTT 5 shared subscriptions, Kafka consumer groups, AMQP shared queues) to prevent message duplication.

## Constraints

- Runtime: Python 3.12+
- Dependencies: `paho-mqtt`, `pika`, `confluent-kafka`, `redis`
- Hermetic testability: Unit tests must execute cleanly without external network broker dependencies via mocking.

---
*Last updated: 2026-10-05*
