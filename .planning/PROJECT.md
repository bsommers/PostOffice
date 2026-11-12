# PostOffice

## What This Is

PostOffice is a hardened, multi-protocol messaging system and translation bridge that routes data between MQTT, AMQP (RabbitMQ), Apache Kafka, and NanoMQ edge brokers. It provides a uniform programmatic interface for local applications alongside a distributed architecture with a Redis-backed Control Plane, horizontally scalable Data Plane workers, Prometheus metrics, and Dead Letter Queue poison-pill quarantine.

## Core Value

Seamlessly route and translate messaging payloads and delivery semantics across heterogeneous pub/sub, streaming, and queueing protocols with zero message loss.

## Current Milestone: v3.2 Observability Exports (OpenTelemetry)

**Goal:** Make PostOffice observable through OpenTelemetry, with the Python and native engines exporting the same signals.

**Target features:**
- Native metrics parity (`sender_events_total`, `routing_duration_seconds`, `inflight_messages`) and native metrics server wiring in `PostOffice`
- OTel metrics/traces/logs export (OTLP) from both engines, with trace-context propagation across protocol hops
- ControlPlane filter validation at admin time plus filter docs/examples
- Placeholder: integration with recognizer-telemetry (github.com/bilism1010/recognizer-telemetry), scope TBD when the repo is supplied

**Parked (future):** Raft / HA control plane.

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
- [x] **DOS-01 (`D1`–`D11`)**: Pre-routing `IngressFirewall`, `ContentGuard`, `SenderLedger`, `InFlightGate`, `RetryLimiter`, `BudgetedDlq`, `MemoryGovernor`, and HMAC-SHA256 Control Plane signing — v3.0
- [x] **NAT-01 (Phases 0–5)**: Hybrid Rust `libpostoffice` C ABI & `NativeEngine` (`native/libpostoffice/`), 71-vector polyglot conformance suite (`conformance/run.py --both`), shadow mode (`ShadowModeRunner`), mixed-fleet canary rollout (`CanaryRolloutController`), native bounded queues (`TargetPublishQueue`), opt-in edge disk store-and-forward (`DiskStoreAndForward`), and Redis Streams (`postoffice:config_stream`) replay — v3.0
- [x] **SDK-01**: Polyglot v1 Client SDK scaffolding (`sdks/c`, `sdks/go`, `sdks/rust`, `sdks/ts`) and `docs/contracts/SDK_GUIDE.md` — Track 1
- [x] **EDGE-01**: Edge binary packaging (`Dockerfile.native`), stripped `postoffice-worker` (`< 1 MB`), and RSS/throughput verification (`scripts/verify_edge_profile.py`) — Track 2
- [x] **INT-01**: Live multi-broker & Redis integration soak harness (`scripts/run_live_soak.py`, `tests/integration/test_live_broker_soak.py`) — Track 3
- [x] **FLT-01**: Dynamic content-based route filtering (`src/postoffice/filtering.py` & `native/libpostoffice/src/engine.rs` JSONPath + boolean combinators with 100% `ShadowHarness` parity) — Track 4 (v3.1)
- [x] **ADPT-01**: Additional protocol adapters (`HttpClient` / `WebhookClient` in `src/postoffice/plugins/http_client.py` and RFC 6455 `WebSocketClient` / `WebSocketServer` in `src/postoffice/plugins/websocket_client.py`) — Track 4 (v3.1)

### Active

- [ ] Native metrics parity and native metrics server wiring (OTEL milestone phase 9)
- [ ] OpenTelemetry export of metrics, traces, logs (Python + native)
- [ ] ControlPlane route-filter validation and filter docs
- [ ] recognizer-telemetry integration (TBD)

### Out of Scope

- Monolithic protocol rewrites — keep adapters isolated in `src/postoffice/plugins/`
- Direct broker-to-broker binary tunnels without application-level routing inspection
- Raft / HA control plane — deferred to a future milestone (backlog)
- Custom message broker storage engine — PostOffice bridges existing brokers, does not store broker state

## Context

- Messaging protocol diversity: IoT devices produce MQTT, enterprise queueing uses RabbitMQ, and analytics backbones consume Kafka.
- Horizontal scaling: Multiple Data Plane workers run against edge brokers using native features (MQTT 5 shared subscriptions, Kafka consumer groups, AMQP shared queues) to prevent message duplication.
- Shipped v3.1 with 616 Python tests, 8 Rust tests, and 71/71 dual-engine conformance vectors.
- Future (parked): Raft / HA control plane. Planned v3.2: OpenTelemetry exports, intersecting with the recognizer-telemetry project.

## Constraints

- Runtime: Python 3.12+
- Dependencies: `paho-mqtt`, `pika`, `confluent-kafka`, `redis`, `prometheus-client`, `fastavro`, `protobuf`
- Hermetic testability: Unit tests must execute cleanly without external network broker dependencies via mocking.

---
*Last updated: 2026-11-11 after starting v3.2 milestone*

