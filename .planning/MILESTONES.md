# Milestones: PostOffice

This document tracks all completed and active milestone releases of PostOffice.

---

## [v3.1] Route Filtering, HTTP/WebSocket Adapters, SDKs & Edge Profile

**Completed:** 2026-11-08 (tag `v3.1.0`)
**Tracks:** SDK-01, EDGE-01, INT-01, FLT-01, ADPT-01 (executed as quick slices)
**Tests:** 616 Python, 8 Rust, 71/71 dual-engine conformance vectors
**Audit Report:** `.planning/milestones/v3.1-MILESTONE-AUDIT.md` (tech_debt, no blockers)

### Delivered
Content-based route filtering with Python/Rust parity, HTTP/Webhook and WebSocket adapters, polyglot C/Go/Rust/TS SDKs, edge binary profile, and a live Redis/broker soak harness.

### Known Gaps / Carried Forward
- Native `/metrics` lacks `sender_events_total`, `routing_duration_seconds`, `inflight_messages`; native metrics server not started by `PostOffice` (-> v3.2 OTel).
- Native crate has no HTTP/WebSocket delivery; `ControlPlane.add_route` does not validate filters.
- Raft / HA control plane deferred to a future milestone.

---

## [v3.0] Native Rust Core, DoS Protection & Mixed-Fleet Rollout

**Completed:** 2026-11-05 (tag `v3.0.0`)
**Scope:** Architecture remediation & DoS protection (D1-D11) plus Native Phases 0-5

### Delivered
Rust `libpostoffice` with C ABI and `NativeEngine`, 71-vector conformance suite, shadow mode, canary rollout, native queues, edge store-and-forward, Redis Streams replay, and ingress firewall/content guard/sender ledger protections.

---

## [v2.1] Operational Resiliency & In-Flight Queuing

**Completed:** 2026-10-18
**Phases:** 2 (Phases 7–8)
**Plans:** 4 total (07-01, 07-02, 08-01, 08-02)
**Unit Tests:** 94 tests passing hermetically in 0.545s
**Audit Report:** `.planning/v2.1-MILESTONE-AUDIT.md` (Passed)

### Delivered
Production resiliency layer providing standardized offline subscription queuing, thread-safe bounded publish buffers, per-broker egress circuit breakers with ring-buffer failover before DLQ drop, and zero-downtime TLS/SASL credential hot-reloading across all protocol adapters and distributed DataPlane workers.

### Key Accomplishments
1. **Standardized Client Queuing & Replay (`RES-01`):** Thread-safe `_subscriptions` registry and bounded `deque` `_publish_buffer` (`reject` and `drop_oldest` policies) across `BaseClient`, `MqttClient`, `NanoMqClient`, `AmqpClient`, and `KafkaClient`.
2. **Egress Circuit Breakers & Ring-Buffer Failover (`RES-02`):** Per-broker `CircuitBreaker` (`CLOSED -> OPEN -> HALF_OPEN -> CLOSED`) absorbing messages into the target's publish buffer while `OPEN` and automatically flushing on `HALF_OPEN -> CLOSED` probe recovery.
3. **In-Flight TLS & Credential Hot-Reload (`RES-03`):** `reload_security()` across all 4 broker adapters and `ControlPlane.update_broker_security()` / `DataPlane` live Pub/Sub broadcast rotating TLS certificates and SASL/broker credentials without process restart.

---

## [v2.0] Schema Transformation & Production Security

**Shipped:** 2026-10-13
**Phases:** 2 (Phases 5–6)
**Plans:** 4 total (05-01, 05-02, 06-01, 06-02)
**Unit Tests:** 76 tests passing hermetically in 0.538s
**Audit Report:** `.planning/v2.0-MILESTONE-AUDIT.md` (Passed)
**Changelog:** `CHANGELOG.md` ([2.0.0])

### Delivered
Enterprise-grade schema management and production security layer enabling runtime schema translation (JSON ↔ Avro ↔ Protobuf) with Confluent wire format framing and unified TLS/mTLS and SASL security across MQTT, AMQP, Kafka, and NanoMQ edge brokers.

### Key Accomplishments
1. **Dynamic Schema Registry & Wire Framing (`SCH-01`):** In-Memory and Redis schema backends, bidirectional transformations, Confluent 5-byte framing, and DLQ error routing.
2. **Production Security & mTLS (`SEC-01`):** Unified `SecurityConfig` dataclass, mutual TLS certificates, Kafka SASL SCRAM/PLAIN, automatic port shifting, and Redis cluster state synchronization.
3. **Comprehensive User Documentation:** 7 exhaustive user guide manuals in `docs/user-guide/` covering all architectural capabilities.
4. **Hermetic Test Suite:** 76 offline unit tests across 11 test modules with 100% pass rate.

### Git Information
- **Tag:** `v2.0.0`
- **Verification:** All 10 requirements across v1, v1.1, and v2 verified in unit test suite.

---

## [v1.1] Hardened Mesh & Delivery Guarantees

**Shipped:** 2026-10-07
**Phases:** 4 (Phases 1–4)
**Plans:** 8 total
**Unit Tests:** 43 tests passing in ~0.5s
**Audit Report:** `.planning/v1.1-MILESTONE-AUDIT.md` (Passed)
**Changelog:** `CHANGELOG.md` ([1.1.0])

### Delivered
A hardened, high-throughput, multi-protocol routing mesh connecting MQTT, AMQP, Kafka, and NanoMQ edge brokers with verifiable end-to-end delivery guarantees, $O(k)$ hierarchical Radix topic matching, Prometheus metrics, and Dead Letter Queue poison-pill isolation.

### Key Accomplishments
1. **End-to-End Delivery Coordination (`SEM-01`, `SEM-02`):** Asynchronous acknowledgement seam with atomic fanout coordinator (`_FanoutCoordinator`) and automatic QoS-to-delivery_mode mapping.
2. **Hierarchical TopicTrie Matching (`ROUT-01`):** Replaced linear topic scanning with an $O(k)$ Radix tree, achieving 648.9x lookup speedup at 10,000 routes.
3. **Hermetic Distributed Testing (`TEST-01`, `TEST-02`, `TEST-03`):** Resilient DataPlane reconnection loops, clean `reset()` state management, and 100% mocked offline unit testing.
4. **Prometheus Observability (`OBS-01`):** Exporter for message routing throughput, routing duration histograms, and error counters with built-in HTTP server.
5. **Dead Letter Queue Routing (`DLQ-01`):** JSON DLQ envelope serialization, base64 binary fallback, per-route overrides, and anti-recursion protection.

### Git Information
- **Tag:** `v1.1.0`
- **Verification:** All 8 requirements verified in unit test suite.

---

## [v1.0] Initial Scaffold Baseline

**Shipped:** 2026-10-05
**Phases:** Foundation Baseline
**Delivered:** Uniform facade (`PostOffice`), protocol client plugins (`MqttClient`, `AmqpClient`, `KafkaClient`, `NanoMqClient`), basic wildcard routing (`+`, `#`), and Redis-backed Control Plane / Data Plane synchronization.
