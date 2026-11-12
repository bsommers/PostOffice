# Roadmap: PostOffice

## Milestones

- ✅ **v1.1 Hardened Mesh & Delivery Guarantees** — Phases 1-4 (shipped 2026-10-07)
- ✅ **v2.0 Schema Transformation & Production Security** — Phases 5-6 (shipped 2026-10-13)
- ✅ **v2.1 Operational Resiliency & In-Flight Queuing** — Phases 7-8 (shipped 2026-10-18)
- ✅ **v3.0 Native Rust Core, DoS Protection & Mixed-Fleet Rollout** — Native Rewrite Phases 0-5 (shipped 2026-11-05)
- ✅ **v3.1 Route Filtering, Adapters, SDKs & Edge** — Tracks 1-4 (shipped 2026-11-08)

## Phases

<details>
<summary>✅ v1.1 Hardened Mesh & Delivery Guarantees (Phases 1-4) — SHIPPED 2026-10-07</summary>

- [x] **Phase 1: Test Suite & Distributed Plane Hardening** (3/3 plans) — completed 2026-10-05
  - Mock Redis unit tests, DataPlane reconnection resiliency, and clean state reset handler.
- [x] **Phase 2: Semantic Translation & Delivery Guarantees** (2/2 plans) — completed 2026-10-06
  - Bidirectional acknowledgement propagation between MQTT QoS, AMQP confirms, and Kafka offsets.
- [x] **Phase 3: Route Engine Trie Optimization** (1/1 plan) — completed 2026-10-06
  - Radix/Prefix topic trie matching engine eliminating O(N) route scan overhead (648.9x faster).
- [x] **Phase 4: Observability & Dead Letter Queues** (2/2 plans) — completed 2026-10-07
  - Prometheus metrics instrumentation and Dead Letter Queue poison-pill quarantine.

</details>

<details>
<summary>✅ v2.0 Schema Transformation & Production Security (Phases 5-6) — SHIPPED 2026-10-13</summary>

- [x] **Phase 5: Dynamic Schema Registry & Serialization** (2/2 plans) — completed 2026-10-08
  - Integrated dynamic payload schema conversion across JSON, Apache Avro, and Protocol Buffers with DLQ error handling.
- [x] **Phase 6: Production Security, TLS/mTLS & Authentication** (2/2 plans) — completed 2026-10-13
  - Enforce TLS/mTLS certificate verification, SASL authentication, and credential rotation across all client plugins.

</details>

<details>
<summary>✅ v2.1 Operational Resiliency & In-Flight Queuing (Phases 7-8) — SHIPPED 2026-10-18</summary>

- [x] **Phase 7: Operational Resiliency & Client Queuing** (2/2 plans) — completed 2026-10-17
  - Standardized offline subscription queuing and thread-safe bounded publish buffers across `BaseClient`, `MqttClient`, `NanoMqClient`, `AmqpClient`, and `KafkaClient` with automatic reconnect replay (`RES-01`).
- [x] **Phase 8: Egress Backpressure, Circuit Breakers & Credential Hot-Reload** (2/2 plans) — completed 2026-10-18
  - Egress circuit breaker state machines, backpressure coordination before DLQ fallback (`RES-02`), and live TLS certificate/credential reload via Control Plane updates (`RES-03`).

</details>

<details open>
<summary>✅ v3.0 Native Rust Core, DoS Protection & Mixed-Fleet Rollout (Native Phases 0-5) — SHIPPED 2026-11-05</summary>

- [x] **Architecture Remediation & DoS Protection (`P0`-`P3`, `D1`-`D11`)** — completed 2026-10-28
  - Real publisher confirms (`M1`, `A1`), contiguous Kafka offsets (`K1`/`K2`), subscribe-then-snapshot Redis `T7` sync, `IngressFirewall`, `ContentGuard`, `SenderLedger`, `InFlightGate`, `RetryLimiter`, `BudgetedDlq`, and HMAC-SHA256 Control Plane signing.
- [x] **Native Phase 0: Measure, Prove & Bake-Off** (4/4 slices) — completed 2026-10-31
  - Open-loop Rust loadgen/sink (`perf/loadgen/`), bridge-added latency probe, Python baseline matrix, Rust-vs-Go bake-off, `MemoryGovernor`, and ADR-0001 (`docs/BENCHMARKS.md`).
- [x] **Native Phase 1: Freeze Contracts & Conformance Suite** (3/3 slices) — completed 2026-10-31
  - Protobuf v1 IDL (`proto/postoffice/v1/postoffice.proto`), OpenAPI 3.1 (`api/openapi.v1.yaml`), C ABI header (`include/postoffice.h`), and 71-vector conformance suite (`conformance/run.py`).
- [x] **Native Phase 2: Native Core Library & FFI Bridge** (3/3 slices) — completed 2026-11-01
  - Rust `libpostoffice` (`native/libpostoffice/`), Python `ctypes` bridge (`src/postoffice/native.py`), edge profile, and property/fuzz suite.
- [x] **Native Phase 3: Native Data Plane Worker & Shadow Mode** (3/3 slices) — completed 2026-11-02
  - `NativeEngine`, Redis RESP2 `T7` sync, `KafkaOffsetTracker`, `AmqpConfirmTracker`, `MqttPubackTracker`, `/metrics` server, and `ShadowModeRunner` (`src/postoffice/shadow.py`).
- [x] **Native Phase 4: Mixed-Fleet Canary Rollout & Operational Parity** (2/2 slices) — completed 2026-11-03
  - `CanaryRolloutController` (`5% -> 25% -> 100%` with SLO auto-rollback), `/healthz` & `/readyz` probes, structured JSON logs, Helm chart, mixed-fleet Docker Compose, Grafana dashboard, and runbook.
- [x] **Native Phase 5: Consolidate, Native Queues & Edge Store-and-Forward** (2/2 slices) — completed 2026-11-04
  - Native `TargetPublishQueue`, `DiskStoreAndForward` (wear-aware batching + CRC32 crash recovery), Redis Streams (`postoffice:config_stream`) `XADD`/`XRANGE` replay, `PostOffice(use_native_engine=True)` consolidation, and ADR-0002.

</details>

## Progress

| Phase | Milestone | Plans Complete | Status | Completed |
|---|---|---|---|---|
| 1. Test Suite & Distributed Plane Hardening | v1.1 | 3/3 | Complete | 2026-10-05 |
| 2. Semantic Translation & Delivery Guarantees | v1.1 | 2/2 | Complete | 2026-10-06 |
| 3. Route Engine Trie Optimization | v1.1 | 1/1 | Complete | 2026-10-06 |
| 4. Observability & Dead Letter Queues | v1.1 | 2/2 | Complete | 2026-10-07 |
| 5. Schema Registry & Transforms | v2.0 | 2/2 | Complete | 2026-10-08 |
| 6. Production Security & Auth | v2.0 | 2/2 | Complete | 2026-10-13 |
| 7. Operational Resiliency & Client Queuing | v2.1 | 2/2 | Complete | 2026-10-17 |
| 8. Backpressure, Circuit Breakers & Hot-Reload | v2.1 | 2/2 | Complete | 2026-10-18 |
| Remediation D1-D11 + Native Phases 0-5 | v3.0 | 20/20 slices | Complete | 2026-11-05 |
| Tracks 1-4 (SDK, Edge, Soak, FLT-01/ADPT-01) | v3.1 | 4/4 tracks | Complete | 2026-11-08 |
