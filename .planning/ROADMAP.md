# Roadmap: PostOffice

## Milestones

- ✅ **v1.1 Hardened Mesh & Delivery Guarantees** — Phases 1-4 (shipped 2026-10-07)
- ✅ **v2.0 Schema Transformation & Production Security** — Phases 5-6 (shipped 2026-10-13)
- ✅ **v2.1 Operational Resiliency & In-Flight Queuing** — Phases 7-8 (shipped 2026-10-18)
- ✅ **v3.0 Native Rust Core, DoS Protection & Mixed-Fleet Rollout** — Native Rewrite Phases 0-5 (shipped 2026-11-05)
- ✅ **v3.1 Route Filtering, Adapters, SDKs & Edge** — Tracks 1-4 (shipped 2026-11-08)
- 🚧 **v3.2 Observability Exports (OpenTelemetry)** — Phases 9-14 (in progress)

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

### 🚧 v3.2 Observability Exports (OpenTelemetry) (In Progress)

**Milestone Goal:** Make PostOffice observable through OpenTelemetry, with the Python and native engines exporting the same signals (metrics, traces, logs) over OTLP, off by default and never in the routing hot path.

- [ ] **Phase 9: Native Metrics Parity** - Native `/metrics` matches the Python metric set and `PostOffice` starts the native metrics server
- [ ] **Phase 10: Python OTLP Metrics Export & Safety Gates** - Opt-in OTLP/HTTP metrics from the Python engine with bounded, non-blocking export
- [ ] **Phase 11: Native OTLP Metrics Export** - Native engine exports the same metrics over OTLP/HTTP with no new Rust dependencies
- [ ] **Phase 12: Distributed Tracing & Context Propagation** - Per-message spans in both engines with W3C `traceparent` carried across every protocol hop
- [ ] **Phase 13: OTLP Logs & DLQ Event Export** - Structured logs and DLQ events exported as OTLP logs correlated with trace IDs
- [ ] **Phase 14: Control Plane Runtime Config & Route-Filter Validation** - Enable/configure OTel at runtime via signed ControlPlane events; reject invalid route filters at admin time

**Deferred by design (not mapped to any phase):** `RTEL-01` — recognizer-telemetry integration contract. Scope is TBD until the repo is supplied; tracked as a future requirement in `REQUIREMENTS.md`.

#### Phase 9: Native Metrics Parity
**Goal**: Native-engine deployments expose the same Prometheus metric series as the Python engine, served by a metrics server that `PostOffice` starts itself.
**Depends on**: Nothing in this milestone (builds on shipped v3.1)
**Requirements**: NMET-01, NMET-02, NMET-03, NMET-04
**Success Criteria** (what must be TRUE):
  1. After routing traffic through a native-engine worker, scraping its `/metrics` shows `postoffice_sender_events_total` with the same label names as the Python engine produces for the same traffic.
  2. The same native scrape shows `postoffice_routing_duration_seconds` as a histogram (bucket, sum, and count series) and `postoffice_inflight_messages` as a gauge that rises while messages are in flight and returns to its idle value once drained.
  3. Constructing `PostOffice(use_native_engine=True)` brings up the native metrics/health server with no extra wiring, and `set_metrics_port` moves that server to the requested port.
  4. A conformance/shadow check fails whenever Python and native differ in exposed metric names or label sets, and passes on the shipped build.
**Plans**: TBD

#### Phase 10: Python OTLP Metrics Export & Safety Gates
**Goal**: Operators can opt in to OTLP/HTTP metrics export from the Python engine using standard `OTEL_*` settings, with a guarantee that the exporter can never stall or drop routing traffic. The bounded, drop-oldest export pipeline built here is the shared safety gate reused by every later signal.
**Depends on**: Phase 9 (defines the canonical metric/label set both engines export)
**Requirements**: OTEL-01, OTEL-06
**Success Criteria** (what must be TRUE):
  1. With no `OTEL_*` configuration, nothing is exported and no exporter thread or connection is started; Prometheus `/metrics` behaves exactly as before.
  2. Setting standard `OTEL_*` environment variables (endpoint, service name, resource attributes, export interval) makes the existing PostOffice metrics arrive at an OTLP/HTTP collector with the same names and labels as the Prometheus output.
  3. With the collector down, unreachable, or slow, routing throughput and upstream acknowledgements are unaffected, and the export buffer stays bounded, discarding the oldest data first.
  4. When the collector comes back, export resumes without restarting the worker.
**Plans**: TBD

#### Phase 11: Native OTLP Metrics Export
**Goal**: Native-engine workers export the same metrics over OTLP/HTTP as the Python engine, without growing the edge footprint past its budget.
**Depends on**: Phase 9 (native metric set), Phase 10 (`OTEL_*` configuration conventions and safety gates)
**Requirements**: OTEL-02
**Success Criteria** (what must be TRUE):
  1. A native worker pointed at an OTLP/HTTP collector via the same `OTEL_*` settings delivers the same metric names and label sets that the Python engine delivers for equivalent traffic.
  2. `native/libpostoffice` builds with no new third-party Rust dependencies, and `scripts/verify_edge_profile.py` still passes the edge size and memory budget.
  3. Native export is off by default; with the collector down, native routing and acknowledgements are unaffected and the native export buffer stays bounded, drop-oldest (same gate as Phase 10).
**Plans**: TBD

#### Phase 12: Distributed Tracing & Context Propagation
**Goal**: A single message's journey through the mesh, across engines and protocol hops, appears as one connected trace with no payload content leaked.
**Depends on**: Phase 10, Phase 11 (export pipelines in both engines)
**Requirements**: OTEL-03, OTEL-04
**Success Criteria** (what must be TRUE):
  1. Each routed message produces ingress, route, and egress spans in both the Python and native engines, carrying route and broker attributes and no payload content.
  2. A message arriving with a W3C `traceparent` in MQTT 5 user properties, AMQP headers, Kafka headers, or HTTP/WebSocket headers continues that same trace, and egress injects `traceparent` into the target protocol's equivalent carrier.
  3. A message relayed across multiple hops (for example MQTT to Kafka to AMQP) shows up in the collector as one trace ID with correct parent/child links.
  4. Messages with no `traceparent`, a malformed one, or an ingress protocol with no header carrier start a new trace and are still routed and acknowledged normally.
  5. Tracing is off by default, and a collector outage never blocks or drops routing (same gate as Phase 10).
**Plans**: TBD

#### Phase 13: OTLP Logs & DLQ Event Export
**Goal**: Operators can find structured logs and DLQ events in their OTel backend, joined to the originating message's trace by trace ID.
**Depends on**: Phase 12 (trace and span IDs to correlate against)
**Requirements**: OTEL-05
**Success Criteria** (what must be TRUE):
  1. With log export enabled, structured log records from the Python and native engines arrive at the collector as OTLP logs.
  2. A log record emitted while a message is being routed carries that message's trace ID and span ID, so an operator can jump from a trace to its logs.
  3. A message sent to the DLQ produces an OTLP log event identifying the failure reason, route, and broker, correlated to the message's trace.
  4. Log export is off by default, and a collector outage never blocks or drops routing (same gate as Phase 10).
**Plans**: TBD

#### Phase 14: Control Plane Runtime Config & Route-Filter Validation
**Goal**: Operators can turn on and tune OTel export across the fleet from the ControlPlane without restarts, and get immediate feedback at admin time when a route filter is invalid.
**Depends on**: Phase 13 (all export signals exist to be configured at runtime)
**Requirements**: OTEL-07, CPL-01, CPL-02
**Success Criteria** (what must be TRUE):
  1. An operator enables and configures OTel export (endpoint and signals) through the ControlPlane, and running Python and native workers begin exporting without a restart.
  2. Changing or disabling OTel export through the ControlPlane takes effect on running workers; an unsigned or tampered config event is rejected and leaves the current configuration unchanged.
  3. `ControlPlane.add_route` with an invalid `filter` spec raises `ValueError` at the call and publishes nothing to Redis, while a valid filter is accepted.
  4. `scripts/admin.py` and the user guide each contain a working route-filter example an operator can copy.
**Plans**: TBD

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
| 9. Native Metrics Parity | v3.2 | 0/TBD | Not started | - |
| 10. Python OTLP Metrics Export & Safety Gates | v3.2 | 0/TBD | Not started | - |
| 11. Native OTLP Metrics Export | v3.2 | 0/TBD | Not started | - |
| 12. Distributed Tracing & Context Propagation | v3.2 | 0/TBD | Not started | - |
| 13. OTLP Logs & DLQ Event Export | v3.2 | 0/TBD | Not started | - |
| 14. Control Plane Runtime Config & Route-Filter Validation | v3.2 | 0/TBD | Not started | - |

## Backlog

### Phase 999.1: Raft / HA control plane (future)
**Goal**: Captured for future planning — highly available, consensus-backed (Raft) control plane replacing the single Redis-backed coordinator.
**Requirements**: TBD
**Plans**: 0 plans

Plans:
- [ ] TBD (promote with `/gsd-review-backlog` when ready)
