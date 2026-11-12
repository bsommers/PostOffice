---
gsd_state_version: "1.0"
milestone: v3.2
milestone_name: Observability Exports (OpenTelemetry)
status: ready_to_plan
last_updated: "2026-11-12T03:30:00.000Z"
last_activity: 2026-11-11
progress:
  total_phases: 6
  completed_phases: 0
  total_plans: 0
  completed_plans: 0
  percent: 0
---

# Project State

## Project Reference

See: `.planning/PROJECT.md` (updated 2026-11-11)

**Core value:** Seamlessly route and translate messaging payloads and delivery semantics across heterogeneous pub/sub, streaming, and queueing protocols with zero message loss.
**Current focus:** Milestone v3.2 Observability Exports (OpenTelemetry) — Phase 9 (Native Metrics Parity) is next. Baseline: v3.1 shipped with `616` Python unit tests, `8` Rust tests, and `71/71` dual-engine conformance vectors passing.

## Current Position

Phase: 9 of 14 (Native Metrics Parity)
Plan: —
Status: Ready to plan
Last activity: 2026-11-11 — v3.2 roadmap created (Phases 9-14, 13/13 mapped requirements covered)

Progress: [░░░░░░░░░░] 0% (0/6 v3.2 phases)

## v3.2 Phase Map

| Phase | Name | Requirements |
|-------|------|--------------|
| 9 | Native Metrics Parity | NMET-01, NMET-02, NMET-03, NMET-04 |
| 10 | Python OTLP Metrics Export & Safety Gates | OTEL-01, OTEL-06 |
| 11 | Native OTLP Metrics Export | OTEL-02 |
| 12 | Distributed Tracing & Context Propagation | OTEL-03, OTEL-04 |
| 13 | OTLP Logs & DLQ Event Export | OTEL-05 |
| 14 | Control Plane Runtime Config & Route-Filter Validation | OTEL-07, CPL-01, CPL-02 |

Deferred by design (unmapped): `RTEL-01` (recognizer-telemetry integration, scope TBD). Backlog: `999.1` Raft / HA control plane (future).

## Performance Metrics

**Velocity:**
- Total plans completed: 16
- Average duration: 13.5 min
- Total execution time: 3.66 hours

**By Phase:**

| Phase | Plans | Total | Avg/Plan |
|-------|-------|-------|----------|
| Phase 1: Test Suite & Distributed Plane Hardening | 3/3 | 0.75h | 15m |
| Phase 2: Semantic Translation & Delivery Guarantees | 2/2 | 0.5h | 15m |
| Phase 3: Route Engine Trie Optimization | 1/1 | 0.25h | 15m |
| Phase 4: Observability & Dead Letter Queues | 2/2 | 0.5h | 15m |
| Phase 5: Dynamic Schema Registry & Serialization | 2/2 | 0.5h | 15m |
| Phase 6: Production Security, TLS/mTLS & Auth | 2/2 | 0.5h | 15m |
| Phase 7: Operational Resiliency & Client Queuing | 2/2 | 0.33h | 10m |
| Phase 8: Backpressure, Circuit Breakers & Hot-Reload | 2/2 | 0.33h | 10m |

**Recent Trend:**
- Trend: v3.0 and v3.1 shipped (tags `v3.0.0`, `v3.1.0`); v3.2 roadmap defined, no v3.2 plans executed yet.

## Accumulated Context

### Decisions

- **Architecture:** Maintain strict three-tier indirection (`PostOffice` facade -> `Router` / `ClientRegistry` -> `BaseClient` plugins).
- **Client Queuing (`RES-01`):** `BaseClient` provides thread-safe `_subscriptions` registry (preserving kwargs like `qos`, `exchange`, `queue` across reconnects) and bounded `deque` `_publish_buffer` with `"reject"` and `"drop_oldest"` overflow policies.
- **Egress Backpressure & Circuit Breakers (`RES-02`):** `CircuitBreaker` (`CLOSED`, `OPEN`, `HALF_OPEN`) isolates failing target brokers, diverts messages to the target's publish ring buffer before DLQ drop while `OPEN`, and flushes buffered messages on `HALF_OPEN -> CLOSED` probe recovery.
- **Credential & TLS Hot-Reload (`RES-03`):** `reload_security()` across all 4 broker adapters and `ControlPlane.update_broker_security()` / `DataPlane` live Pub/Sub updates rotate TLS/SASL credentials in-flight without losing subscriptions or buffered publishes.
- **Security:** Unified `SecurityConfig` dataclass normalizing TLS/mTLS, certificates, and authentication credentials across MQTT, AMQP, Kafka, and NanoMQ.
- **Port Selection:** Automatic port shifting on TLS activation (MQTT 1883<->8883, AMQP 5672<->5671, NanoMQ 1884<->8884).
- **Schema Management:** Pluggable `BaseSchemaRegistry` supporting `InMemorySchemaRegistry` and `RedisSchemaRegistry`.
- **Serialization:** Dual support for Apache Avro (`fastavro`) and Protocol Buffers (`protobuf`) with bidirectional JSON conversion and Confluent 5-byte wire format support.
- **Error Containment:** Schema transform and validation errors route to DLQ with upstream ACK and increment `schema_validation_error` metric.
- **Topic Matching:** $O(k)$ `TopicTrie` hierarchical prefix matching replaces linear scans for high-scale topic routing.
- **Delivery Semantics:** `_FanoutCoordinator` atomic latch coordinates multi-target publisher confirms before acknowledging upstream ingress.
- **Dead Letter Queue:** Structured JSON envelopes with base64 binary encoding fallback and anti-recursion protection (`requeue=False`).
- **Distributed State:** Use Redis hashes and PubSub channel `postoffice:config_updates` for real-time cluster coordination.
- **Hermetic Testing:** 100% offline unit tests without external broker dependencies.
- **v3.2 phase ordering:** Native metrics parity (Phase 9) comes first because it fixes the canonical metric/label set that both OTLP exporters (Phases 10-11) must mirror.
- **v3.2 safety gate (OTEL-06):** The opt-in, bounded, drop-oldest export pipeline is built once in Phase 10 and reused by native metrics, tracing, and logs; later phases re-verify it for their signal but do not own the requirement.
- **v3.2 native export constraint (OTEL-02):** No new third-party Rust dependencies; the edge profile (`scripts/verify_edge_profile.py`) must stay within its size budget.
- **v3.2 control plane grouping:** Route-filter validation and docs (CPL-01/02) ride with runtime OTel config (OTEL-07) in Phase 14 since both are ControlPlane/admin-surface changes; CPL has no upstream dependencies and can be split out earlier if desired.
- **v3.2 deferred:** `RTEL-01` (recognizer-telemetry) is intentionally unmapped until the repo and scope are supplied. Raft / HA control plane is parked as backlog `999.1`.

### Technical Constraints

- Python 3.12+ runtime compatibility
- Zero data loss guarantees when translating between at-least-once systems
- OTel export is opt-in, off by default, and must never block or drop routing (bounded buffer, drop-oldest)

### Pending Todos

None yet.

### Blockers/Concerns

- Phase 10: choose between the `opentelemetry-*` Python SDK as an optional extra and a hand-rolled OTLP/HTTP encoder that can share a wire-format design with the native exporter (research flag for plan-phase).
- Phase 12: MQTT 3.1.1 has no user properties; only MQTT 5 can carry `traceparent`. Plans must define the new-trace fallback for header-less ingress.
- Phase 13: DLQ envelopes contain message payloads; the plan must decide that exported DLQ log events carry reason/route/broker metadata and not raw payload content.

## Session Continuity

Last session: 2026-11-11
Stopped at: v3.2 roadmap created; ready to run `/gsd-plan-phase 9`.
Resume file: None

---
*Last updated: 2026-11-11 after v3.2 roadmap creation*
