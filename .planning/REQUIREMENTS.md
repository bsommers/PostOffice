# Requirements: PostOffice

**Defined:** 2026-11-11
**Core Value:** Seamlessly route and translate messaging payloads and delivery semantics across heterogeneous pub/sub, streaming, and queueing protocols with zero message loss.

## v3.2 Requirements (Observability Exports - OpenTelemetry)

### Native metrics parity

- [ ] **NMET-01**: Native `/metrics` renders `postoffice_sender_events_total` with the same labels as the Python engine
- [ ] **NMET-02**: Native `/metrics` renders `postoffice_routing_duration_seconds` (histogram) and `postoffice_inflight_messages` (gauge)
- [ ] **NMET-03**: `PostOffice(use_native_engine=True)` starts the native metrics/health server, and `set_metrics_port` applies to it
- [ ] **NMET-04**: A conformance/shadow check asserts Python and native expose the same metric names and label sets

### OpenTelemetry export

- [ ] **OTEL-01**: Python engine exports metrics over OTLP/HTTP, bridged from existing metrics, configured via standard `OTEL_*` environment variables
- [ ] **OTEL-02**: Native engine exports the same metrics over OTLP/HTTP with no new third-party Rust dependencies and the edge profile within its size budget
- [ ] **OTEL-03**: Each routed message produces a trace span (ingress, route, egress) in both engines, with route and broker attributes and no payload content
- [ ] **OTEL-04**: W3C `traceparent` context is propagated through MQTT 5 user properties, AMQP headers, Kafka headers, and HTTP/WebSocket headers
- [ ] **OTEL-05**: Structured logs and DLQ events are exportable as OTLP logs correlated with trace IDs
- [ ] **OTEL-06**: OTel export is opt-in and off by default; a collector outage never blocks or drops routing (bounded buffer, drop-oldest)
- [ ] **OTEL-07**: ControlPlane can enable and configure OTel export at runtime (signed config event) for Python and native workers

### Control plane cleanup

- [ ] **CPL-01**: `ControlPlane.add_route` validates `filter` specs at call time and raises `ValueError` for invalid specs
- [ ] **CPL-02**: `scripts/admin.py` and the user guide include a route-filter example

### Integration placeholder

- [ ] **RTEL-01** (future, scope TBD): Define the integration contract with recognizer-telemetry once the repo is supplied

## Future Requirements

- Raft / HA control plane (backlog, tracked as ROADMAP Backlog `999.1`)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Raft / HA control plane | Deferred to a future milestone, tracked as backlog |
| Vendor-specific exporters (Datadog, etc.) | OTLP is the single export path |
| Native HTTP/WebSocket delivery | Separate adapter-parity milestone |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| NMET-01 | Phase 9 | Pending |
| NMET-02 | Phase 9 | Pending |
| NMET-03 | Phase 9 | Pending |
| NMET-04 | Phase 9 | Pending |
| OTEL-01 | Phase 10 | Pending |
| OTEL-06 | Phase 10 | Pending |
| OTEL-02 | Phase 11 | Pending |
| OTEL-03 | Phase 12 | Pending |
| OTEL-04 | Phase 12 | Pending |
| OTEL-05 | Phase 13 | Pending |
| OTEL-07 | Phase 14 | Pending |
| CPL-01 | Phase 14 | Pending |
| CPL-02 | Phase 14 | Pending |
| RTEL-01 | Unmapped by design (future, scope TBD) | Deferred |

**Coverage:** 13/13 mapped v3.2 requirements assigned to exactly one phase; `RTEL-01` intentionally deferred (not counted).

---
*Requirements defined: 2026-11-11*
*Traceability updated: 2026-11-11 after v3.2 roadmap creation*
