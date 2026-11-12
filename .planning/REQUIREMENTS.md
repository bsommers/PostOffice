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

- Raft / HA control plane (backlog)

## Out of Scope

| Feature | Reason |
|---------|--------|
| Raft / HA control plane | Deferred to a future milestone, tracked as backlog |
| Vendor-specific exporters (Datadog, etc.) | OTLP is the single export path |
| Native HTTP/WebSocket delivery | Separate adapter-parity milestone |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|

---
*Requirements defined: 2026-11-11*
