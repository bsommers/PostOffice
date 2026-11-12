# Phase 4: Observability & Dead Letter Queues - Discussion Log

**Date:** 2026-10-06  
**Participants:** User & Antigravity  

### Questions & Locked Decisions

1. **How should Prometheus metrics instrumentation (OBS-01) be implemented?**
   - **Decision:** Standard `prometheus_client` library added to `requirements.txt` with an HTTP `/metrics` endpoint on `PostOffice`.
   - **Rationale:** Exposes industry-standard metrics format compatible with Prometheus, Grafana Agent, Datadog, and OpenTelemetry collectors with zero custom parsing.

2. **Where should Dead Letter Queue (DLQ-01) destinations be configured?**
   - **Decision:** Configurable at both levels: global default DLQ on `PostOffice` with optional per-route override on `add_route()`.
   - **Rationale:** Provides operational safety by default for all unrouted/failed messages across the system, while allowing high-priority or isolated routes to route failures to dedicated queues.

3. **What format should Dead Letter Queue messages use?**
   - **Decision:** JSON-wrapped envelope containing original payload (base64/text), `source_broker`, `source_topic`, `timestamp`, and `error`.
   - **Rationale:** Retains all forensic metadata needed for downstream debugging, auditing, replay, or manual inspection without modifying broker payload schemas.

4. **How should upstream acknowledgement behave when a message is routed to DLQ?**
   - **Decision:** ACK upstream once the message is safely stored in the DLQ.
   - **Rationale:** Prevents poison-pill messages from triggering infinite retry storms on the ingress broker. If the DLQ publish fails, NACK is issued as a fallback.
