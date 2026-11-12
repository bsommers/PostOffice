# Phase 4: Observability & Dead Letter Queues - Context

**Gathered:** 2026-10-06  
**Status:** Ready for planning  

<domain>
## Phase Boundary

Phase 4 delivers production observability and failure containment for PostOffice:
1. **Metrics Instrumentation (`OBS-01`):** Integrating `prometheus_client` to expose metrics on an HTTP endpoint (`/metrics`), measuring throughput, latency, routing errors, and DLQ discards.
2. **Dead Letter Queues (`DLQ-01`):** Providing automated dead-letter routing for unroutable messages and failed downstream deliveries.
3. **Envelope Packaging:** Wrapping DLQ payloads in a structured JSON envelope preserving original data, ingress origin, error reason, and timestamp.
4. **Poison-Pill Containment:** Invoking upstream `ack_fn` once a message is successfully delivered to the DLQ to prevent infinite reprocessing loops.

</domain>

<decisions>
## Implementation Decisions

### Metrics & Observability (`OBS-01`)
- **D-01:** Add `prometheus_client` to `requirements.txt` and install into the project environment.
- **D-02:** Expose metrics via `prometheus_client.start_http_server(port)` on `PostOffice.start()` when a metrics port is configured (e.g. `metrics_port=9100` or via method `enable_metrics(port=9100)`).
- **D-03:** Define standardized metric collectors:
  - `postoffice_messages_routed_total`: Counter with labels `[source_broker, target_broker, status]`.
  - `postoffice_routing_errors_total`: Counter with labels `[source_broker, error_type]`.
  - `postoffice_routing_duration_seconds`: Histogram with label `[source_broker]`.
  - `postoffice_dlq_messages_total`: Counter with labels `[source_broker, dlq_broker, dlq_topic]`.

### Dead Letter Queue Architecture (`DLQ-01`)
- **D-04:** Configurable at two levels:
  - Global default DLQ on `PostOffice` (`po.set_dlq(broker_name, topic)` or `PostOffice(dlq_broker=..., dlq_topic=...)`).
  - Optional per-route override in `po.add_route(..., dlq_broker=..., dlq_topic=...)`.
- **D-05:** DLQ Trigger Conditions:
  - Unroutable message: Ingress message arrives with no matching routes in `TopicTrie`.
  - Delivery failure: Target client publish fails, times out, or reports `on_error`.
- **D-06:** DLQ Envelope Schema:
  JSON-encoded string/bytes containing:
  ```json
  {
    "payload": "<str_or_base64>",
    "source_broker": "mqtt_1",
    "source_topic": "sensor/temp",
    "timestamp": "2026-10-06T17:05:00Z",
    "error": "Error description or exception message",
    "metadata": { ... }
  }
  ```
- **D-07:** Upstream Acknowledgement on DLQ:
  - If a message fails standard routing but is successfully routed to the DLQ, call `ack_fn()` to release the ingress broker and prevent poison-pill infinite loops.
  - If DLQ publish itself fails or no DLQ is configured, invoke `nack_fn(requeue=True)`.

### Claude's Discretion
- Metrics module location (e.g. `src/postoffice/metrics.py`).
- HTTP metrics server lifecycle management on `PostOffice.stop()` / `PostOffice.reset()`.
- Base64 encoding helper for non-UTF8 binary message payloads in DLQ envelopes.

</decisions>

<canonical_refs>
## Canonical References

### Source Files
- `src/postoffice/app.py` — `PostOffice` facade lifecycle (`start`, `stop`, `reset`)
- `src/postoffice/router.py` — `Router` message routing and error handling
- `src/postoffice/trie.py` — `TopicTrie` unroutable matching detection

### Architecture & Requirements
- `.planning/REQUIREMENTS.md` — `OBS-01`, `DLQ-01`
- `.planning/ROADMAP.md` — Phase 4 goals and success criteria

</canonical_refs>

<threat_model>
## Security & Correctness Boundary

1. **DLQ Recursion Cascades:** A failure to publish to the DLQ broker must NEVER attempt to send the failed DLQ message back to the DLQ (infinite loop).
2. **Binary Payload Corruption:** Arbitrary binary payloads (e.g. Protocol Buffers, Avro, raw sensor bytes) must be safely encoded (e.g. Base64) in the JSON envelope so invalid UTF-8 bytes do not cause JSON serialization crashes.
3. **Metrics Cardinality Explosion:** Labels on Prometheus metrics must only include bounded categorical strings (broker names, status `success`/`failure`); never include dynamic payload IDs or high-cardinality topic parameters.

</threat_model>
