# Phase 4: Observability & Dead Letter Queues - Research

**Researched:** 2026-10-06  
**Domain:** Prometheus Metrics, OpenTelemetry Integration, Dead Letter Queues, Failure Containment  
**Confidence:** HIGH  

<user_constraints>
## User Constraints (from 04-CONTEXT.md)

### Locked Decisions
- **D-01:** Add `prometheus_client` to `requirements.txt` and install into the project environment.
- **D-02:** Expose metrics via `prometheus_client.start_http_server(port)` on `PostOffice.start()` when a metrics port is configured (`metrics_port=9100`).
- **D-03:** Define standardized metric collectors:
  - `postoffice_messages_routed_total`: Counter with labels `[source_broker, target_broker, status]`.
  - `postoffice_routing_errors_total`: Counter with labels `[source_broker, error_type]`.
  - `postoffice_routing_duration_seconds`: Histogram with label `[source_broker]`.
  - `postoffice_dlq_messages_total`: Counter with labels `[source_broker, dlq_broker, dlq_topic]`.
- **D-04:** DLQ configurable at two levels:
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
    "is_base64": false,
    "metadata": { ... }
  }
  ```
- **D-07:** Upstream Acknowledgement on DLQ:
  - If a message fails standard routing but is successfully routed to the DLQ, call `ack_fn()` to release the ingress broker and prevent poison-pill infinite loops.
  - If DLQ publish itself fails or no DLQ is configured, invoke `nack_fn(requeue=True)`.

### Claude's Discretion
- Metrics module location (`src/postoffice/metrics.py`).
- HTTP metrics server lifecycle management on `PostOffice.stop()` / `PostOffice.reset()`.
- Base64 encoding helper for non-UTF8 binary message payloads in DLQ envelopes.
- Anti-recursion protection so messages intended for DLQ never route back to DLQ.

</user_constraints>

<architectural_responsibility_map>
## Architectural Responsibility Map

| Component | Responsibility | Implementation Details |
|-----------|----------------|------------------------|
| `MetricsManager` (`src/postoffice/metrics.py`) | Prometheus registry, metrics definition, recording helpers, HTTP server | Encapsulates Prometheus `CollectorRegistry`, counters, histogram, and `get_metrics_text()` |
| `Router` (`src/postoffice/router.py`) | DLQ triggering on unroutable/error, metrics recording on ingress/egress/duration | Records metrics on route entry and exit; routes failed payloads to DLQ |
| `PostOffice` (`src/postoffice/app.py`) | Metrics server lifecycle (`start_http_server`), DLQ facade configuration (`set_dlq`) | Starts/stops metrics server, stores default DLQ configuration |
| `tests/test_metrics.py` | Hermetic unit tests for metrics collection and HTTP export | Tests counter increments, latency recording, and Prometheus text export |
| `tests/test_dlq.py` | Hermetic unit tests for unroutable & failure DLQ routing | Tests unroutable DLQ, delivery failure DLQ, envelope JSON, and anti-recursion |

</architectural_responsibility_map>

<research_summary>
## Algorithmic & Implementation Details

### 1. Prometheus Metrics Architecture
Using `prometheus_client`:
- Create custom `CollectorRegistry` instance inside `MetricsManager` rather than relying solely on global default registry, ensuring unit tests can run hermetically without leaking state between tests.
- Provide `MetricsManager.reset()` to reset metrics between test runs.
- Provide `start_server(port: int)` and `stop_server()`: Note that `prometheus_client.start_http_server` uses an internal daemon `HTTPServer`. To support clean teardown in tests without port collision, provide `get_latest_metrics()` returning raw Prometheus text.

### 2. DLQ Envelope Formatting
Binary payloads (e.g. Protocol Buffers, gzip, raw binary sensor frames) can contain invalid UTF-8 bytes that cause `json.dumps()` to fail.
The envelope formatter must attempt `payload.decode('utf-8')`. If `UnicodeDecodeError` occurs, fallback to `base64.b64encode(payload).decode('ascii')` and set `"is_base64": True`.

### 3. Anti-Recursion Safety
If an error occurs while publishing to the DLQ itself:
- Catch the exception.
- Increment `postoffice_routing_errors_total` with `error_type="dlq_failure"`.
- Invoke `nack_fn(requeue=False)` or log critical error to prevent infinite bounce.

</research_summary>
