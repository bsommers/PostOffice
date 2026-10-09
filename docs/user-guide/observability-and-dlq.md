# Observability & Dead Letter Queues

[← Back to User Guide](README.md) | [← Back to Project README](../../README.md)

PostOffice provides enterprise-grade observability and failure isolation via Prometheus metrics instrumentation and configurable Dead Letter Queue (DLQ) poison-pill quarantine.

---

## Table of Contents
- [Prometheus Metrics Instrumentation](#prometheus-metrics-instrumentation)
  - [Metric Collectors Reference](#metric-collectors-reference)
  - [Enabling the HTTP Scraping Endpoint](#enabling-the-http-scraping-endpoint)
  - [Scraping via Prometheus / OpenTelemetry](#scraping-via-prometheus--opentelemetry)
- [Dead Letter Queue (DLQ) Routing](#dead-letter-queue-dlq-routing)
  - [Global Default vs. Per-Route DLQ](#global-default-vs-per-route-dlq)
  - [Structured Forensic JSON Envelope](#structured-forensic-json-envelope)
  - [Binary & Base64 Payload Safety](#binary--base64-payload-safety)
  - [Anti-Recursion Loop Protection](#anti-recursion-loop-protection)

---

## Prometheus Metrics Instrumentation

PostOffice embeds Prometheus instrumentation via `MetricsManager`, providing real-time visibility into message throughput, routing latencies, error classifications, and dead-letter dispatches.

### Metric Collectors Reference

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `postoffice_messages_routed_total` | Counter | `source_broker`, `target_broker`, `status` | Total count of messages routed, labeled by delivery outcome (`"success"` or `"error"`). |
| `postoffice_routing_errors_total` | Counter | `source_broker`, `error_type` | Total errors encountered, classified by type (`"unroutable"`, `"publish_failure"`, `"schema_validation_error"`, `"unregistered_target"`, `"dlq_recursion"`). |
| `postoffice_routing_duration_seconds` | Histogram | `source_broker` | Latency distribution of routing operations from ingress receipt to egress dispatch. |
| `postoffice_dlq_messages_total` | Counter | `source_broker`, `dlq_broker`, `dlq_topic` | Total messages diverted into Dead Letter Queues. |

### Enabling the HTTP Scraping Endpoint

Pass `metrics_port` when initializing `PostOffice`:

```python
from postoffice.app import PostOffice

po = PostOffice(metrics_port=9100)
po.add_broker("mqtt_local", "mqtt", host="localhost", port=1883)
po.add_route("mqtt_local", "sensors/#", "kafka_main", "raw.sensors")
po.start()
```

When `po.start()` is called, a lightweight WSGI HTTP server starts in a background daemon thread exposing `/metrics` on port 9100. Calling `po.stop()` or `po.reset()` terminates the server cleanly.

### Scraping via Prometheus / OpenTelemetry

Add the scrape target to your `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'postoffice'
    static_configs:
      - targets: ['localhost:9100']
    scrape_interval: 5s
```

Sample output from `curl http://localhost:9100/metrics`:
```text
# HELP postoffice_messages_routed_total Total number of messages successfully or unsuccessfully routed
# TYPE postoffice_messages_routed_total counter
postoffice_messages_routed_total{source_broker="mqtt_local",status="success",target_broker="kafka_main"} 14205.0

# HELP postoffice_routing_duration_seconds Latency of message routing operations in seconds
# TYPE postoffice_routing_duration_seconds histogram
postoffice_routing_duration_seconds_bucket{le="0.001",source_broker="mqtt_local"} 14190.0
postoffice_routing_duration_seconds_bucket{le="0.005",source_broker="mqtt_local"} 14205.0
postoffice_routing_duration_seconds_sum{source_broker="mqtt_local"} 8.52
postoffice_routing_duration_seconds_count{source_broker="mqtt_local"} 14205.0
```

---

## Dead Letter Queue (DLQ) Routing

When a message cannot be delivered—whether due to missing routes, broker connection failures, or schema validation errors—letting it retry endlessly creates a "poison pill" that blocks upstream queues.

PostOffice captures failed payloads into a Dead Letter Queue and acknowledges upstream messages so production traffic continues flowing uninterrupted.

### Global Default vs. Per-Route DLQ

You can define a global default DLQ on `PostOffice`, and selectively override it for sensitive routes:

```python
# Configure global fallback DLQ on Kafka
po = PostOffice(dlq_broker="kafka_bus", dlq_topic="deadletter.global")

# Standard route uses global DLQ
po.add_route("mqtt_edge", "telemetry/#", "kafka_bus", "telemetry.raw")

# High-priority payment route overrides DLQ to dedicated quarantine queue
po.add_route(
    source_broker="mqtt_edge",
    source_topic="payments/orders",
    target_broker="rabbit_mq",
    target_topic="orders.processed",
    dlq_broker="rabbit_mq",
    dlq_topic="orders.failed_deadletter"
)
```

### Structured Forensic JSON Envelope

All dead-letter payloads are wrapped in a forensic JSON envelope (`format_dlq_payload`):

```json
{
  "source_broker": "mqtt_edge",
  "source_topic": "sensors/temperature",
  "timestamp": "2026-10-08T20:15:30.124567+00:00",
  "error": "Schema transformation error: could not convert string to float: 'NOT_A_FLOAT'",
  "payload": "{\"sensor_id\": \"dev-1\", \"temperature\": \"NOT_A_FLOAT\"}",
  "is_base64": false,
  "metadata": {}
}
```

### Binary & Base64 Payload Safety

If the failed message contains binary data that is not valid UTF-8 (e.g. raw Protobuf or compressed Avro bytes), `format_dlq_payload` automatically Base64-encodes the payload and sets `"is_base64": true`. This guarantees safe JSON serialization without payload corruption.

### Anti-Recursion Loop Protection

If an error occurs while processing a message that originated from the Dead Letter Queue topic itself, attempting to DLQ the failure again would create an infinite recursive bounce loop.

PostOffice detects this condition:
```
source_broker == target_broker and source_topic == target_topic
```
When recursion is detected:
1. Logs a `CRITICAL` recursion warning.
2. Increments `postoffice_routing_errors_total{error_type="dlq_recursion"}`.
3. Drops the looping message.
4. Invokes `nack_fn(requeue=False)` to permanently reject the message and prevent broker requeueing.
