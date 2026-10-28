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
| `postoffice_routing_errors_total` | Counter | `source_broker`, `error_type` | Total errors encountered, classified by type (`"unroutable"`, `"unroutable_nacked"`, `"unroutable_dropped"`, `"publish_failure"`, `"circuit_breaker_open"`, `"schema_validation_error"`, `"unregistered_target"`, `"dlq_recursion"`). |
| `postoffice_routing_duration_seconds` | Histogram | `source_broker` | Latency distribution of routing operations from ingress receipt to egress dispatch. |
| `postoffice_dlq_messages_total` | Counter | `source_broker`, `dlq_broker`, `dlq_topic` | Total messages diverted into Dead Letter Queues. |
| `postoffice_dlq_refused_total` | Counter | `source_broker`, `reason` | DLQ writes refused by the budget (`oversize`, `rate`, `bytes`). A rising value means a sender is misbehaving or misconfigured. |
| `postoffice_control_events_total` | Counter | `result` | Control Plane events seen by a Data Plane worker, by outcome (`applied`, `stale`, `gap`, `unsigned`, `bad_signature`, `bad_version`, `oversize`, `rate_limited`, `forbidden_action`, `invalid`). See [Securing the Control Plane](distributed-plane.md#securing-the-control-plane). |
| `postoffice_circuit_breaker_state` | Gauge | `target_broker` | Current state of per-broker egress circuit breaker (`0` = `CLOSED`, `1` = `OPEN`, `2` = `HALF_OPEN`). |

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

### Unroutable Messages (no matching route)

What happens to a message that matches no route is a policy (`on_unroutable`), **default `nack`**:

| Policy | Behaviour |
|---|---|
| `nack` (default) | `nack(requeue=False)` to the source. Never requeued, because an unroutable message fails identically on redelivery. Counted as `unroutable_nacked`. |
| `dlq` | Published to the DLQ (subject to the DLQ budget below). If the DLQ is unset, refuses the write, or fails, falls back to `nack`. |
| `drop` | Acknowledged and discarded. Counted as `unroutable_dropped`. |

```python
po = PostOffice(dlq_broker="kafka_bus", dlq_topic="deadletter.global", on_unroutable="dlq")
po.set_unroutable_policy("drop", broker="mqtt_noisy")   # per-source override
```

Not every source can be pushed back. A non-requeue nack on **Kafka** skips the message and on **MQTT** has no effect, so for those sources an unroutable message is effectively shed. It is always counted (`unroutable_nacked` / `unroutable_dropped`), so watch those counters.

### DLQ Budget (flood protection)

Every DLQ write (unroutable, conversion failure, delivery failure) passes through an admission budget, tracked **per source broker** so one noisy source cannot exhaust another's allowance:

| Limit | Default | Refusal reason |
|---|---|---|
| `max_message_bytes` | 262144 | `oversize`. The message is **refused, never truncated or stored** |
| `msgs_per_sec` | 100 | `rate` |
| `bytes_per_sec` | 1048576 | `bytes` |

```python
po.set_dlq_budget(max_message_bytes=65536, msgs_per_sec=20, bytes_per_sec=262144)
```

A refused write counts in `postoffice_dlq_refused_total`, the source is nacked without requeue, and nothing is logged above DEBUG per message. Refusals are not necessarily attacks: a stuck device or a bad rollout looks the same, so use the counter's rate and `source_broker` label to investigate.

### Protecting MQTT sources

PostOffice cannot slow an MQTT publisher: the client library acknowledges QoS 1/2 messages on arrival, so PostOffice can only shed (drop and count). Set limits on the broker itself, as the primary control:

| Broker | Settings to review |
|---|---|
| Mosquitto | `message_size_limit`, `max_inflight_messages`, `max_queued_messages`, `max_connections`, per-user ACLs |
| NanoMQ | `mqtt.max_packet_size`, `mqtt.max_inflight_window`, `mqtt.max_mqueue_len`, ACLs, connection limits |
| RabbitMQ | per-consumer `prefetch_count`, queue `max-length`/`x-max-length-bytes`, `max_message_size` |
| Kafka | `message.max.bytes`, topic quotas (`producer_byte_rate`, `consumer_byte_rate`) |

Check exact setting names against your broker version.

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
