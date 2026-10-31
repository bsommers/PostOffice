# Prometheus Metrics & Cardinality Contract (v1)

## 1. Registered Metrics

All metrics are exposed in standard Prometheus text exposition format.

| Metric Name | Type | Labels | Description |
|---|---|---|---|
| `postoffice_messages_routed_total` | Counter | `source_broker`, `target_broker`, `source_topic`, `target_topic` | Total number of messages successfully routed between brokers. |
| `postoffice_routing_errors_total` | Counter | `broker`, `error_type` | Total number of message routing or broker errors encountered. |
| `postoffice_message_processing_seconds` | Histogram | `source_broker`, `target_broker` | Time spent processing and routing messages (`DEFAULT_BUCKETS`). |
| `postoffice_dlq_messages_total` | Counter | `source_broker`, `dlq_broker` | Total number of failed messages routed to a Dead-Letter Queue. |
| `postoffice_dlq_dropped_total` | Counter | `source_broker`, `reason` | DLQ writes suppressed or altered by the DLQ budget (`duplicate`, `rate_limited`, `byte_limited`, `oversize_truncated`). |
| `postoffice_circuit_breaker_state` | Gauge | `broker` | Current state of the broker circuit breaker (`0=CLOSED`, `1=HALF_OPEN`, `2=OPEN`). |
| `postoffice_control_events_total` | Counter | `result` | Control-plane messages processed by the DataPlane, by outcome (`applied`, `stale`, `gap`, `unsigned`, `bad_signature`, `bad_version`, `oversize`, `rate_limited`, `invalid`, `forbidden_action`). |
| `postoffice_ingress_decisions_total` | Counter | `broker`, `action`, `rule`, `category` | Ingress firewall verdicts per broker, action (`allow`, `drop`, `reject`), rule, and failure category. |
| `postoffice_ingress_inflight` | Gauge | `broker` | Messages currently inside `Router.route()` for this source broker. |

## 2. Cardinality Protection (`LabelGuard`)

To prevent attacker-controlled topics or error strings from exhausting memory in the metrics registry:

- Every dynamic label dimension (`source_broker`, `target_broker`, `source_topic`, `target_topic`, `broker`, `error_type`, `dlq_broker`) is guarded by a `LabelGuard` with default `max_values = 100` (`DEFAULT_MAX_LABEL_VALUES`) and overflow bucket `"other"` (`OVERFLOW_LABEL`).
- The first `max_values` distinct label strings observed for a dimension are admitted verbatim (including `"other"` if seen early).
- Once a dimension's distinct set reaches `max_values`, any unseen label string is mapped to `"other"`.
- Closed-set enums (`result`, `action`, `rule`, `category`, `reason`) are bounded by code constants.
