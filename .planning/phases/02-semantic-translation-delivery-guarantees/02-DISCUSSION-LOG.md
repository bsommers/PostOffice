# Phase 2: Semantic Translation & Delivery Guarantees - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-10-06
**Phase:** 2-Semantic Translation & Delivery Guarantees
**Areas discussed:** Ack Propagation Mechanism, Egress Failure Behavior, Default QoS Mapping, Multi-Target Policy

---

## Ack Propagation Mechanism

| Option | Description | Selected |
|--------|-------------|----------|
| Callback-based async confirmation seam | Pass ack_fn/nack_fn through Router to egress publish callbacks | ✓ |
| Centralized AckCoordinator | Stateful tracking table with message IDs, latches, timeout sweepers | |
| You decide | Whichever is cleanest to test hermetically | |

**User's choice:** Callback-based async confirmation seam
**Notes:** Lightweight, direct callback chaining avoiding centralized state bottlenecks.

---

## Egress Failure Behavior

| Option | Description | Selected |
|--------|-------------|----------|
| Propagate NACK to ingress when supported | AMQP basic_nack(requeue=True), withhold ACK for MQTT | ✓ |
| Log & Drop | Log error and do not call ack_fn | |
| You decide | Standard messaging best practice | |

**User's choice:** Propagate NACK to ingress when supported
**Notes:** Prevents silent message drops across translation hops.

---

## Default QoS Mapping

| Option | Description | Selected |
|--------|-------------|----------|
| Automatic semantic translation with explicit override | Auto-map MQTT QoS 1/2 <-> AMQP delivery_mode 2 unless overridden | ✓ |
| Explicit kwargs only | Do not infer delivery mode or QoS from ingress message unless explicitly defined | |
| You decide | Match architecture spec in docs/ARCHITECTURE.md | |

**User's choice:** Automatic semantic translation with explicit override
**Notes:** Meets the core promise in `docs/ARCHITECTURE.md` while allowing explicit override in `add_route`.

---

## Multi-Target Policy

| Option | Description | Selected |
|--------|-------------|----------|
| All-Succeed | ACK ingress only after all matching egress targets confirm delivery; NACK if any fails | ✓ |
| At-Least-One | ACK ingress once the first target confirms | |
| You decide | Prioritize zero data loss across fanout targets | |

**User's choice:** All-Succeed
**Notes:** Guarantees no data loss across fanout targets.
