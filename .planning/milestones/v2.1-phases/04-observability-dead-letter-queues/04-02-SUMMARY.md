---
phase: 04-observability-dead-letter-queues
plan: 02
subsystem: dead-letter-queue
tags:
  - dlq
  - error-handling
  - poison-pill
  - routing
  - unittest

requires:
  - 04-01-PLAN.md
provides:
  - DLQ envelope formatting with UTF-8 and base64 fallback in format_dlq_payload
  - Fallback routing for unroutable messages and delivery failures
  - Global default and per-route DLQ configuration
  - Anti-recursion protection preventing infinite bounce loops
  - Hermetic unit tests in tests/test_dlq.py
affects:
  - Router
  - PostOffice
  - _FanoutCoordinator

tech-stack:
  added: []
  patterns:
    - JSON DLQ envelope preserving payload, source broker/topic, error, timestamp, and metadata
    - Upstream ACK upon successful DLQ dispatch to clear poison pills
    - Anti-recursion loop detection with poison-pill rejection (requeue=False)

key-files:
  created:
    - src/postoffice/dlq.py
    - tests/test_dlq.py
  modified:
    - src/postoffice/router.py
    - src/postoffice/app.py

key-decisions:
  - "Serialized payload as UTF-8 string when valid, falling back to base64 encoding with is_base64: true for binary data"
  - "Invoked upstream ack_fn() when message is successfully dispatched to DLQ to prevent poison pills from replaying indefinitely"
  - "Allowed per-route DLQ overrides via add_route(..., dlq_broker=..., dlq_topic=...) taking precedence over global default DLQ"
  - "Added anti-recursion guard rejecting messages originating on the DLQ destination itself with nack_fn(requeue=False)"

patterns-established:
  - "Router dispatches unroutable messages and downstream publish errors to DLQ when configured"
  - "PostOffice exposes set_dlq(broker, topic) and accepts dlq_broker/dlq_topic in __init__ and add_route"
  - "FanoutCoordinator attempts DLQ fallback before issuing nack_fn(requeue=True)"

requirements-completed:
  - DLQ-01

coverage:
  - id: D2
    description: "Dead letter queue fallback for unroutable messages and delivery failures"
    requirement: "DLQ-01"
    verification:
      - kind: unit
        ref: "tests/test_dlq.py"
        status: pass
    human_judgment: false
---

# Plan 04-02 Summary

Implemented Dead Letter Queue (DLQ) support in `src/postoffice/dlq.py`, integrated fallback DLQ routing and anti-recursion protection into `Router` and `PostOffice`, and verified behavior with 8 unit tests in `tests/test_dlq.py`.

## Key Changes
1. **`src/postoffice/dlq.py`:**
   - Created `format_dlq_payload` formatting failed messages into a structured JSON envelope containing `source_broker`, `source_topic`, `timestamp`, `error`, `payload`, `is_base64`, and `metadata`.
   - Safely handles arbitrary binary messages via base64 fallback.
2. **`src/postoffice/router.py`:**
   - Added `set_dlq(broker_name, topic)` for global default DLQ.
   - Added `_route_to_dlq(...)` helper with anti-recursion detection (`source_broker == target_broker and source_topic == target_topic`).
   - Integrated DLQ dispatch into `route()` for unroutable messages and `_FanoutCoordinator.dlq_fn` for delivery failures.
   - Upstream `ack_fn()` called once message is safely stored in DLQ; anti-recursion drops trigger `nack_fn(requeue=False)`.
3. **`src/postoffice/app.py`:**
   - Added `dlq_broker` and `dlq_topic` parameters to `__init__`.
   - Added `set_dlq(broker_name, topic)` delegating to `Router.set_dlq`.
   - Preserved DLQ settings across `reset()`.
4. **`tests/test_dlq.py`:**
   - 8 hermetic unit tests verifying text/binary formatting, unroutable message routing, delivery failure routing, per-route overrides, unconfigured fallback NACK, anti-recursion guards, and facade integration.

## Verification
- `PYTHONPATH=src .venv/bin/python -m unittest tests/test_dlq.py` (8 tests in 0.002s, OK)
- `PYTHONPATH=src .venv/bin/python -m unittest discover tests/` (43 tests in 0.524s, OK)
