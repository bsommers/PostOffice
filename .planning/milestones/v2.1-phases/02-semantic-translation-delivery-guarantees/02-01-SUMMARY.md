---
phase: 02-semantic-translation-delivery-guarantees
plan: 01
subsystem: routing
tags:
  - routing
  - delivery-guarantees
  - qos
  - amqp-delivery-mode
  - callbacks

requires: []
provides:
  - BaseClient async confirmation signatures (ack_fn, nack_fn, on_confirm, on_error)
  - _FanoutCoordinator thread-safe multi-target confirmation tracker
  - _translate_semantics for automatic MQTT QoS <-> AMQP delivery_mode mapping
  - Router.route confirmation propagation
affects:
  - 02-02-PLAN.md
  - plugins
  - tests

tech-stack:
  added: []
  patterns:
    - Thread-safe countdown latch (_FanoutCoordinator) for 1-to-N target publish confirmations
    - Automatic semantic mapping between pub/sub QoS and message queue delivery_mode

key-files:
  created: []
  modified:
    - src/postoffice/base_client.py
    - src/postoffice/router.py

key-decisions:
  - "Made ack_fn and nack_fn strictly optional to maintain full backward compatibility with fire-and-forget callers"
  - "Coordinated multi-target fanout using _FanoutCoordinator with All-Succeed policy: ingress ack_fn called only when all egress targets confirm, and nack_fn called if any egress target errors"
  - "Mapped MQTT QoS 1/2 to AMQP delivery_mode=2 and MQTT QoS 0 to delivery_mode=1, with explicit route kwargs always overriding defaults"

patterns-established:
  - "BaseClient.on_message forwards ingress ack_fn and nack_fn to Router.route"
  - "Router.route passes on_confirm and on_error callbacks into egress client publish calls when ingress confirmation is requested"

requirements-completed:
  - SEM-01 (partial: core router and base client contracts)
  - SEM-02 (partial: router translation helper)

coverage:
  - id: D1
    description: "BaseClient signatures for on_message and publish support delivery confirmation callbacks"
    requirement: "SEM-01"
    verification:
      - kind: unit
        ref: "BaseClient parameter inspection"
        status: pass
    human_judgment: false
  - id: D2
    description: "Router coordinates multi-target confirmations and translates QoS <-> delivery_mode"
    requirement: "SEM-02"
    verification:
      - kind: unit
        ref: "tests/test_postoffice.py"
        status: pass
    human_judgment: false
---

# Plan 02-01 Summary

Implemented asynchronous delivery confirmation contracts in `BaseClient` and added `_FanoutCoordinator` along with semantic translation rules (`_translate_semantics`) to `Router`.

## Key Changes
1. **`src/postoffice/base_client.py`:**
   - Updated `BaseClient.publish` with `on_confirm` and `on_error` optional parameters.
   - Updated `BaseClient.on_message` with `ack_fn` and `nack_fn` optional parameters, routing them into `Router.route`.
2. **`src/postoffice/router.py`:**
   - Implemented `_FanoutCoordinator` to coordinate multi-target fanout publish confirmations with thread-safe atomic countdown and error short-circuiting.
   - Implemented `_translate_semantics` mapping MQTT QoS 0/1/2 to AMQP `delivery_mode` 1/2 and vice versa.
   - Updated `Router.route` to wire `coordinator` callbacks to target clients and preserve backward compatibility when no callbacks are supplied.

## Verification
- BaseClient signature verified with parameter inspection.
- Full test suite verified passing with zero regressions (`11 tests in 0.016s`).
