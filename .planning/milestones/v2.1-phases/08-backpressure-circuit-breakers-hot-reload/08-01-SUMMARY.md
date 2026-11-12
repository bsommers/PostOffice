---
phase: 08-backpressure-circuit-breakers-hot-reload
plan: 01
subsystem: resiliency-circuit-breaker
tags: [resiliency, circuit-breaker, backpressure, router, dlq]
requires:
  - 07-02
provides:
  - "Thread-safe CircuitBreaker state machine (CLOSED, OPEN, HALF_OPEN) and CircuitBreakerOpenError"
  - "Per-broker egress circuit breaking in Router with ring-buffer failover before DLQ drop"
  - "Automatic publish buffer flush upon HALF_OPEN -> CLOSED recovery"
affects:
  - src/postoffice/resiliency.py
  - src/postoffice/router.py
  - src/postoffice/app.py
  - tests/test_circuit_breaker.py
tech-stack:
  added: []
  patterns:
    - "Three-state CircuitBreaker with single-probe HALF_OPEN recovery and ring-buffer absorption"
key-files:
  created:
    - src/postoffice/resiliency.py
    - tests/test_circuit_breaker.py
  modified:
    - src/postoffice/router.py
    - src/postoffice/app.py
decisions:
  - "When a target broker's circuit is OPEN, attempt target.buffer_publish() before falling back to DLQ so transient outages are absorbed in-memory up to max_publish_buffer_size."
  - "Flush the target client's publish buffer automatically when a HALF_OPEN probe publish succeeds and closes the circuit."
metrics:
  duration: 10m
  completed_date: "2026-10-18"
requirements:
  - RES-02
---

# Phase 8 Plan 01: Egress Circuit Breaker & Router Backpressure Summary

## Accomplishments
- Created `src/postoffice/resiliency.py` implementing thread-safe `CircuitBreaker`, `CircuitState` (`CLOSED`, `OPEN`, `HALF_OPEN`), and `CircuitBreakerOpenError`.
- Integrated per-broker egress circuit breakers into `Router` and exposed `configure_circuit_breaker()` (plus optional `circuit_breaker_threshold` and `circuit_breaker_timeout` in `add_broker()`) on `PostOffice`.
- Implemented ring-buffer failover before DLQ drop when a target broker's circuit is `OPEN`, and automatic buffer flushing upon `HALF_OPEN -> CLOSED` probe recovery.
- Verified with hermetic unit and integration tests in `tests/test_circuit_breaker.py`.
