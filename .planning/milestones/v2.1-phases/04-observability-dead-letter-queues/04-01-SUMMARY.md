---
phase: 04-observability-dead-letter-queues
plan: 01
subsystem: observability
tags:
  - prometheus
  - metrics
  - monitoring
  - http-server
  - unittest

requires:
  - 03-01-PLAN.md
provides:
  - MetricsManager encapsulating Prometheus counters, histograms, and HTTP scraping server
  - Latency and routed status tracking in Router
  - Metrics configuration and lifecycle in PostOffice
  - Hermetic unit tests in tests/test_metrics.py
affects:
  - 04-02-PLAN.md
  - Router
  - PostOffice

tech-stack:
  added:
    - prometheus-client 0.26.0
  patterns:
    - Dedicated CollectorRegistry per MetricsManager instance to guarantee hermetic test isolation
    - Clean WSGIServer start/stop lifecycle management

key-files:
  created:
    - src/postoffice/metrics.py
    - tests/test_metrics.py
  modified:
    - src/postoffice/router.py
    - src/postoffice/app.py
    - requirements.txt

key-decisions:
  - "Used custom CollectorRegistry in MetricsManager so unit tests do not leak metrics into global registries"
  - "Added prometheus_client HTTP server lifecycle management (start_server, stop_server) to PostOffice facade"
  - "Instrumented Router with message duration histogram and routed/error counters"

patterns-established:
  - "Router records postoffice_messages_routed_total on every target publish (success or error)"
  - "Router records postoffice_routing_duration_seconds on every ingress route invocation"
  - "PostOffice starts metrics server on start() if metrics_port is set, and stops it cleanly on stop()/reset()"

requirements-completed:
  - OBS-01

coverage:
  - id: D1
    description: "Prometheus metrics instrumentation for messages routed, routing errors, and latency"
    requirement: "OBS-01"
    verification:
      - kind: unit
        ref: "tests/test_metrics.py"
        status: pass
    human_judgment: false
---

# Plan 04-01 Summary

Implemented Prometheus metrics instrumentation in `src/postoffice/metrics.py`, integrated collectors into `Router` and `PostOffice`, and verified metrics generation with hermetic unit tests.

## Key Changes
1. **`src/postoffice/metrics.py`:**
   - Created `MetricsManager` with Prometheus `CollectorRegistry`.
   - Defined collectors:
     - `postoffice_messages_routed_total`: Counter `['source_broker', 'target_broker', 'status']`
     - `postoffice_routing_errors_total`: Counter `['source_broker', 'error_type']`
     - `postoffice_routing_duration_seconds`: Histogram `['source_broker']`
     - `postoffice_dlq_messages_total`: Counter `['source_broker', 'dlq_broker', 'dlq_topic']`
   - Added `get_metrics_text()`, `start_server(port)`, and `stop_server()`.
2. **`src/postoffice/router.py`:**
   - Integrated `MetricsManager`.
   - Recorded route durations and routed counters on success/error.
3. **`src/postoffice/app.py`:**
   - Supported `metrics_port` parameter in `PostOffice.__init__` and `set_metrics_port()`.
   - Managed metrics server lifecycle in `start()`, `stop()`, and `reset()`.
4. **`tests/test_metrics.py`:**
   - 4 hermetic unit tests verifying metrics collection, router integration, and HTTP server lifecycle.

## Verification
- `PYTHONPATH=src .venv/bin/python -m unittest tests/test_metrics.py` (4 tests in 0.504s, OK)
- `PYTHONPATH=src .venv/bin/python -m unittest discover tests/` (35 tests in 0.522s, OK)
