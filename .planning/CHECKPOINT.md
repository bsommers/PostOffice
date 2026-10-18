# Checkpoint — Milestone v2.1 Complete & Released (`v2.1.0`) — 2026-10-18

**Status:** MILESTONE v2.1 COMPLETE  
**Branch:** `main`  
**Release Tag:** `v2.1.0`  

---

## 1. Goal & Context

PostOffice has completed Milestone v1.1, Milestone v2.0, and Milestone v2.1. All 13 requirements across v1, v1.1 (`TEST-01..03`, `SEM-01..02`, `ROUT-01`, `OBS-01`, `DLQ-01`), v2.0 (`SCH-01`, `SEC-01`), and v2.1 (`RES-01`, `RES-02`, `RES-03`) are implemented, verified, documented, and tagged (`v2.1.0`).

---

## 2. Done in Milestone v2.1 (With Evidence)

- **Phase 7 (`RES-01`): Operational Resiliency & Client Queuing**
  - `BaseClient`: `BufferedPublish`, `BufferOverflowError`, thread-safe `_subscriptions` registry (`record_subscription`, `get_subscriptions`), and bounded `deque` `_publish_buffer` (`buffer_publish`, `flush_publish_buffer`) with `"reject"` and `"drop_oldest"` overflow policies.
  - `MqttClient`, `NanoMqClient`, `AmqpClient`, `KafkaClient`: Offline subscription kwargs preservation (`qos`, `exchange`, `queue`, `auto_ack`) and automatic publish buffer flushing upon connection establishment.
- **Phase 8 (`RES-02`, `RES-03`): Egress Backpressure, Circuit Breakers & Credential Hot-Reload**
  - `CircuitBreaker` (`src/postoffice/resiliency.py`): Thread-safe `CLOSED -> OPEN -> HALF_OPEN -> CLOSED` state machine integrated into `Router` with ring-buffer failover before DLQ drop and automatic buffer flush on probe recovery.
  - Live Security Hot-Reload: `reload_security()` across all 4 broker adapters, `PostOffice.reload_broker_security()`, `ControlPlane.update_broker_security()`, and `DataPlane` live Pub/Sub handler.
- **Test Suite Verification:**
  - Command: `PYTHONPATH=src .venv/bin/python -m unittest discover tests/`
  - Output: `Ran 94 tests in 0.545s -> OK` (100% passing hermetically across 14 test modules).
