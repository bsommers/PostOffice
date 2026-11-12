# Phase 8: Egress Backpressure, Circuit Breakers & Credential Hot-Reload — Context & Decisions

**Phase:** 08-backpressure-circuit-breakers-hot-reload
**Milestone:** v2.1
**Requirements:** RES-02, RES-03
**Created:** 2026-10-17

---

## 1. Background & Scope

Phase 7 established per-client subscription replay and bounded offline publish buffers (`RES-01`). Phase 8 completes Milestone v2.1 by delivering two critical production resiliency capabilities:
1. **Egress Circuit Breakers & Backpressure (`RES-02`):** Protect the routing engine from cascading stalls or repeated synchronous/asynchronous publish failures when a target broker is degraded. Before dropping messages to the Dead Letter Queue (DLQ), messages can be absorbed into the target client's bounded publish ring buffer while the circuit breaker is open, and automatically drained when the circuit transitions through `HALF_OPEN` back to `CLOSED`.
2. **Dynamic In-Flight TLS Certificate & Credential Hot-Reload (`RES-03`):** Allow operators and the Redis `ControlPlane` to rotate TLS certificates (`ca_cert`, `client_cert`, `client_key`) and authentication credentials (`username`, `password`, `sasl_mechanism`) on active brokers without restarting `PostOffice` or `DataPlane` workers, preserving all active subscriptions and buffered publishes.

---

## 2. Locked Decisions

### Decision 1: Per-Broker Egress Circuit Breaker (`CircuitBreaker`)
- **State Machine (`src/postoffice/resiliency.py`):**
  - States: `CLOSED` (normal operation), `OPEN` (tripped after `failure_threshold` consecutive publish failures), `HALF_OPEN` (after `recovery_timeout` seconds have elapsed, permits a single probe publish).
  - Configurable parameters (global or per-broker):
    - `failure_threshold: int = 5`
    - `recovery_timeout: float = 30.0`
- **Router Integration & Backpressure Failover (`src/postoffice/router.py`):**
  - Each registered target broker can have a `CircuitBreaker` (managed by `Router` or configured via `router.configure_circuit_breaker(broker_name, failure_threshold=..., recovery_timeout=...)` / `po.add_broker(..., circuit_breaker_threshold=..., circuit_breaker_timeout=...)`).
  - When routing to `target_client`:
    - Check `breaker.allow_request()`.
    - If `False` (circuit is `OPEN`):
      - Record metric `circuit_breaker_open`.
      - **Ring-Buffer Failover before DLQ Drop:** If `target_client_obj.enable_publish_buffer` is `True`, attempt `target_client_obj.buffer_publish(target_topic, out_message, on_confirm=..., on_error=..., **publish_kwargs)`. If buffered without overflow, the message is held in the ring buffer instead of immediately dropping to DLQ! If buffering is disabled or overflows, route to DLQ (or NACK).
    - If `True` (`CLOSED` or `HALF_OPEN` probe):
      - Wrap `on_confirm` to call `breaker.record_success()` (and if transitioning `HALF_OPEN -> CLOSED`, trigger `target_client_obj.flush_publish_buffer()`).
      - Wrap `on_error` (and synchronous exceptions) to call `breaker.record_failure()`.

### Decision 2: Zero-Downtime Credential & TLS Hot-Reload (`RES-03`)
- **Plugin Contract (`BaseClient.reload_security(security_config)`):**
  - Defined on `BaseClient` and implemented across `MqttClient` (and `NanoMqClient`), `AmqpClient`, and `KafkaClient`.
  - Updates `self.security = ensure_security_config(security_config)`.
  - If the client is currently connected/running, re-applies TLS contexts and credentials (`tls_set`/`username_pw_set` for MQTT, reconnects channel/connection for AMQP, recreates Producer/Consumer for Kafka) and seamlessly replays `self.get_subscriptions()` and `self.flush_publish_buffer()` without losing state.
- **Facade & Distributed Control Plane Integration:**
  - `PostOffice.reload_broker_security(name: str, security_config: Union[SecurityConfig, Dict[str, Any]])` invokes `self.brokers[name].reload_security(security_config)`.
  - `ControlPlane.update_broker_security(name: str, security_config: Union[SecurityConfig, Dict[str, Any]])` updates the stored broker JSON in Redis (`postoffice:brokers`) and broadcasts `{"action": "update_broker_security", "data": {"name": name, "security_config": ...}}` over `postoffice:config_updates`.
  - `DataPlane._process_update_message()` handles `"update_broker_security"` and calls `self.po.reload_broker_security(...)`.

---

## 3. Plan Decomposition

- **Plan 08-01 (Wave 1):** Egress Circuit Breaker, Ring-Buffer Failover & Router Backpressure (`RES-02`)
  - Create `src/postoffice/resiliency.py` with thread-safe `CircuitBreaker`, `CircuitState`, and `CircuitBreakerOpenError`.
  - Integrate circuit breaker tracking, ring-buffer failover before DLQ drop, and Prometheus metrics into `Router` (`src/postoffice/router.py`) and `PostOffice` (`src/postoffice/app.py`).
  - Hermetic unit tests in `tests/test_circuit_breaker.py`.

- **Plan 08-02 (Wave 2):** Dynamic TLS Certificate & Credential Hot-Reload across Plugins, Facade & Control Plane (`RES-03`)
  - Implement `reload_security()` in `BaseClient`, `MqttClient`, `NanoMqClient`, `AmqpClient`, and `KafkaClient`.
  - Implement `PostOffice.reload_broker_security()`, `ControlPlane.update_broker_security()`, and `DataPlane` live update handling.
  - Hermetic unit tests in `tests/test_credential_reload.py`.
