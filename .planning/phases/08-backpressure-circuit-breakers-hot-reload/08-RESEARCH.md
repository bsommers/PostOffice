# Phase 8: Egress Backpressure, Circuit Breakers & Credential Hot-Reload — Research & Specifications

**Phase:** 08-backpressure-circuit-breakers-hot-reload
**Date:** 2026-10-17
**Tags:** circuit-breaker, backpressure, hot-reload, tls, security, resiliency

---

## 1. Egress Circuit Breaker State Machine (`RES-02`)

### 1.1 State Transitions
1. **`CLOSED`**:
   - Default initial state.
   - `allow_request()` returns `True`.
   - `record_success()` resets `failure_count = 0`.
   - `record_failure()` increments `failure_count`. When `failure_count >= failure_threshold`, state transitions to `OPEN` and records `opened_at = time_fn()`.
2. **`OPEN`**:
   - `allow_request()` checks if `time_fn() - opened_at >= recovery_timeout`.
   - If elapsed: transitions to `HALF_OPEN` and returns `True` (allowing a probe request).
   - If not elapsed: returns `False`.
3. **`HALF_OPEN`**:
   - `record_success()` transitions to `CLOSED`, resets `failure_count = 0`.
   - `record_failure()` transitions immediately back to `OPEN` and resets `opened_at = time_fn()`.

### 1.2 Ring-Buffer Failover Before DLQ Drop
When a target broker's circuit breaker is `OPEN`:
- Instead of immediately sending every incoming message to the Dead Letter Queue (DLQ), if the target broker has `enable_publish_buffer=True`, `Router` diverts the message into `target_client.buffer_publish(...)`.
- If the target's ring buffer is configured with `buffer_overflow_policy="drop_oldest"` or `"reject"`, only overflowed messages trigger `on_error(BufferOverflowError)` and fall back to the DLQ!
- When the circuit breaker probe succeeds (`HALF_OPEN -> CLOSED`), `Router` invokes `target_client.flush_publish_buffer()`, draining the queued messages to the recovered broker.

---

## 2. Dynamic Credential & Certificate Hot-Reload (`RES-03`)

### 2.1 Driver-Specific Reload Mechanics
- **`MqttClient` & `NanoMqClient`**:
  - Update `self.security = ensure_security_config(security_config)`.
  - Update `username_pw_set` and `tls_set` (if TLS enabled) on `self.client` or recreate `self.client` if TLS context already locked by paho-mqtt, reconnecting (`self.client.reconnect()` or `disconnect()` + `connect()`) if currently active.
  - Because `self.get_subscriptions()` and `self._publish_buffer` are stored on `BaseClient`, `_on_connect` automatically restores all active subscriptions and flushes buffered messages.
- **`AmqpClient`**:
  - Update `self.security`, `self.user`, `self.password`, and `self.port` (if port shift applies).
  - If `self.thread` and `self.thread.is_alive()` (or `self.connection` is active):
    - `self.disconnect()`
    - Reset `self._stop_event.clear()` and `self._connected_event.clear()`
    - `self.connect()` (which replays `self.get_subscriptions()` and flushes `self._publish_buffer`).
- **`KafkaClient`**:
  - Update `self.security`.
  - If `self.producer` or `self.consumer` is active:
    - `self.disconnect()`
    - Reset `self._stop_event.clear()`
    - Move `self.subscribed_topics` back into `self._pending_subscriptions` so the new consumer subscribes to all existing topics.
    - `self.connect()`.
