# Phase 7: Operational Resiliency & Client Queuing — Context & Decisions

**Phase:** 07-operational-resiliency-client-queuing
**Milestone:** v2.1
**Requirements:** RES-01
**Created:** 2026-10-17

---

## 1. Background & Problem Statement

In distributed multi-broker environments, broker connections frequently initialize asynchronously or experience transient network drops. Furthermore, in PostOffice's declarative API (`po.add_broker()`, `po.subscribe()`, `po.start()`), callers and `DataPlane.sync_state()` routinely invoke `subscribe()` before `start()` (or while a broker is still establishing its connection) and may publish messages during momentary broker disconnects.

Currently, offline subscription and publish behavior is fragmented across adapters:
- **`MqttClient` / `NanoMqClient`**: Stores topic strings in `self.subscribed_topics`, losing `qos` parameters upon reconnect (`_on_connect`), and immediately rejects `publish()` calls when disconnected.
- **`AmqpClient`**: Rejects `subscribe()` when `self.channel is None` (breaking pre-start `po.subscribe()` registrations!) and immediately rejects `publish()` when disconnected.
- **`KafkaClient`**: Rejects `subscribe()` when `self.consumer is None` (also breaking pre-start `po.subscribe()` registrations!) and immediately rejects `publish()` when `self.producer is None`.

Phase 7 (`RES-01`) unifies and hardens offline subscription queuing and thread-safe bounded in-flight publish buffering across `BaseClient` and all four protocol plugins.

---

## 2. Locked Decisions

### Decision 1: Standardized Subscription Registry in `BaseClient`
- **Mechanism:** `BaseClient` maintains a thread-safe subscription registry `_subscriptions: Dict[str, Dict[str, Any]]` protected by `self._queue_lock = threading.Lock()`, mapping `topic -> kwargs`.
- **Replay Contract:** Calling `subscribe(topic, **kwargs)` records `(topic, kwargs)` regardless of current connection state. If the client is currently connected, it applies the subscription immediately; if disconnected, it logs that the subscription is queued and replays all registered subscriptions (preserving `qos`, `exchange`, `queue`, `auto_ack`, etc.) via `_replay_subscriptions()` as soon as connection/channel establishment completes.

### Decision 2: Thread-Safe Bounded Publish Buffer with Overflow Policy
- **Mechanism:** `BaseClient` provides a bounded `collections.deque` (`_publish_buffer`) configured via `max_publish_buffer_size: int = 1000` and `buffer_overflow_policy: str = "reject"` (`"reject"` | `"drop_oldest"`).
- **Buffering Behavior:**
  - When `buffer_when_disconnected: bool = True` (configurable per broker, default `False` for strict immediate error callback compatibility unless enabled or configured, OR enabled when `max_publish_buffer_size > 0` — see compatibility note below):
  - **Backward Compatibility & Explicit Control:** Existing unit tests verify that publishing to a disconnected client calls `on_error` when buffering is disabled (`max_publish_buffer_size=0` or `enable_publish_buffer=False`). To maintain 100% backward compatibility with existing tests while enabling resilient buffering:
    - Support `enable_publish_buffer: bool = False` and `max_publish_buffer_size: int = 1000`, `buffer_overflow_policy: str = "reject"` on `BaseClient` (and passed through `add_broker` / plugin `__init__`).
    - Additionally, `enqueue_publish(topic, message, on_confirm, on_error, **kwargs)` and automatic buffering in `publish()` when `enable_publish_buffer=True` seamlessly queue in-flight messages while disconnected and flush them in FIFO order (`_flush_publish_buffer()`) upon connection / reconnection!
    - When the buffer is full:
      - `"reject"`: Invokes `on_error(BufferOverflowError(...))` immediately without dropping existing buffered messages.
      - `"drop_oldest"`: Evicts the oldest buffered item (invoking its `on_error(BufferOverflowError("Dropped oldest message due to publish buffer overflow"))`) and appends the new message.

### Decision 3: Flush & Replay Lifecycle Hooks
- **`MqttClient` / `NanoMqClient`**: `_on_connect` replays `self._subscriptions` with stored `qos` and flushes `_publish_buffer`.
- **`AmqpClient`**: `_consume_loop` after `self.channel = self.connection.channel()` replays `self._subscriptions` and flushes `_publish_buffer` on the connection thread before entering `process_data_events`.
- **`KafkaClient`**: `subscribe()` queues into `_subscriptions` and `_pending_subscriptions` even before `connect()` creates `self.consumer`. Once `connect()` initializes `self.producer` and `self.consumer`, `_flush_publish_buffer()` drains queued messages and `_consume_loop()` applies pending subscriptions.

---

## 3. Plan Decomposition

- **Plan 07-01 (Wave 1):** `BaseClient` Queuing Engine & `MqttClient`/`NanoMqClient` Integration
  - Implement subscription registry, bounded publish buffer (`collections.deque`), overflow policies (`reject`, `drop_oldest`), and flush/replay helpers in `src/postoffice/base_client.py`.
  - Refactor `MqttClient` and `NanoMqClient` to use `BaseClient` subscription replay (preserving `qos` across reconnects) and publish buffer flushing on `_on_connect`.
  - Unit tests in `tests/test_client_queuing.py`.

- **Plan 07-02 (Wave 2):** `AmqpClient` & `KafkaClient` Offline Queuing, Reconnect Replay & E2E Mesh Verification
  - Refactor `AmqpClient` and `KafkaClient` to support pre-connect `subscribe()` queuing, thread-safe publish buffering, and automatic replay/flush on connection establishment.
  - Ensure `DataPlane.sync_state()` and `PostOffice` work seamlessly when subscriptions are registered before `po.start()`.
  - Expand `tests/test_client_queuing.py` and verify full regression suite.
