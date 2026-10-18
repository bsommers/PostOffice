# Phase 7: Operational Resiliency & Client Queuing — Research & Specifications

**Phase:** 07-operational-resiliency-client-queuing
**Date:** 2026-10-17
**Tags:** resiliency, queuing, backpressure, mqtt, amqp, kafka, nanomq, threading

---

## 1. Current State Analysis across Protocol Adapters

### 1.1 `BaseClient` (`src/postoffice/base_client.py`)
Currently a minimal abstract class with `name`, `router`, and abstract methods `connect`, `disconnect`, `subscribe`, `publish`, plus `on_message`. It has no shared state for subscriptions or offline publish buffering.

### 1.2 `MqttClient` & `NanoMqClient` (`src/postoffice/plugins/mqtt_client.py`)
- **Subscriptions:** Stores only topic names in `self.subscribed_topics: List[str]`. In `subscribe(topic, **kwargs)`, `qos = kwargs.get('qos', 0)` is used if immediately connected, but in `_on_connect`, `self.client.subscribe(topic)` is called **without** `qos`, resetting QoS 1/2 subscriptions to QoS 0 on reconnect.
- **Publishes:** Checks `if not self.client.is_connected():` and immediately invokes `on_error(RuntimeError(...))`.

### 1.3 `AmqpClient` (`src/postoffice/plugins/amqp_client.py`)
- **Subscriptions:** Checks `if not self.channel: return`. Because `PostOffice.subscribe()` or `DataPlane.sync_state()` is often called before `PostOffice.start()`, `self.channel` is `None` at subscription registration time, causing pre-start AMQP subscriptions to be silently dropped!
- **Publishes:** Checks `if not self.channel:` and immediately fails. When connected, schedules `_publish` via `self.connection.add_callback_threadsafe(_publish)`.

### 1.4 `KafkaClient` (`src/postoffice/plugins/kafka_client.py`)
- **Subscriptions:** Checks `if not self.consumer: return`. Again, if `po.subscribe("kafka_1", "topic")` is called before `po.start()`, `self.consumer` is `None` and the subscription is dropped!
- **Publishes:** Checks `if not self.producer:` and fails if disconnected.

---

## 2. Unified Client Queuing Architecture (`BaseClient`)

### 2.1 Data Structures
```python
from collections import deque
from dataclasses import dataclass, field
import threading
from typing import Callable, Any, Optional, Dict, Deque, Tuple

class BufferOverflowError(RuntimeError):
    """Raised when the client's offline publish buffer exceeds capacity."""
    pass

@dataclass
class BufferedPublish:
    topic: str
    message: bytes
    on_confirm: Optional[Callable[[], None]] = None
    on_error: Optional[Callable[[Exception], None]] = None
    kwargs: Dict[str, Any] = field(default_factory=dict)
```

### 2.2 `BaseClient` State & Methods
- `self._queue_lock = threading.RLock()`
- `self._subscriptions: Dict[str, Dict[str, Any]] = {}` (maps `topic -> kwargs`)
- `self.enable_publish_buffer: bool = enable_publish_buffer`
- `self.max_publish_buffer_size: int = max_publish_buffer_size`
- `self.buffer_overflow_policy: str = buffer_overflow_policy` (`"reject"` or `"drop_oldest"`)
- `self._publish_buffer: Deque[BufferedPublish] = deque()`

#### Helper Methods on `BaseClient`:
1. `record_subscription(topic: str, **kwargs) -> None`:
   Thread-safely stores `self._subscriptions[topic] = dict(kwargs)`.
2. `get_subscriptions() -> Dict[str, Dict[str, Any]]`:
   Returns a thread-safe snapshot copy of `self._subscriptions`.
3. `buffer_publish(topic: str, message: bytes, on_confirm=None, on_error=None, **kwargs) -> bool`:
   - If `not self.enable_publish_buffer` or `self.max_publish_buffer_size <= 0`: returns `False` (caller handles immediate disconnect error).
   - Acquires `self._queue_lock`.
   - If `len(self._publish_buffer) >= self.max_publish_buffer_size`:
     - If `self.buffer_overflow_policy == "drop_oldest"`:
       pops leftmost `BufferedPublish` and invokes its `on_error(BufferOverflowError(...))` outside lock or safely.
     - Else (`"reject"`):
       invokes `on_error(BufferOverflowError(...))` and returns `True` (handled).
   - Appends `BufferedPublish(topic, message, on_confirm, on_error, kwargs)` to `self._publish_buffer` and returns `True`.
4. `flush_publish_buffer() -> int`:
   - Repeatedly pops from `self._publish_buffer` and calls `self.publish(item.topic, item.message, on_confirm=item.on_confirm, on_error=item.on_error, **item.kwargs)`.
   - Returns count of flushed messages.

---

## 3. Thread Safety & Delivery Guarantees (`_FanoutCoordinator` Interaction)

When a message is buffered in `_publish_buffer` while an egress broker is temporarily offline:
- `on_confirm` and `on_error` callbacks attached by `Router` (`_FanoutCoordinator`) are preserved inside `BufferedPublish`.
- Upstream acknowledgement (`ack_fn`) is **held** until the target broker reconnects, flushes the buffered publish, and confirms delivery (satisfying `SEM-01` zero-message-loss guarantees).
- If the buffer overflows, `on_error(BufferOverflowError(...))` triggers `_FanoutCoordinator.on_error`, which routes the message to the Dead Letter Queue (`DLQ-01`) or NACKs upstream.
