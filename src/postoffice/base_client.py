from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
import logging
import threading
import time
from typing import Callable, Any, Optional, Dict, Deque, List

logger = logging.getLogger(__name__)


class BufferOverflowError(RuntimeError):
    """Raised when a client's offline publish buffer exceeds its maximum capacity."""
    pass


class BufferExpiredError(RuntimeError):
    """Raised (via on_error) when a buffered publish outlived max_publish_buffer_age."""
    pass


@dataclass
class BufferedPublish:
    topic: str
    message: bytes
    on_confirm: Optional[Callable[[], None]] = None
    on_error: Optional[Callable[[Exception], None]] = None
    kwargs: Dict[str, Any] = field(default_factory=dict)
    size: int = 0
    enqueued_at: float = 0.0


class BaseClient(ABC):
    VALID_OVERFLOW_POLICIES = ("reject", "drop_oldest")

    def __init__(
        self,
        name: str,
        router: Any,
        enable_publish_buffer: bool = False,
        max_publish_buffer_size: int = 1000,
        buffer_overflow_policy: str = "reject",
        max_publish_buffer_bytes: Optional[int] = 64 * 1024 * 1024,
        max_publish_buffer_age: Optional[float] = None,
        buffer_clock: Callable[[], float] = time.monotonic,
        **kwargs
    ):
        if buffer_overflow_policy not in self.VALID_OVERFLOW_POLICIES:
            raise ValueError(
                f"Invalid buffer_overflow_policy '{buffer_overflow_policy}'. "
                f"Must be one of {self.VALID_OVERFLOW_POLICIES}."
            )
        if max_publish_buffer_bytes is not None and max_publish_buffer_bytes <= 0:
            raise ValueError("max_publish_buffer_bytes must be > 0 (or None for no byte limit).")
        if max_publish_buffer_age is not None and max_publish_buffer_age <= 0:
            raise ValueError("max_publish_buffer_age must be > 0 (or None for no age limit).")
        self.name = name
        self.router = router
        self.enable_publish_buffer = enable_publish_buffer
        self.max_publish_buffer_size = max_publish_buffer_size
        self.buffer_overflow_policy = buffer_overflow_policy
        self.max_publish_buffer_bytes = max_publish_buffer_bytes
        self.max_publish_buffer_age = max_publish_buffer_age
        self._buffer_clock = buffer_clock
        self._publish_buffer_bytes = 0
        self._queue_lock = threading.RLock()
        self._subscriptions: Dict[str, Dict[str, Any]] = {}
        self._publish_buffer: Deque[BufferedPublish] = deque()

    @property
    def publish_buffer_size(self) -> int:
        with self._queue_lock:
            return len(self._publish_buffer)

    @property
    def publish_buffer_bytes(self) -> int:
        with self._queue_lock:
            return self._publish_buffer_bytes

    def record_subscription(self, topic: str, **kwargs) -> None:
        """Thread-safely records a topic subscription and its broker-specific kwargs for replay."""
        with self._queue_lock:
            self._subscriptions[topic] = dict(kwargs)

    def get_subscriptions(self) -> Dict[str, Dict[str, Any]]:
        """Returns a thread-safe snapshot of registered subscriptions and their kwargs."""
        with self._queue_lock:
            return {t: dict(kw) for t, kw in self._subscriptions.items()}

    def _pop_buffered_locked(self) -> BufferedPublish:
        item = self._publish_buffer.popleft()
        self._publish_buffer_bytes -= item.size
        return item

    def _is_stale(self, item: BufferedPublish, now: float) -> bool:
        age = self.max_publish_buffer_age
        return age is not None and now - item.enqueued_at > age

    def _expire_locked(self, now: float) -> List[BufferedPublish]:
        """Removes stale items from the front (the buffer is FIFO, so the oldest are there)."""
        expired = []
        while self._publish_buffer and self._is_stale(self._publish_buffer[0], now):
            expired.append(self._pop_buffered_locked())
        return expired

    def _notify_expired(self, items: List[BufferedPublish]) -> None:
        for item in items:
            logger.warning(f"{self.name}: Discarded buffered publish to {item.topic}; older than {self.max_publish_buffer_age}s.")
            if item.on_error:
                item.on_error(BufferExpiredError(
                    f"{self.name}: Buffered message on {item.topic} expired after {self.max_publish_buffer_age}s."
                ))

    def buffer_publish(
        self,
        topic: str,
        message: bytes,
        on_confirm: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        **kwargs
    ) -> bool:
        """
        Buffers an outgoing publish when the client is disconnected if buffering is enabled.
        The buffer is bounded by message count, total bytes and (optionally) age.
        Returns True if the publish was handled (buffered or rejected by overflow policy),
        or False if publish buffering is disabled.
        """
        if not self.enable_publish_buffer or self.max_publish_buffer_size <= 0:
            return False

        size = len(message)
        max_bytes = self.max_publish_buffer_bytes
        evicted: List[BufferedPublish] = []
        rejected_exc: Optional[BufferOverflowError] = None

        with self._queue_lock:
            now = self._buffer_clock()
            expired = self._expire_locked(now)
            if max_bytes is not None and size > max_bytes:
                # Evicting everything would still not make room, so keep what is already buffered.
                rejected_exc = BufferOverflowError(
                    f"{self.name}: Message of {size} bytes exceeds the publish buffer byte budget ({max_bytes}); rejecting publish to {topic}."
                )
            else:
                while rejected_exc is None:
                    count_full = len(self._publish_buffer) >= self.max_publish_buffer_size
                    bytes_full = max_bytes is not None and self._publish_buffer_bytes + size > max_bytes
                    if not (count_full or bytes_full):
                        break
                    if self.buffer_overflow_policy == "drop_oldest" and self._publish_buffer:
                        evicted.append(self._pop_buffered_locked())
                    elif count_full:
                        rejected_exc = BufferOverflowError(
                            f"{self.name}: Publish buffer full ({self.max_publish_buffer_size} messages); rejecting publish to {topic}."
                        )
                    else:
                        rejected_exc = BufferOverflowError(
                            f"{self.name}: Publish buffer byte budget ({max_bytes}) exceeded; rejecting publish to {topic}."
                        )

            if rejected_exc is None:
                self._publish_buffer.append(
                    BufferedPublish(
                        topic=topic,
                        message=message,
                        on_confirm=on_confirm,
                        on_error=on_error,
                        kwargs=dict(kwargs),
                        size=size,
                        enqueued_at=now,
                    )
                )
                self._publish_buffer_bytes += size

        self._notify_expired(expired)
        for item in evicted:
            if item.on_error:
                item.on_error(
                    BufferOverflowError(
                        f"{self.name}: Dropped oldest message on {item.topic} due to publish buffer overflow."
                    )
                )

        if rejected_exc is not None:
            logger.warning(str(rejected_exc))
            if on_error:
                on_error(rejected_exc)

        return True

    def flush_publish_buffer(self) -> int:
        """Flushes buffered publishes in FIFO order, discarding stale ones. Returns number flushed."""
        flushed = 0
        while True:
            with self._queue_lock:
                if not self._publish_buffer:
                    break
                item = self._pop_buffered_locked()
                stale = self._is_stale(item, self._buffer_clock())
            if stale:
                self._notify_expired([item])
                continue
            self.publish(
                item.topic,
                item.message,
                on_confirm=item.on_confirm,
                on_error=item.on_error,
                **item.kwargs
            )
            flushed += 1
        return flushed

    @abstractmethod
    def connect(self):
        """Connect to the messaging system."""
        pass

    @abstractmethod
    def disconnect(self):
        """Disconnect from the messaging system."""
        pass

    @abstractmethod
    def subscribe(self, topic: str, **kwargs):
        """Subscribe to a topic/queue/channel with optional broker-specific params."""
        pass

    @abstractmethod
    def publish(
        self,
        topic: str,
        message: bytes,
        on_confirm: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        **kwargs
    ):
        """Publish a message to a topic/queue/channel with optional broker-specific params and delivery confirmation callbacks."""
        pass

    def reload_security(self, security_config: Any) -> None:
        """Hot-reloads TLS certificates and authentication credentials without losing subscriptions or buffered publishes."""
        pass

    def pause_consumption(self) -> None:
        """Back-pressure hook: stop pulling new messages from the broker. Default: not supported."""
        pass

    def resume_consumption(self) -> None:
        """Back-pressure hook: resume pulling messages after pause_consumption."""
        pass

    def set_inflight_limit(self, max_inflight: Optional[int]) -> None:
        """Hook to push the router's in-flight cap to the broker natively (e.g. AMQP prefetch)."""
        pass

    def on_message(
        self,
        topic: str,
        message: bytes,
        ack_fn: Optional[Callable[[], None]] = None,
        nack_fn: Optional[Callable[[bool], None]] = None,
        **kwargs
    ):
        """Callback when a message is received from the underlying broker."""
        self.router.route(self.name, topic, message, ack_fn=ack_fn, nack_fn=nack_fn, **kwargs)


