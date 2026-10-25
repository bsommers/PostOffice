from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
import logging
import threading
from typing import Callable, Any, Optional, Dict, Deque

logger = logging.getLogger(__name__)


class BufferOverflowError(RuntimeError):
    """Raised when a client's offline publish buffer exceeds its maximum capacity."""
    pass


@dataclass
class BufferedPublish:
    topic: str
    message: bytes
    on_confirm: Optional[Callable[[], None]] = None
    on_error: Optional[Callable[[Exception], None]] = None
    kwargs: Dict[str, Any] = field(default_factory=dict)


class BaseClient(ABC):
    VALID_OVERFLOW_POLICIES = ("reject", "drop_oldest")

    def __init__(
        self,
        name: str,
        router: Any,
        enable_publish_buffer: bool = False,
        max_publish_buffer_size: int = 1000,
        buffer_overflow_policy: str = "reject",
        **kwargs
    ):
        if buffer_overflow_policy not in self.VALID_OVERFLOW_POLICIES:
            raise ValueError(
                f"Invalid buffer_overflow_policy '{buffer_overflow_policy}'. "
                f"Must be one of {self.VALID_OVERFLOW_POLICIES}."
            )
        self.name = name
        self.router = router
        self.enable_publish_buffer = enable_publish_buffer
        self.max_publish_buffer_size = max_publish_buffer_size
        self.buffer_overflow_policy = buffer_overflow_policy
        self._queue_lock = threading.RLock()
        self._subscriptions: Dict[str, Dict[str, Any]] = {}
        self._publish_buffer: Deque[BufferedPublish] = deque()

    @property
    def publish_buffer_size(self) -> int:
        with self._queue_lock:
            return len(self._publish_buffer)

    def record_subscription(self, topic: str, **kwargs) -> None:
        """Thread-safely records a topic subscription and its broker-specific kwargs for replay."""
        with self._queue_lock:
            self._subscriptions[topic] = dict(kwargs)

    def get_subscriptions(self) -> Dict[str, Dict[str, Any]]:
        """Returns a thread-safe snapshot of registered subscriptions and their kwargs."""
        with self._queue_lock:
            return {t: dict(kw) for t, kw in self._subscriptions.items()}

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
        Returns True if the publish was handled (buffered or rejected by overflow policy),
        or False if publish buffering is disabled.
        """
        if not self.enable_publish_buffer or self.max_publish_buffer_size <= 0:
            return False

        evicted: Optional[BufferedPublish] = None
        rejected_exc: Optional[BufferOverflowError] = None

        with self._queue_lock:
            if len(self._publish_buffer) >= self.max_publish_buffer_size:
                if self.buffer_overflow_policy == "drop_oldest":
                    evicted = self._publish_buffer.popleft()
                else:
                    rejected_exc = BufferOverflowError(
                        f"{self.name}: Publish buffer full ({self.max_publish_buffer_size} messages); rejecting publish to {topic}."
                    )

            if rejected_exc is None:
                self._publish_buffer.append(
                    BufferedPublish(
                        topic=topic,
                        message=message,
                        on_confirm=on_confirm,
                        on_error=on_error,
                        kwargs=dict(kwargs),
                    )
                )

        if evicted is not None and evicted.on_error:
            evicted.on_error(
                BufferOverflowError(
                    f"{self.name}: Dropped oldest message on {evicted.topic} due to publish buffer overflow."
                )
            )

        if rejected_exc is not None:
            logger.warning(str(rejected_exc))
            if on_error:
                on_error(rejected_exc)

        return True

    def flush_publish_buffer(self) -> int:
        """Flushes all buffered publishes in FIFO order. Returns number of flushed messages."""
        flushed = 0
        while True:
            with self._queue_lock:
                if not self._publish_buffer:
                    break
                item = self._publish_buffer.popleft()
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


