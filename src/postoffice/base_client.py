from abc import ABC, abstractmethod
from typing import Callable, Any, Optional

class BaseClient(ABC):
    def __init__(self, name: str, router: Any):
        self.name = name
        self.router = router

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

