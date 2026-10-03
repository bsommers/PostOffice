from abc import ABC, abstractmethod
from typing import Callable, Any

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
    def subscribe(self, topic: str):
        """Subscribe to a topic/queue/channel."""
        pass

    @abstractmethod
    def publish(self, topic: str, message: bytes):
        """Publish a message to a topic/queue/channel."""
        pass

    def on_message(self, topic: str, message: bytes):
        """Callback when a message is received."""
        self.router.route(self.name, topic, message)
