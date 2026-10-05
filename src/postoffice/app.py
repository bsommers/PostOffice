import logging
from typing import Dict, Any, Optional
from postoffice.router import Router
from postoffice.registry import ClientRegistry

# Import plugins to trigger registration
import postoffice.plugins

logger = logging.getLogger(__name__)

class PostOffice:
    """
    Uniform Interface Layer (Facade) for configuring and starting the multi-protocol router.
    Clients interact strictly with this class and do not need to know about specific broker implementations.
    """
    def __init__(self):
        self.router = Router()
        self.brokers: Dict[str, Any] = {}
        self.is_running = False

    def add_broker(self, name: str, protocol: str, **kwargs) -> None:
        """
        Registers a new messaging broker into the PostOffice network.

        :param name: Unique internal identifier for the broker (e.g. 'mqtt_1')
        :param protocol: The protocol type ('mqtt', 'amqp', 'kafka', 'nanomq')
        :param kwargs: Broker specific connection params (e.g. host, port, bootstrap_servers, user)
        """
        if name in self.brokers:
            raise ValueError(f"Broker with name '{name}' already exists.")

        logger.info(f"Adding broker '{name}' via protocol '{protocol}'")
        client = ClientRegistry.create_client(protocol, name, self.router, **kwargs)
        self.router.register_client(client)
        self.brokers[name] = client

        # If the PostOffice has already started, immediately connect newly injected brokers
        if self.is_running:
            try:
                client.connect()
            except Exception as e:
                logger.error(f"Failed to dynamically connect broker '{name}': {e}")

    def add_route(self, source_broker: str, source_topic: str, target_broker: str, target_topic: str, **target_kwargs) -> None:
        """
        Configures a route between two registered brokers.
        """
        if source_broker not in self.brokers:
            raise ValueError(f"Source broker '{source_broker}' not found.")
        if target_broker not in self.brokers:
            raise ValueError(f"Target broker '{target_broker}' not found.")

        self.router.add_route(
            source_client=source_broker,
            source_topic=source_topic,
            target_client=target_broker,
            target_topic=target_topic,
            **target_kwargs
        )

    def subscribe(self, broker_name: str, topic: str, **kwargs) -> None:
        """
        Subscribes a specific broker to a topic.
        """
        if broker_name not in self.brokers:
            raise ValueError(f"Broker '{broker_name}' not found.")

        try:
            self.brokers[broker_name].subscribe(topic, **kwargs)
        except Exception as e:
            logger.error(f"Failed to subscribe {broker_name} to {topic}: {e}")

    def start(self) -> None:
        """
        Starts connections for all registered brokers.
        """
        self.is_running = True
        logger.info("Starting all PostOffice broker connections...")
        for name, broker in self.brokers.items():
            try:
                broker.connect()
            except Exception as e:
                logger.error(f"Failed to connect broker '{name}': {e}")

    def stop(self) -> None:
        """
        Stops and cleans up connections for all registered brokers.
        """
        logger.info("Stopping all PostOffice broker connections...")
        for name, broker in self.brokers.items():
            try:
                broker.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting broker '{name}': {e}")
