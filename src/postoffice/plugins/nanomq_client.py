from postoffice.plugins.mqtt_client import MqttClient
from postoffice.registry import ClientRegistry
import logging

logger = logging.getLogger(__name__)

@ClientRegistry.register("nanomq")
class NanoMqClient(MqttClient):
    """
    NanoMQ is a lightweight edge MQTT broker.
    It inherently supports MQTT 3.1.1 and 5.0 just like Mosquitto,
    but we can extend this client class with any NanoMQ specific SDK functions if needed.
    """
    def __init__(self, name: str, router: any, host: str = "localhost", port: int = 1884):
        # Default port to 1884 to match local docker compose mapping
        super().__init__(name, router, host=host, port=port)
        logger.info(f"Initialized NanoMQ Client targeting {host}:{port}")
