import paho.mqtt.client as mqtt
from .base_client import BaseClient
import logging

logger = logging.getLogger(__name__)

class MqttClient(BaseClient):
    def __init__(self, name: str, router: any, host: str = "localhost", port: int = 1883):
        super().__init__(name, router)
        self.host = host
        self.port = port
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.name)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.subscribed_topics = []

    def _on_connect(self, client, userdata, flags, rc):
        logger.info(f"{self.name} connected to MQTT broker with result code {rc}")
        for topic in self.subscribed_topics:
            self.client.subscribe(topic)
            logger.info(f"{self.name} subscribed to {topic}")

    def _on_message(self, client, userdata, msg):
        self.on_message(msg.topic, msg.payload)

    def connect(self):
        try:
            self.client.connect(self.host, self.port, 60)
            self.client.loop_start()
        except ConnectionRefusedError:
            logger.error(f"Failed to connect to MQTT broker at {self.host}:{self.port}")

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def subscribe(self, topic: str):
        if not self.client.is_connected():
            logger.warning(f"{self.name}: Client not connected. Topic {topic} queued for subscription on connect.")
        if topic not in self.subscribed_topics:
            self.subscribed_topics.append(topic)
            if self.client.is_connected():
                self.client.subscribe(topic)

    def publish(self, topic: str, message: bytes):
        if not self.client.is_connected():
            logger.error(f"{self.name}: Cannot publish to {topic}, client is not connected.")
            return
        self.client.publish(topic, message)
