import paho.mqtt.client as mqtt
from postoffice.base_client import BaseClient
from postoffice.registry import ClientRegistry
from typing import Optional, Callable
import logging

logger = logging.getLogger(__name__)

@ClientRegistry.register("mqtt")
class MqttClient(BaseClient):
    def __init__(self, name: str, router: any, host: str = "localhost", port: int = 1883):
        super().__init__(name, router)
        self.host = host
        self.port = port
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.name)
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        self.subscribed_topics = []

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f"{self.name} connected to MQTT broker with result code {reason_code}")
        for topic in self.subscribed_topics:
            self.client.subscribe(topic)
            logger.info(f"{self.name} subscribed to {topic}")

    def _on_message(self, client, userdata, msg):
        self.on_message(msg.topic, msg.payload, qos=msg.qos)

    def connect(self):
        try:
            self.client.connect(self.host, self.port, 60)
            self.client.loop_start()
        except ConnectionRefusedError:
            logger.error(f"Failed to connect to MQTT broker at {self.host}:{self.port}")

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def subscribe(self, topic: str, **kwargs):
        qos = kwargs.get('qos', 0)

        if not self.client.is_connected():
            logger.warning(f"{self.name}: Client not connected. Topic {topic} queued for subscription on connect.")

        if topic not in self.subscribed_topics:
            self.subscribed_topics.append(topic)
            if self.client.is_connected():
                self.client.subscribe(topic, qos=qos)

    def publish(
        self,
        topic: str,
        message: bytes,
        on_confirm: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        **kwargs
    ):
        if not self.client.is_connected():
            logger.error(f"{self.name}: Cannot publish to {topic}, client is not connected.")
            if on_error:
                on_error(RuntimeError(f"{self.name}: Client is not connected."))
            return

        qos = kwargs.get('qos', 0)
        retain = kwargs.get('retain', False)

        try:
            info = self.client.publish(topic, message, qos=qos, retain=retain)
            if on_confirm:
                on_confirm()
        except Exception as e:
            logger.error(f"Error publishing MQTT message: {e}")
            if on_error:
                on_error(e)
            raise

