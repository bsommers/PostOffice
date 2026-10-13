import ssl
import paho.mqtt.client as mqtt
from postoffice.base_client import BaseClient
from postoffice.registry import ClientRegistry
from postoffice.security import SecurityConfig, ensure_security_config
from typing import Optional, Callable, Union, Dict, Any
import logging

logger = logging.getLogger(__name__)

@ClientRegistry.register("mqtt")
class MqttClient(BaseClient):
    def __init__(
        self,
        name: str,
        router: any,
        host: str = "localhost",
        port: Optional[int] = None,
        security_config: Optional[Union[SecurityConfig, Dict[str, Any]]] = None,
        **kwargs
    ):
        super().__init__(name, router)
        self.host = host
        self.security = ensure_security_config(security_config)
        if port is None:
            self.port = 8883 if self.security.tls_enabled else 1883
        elif port == 1883 and self.security.tls_enabled:
            self.port = 8883
        else:
            self.port = port

        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.name)
        if self.security.username is not None:
            self.client.username_pw_set(self.security.username, self.security.password)

        if self.security.tls_enabled:
            cert_reqs = ssl.CERT_NONE if self.security.insecure_skip_verify else ssl.CERT_REQUIRED
            self.client.tls_set(
                ca_certs=self.security.ca_cert,
                certfile=self.security.client_cert,
                keyfile=self.security.client_key,
                cert_reqs=cert_reqs,
                tls_version=ssl.PROTOCOL_TLS_CLIENT,
            )
            if self.security.insecure_skip_verify:
                self.client.tls_insecure_set(True)

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

