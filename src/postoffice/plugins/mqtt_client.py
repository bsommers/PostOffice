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
    DEFAULT_PLAINTEXT_PORT = 1883
    DEFAULT_TLS_PORT = 8883

    def __init__(
        self,
        name: str,
        router: any,
        host: str = "localhost",
        port: Optional[int] = None,
        security_config: Optional[Union[SecurityConfig, Dict[str, Any]]] = None,
        **kwargs
    ):
        super().__init__(name, router, **kwargs)
        self.host = host
        self.security = ensure_security_config(security_config)
        if port is None:
            self.port = self.DEFAULT_TLS_PORT if self.security.tls_enabled else self.DEFAULT_PLAINTEXT_PORT
        elif port == self.DEFAULT_PLAINTEXT_PORT and self.security.tls_enabled:
            self.port = self.DEFAULT_TLS_PORT
        else:
            self.port = port

        self.subscribed_topics = []
        self._init_paho_client()

    def _init_paho_client(self):
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

    def reload_security(self, security_config: Optional[Union[SecurityConfig, Dict[str, Any]]]) -> None:
        was_connected = bool(self.client and self.client.is_connected())
        if was_connected:
            self.disconnect()

        self.security = ensure_security_config(security_config)
        if self.port == self.DEFAULT_PLAINTEXT_PORT and self.security.tls_enabled:
            self.port = self.DEFAULT_TLS_PORT
        elif self.port == self.DEFAULT_TLS_PORT and not self.security.tls_enabled:
            self.port = self.DEFAULT_PLAINTEXT_PORT

        self._init_paho_client()
        logger.info(f"{self.name}: Reloaded MQTT security configuration (tls_enabled={self.security.tls_enabled})")
        if was_connected:
            self.connect()

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        logger.info(f"{self.name} connected to MQTT broker with result code {reason_code}")
        recorded = self.get_subscriptions()
        for topic in self.subscribed_topics:
            sub_kwargs = recorded.get(topic, {})
            if "qos" in sub_kwargs:
                self.client.subscribe(topic, qos=sub_kwargs["qos"])
            else:
                self.client.subscribe(topic)
            logger.info(f"{self.name} subscribed to {topic}")
        self.flush_publish_buffer()

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
        sub_kwargs = dict(kwargs)
        sub_kwargs['qos'] = qos
        self.record_subscription(topic, **sub_kwargs)

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
            if self.buffer_publish(topic, message, on_confirm=on_confirm, on_error=on_error, **kwargs):
                logger.info(f"{self.name}: Client not connected. Message to {topic} buffered for publish on connect.")
                return
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


