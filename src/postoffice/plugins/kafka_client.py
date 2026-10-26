from confluent_kafka import Consumer, Producer, KafkaError
from postoffice.base_client import BaseClient
from postoffice.registry import ClientRegistry
from postoffice.security import SecurityConfig, ensure_security_config
from typing import Optional, Callable, Union, Dict, Any
import logging
import threading

logger = logging.getLogger(__name__)

@ClientRegistry.register("kafka")
class KafkaClient(BaseClient):
    def __init__(
        self,
        name: str,
        router: any,
        bootstrap_servers: str = "localhost:9092",
        security_config: Optional[Union[SecurityConfig, Dict[str, Any]]] = None,
        **kwargs
    ):
        super().__init__(name, router, **kwargs)
        self.bootstrap_servers = bootstrap_servers
        self.security = ensure_security_config(security_config)
        self.producer = None
        self.consumer = None
        self.thread = None
        self._stop_event = threading.Event()
        self.subscribed_topics = []
        self._subscribe_lock = threading.Lock()
        self._pending_subscriptions = []
        self._paused = threading.Event()
        self._pause_applied = False

    def _build_kafka_config(self, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        conf: Dict[str, Any] = {'bootstrap.servers': self.bootstrap_servers}
        has_tls = self.security.tls_enabled
        has_sasl = bool(self.security.username and self.security.sasl_mechanism)

        if has_tls and has_sasl:
            conf['security.protocol'] = 'SASL_SSL'
        elif has_tls:
            conf['security.protocol'] = 'SSL'
        elif has_sasl:
            conf['security.protocol'] = 'SASL_PLAINTEXT'

        if has_tls:
            if self.security.ca_cert:
                conf['ssl.ca.location'] = self.security.ca_cert
            if self.security.client_cert:
                conf['ssl.certificate.location'] = self.security.client_cert
            if self.security.client_key:
                conf['ssl.key.location'] = self.security.client_key
            if self.security.client_key_password:
                conf['ssl.key.password'] = self.security.client_key_password
            if self.security.insecure_skip_verify:
                logger.warning("%s: insecure_skip_verify is set; TLS certificates are NOT verified (use only for local development).", self.name)
                conf['enable.ssl.certificate.verification'] = False

        if has_sasl:
            conf['sasl.mechanism'] = self.security.sasl_mechanism
            conf['sasl.username'] = self.security.username
            if self.security.password:
                conf['sasl.password'] = self.security.password

        if extra:
            conf.update(extra)
        return conf

    def connect(self):
        try:
            prod_conf = self._build_kafka_config()
            cons_conf = self._build_kafka_config({
                'group.id': f"{self.name}_group",
                'auto.offset.reset': 'earliest'
            })
            self.producer = Producer(prod_conf)
            self.consumer = Consumer(cons_conf)
            logger.info(f"{self.name} connected to Kafka broker")

            self.flush_publish_buffer()

            self.thread = threading.Thread(target=self._consume_loop)
            self.thread.daemon = True
            self.thread.start()
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka broker at {self.bootstrap_servers}: {e}")

    def disconnect(self):
        self._stop_event.set()
        if self.thread:
            self.thread.join(timeout=2)
        if self.consumer:
            self.consumer.close()
        if self.producer:
            self.producer.flush()

    def reload_security(self, security_config: Optional[Union[SecurityConfig, Dict[str, Any]]]) -> None:
        was_active = bool(self.producer is not None or self.consumer is not None)
        if was_active:
            self.disconnect()

        self.security = ensure_security_config(security_config)
        logger.info(f"{self.name}: Reloaded Kafka security configuration (tls_enabled={self.security.tls_enabled})")

        if was_active:
            self._stop_event.clear()
            with self._subscribe_lock:
                for topic in self.subscribed_topics:
                    if topic not in self._pending_subscriptions:
                        self._pending_subscriptions.append(topic)
                self.subscribed_topics = []
            self.connect()

    def pause_consumption(self) -> None:
        self._paused.set()

    def resume_consumption(self) -> None:
        self._paused.clear()

    def _apply_pause_state(self) -> None:
        """Runs on the consume thread: keeps polling (to stay in the group) with partitions paused."""
        try:
            if self._paused.is_set():
                # Re-applied every cycle so a rebalance cannot silently un-pause new partitions.
                self.consumer.pause(self.consumer.assignment())
            elif self._pause_applied:
                self.consumer.resume(self.consumer.assignment())
            self._pause_applied = self._paused.is_set()
        except Exception as e:
            logger.error(f"{self.name}: Error applying pause state: {e}")

    def subscribe(self, topic: str, **kwargs):
        self.record_subscription(topic, **kwargs)
        if not self.consumer:
            logger.warning(f"{self.name}: Consumer not connected. Topic {topic} queued for subscription on connect.")

        with self._subscribe_lock:
            if topic not in self.subscribed_topics and topic not in self._pending_subscriptions:
                self._pending_subscriptions.append(topic)
                logger.info(f"{self.name} queued subscription for {topic}")

    def publish(
        self,
        topic: str,
        message: bytes,
        on_confirm: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        **kwargs
    ):
        if not self.producer:
            if self.buffer_publish(topic, message, on_confirm=on_confirm, on_error=on_error, **kwargs):
                logger.info(f"{self.name}: Producer not connected. Message to {topic} buffered for publish on connect.")
                return
            logger.error(f"{self.name}: Cannot publish to {topic}, producer is not connected.")
            if on_error:
                on_error(RuntimeError(f"{self.name}: Producer is not connected."))
            return

        def delivery_report(err, msg):
            if err is not None:
                logger.error(f"Message delivery failed: {err}")
                if on_error:
                    on_error(err)
            else:
                if on_confirm:
                    on_confirm()

        # Handle specific kafka publish features
        key = kwargs.get("key", None)
        partition = kwargs.get("partition", -1)

        produce_kwargs = {"topic": topic, "value": message, "callback": delivery_report}

        if key is not None:
            produce_kwargs["key"] = key
        if partition != -1:
            produce_kwargs["partition"] = partition

        try:
            self.producer.produce(**produce_kwargs)
            self.producer.poll(0)
        except Exception as e:
            logger.error(f"{self.name}: Error producing to {topic}: {e}")
            if on_error:
                on_error(e)
            raise

    def _consume_loop(self):
        while not self._stop_event.is_set():
            with self._subscribe_lock:
                if self._pending_subscriptions:
                    self.subscribed_topics.extend(self._pending_subscriptions)
                    self.consumer.subscribe(self.subscribed_topics)
                    logger.info(f"{self.name} applied subscriptions: {self._pending_subscriptions}")
                    self._pending_subscriptions = []

            self._apply_pause_state()
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error(f"Kafka error: {msg.error()}")
                continue

            def _ack():
                try:
                    self.consumer.commit(message=msg, asynchronous=True)
                except Exception as e:
                    logger.error(f"Error committing offset: {e}")

            self.on_message(
                msg.topic(),
                msg.value(),
                ack_fn=_ack,
                key=msg.key(),
                partition=msg.partition(),
                offset=msg.offset()
            )


