import ssl
import pika
from postoffice.base_client import BaseClient
from postoffice.registry import ClientRegistry
from postoffice.security import SecurityConfig, ensure_security_config
from typing import Optional, Callable, Union, Dict, Any
import logging
import threading

logger = logging.getLogger(__name__)

@ClientRegistry.register("amqp")
class AmqpClient(BaseClient):
    def __init__(
        self,
        name: str,
        router: any,
        host: str = "localhost",
        port: Optional[int] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        security_config: Optional[Union[SecurityConfig, Dict[str, Any]]] = None,
        **kwargs
    ):
        super().__init__(name, router, **kwargs)
        self.host = host
        self.security = ensure_security_config(security_config)
        self.user = self.security.username if self.security.username is not None else (user if user is not None else "user")
        self.password = self.security.password if self.security.password is not None else (password if password is not None else "password")

        if port is None:
            self.port = 5671 if self.security.tls_enabled else 5672
        elif port == 5672 and self.security.tls_enabled:
            self.port = 5671
        else:
            self.port = port

        self.connection = None
        self.channel = None
        self.thread = None
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()
        self._prefetch: Optional[int] = None

    def connect(self):
        self.thread = threading.Thread(target=self._consume_loop)
        self.thread.daemon = True
        self.thread.start()
        # Wait briefly for the connection to establish before returning
        self._connected_event.wait(timeout=2.0)

    def disconnect(self):
        self._stop_event.set()
        if self.connection and self.connection.is_open:
            try:
                self.connection.add_callback_threadsafe(self.connection.close)
            except Exception as e:
                logger.error(f"Error scheduling AMQP close: {e}")
        if self.thread:
            self.thread.join(timeout=2)

    def reload_security(self, security_config: Optional[Union[SecurityConfig, Dict[str, Any]]]) -> None:
        was_active = bool(self.connection and self.connection.is_open) or bool(
            self.thread and self.thread.is_alive()
        )
        if was_active:
            self.disconnect()

        self.security = ensure_security_config(security_config)
        if self.security.username is not None:
            self.user = self.security.username
        if self.security.password is not None:
            self.password = self.security.password

        if self.port == 5672 and self.security.tls_enabled:
            self.port = 5671
        elif self.port == 5671 and not self.security.tls_enabled:
            self.port = 5672

        logger.info(f"{self.name}: Reloaded AMQP security configuration (tls_enabled={self.security.tls_enabled})")
        if was_active:
            self._stop_event.clear()
            self._connected_event.clear()
            self.connect()

    def set_inflight_limit(self, max_inflight: Optional[int]) -> None:
        """Bounds unacked deliveries with the broker's own prefetch window (0 = unlimited)."""
        self._prefetch = max_inflight
        if self.channel and self.connection and self.connection.is_open:
            self.connection.add_callback_threadsafe(self._apply_prefetch)

    def _apply_prefetch(self) -> None:
        if self.channel is not None:
            self.channel.basic_qos(prefetch_count=self._prefetch or 0)

    def _apply_subscription(self, topic: str, kwargs: Dict[str, Any]):
        exchange = kwargs.get('exchange', '')
        exchange_type = kwargs.get('exchange_type', 'direct')
        queue = kwargs.get('queue', topic)
        # Manual ack by default: the router acks only after downstream delivery is confirmed.
        auto_ack = kwargs.get('auto_ack', False)

        if exchange:
            self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)

        result = self.channel.queue_declare(queue=queue, exclusive=kwargs.get('exclusive', False))
        queue_name = result.method.queue

        if exchange:
            self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=topic)

        self.channel.basic_consume(
            queue=queue_name,
            on_message_callback=lambda ch, method, properties, body: self._on_message(ch, method, properties, body, auto_ack=auto_ack),
            auto_ack=auto_ack
        )
        logger.info(f"{self.name} subscribed to topic/routing_key {topic} on queue {queue_name} (exchange: {exchange})")

    def subscribe(self, topic: str, **kwargs):
        self.record_subscription(topic, **kwargs)
        if not self.channel or not self.connection:
            logger.warning(f"{self.name}: Channel not open. Topic {topic} queued for subscription on connect.")
            return

        sub_kwargs = dict(kwargs)
        self.connection.add_callback_threadsafe(lambda: self._apply_subscription(topic, sub_kwargs))

    def _on_message(self, ch, method, properties, body, auto_ack: bool = True):
        ack_fn = None
        nack_fn = None
        if not auto_ack:
            delivery_tag = method.delivery_tag
            def _ack():
                try:
                    if self.connection and self.connection.is_open:
                        self.connection.add_callback_threadsafe(lambda: ch.basic_ack(delivery_tag=delivery_tag))
                    else:
                        ch.basic_ack(delivery_tag=delivery_tag)
                except Exception as e:
                    logger.error(f"Error acking AMQP message: {e}")

            def _nack(requeue: bool = True):
                try:
                    if self.connection and self.connection.is_open:
                        self.connection.add_callback_threadsafe(lambda: ch.basic_nack(delivery_tag=delivery_tag, requeue=requeue))
                    else:
                        ch.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
                except Exception as e:
                    logger.error(f"Error nacking AMQP message: {e}")

            ack_fn = _ack
            nack_fn = _nack

        extra_kwargs = {"exchange": method.exchange}
        if properties and getattr(properties, 'delivery_mode', None) is not None:
            extra_kwargs["delivery_mode"] = properties.delivery_mode

        self.on_message(method.routing_key, body, ack_fn=ack_fn, nack_fn=nack_fn, **extra_kwargs)

    def publish(
        self,
        topic: str,
        message: bytes,
        on_confirm: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        **kwargs
    ):
        if not self.channel:
            if self.buffer_publish(topic, message, on_confirm=on_confirm, on_error=on_error, **kwargs):
                logger.info(f"{self.name}: Channel not open. Message to {topic} buffered for publish on connect.")
                return
            logger.error(f"{self.name}: Cannot publish to {topic}, channel is not open.")
            if on_error:
                on_error(RuntimeError(f"{self.name}: Channel is not open."))
            return

        exchange = kwargs.get('exchange', '')

        def _publish():
            try:
                if not exchange:
                    # If no exchange is specified, default to the default exchange which requires the queue to exist
                    self.channel.queue_declare(queue=topic)

                self.channel.basic_publish(
                    exchange=exchange,
                    routing_key=topic,
                    body=message,
                    properties=pika.BasicProperties(
                        delivery_mode=kwargs.get('delivery_mode', 1) # 1 = transient, 2 = persistent
                    )
                )
                if on_confirm:
                    on_confirm()
            except Exception as e:
                logger.error(f"Error publishing AMQP message: {e}")
                if on_error:
                    on_error(e)

        self.connection.add_callback_threadsafe(_publish)


    def _consume_loop(self):
        try:
            credentials = pika.PlainCredentials(self.user, self.password)
            ssl_options = None
            if self.security.tls_enabled:
                context = ssl.create_default_context(cafile=self.security.ca_cert)
                if self.security.client_cert and self.security.client_key:
                    context.load_cert_chain(
                        certfile=self.security.client_cert,
                        keyfile=self.security.client_key,
                        password=self.security.client_key_password,
                    )
                if self.security.insecure_skip_verify:
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE
                ssl_options = pika.SSLOptions(context, server_hostname=self.host)

            parameters = pika.ConnectionParameters(
                host=self.host,
                port=self.port,
                virtual_host='/',
                credentials=credentials,
                ssl_options=ssl_options,
            )
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            if self._prefetch is not None:
                self._apply_prefetch()
            logger.info(f"{self.name} connected to AMQP broker at {self.host}:{self.port}")

            for topic, sub_kwargs in self.get_subscriptions().items():
                self._apply_subscription(topic, sub_kwargs)
            self.flush_publish_buffer()

            self._connected_event.set()
        except pika.exceptions.AMQPConnectionError:
            logger.error(f"Failed to connect to AMQP broker at {self.host}:{self.port}")
            self._connected_event.set() # Unblock connect loop on fail
            return

        while not self._stop_event.is_set():
            try:
                self.connection.process_data_events(time_limit=1)
            except Exception as e:
                logger.error(f"AMQP consume loop error: {e}")
                break

