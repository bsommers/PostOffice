import pika
from postoffice.base_client import BaseClient
from postoffice.registry import ClientRegistry
from typing import Optional, Callable
import logging
import threading

logger = logging.getLogger(__name__)

@ClientRegistry.register("amqp")
class AmqpClient(BaseClient):
    def __init__(self, name: str, router: any, host: str = "localhost", user: str = "user", password: str = "password"):
        super().__init__(name, router)
        self.host = host
        self.user = user
        self.password = password
        self.connection = None
        self.channel = None
        self.thread = None
        self._stop_event = threading.Event()
        self._connected_event = threading.Event()

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

    def subscribe(self, topic: str, **kwargs):
        if not self.channel:
            logger.error(f"{self.name}: Cannot subscribe to {topic}, channel is not open.")
            return

        exchange = kwargs.get('exchange', '')
        exchange_type = kwargs.get('exchange_type', 'direct')
        queue = kwargs.get('queue', topic)
        auto_ack = kwargs.get('auto_ack', True)

        def _subscribe():
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

        self.connection.add_callback_threadsafe(_subscribe)

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
            parameters = pika.ConnectionParameters(self.host, 5672, '/', credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            logger.info(f"{self.name} connected to AMQP broker")
            self._connected_event.set()
        except pika.exceptions.AMQPConnectionError:
            logger.error(f"Failed to connect to AMQP broker at {self.host}:5672")
            self._connected_event.set() # Unblock connect loop on fail
            return

        while not self._stop_event.is_set():
            try:
                self.connection.process_data_events(time_limit=1)
            except Exception as e:
                logger.error(f"AMQP consume loop error: {e}")
                break
