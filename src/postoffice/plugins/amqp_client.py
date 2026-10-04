import pika
from postoffice.base_client import BaseClient
from postoffice.registry import ClientRegistry
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

        def _subscribe():
            if exchange:
                self.channel.exchange_declare(exchange=exchange, exchange_type=exchange_type)

            result = self.channel.queue_declare(queue=queue, exclusive=kwargs.get('exclusive', False))
            queue_name = result.method.queue

            if exchange:
                self.channel.queue_bind(exchange=exchange, queue=queue_name, routing_key=topic)

            self.channel.basic_consume(queue=queue_name, on_message_callback=self._on_message, auto_ack=kwargs.get('auto_ack', True))
            logger.info(f"{self.name} subscribed to topic/routing_key {topic} on queue {queue_name} (exchange: {exchange})")

        self.connection.add_callback_threadsafe(_subscribe)

    def _on_message(self, ch, method, properties, body):
        self.on_message(method.routing_key, body, exchange=method.exchange)

    def publish(self, topic: str, message: bytes, **kwargs):
        if not self.channel:
            logger.error(f"{self.name}: Cannot publish to {topic}, channel is not open.")
            return

        exchange = kwargs.get('exchange', '')

        def _publish():
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
