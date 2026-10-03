import pika
from .base_client import BaseClient
import logging
import threading

logger = logging.getLogger(__name__)

class AmqpClient(BaseClient):
    def __init__(self, name: str, router: any, host: str = "localhost"):
        super().__init__(name, router)
        self.host = host
        self.connection = None
        self.channel = None
        self.thread = None
        self._stop_event = threading.Event()

    def connect(self):
        try:
            credentials = pika.PlainCredentials('user', 'password')
            parameters = pika.ConnectionParameters(self.host, 5672, '/', credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            logger.info(f"{self.name} connected to AMQP broker")

            self.thread = threading.Thread(target=self._consume_loop)
            self.thread.daemon = True
            self.thread.start()
        except pika.exceptions.AMQPConnectionError:
            logger.error(f"Failed to connect to AMQP broker at {self.host}:5672")

    def disconnect(self):
        self._stop_event.set()
        if self.connection and self.connection.is_open:
            # Need to close connection safely from the thread it belongs to
            try:
                self.connection.add_callback_threadsafe(self.connection.close)
            except Exception as e:
                logger.error(f"Error scheduling AMQP close: {e}")
        if self.thread:
            self.thread.join(timeout=2)

    def subscribe(self, topic: str):
        if not self.channel:
            logger.error(f"{self.name}: Cannot subscribe to {topic}, channel is not open.")
            return

        def _subscribe():
            self.channel.queue_declare(queue=topic)
            self.channel.basic_consume(queue=topic, on_message_callback=self._on_message, auto_ack=True)
            logger.info(f"{self.name} subscribed to {topic}")

        self.connection.add_callback_threadsafe(_subscribe)

    def _on_message(self, ch, method, properties, body):
        self.on_message(method.routing_key, body)

    def publish(self, topic: str, message: bytes):
        if not self.channel:
            logger.error(f"{self.name}: Cannot publish to {topic}, channel is not open.")
            return

        def _publish():
            self.channel.queue_declare(queue=topic)
            self.channel.basic_publish(exchange='', routing_key=topic, body=message)

        self.connection.add_callback_threadsafe(_publish)

    def _consume_loop(self):
        while not self._stop_event.is_set():
            try:
                self.connection.process_data_events(time_limit=1)
            except Exception as e:
                logger.error(f"AMQP consume loop error: {e}")
                break
