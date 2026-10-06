from confluent_kafka import Consumer, Producer, KafkaError
from postoffice.base_client import BaseClient
from postoffice.registry import ClientRegistry
from typing import Optional, Callable
import logging
import threading

logger = logging.getLogger(__name__)

@ClientRegistry.register("kafka")
class KafkaClient(BaseClient):
    def __init__(self, name: str, router: any, bootstrap_servers: str = "localhost:9092"):
        super().__init__(name, router)
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.consumer = None
        self.thread = None
        self._stop_event = threading.Event()
        self.subscribed_topics = []
        self._subscribe_lock = threading.Lock()
        self._pending_subscriptions = []

    def connect(self):
        try:
            self.producer = Producer({'bootstrap.servers': self.bootstrap_servers})
            self.consumer = Consumer({
                'bootstrap.servers': self.bootstrap_servers,
                'group.id': f"{self.name}_group",
                'auto.offset.reset': 'earliest'
            })
            logger.info(f"{self.name} connected to Kafka broker")

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

    def subscribe(self, topic: str, **kwargs):
        if not self.consumer:
            logger.error(f"{self.name}: Cannot subscribe to {topic}, consumer is not connected.")
            return

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

