import unittest
from unittest.mock import MagicMock, patch
from postoffice.base_client import BaseClient, BufferedPublish, BufferOverflowError
from postoffice.plugins.mqtt_client import MqttClient
from postoffice.plugins.nanomq_client import NanoMqClient
from postoffice.plugins.amqp_client import AmqpClient
from postoffice.plugins.kafka_client import KafkaClient
from postoffice.router import Router


class DummyClient(BaseClient):
    def __init__(self, name="dummy", router=None, **kwargs):
        super().__init__(name, router, **kwargs)
        self.connected = False
        self.published = []

    def connect(self):
        self.connected = True
        self.flush_publish_buffer()

    def disconnect(self):
        self.connected = False

    def subscribe(self, topic: str, **kwargs):
        self.record_subscription(topic, **kwargs)

    def publish(self, topic: str, message: bytes, on_confirm=None, on_error=None, **kwargs):
        if not self.connected:
            if self.buffer_publish(topic, message, on_confirm=on_confirm, on_error=on_error, **kwargs):
                return
            if on_error:
                on_error(RuntimeError("Not connected"))
            return
        self.published.append((topic, message, kwargs))
        if on_confirm:
            on_confirm()


class TestBaseClientQueuing(unittest.TestCase):
    def test_invalid_overflow_policy_raises(self):
        with self.assertRaises(ValueError):
            DummyClient(enable_publish_buffer=True, buffer_overflow_policy="invalid_policy")

    def test_publish_buffer_fifo_and_flush(self):
        client = DummyClient(enable_publish_buffer=True, max_publish_buffer_size=5)
        confirmed = []

        client.publish("t1", b"m1", on_confirm=lambda: confirmed.append("m1"), qos=1)
        client.publish("t2", b"m2", on_confirm=lambda: confirmed.append("m2"), qos=2)

        self.assertEqual(client.publish_buffer_size, 2)
        self.assertEqual(len(client.published), 0)
        self.assertEqual(confirmed, [])

        # Connect triggers flush in FIFO order
        client.connect()
        self.assertEqual(client.publish_buffer_size, 0)
        self.assertEqual(client.published, [("t1", b"m1", {"qos": 1}), ("t2", b"m2", {"qos": 2})])
        self.assertEqual(confirmed, ["m1", "m2"])

    def test_overflow_policy_reject(self):
        client = DummyClient(
            enable_publish_buffer=True,
            max_publish_buffer_size=2,
            buffer_overflow_policy="reject",
        )
        errors = []

        client.publish("t1", b"m1", on_error=lambda e: errors.append(("m1", e)))
        client.publish("t2", b"m2", on_error=lambda e: errors.append(("m2", e)))
        client.publish("t3", b"m3", on_error=lambda e: errors.append(("m3", e)))

        self.assertEqual(client.publish_buffer_size, 2)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0][0], "m3")
        self.assertIsInstance(errors[0][1], BufferOverflowError)

        client.connect()
        self.assertEqual([p[1] for p in client.published], [b"m1", b"m2"])

    def test_overflow_policy_drop_oldest(self):
        client = DummyClient(
            enable_publish_buffer=True,
            max_publish_buffer_size=2,
            buffer_overflow_policy="drop_oldest",
        )
        errors = []

        client.publish("t1", b"m1", on_error=lambda e: errors.append(("m1", e)))
        client.publish("t2", b"m2", on_error=lambda e: errors.append(("m2", e)))
        client.publish("t3", b"m3", on_error=lambda e: errors.append(("m3", e)))

        self.assertEqual(client.publish_buffer_size, 2)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0][0], "m1")
        self.assertIsInstance(errors[0][1], BufferOverflowError)

        client.connect()
        self.assertEqual([p[1] for p in client.published], [b"m2", b"m3"])


class TestMqttAndNanoMqQueuing(unittest.TestCase):
    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    def test_mqtt_subscription_kwargs_preserved_on_reconnect(self, MockMqtt):
        mock_paho = MockMqtt.return_value
        mock_paho.is_connected.return_value = False

        router = MagicMock()
        client = MqttClient("mqtt_1", router)

        # Subscribe while offline with custom qos
        client.subscribe("sensors/+/temp", qos=2)
        client.subscribe("alerts/#", qos=1)
        mock_paho.subscribe.assert_not_called()

        # Simulate connection established
        mock_paho.is_connected.return_value = True
        client._on_connect(mock_paho, None, None, 0, None)

        mock_paho.subscribe.assert_any_call("sensors/+/temp", qos=2)
        mock_paho.subscribe.assert_any_call("alerts/#", qos=1)
        self.assertEqual(mock_paho.subscribe.call_count, 2)

    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    def test_mqtt_publish_buffering_and_flush_on_connect(self, MockMqtt):
        mock_paho = MockMqtt.return_value
        mock_paho.is_connected.return_value = False

        router = MagicMock()
        client = MqttClient("mqtt_buf", router, enable_publish_buffer=True, max_publish_buffer_size=10)

        confirmed = []
        errors = []
        client.publish(
            "telemetry/1",
            b"payload_1",
            on_confirm=lambda: confirmed.append(1),
            on_error=lambda e: errors.append(e),
            qos=1,
            retain=True,
        )
        client.publish(
            "telemetry/2",
            b"payload_2",
            on_confirm=lambda: confirmed.append(2),
            on_error=lambda e: errors.append(e),
            qos=0,
        )

        mock_paho.publish.assert_not_called()
        self.assertEqual(client.publish_buffer_size, 2)
        self.assertEqual(confirmed, [])
        self.assertEqual(errors, [])

        # Reconnect flushes buffered publishes in FIFO order
        mock_paho.is_connected.return_value = True
        client._on_connect(mock_paho, None, None, 0, None)

        self.assertEqual(client.publish_buffer_size, 0)
        self.assertEqual(mock_paho.publish.call_count, 2)
        mock_paho.publish.assert_any_call("telemetry/1", b"payload_1", qos=1, retain=True)
        mock_paho.publish.assert_any_call("telemetry/2", b"payload_2", qos=0, retain=False)
        self.assertEqual(confirmed, [1, 2])

    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    def test_nanomq_inherits_queuing_and_qos_replay(self, MockMqtt):
        mock_paho = MockMqtt.return_value
        mock_paho.is_connected.return_value = False

        router = MagicMock()
        nano = NanoMqClient("nano_edge", router, enable_publish_buffer=True)
        nano.subscribe("edge/topic", qos=1)
        nano.publish("edge/out", b"data", qos=2)

        self.assertEqual(nano.publish_buffer_size, 1)
        mock_paho.is_connected.return_value = True
        nano._on_connect(mock_paho, None, None, 0, None)

        mock_paho.subscribe.assert_called_once_with("edge/topic", qos=1)
        mock_paho.publish.assert_called_once_with("edge/out", b"data", qos=2, retain=False)
        self.assertEqual(nano.publish_buffer_size, 0)


class TestAmqpAndKafkaQueuing(unittest.TestCase):
    @patch("postoffice.plugins.amqp_client.pika.BlockingConnection")
    def test_amqp_offline_subscription_and_publish_replay(self, MockBlockingConn):
        mock_conn = MockBlockingConn.return_value
        mock_channel = MagicMock()
        mock_conn.channel.return_value = mock_channel
        # Execute threadsafe callbacks immediately
        mock_conn.add_callback_threadsafe.side_effect = lambda cb: cb()
        # Stop consume loop after 1 iteration
        router = MagicMock()
        client = AmqpClient("amqp_1", router, enable_publish_buffer=True)

        # 1. Pre-connect subscribe & publish while channel is None
        client.subscribe("orders.created", exchange="orders_ex", exchange_type="topic", queue="orders_q", auto_ack=False)
        confirmed = []
        client.publish("orders.processed", b"done_1", on_confirm=lambda: confirmed.append("done_1"), delivery_mode=2)

        self.assertEqual(client.publish_buffer_size, 1)
        self.assertIn("orders.created", client.get_subscriptions())

        # 2. Simulate _consume_loop connection setup (stop loop immediately on process_data_events)
        def stop_loop(*args, **kwargs):
            client._stop_event.set()
        mock_conn.process_data_events.side_effect = stop_loop

        client._consume_loop()

        # Verify subscription was replayed with all kwargs
        mock_channel.exchange_declare.assert_called_once_with(exchange="orders_ex", exchange_type="topic")
        mock_channel.queue_declare.assert_any_call(queue="orders_q", exclusive=False)
        mock_channel.basic_consume.assert_called_once()

        # Verify buffered publish was flushed and confirmed
        self.assertEqual(client.publish_buffer_size, 0)
        mock_channel.basic_publish.assert_called_once()
        self.assertEqual(confirmed, ["done_1"])

    @patch("postoffice.plugins.kafka_client.Consumer")
    @patch("postoffice.plugins.kafka_client.Producer")
    def test_kafka_offline_subscription_and_publish_replay(self, MockProducer, MockConsumer):
        mock_prod = MockProducer.return_value
        mock_cons = MockConsumer.return_value

        router = MagicMock()
        client = KafkaClient("kafka_1", router, enable_publish_buffer=True)

        # 1. Subscribe and publish BEFORE connect() (consumer and producer are None)
        client.subscribe("analytics.events")
        confirmed = []
        client.publish(
            "analytics.out",
            b"event_payload",
            on_confirm=lambda: confirmed.append("ok"),
            key=b"k1",
            partition=2,
        )

        self.assertIn("analytics.events", client.get_subscriptions())
        self.assertIn("analytics.events", client._pending_subscriptions)
        self.assertEqual(client.publish_buffer_size, 1)

        # Make producer.produce invoke its callback immediately to simulate delivery confirmation
        def fake_produce(**kwargs):
            cb = kwargs.get("callback")
            if cb:
                cb(None, MagicMock())
        mock_prod.produce.side_effect = fake_produce

        # Stop consume loop after 1 poll
        def fake_poll(timeout):
            client._stop_event.set()
            return None
        mock_cons.poll.side_effect = fake_poll

        # 2. Connect client
        client.connect()
        if client.thread:
            client.thread.join(timeout=2.0)

        # Verify subscription was applied to consumer and publish buffer was flushed
        mock_cons.subscribe.assert_called_once_with(["analytics.events"])
        self.assertEqual(client.publish_buffer_size, 0)
        mock_prod.produce.assert_called_once()
        self.assertEqual(confirmed, ["ok"])


class TestRouterFanoutWithOfflineQueuing(unittest.TestCase):
    def test_router_holds_upstream_ack_until_offline_target_flushes(self):
        router = Router()
        src_client = DummyClient("src", router)
        tgt_client = DummyClient("tgt", router, enable_publish_buffer=True, max_publish_buffer_size=5)
        dlq_client = DummyClient("dlq", router)
        dlq_client.connect()

        router.register_client(src_client)
        router.register_client(tgt_client)
        router.register_client(dlq_client)
        router.set_dlq("dlq", "system/dlq")

        router.add_route("src", "telemetry/+", "tgt", "downstream/telemetry")

        upstream_acked = []
        upstream_nacked = []

        # Route message while tgt_client is offline (disconnected)
        router.route(
            "src",
            "telemetry/temp",
            b'{"v": 42}',
            ack_fn=lambda: upstream_acked.append(True),
            nack_fn=lambda requeue=True: upstream_nacked.append(requeue),
        )

        # Target buffered the message; upstream ACK must NOT fire yet
        self.assertEqual(tgt_client.publish_buffer_size, 1)
        self.assertEqual(upstream_acked, [])
        self.assertEqual(upstream_nacked, [])

        # Target reconnects -> flushes buffer -> confirms delivery -> upstream ACK fires!
        tgt_client.connect()
        self.assertEqual(tgt_client.publish_buffer_size, 0)
        self.assertEqual(len(tgt_client.published), 1)
        self.assertEqual(upstream_acked, [True])
        self.assertEqual(len(dlq_client.published), 0)

    def test_router_routes_to_dlq_when_offline_target_buffer_overflows(self):
        router = Router()
        src_client = DummyClient("src", router)
        tgt_client = DummyClient(
            "tgt",
            router,
            enable_publish_buffer=True,
            max_publish_buffer_size=1,
            buffer_overflow_policy="reject",
        )
        dlq_client = DummyClient("dlq", router)
        dlq_client.connect()

        router.register_client(src_client)
        router.register_client(tgt_client)
        router.register_client(dlq_client)
        router.set_dlq("dlq", "system/dlq")

        router.add_route("src", "telemetry/+", "tgt", "downstream/telemetry")

        ack_1 = []
        ack_2 = []

        # First message fills buffer (capacity=1)
        router.route("src", "telemetry/1", b"msg_1", ack_fn=lambda: ack_1.append(True))
        self.assertEqual(tgt_client.publish_buffer_size, 1)
        self.assertEqual(ack_1, [])
        self.assertEqual(len(dlq_client.published), 0)

        # Second message overflows buffer -> rejected -> routed to DLQ and acknowledged upstream
        router.route("src", "telemetry/2", b"msg_2", ack_fn=lambda: ack_2.append(True))
        self.assertEqual(tgt_client.publish_buffer_size, 1)
        self.assertEqual(len(dlq_client.published), 1)
        self.assertEqual(dlq_client.published[0][0], "system/dlq")
        self.assertEqual(ack_2, [True])


if __name__ == "__main__":
    unittest.main()
