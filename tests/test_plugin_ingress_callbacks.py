import unittest
from unittest.mock import MagicMock, patch
from confluent_kafka import KafkaError
from postoffice.plugins.amqp_client import AmqpClient
from postoffice.plugins.kafka_client import KafkaClient
from postoffice.plugins.mqtt_client import MqttClient


class TestPluginIngressCallbacks(unittest.TestCase):
    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    def test_mqtt_on_message_forwards_topic_payload_and_qos(self, MockMqtt):
        router = MagicMock()
        client = MqttClient("mqtt_ing", router)

        mock_msg = MagicMock()
        mock_msg.topic = "devices/d1/status"
        mock_msg.payload = b'{"online": true}'
        mock_msg.qos = 2

        client._on_message(client.client, None, mock_msg)
        router.route.assert_called_once_with(
            "mqtt_ing", "devices/d1/status", b'{"online": true}', ack_fn=None, nack_fn=None, qos=2
        )

    @patch("postoffice.plugins.amqp_client.pika.BlockingConnection")
    def test_amqp_on_message_manual_ack_and_nack_threadsafe_and_closed_fallback(self, MockConn):
        router = MagicMock()
        client = AmqpClient("amqp_ing", router)

        mock_conn = MagicMock()
        mock_conn.is_open = True
        mock_conn.add_callback_threadsafe.side_effect = lambda cb: cb()
        client.connection = mock_conn

        ch = MagicMock()
        method = MagicMock()
        method.delivery_tag = 77
        method.exchange = "telemetry_ex"
        method.routing_key = "sensors.temp"

        properties = MagicMock()
        properties.delivery_mode = 2

        # 1. Invoke _on_message with auto_ack=False
        client._on_message(ch, method, properties, b"amqp_body", auto_ack=False)

        self.assertTrue(router.route.called)
        args, kwargs = router.route.call_args
        self.assertEqual(args, ("amqp_ing", "sensors.temp", b"amqp_body"))
        self.assertEqual(kwargs["exchange"], "telemetry_ex")
        self.assertEqual(kwargs["delivery_mode"], 2)

        ack_fn = kwargs["ack_fn"]
        nack_fn = kwargs["nack_fn"]
        self.assertIsNotNone(ack_fn)
        self.assertIsNotNone(nack_fn)

        # 2. Execute ack_fn while connection is open -> schedules basic_ack via add_callback_threadsafe
        ack_fn()
        ch.basic_ack.assert_called_once_with(delivery_tag=77)

        # 3. Execute nack_fn(requeue=False) while connection is open -> schedules basic_nack
        nack_fn(requeue=False)
        ch.basic_nack.assert_called_once_with(delivery_tag=77, requeue=False)

        # 4. When connection.is_open is False -> direct fallback on channel
        ch.reset_mock()
        mock_conn.is_open = False
        ack_fn()
        ch.basic_ack.assert_called_once_with(delivery_tag=77)
        nack_fn(requeue=True)
        ch.basic_nack.assert_called_once_with(delivery_tag=77, requeue=True)

        # 5. Exception in basic_ack / basic_nack is caught and logged cleanly
        ch.basic_ack.side_effect = RuntimeError("Channel closed")
        ch.basic_nack.side_effect = RuntimeError("Channel closed")
        ack_fn()
        nack_fn()

    @patch("postoffice.plugins.kafka_client.Consumer")
    @patch("postoffice.plugins.kafka_client.Producer")
    def test_kafka_consume_loop_dispatches_message_and_commits_offset(self, MockProducer, MockConsumer):
        mock_cons = MockConsumer.return_value
        router = MagicMock()
        client = KafkaClient("kafka_ing", router)
        client.consumer = mock_cons

        # Prepare sequence of poll results:
        # 1) Non-EOF KafkaError
        err_msg = MagicMock()
        err_obj = MagicMock()
        err_obj.code.return_value = KafkaError._ALL_BROKERS_DOWN
        err_msg.error.return_value = err_obj

        # 2) Valid message
        valid_msg = MagicMock()
        valid_msg.error.return_value = None
        valid_msg.topic.return_value = "orders.stream"
        valid_msg.value.return_value = b'{"order_id": 101}'
        valid_msg.key.return_value = b"k101"
        valid_msg.partition.return_value = 0
        valid_msg.offset.return_value = 42

        def poll_sequence(timeout):
            if not hasattr(poll_sequence, "step"):
                poll_sequence.step = 0
            poll_sequence.step += 1
            if poll_sequence.step == 1:
                return err_msg
            if poll_sequence.step == 2:
                client._stop_event.set()
                return valid_msg
            return None

        mock_cons.poll.side_effect = poll_sequence

        client._consume_loop()

        self.assertEqual(router.route.call_count, 1)
        args, kwargs = router.route.call_args
        self.assertEqual(args, ("kafka_ing", "orders.stream", b'{"order_id": 101}'))
        self.assertEqual(kwargs["key"], b"k101")
        self.assertEqual(kwargs["partition"], 0)
        self.assertEqual(kwargs["offset"], 42)

        ack_fn = kwargs["ack_fn"]
        self.assertIsNotNone(ack_fn)
        ack_fn()
        mock_cons.commit.assert_called_once_with(message=valid_msg, asynchronous=True)

        # Verify exception in commit is caught cleanly
        mock_cons.commit.side_effect = RuntimeError("Commit failed")
        ack_fn()

    @patch("postoffice.plugins.amqp_client.pika.BlockingConnection")
    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    @patch("postoffice.plugins.kafka_client.Consumer")
    @patch("postoffice.plugins.kafka_client.Producer")
    def test_plugin_connected_subscribe_and_error_branches(
        self, MockProducer, MockConsumer, MockMqtt, MockConn
    ):
        router = MagicMock()

        # 1. AmqpClient connected subscribe & publish error branch
        amqp = AmqpClient("amqp_err", router, port=5679)
        self.assertEqual(amqp.port, 5679)
        mock_conn = MagicMock()
        mock_conn.is_open = True
        mock_conn.add_callback_threadsafe.side_effect = lambda cb: cb()
        mock_ch = MagicMock()
        amqp.connection = mock_conn
        amqp.channel = mock_ch

        amqp.subscribe("live.topic", exchange="ex1", queue="q1")
        mock_ch.exchange_declare.assert_called_once_with(exchange="ex1", exchange_type="direct")

        mock_ch.basic_publish.side_effect = RuntimeError("AMQP publish boom")
        pub_errors = []
        amqp.publish("live.topic", b"msg", exchange="ex1", on_error=lambda e: pub_errors.append(e))
        self.assertEqual(len(pub_errors), 1)

        # Disconnect when connection not open / no thread
        amqp.reload_security({"tls_enabled": False})

        # 2. MqttClient connect refused & publish error branch
        mock_paho = MockMqtt.return_value
        mock_paho.is_connected.return_value = True
        mqtt_c = MqttClient("mqtt_err", router, port=1889)
        self.assertEqual(mqtt_c.port, 1889)
        mqtt_c.subscribe("live/mqtt", qos=1)
        mock_paho.subscribe.assert_called_once_with("live/mqtt", qos=1)

        mock_paho.connect.side_effect = ConnectionRefusedError("Refused")
        mqtt_c.connect()

        mock_paho.publish.side_effect = RuntimeError("MQTT publish boom")
        mqtt_errors = []
        with self.assertRaises(RuntimeError):
            mqtt_c.publish("live/mqtt", b"msg", on_error=lambda e: mqtt_errors.append(e))
        self.assertEqual(len(mqtt_errors), 1)

        # 3. KafkaClient publish when disconnected (buffering disabled) & produce exception
        kafka_c = KafkaClient("kafka_err", router, enable_publish_buffer=False)
        k_errors = []
        kafka_c.publish("t1", b"m1", on_error=lambda e: k_errors.append(e))
        self.assertEqual(len(k_errors), 1)

        mock_prod = MockProducer.return_value
        kafka_c.producer = mock_prod
        mock_prod.produce.side_effect = RuntimeError("Kafka produce boom")
        with self.assertRaises(RuntimeError):
            kafka_c.publish("t1", b"m1", on_error=lambda e: k_errors.append(e))
        self.assertEqual(len(k_errors), 2)

    @patch("postoffice.worker.DataPlane")
    def test_worker_entrypoint(self, MockDataPlane):
        from postoffice.worker import main
        main()
        MockDataPlane.return_value.run.assert_called_once()


if __name__ == "__main__":
    unittest.main()
