import unittest
from unittest.mock import MagicMock, patch
from postoffice.router import Router
from postoffice.plugins.kafka_client import KafkaClient
from postoffice.plugins.amqp_client import AmqpClient
from postoffice.plugins.mqtt_client import MqttClient

class TestDeliveryGuarantees(unittest.TestCase):
    def setUp(self):
        self.router = Router()

    def test_single_target_ack(self):
        source_client = MagicMock()
        source_client.name = "ingress"
        target_client = MagicMock()
        target_client.name = "egress"

        # Mock publish to immediately invoke on_confirm callback
        def fake_publish(topic, msg, on_confirm=None, on_error=None, **kwargs):
            if on_confirm:
                on_confirm()

        target_client.publish.side_effect = fake_publish

        self.router.register_client(source_client)
        self.router.register_client(target_client)
        self.router.add_route("ingress", "in/topic", "egress", "out/topic")

        ack_fn = MagicMock()
        nack_fn = MagicMock()

        self.router.route("ingress", "in/topic", b"hello", ack_fn=ack_fn, nack_fn=nack_fn)

        ack_fn.assert_called_once()
        nack_fn.assert_not_called()

    def test_single_target_nack_on_error(self):
        source_client = MagicMock()
        source_client.name = "ingress"
        target_client = MagicMock()
        target_client.name = "egress"

        def fake_publish(topic, msg, on_confirm=None, on_error=None, **kwargs):
            if on_error:
                on_error(RuntimeError("Publish failed"))

        target_client.publish.side_effect = fake_publish

        self.router.register_client(source_client)
        self.router.register_client(target_client)
        self.router.add_route("ingress", "in/topic", "egress", "out/topic")

        ack_fn = MagicMock()
        nack_fn = MagicMock()

        self.router.route("ingress", "in/topic", b"hello", ack_fn=ack_fn, nack_fn=nack_fn)

        ack_fn.assert_not_called()
        nack_fn.assert_called_once_with(requeue=True)

    def test_fanout_latch_all_succeed(self):
        source_client = MagicMock()
        source_client.name = "ingress"
        t1 = MagicMock()
        t1.name = "egress_1"
        t2 = MagicMock()
        t2.name = "egress_2"

        saved_callbacks = []

        def fake_publish_defer(topic, msg, on_confirm=None, on_error=None, **kwargs):
            saved_callbacks.append((on_confirm, on_error))

        t1.publish.side_effect = fake_publish_defer
        t2.publish.side_effect = fake_publish_defer

        self.router.register_client(source_client)
        self.router.register_client(t1)
        self.router.register_client(t2)
        self.router.add_route("ingress", "in/topic", "egress_1", "out/1")
        self.router.add_route("ingress", "in/topic", "egress_2", "out/2")

        ack_fn = MagicMock()
        nack_fn = MagicMock()

        self.router.route("ingress", "in/topic", b"hello", ack_fn=ack_fn, nack_fn=nack_fn)

        self.assertEqual(len(saved_callbacks), 2)
        # First target confirms; ack should NOT be called yet
        saved_callbacks[0][0]()
        ack_fn.assert_not_called()

        # Second target confirms; now ack should be called
        saved_callbacks[1][0]()
        ack_fn.assert_called_once()
        nack_fn.assert_not_called()

    def test_fanout_latch_any_fail(self):
        source_client = MagicMock()
        source_client.name = "ingress"
        t1 = MagicMock()
        t1.name = "egress_1"
        t2 = MagicMock()
        t2.name = "egress_2"

        saved_callbacks = []

        def fake_publish_defer(topic, msg, on_confirm=None, on_error=None, **kwargs):
            saved_callbacks.append((on_confirm, on_error))

        t1.publish.side_effect = fake_publish_defer
        t2.publish.side_effect = fake_publish_defer

        self.router.register_client(source_client)
        self.router.register_client(t1)
        self.router.register_client(t2)
        self.router.add_route("ingress", "in/topic", "egress_1", "out/1")
        self.router.add_route("ingress", "in/topic", "egress_2", "out/2")

        ack_fn = MagicMock()
        nack_fn = MagicMock()

        self.router.route("ingress", "in/topic", b"hello", ack_fn=ack_fn, nack_fn=nack_fn)

        self.assertEqual(len(saved_callbacks), 2)
        # First target errors out
        saved_callbacks[0][1](RuntimeError("Disk full"))
        nack_fn.assert_called_once_with(requeue=True)

        # Second target confirms subsequently; should not trigger ack
        saved_callbacks[1][0]()
        ack_fn.assert_not_called()

    def test_fire_and_forget_backward_compatibility(self):
        source_client = MagicMock()
        source_client.name = "ingress"
        target_client = MagicMock()
        target_client.name = "egress"

        self.router.register_client(source_client)
        self.router.register_client(target_client)
        self.router.add_route("ingress", "in/topic", "egress", "out/topic", extra="param")

        self.router.route("ingress", "in/topic", b"hello")

        target_client.publish.assert_called_once_with("out/topic", b"hello", extra="param")

    def test_semantic_translation_mqtt_to_amqp(self):
        source_client = MagicMock()
        source_client.name = "mqtt_ingress"
        target_client = MagicMock()
        target_client.name = "amqp_egress"

        self.router.register_client(source_client)
        self.router.register_client(target_client)
        self.router.add_route("mqtt_ingress", "sensor/temp", "amqp_egress", "sensor_queue")

        # Ingress QoS 1 -> target delivery_mode = 2 (persistent)
        self.router.route("mqtt_ingress", "sensor/temp", b"22C", qos=1)
        target_client.publish.assert_called_with("sensor_queue", b"22C", delivery_mode=2)

        # Ingress QoS 2 -> target delivery_mode = 2 (persistent)
        self.router.route("mqtt_ingress", "sensor/temp", b"23C", qos=2)
        target_client.publish.assert_called_with("sensor_queue", b"23C", delivery_mode=2)

        # Ingress QoS 0 -> target delivery_mode = 1 (transient)
        self.router.route("mqtt_ingress", "sensor/temp", b"24C", qos=0)
        target_client.publish.assert_called_with("sensor_queue", b"24C", delivery_mode=1)

    def test_semantic_translation_amqp_to_mqtt(self):
        source_client = MagicMock()
        source_client.name = "amqp_ingress"
        target_client = MagicMock()
        target_client.name = "mqtt_egress"

        self.router.register_client(source_client)
        self.router.register_client(target_client)
        self.router.add_route("amqp_ingress", "orders", "mqtt_egress", "orders/updates")

        # Ingress AMQP persistent (2) -> target MQTT QoS 1
        self.router.route("amqp_ingress", "orders", b"order_1", delivery_mode=2)
        target_client.publish.assert_called_with("orders/updates", b"order_1", qos=1)

        # Ingress AMQP transient (1) -> target MQTT QoS 0
        self.router.route("amqp_ingress", "orders", b"order_2", delivery_mode=1)
        target_client.publish.assert_called_with("orders/updates", b"order_2", qos=0)

    def test_semantic_translation_explicit_override(self):
        source_client = MagicMock()
        source_client.name = "mqtt_ingress"
        target_client = MagicMock()
        target_client.name = "amqp_egress"

        self.router.register_client(source_client)
        self.router.register_client(target_client)
        # Route explicitly sets delivery_mode=1 despite high QoS source
        self.router.add_route("mqtt_ingress", "sensor/temp", "amqp_egress", "sensor_queue", delivery_mode=1)

        self.router.route("mqtt_ingress", "sensor/temp", b"22C", qos=1)
        target_client.publish.assert_called_with("sensor_queue", b"22C", delivery_mode=1)

    @patch('postoffice.plugins.kafka_client.Producer')
    def test_kafka_delivery_report_triggers_callbacks(self, MockProducer):
        client = KafkaClient("kafka_test", self.router)
        client.producer = MockProducer.return_value

        saved_callback = []
        def fake_produce(**kwargs):
            saved_callback.append(kwargs["callback"])

        client.producer.produce.side_effect = fake_produce

        on_confirm = MagicMock()
        on_error = MagicMock()

        # Publish and trigger success
        client.publish("test_topic", b"data", on_confirm=on_confirm, on_error=on_error)
        self.assertEqual(len(saved_callback), 1)
        saved_callback[0](None, MagicMock())
        on_confirm.assert_called_once()
        on_error.assert_not_called()

        # Publish and trigger failure
        on_confirm.reset_mock()
        on_error.reset_mock()
        client.publish("test_topic", b"data", on_confirm=on_confirm, on_error=on_error)
        saved_callback[1]("Simulated broker delivery error", MagicMock())
        on_confirm.assert_not_called()
        on_error.assert_called_once_with("Simulated broker delivery error")

    def test_amqp_client_publish_callbacks(self):
        client = AmqpClient("amqp_test", self.router)
        mock_channel = MagicMock()
        client.channel = mock_channel
        mock_conn = MagicMock()
        client.connection = mock_conn

        # Execute scheduled callback immediately
        mock_conn.add_callback_threadsafe.side_effect = lambda fn: fn()

        on_confirm = MagicMock()
        on_error = MagicMock()

        client.publish("test_queue", b"payload", on_confirm=on_confirm, on_error=on_error)

        mock_channel.basic_publish.assert_called_once()
        on_confirm.assert_called_once()
        on_error.assert_not_called()

    def test_mqtt_client_publish_callbacks(self):
        client = MqttClient("mqtt_test", self.router)
        client.client.is_connected = MagicMock(return_value=True)
        client.client.publish = MagicMock()

        on_confirm = MagicMock()
        on_error = MagicMock()

        client.publish("test_topic", b"payload", on_confirm=on_confirm, on_error=on_error)

        client.client.publish.assert_called_once_with("test_topic", b"payload", qos=0, retain=False)
        on_confirm.assert_called_once()
        on_error.assert_not_called()

if __name__ == '__main__':
    unittest.main()
