import unittest
from unittest.mock import MagicMock, patch
from postoffice.app import PostOffice

class TestPostOfficeScaffold(unittest.TestCase):
    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    @patch('postoffice.plugins.amqp_client.pika.BlockingConnection')
    @patch('postoffice.plugins.kafka_client.Consumer')
    @patch('postoffice.plugins.kafka_client.Producer')
    def test_routing_initialization(self, MockProducer, MockConsumer, MockPika, MockMqtt):
        po = PostOffice()

        # Test registry instantiation via uniform facade
        po.add_broker("mqtt_1", "mqtt")
        po.add_broker("amqp_1", "amqp")
        po.add_broker("kafka_1", "kafka")
        po.add_broker("nano_1", "nanomq")

        po.add_route("mqtt_1", "sensor/data", "kafka_1", "sensor_events", key=b"iot", partition=1)
        po.add_route("kafka_1", "sensor_events", "amqp_1", "amqp_queue", exchange="test_exchange", delivery_mode=2)

        self.assertEqual(len(po.brokers), 4)
        self.assertEqual(len(po.router.clients), 4)
        self.assertIn(("mqtt_1", "sensor/data"), po.router.routes)
        self.assertIn(("kafka_1", "sensor_events"), po.router.routes)

        # Grab specific instances to test routing mock assertions
        kafka_client = po.brokers["kafka_1"]
        amqp_client = po.brokers["amqp_1"]

        # Test routing behavior (MQTT -> Kafka with specific Kafka kwargs)
        kafka_client.publish = MagicMock()
        po.router.route("mqtt_1", "sensor/data", b'{"temp": 25}')
        kafka_client.publish.assert_called_once_with("sensor_events", b'{"temp": 25}', key=b"iot", partition=1)

        # Test routing behavior (Kafka -> AMQP with specific AMQP kwargs)
        amqp_client.publish = MagicMock()
        po.router.route("kafka_1", "sensor_events", b'{"temp": 25}')
        amqp_client.publish.assert_called_once_with("amqp_queue", b'{"temp": 25}', exchange="test_exchange", delivery_mode=2)

    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    def test_facade_lifecycle_and_validation_guards(self, MockMqtt):
        po = PostOffice(dlq_broker="mqtt_1", dlq_topic="dlq/topic")
        po.set_metrics_port(None)

        po.add_broker("mqtt_1", "mqtt", circuit_breaker_threshold=3, circuit_breaker_timeout=10.0)
        self.assertIn("mqtt_1", po.router.circuit_breakers)

        # Duplicate broker registration raises ValueError
        with self.assertRaises(ValueError):
            po.add_broker("mqtt_1", "mqtt")

        # Invalid broker names in add_route / subscribe / reload_broker_security raise ValueError
        with self.assertRaises(ValueError):
            po.add_route("missing_src", "t1", "mqtt_1", "t2")
        with self.assertRaises(ValueError):
            po.add_route("mqtt_1", "t1", "missing_tgt", "t2")
        with self.assertRaises(ValueError):
            po.subscribe("missing_broker", "t1")
        with self.assertRaises(ValueError):
            po.reload_broker_security("missing_broker", {})

        # Start PostOffice -> connects registered brokers
        po.start()
        self.assertTrue(po.is_running)

        # Adding a broker while is_running=True connects it immediately
        po.add_broker("mqtt_2", "mqtt")

        # Subscribe via facade
        po.subscribe("mqtt_1", "sensors/#", qos=1)

        # Reset and stop
        po.reset()
        self.assertFalse(po.is_running)
        self.assertEqual(len(po.brokers), 0)

if __name__ == '__main__':
    unittest.main()
