import unittest
from unittest.mock import MagicMock, patch
from postoffice.router import Router
from postoffice.registry import ClientRegistry
import postoffice.plugins # Triggers registration

class TestPostOfficeScaffold(unittest.TestCase):
    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    @patch('postoffice.plugins.amqp_client.pika.BlockingConnection')
    @patch('postoffice.plugins.kafka_client.Consumer')
    @patch('postoffice.plugins.kafka_client.Producer')
    def test_routing_initialization(self, MockProducer, MockConsumer, MockPika, MockMqtt):
        router = Router()

        # Test registry instantiation
        mqtt_client = ClientRegistry.create_client("mqtt", "mqtt_1", router)
        amqp_client = ClientRegistry.create_client("amqp", "amqp_1", router)
        kafka_client = ClientRegistry.create_client("kafka", "kafka_1", router)
        nanomq_client = ClientRegistry.create_client("nanomq", "nano_1", router)

        router.register_client(mqtt_client)
        router.register_client(amqp_client)
        router.register_client(kafka_client)

        router.add_route("mqtt_1", "sensor/data", "kafka_1", "sensor_events", key=b"iot", partition=1)
        router.add_route("kafka_1", "sensor_events", "amqp_1", "amqp_queue", exchange="test_exchange", delivery_mode=2)

        self.assertEqual(len(router.clients), 3)
        self.assertIn(("mqtt_1", "sensor/data"), router.routes)
        self.assertIn(("kafka_1", "sensor_events"), router.routes)

        # Test routing behavior (MQTT -> Kafka with specific Kafka kwargs)
        kafka_client.publish = MagicMock()
        router.route("mqtt_1", "sensor/data", b'{"temp": 25}')
        kafka_client.publish.assert_called_once_with("sensor_events", b'{"temp": 25}', key=b"iot", partition=1)

        # Test routing behavior (Kafka -> AMQP with specific AMQP kwargs)
        amqp_client.publish = MagicMock()
        router.route("kafka_1", "sensor_events", b'{"temp": 25}')
        amqp_client.publish.assert_called_once_with("amqp_queue", b'{"temp": 25}', exchange="test_exchange", delivery_mode=2)

if __name__ == '__main__':
    unittest.main()
