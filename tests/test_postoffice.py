import unittest
from unittest.mock import MagicMock, patch
from postoffice.router import Router
from postoffice.mqtt_client import MqttClient
from postoffice.amqp_client import AmqpClient
from postoffice.kafka_client import KafkaClient

class TestPostOfficeScaffold(unittest.TestCase):
    @patch('postoffice.mqtt_client.mqtt.Client')
    @patch('postoffice.amqp_client.pika.BlockingConnection')
    @patch('postoffice.kafka_client.Consumer')
    @patch('postoffice.kafka_client.Producer')
    def test_routing_initialization(self, MockProducer, MockConsumer, MockPika, MockMqtt):
        router = Router()

        mqtt_client = MqttClient("mqtt_1", router)
        amqp_client = AmqpClient("amqp_1", router)
        kafka_client = KafkaClient("kafka_1", router)

        router.register_client(mqtt_client)
        router.register_client(amqp_client)
        router.register_client(kafka_client)

        router.add_route("mqtt_1", "sensor/data", "kafka_1", "sensor_events")

        self.assertEqual(len(router.clients), 3)
        self.assertIn(("mqtt_1", "sensor/data"), router.routes)

        # Test routing behavior
        kafka_client.publish = MagicMock()

        router.route("mqtt_1", "sensor/data", b'{"temp": 25}')

        kafka_client.publish.assert_called_once_with("sensor_events", b'{"temp": 25}')

if __name__ == '__main__':
    unittest.main()
