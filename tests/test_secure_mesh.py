import unittest
import json
from unittest.mock import MagicMock, patch
from postoffice.app import PostOffice
from postoffice.security import SecurityConfig
from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane


class TestSecureMesh(unittest.TestCase):
    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    @patch('postoffice.plugins.amqp_client.pika.BlockingConnection')
    @patch('postoffice.plugins.kafka_client.Consumer')
    @patch('postoffice.plugins.kafka_client.Producer')
    def test_facade_secure_mesh_routing(self, MockProducer, MockConsumer, MockPika, MockMqtt):
        po = PostOffice()

        sec_mqtt = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            client_cert="/certs/client.pem",
            client_key="/certs/client.key",
        )
        sec_kafka = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            username="alice",
            password="secret_password",
            sasl_mechanism="SCRAM-SHA-512",
        )
        sec_amqp = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            username="bob",
            password="rabbit_password",
        )

        # 1. Register secure brokers via facade
        po.add_broker("mqtt_sec", "mqtt", host="mqtt.internal", security_config=sec_mqtt)
        po.add_broker("kafka_sec", "kafka", bootstrap_servers="kafka.internal:9094", security_config=sec_kafka)
        po.add_broker("amqp_sec", "amqp", host="amqp.internal", security_config=sec_amqp)

        # Verify secure properties on instantiated plugins
        self.assertTrue(po.brokers["mqtt_sec"].security.tls_enabled)
        self.assertEqual(po.brokers["mqtt_sec"].port, 8883)

        self.assertTrue(po.brokers["kafka_sec"].security.tls_enabled)
        self.assertEqual(po.brokers["kafka_sec"].security.sasl_mechanism, "SCRAM-SHA-512")

        self.assertTrue(po.brokers["amqp_sec"].security.tls_enabled)
        self.assertEqual(po.brokers["amqp_sec"].port, 5671)

        # 2. Add secure routes
        po.add_route("mqtt_sec", "factory/sensors/temperature", "kafka_sec", "factory.telemetry", key=b"sensor-1")
        po.add_route("kafka_sec", "factory.telemetry", "amqp_sec", "alerts_queue", delivery_mode=2)

        kafka_client = po.brokers["kafka_sec"]
        amqp_client = po.brokers["amqp_sec"]
        kafka_client.publish = MagicMock()
        amqp_client.publish = MagicMock()

        # 3. Route from MQTT -> Kafka
        po.router.route("mqtt_sec", "factory/sensors/temperature", b'{"temp_c": 42.5}')
        kafka_client.publish.assert_called_once_with(
            "factory.telemetry", b'{"temp_c": 42.5}', key=b"sensor-1"
        )

        # 4. Route from Kafka -> AMQP
        po.router.route("kafka_sec", "factory.telemetry", b'{"alert": "high_temperature"}')
        amqp_client.publish.assert_called_once_with(
            "alerts_queue", b'{"alert": "high_temperature"}', delivery_mode=2
        )

    def test_control_plane_security_serialization(self):
        cp = ControlPlane.__new__(ControlPlane)
        cp.r = MagicMock()
        cp.r.incr.return_value = 1
        cp.channel = "postoffice:config_updates"

        sec = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            client_cert="/certs/client.pem",
            client_key="/certs/client.key",
            username="admin",
            password="secret_pass",
            sasl_mechanism="SCRAM-SHA-256",
        )

        # Register broker with SecurityConfig object in kwargs
        cp.register_broker("secure_kafka", "kafka", bootstrap_servers="kafka:9094", security_config=sec)

        # Verify Redis hset called with valid JSON
        self.assertTrue(cp.r.hset.called)
        call_args = cp.r.hset.call_args[0]
        self.assertEqual(call_args[0], "postoffice:brokers")
        self.assertEqual(call_args[1], "secure_kafka")
        stored_json = json.loads(call_args[2])

        self.assertEqual(stored_json["protocol"], "kafka")
        self.assertIn("security_config", stored_json["kwargs"])
        self.assertEqual(stored_json["kwargs"]["security_config"]["tls_enabled"], True)
        self.assertEqual(stored_json["kwargs"]["security_config"]["sasl_mechanism"], "SCRAM-SHA-256")

        # Verify Redis publish called with valid JSON
        self.assertTrue(cp.r.publish.called)
        pub_payload = json.loads(cp.r.publish.call_args[0][1])
        self.assertEqual(pub_payload["action"], "add_broker")
        self.assertEqual(pub_payload["data"]["kwargs"]["security_config"]["username"], "admin")

    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    def test_data_plane_sync_secure_broker(self, MockMqtt):
        mock_redis = MagicMock()
        sec_dict = {
            "tls_enabled": True,
            "ca_cert": "/certs/ca.pem",
            "username": "dp_user",
            "password": "dp_password",
        }
        broker_config = {
            "protocol": "mqtt",
            "kwargs": {
                "host": "mqtt.cloud",
                "security_config": sec_dict
            }
        }
        mock_redis.hgetall.side_effect = [
            {"mqtt_dp": json.dumps(broker_config)},  # brokers
            {},                                      # routes
            {}                                       # subscriptions
        ]

        dp = DataPlane.__new__(DataPlane)
        dp._init_convergence_state()
        dp.r = mock_redis
        dp.po = PostOffice()
        dp.channel = "postoffice:config_updates"
        dp._stop_event = MagicMock()
        dp._stop_event.is_set.return_value = False

        dp.sync_state()

        self.assertIn("mqtt_dp", dp.po.brokers)
        synced_broker = dp.po.brokers["mqtt_dp"]
        self.assertTrue(synced_broker.security.tls_enabled)
        self.assertEqual(synced_broker.security.ca_cert, "/certs/ca.pem")
        self.assertEqual(synced_broker.security.username, "dp_user")


if __name__ == '__main__':
    unittest.main()
