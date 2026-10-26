import json
import unittest
from unittest.mock import MagicMock, patch
from postoffice.app import PostOffice
from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane
from postoffice.security import SecurityConfig
from postoffice.plugins.mqtt_client import MqttClient
from postoffice.plugins.nanomq_client import NanoMqClient
from postoffice.plugins.amqp_client import AmqpClient
from postoffice.plugins.kafka_client import KafkaClient


class TestPluginSecurityReload(unittest.TestCase):
    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    def test_mqtt_reload_security_while_connected_preserves_subscriptions(self, MockMqtt):
        paho_1 = MagicMock()
        paho_2 = MagicMock()
        paho_1.is_connected.return_value = True
        paho_2.is_connected.return_value = True
        MockMqtt.side_effect = [paho_1, paho_2]

        router = MagicMock()
        client = MqttClient(
            "mqtt_hot",
            router,
            security_config=SecurityConfig(username="old_user", password="old_password"),
        )
        client.subscribe("sensors/+/temp", qos=2)
        paho_1.username_pw_set.assert_called_once_with("old_user", "old_password")

        # Rotate credentials and enable mTLS in-flight
        new_sec = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/new_ca.pem",
            client_cert="/certs/new_client.pem",
            client_key="/certs/new_client.key",
            username="new_user",
            password="new_password",
        )
        client.reload_security(new_sec)

        # Verify old connection stopped/disconnected and new client configured + connected on TLS port 8883
        paho_1.loop_stop.assert_called_once()
        paho_1.disconnect.assert_called_once()

        self.assertEqual(client.port, 8883)
        paho_2.username_pw_set.assert_called_once_with("new_user", "new_password")
        paho_2.tls_set.assert_called_once()
        paho_2.connect.assert_called_once_with("localhost", 8883, 60)
        paho_2.loop_start.assert_called_once()

        # Simulate _on_connect on new paho client -> subscriptions replayed with qos=2
        client._on_connect(paho_2, None, None, 0, None)
        paho_2.subscribe.assert_called_once_with("sensors/+/temp", qos=2)

    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    def test_nanomq_reload_security_shifts_default_port(self, MockMqtt):
        mock_paho = MockMqtt.return_value
        mock_paho.is_connected.return_value = False

        router = MagicMock()
        nano = NanoMqClient("nano_hot", router)
        self.assertEqual(nano.port, 1884)

        nano.reload_security(SecurityConfig(tls_enabled=True, ca_cert="/certs/ca.pem"))
        self.assertEqual(nano.port, 8884)
        self.assertTrue(nano.security.tls_enabled)

    @patch("postoffice.plugins.amqp_client.pika.BlockingConnection")
    def test_amqp_reload_security_updates_credentials_and_reconnects(self, MockBlockingConn):
        router = MagicMock()
        client = AmqpClient(
            "amqp_hot",
            router,
            security_config=SecurityConfig(username="u1", password="p1"),
        )
        client.subscribe("jobs_queue", exchange="work_ex", auto_ack=False)

        # Simulate active state
        mock_conn = MagicMock()
        mock_conn.is_open = True
        client.connection = mock_conn
        client.channel = MagicMock()

        with patch.object(client, "connect") as mock_connect, patch.object(
            client, "disconnect", wraps=client.disconnect
        ) as spy_disconnect:
            new_sec = SecurityConfig(
                tls_enabled=True,
                ca_cert="/certs/ca.pem",
                username="u2",
                password="p2",
            )
            client.reload_security(new_sec)

            spy_disconnect.assert_called_once()
            mock_connect.assert_called_once()
            self.assertEqual(client.user, "u2")
            self.assertEqual(client.password, "p2")
            self.assertEqual(client.port, 5671)
            self.assertIn("jobs_queue", client.get_subscriptions())

    @patch("postoffice.plugins.kafka_client.Consumer")
    @patch("postoffice.plugins.kafka_client.Producer")
    def test_kafka_reload_security_recreates_clients_and_restores_subscriptions(self, MockProducer, MockConsumer):
        prod_1, prod_2 = MagicMock(), MagicMock()
        cons_1, cons_2 = MagicMock(), MagicMock()
        MockProducer.side_effect = [prod_1, prod_2]
        MockConsumer.side_effect = [cons_1, cons_2]

        # Make consume loop exit after 1 poll
        cons_1.poll.side_effect = lambda t: None
        cons_2.poll.side_effect = lambda t: None

        router = MagicMock()
        client = KafkaClient(
            "kafka_hot",
            router,
            security_config=SecurityConfig(
                username="kuser1", password="kpass1", sasl_mechanism="PLAIN"
            ),
        )
        client.subscribe("stream.in")
        client._stop_event.set()  # prevent infinite background loop during initial connect
        client.connect()
        if client.thread:
            client.thread.join(timeout=1.0)

        # Simulate that topic was already moved into subscribed_topics
        client.subscribed_topics = ["stream.in"]
        client._pending_subscriptions = []

        # Rotate to SCRAM-SHA-512 with TLS
        new_sec = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            username="kuser2",
            password="kpass2",
            sasl_mechanism="SCRAM-SHA-512",
        )

        # Stop loop on first poll of cons_2
        def stop_on_poll(timeout):
            client._stop_event.set()
            return None
        cons_2.poll.side_effect = stop_on_poll

        client.reload_security(new_sec)
        if client.thread:
            client.thread.join(timeout=1.0)

        cons_1.close.assert_called_once()
        prod_1.flush.assert_called_once()
        self.assertEqual(client.security.username, "kuser2")
        self.assertEqual(client.security.sasl_mechanism, "SCRAM-SHA-512")
        cons_2.subscribe.assert_called_once_with(["stream.in"])


class TestDistributedCredentialReload(unittest.TestCase):
    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    def test_control_plane_to_data_plane_live_security_reload(self, MockMqtt):
        # 1. Setup ControlPlane with mocked Redis
        cp = ControlPlane.__new__(ControlPlane)
        cp.r = MagicMock()
        cp.r.incr.return_value = 1
        cp.channel = "postoffice:config_updates"

        existing_broker_json = json.dumps({
            "protocol": "mqtt",
            "kwargs": {
                "host": "mqtt.edge",
                "security_config": SecurityConfig(username="init_user", password="init_pass").to_dict(),
            },
        })
        cp.r.hget.return_value = existing_broker_json

        # 2. Setup DataPlane with live PostOffice instance and registered broker
        dp = DataPlane.__new__(DataPlane)
        dp._init_convergence_state()
        dp.r = MagicMock()
        dp.po = PostOffice()
        dp.channel = "postoffice:config_updates"
        dp._stop_event = MagicMock()

        dp.po.add_broker(
            "mqtt_edge",
            "mqtt",
            host="mqtt.edge",
            security_config=SecurityConfig(username="init_user", password="init_pass"),
        )

        # 3. Rotate credentials via ControlPlane
        rotated_sec = SecurityConfig(
            tls_enabled=True,
            ca_cert="/etc/ssl/ca.pem",
            client_cert="/etc/ssl/client.pem",
            client_key="/etc/ssl/client.key",
            username="rotated_user",
            password="rotated_pass",
        )
        cp.update_broker_security("mqtt_edge", rotated_sec)

        # Verify Redis hset updated the stored broker configuration
        cp.r.hset.assert_called_once()
        saved_cfg = json.loads(cp.r.hset.call_args[0][2])
        self.assertEqual(saved_cfg["kwargs"]["security_config"]["username"], "rotated_user")
        self.assertTrue(saved_cfg["kwargs"]["security_config"]["tls_enabled"])

        # Capture the Pub/Sub message published by ControlPlane and feed it to DataPlane
        published_data = cp.r.publish.call_args[0][1]
        dp._process_update_message({"type": "message", "data": published_data})

        # Verify DataPlane broker hot-reloaded security in-place without restarting PostOffice
        edge_broker = dp.po.brokers["mqtt_edge"]
        self.assertTrue(edge_broker.security.tls_enabled)
        self.assertEqual(edge_broker.security.username, "rotated_user")
        self.assertEqual(edge_broker.security.client_cert, "/etc/ssl/client.pem")
        self.assertEqual(edge_broker.port, 8883)


if __name__ == "__main__":
    unittest.main()
