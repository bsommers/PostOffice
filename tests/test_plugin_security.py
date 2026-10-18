import unittest
import ssl
from unittest.mock import MagicMock, patch
from postoffice.security import SecurityConfig
from postoffice.plugins.mqtt_client import MqttClient
from postoffice.plugins.nanomq_client import NanoMqClient
from postoffice.plugins.amqp_client import AmqpClient
from postoffice.plugins.kafka_client import KafkaClient


class TestPluginSecurity(unittest.TestCase):
    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    def test_mqtt_plaintext_default(self, MockMqtt):
        mock_instance = MockMqtt.return_value
        router = MagicMock()
        client = MqttClient("mqtt_plain", router, host="localhost", port=1883)

        self.assertEqual(client.port, 1883)
        mock_instance.username_pw_set.assert_not_called()
        mock_instance.tls_set.assert_not_called()
        mock_instance.tls_insecure_set.assert_not_called()

    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    def test_mqtt_tls_and_auth(self, MockMqtt):
        mock_instance = MockMqtt.return_value
        router = MagicMock()
        sec = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            username="mqtt_user",
            password="mqtt_password",
        )
        client = MqttClient("mqtt_secure", router, host="localhost", security_config=sec)

        self.assertEqual(client.port, 8883)
        mock_instance.username_pw_set.assert_called_once_with("mqtt_user", "mqtt_password")
        mock_instance.tls_set.assert_called_once_with(
            ca_certs="/certs/ca.pem",
            certfile=None,
            keyfile=None,
            cert_reqs=ssl.CERT_REQUIRED,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        mock_instance.tls_insecure_set.assert_not_called()

    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    def test_mqtt_mtls_and_insecure_skip_verify(self, MockMqtt):
        mock_instance = MockMqtt.return_value
        router = MagicMock()
        sec = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            client_cert="/certs/client.pem",
            client_key="/certs/client.key",
            insecure_skip_verify=True,
        )
        client = MqttClient("mqtt_mtls", router, host="localhost", security_config=sec)

        mock_instance.tls_set.assert_called_once_with(
            ca_certs="/certs/ca.pem",
            certfile="/certs/client.pem",
            keyfile="/certs/client.key",
            cert_reqs=ssl.CERT_NONE,
            tls_version=ssl.PROTOCOL_TLS_CLIENT,
        )
        mock_instance.tls_insecure_set.assert_called_once_with(True)

    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    def test_nanomq_inherits_security(self, MockMqtt):
        mock_instance = MockMqtt.return_value
        router = MagicMock()
        sec = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            username="nano_user",
            password="nano_password",
        )
        client = NanoMqClient("nano_secure", router, host="localhost", security_config=sec)

        mock_instance.username_pw_set.assert_called_once_with("nano_user", "nano_password")
        mock_instance.tls_set.assert_called_once()

    @patch('postoffice.plugins.amqp_client.ssl.create_default_context')
    @patch('postoffice.plugins.amqp_client.pika.SSLOptions')
    @patch('postoffice.plugins.amqp_client.pika.PlainCredentials')
    @patch('postoffice.plugins.amqp_client.pika.ConnectionParameters')
    @patch('postoffice.plugins.amqp_client.pika.BlockingConnection')
    def test_amqp_tls_mtls(self, MockConn, MockParams, MockCreds, MockSSLOptions, MockSSLContext):
        mock_ctx = MagicMock()
        MockSSLContext.return_value = mock_ctx
        router = MagicMock()

        sec = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            client_cert="/certs/client.pem",
            client_key="/certs/client.key",
            client_key_password="keypass",
            insecure_skip_verify=True,
            username="rabbit_user",
            password="rabbit_password",
        )
        client = AmqpClient("amqp_secure", router, host="rabbit.internal", security_config=sec)
        client.connect()

        self.assertEqual(client.port, 5671)
        MockCreds.assert_called_with("rabbit_user", "rabbit_password")
        MockSSLContext.assert_called_once_with(cafile="/certs/ca.pem")
        mock_ctx.load_cert_chain.assert_called_once_with(
            certfile="/certs/client.pem",
            keyfile="/certs/client.key",
            password="keypass",
        )
        self.assertFalse(mock_ctx.check_hostname)
        self.assertEqual(mock_ctx.verify_mode, ssl.CERT_NONE)
        MockSSLOptions.assert_called_once_with(mock_ctx, server_hostname="rabbit.internal")
        client.disconnect()

    @patch('postoffice.plugins.kafka_client.Consumer')
    @patch('postoffice.plugins.kafka_client.Producer')
    def test_kafka_security_protocols(self, MockProducer, MockConsumer):
        router = MagicMock()

        # 1. Plain SSL with encrypted client key password
        sec_ssl = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            client_cert="/certs/client.pem",
            client_key="/certs/client.key",
            client_key_password="secret_key_passphrase",
        )
        client_ssl = KafkaClient("kafka_ssl", router, bootstrap_servers="kafka:9093", security_config=sec_ssl)
        conf_ssl = client_ssl._build_kafka_config()
        self.assertEqual(conf_ssl["security.protocol"], "SSL")
        self.assertEqual(conf_ssl["ssl.ca.location"], "/certs/ca.pem")
        self.assertEqual(conf_ssl["ssl.certificate.location"], "/certs/client.pem")
        self.assertEqual(conf_ssl["ssl.key.location"], "/certs/client.key")
        self.assertEqual(conf_ssl["ssl.key.password"], "secret_key_passphrase")
        self.assertNotIn("sasl.mechanism", conf_ssl)

        # 2. SASL_PLAINTEXT
        sec_sasl = SecurityConfig(
            username="kafka_user",
            password="kafka_password",
            sasl_mechanism="PLAIN",
        )
        client_sasl = KafkaClient("kafka_sasl", router, bootstrap_servers="kafka:9092", security_config=sec_sasl)
        conf_sasl = client_sasl._build_kafka_config()
        self.assertEqual(conf_sasl["security.protocol"], "SASL_PLAINTEXT")
        self.assertEqual(conf_sasl["sasl.mechanism"], "PLAIN")
        self.assertEqual(conf_sasl["sasl.username"], "kafka_user")
        self.assertEqual(conf_sasl["sasl.password"], "kafka_password")

        # 3. SASL_SSL with SCRAM and insecure_skip_verify
        sec_sasl_ssl = SecurityConfig(
            tls_enabled=True,
            ca_cert="/certs/ca.pem",
            username="scram_user",
            password="scram_password",
            sasl_mechanism="SCRAM-SHA-512",
            insecure_skip_verify=True,
        )
        client_sasl_ssl = KafkaClient("kafka_sasl_ssl", router, bootstrap_servers="kafka:9094", security_config=sec_sasl_ssl)
        conf_sasl_ssl = client_sasl_ssl._build_kafka_config()
        self.assertEqual(conf_sasl_ssl["security.protocol"], "SASL_SSL")
        self.assertEqual(conf_sasl_ssl["sasl.mechanism"], "SCRAM-SHA-512")
        self.assertEqual(conf_sasl_ssl["ssl.ca.location"], "/certs/ca.pem")
        self.assertFalse(conf_sasl_ssl["enable.ssl.certificate.verification"])


if __name__ == '__main__':
    unittest.main()
