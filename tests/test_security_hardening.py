import os
import unittest
from unittest.mock import MagicMock, patch

from postoffice.app import PostOffice
from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane
from postoffice.metrics import MetricsManager
from postoffice.plugins.amqp_client import AmqpClient
from postoffice.plugins.kafka_client import KafkaClient
from postoffice.plugins.mqtt_client import MqttClient
from postoffice.security import SecurityConfig


class TestMetricsBinding(unittest.TestCase):
    def test_metrics_binds_loopback_by_default(self):
        manager = MetricsManager()
        with patch("postoffice.metrics.start_http_server", return_value=(MagicMock(), MagicMock())) as start:
            manager.start_server(19101)
        self.assertEqual(start.call_args.kwargs["addr"], "127.0.0.1")

    def test_metrics_addr_override(self):
        manager = MetricsManager()
        with patch("postoffice.metrics.start_http_server", return_value=(MagicMock(), MagicMock())) as start:
            manager.start_server(19101, addr="0.0.0.0")
        self.assertEqual(start.call_args.kwargs["addr"], "0.0.0.0")

    def test_postoffice_passes_addr_to_the_server(self):
        po = PostOffice()
        po.router.metrics.start_server = MagicMock()
        po.set_metrics_port(19102, addr="0.0.0.0")
        po.start()
        po.router.metrics.start_server.assert_called_once_with(19102, addr="0.0.0.0")

    def test_postoffice_default_addr_is_loopback(self):
        po = PostOffice(metrics_port=19103)
        po.router.metrics.start_server = MagicMock()
        po.start()
        po.router.metrics.start_server.assert_called_once_with(19103, addr="127.0.0.1")


class TestRedisAuth(unittest.TestCase):
    def test_redis_client_receives_password_and_ssl(self):
        for cls in (ControlPlane, DataPlane):
            with self.subTest(cls=cls.__name__), \
                    patch(f"postoffice.{'control' if cls is ControlPlane else 'data'}_plane.redis.Redis") as redis_cls:
                cls(redis_host="r", redis_port=6380, redis_password="s3cret", redis_tls=True)
            kwargs = redis_cls.call_args.kwargs
            self.assertEqual((kwargs["host"], kwargs["port"]), ("r", 6380))
            self.assertEqual(kwargs["password"], "s3cret")
            self.assertTrue(kwargs["ssl"])

    def test_defaults_stay_unauthenticated_plaintext_for_local_dev(self):
        with patch("postoffice.control_plane.redis.Redis") as redis_cls:
            ControlPlane()
        kwargs = redis_cls.call_args.kwargs
        self.assertIsNone(kwargs["password"])
        self.assertFalse(kwargs["ssl"])

    def test_worker_reads_redis_settings_from_environment(self):
        env = {"REDIS_HOST": "h", "REDIS_PORT": "6390", "REDIS_PASSWORD": "pw", "REDIS_TLS": "1"}
        with patch.dict(os.environ, env), patch("postoffice.worker.DataPlane") as dp:
            from postoffice import worker
            worker.main()
        dp.assert_called_once_with(redis_host="h", redis_port=6390, redis_password="pw", redis_tls=True)

    def test_worker_defaults(self):
        with patch.dict(os.environ, {}, clear=True), patch("postoffice.worker.DataPlane") as dp:
            from postoffice import worker
            worker.main()
        dp.assert_called_once_with(redis_host="localhost", redis_port=6379, redis_password=None, redis_tls=False)


class TestSkipVerifyWarning(unittest.TestCase):
    def security(self):
        return SecurityConfig(tls_enabled=True, insecure_skip_verify=True)

    def test_mqtt(self):
        with patch("postoffice.plugins.mqtt_client.mqtt.Client"), \
                self.assertLogs("postoffice.plugins.mqtt_client", level="WARNING") as cm:
            MqttClient("m", MagicMock(), security_config=self.security())
        self.assertIn("insecure_skip_verify", cm.output[0])

    def test_kafka(self):
        with patch("postoffice.plugins.kafka_client.Producer"), \
                self.assertLogs("postoffice.plugins.kafka_client", level="WARNING") as cm:
            KafkaClient("k", MagicMock(), security_config=self.security())._build_kafka_config()
        self.assertIn("insecure_skip_verify", cm.output[0])

    def test_amqp(self):
        client = AmqpClient("a", MagicMock(), security_config=self.security())
        with patch("postoffice.plugins.amqp_client.pika.BlockingConnection") as conn, \
                self.assertLogs("postoffice.plugins.amqp_client", level="WARNING") as cm:
            client._stop_event.set()
            client._consume_loop()
        self.assertTrue(any("insecure_skip_verify" in line for line in cm.output))

    def test_no_warning_when_verification_is_on(self):
        with patch("postoffice.plugins.mqtt_client.mqtt.Client"):
            with self.assertNoLogs("postoffice.plugins.mqtt_client", level="WARNING"):
                MqttClient("m", MagicMock(), security_config=SecurityConfig(tls_enabled=True))


if __name__ == "__main__":
    unittest.main()
