import unittest
from unittest.mock import MagicMock, patch
from postoffice.metrics import MetricsManager
from postoffice.router import Router
from postoffice.app import PostOffice

class TestMetrics(unittest.TestCase):
    def setUp(self):
        self.metrics = MetricsManager()

    def test_metrics_manager_counters(self):
        self.metrics.record_routed("mqtt_src", "kafka_tgt", status="success")
        self.metrics.record_error("mqtt_src", "publish_failure")
        self.metrics.record_duration("mqtt_src", 0.005)
        self.metrics.record_dlq("mqtt_src", "amqp_dlq", "dlq_topic")

        metrics_text = self.metrics.get_metrics_text()

        self.assertIn("postoffice_messages_routed_total", metrics_text)
        self.assertIn('source_broker="mqtt_src"', metrics_text)
        self.assertIn('target_broker="kafka_tgt"', metrics_text)
        self.assertIn('status="success"', metrics_text)

        self.assertIn("postoffice_routing_errors_total", metrics_text)
        self.assertIn('error_type="publish_failure"', metrics_text)

        self.assertIn("postoffice_routing_duration_seconds", metrics_text)

        self.assertIn("postoffice_dlq_messages_total", metrics_text)
        self.assertIn('dlq_broker="amqp_dlq"', metrics_text)
        self.assertIn('dlq_topic="dlq_topic"', metrics_text)

    def test_router_metrics_integration(self):
        router = Router(metrics_manager=self.metrics)

        src = MagicMock()
        src.name = "src_broker"
        tgt = MagicMock()
        tgt.name = "tgt_broker"

        router.register_client(src)
        router.register_client(tgt)
        router.add_route("src_broker", "in/topic", "tgt_broker", "out/topic")

        # 1. Successful route
        router.route("src_broker", "in/topic", b"payload1")
        text = self.metrics.get_metrics_text()
        self.assertIn('postoffice_messages_routed_total{source_broker="src_broker",status="success",target_broker="tgt_broker"} 1.0', text)

        # 2. Failed publish
        tgt.publish.side_effect = RuntimeError("Connection lost")
        router.route("src_broker", "in/topic", b"payload2")
        text = self.metrics.get_metrics_text()
        self.assertIn('postoffice_messages_routed_total{source_broker="src_broker",status="error",target_broker="tgt_broker"} 1.0', text)
        self.assertIn('postoffice_routing_errors_total{error_type="publish_failure",source_broker="src_broker"} 1.0', text)

        # 3. Unroutable message
        router.route("src_broker", "unmatched/path", b"payload3")
        text = self.metrics.get_metrics_text()
        self.assertIn('postoffice_routing_errors_total{error_type="unroutable",source_broker="src_broker"} 1.0', text)

    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    def test_postoffice_metrics_lifecycle(self, MockMqtt):
        po = PostOffice(metrics_port=19100)
        po.router.metrics.start_server = MagicMock()
        po.router.metrics.stop_server = MagicMock()

        po.start()
        po.router.metrics.start_server.assert_called_once_with(19100)

        po.stop()
        po.router.metrics.stop_server.assert_called_once()

    def test_metrics_http_server_start_stop(self):
        # Test real start and stop on a high port without conflicts
        manager = MetricsManager()
        test_port = 19876
        manager.start_server(test_port)
        self.assertIsNotNone(manager._server_handle)

        manager.stop_server()
        self.assertIsNone(manager._server_handle)

if __name__ == '__main__':
    unittest.main()
