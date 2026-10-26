import json
import unittest
from unittest.mock import MagicMock, patch

from postoffice.app import PostOffice
from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane
from postoffice.plugins.amqp_client import AmqpClient
from postoffice.router import Router
from postoffice.security import SecurityConfig

SECRET = "s3cr3t-pw-do-not-log"
PAYLOAD = b"top-secret-payload-body"


class TestResetAndClearState(unittest.TestCase):
    """P0.1: a Data Plane clear_state must not leave the worker permanently stopped."""

    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    def test_reset_keeps_metrics_manager(self, _mock_mqtt):
        po = PostOffice()
        manager = po.metrics_manager
        po.reset()
        self.assertIs(po.metrics_manager, manager)
        self.assertIs(po.router.metrics, manager)

    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    @patch("postoffice.data_plane.redis.Redis")
    def test_clear_state_restarts_metrics_server(self, _mock_redis, _mock_mqtt):
        dp = DataPlane(redis_host="mock")
        dp.po.set_metrics_port(0)
        manager = dp.po.metrics_manager
        with patch.object(manager, "start_server") as start_server, \
                patch.object(manager, "stop_server"):
            dp.po.start()
            start_server.reset_mock()
            dp._process_update_message(
                {"type": "message", "data": json.dumps({"action": "clear_state", "data": {}})}
            )
            start_server.assert_called_once_with(0, addr="127.0.0.1")

    @patch("postoffice.data_plane.redis.Redis")
    def test_clear_state_on_stopped_worker_stays_stopped(self, _mock_redis):
        dp = DataPlane(redis_host="mock")
        dp._process_update_message(
            {"type": "message", "data": json.dumps({"action": "clear_state", "data": {}})}
        )
        self.assertFalse(dp.po.is_running)

    @patch("postoffice.plugins.mqtt_client.MqttClient.connect")
    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    @patch("postoffice.data_plane.redis.Redis")
    def test_data_plane_clear_state_then_add_broker_connects(
        self, _mock_redis, _mock_mqtt, mock_connect
    ):
        dp = DataPlane(redis_host="mock")
        dp.po.start()
        mock_connect.reset_mock()

        dp._process_update_message(
            {"type": "message", "data": json.dumps({"action": "clear_state", "data": {}})}
        )
        dp._process_update_message({
            "type": "message",
            "data": json.dumps({
                "action": "add_broker",
                "data": {"name": "m", "protocol": "mqtt", "kwargs": {}},
            }),
        })

        mock_connect.assert_called_once()


class TestFanoutPerRouteDlq(unittest.TestCase):
    """P0.2: a failure on route N must use route N's DLQ target."""

    def _build(self):
        router = Router()
        clients = {}
        for name in ("src", "tgt_a", "tgt_b", "dlq_a", "dlq_b"):
            c = MagicMock()
            c.name = name
            clients[name] = c
            router.register_client(c)
        router.add_route("src", "in", "tgt_a", "out/a", dlq_broker="dlq_a", dlq_topic="dlq/a")
        router.add_route("src", "in", "tgt_b", "out/b", dlq_broker="dlq_b", dlq_topic="dlq/b")
        return router, clients

    def test_second_route_failure_goes_to_second_routes_dlq(self):
        router, c = self._build()
        c["tgt_b"].publish.side_effect = (
            lambda topic, msg, on_confirm=None, on_error=None, **kw: on_error(RuntimeError("boom"))
        )
        ack, nack = MagicMock(), MagicMock()

        router.route("src", "in", b"x", ack_fn=ack, nack_fn=nack)

        c["dlq_b"].publish.assert_called_once()
        self.assertEqual(c["dlq_b"].publish.call_args[0][0], "dlq/b")
        c["dlq_a"].publish.assert_not_called()
        ack.assert_called_once()

    def test_first_route_failure_still_uses_first_routes_dlq(self):
        router, c = self._build()
        c["tgt_a"].publish.side_effect = (
            lambda topic, msg, on_confirm=None, on_error=None, **kw: on_error(RuntimeError("boom"))
        )

        router.route("src", "in", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())

        c["dlq_a"].publish.assert_called_once()
        c["dlq_b"].publish.assert_not_called()

    def test_async_error_callback_binds_to_its_route(self):
        router, c = self._build()
        saved = {}
        c["tgt_a"].publish.side_effect = (
            lambda topic, msg, on_confirm=None, on_error=None, **kw: saved.update(a=(on_confirm, on_error))
        )
        c["tgt_b"].publish.side_effect = (
            lambda topic, msg, on_confirm=None, on_error=None, **kw: saved.update(b=(on_confirm, on_error))
        )

        router.route("src", "in", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        saved["b"][1](RuntimeError("late failure"))

        c["dlq_b"].publish.assert_called_once()
        c["dlq_a"].publish.assert_not_called()


class TestSecretsNotLogged(unittest.TestCase):
    """P0.6: credentials and payload bodies must not reach logs or reprs."""

    def test_security_config_repr_hides_secrets(self):
        cfg = SecurityConfig(username="u", password=SECRET, client_key_password=SECRET)
        self.assertNotIn(SECRET, repr(cfg))
        self.assertNotIn(SECRET, str(cfg))
        # Serialization for Redis sync must still carry the values.
        self.assertEqual(cfg.to_dict()["password"], SECRET)

    @patch("postoffice.control_plane.redis.Redis")
    def test_control_plane_does_not_log_credentials(self, _mock_redis):
        cp = ControlPlane(redis_host="mock")
        cfg = SecurityConfig(username="u", password=SECRET, client_key_password=SECRET)
        cp.r.hget.return_value = None
        with self.assertLogs("postoffice.control_plane", level="DEBUG") as logs:
            cp.register_broker("amqp_1", "amqp", security_config=cfg)
            cp.update_broker_security("amqp_1", cfg)
        joined = "\n".join(logs.output)
        self.assertNotIn(SECRET, joined)
        self.assertIn("add_broker", joined)
        cp.r.publish.assert_called()
        # The broadcast itself still carries the config workers need.
        self.assertIn(SECRET, cp.r.publish.call_args_list[0][0][1])

    def test_router_does_not_log_payload_per_message(self):
        router = Router()
        src, tgt = MagicMock(), MagicMock()
        src.name, tgt.name = "src", "tgt"
        router.register_client(src)
        router.register_client(tgt)
        router.add_route("src", "in", "tgt", "out")

        with self.assertLogs("postoffice.router", level="DEBUG") as logs:
            router.route("src", "in", PAYLOAD)
        self.assertNotIn(PAYLOAD.decode(), "\n".join(logs.output))

        with self.assertNoLogs("postoffice.router", level="INFO"):
            router.route("src", "in", PAYLOAD)


class TestAmqpAutoAckDefault(unittest.TestCase):
    """P0.6: AMQP consumers must not auto-ack before the router has delivered."""

    def _client(self):
        client = AmqpClient("amqp_1", MagicMock())
        client.channel = MagicMock()
        return client

    def test_default_subscription_uses_manual_ack(self):
        client = self._client()
        client._apply_subscription("rk", {})
        self.assertIs(client.channel.basic_consume.call_args.kwargs["auto_ack"], False)

    def test_explicit_auto_ack_true_is_honoured(self):
        client = self._client()
        client._apply_subscription("rk", {"auto_ack": True})
        self.assertIs(client.channel.basic_consume.call_args.kwargs["auto_ack"], True)

    def test_default_message_gets_ack_callbacks(self):
        router = MagicMock()
        client = AmqpClient("amqp_1", router)
        client.channel = MagicMock()
        client._apply_subscription("rk", {})
        on_message = client.channel.basic_consume.call_args.kwargs["on_message_callback"]
        method = MagicMock(delivery_tag=7, routing_key="rk", exchange="")
        on_message(client.channel, method, None, b"body")
        kwargs = router.route.call_args.kwargs
        self.assertIsNotNone(kwargs["ack_fn"])
        self.assertIsNotNone(kwargs["nack_fn"])


if __name__ == "__main__":
    unittest.main()
