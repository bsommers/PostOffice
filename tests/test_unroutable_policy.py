import json
import unittest
from unittest.mock import MagicMock, patch

from postoffice.app import PostOffice
from postoffice.router import Router


def _router(with_dlq=False):
    router = Router()
    src, dlq = MagicMock(), MagicMock()
    src.name, dlq.name = "src", "dlq"
    router.register_client(src)
    router.register_client(dlq)
    if with_dlq:
        router.set_dlq("dlq", "system/dlq")
    return router, dlq


class TestUnroutablePolicy(unittest.TestCase):
    def test_default_policy_is_nack_without_requeue(self):
        router, dlq = _router()
        ack, nack = MagicMock(), MagicMock()
        router.route("src", "nowhere", b"x", ack_fn=ack, nack_fn=nack)
        nack.assert_called_once_with(requeue=False)
        ack.assert_not_called()
        dlq.publish.assert_not_called()

    def test_default_nack_does_not_use_a_configured_dlq(self):
        router, dlq = _router(with_dlq=True)
        nack = MagicMock()
        router.route("src", "nowhere", b"x", ack_fn=MagicMock(), nack_fn=nack)
        dlq.publish.assert_not_called()
        nack.assert_called_once_with(requeue=False)

    def test_policy_dlq_publishes_envelope_and_acks(self):
        router, dlq = _router(with_dlq=True)
        router.set_unroutable_policy("dlq")
        ack, nack = MagicMock(), MagicMock()
        router.route("src", "nowhere", b"x", ack_fn=ack, nack_fn=nack)
        envelope = json.loads(dlq.publish.call_args[0][1])
        self.assertIn("Unroutable", envelope["error"])
        ack.assert_called_once()
        nack.assert_not_called()

    def test_policy_dlq_without_dlq_target_falls_back_to_nack(self):
        router, _ = _router()
        router.set_unroutable_policy("dlq")
        nack = MagicMock()
        router.route("src", "nowhere", b"x", ack_fn=MagicMock(), nack_fn=nack)
        nack.assert_called_once_with(requeue=False)

    def test_policy_dlq_publish_failure_falls_back_to_nack_not_requeue(self):
        router, dlq = _router(with_dlq=True)
        router.set_unroutable_policy("dlq")
        dlq.publish.side_effect = RuntimeError("dlq down")
        nack = MagicMock()
        router.route("src", "nowhere", b"x", ack_fn=MagicMock(), nack_fn=nack)
        nack.assert_called_once_with(requeue=False)

    def test_policy_drop_acks(self):
        router, dlq = _router(with_dlq=True)
        router.set_unroutable_policy("drop")
        ack, nack = MagicMock(), MagicMock()
        router.route("src", "nowhere", b"x", ack_fn=ack, nack_fn=nack)
        ack.assert_called_once()
        nack.assert_not_called()
        dlq.publish.assert_not_called()

    def test_per_broker_override(self):
        router, dlq = _router(with_dlq=True)
        router.set_unroutable_policy("nack")
        router.set_unroutable_policy("drop", broker="src")
        ack = MagicMock()
        router.route("src", "nowhere", b"x", ack_fn=ack, nack_fn=MagicMock())
        ack.assert_called_once()

    def test_invalid_policy_raises_value_error(self):
        router, _ = _router()
        with self.assertRaises(ValueError):
            router.set_unroutable_policy("explode")

    def test_source_without_nack_is_counted_as_dropped(self):
        router, _ = _router()
        router.route("src", "nowhere", b"x")
        text = router.metrics.get_metrics_text()
        self.assertIn('error_type="unroutable_dropped",source_broker="src"} 1.0', text)

    def test_nack_outcome_is_counted(self):
        router, _ = _router()
        router.route("src", "nowhere", b"x", nack_fn=MagicMock())
        text = router.metrics.get_metrics_text()
        self.assertIn('error_type="unroutable_nacked",source_broker="src"} 1.0', text)

    def test_nack_fn_without_requeue_keyword_still_called(self):
        router, _ = _router()
        calls = []

        def legacy_nack():
            calls.append(1)

        router.route("src", "nowhere", b"x", nack_fn=legacy_nack)
        self.assertEqual(calls, [1])


class TestFacadeUnroutablePolicy(unittest.TestCase):
    def test_constructor_param_and_default(self):
        self.assertEqual(PostOffice().router.unroutable_policy, "nack")
        self.assertEqual(PostOffice(on_unroutable="dlq").router.unroutable_policy, "dlq")

    def test_invalid_constructor_policy_raises(self):
        with self.assertRaises(ValueError):
            PostOffice(on_unroutable="nope")

    def test_policy_survives_reset(self):
        po = PostOffice(on_unroutable="drop")
        po.reset()
        self.assertEqual(po.router.unroutable_policy, "drop")


if __name__ == "__main__":
    unittest.main()
