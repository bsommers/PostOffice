import unittest
import json
import base64
from unittest.mock import MagicMock
from postoffice.dlq import format_dlq_payload
from postoffice.router import Router
from postoffice.app import PostOffice

class TestDeadLetterQueue(unittest.TestCase):
    def setUp(self):
        self.router = Router()

    def test_format_dlq_payload_text(self):
        msg = b"temperature sensor: 21.5C"
        formatted = format_dlq_payload(msg, "mqtt_edge", "sensors/temp", "Route not found")
        envelope = json.loads(formatted.decode("utf-8"))

        self.assertEqual(envelope["source_broker"], "mqtt_edge")
        self.assertEqual(envelope["source_topic"], "sensors/temp")
        self.assertEqual(envelope["error"], "Route not found")
        self.assertEqual(envelope["payload"], "temperature sensor: 21.5C")
        self.assertFalse(envelope["is_base64"])
        self.assertIn("timestamp", envelope)

    def test_format_dlq_payload_binary(self):
        binary_data = b"\x80\xff\xfe\x00\x01\xaa"
        formatted = format_dlq_payload(binary_data, "kafka_raw", "raw/binary", "Corrupted payload")
        envelope = json.loads(formatted.decode("utf-8"))

        self.assertEqual(envelope["source_broker"], "kafka_raw")
        self.assertEqual(envelope["source_topic"], "raw/binary")
        self.assertEqual(envelope["error"], "Corrupted payload")
        self.assertTrue(envelope["is_base64"])
        decoded_payload = base64.b64decode(envelope["payload"].encode("ascii"))
        self.assertEqual(decoded_payload, binary_data)

    def test_unroutable_message_to_default_dlq(self):
        src_client = MagicMock()
        src_client.name = "src_broker"
        dlq_client = MagicMock()
        dlq_client.name = "dlq_broker"

        self.router.register_client(src_client)
        self.router.register_client(dlq_client)
        self.router.set_dlq("dlq_broker", "system/dlq")

        ack_fn = MagicMock()
        nack_fn = MagicMock()

        self.router.route(
            "src_broker",
            "unmatched/topic",
            b"unroutable message",
            ack_fn=ack_fn,
            nack_fn=nack_fn
        )

        dlq_client.publish.assert_called_once()
        call_args = dlq_client.publish.call_args
        self.assertEqual(call_args[0][0], "system/dlq")
        envelope = json.loads(call_args[0][1].decode("utf-8"))
        self.assertEqual(envelope["source_broker"], "src_broker")
        self.assertEqual(envelope["source_topic"], "unmatched/topic")
        self.assertIn("Unroutable", envelope["error"])

        ack_fn.assert_called_once()
        nack_fn.assert_not_called()

    def test_target_publish_failure_to_default_dlq(self):
        src_client = MagicMock()
        src_client.name = "src_broker"
        tgt_client = MagicMock()
        tgt_client.name = "tgt_broker"
        dlq_client = MagicMock()
        dlq_client.name = "dlq_broker"

        def fail_publish(topic, msg, on_confirm=None, on_error=None, **kwargs):
            if on_error:
                on_error(RuntimeError("Broker unavailable"))

        tgt_client.publish.side_effect = fail_publish

        self.router.register_client(src_client)
        self.router.register_client(tgt_client)
        self.router.register_client(dlq_client)

        self.router.set_dlq("dlq_broker", "system/dlq")
        self.router.add_route("src_broker", "in/topic", "tgt_broker", "out/topic")

        ack_fn = MagicMock()
        nack_fn = MagicMock()

        self.router.route(
            "src_broker",
            "in/topic",
            b"test data",
            ack_fn=ack_fn,
            nack_fn=nack_fn
        )

        dlq_client.publish.assert_called_once()
        call_args = dlq_client.publish.call_args
        self.assertEqual(call_args[0][0], "system/dlq")
        envelope = json.loads(call_args[0][1].decode("utf-8"))
        self.assertEqual(envelope["error"], "Broker unavailable")

        # Upstream message should be acknowledged once stored in DLQ
        ack_fn.assert_called_once()
        nack_fn.assert_not_called()

    def test_per_route_dlq_override(self):
        src_client = MagicMock()
        src_client.name = "src_broker"
        tgt_client = MagicMock()
        tgt_client.name = "tgt_broker"
        default_dlq = MagicMock()
        default_dlq.name = "default_dlq"
        custom_dlq = MagicMock()
        custom_dlq.name = "custom_dlq"

        def fail_publish(topic, msg, on_confirm=None, on_error=None, **kwargs):
            if on_error:
                on_error(RuntimeError("Downstream error"))

        tgt_client.publish.side_effect = fail_publish

        self.router.register_client(src_client)
        self.router.register_client(tgt_client)
        self.router.register_client(default_dlq)
        self.router.register_client(custom_dlq)

        self.router.set_dlq("default_dlq", "default/dlq")
        self.router.add_route(
            "src_broker", "special/data", "tgt_broker", "processed/data",
            dlq_broker="custom_dlq", dlq_topic="custom/special_dlq"
        )

        ack_fn = MagicMock()
        nack_fn = MagicMock()

        self.router.route(
            "src_broker",
            "special/data",
            b"special payload",
            ack_fn=ack_fn,
            nack_fn=nack_fn
        )

        custom_dlq.publish.assert_called_once()
        self.assertEqual(custom_dlq.publish.call_args[0][0], "custom/special_dlq")
        default_dlq.publish.assert_not_called()
        ack_fn.assert_called_once()
        nack_fn.assert_not_called()

    def test_fallback_nack_when_dlq_unconfigured(self):
        src_client = MagicMock()
        src_client.name = "src_broker"
        tgt_client = MagicMock()
        tgt_client.name = "tgt_broker"

        def fail_publish(topic, msg, on_confirm=None, on_error=None, **kwargs):
            if on_error:
                on_error(RuntimeError("Downstream error"))

        tgt_client.publish.side_effect = fail_publish

        self.router.register_client(src_client)
        self.router.register_client(tgt_client)
        self.router.add_route("src_broker", "in/topic", "tgt_broker", "out/topic")

        ack_fn = MagicMock()
        nack_fn = MagicMock()

        self.router.route(
            "src_broker",
            "in/topic",
            b"data",
            ack_fn=ack_fn,
            nack_fn=nack_fn
        )

        ack_fn.assert_not_called()
        nack_fn.assert_called_once_with(requeue=True)

    def test_anti_recursion_guard(self):
        dlq_client = MagicMock()
        dlq_client.name = "dlq_broker"
        self.router.register_client(dlq_client)
        self.router.set_dlq("dlq_broker", "system/dlq")

        ack_fn = MagicMock()
        nack_fn = MagicMock()

        # Originates from the exact same broker and topic as the DLQ target
        self.router.route(
            "dlq_broker",
            "system/dlq",
            b"looping message",
            ack_fn=ack_fn,
            nack_fn=nack_fn
        )

        # DLQ publish should not be invoked to prevent recursion
        dlq_client.publish.assert_not_called()
        ack_fn.assert_not_called()
        nack_fn.assert_called_once_with(requeue=False)

    def test_postoffice_facade_dlq(self):
        po = PostOffice(dlq_broker="dlq_main", dlq_topic="global/dlq")
        self.assertEqual(po.router.default_dlq_broker, "dlq_main")
        self.assertEqual(po.router.default_dlq_topic, "global/dlq")

        po.set_dlq("dlq_secondary", "secondary/dlq")
        self.assertEqual(po.router.default_dlq_broker, "dlq_secondary")
        self.assertEqual(po.router.default_dlq_topic, "secondary/dlq")

        po.reset()
        self.assertEqual(po.router.default_dlq_broker, "dlq_secondary")
        self.assertEqual(po.router.default_dlq_topic, "secondary/dlq")


if __name__ == "__main__":
    unittest.main()
