import unittest
from unittest.mock import MagicMock, patch

import pika.exceptions

from postoffice.native import NativeKafkaTracker
from postoffice.plugins.amqp_client import AmqpClient
from postoffice.plugins.kafka_client import KafkaClient
from postoffice.plugins.mqtt_client import MqttClient
from postoffice.router import Router


class TestPhase3AdaptersAndDrills(unittest.TestCase):
    def setUp(self):
        self.router = Router()

    def test_native_kafka_contiguous_offset_tracker_and_crash_drill(self):
        tracker = NativeKafkaTracker()
        # Poll offsets 5, 6, 7, 8 on ("orders", 0)
        for off in (5, 6, 7, 8):
            tracker.poll_offset("orders", 0, off)

        # Offset 5 completes -> contiguous watermark advances to 5 (next offset 6)
        self.assertEqual(tracker.ack_offset("orders", 0, 5), 5)
        self.assertEqual(tracker.committed_watermark("orders", 0), 6)

        # Offset 7 completes out of order while 6 is still in-flight -> NO commit!
        self.assertIsNone(tracker.ack_offset("orders", 0, 7))
        self.assertEqual(tracker.committed_watermark("orders", 0), 6)

        # Simulate consumer crash & restart at committed_watermark (6):
        # Uncommitted messages 6, 7, 8 are re-polled from offset 6; offset 5 is never re-processed
        tracker.reset_partition("orders", 0)
        for off in (6, 7, 8):
            tracker.poll_offset("orders", 0, off)

        # Now 7 completes first, then 6 completes -> advances contiguous watermark through 7!
        self.assertIsNone(tracker.ack_offset("orders", 0, 7))
        self.assertEqual(tracker.ack_offset("orders", 0, 6), 7)
        self.assertEqual(tracker.committed_watermark("orders", 0), 8)

        # Finally 8 completes -> advances to 8 (watermark 9)
        self.assertEqual(tracker.ack_offset("orders", 0, 8), 8)
        self.assertEqual(tracker.committed_watermark("orders", 0), 9)

    @patch("postoffice.plugins.kafka_client.Consumer")
    @patch("postoffice.plugins.kafka_client.Producer")
    def test_kafka_client_contiguous_commit_and_idle_producer_poll(
        self, MockProducer, MockConsumer
    ):
        mock_cons = MockConsumer.return_value
        mock_prod = MockProducer.return_value
        client = KafkaClient("kafka_k1", self.router)
        client.connect()
        client._stop_event.set()
        if client.thread:
            client.thread.join(timeout=1.0)

        # Verify consumer config disables auto-commit (K1/K2)
        cons_conf = MockConsumer.call_args[0][0]
        self.assertIs(cons_conf.get("enable.auto.commit"), False)
        self.assertIs(cons_conf.get("enable.auto.offset.store"), False)

        # Simulate polling 3 messages (offsets 10, 11, 12) and completing 10, then 12, then 11
        msgs = []
        for off in (10, 11, 12):
            m = MagicMock()
            m.error.return_value = None
            m.topic.return_value = "events"
            m.value.return_value = f"v{off}".encode()
            m.key.return_value = None
            m.partition.return_value = 0
            m.offset.return_value = off
            msgs.append(m)

        acks = []
        self.router.route = MagicMock(
            side_effect=lambda brk, top, pay, **kw: acks.append(kw["ack_fn"])
        )

        idx = [0]

        def poll_seq(_timeout):
            if idx[0] < len(msgs):
                m = msgs[idx[0]]
                idx[0] += 1
                return m
            client._stop_event.set()
            return None

        mock_cons.poll.side_effect = poll_seq
        mock_cons.commit.reset_mock()
        mock_prod.poll.reset_mock()
        client._stop_event.clear()
        client._consume_loop()

        # Producer must be polled in consume loop even without new publish() calls
        self.assertGreaterEqual(mock_prod.poll.call_count, 1)
        self.assertEqual(len(acks), 3)

        # Ack offset 10 -> commits msg[0] (offset 10)
        acks[0]()
        mock_cons.commit.assert_called_once_with(message=msgs[0], asynchronous=True)
        mock_cons.commit.reset_mock()

        # Ack offset 12 while 11 is still pending -> must NOT commit yet!
        acks[2]()
        mock_cons.commit.assert_not_called()

        # Ack offset 11 -> now both 11 and 12 are contiguous -> commits msg[2] (offset 12)!
        acks[1]()
        mock_cons.commit.assert_called_once_with(message=msgs[2], asynchronous=True)

    @patch("postoffice.plugins.amqp_client.pika.BlockingConnection")
    def test_amqp_publisher_confirms_unroutable_and_queue_cache(self, MockConn):
        mock_conn = MockConn.return_value
        mock_conn.is_open = True
        mock_conn.add_callback_threadsafe.side_effect = lambda fn: fn()
        mock_ch = MagicMock()
        mock_conn.channel.return_value = mock_ch

        client = AmqpClient("amqp_a1", self.router)
        client._stop_event.set()
        client._consume_loop()

        # confirm_delivery() must be enabled when channel opens
        mock_ch.confirm_delivery.assert_called_once()

        # Queue is declared only once per queue name across multiple publishes
        c1, c2 = MagicMock(), MagicMock()
        client.publish("orders_q", b"m1", on_confirm=c1)
        client.publish("orders_q", b"m2", on_confirm=c2)
        self.assertEqual(mock_ch.queue_declare.call_count, 1)
        c1.assert_called_once()
        c2.assert_called_once()

        # UnroutableError invokes on_error and never on_confirm
        mock_ch.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        on_confirm = MagicMock()
        on_error = MagicMock()
        client.publish("orders_q", b"m3", on_confirm=on_confirm, on_error=on_error)
        on_confirm.assert_not_called()
        on_error.assert_called_once()

    @patch("postoffice.plugins.mqtt_client.mqtt.Client")
    def test_mqtt_qos1_puback_confirm_and_connect_failure_guard(self, MockMqtt):
        mock_paho = MockMqtt.return_value
        mock_paho.is_connected.return_value = True
        client = MqttClient("mqtt_m1", self.router)

        # QoS 1 publish waits for _on_publish(mid) PUBACK before calling on_confirm
        pub_info = MagicMock()
        pub_info.rc = 0
        pub_info.mid = 77
        mock_paho.publish.return_value = pub_info

        on_confirm = MagicMock()
        on_error = MagicMock()
        client.publish("telemetry/1", b"data", qos=1, on_confirm=on_confirm, on_error=on_error)
        on_confirm.assert_not_called()
        on_error.assert_not_called()

        # Trigger PUBACK for mid=77
        client._on_publish(mock_paho, None, 77, None, None)
        on_confirm.assert_called_once()

        # Publish with non-zero rc invokes on_error immediately
        bad_info = MagicMock()
        bad_info.rc = 4
        bad_info.mid = 78
        mock_paho.publish.return_value = bad_info
        on_confirm2 = MagicMock()
        on_error2 = MagicMock()
        client.publish("telemetry/1", b"data", qos=1, on_confirm=on_confirm2, on_error=on_error2)
        on_confirm2.assert_not_called()
        on_error2.assert_called_once()

        # _on_connect with failure reason_code skips subscribing
        mock_paho.subscribe.reset_mock()
        client.subscribed_topics = ["telemetry/#"]
        fail_rc = MagicMock()
        fail_rc.is_failure = True
        client._on_connect(mock_paho, None, None, fail_rc, None)
        mock_paho.subscribe.assert_not_called()


if __name__ == "__main__":
    unittest.main()
