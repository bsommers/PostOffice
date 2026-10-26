import unittest
from unittest.mock import MagicMock, patch

from postoffice.app import PostOffice
from postoffice.base_client import BaseClient
from postoffice.ingress.concurrency import InflightLimiter
from postoffice.plugins.amqp_client import AmqpClient
from postoffice.plugins.kafka_client import KafkaClient
from postoffice.router import Router


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class TestInflightLimiter(unittest.TestCase):
    def test_admits_up_to_cap_then_refuses(self):
        lim = InflightLimiter(2)
        a, b = lim.acquire(), lim.acquire()
        self.assertIsNotNone(a)
        self.assertIsNotNone(b)
        self.assertIsNone(lim.acquire())
        self.assertEqual(lim.inflight, 2)

    def test_release_frees_a_slot_and_is_idempotent(self):
        lim = InflightLimiter(1)
        lease = lim.acquire()
        lease.release()
        lease.release()
        self.assertEqual(lim.inflight, 0)
        self.assertIsNotNone(lim.acquire())
        self.assertIsNone(lim.acquire())

    def test_saturate_once_and_drain_only_below_low_water(self):
        saturate, drain = MagicMock(), MagicMock()
        lim = InflightLimiter(4, low_water=1, on_saturate=saturate, on_drain=drain)
        leases = [lim.acquire() for _ in range(4)]
        saturate.assert_called_once()
        self.assertTrue(lim.paused)
        leases[0].release()
        leases[1].release()
        drain.assert_not_called()
        leases[2].release()
        self.assertEqual(lim.inflight, 1)
        drain.assert_called_once()
        self.assertFalse(lim.paused)

    def test_hysteresis_no_drain_between_low_water_and_cap(self):
        drain = MagicMock()
        lim = InflightLimiter(4, low_water=1, on_drain=drain)
        leases = [lim.acquire() for _ in range(4)]
        leases[0].release()
        leases[1].release()
        drain.assert_not_called()
        self.assertTrue(lim.paused)

    def test_default_low_water_is_half_the_cap(self):
        self.assertEqual(InflightLimiter(10).low_water, 5)

    def test_leaked_leases_are_reclaimed_after_timeout(self):
        clock = FakeClock()
        drain = MagicMock()
        lim = InflightLimiter(1, lease_timeout=30, clock=clock, on_drain=drain)
        lim.acquire()  # never released: lost confirmation
        self.assertIsNone(lim.acquire())
        clock.now = 31.0
        self.assertIsNotNone(lim.acquire())
        self.assertEqual(lim.reclaimed, 1)
        drain.assert_called_once()

    def test_late_release_of_reclaimed_lease_does_not_double_free(self):
        clock = FakeClock()
        lim = InflightLimiter(1, lease_timeout=30, clock=clock)
        old = lim.acquire()
        clock.now = 31.0
        lim.acquire()
        old.release()
        self.assertEqual(lim.inflight, 1)

    def test_invalid_config(self):
        for kw in ({"max_inflight": 0}, {"max_inflight": 2, "low_water": 2},
                   {"max_inflight": 2, "low_water": -1}, {"max_inflight": 2, "lease_timeout": 0}):
            with self.assertRaises(ValueError, msg=str(kw)):
                InflightLimiter(**kw)


def make_router(limit=2, low_water=None, **kw):
    router = Router()
    router.set_retry_policy(base_delay=0)
    src, dst = MagicMock(), MagicMock()
    src.name, dst.name = "src", "dst"
    router.register_client(src)
    router.register_client(dst)
    router.add_route("src", "in/#", "dst", "out")
    confirms = []
    dst.publish.side_effect = lambda topic, msg, on_confirm=None, on_error=None, **k: confirms.append(
        (on_confirm, on_error))
    router.set_inflight_limit("src", limit, low_water=low_water, **kw)
    return router, src, dst, confirms


class TestRouterInflight(unittest.TestCase):
    def test_inflight_held_until_delivery_confirmed_then_acked(self):
        router, src, dst, confirms = make_router(limit=2)
        ack = MagicMock()
        router.route("src", "in/a", b"x", ack_fn=ack, nack_fn=MagicMock())
        self.assertEqual(router.inflight("src"), 1)
        ack.assert_not_called()
        confirms[0][0]()
        ack.assert_called_once()
        self.assertEqual(router.inflight("src"), 0)

    def test_delivery_error_releases_slot(self):
        router, src, dst, confirms = make_router(limit=1)
        router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        confirms[0][1](RuntimeError("down"))
        self.assertEqual(router.inflight("src"), 0)

    def test_at_cap_pauses_client_and_requeues_extra_message_unpublished(self):
        router, src, dst, confirms = make_router(limit=2)
        for _ in range(2):
            router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        src.pause_consumption.assert_called_once()
        nack = MagicMock()
        router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=nack)
        nack.assert_called_once_with(requeue=True)
        self.assertEqual(dst.publish.call_count, 2)
        self.assertEqual(router.inflight("src"), 2)

    def test_resume_when_drained_below_low_water(self):
        router, src, dst, confirms = make_router(limit=4, low_water=1)
        for _ in range(4):
            router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        for on_confirm, _ in confirms[:2]:
            on_confirm()
        src.resume_consumption.assert_not_called()
        confirms[2][0]()
        src.resume_consumption.assert_called_once()

    def test_client_told_the_cap_for_native_prefetch(self):
        router, src, dst, confirms = make_router(limit=7)
        src.set_inflight_limit.assert_called_once_with(7)

    def test_source_without_callbacks_is_shed_at_cap_and_never_leaks(self):
        router, src, dst, confirms = make_router(limit=1)
        # A concurrent in-flight route() (the only meaningful slot for MQTT-style sources).
        lease = router._inflight["src"].acquire()
        router.route("src", "in/a", b"x")
        self.assertEqual(dst.publish.call_count, 0)
        lease.release()
        router.route("src", "in/a", b"x")
        self.assertEqual(dst.publish.call_count, 1)
        self.assertEqual(router.inflight("src"), 0)

    def test_exception_in_routing_releases_slot(self):
        router, src, dst, confirms = make_router(limit=1)
        with patch.object(router, "_route", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        self.assertEqual(router.inflight("src"), 0)

    def test_firewall_reject_and_unroutable_release_slot(self):
        router, src, dst, confirms = make_router(limit=1)
        router.route("src", "nowhere", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        self.assertEqual(router.inflight("src"), 0)
        router.set_firewall(MagicMock(evaluate=MagicMock(return_value=MagicMock(
            action=MagicMock(value="reject"), rule="r", category="oversize"))))
        from postoffice.ingress import Action
        router.firewall.evaluate.return_value.action = Action.REJECT
        router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        self.assertEqual(router.inflight("src"), 0)

    def test_other_sources_are_not_limited(self):
        router, src, dst, confirms = make_router(limit=1)
        other = MagicMock()
        other.name = "other"
        router.register_client(other)
        router.add_route("other", "in/#", "dst", "out")
        for _ in range(3):
            router.route("other", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        self.assertEqual(dst.publish.call_count, 3)

    def test_remove_limit(self):
        router, src, dst, confirms = make_router(limit=1)
        router.set_inflight_limit("src", None)
        for _ in range(3):
            router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        self.assertEqual(dst.publish.call_count, 3)

    def test_shed_is_counted(self):
        router, src, dst, confirms = make_router(limit=1)
        router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        value = router.metrics.registry.get_sample_value(
            "postoffice_routing_errors_total", {"source_broker": "src", "error_type": "inflight_limit"})
        self.assertEqual(value, 1.0)


class TestPostOfficeConfig(unittest.TestCase):
    def test_configure_ingress_dict_sets_limits_and_survives_reset(self):
        po = PostOffice()
        po.configure_ingress({"rules": [], "inflight": {"src": {"max_inflight": 3}}})
        self.assertEqual(po.router._inflight["src"].max_inflight, 3)
        po.reset()
        self.assertEqual(po.router._inflight["src"].max_inflight, 3)

    def test_set_inflight_limit_api(self):
        po = PostOffice()
        po.set_inflight_limit("src", 5, low_water=2)
        self.assertEqual(po.router._inflight["src"].low_water, 2)
        po.set_inflight_limit("src", None)
        self.assertNotIn("src", po.router._inflight)


class TestClientHooks(unittest.TestCase):
    def test_base_hooks_default_to_noop(self):
        class C(BaseClient):
            connect = disconnect = subscribe = publish = lambda *a, **k: None
        c = C("c", MagicMock())
        c.pause_consumption()
        c.resume_consumption()
        c.set_inflight_limit(5)

    def test_amqp_sets_prefetch_on_open_channel(self):
        client = AmqpClient("amqp", MagicMock())
        client.connection = MagicMock(is_open=True)
        client.connection.add_callback_threadsafe.side_effect = lambda cb: cb()
        client.channel = MagicMock()
        client.set_inflight_limit(9)
        client.channel.basic_qos.assert_called_once_with(prefetch_count=9)

    @patch("postoffice.plugins.amqp_client.pika.BlockingConnection")
    def test_amqp_applies_stored_prefetch_on_connect(self, MockConn):
        client = AmqpClient("amqp", MagicMock())
        client.set_inflight_limit(4)  # before connecting
        client._stop_event.set()
        client._consume_loop()
        MockConn.return_value.channel.return_value.basic_qos.assert_called_once_with(prefetch_count=4)

    @patch("postoffice.plugins.kafka_client.Consumer")
    @patch("postoffice.plugins.kafka_client.Producer")
    def test_kafka_pauses_and_resumes_assignment_from_consume_loop(self, MockProducer, MockConsumer):
        cons = MockConsumer.return_value
        cons.assignment.return_value = ["tp0", "tp1"]
        client = KafkaClient("kafka", MagicMock())
        client.consumer = cons
        step = [0]

        def poll(timeout):
            step[0] += 1
            if step[0] == 1:
                client.pause_consumption()
            elif step[0] == 3:
                client.resume_consumption()
            elif step[0] == 4:
                client._stop_event.set()
            return None

        cons.poll.side_effect = poll
        client._consume_loop()
        cons.pause.assert_called_with(["tp0", "tp1"])
        cons.resume.assert_called_once_with(["tp0", "tp1"])
        self.assertEqual(cons.poll.call_count, 4)  # kept polling while paused


if __name__ == "__main__":
    unittest.main()
