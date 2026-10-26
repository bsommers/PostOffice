import unittest
from unittest.mock import MagicMock

from postoffice.app import PostOffice
from postoffice.base_client import BaseClient, BufferExpiredError, BufferOverflowError
from postoffice.metrics import MetricsManager
from postoffice.router import Router


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class Client(BaseClient):
    def __init__(self, **kw):
        kw.setdefault("enable_publish_buffer", True)
        super().__init__("c", MagicMock(), **kw)
        self.published = []

    connect = disconnect = subscribe = lambda *a, **k: None

    def publish(self, topic, message, on_confirm=None, on_error=None, **kwargs):
        self.published.append((topic, message))


class TestByteLimit(unittest.TestCase):
    def test_rejects_when_byte_budget_exceeded(self):
        c = Client(max_publish_buffer_bytes=10)
        on_error = MagicMock()
        c.buffer_publish("t", b"123456")
        c.buffer_publish("t", b"123456", on_error=on_error)
        self.assertEqual(c.publish_buffer_size, 1)
        self.assertIsInstance(on_error.call_args.args[0], BufferOverflowError)

    def test_tracks_bytes_and_releases_them_on_flush(self):
        c = Client(max_publish_buffer_bytes=10)
        c.buffer_publish("t", b"123456")
        self.assertEqual(c.publish_buffer_bytes, 6)
        c.flush_publish_buffer()
        self.assertEqual(c.publish_buffer_bytes, 0)
        c.buffer_publish("t", b"123456")  # space is available again
        self.assertEqual(c.publish_buffer_size, 1)

    def test_drop_oldest_evicts_until_the_new_message_fits(self):
        c = Client(max_publish_buffer_bytes=10, buffer_overflow_policy="drop_oldest")
        evicted_a, evicted_b = MagicMock(), MagicMock()
        c.buffer_publish("a", b"1234", on_error=evicted_a)
        c.buffer_publish("b", b"1234", on_error=evicted_b)
        c.buffer_publish("c", b"1234567")  # 8 + 7 > 10: both older messages must go
        self.assertEqual(c.publish_buffer_bytes, 7)
        evicted_a.assert_called_once()
        evicted_b.assert_called_once()
        c.flush_publish_buffer()
        self.assertEqual([t for t, _ in c.published], ["c"])

    def test_single_message_over_budget_is_rejected_without_evicting_others(self):
        c = Client(max_publish_buffer_bytes=10, buffer_overflow_policy="drop_oldest")
        c.buffer_publish("a", b"1234")
        on_error = MagicMock()
        c.buffer_publish("big", b"x" * 11, on_error=on_error)
        self.assertEqual(c.publish_buffer_size, 1)
        self.assertIsInstance(on_error.call_args.args[0], BufferOverflowError)

    def test_invalid_limits_rejected(self):
        for kw in ({"max_publish_buffer_bytes": 0}, {"max_publish_buffer_age": 0}):
            with self.assertRaises(ValueError, msg=str(kw)):
                Client(**kw)


class TestAgeLimit(unittest.TestCase):
    def test_stale_items_discarded_on_flush_with_on_error(self):
        clock = FakeClock()
        c = Client(max_publish_buffer_age=30, buffer_clock=clock)
        stale_err = MagicMock()
        c.buffer_publish("old", b"x", on_error=stale_err)
        clock.now = 20.0
        c.buffer_publish("new", b"y")
        clock.now = 35.0
        flushed = c.flush_publish_buffer()
        self.assertEqual(flushed, 1)
        self.assertEqual([t for t, _ in c.published], ["new"])
        self.assertIsInstance(stale_err.call_args.args[0], BufferExpiredError)

    def test_stale_items_reclaimed_when_new_work_arrives(self):
        clock = FakeClock()
        c = Client(max_publish_buffer_age=30, max_publish_buffer_bytes=10, buffer_clock=clock)
        c.buffer_publish("old", b"123456789")
        clock.now = 31.0
        on_error = MagicMock()
        c.buffer_publish("new", b"123456789", on_error=on_error)  # fits only because old expired
        on_error.assert_not_called()
        self.assertEqual(c.publish_buffer_size, 1)
        self.assertEqual(c.publish_buffer_bytes, 9)

    def test_no_age_limit_by_default(self):
        clock = FakeClock()
        c = Client(buffer_clock=clock)
        c.buffer_publish("t", b"x")
        clock.now = 10 ** 9
        self.assertEqual(c.flush_publish_buffer(), 1)


class TestLabelGuard(unittest.TestCase):
    def sample(self, m, **labels):
        return m.registry.get_sample_value("postoffice_routing_errors_total", labels)

    def test_distinct_values_beyond_cap_collapse_to_other(self):
        m = MetricsManager(max_label_values=3)
        for i in range(5):
            m.record_error(f"src{i}", "boom")
        self.assertEqual(self.sample(m, source_broker="src2", error_type="boom"), 1.0)
        self.assertIsNone(self.sample(m, source_broker="src3", error_type="boom"))
        self.assertEqual(self.sample(m, source_broker="other", error_type="boom"), 2.0)

    def test_values_seen_before_the_cap_keep_their_own_series(self):
        m = MetricsManager(max_label_values=2)
        m.record_error("a", "boom")
        m.record_error("b", "boom")
        m.record_error("c", "boom")
        m.record_error("a", "boom")
        self.assertEqual(self.sample(m, source_broker="a", error_type="boom"), 2.0)

    def test_dlq_topic_and_target_broker_are_guarded(self):
        m = MetricsManager(max_label_values=1)
        m.record_dlq("src", "dlq", "t1")
        m.record_dlq("src", "dlq", "t2")
        m.record_routed("src", "t1", "ok")
        m.record_routed("src", "t2", "ok")
        reg = m.registry
        self.assertEqual(reg.get_sample_value("postoffice_dlq_messages_total",
                         {"source_broker": "src", "dlq_broker": "dlq", "dlq_topic": "other"}), 1.0)
        self.assertEqual(reg.get_sample_value("postoffice_messages_routed_total",
                         {"source_broker": "src", "target_broker": "other", "status": "ok"}), 1.0)

    def test_guard_caps_are_per_label_name(self):
        m = MetricsManager(max_label_values=1)
        m.record_error("only", "boom")
        m.record_routed("only", "target", "ok")  # 'only' also fits under target_broker's own cap
        self.assertEqual(m.registry.get_sample_value(
            "postoffice_messages_routed_total",
            {"source_broker": "only", "target_broker": "target", "status": "ok"}), 1.0)

    def test_invalid_cap(self):
        with self.assertRaises(ValueError):
            MetricsManager(max_label_values=0)


class TestRouteLimit(unittest.TestCase):
    def test_add_route_enforces_per_topic_cap(self):
        router = Router(max_routes_per_topic=2)
        router.add_route("s", "t", "a", "x")
        router.add_route("s", "t", "b", "x")
        with self.assertRaises(ValueError):
            router.add_route("s", "t", "c", "x")
        self.assertEqual(len(router.routes[("s", "t")]), 2)

    def test_cap_is_per_source_and_topic(self):
        router = Router(max_routes_per_topic=1)
        router.add_route("s", "t1", "a", "x")
        router.add_route("s", "t2", "a", "x")
        router.add_route("s2", "t1", "a", "x")

    def test_default_cap_is_generous_but_finite(self):
        self.assertGreaterEqual(Router().max_routes_per_topic, 50)

    def test_none_disables_cap(self):
        router = Router(max_routes_per_topic=None)
        for i in range(200):
            router.add_route("s", "t", f"c{i}", "x")

    def test_postoffice_passes_cap_through_reset(self):
        po = PostOffice(max_routes_per_topic=3)
        po.reset()
        self.assertEqual(po.router.max_routes_per_topic, 3)


class TestInflightGauge(unittest.TestCase):
    def test_gauge_reports_depth_and_zero_after_removal(self):
        router = Router()
        src, dst = MagicMock(), MagicMock()
        src.name, dst.name = "src", "dst"
        router.register_client(src)
        router.register_client(dst)
        router.add_route("src", "in/#", "dst", "out")
        router.set_inflight_limit("src", 5)
        router.route("src", "in/a", b"x", ack_fn=MagicMock(), nack_fn=MagicMock())
        get = lambda: router.metrics.registry.get_sample_value(
            "postoffice_inflight_messages", {"source_broker": "src"})
        self.assertEqual(get(), 1.0)
        router.set_inflight_limit("src", None)
        self.assertEqual(get(), 0.0)


if __name__ == "__main__":
    unittest.main()
