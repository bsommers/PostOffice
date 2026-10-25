import unittest
from unittest.mock import MagicMock

from postoffice.app import PostOffice
from postoffice.ingress import (
    Action, Firewall, MaxPayloadBytes, MessageContext, RateLimit, TopicShape,
    extract_identity,
)
from postoffice.ratelimit import KeyedLimiter
from postoffice.router import Router


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def ctx(topic="a/b", size=10, broker="src", payload=b""):
    return MessageContext(broker=broker, topic=topic, size=size, payload=payload)


class TestKeyedLimiter(unittest.TestCase):
    def test_burst_then_deny_then_refill(self):
        clock = FakeClock()
        lim = KeyedLimiter(rate=1, burst=2, max_keys=10, ttl=60, clock=clock)
        self.assertTrue(lim.try_consume("k"))
        self.assertTrue(lim.try_consume("k"))
        self.assertFalse(lim.try_consume("k"))
        clock.now = 1.0
        self.assertTrue(lim.try_consume("k"))

    def test_keys_are_isolated(self):
        lim = KeyedLimiter(rate=1, burst=1, max_keys=10, ttl=60, clock=FakeClock())
        self.assertTrue(lim.try_consume("a"))
        self.assertFalse(lim.try_consume("a"))
        self.assertTrue(lim.try_consume("b"))

    def test_lru_eviction_bounds_memory(self):
        lim = KeyedLimiter(rate=1, burst=1, max_keys=3, ttl=60, clock=FakeClock())
        for i in range(100):
            lim.try_consume(f"k{i}")
        self.assertEqual(lim.tracked_keys, 3)

    def test_ttl_expires_idle_keys(self):
        clock = FakeClock()
        lim = KeyedLimiter(rate=1, burst=1, max_keys=10, ttl=10, clock=clock)
        lim.try_consume("a")
        clock.now = 11.0
        lim.try_consume("b")
        self.assertEqual(lim.tracked_keys, 1)


class TestIdentity(unittest.TestCase):
    def test_extracts_named_segment(self):
        self.assertEqual(extract_identity("factory/{device}/telemetry", "factory/p7/telemetry"), "p7")

    def test_no_match_returns_none(self):
        self.assertIsNone(extract_identity("factory/{device}/telemetry", "other/p7/telemetry"))

    def test_multi_level_tail(self):
        self.assertEqual(extract_identity("f/{device}/#", "f/p7/x/y"), "p7")


class TestRules(unittest.TestCase):
    def test_payload_over_limit_rejected_by_size_only(self):
        rule = MaxPayloadBytes(100)
        v = rule.check(ctx(size=101, payload=None))
        self.assertEqual(v.action, Action.REJECT)
        self.assertEqual(v.category, "oversize")
        self.assertEqual(rule.check(ctx(size=100)).action, Action.ALLOW)

    def test_topic_too_long_rejected(self):
        self.assertEqual(TopicShape(max_length=5).check(ctx(topic="abcdef")).action, Action.REJECT)

    def test_topic_too_deep_rejected(self):
        self.assertEqual(TopicShape(max_depth=3).check(ctx(topic="a/b/c/d")).action, Action.REJECT)

    def test_denied_pattern_rejected(self):
        rule = TopicShape(deny=["$SYS/#", "+/admin"])
        self.assertEqual(rule.check(ctx(topic="$SYS/broker/x")).action, Action.REJECT)
        self.assertEqual(rule.check(ctx(topic="x/admin")).action, Action.REJECT)
        self.assertEqual(rule.check(ctx(topic="x/user")).action, Action.ALLOW)

    def test_null_byte_and_empty_topic_rejected(self):
        rule = TopicShape()
        self.assertEqual(rule.check(ctx(topic="a\x00b")).action, Action.REJECT)
        self.assertEqual(rule.check(ctx(topic="")).action, Action.REJECT)
        self.assertEqual(rule.check(ctx(topic="a/b")).category, None)

    def test_rate_limit_per_identity(self):
        clock = FakeClock()
        rule = RateLimit(rate=1, burst=1, key="identity", template="d/{device}/t", clock=clock)
        self.assertEqual(rule.check(ctx(topic="d/p1/t")).action, Action.ALLOW)
        v = rule.check(ctx(topic="d/p1/t"))
        self.assertEqual((v.action, v.category), (Action.REJECT, "rate_exceeded"))
        self.assertEqual(rule.check(ctx(topic="d/p2/t")).action, Action.ALLOW)

    def test_rate_limit_bytes_cost(self):
        rule = RateLimit(rate=100, burst=100, key="broker", cost="bytes", clock=FakeClock())
        self.assertEqual(rule.check(ctx(size=80)).action, Action.ALLOW)
        self.assertEqual(rule.check(ctx(size=80)).action, Action.REJECT)

    def test_rate_limit_configurable_drop_action(self):
        rule = RateLimit(rate=1, burst=1, action=Action.DROP, clock=FakeClock())
        rule.check(ctx())
        self.assertEqual(rule.check(ctx()).action, Action.DROP)

    def test_rule_scoped_to_brokers(self):
        rule = MaxPayloadBytes(1, brokers=["only"])
        self.assertEqual(rule.check(ctx(size=5, broker="other")).action, Action.ALLOW)
        self.assertEqual(rule.check(ctx(size=5, broker="only")).action, Action.REJECT)


class TestFirewall(unittest.TestCase):
    def test_first_non_allow_wins_and_names_rule(self):
        fw = Firewall([MaxPayloadBytes(10), TopicShape(max_depth=1)])
        v = fw.evaluate(ctx(topic="a/b/c", size=999))
        self.assertEqual(v.rule, "max_payload_bytes")

    def test_all_allow(self):
        self.assertEqual(Firewall([MaxPayloadBytes(10)]).evaluate(ctx()).action, Action.ALLOW)

    def test_rule_exception_uses_on_rule_error(self):
        bad = MagicMock()
        bad.check.side_effect = RuntimeError("boom")
        bad.name = "bad"
        self.assertEqual(Firewall([bad]).evaluate(ctx()).action, Action.REJECT)
        v = Firewall([bad], on_rule_error=Action.ALLOW).evaluate(ctx())
        self.assertEqual(v.action, Action.ALLOW)

    def test_from_dict(self):
        fw = Firewall.from_dict({
            "on_rule_error": "reject",
            "rules": [
                {"type": "max_payload_bytes", "max_bytes": 5},
                {"type": "topic_shape", "max_depth": 2, "deny": ["x/#"]},
                {"type": "rate_limit", "rate": 5, "burst": 5, "key": "topic", "action": "drop"},
            ],
        })
        self.assertEqual(fw.evaluate(ctx(size=6)).action, Action.REJECT)

    def test_from_dict_unknown_rule_raises(self):
        with self.assertRaises(ValueError):
            Firewall.from_dict({"rules": [{"type": "nope"}]})

    def test_from_dict_bad_action_raises(self):
        with self.assertRaises(ValueError):
            Firewall.from_dict({"on_rule_error": "explode", "rules": []})


class TestRouterIntegration(unittest.TestCase):
    def _router(self, firewall):
        router = Router()
        src, tgt = MagicMock(), MagicMock()
        src.name, tgt.name = "src", "tgt"
        router.register_client(src)
        router.register_client(tgt)
        router.add_route("src", "in/#", "tgt", "out")
        router.set_firewall(firewall)
        return router, tgt

    def test_reject_nacks_without_requeue_and_skips_routing(self):
        router, tgt = self._router(Firewall([MaxPayloadBytes(3)]))
        ack, nack = MagicMock(), MagicMock()
        router.route("src", "in/x", b"toolong", ack_fn=ack, nack_fn=nack)
        nack.assert_called_once_with(requeue=False)
        ack.assert_not_called()
        tgt.publish.assert_not_called()

    def test_drop_acks(self):
        fw = Firewall([RateLimit(rate=1, burst=1, action=Action.DROP, clock=FakeClock())])
        router, tgt = self._router(fw)
        router.route("src", "in/x", b"a", ack_fn=MagicMock(), nack_fn=MagicMock())
        ack = MagicMock()
        router.route("src", "in/x", b"b", ack_fn=ack, nack_fn=MagicMock())
        ack.assert_called_once()
        self.assertEqual(tgt.publish.call_count, 1)

    def test_firewall_runs_before_trie_match(self):
        router, _ = self._router(Firewall([MaxPayloadBytes(1)]))
        router.client_tries = MagicMock()
        router.route("src", "in/x", b"toolong", ack_fn=MagicMock(), nack_fn=MagicMock())
        router.client_tries.get.assert_not_called()

    def test_allow_passes_through(self):
        router, tgt = self._router(Firewall([MaxPayloadBytes(100)]))
        router.route("src", "in/x", b"ok", ack_fn=MagicMock(), nack_fn=MagicMock())
        tgt.publish.assert_called_once()

    def test_reject_without_nack_fn_is_shed_and_counted(self):
        router, _ = self._router(Firewall([MaxPayloadBytes(1)]))
        router.route("src", "in/x", b"toolong")
        sample = router.metrics.registry.get_sample_value(
            "postoffice_ingress_total",
            {"source_broker": "src", "rule": "max_payload_bytes", "action": "reject"},
        )
        self.assertEqual(sample, 1.0)


class TestFacade(unittest.TestCase):
    def test_configure_ingress_accepts_dict_and_survives_reset(self):
        po = PostOffice()
        po.configure_ingress({"rules": [{"type": "max_payload_bytes", "max_bytes": 5}]})
        self.assertIsNotNone(po.router.firewall)
        po.reset()
        self.assertIsNotNone(po.router.firewall)

    def test_configure_ingress_none_disables(self):
        po = PostOffice()
        po.configure_ingress({"rules": []})
        po.configure_ingress(None)
        self.assertIsNone(po.router.firewall)


if __name__ == "__main__":
    unittest.main()
