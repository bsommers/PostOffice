import unittest
from unittest.mock import MagicMock

from postoffice.app import PostOffice
from postoffice.ingress import (
    Action, Firewall, MaxPayloadBytes, MessageContext, Rule, SenderLedger,
)
from postoffice.ingress.rules import ALLOW


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def ledger(clock=None, **kw):
    kw.setdefault("clock", clock or FakeClock())
    return SenderLedger(**kw)


def ctx(topic="dev/d1/data", size=100, broker="src", sender=None):
    return MessageContext(broker=broker, topic=topic, size=size, payload=b"", sender=sender)


class TestCounting(unittest.TestCase):
    def test_counts_per_sender_and_category(self):
        led = ledger()
        led.record("src", "d1", "oversize")
        led.record("src", "d1", "oversize")
        led.record("src", "d1", "malformed")
        led.record("src", "d2", "rate_exceeded")
        rows = {r["sender"]: r for r in led.sender_report()}
        self.assertEqual(rows["d1"]["categories"], {"oversize": 2, "malformed": 1})
        self.assertEqual(rows["d1"]["total"], 3)
        self.assertEqual(rows["d2"]["categories"], {"rate_exceeded": 1})
        self.assertEqual(rows["d1"]["broker"], "src")

    def test_row_shape_has_first_and_last_seen(self):
        clock = FakeClock()
        led = ledger(clock)
        clock.now = 5.0
        led.record("src", "d1", "oversize")
        clock.now = 9.0
        led.record("src", "d1", "oversize")
        row = led.sender_report()[0]
        for field in ("broker", "sender", "total", "categories", "window", "previous_window",
                      "rising", "first_seen", "last_seen", "likely_cause", "cooled_until"):
            self.assertIn(field, row)
        self.assertEqual((row["first_seen"], row["last_seen"]), (5.0, 9.0))

    def test_report_top_n_orders_by_recent_then_total(self):
        led = ledger()
        for _ in range(5):
            led.record("src", "loud", "oversize")
        led.record("src", "quiet", "oversize")
        for _ in range(3):
            led.record("src", "mid", "oversize")
        senders = [r["sender"] for r in led.sender_report(top=2)]
        self.assertEqual(senders, ["loud", "mid"])

    def test_same_sender_on_different_brokers_is_separate(self):
        led = ledger()
        led.record("a", "d1", "oversize")
        led.record("b", "d1", "oversize")
        self.assertEqual(len(led.sender_report()), 2)

    def test_unknown_category_rejected(self):
        with self.assertRaises(ValueError):
            ledger().record("src", "d1", "bogus")


class TestTrend(unittest.TestCase):
    def test_window_rotation_populates_previous_window(self):
        clock = FakeClock()
        led = ledger(clock, window=60)
        for _ in range(4):
            led.record("src", "d1", "oversize")
        clock.now = 61.0
        led.record("src", "d1", "oversize")
        row = led.sender_report()[0]
        self.assertEqual((row["window"], row["previous_window"]), (1, 4))

    def test_skipping_a_full_window_zeroes_previous(self):
        clock = FakeClock()
        led = ledger(clock, window=60)
        for _ in range(4):
            led.record("src", "d1", "oversize")
        clock.now = 130.0
        led.record("src", "d1", "oversize")
        row = led.sender_report()[0]
        self.assertEqual((row["window"], row["previous_window"]), (1, 0))

    def test_rising_when_window_exceeds_factor_times_previous(self):
        clock = FakeClock()
        led = ledger(clock, window=60, rise_factor=2.0, rise_min_events=5)
        for _ in range(3):
            led.record("src", "d1", "oversize")
        clock.now = 61.0
        for _ in range(8):
            led.record("src", "d1", "oversize")
        self.assertTrue(led.sender_report()[0]["rising"])

    def test_steady_rate_is_not_rising(self):
        clock = FakeClock()
        led = ledger(clock, window=60, rise_factor=2.0, rise_min_events=5)
        for _ in range(8):
            led.record("src", "d1", "oversize")
        clock.now = 61.0
        for _ in range(8):
            led.record("src", "d1", "oversize")
        self.assertFalse(led.sender_report()[0]["rising"])

    def test_small_counts_below_min_events_are_not_rising(self):
        led = ledger(rise_min_events=5)
        led.record("src", "d1", "oversize")
        self.assertFalse(led.sender_report()[0]["rising"])

    def test_rising_logs_one_warning_per_window(self):
        led = ledger(rise_min_events=3)
        with self.assertLogs("postoffice.ingress.ledger", level="WARNING") as cm:
            for _ in range(10):
                led.record("src", "d1", "oversize")
        self.assertEqual(len(cm.output), 1)
        self.assertIn("d1", cm.output[0])


class TestBounds(unittest.TestCase):
    def test_overflow_keys_aggregate_into_other(self):
        led = ledger(max_keys=2)
        led.record("src", "a", "oversize")
        led.record("src", "b", "oversize")
        led.record("src", "c", "malformed")
        led.record("src", "d", "malformed")
        self.assertEqual(led.tracked_keys, 2)
        rows = {r["sender"]: r for r in led.sender_report()}
        self.assertEqual(rows["other"]["total"], 2)
        self.assertEqual(rows["other"]["categories"], {"malformed": 2})
        self.assertEqual(led.overflow_events, 2)

    def test_existing_keys_still_tracked_when_full(self):
        led = ledger(max_keys=1)
        led.record("src", "a", "oversize")
        led.record("src", "zzz", "oversize")
        led.record("src", "a", "oversize")
        rows = {r["sender"]: r for r in led.sender_report()}
        self.assertEqual(rows["a"]["total"], 2)

    def test_idle_keys_expire_and_free_slots(self):
        clock = FakeClock()
        led = ledger(clock, max_keys=1, ttl=100)
        led.record("src", "a", "oversize")
        clock.now = 101.0
        led.record("src", "b", "oversize")
        senders = {r["sender"] for r in led.sender_report()}
        self.assertEqual(senders, {"b"})

    def test_invalid_bounds_rejected(self):
        for kw in ({"max_keys": 0}, {"ttl": 0}, {"window": 0}):
            with self.assertRaises(ValueError):
                ledger(**kw)


class TestLikelyCause(unittest.TestCase):
    def test_single_category_repeated_suggests_misconfig(self):
        led = ledger(rise_min_events=100)
        for _ in range(6):
            led.record("src", "d1", "oversize")
        self.assertEqual(led.sender_report()[0]["likely_cause"], "repeated_identical_error")

    def test_mixed_categories_at_high_rate_suggests_abuse(self):
        led = ledger(rise_min_events=5)
        for cat in ("oversize", "malformed", "rate_exceeded", "forbidden_topic") * 3:
            led.record("src", "d1", cat)
        self.assertEqual(led.sender_report()[0]["likely_cause"], "mixed_high_rate")

    def test_many_senders_failing_at_once_suggests_widespread(self):
        led = ledger(widespread_senders=4)
        for i in range(4):
            led.record("src", f"d{i}", "malformed")
        causes = {r["likely_cause"] for r in led.sender_report()}
        self.assertEqual(causes, {"widespread"})

    def test_few_events_has_no_confident_cause(self):
        led = ledger()
        led.record("src", "d1", "oversize")
        self.assertEqual(led.sender_report()[0]["likely_cause"], "insufficient_data")


class TestSenderKey(unittest.TestCase):
    def test_explicit_sender_wins(self):
        self.assertEqual(ledger().key_for(ctx(sender="user-9")), "user-9")

    def test_template_capture_then_topic_fallback(self):
        led = ledger(identity_template="dev/{id}/data")
        self.assertEqual(led.key_for(ctx(topic="dev/abc/data")), "abc")
        self.assertEqual(led.key_for(ctx(topic="other/x")), "other/x")

    def test_default_key_is_topic(self):
        self.assertEqual(ledger().key_for(ctx(topic="a/b")), "a/b")


class TestObserveOnly(unittest.TestCase):
    def test_observe_mode_never_changes_verdicts(self):
        led = ledger(enforce=False, strike_threshold=2)
        fw = Firewall([MaxPayloadBytes(10)], ledger=led)
        for _ in range(20):
            self.assertEqual(fw.evaluate(ctx(size=100)).rule, "max_payload_bytes")
        small = fw.evaluate(ctx(size=1))
        self.assertEqual(small.action, Action.ALLOW)
        self.assertEqual(led.sender_report()[0]["total"], 20)
        self.assertIsNone(led.sender_report()[0]["cooled_until"])

    def test_allowed_messages_are_not_recorded(self):
        led = ledger()
        fw = Firewall([MaxPayloadBytes(10)], ledger=led)
        fw.evaluate(ctx(size=1))
        self.assertEqual(led.sender_report(), [])

    def test_rule_errors_are_not_blamed_on_the_sender(self):
        class Boom(Rule):
            def _check(self, c):
                raise RuntimeError("bug")
        led = ledger()
        fw = Firewall([Boom()], ledger=led)
        self.assertEqual(fw.evaluate(ctx()).category, "rule_error")
        self.assertEqual(led.sender_report(), [])

    def test_ledger_failure_does_not_break_evaluation(self):
        led = ledger()
        led.record = MagicMock(side_effect=RuntimeError("ledger bug"))
        fw = Firewall([MaxPayloadBytes(10)], ledger=led)
        self.assertEqual(fw.evaluate(ctx(size=100)).rule, "max_payload_bytes")


class TestEnforcement(unittest.TestCase):
    def make(self, clock, **kw):
        defaults = dict(enforce=True, strike_threshold=3, cooldown_base=10, cooldown_factor=2,
                        cooldown_cap=40, decay_after=100, ttl=1000, window=60)
        defaults.update(kw)
        return ledger(clock, **defaults)

    def violate(self, fw, n, **kw):
        for _ in range(n):
            fw.evaluate(ctx(size=100, **kw))

    def test_threshold_crossing_starts_cooldown_and_drops_before_rules(self):
        clock = FakeClock()
        led = self.make(clock)
        spy = MagicMock(spec=Rule)
        spy.name = "spy"
        spy.check.return_value = ALLOW
        fw = Firewall([MaxPayloadBytes(10), spy], ledger=led)
        self.violate(fw, 3)
        spy.check.reset_mock()
        clock.now = 5.0
        verdict = fw.evaluate(ctx(size=1))  # would be allowed by every rule
        self.assertEqual((verdict.action, verdict.rule, verdict.category),
                         (Action.DROP, "cooldown", "cooled"))
        spy.check.assert_not_called()

    def test_other_senders_unaffected(self):
        clock = FakeClock()
        fw = Firewall([MaxPayloadBytes(10)], ledger=self.make(clock))
        self.violate(fw, 3, topic="dev/bad/data")
        self.assertEqual(fw.evaluate(ctx(topic="dev/good/data", size=1)).action, Action.ALLOW)

    def test_cooldown_expires(self):
        clock = FakeClock()
        led = self.make(clock)
        fw = Firewall([MaxPayloadBytes(10)], ledger=led)
        self.violate(fw, 3)
        clock.now = 10.0
        self.assertEqual(fw.evaluate(ctx(size=1)).action, Action.ALLOW)

    def test_repeat_crossing_doubles_cooldown_up_to_cap(self):
        clock = FakeClock()
        led = self.make(clock)
        fw = Firewall([MaxPayloadBytes(10)], ledger=led)
        durations = []
        for _ in range(5):
            self.violate(fw, 3)
            until = led.sender_report()[0]["cooled_until"]
            durations.append(until - clock.now)
            clock.now = until
        self.assertEqual(durations, [10, 20, 40, 40, 40])

    def test_quiet_period_decays_strikes(self):
        clock = FakeClock()
        led = self.make(clock)
        fw = Firewall([MaxPayloadBytes(10)], ledger=led)
        self.violate(fw, 3)
        clock.now = 10.0
        self.violate(fw, 3)  # second strike: 20s
        clock.now = 400.0    # long quiet period
        self.violate(fw, 3)
        self.assertEqual(led.sender_report()[0]["cooled_until"] - 400.0, 10)

    def test_hammering_while_cooled_does_not_escalate(self):
        clock = FakeClock()
        led = self.make(clock)
        fw = Firewall([MaxPayloadBytes(10)], ledger=led)
        self.violate(fw, 3)
        for _ in range(20):
            fw.evaluate(ctx(size=100))
        self.assertEqual(led.sender_report()[0]["cooled_until"], 10.0)

    def test_enforce_requires_ttl_longer_than_cooldown_cap(self):
        with self.assertRaises(ValueError):
            ledger(enforce=True, ttl=30, cooldown_cap=60)


class TestIntegration(unittest.TestCase):
    def make_po(self, **ledger_cfg):
        po = PostOffice()
        po.configure_ingress({
            "rules": [{"type": "max_payload_bytes", "max_bytes": 10}],
            "ledger": ledger_cfg,
        })
        return po

    def test_from_dict_builds_ledger(self):
        fw = Firewall.from_dict({"rules": [], "ledger": {"max_keys": 5, "enforce": True,
                                                         "strike_threshold": 2}})
        self.assertIsInstance(fw.ledger, SenderLedger)
        self.assertTrue(fw.ledger.enforce)

    def test_postoffice_sender_report_after_router_rejects(self):
        po = self.make_po(identity_template="dev/{id}/data")
        nack = MagicMock()
        po.router.route("src", "dev/d7/data", b"x" * 100, nack_fn=nack)
        rows = po.sender_report()
        self.assertEqual((rows[0]["sender"], rows[0]["categories"]), ("d7", {"oversize": 1}))

    def test_sender_report_empty_without_ledger(self):
        self.assertEqual(PostOffice().sender_report(), [])

    def test_unroutable_messages_are_recorded(self):
        po = self.make_po(identity_template="dev/{id}/data")
        po.router.route("src", "dev/d9/data", b"ok", nack_fn=MagicMock())
        row = po.sender_report()[0]
        self.assertEqual((row["sender"], row["categories"]), ("d9", {"unroutable": 1}))

    def test_sender_event_metric_counted(self):
        po = self.make_po()
        po.router.route("src", "a/b", b"x" * 100, nack_fn=MagicMock())
        value = po.router.metrics.registry.get_sample_value(
            "postoffice_sender_events_total", {"source_broker": "src", "category": "oversize"})
        self.assertEqual(value, 1.0)

    def test_cooled_sender_is_acked_and_dropped_via_router(self):
        po = self.make_po(enforce=True, strike_threshold=2, cooldown_base=10, cooldown_cap=20,
                          ttl=100)
        for _ in range(2):
            po.router.route("src", "a/b", b"x" * 100, nack_fn=MagicMock())
        ack = MagicMock()
        po.router.route("src", "a/b", b"ok", ack_fn=ack, nack_fn=MagicMock())
        ack.assert_called_once()


if __name__ == "__main__":
    unittest.main()
