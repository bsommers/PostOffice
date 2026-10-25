import json
import unittest
from unittest.mock import MagicMock

from postoffice.dlq import BudgetedDlq
from postoffice.ratelimit import TokenBucket
from postoffice.router import Router


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class TestTokenBucket(unittest.TestCase):
    def test_allows_burst_then_denies_then_refills(self):
        clock = FakeClock()
        bucket = TokenBucket(rate=2.0, burst=3.0, clock=clock)
        self.assertEqual([bucket.try_consume() for _ in range(4)], [True, True, True, False])
        clock.advance(1.0)
        self.assertEqual([bucket.try_consume() for _ in range(3)], [True, True, False])

    def test_refill_never_exceeds_burst(self):
        clock = FakeClock()
        bucket = TokenBucket(rate=10.0, burst=2.0, clock=clock)
        clock.advance(100.0)
        self.assertEqual([bucket.try_consume() for _ in range(3)], [True, True, False])

    def test_cost_larger_than_burst_is_denied(self):
        bucket = TokenBucket(rate=10.0, burst=5.0, clock=FakeClock())
        self.assertFalse(bucket.try_consume(6.0))

    def test_invalid_arguments_raise_value_error(self):
        with self.assertRaises(ValueError):
            TokenBucket(rate=0, burst=1)
        with self.assertRaises(ValueError):
            TokenBucket(rate=1, burst=0)


class TestBudgetedDlq(unittest.TestCase):
    def test_oversize_payload_is_refused_not_truncated(self):
        budget = BudgetedDlq(max_message_bytes=100, clock=FakeClock())
        self.assertEqual(budget.check("src", 101), "oversize")
        self.assertIsNone(budget.check("src", 100))

    def test_message_rate_exceeded_returns_rate_reason_then_recovers(self):
        clock = FakeClock()
        budget = BudgetedDlq(msgs_per_sec=2, clock=clock)
        self.assertIsNone(budget.check("src", 10))
        self.assertIsNone(budget.check("src", 10))
        self.assertEqual(budget.check("src", 10), "rate")
        clock.advance(1.0)
        self.assertIsNone(budget.check("src", 10))

    def test_byte_rate_exceeded_returns_bytes_reason(self):
        budget = BudgetedDlq(max_message_bytes=100, bytes_per_sec=150, clock=FakeClock())
        self.assertIsNone(budget.check("src", 100))
        self.assertEqual(budget.check("src", 100), "bytes")

    def test_budgets_are_isolated_per_source_broker(self):
        budget = BudgetedDlq(msgs_per_sec=1, clock=FakeClock())
        self.assertIsNone(budget.check("noisy", 10))
        self.assertEqual(budget.check("noisy", 10), "rate")
        self.assertIsNone(budget.check("quiet", 10))

    def test_source_table_is_bounded(self):
        budget = BudgetedDlq(msgs_per_sec=1, max_sources=2, clock=FakeClock())
        for name in ("a", "b", "c", "d"):
            budget.check(name, 1)
        self.assertLessEqual(budget.tracked_sources, 2)

    def test_refused_check_does_not_consume_tokens_for_other_limits(self):
        budget = BudgetedDlq(max_message_bytes=10, msgs_per_sec=1, clock=FakeClock())
        self.assertEqual(budget.check("src", 11), "oversize")
        self.assertIsNone(budget.check("src", 5))

    def test_invalid_arguments_raise_value_error(self):
        with self.assertRaises(ValueError):
            BudgetedDlq(max_message_bytes=0)


class TestRouterAppliesDlqBudget(unittest.TestCase):
    def _router(self, budget):
        router = Router()
        src, dlq = MagicMock(), MagicMock()
        src.name, dlq.name = "src", "dlq"
        router.register_client(src)
        router.register_client(dlq)
        router.set_dlq("dlq", "system/dlq")
        router.set_unroutable_policy("dlq")
        router.set_dlq_budget(budget)
        return router, dlq

    def test_oversize_unroutable_not_stored_and_nacked(self):
        router, dlq = self._router(BudgetedDlq(max_message_bytes=10, clock=FakeClock()))
        ack, nack = MagicMock(), MagicMock()
        router.route("src", "nowhere", b"x" * 11, ack_fn=ack, nack_fn=nack)
        dlq.publish.assert_not_called()
        nack.assert_called_once_with(requeue=False)
        ack.assert_not_called()

    def test_flood_is_capped_at_budget(self):
        router, dlq = self._router(BudgetedDlq(msgs_per_sec=3, clock=FakeClock()))
        nack = MagicMock()
        for _ in range(50):
            router.route("src", "nowhere", b"junk", ack_fn=MagicMock(), nack_fn=nack)
        self.assertEqual(dlq.publish.call_count, 3)
        self.assertEqual(nack.call_count, 47)

    def test_refusals_are_counted_by_reason(self):
        router, _ = self._router(BudgetedDlq(max_message_bytes=10, clock=FakeClock()))
        router.route("src", "nowhere", b"x" * 11, nack_fn=MagicMock())
        text = router.metrics.get_metrics_text()
        self.assertIn('postoffice_dlq_refused_total{reason="oversize",source_broker="src"} 1.0', text)

    def test_delivery_failure_dlq_also_budgeted(self):
        router = Router()
        src, tgt, dlq = MagicMock(), MagicMock(), MagicMock()
        src.name, tgt.name, dlq.name = "src", "tgt", "dlq"
        for c in (src, tgt, dlq):
            router.register_client(c)
        router.set_dlq("dlq", "system/dlq")
        router.set_dlq_budget(BudgetedDlq(max_message_bytes=10, clock=FakeClock()))
        router.add_route("src", "in", "tgt", "out")
        tgt.publish.side_effect = RuntimeError("down")
        router.route("src", "in", b"x" * 11, ack_fn=MagicMock(), nack_fn=MagicMock())
        dlq.publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
