import random
import unittest
from unittest.mock import MagicMock

from postoffice.app import PostOffice
from postoffice.backoff import ExponentialBackoff
from postoffice.ingress.retry import RetryTracker
from postoffice.router import Router


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class TestDelayFor(unittest.TestCase):
    def test_delay_for_is_stateless_and_capped(self):
        b = ExponentialBackoff(base=1, factor=2, cap=10, jitter="none")
        self.assertEqual([b.delay_for(n) for n in range(6)], [1, 2, 4, 8, 10, 10])
        self.assertEqual(b.delay_for(2), 4)  # no hidden state

    def test_delay_for_survives_huge_attempt_numbers(self):
        b = ExponentialBackoff(base=1, factor=2, cap=10, jitter="none")
        self.assertEqual(b.delay_for(100000), 10)

    def test_full_jitter_stays_within_ceiling(self):
        b = ExponentialBackoff(base=1, factor=2, cap=10, jitter="full", rng=random.Random(1))
        for n in range(8):
            self.assertLessEqual(b.delay_for(n), min(10, 2 ** n))


class TestRetryTracker(unittest.TestCase):
    def tracker(self, **kw):
        kw.setdefault("jitter", "none")
        return RetryTracker(**kw)

    def test_requeues_until_attempt_budget_is_spent(self):
        t = self.tracker(max_attempts=3, base_delay=0)
        decisions = [t.record_failure("m") for _ in range(3)]
        self.assertEqual([d.exhausted for d in decisions], [False, False, True])
        self.assertEqual([d.attempt for d in decisions], [1, 2, 3])

    def test_delay_follows_backoff(self):
        t = self.tracker(max_attempts=10, base_delay=1, factor=2, cap=5)
        self.assertEqual([t.record_failure("m").delay for _ in range(5)], [1, 2, 4, 5, 5])

    def test_zero_base_delay_means_immediate(self):
        self.assertEqual(self.tracker(base_delay=0).record_failure("m").delay, 0)

    def test_forget_resets_the_count(self):
        t = self.tracker(max_attempts=2, base_delay=0)
        t.record_failure("m")
        t.forget("m")
        self.assertFalse(t.record_failure("m").exhausted)

    def test_keys_are_independent(self):
        t = self.tracker(max_attempts=2, base_delay=0)
        t.record_failure("a")
        self.assertFalse(t.record_failure("b").exhausted)
        self.assertTrue(t.record_failure("a").exhausted)

    def test_table_is_bounded_lru(self):
        t = self.tracker(max_keys=2, base_delay=0)
        for key in ("a", "b", "c"):
            t.record_failure(key)
        self.assertEqual(t.tracked_keys, 2)

    def test_idle_keys_expire(self):
        clock = FakeClock()
        t = self.tracker(max_attempts=2, base_delay=0, ttl=60, clock=clock)
        t.record_failure("m")
        clock.now = 61.0
        self.assertFalse(t.record_failure("m").exhausted)

    def test_invalid_config(self):
        for kw in ({"max_attempts": 0}, {"base_delay": -1}, {"max_keys": 0}, {"ttl": 0},
                   {"base_delay": 2, "cap": 1}):
            with self.assertRaises(ValueError, msg=str(kw)):
                RetryTracker(**kw)


def make_router(fail=True, **policy):
    router = Router()
    src, dst = MagicMock(), MagicMock()
    src.name, dst.name = "src", "dst"
    router.register_client(src)
    router.register_client(dst)
    router.add_route("src", "in/#", "dst", "out")
    outcome = {"fail": fail}

    def publish(topic, msg, on_confirm=None, on_error=None, **kw):
        (on_error(RuntimeError("target down")) if outcome["fail"] else on_confirm())

    dst.publish.side_effect = publish
    scheduled = []
    policy.setdefault("jitter", "none")
    router.set_retry_policy(scheduler=lambda delay, fn: scheduled.append((delay, fn)), **policy)
    return router, outcome, scheduled


def deliver(router, body=b"x", **kw):
    ack, nack = MagicMock(), MagicMock()
    router.route("src", "in/a", body, ack_fn=ack, nack_fn=nack, **kw)
    return ack, nack


class TestRouterRetryCap(unittest.TestCase):
    def test_failed_delivery_is_requeued_only_after_the_backoff_delay(self):
        router, _, scheduled = make_router(max_attempts=5, base_delay=2)
        ack, nack = deliver(router)
        nack.assert_not_called()  # held, not hot-looped
        self.assertEqual([d for d, _ in scheduled], [2])
        scheduled[0][1]()
        nack.assert_called_once_with(requeue=True)
        ack.assert_not_called()

    def test_zero_delay_requeues_immediately(self):
        router, _, scheduled = make_router(max_attempts=5, base_delay=0)
        _, nack = deliver(router)
        nack.assert_called_once_with(requeue=True)
        self.assertEqual(scheduled, [])

    def test_poison_message_stops_requeueing_at_the_cap(self):
        router, _, scheduled = make_router(max_attempts=3, base_delay=0)
        results = [deliver(router)[1] for _ in range(5)]
        calls = [n.call_args.kwargs["requeue"] for n in results]
        self.assertEqual(calls, [True, True, False, True, True])  # 4th restarts: key was cleared

    def test_exhaustion_is_counted(self):
        router, _, _ = make_router(max_attempts=2, base_delay=0)
        deliver(router)
        deliver(router)
        value = router.metrics.registry.get_sample_value(
            "postoffice_routing_errors_total",
            {"source_broker": "src", "error_type": "retry_exhausted"})
        self.assertEqual(value, 1.0)

    def test_success_forgets_the_message(self):
        router, outcome, _ = make_router(max_attempts=2, base_delay=0)
        deliver(router)  # failure 1
        outcome["fail"] = False
        ack, _ = deliver(router)
        ack.assert_called_once()
        outcome["fail"] = True
        _, nack = deliver(router)  # counts as failure 1 again, not 2
        nack.assert_called_once_with(requeue=True)

    def test_different_messages_are_counted_separately(self):
        router, _, _ = make_router(max_attempts=2, base_delay=0)
        deliver(router, body=b"one")
        _, nack = deliver(router, body=b"two")
        nack.assert_called_once_with(requeue=True)

    def test_explicit_message_id_groups_redeliveries(self):
        router, _, _ = make_router(max_attempts=2, base_delay=0)
        deliver(router, body=b"v1", message_id="m-1")
        _, nack = deliver(router, body=b"v2", message_id="m-1")
        nack.assert_called_once_with(requeue=False)

    def test_cap_disabled_keeps_legacy_unbounded_requeue(self):
        router, _, _ = make_router(max_attempts=None)
        for _ in range(10):
            _, nack = deliver(router)
            nack.assert_called_once_with(requeue=True)

    def test_dlq_still_takes_precedence_over_retry(self):
        router, _, scheduled = make_router(max_attempts=3, base_delay=1)
        dlq = MagicMock()
        dlq.name = "dlq"
        router.register_client(dlq)
        router.set_dlq("dlq", "system/dlq")
        ack, nack = deliver(router)
        ack.assert_called_once()
        nack.assert_not_called()
        self.assertEqual(scheduled, [])

    def test_slot_stays_held_through_the_delay(self):
        router, _, scheduled = make_router(max_attempts=3, base_delay=1)
        router.set_inflight_limit("src", 2)
        deliver(router)
        self.assertEqual(router.inflight("src"), 1)
        scheduled[0][1]()
        self.assertEqual(router.inflight("src"), 0)

    def test_default_policy_is_a_bounded_cap_with_delay(self):
        policy = Router().retry
        self.assertIsNotNone(policy)
        self.assertGreater(policy.max_attempts, 1)
        self.assertGreater(policy.base_delay, 0)

    def test_postoffice_policy_survives_reset(self):
        po = PostOffice()
        po.set_retry_policy(max_attempts=7, base_delay=0)
        po.reset()
        self.assertEqual(po.router.retry.max_attempts, 7)


if __name__ == "__main__":
    unittest.main()
