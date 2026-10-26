import json
import unittest
from unittest.mock import MagicMock

from postoffice.dlq import BudgetedDlq
from postoffice.router import Router


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


class TestBudgetDedupe(unittest.TestCase):
    def budget(self, clock=None, **kw):
        kw.setdefault("dedupe_window", 10)
        return BudgetedDlq(clock=clock or FakeClock(), **kw)

    def test_identical_fingerprint_within_window_is_duplicate(self):
        b = self.budget()
        self.assertIsNone(b.admit("src", 10, "fp").refusal)
        self.assertEqual(b.admit("src", 10, "fp").refusal, "duplicate")

    def test_different_fingerprints_are_independent(self):
        b = self.budget()
        b.admit("src", 10, "a")
        self.assertIsNone(b.admit("src", 10, "b").refusal)

    def test_repeat_count_reported_on_next_admission_after_window(self):
        clock = FakeClock()
        b = self.budget(clock)
        b.admit("src", 10, "fp")
        for _ in range(3):
            b.admit("src", 10, "fp")
        clock.now = 11.0
        admission = b.admit("src", 10, "fp")
        self.assertIsNone(admission.refusal)
        self.assertEqual(admission.repeat_count, 3)
        clock.now = 22.0
        self.assertEqual(b.admit("src", 10, "fp").repeat_count, 0)

    def test_duplicates_do_not_consume_rate_budget(self):
        b = self.budget(msgs_per_sec=1)
        b.admit("src", 10, "fp")
        for _ in range(20):
            self.assertEqual(b.admit("src", 10, "fp").refusal, "duplicate")

    def test_off_by_default(self):
        b = BudgetedDlq(clock=FakeClock())
        self.assertIsNone(b.admit("src", 10, "fp").refusal)
        self.assertIsNone(b.admit("src", 10, "fp").refusal)

    def test_fingerprint_table_is_bounded(self):
        b = self.budget(max_fingerprints=2)
        for fp in ("a", "b", "c"):
            b.admit("src", 1, fp)
        self.assertEqual(b.tracked_fingerprints, 2)

    def test_check_still_returns_the_refusal_string(self):
        self.assertEqual(self.budget(max_message_bytes=5).check("src", 6), "oversize")


def make_router(**budget):
    router = Router()
    router.set_retry_policy(base_delay=0)
    src, dlq = MagicMock(), MagicMock()
    src.name, dlq.name = "src", "dlq"
    router.register_client(src)
    router.register_client(dlq)
    router.set_dlq("dlq", "system/dlq")
    router.set_unroutable_policy("dlq")
    router.set_dlq_budget(BudgetedDlq(**budget))
    return router, dlq


def send(router, body=b"x"):
    ack, nack = MagicMock(), MagicMock()
    router.route("src", "nowhere", body, ack_fn=ack, nack_fn=nack)
    return ack, nack


def envelope(dlq, index=-1):
    return json.loads(dlq.publish.call_args_list[index].args[1])


class TestRouterDedupe(unittest.TestCase):
    def test_identical_messages_published_once_and_duplicates_acked(self):
        router, dlq = make_router(dedupe_window=60)
        send(router)
        ack, nack = send(router)
        self.assertEqual(dlq.publish.call_count, 1)
        ack.assert_called_once()
        nack.assert_not_called()
        value = router.metrics.registry.get_sample_value(
            "postoffice_dlq_refused_total", {"source_broker": "src", "reason": "duplicate"})
        self.assertEqual(value, 1.0)

    def test_distinct_payloads_both_published(self):
        router, dlq = make_router(dedupe_window=60)
        send(router, b"one")
        send(router, b"two")
        self.assertEqual(dlq.publish.call_count, 2)

    def test_repeat_count_lands_in_next_envelope(self):
        clock = FakeClock()
        router, dlq = make_router(dedupe_window=10, clock=clock)
        for _ in range(4):
            send(router)
        clock.now = 11.0
        send(router)
        self.assertEqual(dlq.publish.call_count, 2)
        self.assertEqual(envelope(dlq)["metadata"]["repeat_count"], 3)
        self.assertNotIn("repeat_count", envelope(dlq, 0)["metadata"])


class TestOversizeMetadataRecord(unittest.TestCase):
    def test_oversize_payload_is_never_stored_but_leaves_a_metadata_record(self):
        router, dlq = make_router(max_message_bytes=100)
        ack, nack = send(router, b"S" * 5000)
        env = envelope(dlq)
        self.assertEqual(env["payload"], "")
        self.assertTrue(env["metadata"]["payload_omitted"])
        self.assertEqual(env["metadata"]["original_size"], 5000)
        self.assertEqual(env["source_topic"], "nowhere")
        self.assertNotIn("SSSS", json.dumps(env))
        nack.assert_called_once_with(requeue=False)  # not safely stored, so not acked
        ack.assert_not_called()

    def test_metadata_record_respects_the_budget(self):
        router, dlq = make_router(max_message_bytes=100, msgs_per_sec=1)
        send(router, b"small")                 # uses the one message token
        send(router, b"S" * 5000)
        self.assertEqual(dlq.publish.call_count, 1)

    def test_oversize_event_is_attributed_to_the_sender_ledger(self):
        router, dlq = make_router(max_message_bytes=100)
        from postoffice.ingress import Firewall
        router.set_firewall(Firewall.from_dict({"rules": [], "ledger": {}}))
        send(router, b"S" * 5000)
        row = router.firewall.ledger.sender_report()[0]
        self.assertEqual(row["categories"].get("oversize"), 1)


if __name__ == "__main__":
    unittest.main()
