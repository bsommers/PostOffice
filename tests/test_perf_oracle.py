"""Hermetic tests for the Phase 0 performance tooling in ``perf/`` (header, accounting oracle, results)."""
import json
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from perf import header, oracle, results  # noqa: E402


class HeaderTest(unittest.TestCase):
    def test_round_trip(self):
        msg = header.encode(7, 42, 1_000_000, b"hello")
        self.assertEqual(len(msg), header.HEADER_SIZE + 5)
        h = header.decode(msg)
        self.assertEqual((h.producer, h.seq, h.send_ts_ns), (7, 42, 1_000_000))
        self.assertTrue(h.intact)
        self.assertEqual(h.body, b"hello")

    def test_padded_to_size(self):
        msg = header.encode(1, 0, 0, size=1024)
        self.assertEqual(len(msg), 1024)
        self.assertTrue(header.decode(msg).intact)

    def test_size_smaller_than_header_rejected(self):
        with self.assertRaises(ValueError):
            header.encode(1, 0, 0, size=header.HEADER_SIZE - 1)

    def test_corruption_detected(self):
        msg = bytearray(header.encode(1, 0, 0, b"abc"))
        msg[-1] ^= 0xFF
        self.assertFalse(header.decode(bytes(msg)).intact)

    def test_foreign_message(self):
        self.assertIsNone(header.decode(b"not a perf message at all, too short?"))
        self.assertIsNone(header.decode(b"x"))


class OracleTest(unittest.TestCase):
    def _msg(self, producer, seq, ts=0, body=b"p"):
        return header.encode(producer, seq, ts, body)

    def test_clean_run_balances(self):
        o = oracle.Oracle()
        for s in range(10):
            o.sent(1, s)
            o.received(self._msg(1, s, ts=s * 1000), recv_ts_ns=s * 1000 + 500)
        r = o.report()
        self.assertEqual(r["delivered"], 10)
        self.assertEqual(r["lost"], 0)
        self.assertTrue(r["balanced"])
        self.assertTrue(r["passed"])
        self.assertEqual(r["latency_ns"]["p50"], 500)
        self.assertEqual(r["latency_ns"]["max"], 500)

    def test_loss_unbalances_books(self):
        o = oracle.Oracle()
        for s in range(5):
            o.sent(1, s)
        for s in (0, 1, 3, 4):
            o.received(self._msg(1, s), 0)
        r = o.report()
        self.assertEqual(r["lost"], 1)
        self.assertEqual(r["lost_sample"], [[1, 2]])
        self.assertFalse(r["balanced"])
        self.assertFalse(r["passed"])

    def test_accounted_loss_balances(self):
        o = oracle.Oracle()
        for s in range(5):
            o.sent(1, s)
        for s in range(3):
            o.received(self._msg(1, s), 0)
        r = o.report(dlq=1, shed=1)
        self.assertEqual(r["lost"], 2)
        self.assertTrue(r["balanced"])
        self.assertTrue(r["passed"])

    def test_duplicates_and_reorder(self):
        o = oracle.Oracle()
        for s in range(3):
            o.sent(1, s)
        for s in (0, 2, 1, 1):
            o.received(self._msg(1, s), 0)
        r = o.report()
        self.assertEqual(r["delivered"], 3)
        self.assertEqual(r["duplicated"], 1)
        self.assertEqual(r["reordered"], 1)
        self.assertTrue(r["passed"])  # at-least-once allows duplicates

    def test_corruption_fails(self):
        o = oracle.Oracle()
        o.sent(1, 0)
        bad = bytearray(self._msg(1, 0, body=b"abc"))
        bad[-1] ^= 1
        o.received(bytes(bad), 0)
        r = o.report()
        self.assertEqual(r["corrupted"], 1)
        self.assertFalse(r["passed"])

    def test_foreign_and_unknown(self):
        o = oracle.Oracle()
        o.received(b"junk", 0)
        o.received(self._msg(9, 0), 0)  # never sent
        r = o.report()
        self.assertEqual(r["foreign"], 1)
        self.assertEqual(r["unexpected"], 1)
        self.assertFalse(r["passed"])

    def test_sent_range(self):
        o = oracle.Oracle()
        o.sent_range(2, 4)
        for s in range(4):
            o.received(self._msg(2, s), 0)
        self.assertEqual(o.report()["sent"], 4)

    def test_negative_latency_clamped_and_empty_percentiles(self):
        o = oracle.Oracle()
        self.assertIsNone(o.report()["latency_ns"])
        o.sent(1, 0)
        o.received(self._msg(1, 0, ts=100), recv_ts_ns=50)
        self.assertEqual(o.report()["latency_ns"]["p99"], 0)

    def test_percentile(self):
        vals = list(range(1, 101))
        self.assertEqual(oracle.percentile(vals, 50), 50)
        self.assertEqual(oracle.percentile(vals, 99), 99)
        self.assertEqual(oracle.percentile(vals, 100), 100)
        self.assertEqual(oracle.percentile([5], 99.9), 5)


class ResultsTest(unittest.TestCase):
    def test_document_shape(self):
        doc = results.make_result("t1-micro", {"x": 1}, verdict="pass")
        self.assertEqual(doc["schema"], results.SCHEMA)
        for key in ("scenario", "created_at", "git", "environment", "metrics", "verdict"):
            self.assertIn(key, doc)
        self.assertIn("python", doc["environment"])
        self.assertIn("cpu_count", doc["environment"])
        json.dumps(doc)

    def test_bad_verdict(self):
        with self.assertRaises(ValueError):
            results.make_result("s", {}, verdict="maybe")

    def test_write(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            path = results.write_result(results.make_result("s", {}), d)
            with open(path) as f:
                self.assertEqual(json.load(f)["scenario"], "s")


if __name__ == "__main__":
    unittest.main()
