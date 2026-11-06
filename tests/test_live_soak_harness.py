"""
Hermetic Unit Test for Live Broker Soak Harness (scripts/run_live_soak.py).
Runs in-process without requiring Docker or external network services.
"""

from __future__ import annotations

import json
import pathlib
import sys
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from run_live_soak import MockResp2Server, calc_percentiles, run_soak


class TestLiveSoakHarness(unittest.TestCase):
    def test_percentile_calculation(self) -> None:
        empty = calc_percentiles([])
        self.assertEqual(empty, {"p50": 0.0, "p95": 0.0, "p99": 0.0, "p99_9": 0.0})

        latencies = [float(i) for i in range(1, 1001)]  # 1.0 to 1000.0
        pcts = calc_percentiles(latencies)
        self.assertAlmostEqual(pcts["p50"], 500.5, delta=1.0)
        self.assertAlmostEqual(pcts["p95"], 950.05, delta=1.0)
        self.assertAlmostEqual(pcts["p99"], 990.01, delta=1.0)
        self.assertAlmostEqual(pcts["p99_9"], 999.0, delta=1.0)

    def test_mock_resp2_server_operations(self) -> None:
        server = MockResp2Server()
        try:
            import redis

            r = redis.Redis(host="127.0.0.1", port=server.port, decode_responses=True)
            self.assertTrue(r.ping())

            # INCR / GET
            self.assertEqual(r.incr("counter"), 1)
            self.assertEqual(r.incr("counter"), 2)
            self.assertEqual(r.get("counter"), "2")

            # HSET / HGET / HGETALL
            r.hset("hash1", "f1", "v1")
            r.hset("hash1", "f2", "v2")
            self.assertEqual(r.hget("hash1", "f1"), "v1")
            self.assertEqual(r.hgetall("hash1"), {"f1": "v1", "f2": "v2"})

            # XADD / XRANGE
            eid = r.xadd("test_stream", {"version": "1", "payload": "hello"})
            self.assertIsNotNone(eid)
            entries = r.xrange("test_stream", "-", "+")
            self.assertEqual(len(entries), 1)
            self.assertEqual(entries[0][1]["payload"], "hello")

            # FLUSHALL
            r.flushall()
            self.assertIsNone(r.get("counter"))
            self.assertEqual(r.hgetall("hash1"), {})
        finally:
            server.close()

    def test_hermetic_soak_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp_dir:
            out_json = pathlib.Path(tmp_dir) / "soak_out.json"
            results = run_soak(
                hermetic=True,
                message_count=500,
                output_path=str(out_json),
                run_integration_tests=False,
            )

            # Verification of structured results
            self.assertEqual(results["mode"], "hermetic")
            self.assertEqual(results["shadow_soak"]["divergences"], 0)
            self.assertTrue(results["shadow_soak"]["clean"])
            self.assertEqual(results["shadow_soak"]["total_messages"], 500)

            # Canary rollout verification
            canary = results["canary_rollout"]
            self.assertTrue(canary["simulated_breach_rollback"])
            self.assertEqual(canary["final_native_percentage"], 0)
            self.assertEqual(canary["status"], "rolled_back")
            self.assertEqual(canary["promoted_stages"], [5, 25, 100])

            # Performance verification
            perf = results["performance"]
            self.assertEqual(perf["python"]["message_count"], 500)
            self.assertEqual(perf["native"]["message_count"], 500)
            self.assertGreater(perf["python"]["throughput_msg_per_sec"], 0)
            self.assertGreater(perf["native"]["throughput_msg_per_sec"], 0)
            self.assertGreater(perf["native"]["latency_us"]["p50"], 0)

            # File output verification
            self.assertTrue(out_json.is_file())
            loaded = json.loads(out_json.read_text(encoding="utf-8"))
            self.assertEqual(loaded["shadow_soak"]["divergences"], 0)
            self.assertEqual(loaded["canary_rollout"]["status"], "rolled_back")


if __name__ == "__main__":
    unittest.main()
