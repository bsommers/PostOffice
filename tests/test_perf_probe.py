"""Hermetic tests for the Phase 0 bridge-added latency probe and Python baseline runner."""
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from perf import baseline, probe  # noqa: E402
from postoffice.router import Router  # noqa: E402


class LatencyProbeTest(unittest.TestCase):
    def test_probe_records_quantiles_and_resets(self):
        p = probe.LatencyProbe()
        self.assertEqual(p.count, 0)
        self.assertIsNone(p.summary())
        for v in range(1, 101):
            p.record(v * 1000)
        s = p.summary()
        self.assertEqual(s["count"], 100)
        self.assertEqual(s["p50"], 50_000)
        self.assertEqual(s["p99"], 99_000)
        self.assertEqual(s["max"], 100_000)
        p.reset()
        self.assertEqual(p.count, 0)

    def test_router_latency_probe_captures_full_route_duration(self):
        router = Router()
        src = MagicMock()
        src.name = "mqtt"
        dst = MagicMock()
        dst.name = "kafka"
        router.register_client(src)
        router.register_client(dst)
        router.add_route("mqtt", "sensors/+", "kafka", "telemetry")

        p = probe.LatencyProbe()
        router.set_latency_probe(p.record)
        for _ in range(10):
            router.route("mqtt", "sensors/temp", b'{"t":22.5}')

        self.assertEqual(p.count, 10)
        self.assertGreater(p.summary()["p50"], 0)

        router.set_latency_probe(None)
        router.route("mqtt", "sensors/temp", b'{"t":22.5}')
        self.assertEqual(p.count, 10)

    def test_router_env_var_enables_default_probe(self):
        with patch.dict(os.environ, {"POSTOFFICE_LATENCY_PROBE": "1"}):
            router = Router()
            self.assertIsNotNone(router.latency_probe)

    def test_baseline_smoke_run(self):
        res = baseline.run_matrix(iterations=100, repeats=1)
        self.assertIn("reference_1kb_passthrough", res)
        ref = res["reference_1kb_passthrough"]
        self.assertGreater(ref["msg_per_s"], 0)
        self.assertIn("p99_us", ref)
        self.assertTrue(ref["oracle_passed"])


if __name__ == "__main__":
    unittest.main()
