"""Hermetic tests for the Phase 0 Rust-vs-Go bake-off (P0-4)."""
import os
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from perf import bakeoff  # noqa: E402


class BakeoffTest(unittest.TestCase):
    def test_rust_and_go_slices_pass_and_balance_books(self):
        res = bakeoff.run_bakeoff(messages=2_000)
        for lang in ("rust", "go"):
            self.assertIn(lang, res)
            info = res[lang]
            self.assertGreater(info["binary_bytes"], 0)
            for cell in ("threads_1_passthrough", "threads_1_full", "threads_4_full", "threads_8_full"):
                c = info["cells"][cell]
                self.assertGreater(c["msg_per_s"], 10_000)
                self.assertLess(c["p99_ns"], 5_000_000)  # < 5 ms SLO
                self.assertTrue(c["oracle_passed"])
            ov = info["cells"]["overload_10x"]
            self.assertTrue(ov["balanced"])
            self.assertTrue(ov["passed"])
            self.assertGreater(ov["shed"], 0)
            self.assertLessEqual(ov["peak_governor_bytes"], ov["shed_limit_bytes"])


if __name__ == "__main__":
    unittest.main()
