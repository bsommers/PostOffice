import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
DOCKERFILE_PATH = REPO_ROOT / "Dockerfile.native"
REPORT_PATH = REPO_ROOT / "docs" / "benchmarks" / "edge_profile_report.json"
VERIFY_SCRIPT = REPO_ROOT / "scripts" / "verify_edge_profile.py"

# Add scripts directory to sys.path to enable direct module import testing
sys.path.insert(0, str(REPO_ROOT / "scripts"))
import verify_edge_profile  # noqa: E402


class TestEdgePackaging(unittest.TestCase):
    """Hermetic unit tests validating Edge Profile packaging, Dockerfile invariants, and RSS bounds."""

    def test_dockerfile_native_invariants(self) -> None:
        self.assertTrue(DOCKERFILE_PATH.is_file(), f"Missing {DOCKERFILE_PATH}")
        content = DOCKERFILE_PATH.read_text(encoding="utf-8")

        # Invariant 1: Multi-stage build
        self.assertIn("AS builder", content)
        self.assertIn("AS runtime", content)

        # Invariant 2: Edge cargo feature flag
        self.assertIn("--no-default-features", content)
        self.assertIn("--features edge", content)

        # Invariant 3: Unprivileged postoffice user (UID 10001)
        self.assertIn("10001", content)
        self.assertIn("USER postoffice", content)

        # Invariant 4: Persistent store-and-forward state volume
        self.assertIn("/var/lib/postoffice", content)
        self.assertIn('VOLUME ["/var/lib/postoffice"]', content)

        # Invariant 5: Container healthcheck probing /healthz
        self.assertIn("HEALTHCHECK", content)
        self.assertIn("/healthz", content)

        # Invariant 6: Binary entrypoint & metrics port
        self.assertIn('ENTRYPOINT ["/usr/local/bin/postoffice-worker"]', content)
        self.assertIn("EXPOSE 9100", content)

        # Invariant 7: Artifact copies & strip
        self.assertIn("/usr/local/bin/postoffice-worker", content)
        self.assertIn("/usr/local/lib/libpostoffice.so", content)
        self.assertIn("strip", content)

    def test_edge_profile_verification_via_module_call(self) -> None:
        with tempfile.TemporaryDirectory(prefix="po-edge-test-") as tmpdir:
            tmp_report = pathlib.Path(tmpdir) / "test_report.json"
            report = verify_edge_profile.run_verification(
                output_path=tmp_report,
                skip_build=True,
                count=1500,
            )

            self.assertTrue(tmp_report.is_file())
            self.assertTrue(report["all_gates_passed"])

            # Verify memory budgets (§0.1 / §7 Decision 2)
            self.assertLess(report["idle_rss_mb"], 64.0, "Idle/Sync RSS must be < 64 MB")
            self.assertLess(report["loaded_rss_mb"], 128.0, "Loaded RSS must be < 128 MB")

            # Verify throughput (§7 Decision 2)
            self.assertGreaterEqual(
                report["throughput_msg_per_sec"],
                1000.0,
                "Loaded throughput must be >= 1,000 msg/s",
            )

            # Verify binary footprint (< 4 MB each for edge)
            sizes = report["binary_sizes"]
            self.assertLess(sizes["worker_binary_stripped_bytes"], 4 * 1024 * 1024)
            self.assertLess(sizes["libpostoffice_so_stripped_bytes"], 4 * 1024 * 1024)

            # Verify gates dictionary
            gates = report["gates"]
            self.assertTrue(gates["idle_rss_gate"])
            self.assertTrue(gates["loaded_rss_gate"])
            self.assertTrue(gates["throughput_gate"])
            self.assertTrue(gates["all_gates_passed"])

    def test_edge_profile_verification_via_cli_subprocess(self) -> None:
        with tempfile.TemporaryDirectory(prefix="po-edge-cli-") as tmpdir:
            tmp_report = pathlib.Path(tmpdir) / "cli_report.json"
            env = os.environ.copy()
            env["PYTHONPATH"] = str(REPO_ROOT / "src")

            cmd = [
                sys.executable,
                str(VERIFY_SCRIPT),
                "--output",
                str(tmp_report),
                "--skip-build",
                "--count",
                "1500",
            ]
            res = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
            self.assertEqual(res.returncode, 0, f"CLI verification failed:\n{res.stderr}")
            self.assertTrue(tmp_report.is_file())

            data = json.loads(tmp_report.read_text(encoding="utf-8"))
            self.assertTrue(data["all_gates_passed"])
            self.assertLess(data["idle_rss_mb"], 64.0)
            self.assertLess(data["loaded_rss_mb"], 128.0)
            self.assertGreaterEqual(data["throughput_msg_per_sec"], 1000.0)

    def test_persisted_benchmark_report_satisfies_edge_slos(self) -> None:
        self.assertTrue(REPORT_PATH.is_file(), f"Missing persisted report at {REPORT_PATH}")
        data = json.loads(REPORT_PATH.read_text(encoding="utf-8"))

        self.assertEqual(data["profile"], "edge")
        self.assertTrue(data["all_gates_passed"])
        self.assertLess(data["idle_rss_mb"], 64.0)
        self.assertLess(data["loaded_rss_mb"], 128.0)
        self.assertGreaterEqual(data["throughput_msg_per_sec"], 1000.0)
        self.assertTrue(data["gates"]["idle_rss_gate"])
        self.assertTrue(data["gates"]["loaded_rss_gate"])
        self.assertTrue(data["gates"]["throughput_gate"])


if __name__ == "__main__":
    unittest.main()
