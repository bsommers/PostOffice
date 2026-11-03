import os
import pathlib
import subprocess
import tempfile
import unittest
from unittest.mock import patch

from perf import micro
from postoffice.governor import MemoryGovernor, detect_host_ram_bytes
from postoffice.native import (
    NativeClient,
    NativeCodec,
    NativeFirewall,
    NativeTrie,
    native_decode_confluent,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
CRATE_DIR = REPO_ROOT / "native" / "libpostoffice"


class TestPhase2FuzzAndEdge(unittest.TestCase):
    def test_rust_fuzz_targets_zero_panics(self) -> None:
        fuzz_file = CRATE_DIR / "tests" / "fuzz_targets.rs"
        self.assertTrue(fuzz_file.is_file(), f"Missing {fuzz_file}")
        proc = subprocess.run(
            [
                "cargo",
                "test",
                "--release",
                "--manifest-path",
                str(CRATE_DIR / "Cargo.toml"),
                "--test",
                "fuzz_targets",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"Rust fuzz_targets failed:\n{proc.stdout}\n{proc.stderr}",
        )

    def test_edge_build_profile_compiles_and_fits_size_budget(self) -> None:
        proc = subprocess.run(
            [
                "cargo",
                "build",
                "--release",
                "--no-default-features",
                "--features",
                "edge",
                "--manifest-path",
                str(CRATE_DIR / "Cargo.toml"),
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(
            proc.returncode,
            0,
            f"Edge build failed:\n{proc.stderr}",
        )
        so_path = CRATE_DIR / "target" / "release" / "libpostoffice.so"
        a_path = CRATE_DIR / "target" / "release" / "libpostoffice.a"
        self.assertTrue(so_path.is_file())
        self.assertTrue(a_path.is_file())
        # Edge library must be well below the 64 MB ceiling (< 4 MB unstripped)
        self.assertLess(so_path.stat().st_size, 4 * 1024 * 1024)

    def test_micro_benchmark_compare_native_shows_speedup(self) -> None:
        report = micro.main(["--quick", "--compare-native"])
        self.assertIn("python", report)
        self.assertIn("native_c_abi", report)
        py_fw = report["python"]["firewall.evaluate"]["rules=5+content_guard"]["ns_per_op"]
        rs_fw = report["native_c_abi"]["firewall.evaluate"]["rules=5+content_guard"]["ns_per_op"]
        self.assertLess(rs_fw, py_fw, "Native C ABI firewall should be faster than Python")

        py_trie = report["python"]["topic_trie.match"]["filters=1000"]["ns_per_op"]
        rs_trie = report["native_c_abi"]["topic_trie.match"]["filters=1000"]["ns_per_op"]
        self.assertLess(rs_trie, py_trie, "Native C ABI TopicTrie should be faster than Python")

    def test_governor_and_native_validation_error_paths(self) -> None:
        with self.assertRaises(ValueError):
            MemoryGovernor(budget_bytes=0)
        with self.assertRaises(ValueError):
            MemoryGovernor(memory_fraction=0.0)
        with self.assertRaises(ValueError):
            MemoryGovernor(shed_ratio=1.5)

        gov = MemoryGovernor(budget_bytes=1024, use_native=False)
        with self.assertRaises(ValueError):
            gov.try_acquire(-1)
        with self.assertRaises(ValueError):
            gov.release(-1)

        with tempfile.TemporaryDirectory() as tmpdir:
            bad_cg = pathlib.Path(tmpdir) / "memory.max"
            bad_cg.write_text("max\n", encoding="utf-8")
            ram = detect_host_ram_bytes(
                cgroup_paths=[str(bad_cg), str(pathlib.Path(tmpdir) / "missing")],
                meminfo_path=str(pathlib.Path(tmpdir) / "missing_meminfo"),
            )
            self.assertEqual(ram, 1024 * 1024 * 1024)

        with self.assertRaises(ValueError):
            NativeFirewall({"rules": [{"type": "unknown_rule"}]})

        nt = NativeTrie()
        with self.assertRaises(ValueError):
            nt.remove_route("bad/#/pattern", "b", "t")

        nc = NativeCodec()
        with self.assertRaises(ValueError):
            nc.register_schema(1, "", "AVRO", {})
        with self.assertRaises(ValueError):
            nc.serialize({"a": 1}, "unknown_fmt", "s")
        with self.assertRaises(ValueError):
            nc.deserialize(b"bad", "avro", "missing")

        with self.assertRaises(ValueError):
            NativeClient("", "")

        with self.assertRaises(ValueError):
            native_decode_confluent(b"\x00\x01")


if __name__ == "__main__":
    unittest.main()
