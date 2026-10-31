"""Cross-language verification between Python `perf/header.py` + `perf/oracle.py` and the Rust `postoffice-loadgen` binary."""
import json
import os
import subprocess
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from perf import header, oracle  # noqa: E402

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
LOADGEN_DIR = os.path.join(REPO_ROOT, "perf", "loadgen")
RELEASE_BIN = os.path.join(LOADGEN_DIR, "target", "release", "postoffice-loadgen")
DEBUG_BIN = os.path.join(LOADGEN_DIR, "target", "debug", "postoffice-loadgen")


def _bin() -> str:
    if os.path.isfile(RELEASE_BIN):
        return RELEASE_BIN
    if os.path.isfile(DEBUG_BIN):
        return DEBUG_BIN
    subprocess.run(["cargo", "build", "-q"], cwd=LOADGEN_DIR, check=True)
    return DEBUG_BIN


class LoadgenCrossLanguageTest(unittest.TestCase):
    def test_rust_encoded_header_decoded_by_python(self):
        proc = subprocess.run(
            [_bin(), "encode", "--producer", "11", "--seq", "99", "--ts", "123456789", "--body", "hello", "--size", "128"],
            capture_output=True,
            check=True,
        )
        raw = proc.stdout
        self.assertEqual(len(raw), 128)
        py_Encoded = header.encode(11, 99, 123456789, b"hello", size=128)
        self.assertEqual(raw, py_Encoded)

        h = header.decode(raw)
        self.assertIsNotNone(h)
        self.assertEqual((h.producer, h.seq, h.send_ts_ns), (11, 99, 123456789))
        self.assertTrue(h.intact)

    def test_python_encoded_header_decoded_by_rust(self):
        raw = header.encode(42, 7, 987654321, b"postoffice", size=256)
        proc = subprocess.run(
            [_bin(), "decode"],
            input=raw,
            capture_output=True,
            check=True,
        )
        decoded = json.loads(proc.stdout.decode("utf-8"))
        self.assertEqual(decoded["producer"], 42)
        self.assertEqual(decoded["seq"], 7)
        self.assertEqual(decoded["send_ts_ns"], 987654321)
        self.assertTrue(decoded["intact"])
        self.assertEqual(decoded["body_len"], 256 - header.HEADER_SIZE)

    def test_loopback_mqtt_open_loop_and_oracle(self):
        proc = subprocess.run(
            [_bin(), "loopback", "--rate", "10000", "--count", "100", "--size", "512", "--qos", "1"],
            capture_output=True,
            text=True,
            check=True,
        )
        out = json.loads(proc.stdout)
        self.assertEqual(out["publisher"]["sent"], 100)
        rep = out["oracle"]
        self.assertEqual(rep["sent"], 100)
        self.assertEqual(rep["delivered"], 100)
        self.assertEqual(rep["lost"], 0)
        self.assertTrue(rep["balanced"])
        self.assertTrue(rep["passed"])
        self.assertIn("p99", rep["latency_ns"])


if __name__ == "__main__":
    unittest.main()
