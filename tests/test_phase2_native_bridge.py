import os
import pathlib
import tempfile
import unittest
from unittest.mock import patch

from postoffice.conformance import run_conformance
from postoffice.dlq import dlq_fingerprint
from postoffice.governor import MemoryGovernor, detect_host_ram_bytes
from postoffice.ingress.firewall import Firewall
from postoffice.ingress.rules import Action, MessageContext
from postoffice.native import (
    NativeClient,
    NativeCodec,
    NativeFirewall,
    NativeTrie,
    is_native_enabled,
)
from postoffice.schema.converters import (
    SchemaValidationError,
    decode_wire_format,
    encode_wire_format,
)
from postoffice.trie import TopicTrie

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
VECTORS_DIR = REPO_ROOT / "conformance" / "vectors"


class TestPhase2NativeBridge(unittest.TestCase):
    def test_feature_flag_default_is_python(self) -> None:
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("POSTOFFICE_NATIVE", None)
            self.assertFalse(is_native_enabled())
        self.assertFalse(is_native_enabled({"POSTOFFICE_NATIVE": "0"}))
        self.assertFalse(is_native_enabled({"POSTOFFICE_NATIVE": "false"}))
        self.assertTrue(is_native_enabled({"POSTOFFICE_NATIVE": "1"}))
        self.assertTrue(is_native_enabled({"POSTOFFICE_NATIVE": "true"}))
        self.assertTrue(is_native_enabled({"POSTOFFICE_NATIVE": "YES"}))

    def test_memory_governor_cgroup_and_default_budget(self) -> None:
        host_ram = detect_host_ram_bytes()
        self.assertGreater(host_ram, 0)

        with tempfile.TemporaryDirectory() as tmpdir:
            cg_file = pathlib.Path(tmpdir) / "memory.max"
            cg_file.write_text("268435456\n", encoding="utf-8")  # 256 MiB
            detected = detect_host_ram_bytes(cgroup_paths=[str(cg_file)])
            self.assertEqual(detected, 268435456)

            gov = MemoryGovernor(
                memory_fraction=0.5,
                shed_ratio=0.80,
                cgroup_paths=[str(cg_file)],
                use_native=False,
            )
            self.assertEqual(gov.budget_bytes, 134217728)  # 128 MiB (50% of 256 MiB)
            self.assertEqual(gov.shed_limit_bytes, int(134217728 * 0.80))

    def test_memory_governor_parity_python_and_native_under_10x_overload(self) -> None:
        for use_native in (False, True):
            with self.subTest(use_native=use_native):
                gov = MemoryGovernor(
                    budget_bytes=10_000,
                    shed_ratio=0.80,
                    use_native=use_native,
                )
                self.assertEqual(gov.budget_bytes, 10_000)
                self.assertEqual(gov.shed_limit_bytes, 8_000)
                admitted = 0
                for _ in range(100):  # 10x overload of 1,000-byte messages
                    if gov.try_acquire(1_000):
                        admitted += 1
                self.assertEqual(admitted, 8)
                self.assertEqual(gov.in_use_bytes, 8_000)
                self.assertEqual(gov.peak_bytes, 8_000)
                self.assertEqual(gov.shed_total, 92)

                for _ in range(admitted):
                    gov.release(1_000)
                self.assertEqual(gov.in_use_bytes, 0)
                self.assertTrue(gov.try_acquire(1_000))
                gov.release(1_000)

    def test_topic_trie_native_mode_parity(self) -> None:
        with patch.dict(os.environ, {"POSTOFFICE_NATIVE": "1"}):
            trie = TopicTrie()
            self.assertTrue(trie._native is not None)
            trie.insert("sensors/+/temp", {"id": 1})
            trie.insert("sensors/#", {"id": 2})
            trie.insert("#", {"id": 3})
            trie.insert("$SYS/#", {"id": 4})

            self.assertEqual(len(trie), 4)
            matched = trie.match("sensors/room1/temp")
            self.assertEqual(sorted(m["id"] for m in matched), [1, 2, 3])
            sys_matched = trie.match("$SYS/broker/load")
            self.assertEqual([m["id"] for m in sys_matched], [4])

            with self.assertRaises(ValueError):
                trie.insert("sensors/#/bad", {"id": 99})

            trie.clear()
            self.assertEqual(len(trie), 0)
            self.assertEqual(trie.match("sensors/room1/temp"), [])

    def test_native_trie_idempotent_add_and_remove(self) -> None:
        nt = NativeTrie()
        route_doc = '{"target_broker":"kafka_1","target_topic":"out"}'
        self.assertTrue(nt.add_route("sensors/+", route_doc))
        self.assertFalse(nt.add_route("sensors/+", route_doc))
        self.assertEqual(nt.match("sensors/a"), [{"target_broker": "kafka_1", "target_topic": "out"}])
        removed = nt.remove_route("sensors/+", "kafka_1", "out")
        self.assertEqual(removed, 1)
        self.assertEqual(nt.match("sensors/a"), [])

    def test_firewall_native_mode_parity(self) -> None:
        cfg = {
            "rules": [
                {"type": "max_payload_bytes", "max_bytes": 32, "action": "reject"},
                {"type": "topic_shape", "deny": ["$SYS/#"], "action": "reject"},
                {
                    "type": "content_guard",
                    "topic": "devices/+/telemetry",
                    "format": "json",
                    "required_fields": ["temp"],
                    "action": "reject",
                },
            ],
            "ledger": {
                "window": 60.0,
                "identity_template": "devices/{device}/#",
                "enforce": True,
                "strike_threshold": 2,
                "cooldown_base": 30.0,
                "cooldown_factor": 2.0,
                "cooldown_cap": 300.0,
            },
        }
        with patch.dict(os.environ, {"POSTOFFICE_NATIVE": "1"}):
            fw = Firewall.from_dict(cfg)
            self.assertTrue(fw._native is not None)
            v_ok = fw.evaluate(
                MessageContext("mqtt", "devices/d1/telemetry", 14, b'{"temp": 21.5}')
            )
            self.assertEqual(v_ok.action, Action.ALLOW)

            v_big = fw.evaluate(
                MessageContext("mqtt", "devices/d1/telemetry", 40, b"x" * 40)
            )
            self.assertEqual(v_big.action, Action.REJECT)
            self.assertEqual(v_big.category, "oversize")

            v_bad = fw.evaluate(
                MessageContext("mqtt", "devices/d1/telemetry", 2, b"{}")
            )
            self.assertEqual(v_bad.action, Action.REJECT)
            self.assertEqual(v_bad.category, "schema_mismatch")

            # After 2 strikes, d1 is cooled
            v_cooled = fw.evaluate(
                MessageContext("mqtt", "devices/d1/telemetry", 14, b'{"temp": 21.5}')
            )
            self.assertEqual(v_cooled.action, Action.DROP)
            self.assertEqual(v_cooled.category, "cooled")

            report = fw.sender_report(top=5)
            self.assertEqual(len(report), 1)
            self.assertEqual(report[0]["sender"], "d1")

    def test_dlq_fingerprint_and_wire_format_native_mode(self) -> None:
        with patch.dict(os.environ, {"POSTOFFICE_NATIVE": "0"}):
            fp_py = dlq_fingerprint("b1", "t1", "err", b"payload")
            framed_py = encode_wire_format(b"abc", 42)
            sid_py, dec_py = decode_wire_format(framed_py)

        with patch.dict(os.environ, {"POSTOFFICE_NATIVE": "1"}):
            fp_rs = dlq_fingerprint("b1", "t1", "err", b"payload")
            framed_rs = encode_wire_format(b"abc", 42)
            sid_rs, dec_rs = decode_wire_format(framed_rs)
            with self.assertRaises(SchemaValidationError):
                decode_wire_format(b"\x01\x00\x00\x00\x01abc")

        self.assertEqual(fp_py, fp_rs)
        self.assertEqual(framed_py, framed_rs)
        self.assertEqual((sid_py, dec_py), (sid_rs, dec_rs))

    def test_native_codec_and_client_wrappers(self) -> None:
        codec = NativeCodec()
        codec.register_schema(
            1,
            "s_avro",
            "AVRO",
            {"type": "record", "name": "R", "fields": [{"name": "k", "type": "string"}]},
        )
        raw = codec.serialize({"k": "v1"}, "avro", "s_avro", wire_format=True)
        self.assertEqual( codec.deserialize(raw, "avro", wire_format=True), {"k": "v1"} )

        client = NativeClient("http://127.0.0.1:8080", "tok")
        resp = client.publish("mqtt_1", "sensors/temp", b'{"t":1}', sender_id="dev1")
        self.assertTrue(resp["accepted"])

    def test_all_71_conformance_vectors_pass_in_both_modes(self) -> None:
        with patch.dict(os.environ, {"POSTOFFICE_NATIVE": "0"}):
            passed_py, failed_py, errors_py = run_conformance(VECTORS_DIR)
            self.assertEqual(failed_py, 0, f"Python mode failed: {errors_py}")
            self.assertEqual(passed_py, 71)

        with patch.dict(os.environ, {"POSTOFFICE_NATIVE": "1"}):
            passed_rs, failed_rs, errors_rs = run_conformance(VECTORS_DIR)
            self.assertEqual(failed_rs, 0, f"Native mode failed: {errors_rs}")
            self.assertEqual(passed_rs, 71)


if __name__ == "__main__":
    unittest.main()
