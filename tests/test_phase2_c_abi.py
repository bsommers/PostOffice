import ctypes
import json
import pathlib
import subprocess
import tempfile
import unittest

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
HEADER_PATH = REPO_ROOT / "include" / "postoffice.h"
CRATE_DIR = REPO_ROOT / "native" / "libpostoffice"
VECTORS_DIR = REPO_ROOT / "conformance" / "vectors"

PO_OK = 0
PO_ERR_INVALID_ARGUMENT = -1
PO_ERR_NOT_FOUND = -2
PO_ERR_OVERLOAD_SHED = -4
PO_ERR_FIREWALL_DROP = -5
PO_ERR_FIREWALL_REJECT = -6
PO_ERR_WIRE_FORMAT = -7

PO_VERDICT_ALLOW = 1
PO_VERDICT_DROP = 2
PO_VERDICT_REJECT = 3


class PoSlice(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
    ]


class PoOwnedBuf(ctypes.Structure):
    _fields_ = [
        ("data", ctypes.POINTER(ctypes.c_uint8)),
        ("len", ctypes.c_size_t),
        ("cap", ctypes.c_size_t),
    ]


class PoFirewallVerdict(ctypes.Structure):
    _fields_ = [
        ("kind", ctypes.c_int),
        ("rule", ctypes.c_char * 32),
        ("reason", ctypes.c_char * 128),
    ]


def _to_slice(raw: bytes) -> tuple[PoSlice, ctypes.Array]:
    buf = (ctypes.c_uint8 * len(raw)).from_buffer_copy(raw)
    ptr = ctypes.cast(buf, ctypes.POINTER(ctypes.c_uint8)) if raw else None
    return PoSlice(ptr, len(raw)), buf


class TestPhase2CAbi(unittest.TestCase):
    lib: ctypes.CDLL
    so_path: pathlib.Path

    @classmethod
    def setUpClass(cls) -> None:
        if not (CRATE_DIR / "Cargo.toml").is_file():
            raise AssertionError(f"Missing Rust crate at {CRATE_DIR / 'Cargo.toml'}")
        proc = subprocess.run(
            ["cargo", "build", "--release", "--manifest-path", str(CRATE_DIR / "Cargo.toml")],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise AssertionError(f"cargo build --release failed:\n{proc.stderr}")

        cls.so_path = CRATE_DIR / "target" / "release" / "libpostoffice.so"
        if not cls.so_path.is_file():
            raise AssertionError(f"Expected shared library at {cls.so_path}")

        cls.lib = ctypes.CDLL(str(cls.so_path))
        cls._bind_signatures(cls.lib)

    @staticmethod
    def _bind_signatures(lib: ctypes.CDLL) -> None:
        lib.po_abi_version.argtypes = []
        lib.po_abi_version.restype = ctypes.c_uint32

        lib.po_buf_free.argtypes = [ctypes.POINTER(PoOwnedBuf)]
        lib.po_buf_free.restype = None

        lib.po_trie_new.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        lib.po_trie_new.restype = ctypes.c_int

        lib.po_trie_free.argtypes = [ctypes.c_void_p]
        lib.po_trie_free.restype = None

        lib.po_trie_add_route.argtypes = [
            ctypes.c_void_p,
            PoSlice,
            PoSlice,
            ctypes.POINTER(ctypes.c_bool),
        ]
        lib.po_trie_add_route.restype = ctypes.c_int

        lib.po_trie_remove_route.argtypes = [
            ctypes.c_void_p,
            PoSlice,
            PoSlice,
            PoSlice,
            ctypes.POINTER(ctypes.c_size_t),
        ]
        lib.po_trie_remove_route.restype = ctypes.c_int

        lib.po_trie_match.argtypes = [
            ctypes.c_void_p,
            PoSlice,
            ctypes.POINTER(PoOwnedBuf),
        ]
        lib.po_trie_match.restype = ctypes.c_int

        lib.po_firewall_new.argtypes = [
            PoSlice,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.po_firewall_new.restype = ctypes.c_int

        lib.po_firewall_free.argtypes = [ctypes.c_void_p]
        lib.po_firewall_free.restype = None

        lib.po_firewall_check.argtypes = [
            ctypes.c_void_p,
            PoSlice,
            PoSlice,
            PoSlice,
            ctypes.c_double,
            ctypes.POINTER(PoFirewallVerdict),
        ]
        lib.po_firewall_check.restype = ctypes.c_int

        lib.po_firewall_evaluate.argtypes = [
            ctypes.c_void_p,
            PoSlice,
            PoSlice,
            PoSlice,
            PoSlice,
            ctypes.c_double,
            ctypes.POINTER(PoOwnedBuf),
        ]
        lib.po_firewall_evaluate.restype = ctypes.c_int

        lib.po_firewall_sender_report.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            ctypes.c_double,
            ctypes.POINTER(PoOwnedBuf),
        ]
        lib.po_firewall_sender_report.restype = ctypes.c_int

        lib.po_governor_new.argtypes = [
            ctypes.c_uint64,
            ctypes.c_double,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.po_governor_new.restype = ctypes.c_int

        lib.po_governor_free.argtypes = [ctypes.c_void_p]
        lib.po_governor_free.restype = None

        lib.po_governor_try_acquire.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
        lib.po_governor_try_acquire.restype = ctypes.c_int

        lib.po_governor_release.argtypes = [ctypes.c_void_p, ctypes.c_uint64]
        lib.po_governor_release.restype = ctypes.c_int

        lib.po_governor_in_use_bytes.argtypes = [ctypes.c_void_p]
        lib.po_governor_in_use_bytes.restype = ctypes.c_uint64

        lib.po_governor_shed_total.argtypes = [ctypes.c_void_p]
        lib.po_governor_shed_total.restype = ctypes.c_uint64

        lib.po_governor_detect_host_ram_bytes.argtypes = []
        lib.po_governor_detect_host_ram_bytes.restype = ctypes.c_uint64

        lib.po_codec_encode_confluent.argtypes = [
            ctypes.c_uint32,
            PoSlice,
            ctypes.POINTER(PoOwnedBuf),
        ]
        lib.po_codec_encode_confluent.restype = ctypes.c_int

        lib.po_codec_decode_confluent.argtypes = [
            PoSlice,
            ctypes.POINTER(ctypes.c_uint32),
            ctypes.POINTER(PoOwnedBuf),
        ]
        lib.po_codec_decode_confluent.restype = ctypes.c_int

        lib.po_dlq_fingerprint.argtypes = [
            PoSlice,
            PoSlice,
            PoSlice,
            PoSlice,
            ctypes.c_char_p,
        ]
        lib.po_dlq_fingerprint.restype = ctypes.c_int

        lib.po_codec_new.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        lib.po_codec_new.restype = ctypes.c_int

        lib.po_codec_free.argtypes = [ctypes.c_void_p]
        lib.po_codec_free.restype = None

        lib.po_codec_register_schema.argtypes = [
            ctypes.c_void_p,
            ctypes.c_uint32,
            PoSlice,
            PoSlice,
            PoSlice,
        ]
        lib.po_codec_register_schema.restype = ctypes.c_int

        lib.po_codec_serialize.argtypes = [
            ctypes.c_void_p,
            PoSlice,
            PoSlice,
            PoSlice,
            ctypes.c_bool,
            ctypes.POINTER(PoOwnedBuf),
        ]
        lib.po_codec_serialize.restype = ctypes.c_int

        lib.po_codec_deserialize.argtypes = [
            ctypes.c_void_p,
            PoSlice,
            PoSlice,
            PoSlice,
            ctypes.c_bool,
            ctypes.POINTER(PoOwnedBuf),
        ]
        lib.po_codec_deserialize.restype = ctypes.c_int

        lib.po_client_connect.argtypes = [
            PoSlice,
            PoSlice,
            ctypes.POINTER(ctypes.c_void_p),
        ]
        lib.po_client_connect.restype = ctypes.c_int

        lib.po_client_free.argtypes = [ctypes.c_void_p]
        lib.po_client_free.restype = None

        lib.po_client_publish.argtypes = [
            ctypes.c_void_p,
            PoSlice,
            PoSlice,
            PoSlice,
            PoSlice,
            ctypes.POINTER(PoOwnedBuf),
        ]
        lib.po_client_publish.restype = ctypes.c_int

    def _read_owned_buf(self, buf: PoOwnedBuf) -> bytes:
        try:
            if not buf.data or buf.len == 0:
                return b""
            return ctypes.string_at(buf.data, buf.len)
        finally:
            self.lib.po_buf_free(ctypes.byref(buf))

    def test_abi_version_is_one(self) -> None:
        self.assertEqual(self.lib.po_abi_version(), 1)

    def test_topic_trie_conformance_vectors_and_idempotent_routes(self) -> None:
        vec = json.loads((VECTORS_DIR / "topic_matching.json").read_text(encoding="utf-8"))
        trie = ctypes.c_void_p()
        self.assertEqual(self.lib.po_trie_new(ctypes.byref(trie)), PO_OK)
        try:
            for item in vec["registered_patterns"]:
                pat_s, _b1 = _to_slice(item["pattern"].encode("utf-8"))
                route_json = json.dumps(item["route_id"]).encode("utf-8")
                route_s, _b2 = _to_slice(route_json)
                added = ctypes.c_bool(False)
                rc = self.lib.po_trie_add_route(trie, pat_s, route_s, ctypes.byref(added))
                self.assertEqual(rc, PO_OK)
                self.assertTrue(added.value)

                # Idempotent duplicate check
                added_dup = ctypes.c_bool(True)
                rc_dup = self.lib.po_trie_add_route(trie, pat_s, route_s, ctypes.byref(added_dup))
                self.assertEqual(rc_dup, PO_OK)
                self.assertFalse(added_dup.value)

            for case in vec["trie_cases"]:
                top_s, _b = _to_slice(case["topic"].encode("utf-8"))
                out_buf = PoOwnedBuf()
                rc = self.lib.po_trie_match(trie, top_s, ctypes.byref(out_buf))
                self.assertEqual(rc, PO_OK)
                matched = sorted(json.loads(self._read_owned_buf(out_buf).decode("utf-8")))
                self.assertEqual(matched, case["matched_routes"], f"Topic {case['topic']} mismatch")

            for bad in vec["invalid_patterns"]:
                pat_s, _b1 = _to_slice(bad["pattern"].encode("utf-8"))
                route_s, _b2 = _to_slice(b'"x"')
                added = ctypes.c_bool(False)
                rc = self.lib.po_trie_add_route(trie, pat_s, route_s, ctypes.byref(added))
                self.assertEqual(rc, PO_ERR_INVALID_ARGUMENT)
        finally:
            self.lib.po_trie_free(trie)

    def test_firewall_conformance_vectors_and_sender_report(self) -> None:
        vec = json.loads((VECTORS_DIR / "firewall_verdicts.json").read_text(encoding="utf-8"))
        fw_config = {
            "rules": [
                {"type": "max_payload_bytes", "max_bytes": 64, "action": "reject"},
                {
                    "type": "topic_shape",
                    "max_length": 64,
                    "max_depth": 5,
                    "deny": ["$SYS/#", "+/admin"],
                    "action": "reject",
                },
                {
                    "type": "rate_limit",
                    "rate": 2.0,
                    "burst": 2.0,
                    "key": "identity",
                    "template": "devices/{device}/#",
                    "action": "drop",
                },
                {
                    "type": "content_guard",
                    "topic": "devices/+/telemetry",
                    "format": "json",
                    "max_bytes": 64,
                    "max_depth": 4,
                    "max_keys": 16,
                    "max_array_length": 8,
                    "required_fields": ["temp", "unit"],
                    "field_ranges": {"temp": {"min": -40.0, "max": 125.0}},
                    "enums": {"unit": ["C", "F"]},
                    "action": "reject",
                },
            ],
            "ledger": {
                "window": 60.0,
                "identity_template": "devices/{device}/#",
                "enforce": True,
                "strike_threshold": 3,
                "cooldown_base": 30.0,
                "cooldown_factor": 2.0,
                "cooldown_cap": 300.0,
            },
        }
        cfg_s, _b = _to_slice(json.dumps(fw_config).encode("utf-8"))
        fw = ctypes.c_void_p()
        self.assertEqual(self.lib.po_firewall_new(cfg_s, ctypes.byref(fw)), PO_OK)
        try:
            now = 1000.0
            for step in vec["steps"]:
                now += step["advance_sec"]
                broker_s, _b1 = _to_slice(step["broker"].encode("utf-8"))
                sender_s, _b2 = _to_slice(b"")
                topic_s, _b3 = _to_slice(step["topic"].encode("utf-8"))
                payload_s, _b4 = _to_slice(step["payload_utf8"].encode("utf-8"))
                out_buf = PoOwnedBuf()
                rc = self.lib.po_firewall_evaluate(
                    fw, broker_s, sender_s, topic_s, payload_s, now, ctypes.byref(out_buf)
                )
                self.assertEqual(rc, PO_OK)
                verdict = json.loads(self._read_owned_buf(out_buf).decode("utf-8"))
                expected = step["expected_verdict"]
                self.assertEqual(verdict["action"], expected["action"], f"Step {step['step']}")
                self.assertEqual(verdict["rule"], expected["rule"], f"Step {step['step']}")
                self.assertEqual(verdict["category"], expected["category"], f"Step {step['step']}")
                if expected["reason"] is not None and not expected["reason"].startswith("invalid json:"):
                    self.assertEqual(verdict["reason"], expected["reason"], f"Step {step['step']}")
                elif expected["reason"] is not None:
                    self.assertTrue(verdict["reason"].startswith("invalid json:"))

            rep_buf = PoOwnedBuf()
            self.assertEqual(
                self.lib.po_firewall_sender_report(fw, 10, now, ctypes.byref(rep_buf)),
                PO_OK,
            )
            report = json.loads(self._read_owned_buf(rep_buf).decode("utf-8"))
            self.assertEqual(report, vec["final_sender_report"])
        finally:
            self.lib.po_firewall_free(fw)

    def test_memory_governor_and_host_ram_detection(self) -> None:
        ram = self.lib.po_governor_detect_host_ram_bytes()
        self.assertGreater(ram, 0)

        gov = ctypes.c_void_p()
        self.assertEqual(self.lib.po_governor_new(1000, 0.80, ctypes.byref(gov)), PO_OK)
        try:
            self.assertEqual(self.lib.po_governor_try_acquire(gov, 500), PO_OK)
            self.assertEqual(self.lib.po_governor_in_use_bytes(gov), 500)
            self.assertEqual(self.lib.po_governor_try_acquire(gov, 300), PO_OK)
            self.assertEqual(self.lib.po_governor_in_use_bytes(gov), 800)
            # Exceeds 800-byte shed limit
            self.assertEqual(self.lib.po_governor_try_acquire(gov, 1), PO_ERR_OVERLOAD_SHED)
            self.assertEqual(self.lib.po_governor_shed_total(gov), 1)
            self.assertEqual(self.lib.po_governor_release(gov, 300), PO_OK)
            self.assertEqual(self.lib.po_governor_in_use_bytes(gov), 500)
        finally:
            self.lib.po_governor_free(gov)

    def test_dlq_fingerprint_conformance_vectors(self) -> None:
        vec = json.loads((VECTORS_DIR / "dlq_envelopes.json").read_text(encoding="utf-8"))
        for case in vec["fingerprint_cases"]:
            b_s, _b1 = _to_slice(case["source_broker"].encode("utf-8"))
            t_s, _b2 = _to_slice(case["source_topic"].encode("utf-8"))
            e_s, _b3 = _to_slice(case["error"].encode("utf-8"))
            p_s, _b4 = _to_slice(bytes.fromhex(case["payload_hex"]))
            out_hex = ctypes.create_string_buffer(33)
            rc = self.lib.po_dlq_fingerprint(b_s, t_s, e_s, p_s, out_hex)
            self.assertEqual(rc, PO_OK)
            self.assertEqual(out_hex.value.decode("ascii"), case["fingerprint"])

    def test_confluent_wire_framing_and_avro_protobuf_codecs(self) -> None:
        vec = json.loads((VECTORS_DIR / "wire_framing.json").read_text(encoding="utf-8"))
        codec = ctypes.c_void_p()
        self.assertEqual(self.lib.po_codec_new(ctypes.byref(codec)), PO_OK)
        try:
            for schema in vec["schemas"]:
                name_s, _b1 = _to_slice(schema["name"].encode("utf-8"))
                type_s, _b2 = _to_slice(schema["type"].encode("utf-8"))
                def_s, _b3 = _to_slice(json.dumps(schema["definition"]).encode("utf-8"))
                self.assertEqual(
                    self.lib.po_codec_register_schema(
                        codec, schema["schema_id"], name_s, type_s, def_s
                    ),
                    PO_OK,
                )

            rec_bytes = json.dumps(vec["sample_object"], sort_keys=True).encode("utf-8")
            rec_s, _b = _to_slice(rec_bytes)

            # Avro serialize + deserialize
            fmt_avro, _ba = _to_slice(b"avro")
            sch_avro, _bsa = _to_slice(b"reading_avro")
            out_avro = PoOwnedBuf()
            self.assertEqual(
                self.lib.po_codec_serialize(
                    codec, rec_s, fmt_avro, sch_avro, True, ctypes.byref(out_avro)
                ),
                PO_OK,
            )
            framed_avro = self._read_owned_buf(out_avro)
            self.assertEqual(framed_avro.hex(), vec["framed_avro_hex"])

            avro_in_s, _bai = _to_slice(framed_avro)
            empty_s, _be = _to_slice(b"")
            dec_avro_buf = PoOwnedBuf()
            self.assertEqual(
                self.lib.po_codec_deserialize(
                    codec, avro_in_s, fmt_avro, empty_s, True, ctypes.byref(dec_avro_buf)
                ),
                PO_OK,
            )
            self.assertEqual(
                json.loads(self._read_owned_buf(dec_avro_buf).decode("utf-8")),
                vec["decoded_avro"],
            )

            # Protobuf serialize + deserialize
            fmt_proto, _bp = _to_slice(b"protobuf")
            sch_proto, _bsp = _to_slice(b"reading_proto")
            out_proto = PoOwnedBuf()
            self.assertEqual(
                self.lib.po_codec_serialize(
                    codec, rec_s, fmt_proto, sch_proto, True, ctypes.byref(out_proto)
                ),
                PO_OK,
            )
            framed_proto = self._read_owned_buf(out_proto)
            self.assertEqual(framed_proto.hex(), vec["framed_proto_hex"])

            proto_in_s, _bpi = _to_slice(framed_proto)
            dec_proto_buf = PoOwnedBuf()
            self.assertEqual(
                self.lib.po_codec_deserialize(
                    codec, proto_in_s, fmt_proto, empty_s, True, ctypes.byref(dec_proto_buf)
                ),
                PO_OK,
            )
            self.assertEqual(
                json.loads(self._read_owned_buf(dec_proto_buf).decode("utf-8")),
                vec["decoded_proto"],
            )

            # Error cases on decode_confluent
            for err_case in vec["error_cases"]:
                raw_s, _berr = _to_slice(bytes.fromhex(err_case["raw_hex"]))
                sid = ctypes.c_uint32(0)
                out_p = PoOwnedBuf()
                rc = self.lib.po_codec_decode_confluent(
                    raw_s, ctypes.byref(sid), ctypes.byref(out_p)
                )
                self.assertEqual(rc, PO_ERR_WIRE_FORMAT)
        finally:
            self.lib.po_codec_free(codec)

    def test_c99_executable_links_and_runs_against_libpostoffice(self) -> None:
        c_code = """
#include "postoffice.h"
#include <assert.h>
#include <string.h>

int main(void) {
    assert(po_abi_version() == PO_ABI_VERSION);

    po_governor_t *gov = NULL;
    assert(po_governor_new(1000, 0.8, &gov) == PO_OK);
    assert(po_governor_try_acquire(gov, 800) == PO_OK);
    assert(po_governor_try_acquire(gov, 1) == PO_ERR_OVERLOAD_SHED);
    assert(po_governor_release(gov, 800) == PO_OK);
    po_governor_free(gov);

    po_trie_t *trie = NULL;
    assert(po_trie_new(&trie) == PO_OK);
    const char *pat = "sensors/+/temp";
    const char *rjson = "\\"r1\\"";
    bool added = false;
    po_slice_t pat_s = { (const uint8_t*)pat, strlen(pat) };
    po_slice_t r_s = { (const uint8_t*)rjson, strlen(rjson) };
    assert(po_trie_add_route(trie, pat_s, r_s, &added) == PO_OK);
    assert(added == true);

    const char *top = "sensors/room1/temp";
    po_slice_t top_s = { (const uint8_t*)top, strlen(top) };
    po_owned_buf_t out = { NULL, 0, 0 };
    assert(po_trie_match(trie, top_s, &out) == PO_OK);
    assert(out.len > 0);
    po_buf_free(&out);
    assert(out.data == NULL && out.len == 0);
    po_trie_free(trie);
    return 0;
}
"""
        with tempfile.TemporaryDirectory() as tmpdir:
            c_file = pathlib.Path(tmpdir) / "main.c"
            bin_file = pathlib.Path(tmpdir) / "test_c_abi"
            c_file.write_text(c_code, encoding="utf-8")
            proc = subprocess.run(
                [
                    "gcc",
                    "-std=c99",
                    "-Wall",
                    "-Wextra",
                    "-Werror",
                    "-I",
                    str(HEADER_PATH.parent),
                    str(c_file),
                    "-L",
                    str(self.so_path.parent),
                    "-lpostoffice",
                    f"-Wl,-rpath,{self.so_path.parent}",
                    "-o",
                    str(bin_file),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(proc.returncode, 0, f"gcc link failed:\n{proc.stderr}")
            run_proc = subprocess.run([str(bin_file)], capture_output=True, text=True, check=False)
            self.assertEqual(run_proc.returncode, 0, f"C binary failed:\n{run_proc.stderr}")


if __name__ == "__main__":
    unittest.main()
