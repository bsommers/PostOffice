"""
Python FFI bridge to `libpostoffice` (`include/postoffice.h`).

Activated when `POSTOFFICE_NATIVE=1` (or `true`/`yes`) is set in the environment,
or when `NativeTrie`, `NativeFirewall`, `NativeCodec`, `NativeClient`, or
`MemoryGovernor(..., use_native=True)` are instantiated directly.
"""

import ctypes
import json
import os
import pathlib
import subprocess
import threading
import time
from typing import Any, Callable, Dict, List, Mapping, Optional, Tuple

from postoffice.ingress.rules import Action, MessageContext, Verdict

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
CRATE_DIR = REPO_ROOT / "native" / "libpostoffice"

PO_OK = 0
PO_ERR_INVALID_ARGUMENT = -1
PO_ERR_NOT_FOUND = -2
PO_ERR_OUT_OF_MEMORY = -3
PO_ERR_OVERLOAD_SHED = -4
PO_ERR_FIREWALL_DROP = -5
PO_ERR_FIREWALL_REJECT = -6
PO_ERR_WIRE_FORMAT = -7
PO_ERR_SIGNATURE_INVALID = -8
PO_ERR_UNSUPPORTED_VERSION = -9
PO_ERR_IO = -10
PO_ERR_INTERNAL = -99


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


def is_native_enabled(env: Optional[Mapping[str, str]] = None) -> bool:
    """Returns True if `POSTOFFICE_NATIVE` is set to `1`, `true`, or `yes`."""
    source = os.environ if env is None else env
    raw = source.get("POSTOFFICE_NATIVE", "0").strip().lower()
    return raw in ("1", "true", "yes")


def _to_slice(raw: bytes) -> Tuple[PoSlice, Any]:
    if not raw:
        return PoSlice(None, 0), None
    buf = (ctypes.c_uint8 * len(raw)).from_buffer_copy(raw)
    ptr = ctypes.cast(buf, ctypes.POINTER(ctypes.c_uint8))
    return PoSlice(ptr, len(raw)), buf


_lib_lock = threading.Lock()
_cached_lib: Optional[ctypes.CDLL] = None


def get_libpostoffice() -> ctypes.CDLL:
    """Loads `libpostoffice.so`, building it via Cargo if not yet compiled."""
    global _cached_lib
    if _cached_lib is not None:
        return _cached_lib
    with _lib_lock:
        if _cached_lib is not None:
            return _cached_lib
        so_path = CRATE_DIR / "target" / "release" / "libpostoffice.so"
        if not so_path.is_file():
            proc = subprocess.run(
                [
                    "cargo",
                    "build",
                    "--release",
                    "--manifest-path",
                    str(CRATE_DIR / "Cargo.toml"),
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if proc.returncode != 0:
                raise RuntimeError(f"Failed to build libpostoffice:\n{proc.stderr}")
        lib = ctypes.CDLL(str(so_path))
        _bind_signatures(lib)
        _cached_lib = lib
        return lib


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

    lib.po_firewall_new.argtypes = [PoSlice, ctypes.POINTER(ctypes.c_void_p)]
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


def _consume_owned_buf(lib: ctypes.CDLL, buf: PoOwnedBuf) -> bytes:
    try:
        if not buf.data or buf.len == 0:
            return b""
        return ctypes.string_at(buf.data, buf.len)
    finally:
        lib.po_buf_free(ctypes.byref(buf))


class NativeTrie:
    """FFI wrapper over `po_trie_t`."""

    def __init__(self) -> None:
        self._lib = get_libpostoffice()
        self._handle = ctypes.c_void_p()
        rc = self._lib.po_trie_new(ctypes.byref(self._handle))
        if rc != PO_OK:
            raise RuntimeError(f"po_trie_new failed with status {rc}")
        self._payloads: List[Any] = []

    def __del__(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle and handle.value:
            self._lib.po_trie_free(handle)
            self._handle = ctypes.c_void_p()

    def add_route(self, pattern: str, route_json: str) -> bool:
        pat_s, _b1 = _to_slice(pattern.encode("utf-8"))
        r_s, _b2 = _to_slice(route_json.encode("utf-8"))
        added = ctypes.c_bool(False)
        rc = self._lib.po_trie_add_route(
            self._handle, pat_s, r_s, ctypes.byref(added)
        )
        if rc != PO_OK:
            raise ValueError(f"Invalid MQTT topic pattern: '{pattern}'")
        return bool(added.value)

    def remove_route(
        self, pattern: str, target_broker: str = "", target_topic: str = ""
    ) -> int:
        pat_s, _b1 = _to_slice(pattern.encode("utf-8"))
        tb_s, _b2 = _to_slice(target_broker.encode("utf-8"))
        tt_s, _b3 = _to_slice(target_topic.encode("utf-8"))
        removed = ctypes.c_size_t(0)
        rc = self._lib.po_trie_remove_route(
            self._handle, pat_s, tb_s, tt_s, ctypes.byref(removed)
        )
        if rc != PO_OK:
            raise ValueError(f"Invalid MQTT topic pattern: '{pattern}'")
        return int(removed.value)

    def match(self, topic: str) -> List[Any]:
        top_s, _b = _to_slice(topic.encode("utf-8"))
        out = PoOwnedBuf()
        rc = self._lib.po_trie_match(self._handle, top_s, ctypes.byref(out))
        if rc != PO_OK:
            raise RuntimeError(f"po_trie_match failed with status {rc}")
        raw = _consume_owned_buf(self._lib, out)
        return json.loads(raw.decode("utf-8"))

    def insert_python_payload(self, pattern: str, route_data: Any) -> None:
        slot = len(self._payloads)
        self.add_route(pattern, str(slot))
        self._payloads.append(route_data)

    def match_python_payloads(self, topic: str) -> List[Any]:
        slots = self.match(topic)
        return [self._payloads[idx] for idx in slots]


class NativeFirewall:
    """FFI wrapper over `po_firewall_t`."""

    def __init__(
        self,
        config: Dict[str, Any],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._lib = get_libpostoffice()
        self._clock = clock
        self._lock = threading.Lock()
        self._handle = ctypes.c_void_p()
        cfg_bytes = json.dumps(config).encode("utf-8")
        cfg_s, _b = _to_slice(cfg_bytes)
        rc = self._lib.po_firewall_new(cfg_s, ctypes.byref(self._handle))
        if rc != PO_OK:
            raise ValueError(f"Invalid firewall configuration (status {rc})")

    def __del__(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle and handle.value:
            self._lib.po_firewall_free(handle)
            self._handle = ctypes.c_void_p()

    def evaluate(
        self, ctx: MessageContext, now: Optional[float] = None
    ) -> Verdict:
        ts = self._clock() if now is None else now
        b_s, _b1 = _to_slice(ctx.broker.encode("utf-8"))
        s_s, _b2 = _to_slice((ctx.sender or "").encode("utf-8"))
        t_s, _b3 = _to_slice(ctx.topic.encode("utf-8"))
        raw_payload = ctx.payload if ctx.payload is not None else (b"\x00" * max(0, ctx.size))
        p_s, _b4 = _to_slice(raw_payload)
        out = PoOwnedBuf()
        with self._lock:
            rc = self._lib.po_firewall_evaluate(
                self._handle,
                b_s,
                s_s,
                t_s,
                p_s,
                ts,
                ctypes.byref(out),
            )
        if rc != PO_OK:
            raise RuntimeError(f"po_firewall_evaluate failed with status {rc}")
        doc = json.loads(_consume_owned_buf(self._lib, out).decode("utf-8"))
        return Verdict(
            action=Action(doc["action"]),
            rule=doc.get("rule"),
            category=doc.get("category"),
            reason=doc.get("reason"),
        )

    def sender_report(
        self, top: int = 10, now: Optional[float] = None
    ) -> List[Dict[str, Any]]:
        ts = self._clock() if now is None else now
        out = PoOwnedBuf()
        with self._lock:
            rc = self._lib.po_firewall_sender_report(
                self._handle, int(top), ts, ctypes.byref(out)
            )
        if rc != PO_OK:
            raise RuntimeError(f"po_firewall_sender_report failed with status {rc}")
        return json.loads(_consume_owned_buf(self._lib, out).decode("utf-8"))


class NativeCodec:
    """FFI wrapper over `po_codec_t`."""

    def __init__(self) -> None:
        self._lib = get_libpostoffice()
        self._handle = ctypes.c_void_p()
        rc = self._lib.po_codec_new(ctypes.byref(self._handle))
        if rc != PO_OK:
            raise RuntimeError(f"po_codec_new failed with status {rc}")

    def __del__(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle and handle.value:
            self._lib.po_codec_free(handle)
            self._handle = ctypes.c_void_p()

    def register_schema(
        self,
        schema_id: int,
        name: str,
        schema_type: str,
        definition: Dict[str, Any],
    ) -> None:
        n_s, _b1 = _to_slice(name.encode("utf-8"))
        t_s, _b2 = _to_slice(schema_type.encode("utf-8"))
        d_s, _b3 = _to_slice(json.dumps(definition).encode("utf-8"))
        rc = self._lib.po_codec_register_schema(
            self._handle, int(schema_id), n_s, t_s, d_s
        )
        if rc != PO_OK:
            raise ValueError(f"Failed to register native schema '{name}' (status {rc})")

    def serialize(
        self,
        record: Dict[str, Any],
        format_type: str,
        schema_name: Optional[str] = None,
        wire_format: bool = False,
    ) -> bytes:
        r_s, _b1 = _to_slice(json.dumps(record, sort_keys=True).encode("utf-8"))
        f_s, _b2 = _to_slice(format_type.encode("utf-8"))
        s_s, _b3 = _to_slice((schema_name or "").encode("utf-8"))
        out = PoOwnedBuf()
        rc = self._lib.po_codec_serialize(
            self._handle, r_s, f_s, s_s, bool(wire_format), ctypes.byref(out)
        )
        if rc != PO_OK:
            raise ValueError(f"Native serialize failed for {format_type} (status {rc})")
        return _consume_owned_buf(self._lib, out)

    def deserialize(
        self,
        payload: bytes,
        format_type: str,
        schema_name: Optional[str] = None,
        wire_format: bool = False,
    ) -> Dict[str, Any]:
        p_s, _b1 = _to_slice(payload)
        f_s, _b2 = _to_slice(format_type.encode("utf-8"))
        s_s, _b3 = _to_slice((schema_name or "").encode("utf-8"))
        out = PoOwnedBuf()
        rc = self._lib.po_codec_deserialize(
            self._handle, p_s, f_s, s_s, bool(wire_format), ctypes.byref(out)
        )
        if rc != PO_OK:
            raise ValueError(f"Native deserialize failed for {format_type} (status {rc})")
        return json.loads(_consume_owned_buf(self._lib, out).decode("utf-8"))


class NativeClient:
    """FFI wrapper over `po_client_t`."""

    def __init__(self, endpoint_url: str, bearer_token: str = "") -> None:
        self._lib = get_libpostoffice()
        self._handle = ctypes.c_void_p()
        u_s, _b1 = _to_slice(endpoint_url.encode("utf-8"))
        t_s, _b2 = _to_slice(bearer_token.encode("utf-8"))
        rc = self._lib.po_client_connect(u_s, t_s, ctypes.byref(self._handle))
        if rc != PO_OK:
            raise ValueError(f"po_client_connect failed with status {rc}")

    def __del__(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle and handle.value:
            self._lib.po_client_free(handle)
            self._handle = ctypes.c_void_p()

    def publish(
        self,
        broker: str,
        topic: str,
        payload: bytes,
        sender_id: str = "",
    ) -> Dict[str, Any]:
        b_s, _b1 = _to_slice(broker.encode("utf-8"))
        t_s, _b2 = _to_slice(topic.encode("utf-8"))
        p_s, _b3 = _to_slice(payload)
        s_s, _b4 = _to_slice(sender_id.encode("utf-8"))
        out = PoOwnedBuf()
        rc = self._lib.po_client_publish(
            self._handle, b_s, t_s, p_s, s_s, ctypes.byref(out)
        )
        if rc != PO_OK:
            raise RuntimeError(f"po_client_publish failed with status {rc}")
        return json.loads(_consume_owned_buf(self._lib, out).decode("utf-8"))


def native_encode_confluent(payload: bytes, schema_id: int) -> bytes:
    lib = get_libpostoffice()
    p_s, _b = _to_slice(payload)
    out = PoOwnedBuf()
    rc = lib.po_codec_encode_confluent(int(schema_id), p_s, ctypes.byref(out))
    if rc != PO_OK:
        raise ValueError(f"po_codec_encode_confluent failed with status {rc}")
    return _consume_owned_buf(lib, out)


def native_decode_confluent(framed: bytes) -> Tuple[int, bytes]:
    lib = get_libpostoffice()
    f_s, _b = _to_slice(framed)
    sid = ctypes.c_uint32(0)
    out = PoOwnedBuf()
    rc = lib.po_codec_decode_confluent(
        f_s, ctypes.byref(sid), ctypes.byref(out)
    )
    if rc != PO_OK:
        raise ValueError(f"Invalid Confluent wire format (status {rc})")
    body = _consume_owned_buf(lib, out)
    return int(sid.value), body


def native_dlq_fingerprint(
    source_broker: str, source_topic: str, error: str, payload: bytes
) -> str:
    lib = get_libpostoffice()
    b_s, _b1 = _to_slice(source_broker.encode("utf-8", "replace"))
    t_s, _b2 = _to_slice(source_topic.encode("utf-8", "replace"))
    e_s, _b3 = _to_slice(error.encode("utf-8", "replace"))
    p_s, _b4 = _to_slice(payload)
    out_hex = ctypes.create_string_buffer(33)
    rc = lib.po_dlq_fingerprint(b_s, t_s, e_s, p_s, out_hex)
    if rc != PO_OK:
        raise RuntimeError(f"po_dlq_fingerprint failed with status {rc}")
    return out_hex.value.decode("ascii")
