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

    lib.po_signer_new.argtypes = [PoSlice, ctypes.POINTER(ctypes.c_void_p)]
    lib.po_signer_new.restype = ctypes.c_int

    lib.po_signer_free.argtypes = [ctypes.c_void_p]
    lib.po_signer_free.restype = None

    lib.po_signer_sign_event.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        ctypes.POINTER(PoOwnedBuf),
    ]
    lib.po_signer_sign_event.restype = ctypes.c_int

    lib.po_signer_sign_state.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        PoSlice,
        PoSlice,
        ctypes.POINTER(PoOwnedBuf),
    ]
    lib.po_signer_sign_state.restype = ctypes.c_int

    lib.po_signer_sign_version.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int64,
        ctypes.POINTER(PoOwnedBuf),
    ]
    lib.po_signer_sign_version.restype = ctypes.c_int

    lib.po_signer_verify_event.argtypes = [ctypes.c_void_p, PoSlice]
    lib.po_signer_verify_event.restype = ctypes.c_int

    lib.po_signer_verify_state.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        PoSlice,
        PoSlice,
    ]
    lib.po_signer_verify_state.restype = ctypes.c_int

    lib.po_signer_verify_version.argtypes = [
        ctypes.c_void_p,
        ctypes.c_int64,
        PoSlice,
    ]
    lib.po_signer_verify_version.restype = ctypes.c_int

    lib.po_validate_action.argtypes = [PoSlice, PoSlice]
    lib.po_validate_action.restype = ctypes.c_int

    lib.po_dlq_format_payload.argtypes = [
        PoSlice,
        PoSlice,
        PoSlice,
        PoSlice,
        PoSlice,
        PoSlice,
        ctypes.POINTER(PoOwnedBuf),
    ]
    lib.po_dlq_format_payload.restype = ctypes.c_int

    lib.po_dlq_budget_new.argtypes = [
        ctypes.c_size_t,
        ctypes.c_double,
        ctypes.c_double,
        ctypes.c_size_t,
        ctypes.c_double,
        ctypes.c_size_t,
        ctypes.POINTER(ctypes.c_void_p),
    ]
    lib.po_dlq_budget_new.restype = ctypes.c_int

    lib.po_dlq_budget_free.argtypes = [ctypes.c_void_p]
    lib.po_dlq_budget_free.restype = None

    lib.po_dlq_budget_admit.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        PoSlice,
        PoSlice,
        PoSlice,
        ctypes.c_bool,
        ctypes.c_double,
        ctypes.POINTER(PoOwnedBuf),
    ]
    lib.po_dlq_budget_admit.restype = ctypes.c_int

    lib.po_engine_new.argtypes = [ctypes.c_uint32, ctypes.POINTER(ctypes.c_void_p)]
    lib.po_engine_new.restype = ctypes.c_int

    lib.po_engine_free.argtypes = [ctypes.c_void_p]
    lib.po_engine_free.restype = None

    lib.po_engine_apply_action.argtypes = [ctypes.c_void_p, PoSlice, PoSlice]
    lib.po_engine_apply_action.restype = ctypes.c_int

    lib.po_engine_route.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        PoSlice,
        PoSlice,
        PoSlice,
        ctypes.c_double,
        ctypes.POINTER(PoOwnedBuf),
    ]
    lib.po_engine_route.restype = ctypes.c_int

    lib.po_engine_metrics_text.argtypes = [
        ctypes.c_void_p,
        ctypes.POINTER(PoOwnedBuf),
    ]
    lib.po_engine_metrics_text.restype = ctypes.c_int

    lib.po_engine_start_metrics_server.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        ctypes.c_uint16,
        ctypes.POINTER(ctypes.c_uint16),
    ]
    lib.po_engine_start_metrics_server.restype = ctypes.c_int

    lib.po_engine_stop_metrics_server.argtypes = [ctypes.c_void_p]
    lib.po_engine_stop_metrics_server.restype = ctypes.c_int

    lib.po_engine_sync_redis.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        ctypes.c_uint16,
        ctypes.c_double,
        ctypes.POINTER(PoOwnedBuf),
    ]
    lib.po_engine_sync_redis.restype = ctypes.c_int

    lib.po_kafka_tracker_new.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
    lib.po_kafka_tracker_new.restype = ctypes.c_int

    lib.po_kafka_tracker_free.argtypes = [ctypes.c_void_p]
    lib.po_kafka_tracker_free.restype = None

    lib.po_kafka_tracker_poll.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        ctypes.c_int32,
        ctypes.c_int64,
    ]
    lib.po_kafka_tracker_poll.restype = ctypes.c_int

    lib.po_kafka_tracker_ack.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        ctypes.c_int32,
        ctypes.c_int64,
        ctypes.POINTER(ctypes.c_bool),
        ctypes.POINTER(ctypes.c_int64),
    ]
    lib.po_kafka_tracker_ack.restype = ctypes.c_int

    lib.po_kafka_tracker_watermark.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        ctypes.c_int32,
    ]
    lib.po_kafka_tracker_watermark.restype = ctypes.c_int64

    lib.po_kafka_tracker_reset.argtypes = [
        ctypes.c_void_p,
        PoSlice,
        ctypes.c_int32,
    ]
    lib.po_kafka_tracker_reset.restype = ctypes.c_int


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


def native_validate_action(action: str, data: Dict[str, Any]) -> bool:
    lib = get_libpostoffice()
    a_s, _b1 = _to_slice(action.encode("utf-8"))
    d_s, _b2 = _to_slice(json.dumps(data).encode("utf-8"))
    rc = lib.po_validate_action(a_s, d_s)
    return rc == PO_OK


def native_format_dlq_payload(
    original_payload: bytes,
    source_broker: str,
    source_topic: str,
    error: str,
    metadata: Optional[Dict[str, Any]] = None,
    timestamp: Optional[str] = None,
) -> bytes:
    lib = get_libpostoffice()
    p_s, _b1 = _to_slice(original_payload)
    sb_s, _b2 = _to_slice(source_broker.encode("utf-8"))
    st_s, _b3 = _to_slice(source_topic.encode("utf-8"))
    e_s, _b4 = _to_slice(str(error).encode("utf-8"))
    m_bytes = json.dumps(metadata).encode("utf-8") if metadata is not None else b""
    m_s, _b5 = _to_slice(m_bytes)
    ts_bytes = timestamp.encode("utf-8") if timestamp is not None else b""
    ts_s, _b6 = _to_slice(ts_bytes)
    out = PoOwnedBuf()
    rc = lib.po_dlq_format_payload(
        p_s, sb_s, st_s, e_s, m_s, ts_s, ctypes.byref(out)
    )
    if rc != PO_OK:
        raise RuntimeError(f"po_dlq_format_payload failed with status {rc}")
    return _consume_owned_buf(lib, out)


class NativeSigner:
    """FFI wrapper over `po_signer_t` (D9 HMAC-SHA256 signer/verifier)."""

    def __init__(self, keyring: str) -> None:
        self._lib = get_libpostoffice()
        self._handle = ctypes.c_void_p()
        k_s, _b = _to_slice(keyring.encode("utf-8"))
        rc = self._lib.po_signer_new(k_s, ctypes.byref(self._handle))
        if rc != PO_OK:
            raise ValueError(f"Invalid keyring for NativeSigner (status {rc})")

    def __del__(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle and handle.value:
            self._lib.po_signer_free(handle)
            self._handle = ctypes.c_void_p()

    def sign_event(self, payload: str) -> str:
        p_s, _b = _to_slice(payload.encode("utf-8"))
        out = PoOwnedBuf()
        rc = self._lib.po_signer_sign_event(self._handle, p_s, ctypes.byref(out))
        if rc != PO_OK:
            raise RuntimeError(f"po_signer_sign_event failed ({rc})")
        return _consume_owned_buf(self._lib, out).decode("utf-8")

    def sign_state(self, hash_name: str, field: str, payload: str) -> str:
        h_s, _b1 = _to_slice(hash_name.encode("utf-8"))
        f_s, _b2 = _to_slice(field.encode("utf-8"))
        p_s, _b3 = _to_slice(payload.encode("utf-8"))
        out = PoOwnedBuf()
        rc = self._lib.po_signer_sign_state(
            self._handle, h_s, f_s, p_s, ctypes.byref(out)
        )
        if rc != PO_OK:
            raise RuntimeError(f"po_signer_sign_state failed ({rc})")
        return _consume_owned_buf(self._lib, out).decode("utf-8")

    def sign_version(self, version: int) -> str:
        out = PoOwnedBuf()
        rc = self._lib.po_signer_sign_version(
            self._handle, int(version), ctypes.byref(out)
        )
        if rc != PO_OK:
            raise RuntimeError(f"po_signer_sign_version failed ({rc})")
        return _consume_owned_buf(self._lib, out).decode("utf-8")

    def verify_event(self, envelope_json: str) -> bool:
        e_s, _b = _to_slice(envelope_json.encode("utf-8"))
        return self._lib.po_signer_verify_event(self._handle, e_s) == PO_OK

    def verify_state(self, hash_name: str, field: str, envelope_json: str) -> bool:
        h_s, _b1 = _to_slice(hash_name.encode("utf-8"))
        f_s, _b2 = _to_slice(field.encode("utf-8"))
        e_s, _b3 = _to_slice(envelope_json.encode("utf-8"))
        return (
            self._lib.po_signer_verify_state(self._handle, h_s, f_s, e_s)
            == PO_OK
        )

    def verify_version(self, version: int, entry_json: str) -> bool:
        e_s, _b = _to_slice(entry_json.encode("utf-8"))
        return (
            self._lib.po_signer_verify_version(self._handle, int(version), e_s)
            == PO_OK
        )


class NativeDlqBudget:
    """FFI wrapper over `po_dlq_budget_t`."""

    def __init__(
        self,
        max_message_bytes: int = 256 * 1024,
        msgs_per_sec: float = 100.0,
        bytes_per_sec: float = 1024.0 * 1024.0,
        max_sources: int = 1024,
        dedupe_window: Optional[float] = None,
        max_fingerprints: int = 4096,
    ) -> None:
        self._lib = get_libpostoffice()
        self._handle = ctypes.c_void_p()
        win = float(dedupe_window) if dedupe_window is not None else 0.0
        rc = self._lib.po_dlq_budget_new(
            int(max_message_bytes),
            float(msgs_per_sec),
            float(bytes_per_sec),
            int(max_sources),
            win,
            int(max_fingerprints),
            ctypes.byref(self._handle),
        )
        if rc != PO_OK:
            raise ValueError(f"po_dlq_budget_new failed ({rc})")

    def __del__(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle and handle.value:
            self._lib.po_dlq_budget_free(handle)
            self._handle = ctypes.c_void_p()

    def admit(
        self,
        source_broker: str,
        source_topic: str,
        error: str,
        payload: bytes,
        metadata_only: bool = False,
        now_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        ts = time.monotonic() if now_sec is None else float(now_sec)
        sb_s, _b1 = _to_slice(source_broker.encode("utf-8"))
        st_s, _b2 = _to_slice(source_topic.encode("utf-8"))
        e_s, _b3 = _to_slice(error.encode("utf-8"))
        p_s, _b4 = _to_slice(payload)
        out = PoOwnedBuf()
        rc = self._lib.po_dlq_budget_admit(
            self._handle,
            sb_s,
            st_s,
            e_s,
            p_s,
            bool(metadata_only),
            ts,
            ctypes.byref(out),
        )
        if rc != PO_OK:
            raise RuntimeError(f"po_dlq_budget_admit failed ({rc})")
        return json.loads(_consume_owned_buf(self._lib, out).decode("utf-8"))


class NativeEngine:
    """FFI wrapper over `po_engine_t` (Phase 3 Native Data Plane Engine)."""

    def __init__(self, max_label_values: int = 100) -> None:
        self._lib = get_libpostoffice()
        self._handle = ctypes.c_void_p()
        rc = self._lib.po_engine_new(
            int(max_label_values), ctypes.byref(self._handle)
        )
        if rc != PO_OK:
            raise ValueError(f"po_engine_new failed ({rc})")

    def __del__(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle and handle.value:
            self._lib.po_engine_free(handle)
            self._handle = ctypes.c_void_p()

    def apply_action(self, action: str, data: Dict[str, Any]) -> None:
        a_s, _b1 = _to_slice(action.encode("utf-8"))
        d_s, _b2 = _to_slice(json.dumps(data).encode("utf-8"))
        rc = self._lib.po_engine_apply_action(self._handle, a_s, d_s)
        if rc != PO_OK:
            raise ValueError(f"po_engine_apply_action({action}) failed ({rc})")

    def register_broker(
        self, name: str, protocol: str, kwargs: Optional[Dict[str, Any]] = None
    ) -> None:
        self.apply_action(
            "add_broker",
            {"name": name, "protocol": protocol, "kwargs": kwargs or {}},
        )

    def add_route(
        self,
        source_broker: str,
        source_topic: str,
        target_broker: str,
        target_topic: str,
        target_kwargs: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.apply_action(
            "add_route",
            {
                "source_broker": source_broker,
                "source_topic": source_topic,
                "target_broker": target_broker,
                "target_topic": target_topic,
                "target_kwargs": target_kwargs or {},
            },
        )

    def set_dlq(self, broker: str, topic: str) -> None:
        self.apply_action("set_dlq", {"broker": broker, "topic": topic})

    def set_unroutable_policy(
        self, policy: str, broker: Optional[str] = None
    ) -> None:
        data: Dict[str, Any] = {"policy": policy}
        if broker is not None:
            data["broker"] = broker
        self.apply_action("set_unroutable_policy", data)

    def set_ingress(self, config: Dict[str, Any]) -> None:
        self.apply_action("set_ingress", config)

    def route(
        self,
        source_broker: str,
        source_topic: str,
        payload: bytes,
        source_kwargs: Optional[Dict[str, Any]] = None,
        now_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        ts = time.monotonic() if now_sec is None else float(now_sec)
        sb_s, _b1 = _to_slice(source_broker.encode("utf-8"))
        st_s, _b2 = _to_slice(source_topic.encode("utf-8"))
        p_s, _b3 = _to_slice(payload)
        skw_bytes = (
            json.dumps(source_kwargs).encode("utf-8")
            if source_kwargs
            else b"{}"
        )
        skw_s, _b4 = _to_slice(skw_bytes)
        out = PoOwnedBuf()
        rc = self._lib.po_engine_route(
            self._handle, sb_s, st_s, p_s, skw_s, ts, ctypes.byref(out)
        )
        if rc != PO_OK:
            raise RuntimeError(f"po_engine_route failed ({rc})")
        return json.loads(_consume_owned_buf(self._lib, out).decode("utf-8"))

    def metrics_text(self) -> str:
        out = PoOwnedBuf()
        rc = self._lib.po_engine_metrics_text(self._handle, ctypes.byref(out))
        if rc != PO_OK:
            raise RuntimeError(f"po_engine_metrics_text failed ({rc})")
        return _consume_owned_buf(self._lib, out).decode("utf-8")

    def start_metrics_server(
        self, addr: str = "127.0.0.1", port: int = 0
    ) -> int:
        a_s, _b = _to_slice(addr.encode("utf-8"))
        bound_port = ctypes.c_uint16(0)
        rc = self._lib.po_engine_start_metrics_server(
            self._handle, a_s, int(port), ctypes.byref(bound_port)
        )
        if rc != PO_OK:
            raise RuntimeError(f"po_engine_start_metrics_server failed ({rc})")
        return int(bound_port.value)

    def stop_metrics_server(self) -> None:
        self._lib.po_engine_stop_metrics_server(self._handle)

    def sync_from_redis(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        now_sec: Optional[float] = None,
    ) -> Dict[str, Any]:
        ts = time.monotonic() if now_sec is None else float(now_sec)
        h_s, _b = _to_slice(host.encode("utf-8"))
        out = PoOwnedBuf()
        rc = self._lib.po_engine_sync_redis(
            self._handle, h_s, int(port), ts, ctypes.byref(out)
        )
        if rc != PO_OK:
            raise RuntimeError(f"po_engine_sync_redis failed ({rc})")
        return json.loads(_consume_owned_buf(self._lib, out).decode("utf-8"))


class NativeKafkaTracker:
    """FFI wrapper over `po_kafka_tracker_t` (contiguous offset watermark tracker)."""

    def __init__(self) -> None:
        self._lib = get_libpostoffice()
        self._handle = ctypes.c_void_p()
        rc = self._lib.po_kafka_tracker_new(ctypes.byref(self._handle))
        if rc != PO_OK:
            raise RuntimeError(f"po_kafka_tracker_new failed ({rc})")

    def __del__(self) -> None:
        handle = getattr(self, "_handle", None)
        if handle and handle.value:
            self._lib.po_kafka_tracker_free(handle)
            self._handle = ctypes.c_void_p()

    def poll_offset(self, topic: str, partition: int, offset: int) -> None:
        t_s, _b = _to_slice(topic.encode("utf-8"))
        rc = self._lib.po_kafka_tracker_poll(
            self._handle, t_s, int(partition), int(offset)
        )
        if rc != PO_OK:
            raise RuntimeError(f"po_kafka_tracker_poll failed ({rc})")

    def ack_offset(
        self, topic: str, partition: int, offset: int
    ) -> Optional[int]:
        t_s, _b = _to_slice(topic.encode("utf-8"))
        adv = ctypes.c_bool(False)
        high = ctypes.c_int64(-1)
        rc = self._lib.po_kafka_tracker_ack(
            self._handle,
            t_s,
            int(partition),
            int(offset),
            ctypes.byref(adv),
            ctypes.byref(high),
        )
        if rc != PO_OK:
            raise RuntimeError(f"po_kafka_tracker_ack failed ({rc})")
        return int(high.value) if adv.value else None

    def committed_watermark(self, topic: str, partition: int) -> int:
        t_s, _b = _to_slice(topic.encode("utf-8"))
        return int(
            self._lib.po_kafka_tracker_watermark(
                self._handle, t_s, int(partition)
            )
        )

    def reset_partition(self, topic: str, partition: int) -> None:
        t_s, _b = _to_slice(topic.encode("utf-8"))
        self._lib.po_kafka_tracker_reset(self._handle, t_s, int(partition))


