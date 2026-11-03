"""Phase 3 Shadow Mode Differential Harness (`src/postoffice/shadow.py`).

Runs the Python Data Plane (`Router`) and the Native Rust Data Plane (`NativeEngine`)
side-by-side on mirrored control-plane events and ingress message streams,
diffing egress envelopes (`target_broker`, `topic`, `payload_hex`, `kwargs`),
acknowledgement decisions (`acked`, `nacked`, `requeue`), DLQ envelopes, and
Prometheus metrics counters.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any, Dict, List, Optional, Sequence

from postoffice.dlq import BudgetedDlq
from postoffice.ingress.firewall import Firewall
from postoffice.native import NativeEngine
from postoffice.router import Router


@dataclass(frozen=True)
class ShadowDiff:
    """A single divergence detected between Python and Native Data Plane outputs."""

    index: int
    field: str
    python_value: Any
    native_value: Any


@dataclass
class ShadowReport:
    """Summary of a shadow-mode differential soak run."""

    total_messages: int
    diffs: List[ShadowDiff] = field(default_factory=list)

    @property
    def is_clean(self) -> bool:
        return len(self.diffs) == 0


class _CaptureBrokerClient:
    """Hermetic broker client capturing published messages for Python Router comparison."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.published: List[Dict[str, Any]] = []
        self.fail_publish: bool = False

    def publish(
        self,
        topic: str,
        message: bytes,
        on_confirm: Optional[Any] = None,
        on_error: Optional[Any] = None,
        **kwargs: Any,
    ) -> None:
        if self.fail_publish:
            exc = RuntimeError(f"Simulated publish failure on {self.name}")
            if on_error is not None:
                on_error(exc)
                return
            raise exc
        clean_kwargs = {
            k: v
            for k, v in kwargs.items()
            if k not in ("on_confirm", "on_error")
        }
        self.published.append(
            {
                "target_broker": self.name,
                "topic": topic,
                "payload_hex": bytes(message).hex(),
                "kwargs": clean_kwargs,
            }
        )
        if on_confirm is not None:
            on_confirm()


def _normalize_dlq_list(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalizes JSON DLQ envelopes for cross-runtime structural comparison."""
    out: List[Dict[str, Any]] = []
    for item in items:
        raw_bytes = bytes.fromhex(item["payload_hex"])
        parsed = json.loads(raw_bytes.decode("utf-8"))
        parsed.pop("timestamp", None)
        err = parsed.get("error", "")
        if isinstance(err, str) and err.startswith("Schema transformation error:"):
            parsed["error"] = "Schema transformation error"
        out.append(
            {
                "target_broker": item["target_broker"],
                "topic": item["topic"],
                "envelope": parsed,
            }
        )
    return out


class ShadowHarness:
    """Runs Python `Router` and Rust `NativeEngine` in lockstep and diffs every outcome."""

    def __init__(self, unroutable_policy: str = "nack") -> None:
        self._unroutable_policy = unroutable_policy
        self._now_sec: float = 1.0
        self._step_index: int = 0
        self._init_engines()

    def _init_engines(self) -> None:
        self.py_router = Router()
        self.py_router.set_retry_policy(max_attempts=None)
        self.py_router.set_unroutable_policy(self._unroutable_policy)
        self._py_brokers: Dict[str, _CaptureBrokerClient] = {}

        self.native = NativeEngine()
        self.native.set_unroutable_policy(self._unroutable_policy)

    def _ensure_broker(self, name: str, protocol: str = "mqtt") -> _CaptureBrokerClient:
        if name not in self._py_brokers:
            client = _CaptureBrokerClient(name)
            self._py_brokers[name] = client
            self.py_router.register_client(client)
            self.native.register_broker(name, protocol, {})
        return self._py_brokers[name]

    def apply_action(self, action_spec: Dict[str, Any]) -> None:
        """Applies a control-plane action identically to both Python and Native engines."""
        spec = dict(action_spec)
        act = spec.pop("action")

        if act == "clear_state":
            self._init_engines()
            return

        if act == "add_broker":
            name = spec["name"]
            protocol = spec.get("protocol", "mqtt")
            self._ensure_broker(name, protocol)
            return

        if act == "set_unroutable_policy":
            policy = spec["policy"]
            broker = spec.get("broker")
            self._unroutable_policy = policy if broker is None else self._unroutable_policy
            self.py_router.set_unroutable_policy(policy, broker=broker)
            self.native.set_unroutable_policy(policy, broker=broker)
            return

        if act == "set_dlq":
            broker = spec.get("broker") or spec.get("target_broker", "dlq")
            topic = spec["topic"]
            self._ensure_broker(broker)
            self.py_router.set_dlq(broker, topic)
            max_bytes = int(
                spec.get("max_payload_bytes", spec.get("max_message_bytes", 256 * 1024))
            )
            msgs_per_sec = float(spec.get("rate_per_sec", spec.get("msgs_per_sec", 100.0)))
            bytes_per_sec = float(spec.get("bytes_per_sec", 1024.0 * 1024.0))
            dedupe_win = spec.get("dedupe_window_sec", spec.get("dedupe_window"))
            self.py_router.set_dlq_budget(
                BudgetedDlq(
                    max_message_bytes=max_bytes,
                    msgs_per_sec=msgs_per_sec,
                    bytes_per_sec=bytes_per_sec,
                    dedupe_window=float(dedupe_win) if dedupe_win else None,
                    clock=lambda: self._now_sec,
                )
            )
            self.native.apply_action("set_dlq", spec | {"broker": broker})
            return

        if act == "register_schema":
            name = spec["name"]
            schema_str = spec.get("schema_str") or spec["definition"]
            s_type = str(spec.get("schema_type", spec.get("type", "AVRO"))).upper()
            py_def = json.loads(schema_str) if isinstance(schema_str, str) else schema_str
            sid = self.py_router.schema_registry.register_schema(
                name, py_def, schema_type=s_type
            )
            self.native.apply_action(
                "register_schema",
                {
                    "name": name,
                    "schema_type": s_type,
                    "schema_str": (
                        schema_str
                        if isinstance(schema_str, str)
                        else json.dumps(schema_str)
                    ),
                    "schema_id": sid,
                },
            )
            return

        if act == "set_ingress":
            fw_cfg = spec.get("config", spec)
            self.py_router.set_firewall(Firewall.from_dict(fw_cfg))
            self.native.set_ingress(fw_cfg)
            return

        if act == "add_route":
            sb = spec["source_broker"]
            st = spec["source_topic"]
            tb = spec["target_broker"]
            tt = spec["target_topic"]
            kw = dict(spec.get("kwargs") or spec.get("target_kwargs") or {})
            if kw.get("confluent_framing"):
                kw["wire_format_out"] = True
            self._ensure_broker(sb)
            self._ensure_broker(tb)
            py_kw = {k: v for k, v in kw.items() if k != "confluent_framing"}
            self.py_router.add_route(sb, st, tb, tt, **py_kw)
            self.native.add_route(sb, st, tb, tt, kw)
            return

        raise ValueError(f"Unsupported shadow control action: {act}")

    def step(
        self,
        source_broker: str,
        topic: str,
        payload: bytes,
        kwargs: Optional[Dict[str, Any]] = None,
        now_ns: Optional[int] = None,
        egress_failures: Optional[Sequence[str]] = None,
    ) -> List[ShadowDiff]:
        """Routes one message through both Python and Native engines and returns any diffs."""
        idx = self._step_index
        self._step_index += 1
        if now_ns is not None:
            self._now_sec = float(now_ns) / 1_000_000_000.0
        else:
            self._now_sec += 0.001

        self._ensure_broker(source_broker)
        fail_set = set(egress_failures or ())
        for b_name, client in self._py_brokers.items():
            client.published.clear()
            client.fail_publish = b_name in fail_set
            self.native.register_broker(
                b_name, "mqtt", {"fail_publish": b_name in fail_set}
            )

        py_acked = [False]
        py_nacked = [False]
        py_requeue = [False]

        def _ack() -> None:
            py_acked[0] = True

        def _nack(requeue: bool = False) -> None:
            py_nacked[0] = True
            py_requeue[0] = bool(requeue)

        src_kw = dict(kwargs or {})
        self.py_router.route(
            source_broker,
            topic,
            payload,
            ack_fn=_ack,
            nack_fn=_nack,
            **src_kw,
        )

        dlq_broker = self.py_router.default_dlq_broker
        dlq_topic = self.py_router.default_dlq_topic
        py_egress: List[Dict[str, Any]] = []
        py_dlq: List[Dict[str, Any]] = []
        for b_name, client in self._py_brokers.items():
            for pub in client.published:
                if b_name == dlq_broker and pub["topic"] == dlq_topic:
                    py_dlq.append(
                        {
                            "target_broker": pub["target_broker"],
                            "topic": pub["topic"],
                            "payload_hex": pub["payload_hex"],
                        }
                    )
                else:
                    py_egress.append(pub)

        nat_out = self.native.route(
            source_broker,
            topic,
            payload,
            source_kwargs=src_kw,
            now_sec=self._now_sec,
        )

        diffs: List[ShadowDiff] = []
        if py_acked[0] != bool(nat_out.get("acked")):
            diffs.append(ShadowDiff(idx, "acked", py_acked[0], nat_out.get("acked")))
        if py_nacked[0] != bool(nat_out.get("nacked")):
            diffs.append(ShadowDiff(idx, "nacked", py_nacked[0], nat_out.get("nacked")))
        if py_nacked[0] and py_requeue[0] != bool(nat_out.get("requeue")):
            diffs.append(ShadowDiff(idx, "requeue", py_requeue[0], nat_out.get("requeue")))

        nat_egress = nat_out.get("published", [])
        if py_egress != nat_egress:
            diffs.append(ShadowDiff(idx, "egress", py_egress, nat_egress))

        norm_py_dlq = _normalize_dlq_list(py_dlq)
        norm_nat_dlq = _normalize_dlq_list(nat_out.get("dlq_published", []))
        if norm_py_dlq != norm_nat_dlq:
            diffs.append(ShadowDiff(idx, "dlq", norm_py_dlq, norm_nat_dlq))

        return diffs

    def run_soak(self, messages: Sequence[Dict[str, Any]]) -> ShadowReport:
        """Runs a sequence of messages through `step()` and aggregates a `ShadowReport`."""
        all_diffs: List[ShadowDiff] = []
        for msg in messages:
            step_diffs = self.step(
                source_broker=msg["source_broker"],
                topic=msg["topic"],
                payload=msg["payload"],
                kwargs=msg.get("kwargs"),
                now_ns=msg.get("now_ns"),
                egress_failures=msg.get("egress_failures"),
            )
            all_diffs.extend(step_diffs)
        return ShadowReport(total_messages=len(messages), diffs=all_diffs)
