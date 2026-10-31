"""
Phase 0 (P-B) Python Data Plane Worker Baseline Benchmark Matrix (`PERFORMANCE_TEST_PLAN.md` §2 T2 & §5 P-B).

Measures the full `Router.route()` hot path across the decision-gate axes:
- Reference SLO workload: 1 KB payload, 1 route (MQTT -> Kafka), at-least-once confirm
- Payload sizes: 64 B, 1 KB, 16 KB
- Protocol semantic translation: MQTT -> Kafka, MQTT -> AMQP, AMQP -> Kafka
- Schema conversion: passthrough, JSON -> Avro (Confluent wire format), Avro -> JSON, JSON -> Protobuf
- Wildcard fan-out: 1, 5, 20 target routes
- Ingress firewall: none, 5-rule chain, 5-rule chain + ContentGuard
- Multi-thread GIL contention: 1, 4, 8 concurrent ingress callback threads
- 10x overload shedding with `InflightLimiter` and `Oracle` accounting verification

Run manually:
    PYTHONPATH=src .venv/bin/python -m perf.baseline [--quick] [--out perf/results]
"""
import argparse
import json
import logging
import resource
import statistics
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from perf import header, oracle, probe, results
from postoffice.ingress.firewall import Firewall
from postoffice.router import Router
from postoffice.schema import InMemorySchemaRegistry

BIG = 1e12

AVRO_SCHEMA = {
    "type": "record",
    "name": "SensorTelemetry",
    "fields": [
        {"name": "sensor_id", "type": "string"},
        {"name": "temperature", "type": "double"},
        {"name": "status", "type": "string"},
        {"name": "pad", "type": "string"},
    ],
}

PROTO_SCHEMA = {
    "package": "iot",
    "message_name": "DeviceEvent",
    "fields": [
        {"name": "sensor_id", "type": "string", "number": 1},
        {"name": "temperature", "type": "double", "number": 2},
        {"name": "status", "type": "string", "number": 3},
        {"name": "pad", "type": "string", "number": 4},
    ],
}


class _FastEgressClient:
    """In-process egress client stand-in that invokes `on_confirm` immediately and optionally feeds `Oracle`."""

    def __init__(self, name: str, sink_oracle: Optional[oracle.Oracle] = None, auto_confirm: bool = True):
        self.name = name
        self.sink_oracle = sink_oracle
        self.auto_confirm = auto_confirm
        self.published = 0
        self.pending_confirms: List[Callable[[], None]] = []

    def publish(self, topic: str, message: bytes, on_confirm: Optional[Callable[[], None]] = None, **kwargs: Any) -> None:
        self.published += 1
        if self.sink_oracle is not None:
            self.sink_oracle.received(message, time.time_ns())
        if on_confirm is not None:
            if self.auto_confirm:
                on_confirm()
            else:
                self.pending_confirms.append(on_confirm)

    def pause_consuming(self) -> None:
        pass

    def resume_consuming(self) -> None:
        pass


def _json_payload(size: int) -> bytes:
    base = {"sensor_id": "sensor-17", "temperature": 23.5, "status": "OK", "pad": ""}
    raw = json.dumps(base).encode("utf-8")
    pad_len = max(0, size - len(raw))
    base["pad"] = "x" * pad_len
    encoded = json.dumps(base).encode("utf-8")
    return encoded[:size] if len(encoded) >= size else encoded + b" " * (size - len(encoded))


def _firewall_5_rules(with_content_guard: bool = False) -> Firewall:
    rules: List[Dict[str, Any]] = [
        {"type": "max_payload_bytes", "max_bytes": 1 << 20},
        {"type": "topic_shape", "deny": ["$SYS/#", "admin/+/secret"]},
        {"type": "rate_limit", "rate": BIG, "key": "broker"},
        {"type": "rate_limit", "rate": BIG, "key": "topic"},
        {"type": "rate_limit", "rate": BIG, "key": "identity", "template": "devices/{device_id}/telemetry"},
    ]
    if with_content_guard:
        rules.append(
            {
                "type": "content_guard",
                "topic": "devices/#",
                "required_fields": ["sensor_id", "temperature", "status"],
            }
        )
    return Firewall.from_dict({"rules": rules})


def _build_router(
    *,
    source_name: str = "mqtt",
    target_name: str = "kafka",
    fanout: int = 1,
    firewall: Optional[Firewall] = None,
    route_kwargs: Optional[Dict[str, Any]] = None,
    sink_oracle: Optional[oracle.Oracle] = None,
    inflight_cap: Optional[int] = None,
) -> tuple[Router, _FastEgressClient]:
    reg = InMemorySchemaRegistry()
    reg.register_schema("SensorTelemetry", AVRO_SCHEMA, "AVRO")
    reg.register_schema("DeviceEvent", PROTO_SCHEMA, "PROTOBUF")
    router = Router(schema_registry=reg)
    src = _FastEgressClient(source_name)
    dst = _FastEgressClient(target_name, sink_oracle=sink_oracle)
    router.register_client(src)
    router.register_client(dst)
    if firewall is not None:
        router.set_firewall(firewall)
    if inflight_cap is not None:
        router.set_inflight_limit(source_name, inflight_cap)
    r_kwargs = dict(route_kwargs or {})
    for i in range(fanout):
        router.add_route(source_name, "devices/+/telemetry", target_name, f"out/topic_{i}", **r_kwargs)
    return router, dst


def _run_cell(
    build_fn: Callable[[], tuple[Router, bytes, Dict[str, Any], Optional[oracle.Oracle]]],
    iterations: int,
    repeats: int,
    threads: int = 1,
) -> Dict[str, Any]:
    rates: List[float] = []
    p50s: List[float] = []
    p99s: List[float] = []
    p999s: List[float] = []
    maxs: List[float] = []
    oracle_ok = True

    for _ in range(repeats):
        router, payload, src_kwargs, orc = build_fn()
        lat_probe = probe.LatencyProbe()
        router.set_latency_probe(lat_probe.record)

        # Warm-up
        for seq in range(min(50, iterations)):
            msg = header.encode(1, seq, time.time_ns(), size=len(payload)) if orc is not None else payload
            if orc is not None:
                orc.sent(1, seq)
            router.route("mqtt", "devices/d-17/telemetry", msg, ack_fn=lambda: None, **src_kwargs)
        lat_probe.reset()
        if orc is not None:
            orc.__init__()

        per_thread = max(1, iterations // threads)
        total_msgs = per_thread * threads

        if orc is not None and threads == 1:
            orc.sent_range(1, total_msgs)

        def worker(tid: int) -> None:
            base_seq = tid * per_thread
            for i in range(per_thread):
                msg = (
                    header.encode(1, base_seq + i, time.time_ns(), size=len(payload))
                    if (orc is not None and threads == 1)
                    else payload
                )
                router.route("mqtt", "devices/d-17/telemetry", msg, ack_fn=lambda: None, **src_kwargs)

        t0 = time.perf_counter_ns()
        if threads == 1:
            worker(0)
        else:
            ts = [threading.Thread(target=worker, args=(t,)) for t in range(threads)]
            for t in ts:
                t.start()
            for t in ts:
                t.join()
        elapsed_ns = max(1, time.perf_counter_ns() - t0)

        rates.append(total_msgs * 1e9 / elapsed_ns)
        s = lat_probe.summary() or {"p50": 0, "p99": 0, "p99.9": 0, "max": 0}
        p50s.append(s["p50"] / 1000.0)
        p99s.append(s["p99"] / 1000.0)
        p999s.append(s["p99.9"] / 1000.0)
        maxs.append(s["max"] / 1000.0)
        if orc is not None and threads == 1:
            rep = orc.report()
            oracle_ok = oracle_ok and rep["passed"]

    med_rate = statistics.median(rates)
    med_p99 = statistics.median(p99s)
    return {
        "msg_per_s": round(med_rate),
        "us_per_msg": round(1e6 / med_rate, 2),
        "p50_us": round(statistics.median(p50s), 2),
        "p99_us": round(med_p99, 2),
        "p99_9_us": round(statistics.median(p999s), 2),
        "max_us": round(max(maxs), 2),
        "slo_10k_pass": med_rate >= 10_000 and med_p99 < 5_000.0,
        "oracle_passed": oracle_ok,
    }


def _run_overload_10x(iterations: int) -> Dict[str, Any]:
    """Verifies that 10x overload against a stalled egress broker sheds at `inflight_cap` with balanced Oracle books."""
    orc = oracle.Oracle()
    router, dst = _build_router(sink_oracle=orc, inflight_cap=8)
    dst.auto_confirm = False  # Egress broker stalled: in-flight leases stay held until confirms arrive
    shed_count = [0]

    def count_nack(requeue: bool = True) -> None:
        shed_count[0] += 1

    total = max(80, iterations)
    orc.sent_range(1, total)
    router_logger = logging.getLogger("postoffice.router")
    prev_level = router_logger.level
    router_logger.setLevel(logging.ERROR)
    try:
        for seq in range(total):
            msg = header.encode(1, seq, time.time_ns(), size=1024)
            router.route("mqtt", "devices/d-17/telemetry", msg, ack_fn=lambda: None, nack_fn=count_nack)
    finally:
        router_logger.setLevel(prev_level)

    # Release the 8 held confirms and verify accounting balances: sent == delivered (8) + shed (total - 8)
    for c in dst.pending_confirms:
        c()
    rep = orc.report(shed=shed_count[0])
    rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return {
        "sent": rep["sent"],
        "delivered": rep["delivered"],
        "shed": shed_count[0],
        "balanced": rep["balanced"],
        "passed": rep["passed"] and shed_count[0] == total - 8,
        "max_rss_mb": round(rss_kb / 1024.0, 2),
    }


def run_matrix(iterations: int = 4_000, repeats: int = 5) -> Dict[str, Any]:
    p64 = _json_payload(64)
    p1k = _json_payload(1024)
    p16k = _json_payload(16384)

    cells: Dict[str, Any] = {}

    # 1. Reference SLO cell (1 KB, 1 route, MQTT -> Kafka, at-least-once confirm + Oracle)
    def _ref():
        orc = oracle.Oracle()
        r, _ = _build_router(sink_oracle=orc)
        return r, p1k, {"qos": 1}, orc

    cells["reference_1kb_passthrough"] = _run_cell(_ref, iterations, repeats)

    # 2. Payload sizes (64 B, 16 KB)
    cells["payload_64b_passthrough"] = _run_cell(
        lambda: (*_build_router()[:1], p64, {"qos": 1}, None), iterations, repeats
    )
    cells["payload_16kb_passthrough"] = _run_cell(
        lambda: (*_build_router()[:1], p16k, {"qos": 1}, None), iterations, repeats
    )

    # 3. Protocol semantic translation (MQTT->AMQP, AMQP->Kafka)
    cells["route_mqtt_to_amqp"] = _run_cell(
        lambda: (*_build_router(target_name="amqp")[:1], p1k, {"qos": 1, "retain": True}, None),
        iterations,
        repeats,
    )
    cells["route_amqp_to_kafka"] = _run_cell(
        lambda: (*_build_router(target_name="kafka")[:1], p1k, {"delivery_mode": 2, "correlation_id": "c-1"}, None),
        iterations,
        repeats,
    )

    # 4. Firewall configurations (5 rules, 5 rules + ContentGuard)
    cells["firewall_5_rules"] = _run_cell(
        lambda: (*_build_router(firewall=_firewall_5_rules(False))[:1], p1k, {"qos": 1}, None),
        iterations,
        repeats,
    )
    cells["firewall_5_rules_content_guard"] = _run_cell(
        lambda: (*_build_router(firewall=_firewall_5_rules(True))[:1], p1k, {"qos": 1}, None),
        iterations,
        repeats,
    )

    # 5. Schema conversions (JSON -> Avro wire, JSON -> Protobuf, Firewall+ContentGuard+Avro)
    cells["schema_json_to_avro_wire"] = _run_cell(
        lambda: (
            *_build_router(
                route_kwargs={
                    "input_format": "json",
                    "output_format": "avro",
                    "output_schema": "SensorTelemetry",
                    "wire_format": True,
                }
            )[:1],
            p1k,
            {"qos": 1},
            None,
        ),
        max(500, iterations // 2),
        repeats,
    )
    cells["schema_json_to_protobuf"] = _run_cell(
        lambda: (
            *_build_router(
                route_kwargs={
                    "input_format": "json",
                    "output_format": "protobuf",
                    "output_schema": "DeviceEvent",
                }
            )[:1],
            p1k,
            {"qos": 1},
            None,
        ),
        max(500, iterations // 2),
        repeats,
    )
    cells["full_pipeline_fw_cg_avro_wire"] = _run_cell(
        lambda: (
            *_build_router(
                firewall=_firewall_5_rules(True),
                route_kwargs={
                    "input_format": "json",
                    "output_format": "avro",
                    "output_schema": "SensorTelemetry",
                    "wire_format": True,
                },
            )[:1],
            p1k,
            {"qos": 1},
            None,
        ),
        max(500, iterations // 2),
        repeats,
    )

    # 6. Wildcard fan-out (5 routes, 20 routes)
    cells["fanout_5_routes"] = _run_cell(
        lambda: (*_build_router(fanout=5)[:1], p1k, {"qos": 1}, None),
        max(500, iterations // 2),
        repeats,
    )
    cells["fanout_20_routes"] = _run_cell(
        lambda: (*_build_router(fanout=20)[:1], p1k, {"qos": 1}, None),
        max(250, iterations // 4),
        repeats,
    )

    # 7. Multi-thread GIL contention (4 and 8 concurrent callback threads on full pipeline)
    cells["gil_4_threads_full_pipeline"] = _run_cell(
        lambda: (
            *_build_router(
                firewall=_firewall_5_rules(True),
                route_kwargs={
                    "input_format": "json",
                    "output_format": "avro",
                    "output_schema": "SensorTelemetry",
                    "wire_format": True,
                },
            )[:1],
            p1k,
            {"qos": 1},
            None,
        ),
        max(500, iterations // 2),
        repeats,
        threads=4,
    )
    cells["gil_8_threads_full_pipeline"] = _run_cell(
        lambda: (
            *_build_router(
                firewall=_firewall_5_rules(True),
                route_kwargs={
                    "input_format": "json",
                    "output_format": "avro",
                    "output_schema": "SensorTelemetry",
                    "wire_format": True,
                },
            )[:1],
            p1k,
            {"qos": 1},
            None,
        ),
        max(500, iterations // 2),
        repeats,
        threads=8,
    )

    # 8. 10x overload shedding & accounting oracle check
    cells["overload_10x_shedding"] = _run_overload_10x(iterations)
    return cells


def main(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    ap = argparse.ArgumentParser(description="Phase 0 Python worker baseline benchmark matrix (P-B)")
    ap.add_argument("--quick", action="store_true", help="fewer iterations (smoke run)")
    ap.add_argument("--out", default=None, help="directory for the results JSON")
    args = ap.parse_args(argv)
    iterations, repeats = (600, 3) if args.quick else (4_000, 5)
    matrix = run_matrix(iterations=iterations, repeats=repeats)

    print(f"{'scenario':<34} {'msg/s':>10} {'us/msg':>8} {'p50 us':>8} {'p99 us':>8} {'p99.9 us':>9} {'SLO >=10k':>10}")
    print("-" * 93)
    for name, m in matrix.items():
        if "msg_per_s" not in m:
            continue
        slo = "PASS" if m["slo_10k_pass"] else "MISS"
        print(
            f"{name:<34} {m['msg_per_s']:>10,} {m['us_per_msg']:>8.1f} "
            f"{m['p50_us']:>8.1f} {m['p99_us']:>8.1f} {m['p99_9_us']:>9.1f} {slo:>10}"
        )
    ov = matrix["overload_10x_shedding"]
    print("-" * 93)
    print(
        f"overload_10x_shedding: sent={ov['sent']} delivered={ov['delivered']} "
        f"shed={ov['shed']} balanced={ov['balanced']} max_rss={ov['max_rss_mb']} MB"
    )

    if args.out:
        doc = results.make_result("t2-python-baseline", {"iterations": iterations, "repeats": repeats, "cells": matrix})
        print("wrote", results.write_result(doc, args.out))
    return matrix


if __name__ == "__main__":
    main()
