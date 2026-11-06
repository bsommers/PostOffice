#!/usr/bin/env python3
"""
Phase 3-5 Live Containerized Broker Integration & Soak Harness.
Supports live containerized Redis (--live) and in-process hermetic mock (--hermetic).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import threading
import time
import unittest
from typing import Any, Dict, List, Optional, Tuple

import redis

# Ensure 'src' is on sys.path
REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SRC_DIR = REPO_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane
from postoffice.native import NativeEngine
from postoffice.rollout import (
    CanaryPolicy,
    CanaryRolloutController,
    RolloutMetricsSample,
)
from postoffice.router import Router
from postoffice.shadow import ShadowHarness, ShadowReport

# Alias for soak harness semantics
ShadowModeRunner = ShadowHarness

logger = logging.getLogger("live_soak")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


class MockResp2Server:
    """In-process Redis RESP2 TCP server for hermetic testing."""

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.host = host
        self.kv: Dict[str, str] = {}
        self.hashes: Dict[str, Dict[str, str]] = {}
        self.streams: Dict[str, List[Tuple[str, List[str]]]] = {}
        self.commands_seen: List[List[str]] = []
        self._all_conns: List[socket.socket] = []
        self._pubsub_conns: List[socket.socket] = []
        self._lock = threading.Lock()
        self._stop = threading.Event()

        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind((self.host, port))
        self._sock.listen(16)
        self.port = self._sock.getsockname()[1]

        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        with self._lock:
            for c in list(self._all_conns):
                try:
                    c.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                try:
                    c.close()
                except OSError:
                    pass
            self._all_conns.clear()
            self._pubsub_conns.clear()

    def _accept_loop(self) -> None:
        self._sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                continue
            t = threading.Thread(target=self._handle_conn, args=(conn,), daemon=True)
            t.start()

    @staticmethod
    def _encode_bulk(s: Optional[str], proto3: bool = False) -> bytes:
        if s is None:
            return b"_\r\n" if proto3 else b"$-1\r\n"
        b = s.encode("utf-8")
        return f"${len(b)}\r\n".encode("ascii") + b + b"\r\n"

    @classmethod
    def _encode_array(cls, items: list) -> bytes:
        out = [f"*{len(items)}\r\n".encode("ascii")]
        for item in items:
            if isinstance(item, int):
                out.append(f":{item}\r\n".encode("ascii"))
            elif isinstance(item, bytes):
                out.append(f"${len(item)}\r\n".encode("ascii") + item + b"\r\n")
            elif isinstance(item, list):
                out.append(cls._encode_array(item))
            else:
                out.append(cls._encode_bulk(str(item) if item is not None else None))
        return b"".join(out)

    def _broadcast_pubsub(self, channel: str, message: str) -> int:
        payload = self._encode_array(["message", channel, message])
        count = 0
        with self._lock:
            dead: List[socket.socket] = []
            for c in self._pubsub_conns:
                try:
                    c.sendall(payload)
                    count += 1
                except OSError:
                    dead.append(c)
            for d in dead:
                self._pubsub_conns.remove(d)
        return count

    def _handle_conn(self, conn: socket.socket) -> None:
        with self._lock:
            self._all_conns.append(conn)
        conn.settimeout(None)
        buf = b""
        proto3 = False
        try:
            while not self._stop.is_set():
                cmd, buf = self._read_command(conn, buf)
                if cmd is None:
                    break
                with self._lock:
                    self.commands_seen.append(cmd)
                op = cmd[0].upper()

                if op == "PING":
                    conn.sendall(b"+PONG\r\n")
                elif op == "HELLO":
                    proto3 = True
                    resp = (
                        b"%7\r\n"
                        b"$6\r\nserver\r\n$5\r\nredis\r\n"
                        b"$7\r\nversion\r\n$5\r\n7.2.0\r\n"
                        b"$5\r\nproto\r\n:3\r\n"
                        b"$2\r\nid\r\n:1\r\n"
                        b"$4\r\nmode\r\n$10\r\nstandalone\r\n"
                        b"$4\r\nrole\r\n$6\r\nmaster\r\n"
                        b"$7\r\nmodules\r\n*0\r\n"
                    )
                    conn.sendall(resp)
                elif op == "SUBSCRIBE":
                    chan = cmd[1]
                    with self._lock:
                        if conn not in self._pubsub_conns:
                            self._pubsub_conns.append(conn)
                    conn.sendall(self._encode_array(["subscribe", chan, 1]))
                elif op == "PUBLISH":
                    chan = cmd[1]
                    msg = cmd[2]
                    delivered = self._broadcast_pubsub(chan, msg)
                    conn.sendall(f":{delivered}\r\n".encode("ascii"))
                elif op in ("INCR", "INCRBY"):
                    key = cmd[1]
                    by = int(cmd[2]) if len(cmd) > 2 else 1
                    with self._lock:
                        val = int(self.kv.get(key, 0)) + by
                        self.kv[key] = str(val)
                    conn.sendall(f":{val}\r\n".encode("ascii"))
                elif op == "GET":
                    key = cmd[1]
                    with self._lock:
                        v = self.kv.get(key)
                    conn.sendall(self._encode_bulk(v, proto3=proto3))
                elif op == "SET":
                    key = cmd[1]
                    val = cmd[2]
                    with self._lock:
                        self.kv[key] = str(val)
                    conn.sendall(b"+OK\r\n")
                elif op == "HSET":
                    hname = cmd[1]
                    # Supports HSET key field value [field value ...]
                    with self._lock:
                        h = self.hashes.setdefault(hname, {})
                        added = 0
                        i = 2
                        while i < len(cmd) - 1:
                            f = cmd[i]
                            v = cmd[i + 1]
                            h[f] = v
                            added += 1
                            i += 2
                    conn.sendall(f":{added}\r\n".encode("ascii"))
                elif op == "HGET":
                    hname = cmd[1]
                    field = cmd[2]
                    with self._lock:
                        v = self.hashes.get(hname, {}).get(field)
                    conn.sendall(self._encode_bulk(v, proto3=proto3))
                elif op == "HDEL":
                    hname = cmd[1]
                    fields = cmd[2:]
                    rem = 0
                    with self._lock:
                        h = self.hashes.get(hname, {})
                        for f in fields:
                            if h.pop(f, None) is not None:
                                rem += 1
                    conn.sendall(f":{rem}\r\n".encode("ascii"))
                elif op == "HGETALL":
                    hname = cmd[1]
                    with self._lock:
                        h = dict(self.hashes.get(hname, {}))
                    if proto3:
                        parts = [f"%{len(h)}\r\n".encode("ascii")]
                        for k, v in h.items():
                            parts.append(self._encode_bulk(str(k)))
                            parts.append(self._encode_bulk(str(v)))
                        conn.sendall(b"".join(parts))
                    else:
                        flat: List[str] = []
                        for k, v in h.items():
                            flat.extend([k, v])
                        conn.sendall(self._encode_array(flat))
                elif op == "XADD":
                    # XADD stream_key [MAXLEN [~] count] id k1 v1 k2 v2 ...
                    stream_key = cmd[1]
                    idx = 2
                    if idx < len(cmd) and cmd[idx].upper() == "MAXLEN":
                        idx += 1
                        if idx < len(cmd) and cmd[idx] == "~":
                            idx += 1
                        idx += 1  # maxlen value
                    entry_id = cmd[idx] if idx < len(cmd) else "*"
                    idx += 1
                    if entry_id == "*":
                        entry_id = f"{int(time.time() * 1000)}-0"
                    kv_pairs: List[str] = cmd[idx:]
                    with self._lock:
                        self.streams.setdefault(stream_key, []).append((entry_id, kv_pairs))
                    conn.sendall(self._encode_bulk(entry_id))
                elif op == "XRANGE":
                    stream_key = cmd[1]
                    with self._lock:
                        raw_entries = list(self.streams.get(stream_key, []))
                    resp_entries = []
                    for eid, kv_list in raw_entries:
                        resp_entries.append([eid, kv_list])
                    conn.sendall(self._encode_array(resp_entries))
                elif op == "FLUSHALL":
                    with self._lock:
                        self.kv.clear()
                        self.hashes.clear()
                        self.streams.clear()
                    conn.sendall(b"+OK\r\n")
                elif op in ("DEL", "UNLINK"):
                    keys = cmd[1:]
                    rem = 0
                    with self._lock:
                        for k in keys:
                            if self.kv.pop(k, None) is not None:
                                rem += 1
                            if self.hashes.pop(k, None) is not None:
                                rem += 1
                            if self.streams.pop(k, None) is not None:
                                rem += 1
                    conn.sendall(f":{rem}\r\n".encode("ascii"))
                elif op == "CONFIG":
                    sub = cmd[1].upper() if len(cmd) > 1 else ""
                    if sub == "GET":
                        param = cmd[2] if len(cmd) > 2 else ""
                        conn.sendall(self._encode_array([param, "pubsub 33554432 8388608 60"]))
                    else:
                        conn.sendall(b"+OK\r\n")
                elif op == "CLIENT":
                    sub = cmd[1].upper() if len(cmd) > 1 else ""
                    if sub == "LIST":
                        conn.sendall(self._encode_bulk(""))
                    elif sub == "KILL":
                        conn.sendall(b":1\r\n")
                    else:
                        conn.sendall(b"+OK\r\n")
                else:
                    conn.sendall(b"+OK\r\n")
        except OSError:
            pass
        finally:
            with self._lock:
                if conn in self._all_conns:
                    self._all_conns.remove(conn)
                if conn in self._pubsub_conns:
                    self._pubsub_conns.remove(conn)
            try:
                conn.close()
            except OSError:
                pass

    @staticmethod
    def _read_line(conn: socket.socket, buf: bytes) -> Tuple[Optional[bytes], bytes]:
        while b"\r\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                return None, buf
            buf += chunk
        idx = buf.index(b"\r\n")
        return buf[:idx], buf[idx + 2 :]

    @classmethod
    def _read_command(cls, conn: socket.socket, buf: bytes) -> Tuple[Optional[List[str]], bytes]:
        line, buf = cls._read_line(conn, buf)
        if line is None:
            return None, buf
        if not line.startswith(b"*"):
            return [line.decode("utf-8", "replace")], buf
        count = int(line[1:])
        args: List[str] = []
        for _ in range(count):
            hdr, buf = cls._read_line(conn, buf)
            if hdr is None or not hdr.startswith(b"$"):
                return None, buf
            length = int(hdr[1:])
            while len(buf) < length + 2:
                chunk = conn.recv(4096)
                if not chunk:
                    return None, buf
                buf += chunk
            args.append(buf[:length].decode("utf-8", "replace"))
            buf = buf[length + 2 :]
        return args, buf


class DockerContainerManager:
    """Manages ephemeral Docker containers for live integration & soak runs."""

    def __init__(self, redis_port: int = 16379, container_name: str = "postoffice-live-soak-redis") -> None:
        self.redis_port = redis_port
        self.container_name = container_name
        self.started = False

    @staticmethod
    def is_docker_available() -> bool:
        if not shutil.which("docker"):
            return False
        try:
            res = subprocess.run(
                ["docker", "info"],
                capture_output=True,
                text=True,
                timeout=5,
                check=False,
            )
            return res.returncode == 0
        except Exception:
            return False

    def start(self) -> None:
        self.cleanup()
        cmd = [
            "docker",
            "run",
            "-d",
            "--rm",
            "--name",
            self.container_name,
            "-p",
            f"127.0.0.1:{self.redis_port}:6379",
            "redis:7.2-alpine",
        ]
        logger.info(f"Starting ephemeral container: {' '.join(cmd)}")
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
        if proc.returncode != 0:
            raise RuntimeError(f"Failed to start Redis container: {proc.stderr}")
        self.started = True
        self._wait_ready()

    def _wait_ready(self, timeout: float = 15.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                r = redis.Redis(host="127.0.0.1", port=self.redis_port, decode_responses=True)
                if r.ping():
                    logger.info(f"Redis container ready on 127.0.0.1:{self.redis_port}")
                    return
            except Exception:
                time.sleep(0.15)
        raise TimeoutError(f"Redis container on 127.0.0.1:{self.redis_port} failed to become ready")

    def cleanup(self) -> None:
        if not shutil.which("docker"):
            return
        logger.info(f"Cleaning up container {self.container_name}...")
        subprocess.run(
            ["docker", "rm", "-f", self.container_name],
            capture_output=True,
            check=False,
        )
        self.started = False


def calc_percentiles(latencies: List[float]) -> Dict[str, float]:
    """Computes p50, p95, p99, and p99.9 percentiles in microseconds."""
    if not latencies:
        return {"p50": 0.0, "p95": 0.0, "p99": 0.0, "p99_9": 0.0}
    sorted_lats = sorted(latencies)
    n = len(sorted_lats)

    def pct(p: float) -> float:
        k = (n - 1) * (p / 100.0)
        f = int(k)
        c = min(f + 1, n - 1)
        d0 = sorted_lats[f] * (c - k)
        d1 = sorted_lats[c] * (k - f)
        return round(d0 + d1, 2)

    return {
        "p50": pct(50.0),
        "p95": pct(95.0),
        "p99": pct(99.0),
        "p99_9": pct(99.9),
    }


def run_integration_suites(redis_target: str) -> Dict[str, str]:
    """Runs tests/integration/test_redis_convergence.py and test_live_broker_soak.py."""
    logger.info("Executing live integration test suites...")
    suite_results: Dict[str, str] = {}
    env = os.environ.copy()
    env["POSTOFFICE_INTEGRATION"] = "1"
    env["POSTOFFICE_TEST_REDIS"] = redis_target
    env["PYTHONPATH"] = str(SRC_DIR)

    for test_module in ("tests.integration.test_redis_convergence", "tests.integration.test_live_broker_soak"):
        short_name = test_module.split(".")[-1]
        cmd = [sys.executable, "-m", "unittest", test_module]
        logger.info(f"Running {short_name}...")
        proc = subprocess.run(
            cmd,
            env=env,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
        if proc.returncode == 0:
            suite_results[short_name] = "PASS"
            logger.info(f"{short_name}: PASS")
        else:
            suite_results[short_name] = f"FAIL (exit {proc.returncode})"
            logger.error(f"{short_name} FAILED:\n{proc.stderr}\n{proc.stdout}")
            raise RuntimeError(f"Integration suite {short_name} failed:\n{proc.stderr}")

    return suite_results


def build_soak_message_fleet(message_count: int) -> List[Dict[str, Any]]:
    """Synthesizes a mixed-fleet message workload for shadow soak & performance benchmarking."""
    messages: List[Dict[str, Any]] = []
    base_ts = 1_700_000_000_000_000_000

    for i in range(message_count):
        mod = i % 10
        now_ns = base_ts + (i * 10_000)

        if mod in (0, 1, 2, 3):
            # Telemetry JSON -> Avro route
            messages.append(
                {
                    "source_broker": "mqtt_edge",
                    "topic": f"sensors/s{mod}/temp",
                    "payload": json.dumps({"device": f"s{mod}", "temp": 20.0 + (i % 50) * 0.1}).encode("utf-8"),
                    "kwargs": {"qos": 1},
                    "now_ns": now_ns,
                }
            )
        elif mod in (4, 5):
            # Alert JSON -> Protobuf route
            messages.append(
                {
                    "source_broker": "mqtt_edge",
                    "topic": f"alerts/zone{mod}",
                    "payload": json.dumps({"code": f"EVT_{i}", "severity": (i % 5) + 1}).encode("utf-8"),
                    "kwargs": {"qos": 1},
                    "now_ns": now_ns,
                }
            )
        elif mod == 6:
            # Firewall drop: Denied topic shape
            messages.append(
                {
                    "source_broker": "mqtt_edge",
                    "topic": "sensors/banned/temp",
                    "payload": b'{"device":"banned","temp":99.9}',
                    "kwargs": {},
                    "now_ns": now_ns,
                }
            )
        elif mod == 7:
            # DLQ error: Schema mismatch (malformed string for float)
            messages.append(
                {
                    "source_broker": "mqtt_edge",
                    "topic": "sensors/s1/temp",
                    "payload": b'{"device":"s1","temp":"NOT_A_DOUBLE"}',
                    "kwargs": {},
                    "now_ns": now_ns,
                }
            )
        elif mod == 8:
            # Canary route
            messages.append(
                {
                    "source_broker": "mqtt_edge",
                    "topic": f"canary/dev_{i % 100}",
                    "payload": json.dumps({"seq": i, "data": "canary_payload"}).encode("utf-8"),
                    "kwargs": {},
                    "now_ns": now_ns,
                }
            )
        else:
            # Passthrough route
            messages.append(
                {
                    "source_broker": "mqtt_edge",
                    "topic": f"passthrough/stream_{i % 10}",
                    "payload": f"passthrough_data_{i}".encode("utf-8"),
                    "kwargs": {},
                    "now_ns": now_ns,
                }
            )

    return messages


def configure_soak_topology(harness: ShadowHarness) -> None:
    """Configures schemas, ingress rules, routes, and DLQ in the shadow harness."""
    avro_schema = json.dumps(
        {
            "type": "record",
            "name": "SensorReading",
            "fields": [
                {"name": "device", "type": "string"},
                {"name": "temp", "type": "double"},
            ],
        }
    )
    proto_schema = json.dumps(
        {
            "name": "AlertMsg",
            "fields": [
                {"name": "code", "type": "string", "number": 1},
                {"name": "severity", "type": "int64", "number": 2},
            ],
        }
    )

    harness.apply_action(
        {
            "action": "set_dlq",
            "target_broker": "dlq_brk",
            "topic": "system/dlq",
            "rate_per_sec": 50000.0,
            "burst": 10000,
            "dedupe_window_sec": 60.0,
            "max_payload_bytes": 1024,
        }
    )
    harness.apply_action(
        {
            "action": "register_schema",
            "name": "SensorReading",
            "schema_str": avro_schema,
            "schema_type": "AVRO",
            "version": 1,
            "compatibility": "BACKWARD",
        }
    )
    harness.apply_action(
        {
            "action": "register_schema",
            "name": "AlertMsg",
            "schema_str": proto_schema,
            "schema_type": "PROTOBUF",
            "version": 1,
            "compatibility": "BACKWARD",
        }
    )
    harness.apply_action(
        {
            "action": "set_ingress",
            "rules": [
                {
                    "type": "topic_shape",
                    "deny": ["sensors/banned/temp"],
                    "action": "drop",
                },
                {
                    "type": "content_guard",
                    "topic": "sensors/#",
                    "format": "json",
                    "max_depth": 6,
                    "action": "drop",
                },
            ],
        }
    )
    harness.apply_action(
        {
            "action": "add_route",
            "source_broker": "mqtt_edge",
            "source_topic": "sensors/+/temp",
            "target_broker": "kafka_core",
            "target_topic": "telemetry.avro",
            "kwargs": {
                "input_format": "json",
                "output_format": "avro",
                "output_schema": "SensorReading",
                "confluent_framing": True,
            },
        }
    )
    harness.apply_action(
        {
            "action": "add_route",
            "source_broker": "mqtt_edge",
            "source_topic": "alerts/#",
            "target_broker": "amqp_core",
            "target_topic": "alerts_q",
            "kwargs": {
                "input_format": "json",
                "output_format": "protobuf",
                "output_schema": "AlertMsg",
                "qos": 1,
            },
        }
    )
    harness.apply_action(
        {
            "action": "add_route",
            "source_broker": "mqtt_edge",
            "source_topic": "passthrough/#",
            "target_broker": "kafka_core",
            "target_topic": "passthrough.out",
            "kwargs": {},
        }
    )
    harness.apply_action(
        {
            "action": "add_route",
            "source_broker": "mqtt_edge",
            "source_topic": "canary/#",
            "target_broker": "kafka_core",
            "target_topic": "canary.out",
            "kwargs": {"engine": "canary"},
        }
    )


def run_soak(
    hermetic: bool = False,
    message_count: int = 5000,
    output_path: Optional[str] = None,
    redis_target: Optional[str] = None,
    run_integration_tests: bool = True,
) -> Dict[str, Any]:
    """
    Executes live/hermetic convergence, shadow soak, canary progression & performance benchmark.
    """
    out_file = (
        pathlib.Path(output_path)
        if output_path
        else REPO_ROOT / "docs" / "benchmarks" / "live_soak_results.json"
    )

    server: Optional[MockResp2Server] = None
    container_mgr: Optional[DockerContainerManager] = None
    dp: Optional[DataPlane] = None
    cp: Optional[ControlPlane] = None
    target_host = "127.0.0.1"
    target_port = 16379

    if hermetic:
        logger.info("Operating in HERMETIC in-process mode...")
        server = MockResp2Server()
        target_port = server.port
        target_str = f"{target_host}:{target_port}"
    else:
        logger.info("Operating in LIVE containerized mode...")
        if redis_target:
            target_str = redis_target
            h, p = redis_target.rsplit(":", 1)
            target_host, target_port = h, int(p)
        else:
            container_mgr = DockerContainerManager(redis_port=16379)
            container_mgr.start()
            target_str = f"{target_host}:{target_port}"

    try:
        suite_results: Dict[str, str] = {}
        if not hermetic and run_integration_tests:
            suite_results = run_integration_suites(target_str)

        # 1. Live Redis convergence drill
        logger.info(f"Synchronizing ControlPlane, DataPlane & NativeEngine against Redis at {target_str}...")
        cp = ControlPlane(redis_host=target_host, redis_port=target_port, use_streams=True)
        cp.register_broker("mqtt_edge", "mqtt", host="127.0.0.1", port=1883)
        cp.register_broker("kafka_core", "kafka", bootstrap_servers="127.0.0.1:9092")
        cp.register_broker("amqp_core", "amqp", host="127.0.0.1", port=5672)
        cp.add_route("mqtt_edge", "sensors/+/temp", "kafka_core", "telemetry.avro")
        cp.add_route("mqtt_edge", "alerts/#", "amqp_core", "alerts_q")
        cp.add_subscription("mqtt_edge", "sensors/+/temp")

        dp = DataPlane(redis_host=target_host, redis_port=target_port)
        dp._converge()

        native_eng = NativeEngine()
        sync_rep = native_eng.sync_from_redis(host=target_host, port=target_port)

        # Redis Streams XADD & XRANGE replay catch-up
        cp.add_route("mqtt_edge", "stream/catchup", "kafka_core", "stream.out")
        replayed_dp = dp.replay_stream()
        native_eng.sync_from_redis(host=target_host, port=target_port)

        convergence_report = {
            "version": int(dp._version or 0),
            "python_dataplane_synced": dp._synced_once,
            "native_engine_synced": sync_rep.get("active_routes", 0) >= 2,
            "stream_replayed_count": replayed_dp,
        }

        # 2. ShadowModeRunner Soak
        logger.info(f"Executing ShadowModeRunner parity soak ({message_count} mixed messages)...")
        harness = ShadowModeRunner(unroutable_policy="dlq")
        configure_soak_topology(harness)
        soak_fleet = build_soak_message_fleet(message_count)
        shadow_report: ShadowReport = harness.run_soak(soak_fleet)
        logger.info(
            f"Shadow soak completed: {shadow_report.total_messages} messages, "
            f"{len(shadow_report.diffs)} divergences (clean={shadow_report.is_clean})"
        )

        # 3. CanaryRolloutController progression & simulated breach rollback
        logger.info("Executing CanaryRolloutController live progression and rollback...")
        policy = CanaryPolicy(
            stages=(5, 25, 100),
            max_error_rate=0.01,
            max_lag_messages=1000,
            max_p99_latency_ms=5.0,
            min_samples=100,
        )
        controller = CanaryRolloutController(environment="prod", policy=policy, control_plane=cp)
        s0 = controller.start()
        dp._converge()
        native_eng.sync_from_redis(host=target_host, port=target_port)

        # Step to 25%
        s1 = controller.evaluate_and_step(
            RolloutMetricsSample(routed_total=500, errors_total=0, consumer_lag=10, p99_latency_ms=0.5)
        )
        dp._converge()
        native_eng.sync_from_redis(host=target_host, port=target_port)

        # Step to 100%
        s2 = controller.evaluate_and_step(
            RolloutMetricsSample(routed_total=2000, errors_total=1, consumer_lag=20, p99_latency_ms=0.8)
        )
        dp._converge()
        native_eng.sync_from_redis(host=target_host, port=target_port)

        # Simulate SLO breach: error rate 5% > 1% threshold -> triggers immediate rollback to 0%
        s3 = controller.evaluate_and_step(
            RolloutMetricsSample(routed_total=1000, errors_total=50, consumer_lag=15, p99_latency_ms=1.2)
        )
        dp._converge()
        native_eng.sync_from_redis(host=target_host, port=target_port)

        canary_report = {
            "stages_tested": [5, 25, 100],
            "promoted_stages": [s0.native_percentage, s1.native_percentage, s2.native_percentage],
            "simulated_breach_rollback": s3.status == "rolled_back" and s3.native_percentage == 0,
            "final_native_percentage": s3.native_percentage,
            "status": s3.status,
            "rollback_reason": s3.reason,
        }

        # 4. End-to-end performance & latency percentiles
        logger.info(f"Benchmarking live performance on {message_count} messages...")
        py_router = harness.py_router
        nat_engine = harness.native

        # Python performance run
        py_latencies: List[float] = []
        py_start = time.perf_counter()
        for msg in soak_fleet:
            t0 = time.perf_counter_ns()
            py_router.route(msg["source_broker"], msg["topic"], msg["payload"], **msg.get("kwargs", {}))
            t1 = time.perf_counter_ns()
            py_latencies.append((t1 - t0) / 1000.0)
        py_elapsed = time.perf_counter() - py_start
        py_throughput = round(len(soak_fleet) / py_elapsed, 2) if py_elapsed > 0 else 0.0

        # Native performance run
        nat_latencies: List[float] = []
        nat_start = time.perf_counter()
        for msg in soak_fleet:
            t0 = time.perf_counter_ns()
            nat_engine.route(
                msg["source_broker"],
                msg["topic"],
                msg["payload"],
                source_kwargs=msg.get("kwargs", {}),
            )
            t1 = time.perf_counter_ns()
            nat_latencies.append((t1 - t0) / 1000.0)
        nat_elapsed = time.perf_counter() - nat_start
        nat_throughput = round(len(soak_fleet) / nat_elapsed, 2) if nat_elapsed > 0 else 0.0

        py_pcts = calc_percentiles(py_latencies)
        nat_pcts = calc_percentiles(nat_latencies)

        perf_report = {
            "python": {
                "message_count": len(soak_fleet),
                "throughput_msg_per_sec": py_throughput,
                "elapsed_sec": round(py_elapsed, 4),
                "latency_us": py_pcts,
            },
            "native": {
                "message_count": len(soak_fleet),
                "throughput_msg_per_sec": nat_throughput,
                "elapsed_sec": round(nat_elapsed, 4),
                "latency_us": nat_pcts,
            },
            "speedup": {
                "throughput": round(nat_throughput / py_throughput, 2) if py_throughput > 0 else 1.0,
                "p50_reduction": round(py_pcts["p50"] / max(0.01, nat_pcts["p50"]), 2),
                "p99_reduction": round(py_pcts["p99"] / max(0.01, nat_pcts["p99"]), 2),
                "p99_9_reduction": round(py_pcts["p99_9"] / max(0.01, nat_pcts["p99_9"]), 2),
            },
        }

        final_results = {
            "mode": "hermetic" if hermetic else "live",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "target": target_str,
            "integration_suite": suite_results,
            "convergence": convergence_report,
            "shadow_soak": {
                "total_messages": shadow_report.total_messages,
                "divergences": len(shadow_report.diffs),
                "clean": shadow_report.is_clean,
            },
            "canary_rollout": canary_report,
            "performance": perf_report,
        }

        out_file.parent.mkdir(parents=True, exist_ok=True)
        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(final_results, f, indent=2)
        logger.info(f"Results written to {out_file}")

        return final_results

    finally:
        if dp is not None:
            dp._stop_event.set()
            dp._buffering.clear()
        if server is not None:
            server.close()
        if dp is not None:
            try:
                if getattr(dp, "pubsub", None) is not None:
                    dp.pubsub.close()
                dp.r.close()
            except Exception:
                pass
        if cp is not None:
            try:
                cp.r.close()
            except Exception:
                pass
        if container_mgr is not None:
            container_mgr.cleanup()


def main() -> None:
    parser = argparse.ArgumentParser(description="Live containerized broker integration and soak runner")
    parser.add_argument("--live", action="store_true", help="Run against ephemeral live Redis Docker container")
    parser.add_argument("--hermetic", action="store_true", help="Run against in-process mock RESP2 server")
    parser.add_argument("--messages", type=int, default=5000, help="Number of soak messages (default: 5000)")
    parser.add_argument("--output", type=str, default=None, help="Output JSON path")
    parser.add_argument("--redis-target", type=str, default=None, help="Existing Redis host:port target")
    parser.add_argument("--skip-it", action="store_true", help="Skip integration test suites")

    args = parser.parse_args()

    use_hermetic = args.hermetic
    if not args.live and not args.hermetic:
        if not DockerContainerManager.is_docker_available():
            logger.warning("Docker not available; falling back to hermetic mode")
            use_hermetic = True
        else:
            use_hermetic = False

    res = run_soak(
        hermetic=use_hermetic,
        message_count=args.messages,
        output_path=args.output,
        redis_target=args.redis_target,
        run_integration_tests=not args.skip_it and not use_hermetic,
    )

    print("\n================== SOAK BENCHMARK SUMMARY ==================")
    print(f"Mode:              {res['mode']} ({res['target']})")
    print(f"Shadow Parity:     {res['shadow_soak']['divergences']} divergences ({res['shadow_soak']['total_messages']} msgs)")
    print(f"Canary Rollback:   {res['canary_rollout']['simulated_breach_rollback']} (final: {res['canary_rollout']['final_native_percentage']}%)")
    print(f"Python Throughput: {res['performance']['python']['throughput_msg_per_sec']} msg/s (p50: {res['performance']['python']['latency_us']['p50']} µs, p99: {res['performance']['python']['latency_us']['p99']} µs)")
    print(f"Native Throughput: {res['performance']['native']['throughput_msg_per_sec']} msg/s (p50: {res['performance']['native']['latency_us']['p50']} µs, p99: {res['performance']['native']['latency_us']['p99']} µs)")
    print(f"Speedup:           {res['performance']['speedup']['throughput']}x throughput, {res['performance']['speedup']['p99_reduction']}x p99 reduction")
    print("============================================================\n")


if __name__ == "__main__":
    main()
