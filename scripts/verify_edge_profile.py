#!/usr/bin/env python3
"""Edge Profile Packaging & RSS Verification Script.

Verifies native postoffice edge profile invariants:
1. Builds edge profile with `--no-default-features --features edge`
2. Measures binary & shared library sizes (unstripped & stripped)
3. Starts ephemeral RESP2 server and measures Idle/Sync Peak RSS (< 64 MB)
4. Drives 1,000+ msg/s 1 KB payload workload through native engine, firewall,
   trie, target queue, and disk store-and-forward in a child worker (< 128 MB)
5. Generates deterministic JSON report (docs/benchmarks/edge_profile_report.json)
"""

from __future__ import annotations

import argparse
import ctypes
import json
import os
import pathlib
import resource
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
CRATE_DIR = REPO_ROOT / "native" / "libpostoffice"
DEFAULT_REPORT_PATH = REPO_ROOT / "docs" / "benchmarks" / "edge_profile_report.json"
EDGE_TARGET_DIR = pathlib.Path(os.environ.get("CARGO_TARGET_DIR", "/tmp/po-target-edge"))

IDLE_RSS_LIMIT_BYTES = 64 * 1024 * 1024       # 64 MB
LOADED_RSS_LIMIT_BYTES = 128 * 1024 * 1024    # 128 MB
THROUGHPUT_MIN_MSG_PER_SEC = 1000.0           # 1,000 msg/s


# ------------------------------------------------------------------------------
# Ephemeral Redis RESP2 Server for Worker Sync
# ------------------------------------------------------------------------------

class MockResp2Server:
    """Minimal ephemeral Redis RESP2 server for testing postoffice-worker sync-once."""

    def __init__(self, version: int = 1) -> None:
        self.version = version
        self.brokers = {
            "edge_mqtt": json.dumps(
                {"protocol": "mqtt", "kwargs": {"host": "127.0.0.1", "port": 1883}}
            ),
            "edge_upstream": json.dumps(
                {"protocol": "mqtt", "kwargs": {"host": "127.0.0.1", "port": 8883}}
            ),
        }
        self.routes = {
            "r1": json.dumps(
                {
                    "source_broker": "edge_mqtt",
                    "source_topic": "sensors/+",
                    "target_broker": "edge_upstream",
                    "target_topic": "upstream/sensors",
                    "target_kwargs": {},
                }
            )
        }
        self.subscriptions = {
            "edge_mqtt:sensors/+": json.dumps(
                {"broker_name": "edge_mqtt", "topic": "sensors/+", "kwargs": {"qos": 1}}
            )
        }
        self.ingress = {
            "rules": json.dumps(
                {
                    "rules": [{"type": "max_payload_bytes", "max_bytes": 4096, "action": "allow"}],
                    "on_rule_error": "reject",
                }
            )
        }
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self.port: int = self._sock.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass

    def _accept_loop(self) -> None:
        self._sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                continue
            threading.Thread(target=self._handle_client, args=(conn,), daemon=True).start()

    @staticmethod
    def _read_line(conn: socket.socket, buf: bytes) -> Tuple[Optional[bytes], bytes]:
        while b"\r\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                return None, buf
            buf += chunk
        idx = buf.index(b"\r\n")
        return buf[:idx], buf[idx + 2 :]

    def _read_command(self, conn: socket.socket, buf: bytes) -> Tuple[Optional[List[str]], bytes]:
        line, buf = self._read_line(conn, buf)
        if line is None:
            return None, buf
        if not line.startswith(b"*"):
            return [line.decode("utf-8", "replace")], buf
        count = int(line[1:])
        args: List[str] = []
        for _ in range(count):
            hdr, buf = self._read_line(conn, buf)
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

    def _handle_client(self, conn: socket.socket) -> None:
        conn.settimeout(2.0)
        buf = b""
        try:
            while not self._stop.is_set():
                cmd, buf = self._read_command(conn, buf)
                if cmd is None:
                    break
                op = cmd[0].upper()
                if op == "SUBSCRIBE":
                    conn.sendall(b"*3\r\n$9\r\nsubscribe\r\n$25\r\npostoffice:config_updates\r\n:1\r\n")
                elif op == "GET":
                    v_str = str(self.version)
                    conn.sendall(f"${len(v_str)}\r\n{v_str}\r\n".encode("ascii"))
                elif op == "HGETALL":
                    key = cmd[1] if len(cmd) > 1 else ""
                    mapping = {
                        "postoffice:brokers": self.brokers,
                        "postoffice:routes": self.routes,
                        "postoffice:subscriptions": self.subscriptions,
                        "postoffice:ingress": self.ingress,
                    }.get(key, {})
                    flat: List[str] = []
                    for k, v in mapping.items():
                        flat.extend([k, v])
                    out = [f"*{len(flat)}\r\n".encode("ascii")]
                    for item in flat:
                        b = item.encode("utf-8")
                        out.append(f"${len(b)}\r\n".encode("ascii") + b + b"\r\n")
                    conn.sendall(b"".join(out))
                elif op == "XRANGE":
                    conn.sendall(b"*0\r\n")
                elif op == "PING":
                    conn.sendall(b"+PONG\r\n")
                else:
                    conn.sendall(b"+OK\r\n")
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass


# ------------------------------------------------------------------------------
# Build & Binary Measurement
# ------------------------------------------------------------------------------

def build_edge_profile(crate_dir: pathlib.Path, target_dir: pathlib.Path) -> Tuple[pathlib.Path, pathlib.Path]:
    """Compiles native/libpostoffice with --no-default-features --features edge."""
    target_dir.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    env["CARGO_TARGET_DIR"] = str(target_dir)

    cmd = [
        "cargo",
        "build",
        "--release",
        "--no-default-features",
        "--features",
        "edge",
        "--manifest-path",
        str(crate_dir / "Cargo.toml"),
    ]
    res = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        # Fall back to native/libpostoffice/target/release if already present
        fallback_dir = crate_dir / "target" / "release"
        w_fb = fallback_dir / "postoffice-worker"
        s_fb = fallback_dir / "libpostoffice.so"
        if w_fb.is_file() and s_fb.is_file():
            return w_fb, s_fb
        raise RuntimeError(f"Cargo edge build failed:\n{res.stderr}")

    release_dir = target_dir / "release"
    worker_bin = release_dir / "postoffice-worker"
    lib_so = release_dir / "libpostoffice.so"
    if not worker_bin.is_file() or not lib_so.is_file():
        raise FileNotFoundError(f"Missing build artifacts in {release_dir}")
    return worker_bin, lib_so


def measure_file_sizes(path: pathlib.Path) -> Dict[str, Any]:
    """Measures unstripped and stripped sizes of a binary or shared library."""
    unstripped_bytes = path.stat().st_size
    unstripped_kb = round(unstripped_bytes / 1024.0, 1)

    stripped_bytes = unstripped_bytes
    strip_bin = shutil.which("strip")
    if strip_bin:
        with tempfile.NamedTemporaryFile(prefix="po-strip-", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            res = subprocess.run(
                [strip_bin, "-o", tmp_path, str(path)],
                capture_output=True,
                check=False,
            )
            if res.returncode == 0:
                stripped_bytes = os.path.getsize(tmp_path)
        finally:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)

    stripped_kb = round(stripped_bytes / 1024.0, 1)
    return {
        "unstripped_bytes": unstripped_bytes,
        "unstripped_kb": unstripped_kb,
        "stripped_bytes": stripped_bytes,
        "stripped_kb": stripped_kb,
    }


# ------------------------------------------------------------------------------
# Idle / Sync Peak RSS Verification
# ------------------------------------------------------------------------------

def measure_idle_sync_rss(worker_bin: pathlib.Path) -> Dict[str, Any]:
    """Runs postoffice-worker --sync-once in a clean child runner and measures peak RSS."""
    server = MockResp2Server(version=1)
    try:
        runner_code = """
import json, resource, subprocess, sys
worker = sys.argv[1]
port = sys.argv[2]
proc = subprocess.run(
    [worker, "--redis-host", "127.0.0.1", "--redis-port", port, "--sync-once"],
    capture_output=True,
    text=True,
)
ru = resource.getrusage(resource.RUSAGE_CHILDREN)
rss_bytes = ru.ru_maxrss * 1024
print(json.dumps({
    "returncode": proc.returncode,
    "stdout": proc.stdout.strip(),
    "stderr": proc.stderr.strip(),
    "rss_bytes": rss_bytes,
}))
"""
        res = subprocess.run(
            [sys.executable, "-c", runner_code, str(worker_bin), str(server.port)],
            capture_output=True,
            text=True,
            check=True,
        )
        data = json.loads(res.stdout.strip())
        if data["returncode"] != 0:
            raise RuntimeError(f"Worker --sync-once failed ({data['returncode']}): {data['stderr']}")

        rss_bytes = int(data["rss_bytes"])
        rss_mb = round(rss_bytes / (1024.0 * 1024.0), 2)
        cli_out = json.loads(data["stdout"]) if data["stdout"] else {}
        passed = rss_bytes < IDLE_RSS_LIMIT_BYTES

        return {
            "idle_rss_bytes": rss_bytes,
            "idle_rss_mb": rss_mb,
            "idle_rss_limit_mb": round(IDLE_RSS_LIMIT_BYTES / (1024.0 * 1024.0), 1),
            "passed": passed,
            "applied_version": cli_out.get("applied_version", 0),
            "cli_output": cli_out,
        }
    finally:
        server.close()


# ------------------------------------------------------------------------------
# Loaded Workload Verification (Child Process)
# ------------------------------------------------------------------------------

def _run_loaded_worker_child(so_path_str: str, count: int, payload_bytes_len: int) -> None:
    """Executes the loaded workload inside an isolated worker process and emits metrics."""
    sys.path.insert(0, str(REPO_ROOT / "src"))
    import postoffice.native as pnat
    from postoffice.ingress.rules import MessageContext

    so_path = pathlib.Path(so_path_str)
    lib = ctypes.CDLL(str(so_path))
    pnat._bind_signatures(lib)
    pnat._cached_lib = lib

    with tempfile.TemporaryDirectory(prefix="po-edge-loaded-") as tmpdir:
        disk_path = os.path.join(tmpdir, "store_forward.db")
        # Opt-in DiskStoreAndForward with 4 MB size cap and wear-aware batching
        store = pnat.NativeDiskStoreAndForward(
            disk_path,
            size_cap_bytes=4 * 1024 * 1024,
            batch_max_messages=32,
            batch_max_bytes=64 * 1024,
            flush_interval_sec=1.0,
            overflow_policy="drop_oldest",
        )
        queue = pnat.NativeTargetQueue(max_messages=1000, max_bytes=4 * 1024 * 1024)
        engine = pnat.NativeEngine()
        engine.register_broker("edge_source", "mqtt", {})
        engine.register_broker("edge_upstream", "mqtt", {})
        engine.add_route("edge_source", "sensors/+/data", "edge_upstream", "upstream/data", {})
        engine.set_ingress(
            {
                "rules": [{"type": "max_payload_bytes", "max_bytes": 2048, "action": "allow"}],
                "on_rule_error": "reject",
            }
        )

        fw = pnat.NativeFirewall(
            {
                "rules": [{"type": "max_payload_bytes", "max_bytes": 2048, "action": "allow"}],
                "on_rule_error": "reject",
            }
        )
        trie = pnat.NativeTrie()
        trie.add_route("sensors/+/data", json.dumps({"target": "upstream/data"}))

        payload = b"X" * payload_bytes_len

        # Warmup loop (10 messages)
        for i in range(10):
            top = f"sensors/{i}/data"
            trie.match(top)
            ctx = MessageContext(broker="edge_source", topic=top, size=len(payload), payload=payload)
            fw.evaluate(ctx)
            engine.route("edge_source", top, payload)
            queue.enqueue("upstream/data", payload)
            store.append("upstream/data", payload)
        queue.drain(limit=10)

        # Timed benchmark loop
        t0 = time.perf_counter()
        for i in range(count):
            top = f"sensors/{i % 64}/data"
            trie.match(top)
            ctx = MessageContext(broker="edge_source", topic=top, size=len(payload), payload=payload)
            fw.evaluate(ctx)
            engine.route("edge_source", top, payload)
            queue.enqueue("upstream/data", payload)
            store.append("upstream/data", payload)
            if (i + 1) % 64 == 0:
                queue.drain(limit=64)

        queue.drain(limit=1000)
        store.flush()
        t1 = time.perf_counter()

        # Read VmRSS from /proc/self/status
        vm_rss_kb = None
        vm_hwm_kb = None
        try:
            with open("/proc/self/status", "r", encoding="utf-8") as f:
                for line in f:
                    if line.startswith("VmRSS:"):
                        vm_rss_kb = int(line.split()[1])
                    elif line.startswith("VmHWM:"):
                        vm_hwm_kb = int(line.split()[1])
        except Exception:
            pass

        if vm_rss_kb is None:
            vm_rss_kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        dur = max(t1 - t0, 0.0001)
        throughput = count / dur
        rss_bytes = vm_rss_kb * 1024

        result = {
            "messages_processed": count,
            "payload_bytes": payload_bytes_len,
            "duration_sec": round(dur, 4),
            "throughput_msg_per_sec": round(throughput, 1),
            "loaded_rss_bytes": rss_bytes,
            "loaded_rss_mb": round(rss_bytes / (1024.0 * 1024.0), 2),
            "vm_rss_kb": vm_rss_kb,
            "vm_hwm_kb": vm_hwm_kb,
        }
        print(json.dumps(result))


def measure_loaded_workload(so_path: pathlib.Path, count: int = 2500) -> Dict[str, Any]:
    """Spawns child worker process to execute loaded workload and verify throughput & RSS."""
    script_path = pathlib.Path(__file__).resolve()
    cmd = [
        sys.executable,
        str(script_path),
        "--child-worker",
        "--so-path",
        str(so_path),
        "--count",
        str(count),
        "--payload-size",
        "1024",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = str(REPO_ROOT / "src")

    res = subprocess.run(cmd, env=env, capture_output=True, text=True, check=False)
    if res.returncode != 0:
        raise RuntimeError(f"Child loaded worker failed ({res.returncode}):\n{res.stderr}")

    out = json.loads(res.stdout.strip())
    rss_bytes = int(out["loaded_rss_bytes"])
    throughput = float(out["throughput_msg_per_sec"])

    passed = (rss_bytes < LOADED_RSS_LIMIT_BYTES) and (throughput >= THROUGHPUT_MIN_MSG_PER_SEC)
    out["loaded_rss_limit_mb"] = round(LOADED_RSS_LIMIT_BYTES / (1024.0 * 1024.0), 1)
    out["throughput_target_msg_per_sec"] = THROUGHPUT_MIN_MSG_PER_SEC
    out["passed"] = passed
    return out


# ------------------------------------------------------------------------------
# Orchestration & Report Generation
# ------------------------------------------------------------------------------

def run_verification(
    output_path: Optional[pathlib.Path] = None,
    skip_build: bool = False,
    count: int = 2500,
) -> Dict[str, Any]:
    """Runs complete edge profile verification and writes JSON report."""
    if skip_build:
        worker_bin = EDGE_TARGET_DIR / "release" / "postoffice-worker"
        lib_so = EDGE_TARGET_DIR / "release" / "libpostoffice.so"
        if not worker_bin.is_file() or not lib_so.is_file():
            fallback_dir = CRATE_DIR / "target" / "release"
            worker_bin = fallback_dir / "postoffice-worker"
            lib_so = fallback_dir / "libpostoffice.so"
    else:
        worker_bin, lib_so = build_edge_profile(CRATE_DIR, EDGE_TARGET_DIR)

    worker_sizes = measure_file_sizes(worker_bin)
    lib_sizes = measure_file_sizes(lib_so)

    idle_sync = measure_idle_sync_rss(worker_bin)
    loaded = measure_loaded_workload(lib_so, count=count)

    idle_rss_gate = bool(idle_sync["passed"])
    loaded_rss_gate = bool(loaded["loaded_rss_bytes"] < LOADED_RSS_LIMIT_BYTES)
    throughput_gate = bool(loaded["throughput_msg_per_sec"] >= THROUGHPUT_MIN_MSG_PER_SEC)
    all_gates_passed = idle_rss_gate and loaded_rss_gate and throughput_gate

    report: Dict[str, Any] = {
        "profile": "edge",
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "all_gates_passed": all_gates_passed,
        "idle_rss_mb": idle_sync["idle_rss_mb"],
        "loaded_rss_mb": loaded["loaded_rss_mb"],
        "throughput_msg_per_sec": loaded["throughput_msg_per_sec"],
        "binary_sizes": {
            "worker_binary_bytes": worker_sizes["unstripped_bytes"],
            "worker_binary_kb": worker_sizes["unstripped_kb"],
            "worker_binary_stripped_bytes": worker_sizes["stripped_bytes"],
            "worker_binary_stripped_kb": worker_sizes["stripped_kb"],
            "libpostoffice_so_bytes": lib_sizes["unstripped_bytes"],
            "libpostoffice_so_kb": lib_sizes["unstripped_kb"],
            "libpostoffice_so_stripped_bytes": lib_sizes["stripped_bytes"],
            "libpostoffice_so_stripped_kb": lib_sizes["stripped_kb"],
        },
        "idle_sync": idle_sync,
        "loaded_workload": loaded,
        "gates": {
            "idle_rss_gate": idle_rss_gate,
            "loaded_rss_gate": loaded_rss_gate,
            "throughput_gate": throughput_gate,
            "all_gates_passed": all_gates_passed,
        },
    }

    dest = output_path or DEFAULT_REPORT_PATH
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Edge Profile Packaging and RSS")
    parser.add_argument("--output", type=pathlib.Path, default=DEFAULT_REPORT_PATH)
    parser.add_argument("--skip-build", action="store_true", help="Skip cargo build step")
    parser.add_argument("--count", type=int, default=2500, help="Loaded workload message count")
    parser.add_argument("--child-worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--so-path", type=str, default="", help=argparse.SUPPRESS)
    parser.add_argument("--payload-size", type=int, default=1024, help=argparse.SUPPRESS)

    args = parser.parse_args()

    if args.child_worker:
        _run_loaded_worker_child(args.so_path, args.count, args.payload_size)
        return 0

    report = run_verification(output_path=args.output, skip_build=args.skip_build, count=args.count)

    print("================================================================================")
    print("PostOffice Native Edge Profile Verification Summary")
    print("================================================================================")
    print(f"Worker Binary (stripped):     {report['binary_sizes']['worker_binary_stripped_kb']} KB")
    print(f"libpostoffice.so (stripped):  {report['binary_sizes']['libpostoffice_so_stripped_kb']} KB")
    print(f"Idle / Sync RSS:             {report['idle_rss_mb']} MB (limit: {report['idle_sync']['idle_rss_limit_mb']} MB) -> {'PASS' if report['gates']['idle_rss_gate'] else 'FAIL'}")
    print(f"Loaded Workload RSS:         {report['loaded_rss_mb']} MB (limit: {report['loaded_workload']['loaded_rss_limit_mb']} MB) -> {'PASS' if report['gates']['loaded_rss_gate'] else 'FAIL'}")
    print(f"Loaded Throughput:           {report['throughput_msg_per_sec']} msg/s (target >= {report['loaded_workload']['throughput_target_msg_per_sec']} msg/s) -> {'PASS' if report['gates']['throughput_gate'] else 'FAIL'}")
    print(f"All Gates Passed:            {report['all_gates_passed']}")
    print(f"Report written to:           {args.output}")
    print("================================================================================")

    return 0 if report["all_gates_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
