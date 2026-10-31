"""
Phase 0 (P0-4) Rust vs. Go Bake-Off Orchestrator (`NATIVE_REWRITE_PLAN.md` §2 & §4 Phase 0).

Builds and runs the identical pipeline slice (`MQTT -> 5-rule Firewall + ContentGuard -> TopicTrie -> Egress + MemoryGovernor`)
in Rust (`perf/bakeoff/rust`) and Go (`perf/bakeoff/go`), recording throughput, p50/p99/p99.9 latency,
RSS under 10x overload, binary size, and build time.

Run manually:
    PYTHONPATH=src .venv/bin/python -m perf.bakeoff [--messages 100000] [--out perf/results]
"""
import argparse
import json
import os
import subprocess
import time
from typing import Any, Dict, List, Optional

from perf import results

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
RUST_DIR = os.path.join(REPO_ROOT, "perf", "bakeoff", "rust")
GO_DIR = os.path.join(REPO_ROOT, "perf", "bakeoff", "go")
RUST_BIN = os.path.join(RUST_DIR, "target", "release", "postoffice-bakeoff-rust")
GO_BIN = os.path.join(GO_DIR, "postoffice-bakeoff-go")


def _build_rust() -> float:
    t0 = time.perf_counter()
    subprocess.run(["cargo", "build", "--release", "-q"], cwd=RUST_DIR, check=True)
    return round((time.perf_counter() - t0) * 1000.0, 1)


def _build_go() -> float:
    t0 = time.perf_counter()
    subprocess.run(["go", "build", "-ldflags=-s -w", "-o", GO_BIN, "."], cwd=GO_DIR, check=True)
    return round((time.perf_counter() - t0) * 1000.0, 1)


def _run_bin(bin_path: str, messages: int, build_ms: float) -> Dict[str, Any]:
    proc = subprocess.run(
        [bin_path, "--messages", str(messages)],
        capture_output=True,
        text=True,
        check=True,
    )
    data = json.loads(proc.stdout.strip())
    data["binary_bytes"] = os.path.getsize(bin_path)
    data["binary_kb"] = round(data["binary_bytes"] / 1024.0, 1)
    data["build_ms"] = build_ms
    return data


def run_bakeoff(messages: int = 100_000) -> Dict[str, Any]:
    rust_build_ms = _build_rust()
    go_build_ms = _build_go()
    return {
        "rust": _run_bin(RUST_BIN, messages, rust_build_ms),
        "go": _run_bin(GO_BIN, messages, go_build_ms),
    }


def main(argv: Optional[List[str]] = None) -> Dict[str, Any]:
    ap = argparse.ArgumentParser(description="Phase 0 Rust vs Go Bake-Off")
    ap.add_argument("--messages", type=int, default=100_000, help="messages per benchmark cell")
    ap.add_argument("--out", default=None, help="directory for results JSON")
    args = ap.parse_args(argv)

    res = run_bakeoff(messages=args.messages)
    print(f"{'lang':<6} {'cell':<24} {'msg/s':>12} {'p50 ns':>9} {'p99 ns':>9} {'p99.9 ns':>10} {'bin KB':>9} {'RSS KB':>9}")
    print("-" * 96)
    for lang in ("rust", "go"):
        info = res[lang]
        for cell_name in ("threads_1_passthrough", "threads_1_full", "threads_4_full", "threads_8_full"):
            c = info["cells"][cell_name]
            print(
                f"{lang:<6} {cell_name:<24} {c['msg_per_s']:>12,} {c['p50_ns']:>9,} "
                f"{c['p99_ns']:>9,} {c['p99_9_ns']:>10,} {info['binary_kb']:>9.1f} {info['max_rss_kb']:>9,}"
            )
        ov = info["cells"]["overload_10x"]
        print(
            f"{lang:<6} {'overload_10x':<24} sent={ov['sent']} delivered={ov['delivered']} "
            f"shed={ov['shed']} peak_gov={ov['peak_governor_bytes']}B (limit={ov['shed_limit_bytes']}B) balanced={ov['balanced']}"
        )
        print("-" * 96)

    if args.out:
        doc = results.make_result("t2-rust-vs-go-bakeoff", {"messages": args.messages, "bakeoff": res})
        print("wrote", results.write_result(doc, args.out))
    return res


if __name__ == "__main__":
    main()
