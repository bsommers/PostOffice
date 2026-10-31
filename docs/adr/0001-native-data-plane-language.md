# ADR-0001: Native Data Plane & Core Library Language Selection (Phase 0 Decision Gate)

- **Status:** Accepted (2026-10-31)
- **Deciders:** Project Owner & Engineering
- **Context:** [`NATIVE_REWRITE_PLAN.md`](../../.planning/reviews/NATIVE_REWRITE_PLAN.md) (§0, §2, §4 Phase 0) and [`docs/BENCHMARKS.md`](../BENCHMARKS.md)

## 1. Context and Problem Statement

PostOffice's target SLOs (`NATIVE_REWRITE_PLAN.md` §0.1, confirmed 2026-10-30) require:
1. **Cluster throughput:** $\ge 100,000\text{ msg/s}$ sustained at 1 KB payload, at-least-once.
2. **Per-worker throughput:** $\ge 10,000\text{ msg/s}$ sustained with near-linear multi-core scaling.
3. **Bridge-added latency:** $p99 < 5\text{ ms}$ (ingress callback entry $\to$ egress client handoff).
4. **Memory governance:** Byte-accurate soft budget (`0.5 * host RAM` default), shedding at 80% of budget, zero OOM under $10\times$ overload.
5. **Edge profile:** ARMv7/ARM64 static binary, $< 64\text{ MB}$ idle RSS, $< 128\text{ MB}$ loaded RSS, $1,000\text{ msg/s}$ on 1 core, optional wear-aware disk store-and-forward.
6. **Polyglot client contract:** v1 support for Python, Go, Java, C#, C/C++ (via C ABI), Rust, and JS/TS.

Phase 0 built the accounting oracle (`POB1` wire header), open-loop load generator, nanosecond latency probe, Python worker baseline matrix (`perf/baseline.py`), and the Rust-vs-Go bake-off (`perf/bakeoff.py`).

## 2. Measured Evidence (`docs/BENCHMARKS.md`)

On the identical 1 KB pipeline (`POB1` CRC32 check $\to$ `MemoryGovernor` 80% check $\to$ 5-rule `Firewall` + JSON `ContentGuard` $\to$ `TopicTrie` match $\to$ egress confirm):

| Dimension | Python 3.13 | Go 1.26 | Rust 1.89 |
|---|---:|---:|---:|
| 1-Thread Full Pipeline Throughput | 39,667 msg/s | 140,063 msg/s | **222,976 msg/s** |
| 4-Thread Full Pipeline Throughput | 31,103 msg/s *(GIL serialized)* | 535,176 msg/s (3.82x) | **852,442 msg/s (3.82x)** |
| 8-Thread Full Pipeline Throughput | 30,483 msg/s *(GIL serialized)* | 995,274 msg/s (7.11x) | **1,586,661 msg/s (7.12x)** |
| 8-Thread Bridge `p99` Latency | 103.2 µs | 10.90 µs | **6.46 µs** |
| 8-Thread Bridge `p99.9` Latency | **5,221.8 µs** | 30.21 µs | **9.66 µs** |
| Peak RSS (10x Overload) | 32.58 MB | 9.93 MB | **4.46 MB** |
| Stripped Binary Size | N/A (venv + native wheels) | 1,764.2 KB | **527.9 KB** |

### Key Observations
1. **Why Python Alone Cannot Meet Multi-Thread & Edge Targets:** While single-thread Python clears 10k msg/s in isolation, multi-broker callback threads (4–8 threads) hit GIL serialization: aggregate throughput drops from `33.4k` to `30.5k msg/s` and `p99.9` latency spikes to `5.2 ms – 11.0 ms`. Furthermore, CPython + native wheels consume `32.6 MB` RSS before broker client buffers and cannot ship as a single static binary for ARMv7/ARM64 edge gateways.
2. **Rust vs. Go on the Data Plane Hot Path:**
   - Both Rust and Go scale linearly across 8 threads (`7.1x` speedup on 8 threads) and easily pass the throughput SLO.
   - **Tail Latency:** At 8 threads, Rust maintains a `9.66 µs` `p99.9` (`3.1x` tighter than Go's `30.21 µs`) with zero GC pause jitter.
   - **Memory Determinism & Edge Footprint:** Rust's peak RSS is **4.46 MB** (`2.2x` smaller than Go's `9.93 MB`, `7.3x` smaller than Python) and its stripped binary is **528 KB** (`3.3x` smaller than Go), giving massive headroom under the 64 MB edge ceiling and exact byte-level accounting for the `MemoryGovernor` without GC heap overhead.
   - **C ABI (`libpostoffice`):** Rust natively emits a zero-runtime `cdylib`/`staticlib` C ABI without spinning up a secondary garbage collector or cgo runtime inside host C/C++/C#/Java/Python processes.

## 3. Decision

We adopt the **Rust Core + Polyglot SDK** architecture defined in `NATIVE_REWRITE_PLAN.md`:
1. **Rust** for the core library (`libpostoffice`: Firewall, ContentGuard, TopicTrie, Schema Codecs, MemoryGovernor, C ABI) and the **Data Plane worker binary** (server and edge profiles).
2. **Go** where development velocity off the hot path matters (Kubernetes operator, deployment tooling, optional gRPC-to-REST gateway).
3. **Python** for the Control Plane CLI/admin scripts, conformance-vector generator (`conformance/`), benchmark orchestration (`perf/`), and Python SDK.

## 4. Consequences & Next Phase (Phase 1)

- **Phase 0 is complete.** All exit criteria in `NATIVE_REWRITE_PLAN.md` §4 Phase 0 are satisfied and verified by hermetic tests (`tests/test_perf_*.py`).
- **Next is Phase 1 (Freeze Contracts & Client Interface):**
  1. Author `api/postoffice/v1/*.proto` (`Admin`, `Observe`, `Stream`), OpenAPI spec, and `include/postoffice.h` C ABI header.
  2. Export deterministic `conformance/` JSON test vectors from the Python reference implementation covering routing, wildcard `$` rules, firewall verdicts, DLQ envelopes, and D9 control-plane signing.
