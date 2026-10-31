---
status: complete
---
# Phase 0 slice P0-3: Bridge-added latency probe, Python baseline matrix (P-B), and `docs/BENCHMARKS.md`

- Added `Router.set_latency_probe` and `POSTOFFICE_LATENCY_PROBE=1` env hook in `src/postoffice/router.py` plus `perf/probe.py` (`LatencyProbe`).
- Added `perf/baseline.py` implementing the 15-cell Python worker benchmark matrix across payload sizes, protocol translations, 5-rule Firewall + `ContentGuard`, Avro/Protobuf schema conversions, 1/5/20 wildcard fan-out, 1/4/8 thread GIL contention, and 10x stalled-egress overload shedding.
- Documented full results and bottleneck breakdown in `docs/BENCHMARKS.md`:
  - Single-thread passthrough: 104.5k msg/s (p99 12.1 us); full pipeline (5-rule FW + ContentGuard + JSON->Avro wire): 33.4k msg/s (p99 43.5 us).
  - Multi-thread GIL contention (4 and 8 threads): throughput slightly degrades (31.1k / 30.5k msg/s) while p99.9 jumps from 210 us to 5.2-11.0 ms.
  - 10x overload shedding: 4,000 sent -> 8 delivered + 3,992 shed (`balanced=True`, `max_rss=32.58 MB`).
- Added `tests/test_perf_probe.py` (4 hermetic tests). All 526 Python unit tests and 7 Rust unit tests pass; coverage 93%.
