---
status: complete
---
# Phase 0 slice P0-4: Rust-vs-Go bake-off, MemoryGovernor, `docs/BENCHMARKS.md`, and ADR-0001

- Implemented `perf/bakeoff/rust` (Rust 1.89) and `perf/bakeoff/go` (Go 1.26) covering the identical pipeline slice: `POB1` CRC32 verification -> atomic `MemoryGovernor` (80% shed threshold) -> 5-rule `Firewall` + single-pass JSON `ContentGuard` -> `TopicTrie` wildcard match -> egress confirm + `HdrHistogram`.
- Added `perf/bakeoff.py` orchestrator and `tests/test_perf_bakeoff.py`.
- Recorded bake-off measurements (100k messages/cell) in `docs/BENCHMARKS.md` and `docs/adr/0001-native-data-plane-language.md`:
  - 1-thread full pipeline: Rust 223.0k msg/s, Go 140.1k msg/s, Python 39.7k msg/s.
  - 8-thread full pipeline: Rust 1.59M msg/s (p99.9 = 9.66 us), Go 995.3k msg/s (p99.9 = 30.21 us), Python 30.5k msg/s (p99.9 = 5,221.8 us).
  - Footprint: Rust 4.46 MB RSS / 527.9 KB binary vs Go 9.93 MB RSS / 1,764.2 KB binary vs Python 32.58 MB RSS.
  - 10x overload MemoryGovernor (`budget=10KB, shed@8KB`): both Rust and Go admit 8 messages, shed 99,992, cap peak bytes at 8,192 B, and balance Oracle books.
- Phase 0 complete. Suite: 527 Python unit tests + 9 Rust unit tests + 2 Go unit tests pass; Python coverage 94%.
