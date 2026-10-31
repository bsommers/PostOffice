---
status: complete
---
# Phase 0 slice P0-2: Rust open-loop load generator + sink (MQTT) and HdrHistogram

- Added `perf/loadgen/` Rust crate (`postoffice-loadgen`) with zero external crate dependencies (compiles offline in <1s):
  - `src/header.rs`: 32-byte big-endian `POB1` header and IEEE CRC32, verified byte-for-byte against Python `perf/header.py`.
  - `src/hist.rs`: Sub-bucket base-2 logarithmic `HdrHistogram` (~0.1% relative precision, 1 ns to 68 s) with `record_corrected` coordinated-omission backfill.
  - `src/oracle.rs`: Native message-accounting oracle matching `perf/oracle.py` (`sent = delivered + dlq + shed + dropped + in_flight`).
  - `src/mqtt.rs`: MQTT v3.1.1 wire codec, open-loop scheduled publisher (`run_publisher`), accounting sink (`run_sink`), and hermetic in-memory loopback (`run_in_memory_loopback`).
- Added `tests/test_perf_loadgen.py` (3 cross-language integration tests).
- Verification: 7 Rust unit tests + 522 Python unit tests pass; statement + branch coverage 93%.
