# perf/: Phase 0 performance tooling

Off the data path. Plans: [`PERFORMANCE_TEST_PLAN.md`](../.planning/reviews/PERFORMANCE_TEST_PLAN.md), [`NATIVE_REWRITE_PLAN.md`](../.planning/reviews/NATIVE_REWRITE_PLAN.md) (SLOs §0.1, decisions §7).

| Module | Purpose |
|---|---|
| `header.py` | 32-byte message header `POB1 \| producer u32 \| seq u64 \| send_ts_ns u64 \| crc32 u32 \| reserved u32` (big-endian). The native load generator must produce the same bytes. |
| `oracle.py` | Message-accounting oracle: lost, duplicated, reordered, corrupted, latency; checks `sent = delivered + dlq + shed + dropped + in_flight`. |
| `results.py` | Results document `postoffice.perf.result/v1` with git SHA and environment. |
| `micro.py` | T1 micro-benchmarks of the Python components (trie, token bucket, DLQ budget, firewall chains). |

```bash
PYTHONPATH=src .venv/bin/python -m perf.micro [--quick] [--out perf/results]
PYTHONPATH=src .venv/bin/python -m unittest tests/test_perf_oracle.py
```

`perf/results/` is git-ignored. Only reviewed baselines go into `docs/BENCHMARKS.md`.
