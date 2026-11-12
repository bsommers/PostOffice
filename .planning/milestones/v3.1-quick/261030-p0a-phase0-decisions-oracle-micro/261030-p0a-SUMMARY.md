---
status: complete
---
# Phase 0 slice P0-1

- NATIVE_REWRITE_PLAN v3: §7 closed (user answers 2026-10-30), edge profile confirmed, JS/TS added to v1.
- `perf/` package: header (shared wire format), accounting oracle, results v1 schema, T1 micro-benchmarks.
- `tests/test_perf_oracle.py`: 17 hermetic tests (TDD: red on import, then green). Suite 519 OK, coverage 93%.
- T1 Python, this host (median of 7 x 50k, ns/op): trie match ~850 flat at 10/1k/100k filters; token bucket 208 (1 thr) / 228 (16 thr);
  DLQ check 1,188, admit+dedupe 1,552; firewall 1 rule 201, 5 rules 3,544, 20 rules 13,957, 5+content_guard 19,230, 5+ledger 3,600.
- Read: a 5-rule chain plus content guard costs ~19 us of the 100 us/msg budget at 10k msg/s, before conversion and publish.
- Next: P0-2 Rust open-loop loadgen + sink (MQTT), same header.
