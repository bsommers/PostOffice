# PostOffice Phase 0 Performance Benchmarks & Decision-Gate Baseline

> **Status:** Phase 0 (Slices P0-1, P0-2, P0-3) recorded 2026-10-31.
> **Plans:** [`NATIVE_REWRITE_PLAN.md`](../.planning/reviews/NATIVE_REWRITE_PLAN.md) (§0.1 SLOs, §4 Phase 0) and [`PERFORMANCE_TEST_PLAN.md`](../.planning/reviews/PERFORMANCE_TEST_PLAN.md) (T1 & T2 / P-A & P-B).

## 1. Environment & Methodology

- **Host:** Linux x86_64 (32 logical CPUs), CPython 3.13.
- **Bridge-Added Latency Probe (`Router.set_latency_probe`):** High-resolution `time.perf_counter_ns()` timer wrapping `Router.route()` from ingress callback entry (`InflightLimiter.acquire`, `RetryTracker`, `Firewall.evaluate`, `TopicTrie.match`, `SchemaConverter.convert`, `CircuitBreaker`) to egress `publish()` handoff completion.
- **Accounting Oracle (`perf/oracle.py` & `perf/loadgen/src/oracle.rs`):** 32-byte `POB1` wire header (`producer_id`, `seq`, `send_ts_ns`, `crc32`). Every run reconciles `sent == delivered + dlq + shed + dropped + in_flight` and verifies zero payload corruption.
- **Reproducibility Commands:**
  ```bash
  PYTHONPATH=src .venv/bin/python -m perf.micro --out perf/results
  PYTHONPATH=src .venv/bin/python -m perf.baseline --out perf/results
  cargo test --manifest-path perf/loadgen/Cargo.toml -q
  ```

---

## 2. Tier 1 (T1): Component Micro-Benchmarks (`perf.micro`)

Median of 7 runs $\times$ 50,000 iterations per case on 1 KB telemetry payloads (`devices/d-17/telemetry`). At the **10,000 msg/s per-worker SLO**, the total per-message time budget is **100 µs**.

| Component | Case | Median (ns/op) | Throughput (ops/s) | Share of 100 µs Budget |
|---|---|---:|---:|---:|
| `TopicTrie.match` | `filters=10` | 847 | 1,180,925 | 0.8% |
| `TopicTrie.match` | `filters=1,000` | 858 | 1,164,942 | 0.9% |
| `TopicTrie.match` | `filters=100,000` (distinct) | 849 | 1,177,398 | 0.8% |
| `TokenBucket.try_consume` | `threads=1` | 208 | 4,814,614 | 0.2% |
| `TokenBucket.try_consume` | `threads=16` (contended) | 228 | 4,381,788 | 0.2% |
| `BudgetedDlq` | `check` | 1,188 | 841,368 | 1.2% |
| `BudgetedDlq` | `admit+dedupe` (blake2b-128) | 1,552 | 644,408 | 1.6% |
| `Firewall.evaluate` | `rules=1` (`max_payload_bytes`) | 201 | 4,978,213 | 0.2% |
| `Firewall.evaluate` | `rules=5` (size + shape + 3 rate limits) | 3,544 | 282,140 | 3.5% |
| `Firewall.evaluate` | `rules=20` | 13,957 | 71,647 | 14.0% |
| `Firewall.evaluate` | `rules=5 + ContentGuard` (JSON scan + parse) | 19,230 | 52,001 | 19.2% |
| `Firewall.evaluate` | `rules=5 + SenderLedger` | 3,600 | 277,789 | 3.6% |

---

## 3. Tier 2 (P-B): Python Worker Hot-Path Baseline Matrix (`perf.baseline`)

Median of 5 runs $\times$ 4,000 messages per cell through `Router.route()` with at-least-once `ack_fn` confirmation and Prometheus metrics active.

| Scenario | Sustained Rate (msg/s) | Mean (µs/msg) | Bridge p50 (µs) | Bridge p99 (µs) | Bridge p99.9 (µs) | >= 10k msg/s & p99 < 5 ms |
|---|---:|---:|---:|---:|---:|:---:|
| **Reference SLO** (1 KB, 1 route, MQTT $\to$ Kafka, passthrough + Oracle) | **104,510** | 9.6 | 7.2 | 12.1 | 125.8 | **PASS** |
| Payload 64 B passthrough | 143,282 | 7.0 | 5.9 | 10.0 | 115.7 | PASS |
| Payload 16 KB passthrough | 144,422 | 6.9 | 5.9 | 10.1 | 124.2 | PASS |
| Route MQTT $\to$ AMQP (`qos=1, retain=True` translation) | 141,267 | 7.1 | 5.9 | 10.4 | 122.3 | PASS |
| Route AMQP $\to$ Kafka (`delivery_mode=2, correlation_id` translation) | 140,829 | 7.1 | 6.0 | 10.0 | 122.8 | PASS |
| Ingress Firewall (5 rules: size + shape + 3 rate limits) | 87,764 | 11.4 | 10.2 | 17.5 | 132.4 | PASS |
| Ingress Firewall (5 rules + JSON `ContentGuard`) | 39,667 | 25.2 | 23.6 | 39.1 | 160.7 | PASS |
| Schema Conversion: JSON $\to$ Avro (Confluent wire format) | 88,523 | 11.3 | 10.1 | 17.1 | 153.7 | PASS |
| Schema Conversion: JSON $\to$ Protobuf | 36,138 | 27.7 | 25.8 | 47.2 | 205.7 | PASS |
| **Full Pipeline** (5-rule Firewall + `ContentGuard` + JSON $\to$ Avro wire) | **33,363** | 30.0 | 28.3 | 43.5 | 209.6 | **PASS** |
| Wildcard Fan-out: 5 target routes | 56,336 | 17.8 | 16.4 | 24.3 | 158.2 | PASS |
| Wildcard Fan-out: 20 target routes | 17,489 | 57.2 | 55.5 | 66.2 | 327.8 | PASS |
| **GIL Contention: 4 callback threads** (Full Pipeline) | **31,103** | 32.1 | 28.2 | 104.5 | **10,959.8** | **p99.9 > 10 ms** |
| **GIL Contention: 8 callback threads** (Full Pipeline) | **30,483** | 32.8 | 28.4 | 103.2 | **5,221.8** | **p99.9 > 5 ms** |

### 10x Overload Shedding & Memory Verification
- **Test (`overload_10x_shedding`):** 4,000 messages (1 KB each, 500x the in-flight cap of 8) injected against a stalled egress broker (`auto_confirm=False`).
- **Result:** `sent=4000`, `delivered=8`, `shed=3992`, `balanced=True`, `passed=True`, `max_rss=32.58 MB`. Zero unbounded buffering or memory growth.

---

## 4. Phase 0 Key Findings & Bottleneck Analysis

1. **Single-Thread CPU Headroom:** On a single callback thread in-process, Python `Router.route()` achieves **104.5k msg/s** (p99 `12.1 µs`) on passthrough and **33.4k msg/s** (p99 `43.5 µs`) with the full 5-rule Firewall + JSON `ContentGuard` + Avro wire serialization enabled.
2. **Where Time Goes on the Full Pipeline (~30 µs/msg total):**
   - `ContentGuard` (streaming JSON depth/key/string scan + `json.loads` + field checks): **~15.7 µs** (52%)
   - `SchemaConverter` (JSON $\to$ `fastavro` schemaless write + Confluent 5-byte wire header): **~4.3 µs** (14%)
   - `Firewall` 5-rule chain (3 `KeyedLimiter`/`TokenBucket` locks + `TopicShape` + `MaxPayloadBytes`): **~3.5 µs** (12%)
   - `Router` dispatch + `TopicTrie.match` + `_FanoutCoordinator` + Prometheus `Counter`/`Histogram` label guards: **~6.5 µs** (22%)
3. **Double JSON Parse in `ContentGuard` + `SchemaConverter`:** When both `ContentGuard(format="json")` and `SchemaConverter(input_format="json")` are active on a route, the payload is parsed twice (`ContentGuard._parse_json` and `SchemaConverter.deserialize`).
4. **GIL Serialization Under Multi-Broker Callback Threads:** Moving from 1 thread to 4 or 8 concurrent ingress callback threads on the full pipeline yields **zero throughput gain** (`33.4k` $\to$ `31.1k` $\to$ `30.5k msg/s`) while **p99.9 latency spikes 25x–50x** (`209 µs` $\to$ `5.2 ms – 11.0 ms`) due to CPython GIL contention and lock convoys across `KeyedLimiter` and Prometheus counters.
5. **Edge Profile Gap:** CPython idle/loaded RSS sits around **32.6 MB** before loading `confluent-kafka`/`librdkafka` and `pika` worker threads, leaving minimal headroom under the `< 64 MB` idle edge target and requiring a Python runtime + native wheels on ARMv7/ARM64.
6. **Implication for Slice P0-4 (Rust vs. Go Bake-off):** Multi-core scaling inside a single worker and deterministic sub-millisecond tail latency (`p99.9`) under concurrent broker callbacks require a GIL-free hot path. Slice P0-4 will benchmark the identical pipeline slice (`MQTT -> Firewall -> Egress + Memory Governor`) in Rust and Go.
