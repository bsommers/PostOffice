# Performance and Resilience Test Suite Plan

> Status: **plan**. Written 2026-10-25. Companion to [`NATIVE_REWRITE_PLAN.md`](NATIVE_REWRITE_PLAN.md) (SLOs in its §0.1) and [`DOS_PROTECTION_PLAN.md`](DOS_PROTECTION_PLAN.md). Nothing here is built yet and no number below is a measurement.

## 1. Purpose and principles

The suite answers four questions, in this order: **Is it correct under load** (no loss, no unexpected duplication, ordering as promised)? **Is it fast enough** (the SLOs)? **Does it degrade gracefully** (bounded memory, shed rather than crash)? **Does it survive abuse and failure** (torture)?

Rules that keep the numbers trustworthy:
1. **Correctness before speed.** Every performance run also runs the accounting oracle (§3). A fast run that lost messages is a failed run.
2. **Measure latency without coordinated omission.** Open-loop load at a fixed schedule; record intended send time; HdrHistogram. A closed-loop generator that slows down when the system slows down hides the tail.
3. **Load generator must not be the bottleneck.** 100k+ msg/s needs a native generator (Rust or Go), run on separate hosts from the system under test. Python only orchestrates.
4. **Reproducible.** Pinned versions, recorded host/kernel/CPU governor/cgroup limits, seeded RNG, fixed payload corpora, results as JSON with full environment metadata. Same scenario file runs against the Python and the native worker.
5. **Gate with ranges, not single numbers.** Median of N runs with a noise band; regressions beyond the band fail CI.
6. **Hermetic tier stays hermetic.** Tier 1 follows the project rule (mocks, no network). Tiers 2+ need real brokers and run in a separate CI lane and on a dedicated perf host.

## 2. Tiers

| Tier | Name | Scope | Runs | Hermetic |
|---|---|---|---|---|
| T1 | Unit / micro | Single functions and components in-process | every commit | yes |
| T2 | Component | One worker, real brokers, one route | nightly + on release branches | no |
| T3 | Scale | Cluster of workers, rising load, scaling efficiency | weekly + before release | no |
| T4 | Realistic scenarios | Workload models from `docs/use-cases/` | weekly + before release | no |
| T5 | Stress | Beyond capacity: overload, slow consumers, resource exhaustion | weekly | no |
| T6 | Torture | Chaos, faults, adversarial input, long soak | nightly (short), release (long) | no |

### T1. Unit / micro-benchmarks

Python (until retired): `unittest`-compatible timing tests with generous budgets plus `pyperf` benchmarks outside the unit suite. Native: `criterion` (Rust) / `testing.B` (Go), plus the shared conformance vectors for correctness.

| Target | Measured | Initial budget (to calibrate in Phase 0) |
|---|---|---|
| `TopicTrie.match` | ns/op by trie size (10 / 1k / 100k filters), wildcard mix | O(depth), flat vs. filter count |
| `TokenBucket.try_consume` | ns/op, 1 and 16 threads contended | far below 1 us native |
| `BudgetedDlq.check`, `Firewall.evaluate` | ns/op per rule chain length (1 / 5 / 20 rules) | chain of 5 < 5% of the 5 ms p99 budget |
| ContentGuard parse (JSON depth/size, Avro, Protobuf) | ns/op and bytes allocated vs. input size | linear time, bounded memory |
| Conversion (JSON <-> Avro <-> Protobuf, wire framing) | ns/op, allocations | tracked, regression-gated |
| `SenderLedger.record` | ns/op, memory per tracked key, eviction cost at cap | bounded memory at max keys |
| `ExponentialBackoff.next` | ns/op; distribution sanity | n/a |
| Property tests | invariants under random input: no panic, bounded alloc, idempotent `add_route`, bucket never exceeds burst, ledger size <= cap | pass at 10^6 cases |
| Fuzzing (native) | cargo-fuzz / go-fuzz on every parser and the rule loader; corpus checked in | no crash, no OOM, no hang over a 10 min CI run, 8 h weekly |
| Memory accounting | allocation counters on hot path | zero allocations per message on the pass-through path (goal) |

Gate: any micro-benchmark worse than baseline by more than its noise band fails the PR.

### T2. Component tests (one worker, real brokers)

Docker Compose rig (extends the existing broker stack: Mosquitto, NanoMQ, RabbitMQ, Kafka, Redis). Matrix of single routes and payload sizes:

| Axis | Values |
|---|---|
| Route | MQTT->Kafka, MQTT->AMQP, AMQP->Kafka, Kafka->MQTT, AMQP->MQTT, Kafka->AMQP, NanoMQ->Kafka |
| Payload | 64 B, 1 KB (reference), 16 KB, 256 KB, 1 MB (near the DLQ cap) |
| Conversion | none, JSON->Avro, Avro->JSON, Protobuf<->JSON, with/without Confluent wire format |
| QoS / ack mode | MQTT QoS 0/1/2, AMQP confirms on/off, Kafka acks 1/all |
| Fan-out | 1, 5, 20 target routes per source |
| Firewall | none, 5-rule chain, full chain with ContentGuard |

Per cell, with the open-loop generator: max sustainable rate (highest rate with p99 under SLO and zero loss), latency percentiles p50/p90/p99/p99.9/max at 50% and 90% of that rate, CPU per core, RSS, allocation rate, GC/pause time (Python: py-spy + GIL stats; native: perf, flamegraphs).

Both latency views are reported: **bridge-added** (in-process timestamps, the gated SLO) and **end-to-end** (producer timestamp -> consumer receipt, with confirms).

### T3. Scale tests

- **Worker scale-out:** 1, 2, 4, 8, 12, 16, 24 workers on the same Kafka consumer group / AMQP queue; plot aggregate msg/s. Gate: >= 85% scaling efficiency to 12 workers; the cluster reaches >= 100,000 msg/s at 1 KB with every worker >= 10,000 msg/s.
- **Route table scale:** 10 / 1k / 100k routes and wildcard filters; memory and match latency vs. size.
- **Connection scale:** 1k / 10k / 100k MQTT client connections to the source broker (broker-bound; measures the bridge's subscription overhead, not the broker).
- **Topic cardinality:** 10^3 to 10^7 distinct topics, verifying the metric-cardinality guard (D8) keeps `/metrics` size and memory flat.
- **Control plane scale:** config churn (1/s to 1,000/s route updates) while carrying traffic: no message loss, bounded apply latency, convergence after Redis reconnect.
- **Mixed-fleet scale:** Python and native workers in one cluster (Phase 3-4), same totals.
- **Memory budget scale:** run the same load on hosts with 2, 8, 64 GB RAM; verify the default 50% budget is honoured (cgroup-aware) and the shedding threshold scales with it.

### T4. Realistic scenarios

Workload models drawn from `docs/use-cases/`, each a declarative scenario file (message rate curves, size distributions, topic hierarchies, burstiness, device churn):

| Scenario | Shape | What it proves |
|---|---|---|
| Factory floor (IIoT) | 50k PLC tags at 1-10 Hz on MQTT, change-driven bursts on alarms, bridged to Kafka as Avro; shift-change surge | sustained mixed rates, wildcard routing, Avro conversion, burst absorption |
| Fleet telemetry | 1M devices, tiny messages, constant connect/disconnect churn, regional gateways | connection churn, cardinality guard, sender ledger at scale |
| Edge store-and-forward | edge worker, uplink flaps for minutes/hours, then drains backlog | bounded buffer, drain rate, ordering after recovery, flash/disk write volume |
| Order/transaction flow | AMQP orders -> Kafka events with idempotency keys, 1% poison messages, DLQ | at-least-once, duplicate rate, DLQ budget, retry caps |
| Fan-out distribution | one event stream to 20 consumers/protocols | per-route failure isolation, fan-out ack accounting |
| Day in the life | 24 h diurnal curve with two incidents (broker restart, deploy) | everything together, drift, leaks |
| Multi-tenant | 1 noisy tenant at 10x quota beside 50 normal tenants | firewall rate limits protect neighbours' p99 |

Gate: each scenario has an SLO table (loss, duplicates, p99, lag, RSS) and runs unattended with a pass/fail report.

### T5. Stress tests (beyond capacity)

| Test | Method | Pass criteria |
|---|---|---|
| Overload ramp | 1x -> 10x target load, then back | no crash, memory <= budget, shed counters match accounting, latency recovers within N seconds of load dropping |
| Slow / stalled consumer | target broker throttled to 1%, then stopped | back-pressure engages (AMQP prefetch, Kafka pause), bounded queues, no unbounded RSS growth, circuit breaker opens and half-opens |
| Large-message storm | 1 MB messages at max rate | refused/handled per firewall and DLQ cap, memory flat |
| Many small messages | 10^6 msg/s offered to one worker | graceful shedding with correct metrics |
| Poison message storm | 100% undeliverable | DLQ budget caps writes, retry caps stop redelivery loops, no CPU spin |
| Unroutable flood | 100% no matching route, all three policies | nack/dlq/drop behave; DLQ refused counters; ledger flags sender |
| Hot key | all traffic one topic / one partition | per-key limits hold, no starvation of others |
| Resource exhaustion | cap CPU (cgroup), RAM, file descriptors, disk | explicit, logged degradation rather than crash |
| Reconnect storm | 100k clients reconnect at once to the source broker | jittered backoff keeps worker stable; no thundering herd upstream |

### T6. Torture tests

Fault injection with Toxiproxy and `tc netem` (latency, loss, reorder, bandwidth, partition), `docker kill/pause`, cgroup limits, clock skew (libfaketime). Every test runs with the message-accounting oracle active.

| Class | Faults |
|---|---|
| Broker failures | kill/restart each broker mid-flight, rolling restart, split-brain, leader election in Kafka, RabbitMQ queue master move, disk-full on broker |
| Network | partition for 1 s / 30 s / 10 min, flapping at 1 Hz, 500 ms added latency, 20% packet loss, MTU black hole, half-open TCP |
| Redis / control plane | Redis down and back, failover, lost pub/sub events (verify resync), duplicated and reordered events, `clear_state` during traffic, forged/unsigned events rejected (D9) |
| Worker | `kill -9` mid-batch (no loss beyond at-least-once duplicates, offsets only advance after confirm), SIGTERM drain time, OOM-kill simulation, crash loops with backoff |
| Adversarial input | malformed JSON/Avro/Protobuf, decompression/parser bombs, deeply nested structures, invalid UTF-8, topic injection (`$SYS`, null bytes, huge topics), oversized headers, slowloris-style stalls |
| Clock | forward and backward jumps, NTP step; rate limits and expiries must use monotonic time |
| Time | **Soak:** 72 h at 70% of max rate (release gate) and 7 d for major releases; trend RSS, FDs, threads, goroutines, latency drift, log volume; **leak detection** by slope test (RSS growth < 1% over the soak after warm-up) |
| Combined chaos | randomised sequence of the above with a fixed seed (replayable), 1 h and 8 h variants |

Pass: zero unexplained message loss; duplicates only where at-least-once allows and below the stated rate; no deadlock or livelock (watchdog); memory <= budget at all times; clean recovery to baseline performance after each fault.

## 3. Shared infrastructure

- **Message-accounting oracle.** Each generated message carries `(producer_id, sequence, send_ts, checksum)`. The sink records what arrives. The oracle reports: lost (gap with no DLQ/shed/drop record), duplicated (and whether attributable to a redelivery), reordered (per key), corrupted (checksum), late. Shed, DLQ and refuse counts must reconcile with PostOffice metrics: `sent = delivered + dlq + shed + dropped + in_flight`. A run whose books do not balance fails.
- **Load generator (`perf/loadgen/`, Rust).** Open-loop, schedule-driven, many protocols, declarative scenario files (YAML), deterministic by seed, distributed mode (controller + agents), HdrHistogram output.
- **Sink / verifier** consuming from the target brokers at line rate.
- **Fault controller** exposing named faults (kill broker, partition, delay) scripted from scenario files.
- **Observability during runs:** worker `/metrics` scraped at 1 s, host metrics (node exporter), `perf`/py-spy flamegraph capture, allocation profiling (heaptrack / dhat / pprof), all stored with the result.
- **Results store:** JSON per run (scenario, git SHA, build flags, environment, raw histograms, verdict); a baseline file per scenario per host class; a generated HTML/Markdown report in `docs/benchmarks/` with trend charts.
- **Environments:** `perf-ci` (small, noisy, relative regression only), `perf-lab` (dedicated bare metal, fixed CPU frequency, isolated NIC; the source of published numbers), `edge-lab` (the real constrained device class, e.g. a Raspberry Pi 4 or the target gateway).
- **Run orchestration:** `perf/run.py` (Python) reads a scenario, provisions Compose or Kubernetes, starts agents, runs, collects, evaluates gates, tears down.

## 4. Gates summary

| Gate | When | Fails build / release if |
|---|---|---|
| G1 | every PR | T1 micro regression > noise band; conformance vectors fail; fuzz regression corpus crashes |
| G2 | nightly | T2 sustainable rate regresses > 10%, p99 > SLO, any lost message, RSS above budget |
| G3 | weekly | T3 scaling efficiency < 85%, cluster < 100k msg/s on lab hardware; T4 scenario SLO miss; T5 pass criteria miss |
| G4 | release | T6 soak (72 h) leak slope, chaos run loses messages, edge-lab profile misses its footprint |

## 5. Phasing (aligned with `NATIVE_REWRITE_PLAN.md`)

| Phase | Deliverable | When |
|---|---|---|
| **P-A: Foundations** | Message-accounting oracle, native load generator (single protocol), HdrHistogram harness, results JSON schema, Compose rig, T1 micro-benchmarks for existing Python components (TopicTrie, TokenBucket, BudgetedDlq) | Native Phase 0, week 1-2 |
| **P-B: Python baseline** | T2 matrix against the current Python worker; flamegraphs; baseline report `docs/BENCHMARKS.md`; the decision-gate numbers | Native Phase 0, week 2-3 |
| **P-C: Stress and fault harness** | Toxiproxy/netem fault controller; T5 stress tests; first T6 faults (broker kill, Redis down, worker kill) run against the Python worker so D6/D7/D8 are validated | Alongside D6-D8 |
| **P-D: Scale + scenarios** | Distributed loadgen agents; T3 scale suite; T4 scenario files for factory floor, fleet telemetry, order flow | Native Phase 1-2 |
| **P-E: Native parity** | Same suites against the native worker; mixed-fleet runs; shadow-mode comparisons feed Phase 3 exit criteria | Native Phase 3-4 |
| **P-F: Soak and chaos** | 72 h soak, randomised chaos, edge-lab runs, CI gates G2-G4 enforced, published numbers | Before first native GA |

## 6. Decisions needed

1. **Hardware for `perf-lab` and `edge-lab`:** what machines are available (cores, RAM, NIC), and which edge device is the reference?
2. **Where heavy runs execute:** a self-hosted runner, cloud burst (cost per soak run), or both?
3. **Soak duration policy:** is 72 h per release acceptable, or 24 h with 7 d reserved for majors?
4. **Published numbers:** may benchmark results be published in the repo docs, and under what reference configuration?
5. **Realistic scenarios:** can you share (anonymised) message rates, topic layouts and payload samples from real deployments to calibrate T4? Synthetic models are a fallback.
