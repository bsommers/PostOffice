# Native (Rust / Go) Re-platforming Plan

> Status: **plan v2** (decisions recorded 2026-10-25). No code changed. Nothing here has been measured yet: the review and tests ran against mocked brokers, so *Phase 0 exists to find out where Python actually tops out and to choose the language by measurement.*
> Companion: [`PERFORMANCE_TEST_PLAN.md`](PERFORMANCE_TEST_PLAN.md) (the test and benchmark suite that gates every phase).

## 0. Decisions recorded (user, 2026-10-25)

| # | Question | Answer | Consequence |
|---|---|---|---|
| 1 | Team skills | Go **and** Rust experience | Language is chosen by measurement (Phase 0 bake-off), not by ramp-up cost |
| 2 | Scale / SLOs | Cluster > 100,000 msg/s; > 10,000 msg/s per worker; p99 < 5 ms; memory ceiling scales with host, default 50% of RAM | See §0.1. Implies >= 10 workers at the target, hard per-worker byte budgets, GIL-free hot path |
| 3 | Embedded Python library | Not important; Python only needs to be able to **connect** as a client. Admin (CLI, scripts, ops) may stay Python | Drop the "embedded facade" constraint; Python becomes one client SDK plus the admin tooling |
| 4 | Edge | Both constrained edge and servers | One codebase, two build profiles (see §0.2) |
| 5 | Client interfaces | Must support Python, C, C++, Java, Go, Rust, C#, others | A language-neutral interface contract comes **before** native code (new Phase 1, §0.3) |
| 6 | Perf testing | Full suite: unit, scale, realistic scenario, stress, torture | See `PERFORMANCE_TEST_PLAN.md`; Phase 0 builds its first slice |

### 0.1 SLOs, made testable

The numbers are the user's; the definitions are my proposal and must be confirmed because they change results by an order of magnitude.

| SLO | Definition used by the test suite |
|---|---|
| Cluster throughput | >= 100,000 msg/s sustained, sum over workers, 1 KB payload, 1 source -> 1 target route, at-least-once |
| Per-worker throughput | >= 10,000 msg/s sustained per worker (implies >= 10 workers; horizontal scaling must be near-linear) |
| Latency p99 < 5 ms | **PostOffice-added latency**: ingress callback entry -> egress client-library handoff, measured in-process. A second metric, end-to-end through real brokers with confirms, is reported but not gated, because Kafka `acks=all` and broker fsync dominate it |
| Memory | Soft budget = `memory_fraction` x host RAM (default 0.5), applied as a cgroup-aware limit. The process accounts bytes in queues, DLQ buffers and caches (D8) and sheds at 80% of budget; it must never be OOM-killed under 10x overload |
| Edge profile | Proposed (needs your numbers): RSS < 64 MB idle, < 128 MB loaded, ARMv7/ARM64, static binary, no external runtime. The edge target scales the throughput SLO down, e.g. 1,000 msg/s on 1 core |

Honest read: 10k msg/s per worker with p99 < 5 ms is at or beyond what the current threaded Python design is likely to hold once conversion and firewall checks are in the path (GIL, per-message allocation, GC pauses). That is a prediction to test in Phase 0, not a result.

### 0.2 One codebase, two profiles

- **Server profile:** full adapters (Kafka, AMQP, MQTT), all schema codecs, Prometheus, multi-core.
- **Edge profile:** feature-flagged build (MQTT/NanoMQ + one upstream, JSON passthrough, minimal metrics), static musl binary, optional disk-backed store-and-forward (replaces unbounded RAM buffering when the uplink is down).

### 0.3 Polyglot client interface contract

Two kinds of "client" exist and are treated separately:

1. **Traffic clients (producers/consumers).** They should not need a PostOffice SDK at all: they keep using their native MQTT/AMQP/Kafka client in any language, and PostOffice bridges transparently. This is the main polyglot story and already works.
2. **Platform clients (admin, observability, optional direct publish/subscribe).** These need a stable, versioned, language-neutral API:
   - **Source of truth:** Protobuf IDL in `api/` (`postoffice.v1`): `Admin` (brokers, routes, firewall rules, policies), `Observe` (health, sender report, stats), optional `Stream` (publish/subscribe through the bridge, with acks).
   - **Transports:** gRPC (primary, generated clients for Python, Java, Go, C#, C++, Rust, others) plus REST/JSON via a generated gateway and OpenAPI document for scripting and for stacks without good gRPC.
   - **C ABI (`libpostoffice`)**, produced by the Rust core, for FFI from C, C++, C#, Java (Panama/JNI), Python (cffi) and others, for anyone who must embed the engine or its firewall in-process. Opaque handles, no ownership leaks, ABI-versioned.
   - **Versioning rules:** additive-only within `v1`, field numbers never reused, `buf breaking` in CI, a deprecation window of two minor releases.
   - **Conformance per SDK:** every generated or hand-written client passes the same `conformance/` vectors (admin round-trips, error codes, auth) against a live worker; a new language is "supported" only when its suite is green in CI.
   - **Control-plane transport:** Redis stays an internal coordination detail behind the `Admin` API; clients never speak raw Redis. Redis keys and events are still frozen (Phase 1) for Python/native worker coexistence.
   - **Auth:** mTLS and token auth on the API from day one (reuses D9 signing and the SecurityConfig model).

### 0.4 Where Python is still used

Admin CLI, ops scripts, schema tooling, the conformance-vector generator, the benchmark orchestrator and the Python client SDK. It is off the data path, so its speed is irrelevant.

## 1. Why consider it (and what would not justify it)

Real pressures in the current design:
- `Router.route()` runs on each client's callback thread under the GIL; CPU-heavy work (JSON/Avro/Protobuf conversion, firewall content checks, DLQ formatting) serialises across all sources.
- Memory and latency bounds are hard to guarantee in CPython (parser bombs, GC pauses, per-message object overhead). The DoS plan (D3–D8) wants *bounded* per-message cost.
- Three hand-rolled thread lifecycles (paho, pika `BlockingConnection`, confluent-kafka polling) are the source of most review defects (reconnect, confirms, offsets). Async runtimes make that structure natural.
- Deployment: a single static binary per worker is easier at edge sites than Python + venv + librdkafka wheels.

Not a reason: "Python is slow" in general. Brokers, network and Kafka dominate many bridge workloads. If Phase 0 shows the bridge is I/O-bound at the target rate, the right move is the Python fixes already planned.

## 2. Language choice

| | Go | Rust |
|---|---|---|
| Concurrency model for many connections | goroutines, simple | tokio, more ceremony |
| Broker clients | franz-go / sarama, paho.golang, amqp091-go (all mature) | rdkafka (librdkafka binding), rumqttc, lapin (good, less batteries) |
| Memory/latency determinism | GC, usually fine | no GC, strongest bounds |
| Parser safety (untrusted Avro/Protobuf/JSON) | memory-safe, GC cost | memory-safe, zero-copy, bounded by construction |
| Team velocity, hiring | faster | slower ramp |
| Embeds into Python (hot path only) | via gRPC/cgo, awkward | PyO3, natural |

**Recommendation, revised with the decisions in §0 (confirm by Phase 0 bake-off):**
- **Rust for the core and the Data Plane worker.** Reasons from your requirements, not taste: p99 < 5 ms with no GC pauses, byte-accurate memory budgets (D8) that must hold at 50% of RAM, a 64 MB-class edge profile, a static binary, and a **C ABI** that serves C, C++, C#, Java and Python clients from one core. The team has Rust experience, so the usual ramp-up objection is weak.
- **Go where it is the faster tool and off the hot path:** the Kubernetes operator or deployment tooling, an optional REST gateway, load-generator helpers. Not the data plane, unless the bake-off shows Rust adapters lagging badly.
- **Python** for admin CLI/scripts, schema tooling, conformance-vector generation, benchmark orchestration and the Python SDK.
- **Bake-off (first week of Phase 0):** implement the same slice in Rust and in Go: MQTT subscribe -> firewall (size + rate limit) -> Kafka produce with confirm, plus the memory governor. Compare throughput, p99/p99.9, RSS under 10x overload, binary size, build time and defects found. If Go is within SLO, simplicity may still win; if not, Rust is decided. Record in an ADR.

## 3. Principle: strangler fig, with the Python suite as the spec

Do not big-bang rewrite. The existing hermetic tests, plus D2–D9 tests, define behaviour. Convert them into **language-neutral conformance vectors** (JSON files: input message, route table, firewall rules, expected outputs/acks/DLQ envelope/metrics). Both implementations must pass the same vectors. Redis contracts and the DLQ envelope stay stable so Python and native workers can run **side by side** in one cluster.

## 4. Phases

### Phase 0: Measure and decide (1–2 weeks)
- Build a repeatable benchmark rig (`docker compose`: MQTT→Kafka, MQTT→AMQP, AMQP→Kafka; JSON→Avro with and without wire format; wildcard fan-out of 1/5/20 routes).
- Record per scenario: max sustained msg/s, p50/p99 latency, CPU per core, RSS, behaviour at 10× overload, GC/GIL stalls (py-spy).
- Profile where time goes: matching, conversion, publish, ack, logging.
- Build the first slice of the performance suite (`PERFORMANCE_TEST_PLAN.md` phases P-A and P-B): native load generator, latency harness with coordinated-omission correction, message-accounting oracle.
- Run the Rust-vs-Go bake-off described in §2.
- **Exit criteria / decision gate:** the SLOs in §0.1 are the pass/fail numbers (confirm the definitions and the edge profile numbers first). Proceed to native only if the Python baseline, after the planned Python fixes (T1–T4, D3–D8), misses them. Output: `docs/BENCHMARKS.md` and an ADR choosing Rust / Go / hybrid / stay-in-Python. Given the SLOs, expect the result to be "native"; the gate exists so that this is shown, not assumed.

### Phase 1: Freeze the contracts and the client interface (3 weeks)
- **Client interface (new, §0.3):** write `api/postoffice/v1/*.proto` (Admin, Observe, Stream), the OpenAPI document, the C ABI header (`postoffice.h`) and the SDK conformance suite. Generate Python first (replaces direct Redis use in `scripts/admin.py`), then Go, Java, C#, C++ and Rust clients; each must pass conformance against the Python reference server before native code exists. This validates the API design cheaply.
- Specify, versioned: Redis keys and event payloads (`postoffice:brokers|routes|subscriptions`, `config_updates` actions), DLQ envelope, Prometheus metric names/labels, route and firewall rule schema (JSON Schema), unroutable policy and verdict semantics, ack/nack rules per protocol, Confluent wire framing, topic matching (including the `$` rule and idempotent add).
- Produce `conformance/` vectors generated from the Python implementation (deterministic: injectable clocks and seeded RNG, already used in D1/D2 tests).
- Add event `schema_version` and HMAC signing (D9) *before* a second implementation exists, so the contract starts safe.
- **Exit:** Python passes 100% of the vectors via a thin runner; contracts documented in `docs/contracts/`.

### Phase 2: First native component, lowest risk (3–5 weeks)
Pick by Phase 0 evidence. Default candidate: **ingress firewall + codecs as a library** (pure functions, no network, easiest to verify; highest DoS value).
- Implement rule engine (size, topic shape, token buckets, content guard, sender ledger) and JSON/Avro/Protobuf conversion natively; fuzz with cargo-fuzz or go-fuzz plus the conformance vectors.
- Integrate: Rust core exposed through the C ABI and a PyO3/cffi wheel behind the existing Python interfaces (feature flag `POSTOFFICE_NATIVE=1`), or as a sidecar over a local socket/gRPC.
- Add the memory governor (byte-accounted budget, cgroup-aware, 50% default, shed at 80%) and the edge build profile here, so every later component is written against them.
- **Exit:** vectors pass in both modes; unit/micro tier of the performance suite shows the improvement Phase 0 predicted; fuzzing finds no panics/unbounded allocation; Python path remains default.

### Phase 3: Native Data Plane worker, read path first (6–10 weeks)
- New worker binary implementing: Redis sync (subscribe-then-snapshot, idempotent, resync on reconnect: the corrected design from T7), router core, supervisor/backoff, metrics endpoint, unroutable policy, DLQ budget.
- Adapters in this order, each with integration tests against `docker compose`: **Kafka** (offset tracking by contiguous completion, K1/K2 solved by design), **AMQP** (publisher confirms, prefetch back-pressure), **MQTT/NanoMQ** (QoS1 confirm via PUBACK, reconnect).
- Run **shadow mode**: native worker consumes a mirrored topic copy and compares outputs to Python's, diffing envelopes and metrics.
- **Exit:** conformance vectors + integration tier green; shadow diff clean for a defined soak period; kill/restart/broker-outage drills pass.

### Phase 4: Mixed-fleet rollout (4–6 weeks)
- Canary: native workers handle a subset of routes (control-plane route tag `engine=native`), same Redis, same consumer groups; ramp 5% → 25% → 100% per environment with automatic rollback on error-rate or lag regression.
- Operational parity: Helm/compose, health and readiness endpoints, structured logs, dashboards, runbooks.
- **Exit:** a full release cycle on native with no sev-1; rollback rehearsed.

### Phase 5: Consolidate (ongoing)
- Python becomes: Control Plane, admin CLI, library facade for embedders (or a thin client of the native worker), and the spec generator.
- Decide per component: retire the Python data plane, or keep it as the reference implementation and test oracle. Keep the conformance suite in CI for both.
- Revisit P3 items natively where cheaper: per-target bounded queues, durable buffer (disk), Redis Streams.

## 5. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Rewrite stalls feature work | Contracts frozen first; DoS/adapter fixes continue in Python and become vectors, not throwaway |
| Behavioural drift between implementations | Shared conformance vectors, shadow mode, canary with rollback |
| Python users lose the in-process library | Accepted (decision 3); Python keeps a gRPC/REST SDK and the admin tooling, and can use the C ABI via cffi |
| Native client library gaps (e.g. NanoMQ specifics, Kafka SASL/Kerberos) | Spike each adapter in Phase 0/1 before committing; confluent-kafka-go or rdkafka bind librdkafka for parity |
| Language choice regret | Phase 0 Rust-vs-Go bake-off on one slice, recorded in an ADR |
| API surface grows too fast | Protobuf IDL with `buf breaking` in CI; additive-only within v1 |
| Over-engineering for unmeasured load | Phase 0 decision gate can end the effort cheaply |

## 6. Order relative to existing plans

Continue D3–D9 and T1–T4 in Python now: they are the executable specification and fix real defects regardless of language. Start Phase 0 in parallel (benchmark rig is independent). Phase 1 contracts should absorb D3–D9 outputs; Phase 2 must not start before D3/D5 settle the firewall rule schema.

## 7. Decisions still open

1. **Latency definition (§0.1):** is "p99 < 5 ms" the bridge-added latency (my assumption), or end-to-end including broker confirms? The second is not achievable with Kafka `acks=all` on most storage.
2. **Edge numbers:** target hardware (CPU/arch, RAM) and throughput for the edge profile, so the 64 MB / 1,000 msg/s proposal can be replaced with real figures.
3. **Reference payload and fan-out** for the 100k msg/s figure (1 KB, 1 route is my assumption; real mixes are covered by the scenario tier).
4. **Client languages to commit to in v1:** I propose Python, Go, Java, C#, C/C++ (via C ABI) and Rust. Others (JS/TS, Swift) follow from gRPC generation on demand.
5. **Durability at the edge:** is disk-backed store-and-forward acceptable on edge devices (flash wear), or RAM-only with loss on power failure?
