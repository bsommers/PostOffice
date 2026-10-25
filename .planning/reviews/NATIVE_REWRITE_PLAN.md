# Native (Rust / Go) Re-platforming Plan

> Status: **proposal**. Written 2026-10-25. No code changed. Nothing here has been measured yet: the review and tests were read-only against mocked brokers, so *Phase 0 exists to find out whether, and where, Python is actually the limit.*

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

**Preliminary recommendation (confirm after Phase 0):**
- **Go for the Data Plane worker** (adapters, router, supervision, metrics): I/O-bound concurrency, mature clients, fast to build and test.
- **Rust only for a CPU-bound hot path that Phase 0 proves** (ingress firewall content checks, schema codecs), shipped either as a PyO3 extension (while Python remains) or as a library inside the worker. Choose all-Rust instead if the team already has Rust skill or hard tail-latency/memory ceilings are a requirement.
- **Keep Python** for the Control Plane, admin CLI, schema tooling and the executable specification (below). They are not on the data path.

## 3. Principle: strangler fig, with the Python suite as the spec

Do not big-bang rewrite. The existing hermetic tests, plus D2–D9 tests, define behaviour. Convert them into **language-neutral conformance vectors** (JSON files: input message, route table, firewall rules, expected outputs/acks/DLQ envelope/metrics). Both implementations must pass the same vectors. Redis contracts and the DLQ envelope stay stable so Python and native workers can run **side by side** in one cluster.

## 4. Phases

### Phase 0: Measure and decide (1–2 weeks)
- Build a repeatable benchmark rig (`docker compose`: MQTT→Kafka, MQTT→AMQP, AMQP→Kafka; JSON→Avro with and without wire format; wildcard fan-out of 1/5/20 routes).
- Record per scenario: max sustained msg/s, p50/p99 latency, CPU per core, RSS, behaviour at 10× overload, GC/GIL stalls (py-spy).
- Profile where time goes: matching, conversion, publish, ack, logging.
- **Exit criteria / decision gate:** write down target SLOs (e.g. N msg/s per worker, p99 < X ms, RSS < Y MB). Proceed only if Python cannot meet them after the planned Python fixes (T1–T4, D3–D8). Output: `docs/BENCHMARKS.md` and an ADR choosing Go / Rust / PyO3 hybrid / stay.

### Phase 1: Freeze the contracts (2 weeks)
- Specify, versioned: Redis keys and event payloads (`postoffice:brokers|routes|subscriptions`, `config_updates` actions), DLQ envelope, Prometheus metric names/labels, route and firewall rule schema (JSON Schema), unroutable policy and verdict semantics, ack/nack rules per protocol, Confluent wire framing, topic matching (including the `$` rule and idempotent add).
- Produce `conformance/` vectors generated from the Python implementation (deterministic: injectable clocks and seeded RNG, already used in D1/D2 tests).
- Add event `schema_version` and HMAC signing (D9) *before* a second implementation exists, so the contract starts safe.
- **Exit:** Python passes 100% of the vectors via a thin runner; contracts documented in `docs/contracts/`.

### Phase 2: First native component, lowest risk (3–5 weeks)
Pick by Phase 0 evidence. Default candidate: **ingress firewall + codecs as a library** (pure functions, no network, easiest to verify; highest DoS value).
- Implement rule engine (size, topic shape, token buckets, content guard, sender ledger) and JSON/Avro/Protobuf conversion natively; fuzz with cargo-fuzz or go-fuzz plus the conformance vectors.
- Integrate: Rust via PyO3 wheel behind the existing Python interfaces (feature flag `POSTOFFICE_NATIVE=1`), or Go as a sidecar over a local socket/gRPC if Go was chosen.
- **Exit:** vectors pass in both modes; benchmark shows the improvement Phase 0 predicted; fuzzing finds no panics/unbounded allocation; Python path remains default.

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
| Embedded users lose the Python library | Keep the facade; native engine optional behind a flag or sidecar |
| Native client library gaps (e.g. NanoMQ specifics, Kafka SASL/Kerberos) | Spike each adapter in Phase 0/1 before committing; confluent-kafka-go or rdkafka bind librdkafka for parity |
| Team skill | Prefer Go unless Rust expertise exists; pair on the first component |
| Over-engineering for unmeasured load | Phase 0 decision gate can end the effort cheaply |

## 6. Order relative to existing plans

Continue D3–D9 and T1–T4 in Python now: they are the executable specification and fix real defects regardless of language. Start Phase 0 in parallel (benchmark rig is independent). Phase 1 contracts should absorb D3–D9 outputs; Phase 2 must not start before D3/D5 settle the firewall rule schema.

## 7. Decisions needed

1. Team constraints: any existing Go/Rust expertise or preference?
2. Target scale/SLOs, so Phase 0 has pass/fail numbers (msg/s per worker, p99, memory ceiling, edge hardware).
3. Do embedded Python library users matter long term (drives PyO3 vs sidecar)?
4. Deployment targets: constrained edge devices (favours Rust/Go static binary) vs servers only.
