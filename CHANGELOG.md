# Changelog

All notable changes to the PostOffice project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [3.0.0] - 2026-11-05

### Added
- **Native Rust Core Library & C ABI (`libpostoffice`, Phases 1–2):**
  - Implemented zero-dependency Rust shared/static library (`native/libpostoffice/`) exported across a stable C ABI (`include/postoffice.h`) and Python `ctypes` bridge (`src/postoffice/native.py`) gated by `POSTOFFICE_NATIVE=1` or `PostOffice(use_native_engine=True)`.
  - Native $O(k)$ `TopicTrie` (with MQTT `$`-prefix isolation), `IngressFirewall`, `ContentGuard`, `SenderLedger`, `MemoryGovernor`, `BudgetedDlq`, and Confluent 5-byte wire codec.
  - Edge build profile (`--no-default-features --features edge`) and deterministic property/fuzz test suite (`native/libpostoffice/tests/fuzz_targets.rs`).
- **Native Data Plane Engine, Protocol Trackers & Shadow Mode (Phase 3):**
  - Full in-process `NativeEngine` (`native/libpostoffice/src/engine.rs`) with subscribe-then-snapshot Redis RESP2 synchronization (`native/libpostoffice/src/redis_sync.rs`), Prometheus `/metrics`, `/healthz`, and `/readyz` HTTP endpoints.
  - Native protocol state trackers (`native/libpostoffice/src/adapters.rs`): `KafkaOffsetTracker` (contiguous watermark commits solving out-of-order batch gaps), `AmqpConfirmTracker` (publisher confirms and prefetch back-pressure), and `MqttPubackTracker` (QoS 1 `PUBACK` correlation and offline replay).
  - Differential shadow harness (`src/postoffice/shadow.py`) comparing Python and Native routing outcomes, payloads, and dispositions side-by-side.
- **Mixed-Fleet Canary Rollout & Operational Parity (Phase 4):**
  - Control-plane route engine tagging (`engine="any" | "python" | "native"`) and `CanaryRolloutController` (`src/postoffice/rollout.py`) supporting staged `5% -> 25% -> 100%` traffic ramps with automatic SLO-breach rollback.
  - Structured JSON logging (`src/postoffice/logging_config.py`), Kubernetes Helm chart (`deploy/helm/postoffice/`), mixed-fleet Docker Compose (`deploy/docker-compose.mixed-fleet.yml`), Grafana dashboard (`deploy/dashboards/postoffice-mixed-fleet.json`), and operator runbook (`docs/runbooks/mixed-fleet-rollout.md`).
- **Native Bounded Queues, Edge Store-and-Forward & Redis Streams (Phase 5):**
  - Native per-target bounded publish queue (`TargetPublishQueue` / `po_target_queue_t`) enforcing `max_messages`, `max_bytes`, `max_age_sec`, and `overflow_policy` (`"reject"` / `"drop_oldest"`).
  - Opt-in edge disk-backed store-and-forward (`DiskStoreAndForward` / `po_disk_store_t`) in `native/libpostoffice/src/queue.rs` and `BaseClient(..., disk_store_path=...)` with `size_cap_bytes`, wear-aware write batching, and IEEE CRC32 crash-recovery framing.
  - Redis Streams (`postoffice:config_stream`) event log publishing (`XADD` in `ControlPlane`) and catch-up replay (`XRANGE` in `DataPlane.replay_stream` and Rust `RespConn::xrange_payloads`).
  - Architectural Decision Records: `docs/adr/0001-native-data-plane-language.md` and `docs/adr/0002-phase5-consolidation.md`.
- **Polyglot v1 Contracts & 71-Vector Conformance Suite (Phase 1):**
  - Frozen v1 Protobuf IDL (`proto/postoffice/v1/postoffice.proto`), OpenAPI 3.1 Control Plane specification (`api/openapi.v1.yaml`), and C ABI header (`include/postoffice.h`).
  - Language-neutral 71-vector JSON conformance suite (`conformance/vectors/`, `conformance/run.py --both`) verifying 100% parity between Python and Native Rust engines.
- **Ingress Firewall, DoS Protection & Control Plane Signing (`D1`–`D11`):**
  - Pluggable `IngressFirewall` (`src/postoffice/ingress/`) with `MaxPayloadBytes`, `TopicShapeRule`, `RateLimitRule`, `ContentGuard` (streaming UTF-8/JSON depth/key-count scanner and Avro/Protobuf pre-routing validation), `SenderLedger` (escalating cool-down), `InFlightGate` (global/per-broker concurrency semaphores), and `RetryLimiter` (poison-message backoff).
  - Token-bucket `BudgetedDlq` with identical-error deduplication and payload-free oversize records.
  - Control-plane HMAC-SHA256 event signing (`src/postoffice/signing.py`) with `off` / `observe` / `enforce` modes, event size/rate limits, action allow-lists, and dynamic schema complexity caps (`src/postoffice/schema/limits.py`).
- **Performance Suite & Rust-vs-Go Bake-Off (Phase 0):**
  - Open-loop Rust load generator and accounting sink (`perf/loadgen/`), bridge-added latency probe (`perf/bridge_probe.py`), `MemoryGovernor` (`src/postoffice/governor.py`), and comprehensive benchmark report (`docs/BENCHMARKS.md`).

### Changed
- `PostOffice` facade (`src/postoffice/app.py`) now supports `use_native_engine=True` (`POSTOFFICE_NATIVE_ENGINE=1`), `route_native()`, `configure_ingress()`, `on_unroutable` policy (`"nack"`, `"drop"`, `"dlq"`), `dlq_rate_per_sec`, `dlq_burst`, `max_routes_per_message`, and `max_redeliveries`.
- Metrics HTTP server defaults to loopback (`127.0.0.1`) instead of `0.0.0.0` unless explicitly configured.

### Fixed
- Real broker acceptance confirmation across MQTT (`_on_publish` mid tracking for QoS >= 1), AMQP (`confirm_delivery` + `mandatory=True`), and Kafka (`enable.auto.offset.store=False` with contiguous offset commits).
- Race-free Control/Data Plane convergence via subscribe-before-snapshot (`T7`), bounded event buffering, and monotonic version tracking.

---

## [2.1.0] - 2026-10-18

### Added
- **Standardized Client Queuing & Offline Replay (`RES-01`):**
  - Thread-safe subscription registry (`record_subscription`, `get_subscriptions`) in `BaseClient` preserving broker-specific subscription options (`qos`, `exchange`, `exchange_type`, `queue`, `auto_ack`) across pre-connect registration and reconnect replay across `MqttClient`, `NanoMqClient`, `AmqpClient`, and `KafkaClient`.
  - Bounded thread-safe in-flight publish buffer (`BufferedPublish`, `BufferOverflowError`, `enable_publish_buffer`, `max_publish_buffer_size`, `buffer_overflow_policy`) supporting `"reject"` and `"drop_oldest"` policies with FIFO flush on broker connection.
- **Egress Circuit Breakers & Ring-Buffer Failover (`RES-02`):**
  - Per-broker egress `CircuitBreaker` (`CLOSED`, `OPEN`, `HALF_OPEN`) in `postoffice.resiliency` with configurable `failure_threshold` and `recovery_timeout`.
  - Automatic ring-buffer failover before Dead Letter Queue (DLQ) drop when a target broker's circuit breaker is `OPEN`, and automatic buffer flush upon `HALF_OPEN -> CLOSED` probe recovery.
  - Facade configuration via `PostOffice.configure_circuit_breaker()` and `add_broker(..., circuit_breaker_threshold=..., circuit_breaker_timeout=...)`.
- **Dynamic In-Flight TLS & Credential Hot-Reload (`RES-03`):**
  - Zero-downtime `reload_security()` across `MqttClient`, `NanoMqClient`, `AmqpClient`, and `KafkaClient` preserving active subscriptions and buffered publishes.
  - Programmatic reload via `PostOffice.reload_broker_security()` and cluster-wide live broadcast via `ControlPlane.update_broker_security()` and `DataPlane` Pub/Sub listener.
- **Test Coverage Gate & Audit (`TEST_COVERAGE.md`):**
  - Expanded hermetic unit test suite to 106 tests across 15 suites (`tests/test_plugin_ingress_callbacks.py`), achieving 92.9% statement coverage (`1,392 / 1,498`), 99.3% function reachability (`138 / 139`), and enforcing a `>= 90%` combined branch + statement coverage threshold in `.coveragerc`.
- **User Guide Documentation (`docs/user-guide/`):**
  - Added `docs/user-guide/resiliency-and-circuit-breakers.md` and synchronized all user guide chapters and API references for Milestone v2.1.

---

## [2.0.0] - 2026-10-13

### Added
- **Dynamic Schema Registry & Serialization (`SCH-01`):**
  - Pluggable schema registry backends (`postoffice.schema.InMemorySchemaRegistry`, `postoffice.schema.RedisSchemaRegistry`) supporting runtime registration and schema versioning.
  - Bidirectional dynamic payload translation across JSON, Apache Avro (`fastavro`), and Protocol Buffers (`protobuf 7.x`).
  - Confluent 5-byte wire format framing (`encode_wire_format`, `decode_wire_format`) with 4-byte big-endian Schema IDs.
  - Automatic Dead Letter Queue diversion on schema validation failures with upstream acknowledgement.
  - Facade helper methods: `register_schema()`, `get_schema()`, `get_schema_by_id()`.
- **Production Security, TLS/mTLS & Authentication (`SEC-01`):**
  - Unified `SecurityConfig` dataclass normalizing TLS/mTLS options (`ca_cert`, `client_cert`, `client_key`, `insecure_skip_verify`) and credentials (`username`, `password`, `sasl_mechanism`).
  - Hardened MQTT & NanoMQ drivers with OpenSSL socket configuration and automated port shifting (1883->8883, 1884->8884).
  - Hardened AMQP driver with `pika.SSLOptions`, `ssl.SSLContext`, and automated port shifting (5672->5671).
  - Hardened Kafka driver with dynamic `security.protocol` resolution (`SSL`, `SASL_PLAINTEXT`, `SASL_SSL`) and librdkafka SCRAM/PLAIN credentials.
  - Redis Control Plane state serialization and DataPlane worker live sync for secure brokers.
- **Documentation Suite:**
  - 7 comprehensive user guide documents in `docs/user-guide/` covering wildcards, delivery guarantees, schemas, observability, distributed planes, security, and troubleshooting.

---

## [1.1.0] - 2026-10-07

### Added
- **Hierarchical TopicTrie Matching (`ROUT-01`):** Replaced linear $O(N)$ route scan with a Radix/Prefix topic trie (`postoffice.trie.TopicTrie`) supporting exact tokens, single-level (`+`), and multi-level (`#`) MQTT wildcards with $O(k)$ lookup depth scaling (648.9x faster at 10,000 routes).
- **End-to-End Delivery Guarantees (`SEM-01`):** Added asynchronous publisher confirmation seam (`ack_fn`, `nack_fn`, `on_confirm`, `on_error`) with atomic latch coordination (`_FanoutCoordinator`) across MQTT QoS 1/2, AMQP publisher confirms, and Kafka offset commits.
- **Automatic Semantic Translation (`SEM-02`):** Automatic mapping between MQTT QoS (0, 1, 2) and AMQP `delivery_mode` (1=transient, 2=persistent).
- **Prometheus Metrics Instrumentation (`OBS-01`):** Integrated `MetricsManager` with Prometheus counters (`postoffice_messages_routed_total`, `postoffice_routing_errors_total`, `postoffice_dlq_messages_total`), routing duration histogram, and built-in HTTP server (`PostOffice(metrics_port=...)`).
- **Dead Letter Queue Routing (`DLQ-01`):** Added structured JSON DLQ envelope serialization (`postoffice.dlq.format_dlq_payload`) with Base64 binary fallback, global default and per-route DLQ configuration, poison-pill ACK release, and anti-recursion loop protection.
- **Distributed Plane Hardening (`TEST-01`, `TEST-02`, `TEST-03`):** Resilient reconnection loop with exponential backoff for `DataPlane` on Redis disconnects, clean state resets via `ControlPlane.clear_state()`, and hermetic unit tests with mock Redis.
- **Multi-Language Architecture Spike:** Comprehensive evaluation of Python vs Go, Rust, Zig, C, and C++ for message routing mesh performance.

### Changed
- `PostOffice` facade expanded to support `metrics_port`, `dlq_broker`, `dlq_topic`, `set_dlq()`, and state `reset()`.
- Topic matching in `Router.route()` now delegates to per-client `TopicTrie` instances.

---

## [1.0.0] - 2026-10-05

### Added
- Initial PostOffice uniform multi-protocol messaging bridge facade.
- Protocol client plugins: MQTT (`paho-mqtt`), AMQP (`pika`), Apache Kafka (`confluent-kafka`), and NanoMQ edge broker.
- Basic linear wildcard topic router (`+`, `#`).
- Redis-synchronized Control Plane and Data Plane worker architecture.
