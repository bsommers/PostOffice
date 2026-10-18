# Changelog

All notable changes to the PostOffice project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
