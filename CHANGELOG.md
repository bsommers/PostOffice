# Changelog

All notable changes to the PostOffice project are documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
