# Milestones: PostOffice

This document tracks all completed and active milestone releases of PostOffice.

---

## [v1.1] Hardened Mesh & Delivery Guarantees

**Shipped:** 2026-10-07
**Phases:** 4 (Phases 1–4)
**Plans:** 8 total
**Unit Tests:** 43 tests passing in ~0.5s
**Audit Report:** `.planning/v1.1-MILESTONE-AUDIT.md` (Passed)
**Changelog:** `CHANGELOG.md` ([1.1.0])

### Delivered
A hardened, high-throughput, multi-protocol routing mesh connecting MQTT, AMQP, Kafka, and NanoMQ edge brokers with verifiable end-to-end delivery guarantees, $O(k)$ hierarchical Radix topic matching, Prometheus metrics, and Dead Letter Queue poison-pill isolation.

### Key Accomplishments
1. **End-to-End Delivery Coordination (`SEM-01`, `SEM-02`):** Asynchronous acknowledgement seam with atomic fanout coordinator (`_FanoutCoordinator`) and automatic QoS-to-delivery_mode mapping.
2. **Hierarchical TopicTrie Matching (`ROUT-01`):** Replaced linear topic scanning with an $O(k)$ Radix tree, achieving 648.9x lookup speedup at 10,000 routes.
3. **Hermetic Distributed Testing (`TEST-01`, `TEST-02`, `TEST-03`):** Resilient DataPlane reconnection loops, clean `reset()` state management, and 100% mocked offline unit testing.
4. **Prometheus Observability (`OBS-01`):** Exporter for message routing throughput, routing duration histograms, and error counters with built-in HTTP server.
5. **Dead Letter Queue Routing (`DLQ-01`):** JSON DLQ envelope serialization, base64 binary fallback, per-route overrides, and anti-recursion protection.

### Git Information
- **Tag:** `v1.1.0`
- **Verification:** All 8 requirements verified in unit test suite.

---

## [v1.0] Initial Scaffold Baseline

**Shipped:** 2026-10-05
**Phases:** Foundation Baseline
**Delivered:** Uniform facade (`PostOffice`), protocol client plugins (`MqttClient`, `AmqpClient`, `KafkaClient`, `NanoMqClient`), basic wildcard routing (`+`, `#`), and Redis-backed Control Plane / Data Plane synchronization.
