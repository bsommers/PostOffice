# Roadmap: PostOffice

## Overview

This roadmap defines the engineering milestones to advance PostOffice from its initial scaffold into a hardened, high-throughput, enterprise-grade multi-protocol routing mesh with verified delivery guarantees and comprehensive test coverage.

## Phases

- [ ] **Phase 1: Test Suite & Distributed Plane Hardening** - Add mock Redis unit tests, implement reconnect resiliency in DataPlane, and handle clean state resets.
- [ ] **Phase 2: Semantic Translation & Delivery Guarantees** - Implement bidirectional acknowledgement propagation between MQTT QoS, AMQP confirms, and Kafka offsets.
- [ ] **Phase 3: Route Engine Trie Optimization** - Upgrade topic matching to a Radix/Trie structure for sub-millisecond routing at scale.
- [ ] **Phase 4: Observability & Dead Letter Queues** - Add OpenTelemetry/Prometheus metrics and dead-letter queue routing for failed deliveries.

---

## Phase Details

### Phase 1: Test Suite & Distributed Plane Hardening
**Goal**: Ensure 100% test coverage across the Control Plane and Data Plane components and make workers resilient to Redis disconnects and clear_state signals.
**Depends on**: Nothing (first phase)
**Requirements**: TEST-01, TEST-02, TEST-03
**Success Criteria**:
  1. Unit tests cover `ControlPlane` and `DataPlane` using a mock Redis interface without requiring live network services.
  2. DataPlane worker recovers gracefully if Redis connection is lost and restored.
  3. `ControlPlane.clear_state()` cleanly resets internal PostOffice routes and brokers without requiring process restart.
**Plans**: 2 plans

Plans:
- [ ] 01-01: Implement `ControlPlane` and `DataPlane` unit tests with mock Redis.
- [ ] 01-02: Add Redis reconnection backoff and `clear_state` reset handler to `DataPlane`.

### Phase 2: Semantic Translation & Delivery Guarantees
**Goal**: Coordinate end-to-end acknowledgement semantics across protocols so that ingress messages are only acknowledged after egress broker confirmation.
**Depends on**: Phase 1
**Requirements**: SEM-01, SEM-02
**Success Criteria**:
  1. MQTT QoS 1/2 publishes trigger AMQP persistent messages and wait for publisher confirm before PUBACK.
  2. Kafka offset commits are tied to successful downstream delivery.
**Plans**: 2 plans

Plans:
- [ ] 02-01: Design and implement asynchronous ACK/delivery callback seam in `BaseClient` and `Router`.
- [ ] 02-02: Wire publisher confirms and offset commit coordination across MQTT, AMQP, and Kafka plugins.

### Phase 3: Route Engine Trie Optimization
**Goal**: Replace the linear O(N) route scan with a hierarchical topic Radix/Trie matching engine.
**Depends on**: Phase 2
**Requirements**: ROUT-01
**Success Criteria**:
  1. Topic matching operates in O(k) time relative to topic depth k rather than route count N.
  2. All existing wildcard behaviors (`+`, `#`, exact match) pass regression suites.
**Plans**: 1 plan

Plans:
- [ ] 03-01: Implement Trie data structure for topic hierarchy and benchmark against large routing tables.

### Phase 4: Observability & Dead Letter Queues
**Goal**: Provide runtime visibility and failure recovery mechanisms for unroutable or rejected messages.
**Depends on**: Phase 3
**Requirements**: OBS-01, DLQ-01
**Success Criteria**:
  1. Prometheus counters expose message throughput, routing latency, and error counts.
  2. Messages failing egress delivery are routed to designated Dead Letter topics/queues.
**Plans**: 2 plans

Plans:
- [ ] 04-01: Instrument `PostOffice` and `Router` with OpenTelemetry and Prometheus metric collectors.
- [ ] 04-02: Implement configurable Dead Letter Queue (DLQ) routing for unhandled or rejected payloads.

---
*Roadmap created: 2026-10-05*
