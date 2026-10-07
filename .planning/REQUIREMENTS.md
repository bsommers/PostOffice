# Requirements: PostOffice

**Defined:** 2026-10-05
**Core Value:** Seamlessly route and translate messaging payloads and delivery semantics across heterogeneous pub/sub, streaming, and queueing protocols with zero message loss.

## v1 Requirements (Established Baseline)

- [x] **CORE-01**: `BaseClient` abstract interface contract for message systems
- [x] **CORE-02**: `ClientRegistry` factory and decorator pattern for protocol registration
- [x] **CORE-03**: `Router` class supporting topic mappings and MQTT-style `+` and `#` wildcards
- [x] **CORE-04**: `PostOffice` unified application facade (`add_broker`, `add_route`, `subscribe`, `start`, `stop`)
- [x] **PLUG-01**: MQTT plugin (`MqttClient`) with paho-mqtt CallbackAPIVersion.VERSION2
- [x] **PLUG-02**: NanoMQ plugin (`NanoMqClient`) extending MQTT client for edge broker support
- [x] **PLUG-03**: AMQP plugin (`AmqpClient`) using pika BlockingConnection and thread-safe callbacks
- [x] **PLUG-04**: Kafka plugin (`KafkaClient`) using confluent-kafka Producer and Consumer
- [x] **DIST-01**: `ControlPlane` managing cluster state in Redis hashes (`brokers`, `routes`, `subscriptions`)
- [x] **DIST-02**: `DataPlane` worker syncing configuration from Redis and listening on Pub/Sub channel
- [x] **DIST-03**: `worker.py` and `admin.py` executable entry points

## v1.1 Active Requirements

### Semantic Translation & Guarantees
- [x] **SEM-01**: Coordinate QoS 1/2 acknowledgements between ingress and egress protocols (e.g. wait for Kafka delivery callback or AMQP ack before sending MQTT PUBACK)
- [x] **SEM-02**: Map MQTT QoS levels to AMQP `delivery_mode` (1=transient, 2=persistent) and Kafka `acks` configurations automatically

### Routing Engine Optimization
- [x] **ROUT-01**: Radix / Prefix Trie matching algorithm in `Router` to eliminate O(N) route scan overhead

### Testing & Reliability
- [x] **TEST-01**: Unit tests for `ControlPlane` and `DataPlane` with mock Redis (`fakeredis` or mock objects)
- [x] **TEST-02**: Reconnection resiliency in `DataPlane` if Redis drops connection
- [x] **TEST-03**: Clean state reset handling in `DataPlane` when `clear_state` is broadcast

### Observability & Error Handling
- [x] **OBS-01**: Metrics instrumentation (counters for messages routed, errors, latency)
- [x] **DLQ-01**: Dead Letter Queue (DLQ) support for unroutable or failed messages

## v2 Requirements

### Schema Registry & Transforms
- **SCH-01**: Dynamic payload schema conversion (e.g. JSON to Avro/Protobuf)
- **SEC-01**: TLS/mTLS and token authentication enforcement for production broker connections

## Out of Scope

| Feature | Reason |
|---------|--------|
| Custom message broker daemon | PostOffice bridges existing brokers, does not implement broker storage |
| Heavyweight GUI admin panel | Control Plane is programmatic/API-driven via Redis/CLI |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| TEST-01 | Phase 1 | Complete |
| TEST-02 | Phase 1 | Complete |
| TEST-03 | Phase 1 | Complete |
| SEM-01 | Phase 2 | Complete |
| SEM-02 | Phase 2 | Complete |
| ROUT-01 | Phase 3 | Complete |
| OBS-01 | Phase 4 | Complete |
| DLQ-01 | Phase 4 | Complete |

---
*Requirements status: 2026-10-05*
