# Phase 5: Dynamic Schema Registry & Serialization — Context & Decisions

**Phase:** 05-dynamic-schema-registry-serialization
**Milestone:** v2.0
**Requirements:** SCH-01
**Created:** 2026-10-08

---

## 1. Background & Scope

PostOffice routes messages across heterogeneous brokers (MQTT IoT devices, AMQP microservices, Kafka streaming analytics). In many production deployments, edge devices emit raw JSON or compact binary payloads, while enterprise pipelines mandate schema-validated formats like Apache Avro (with Confluent wire format) or Protocol Buffers.

Phase 5 introduces dynamic schema management, format translation, and schema validation into the PostOffice routing pipeline.

---

## 2. Locked Decisions

### Decision 1: Hybrid Pluggable Schema Registry Architecture
- **Choice:** Pluggable `BaseSchemaRegistry` interface.
- **Implementations:**
  - `InMemorySchemaRegistry`: Default local registry for standalone applications and hermetic testing.
  - `RedisSchemaRegistry`: Distributed registry storing schemas in Redis hashes, synchronized via the Control Plane.
  - `ConfluentSchemaRegistryClient`: Optional client communicating with standard Confluent-compatible Schema Registry HTTP endpoints.

### Decision 2: Supported Formats & Libraries
- **Formats:** JSON, Apache Avro, Protocol Buffers.
- **Libraries:**
  - Avro: `fastavro` (lightweight, high-performance C-extension/Python Avro implementation).
  - Protobuf: `protobuf` (`google.protobuf.json_format` and dynamic runtime `DescriptorPool` / `FileDescriptorProto`).
  - Wire Formats: Standard Confluent wire format support (Magic byte `0x00` + 4-byte big-endian Schema ID + serialized payload) as well as raw Avro / raw Protobuf binary.

### Decision 3: Route Configuration Syntax & DLQ Integration
- **Syntax:** Explicit parameters in `add_route`:
  ```python
  postoffice.add_route(
      source_broker="mqtt_edge",
      source_topic="telemetry/temp",
      target_broker="kafka_main",
      target_topic="analytics.temperatures",
      input_schema="SensorReading",
      input_format="json",
      output_schema="SensorReading",
      output_format="avro"
  )
  ```
- **Error Handling & Failure Containment:**
  - If a message fails schema validation or deserialization, it is routed to the configured Dead Letter Queue (`_route_to_dlq`) with error description `"Schema transformation failure: <reason>"`.
  - Prometheus error counter increments `postoffice_routing_errors_total{error_type="schema_validation_error"}`.
  - Upstream ingress message is acknowledged (`ack_fn()`) once DLQed to avoid poison-pill retry loops.

---

## 3. Plan Decomposition

- **Plan 05-01 (Wave 1):** Core Schema Registry & Converters
  - Implement `src/postoffice/schema/` package:
    - `registry.py`: `BaseSchemaRegistry`, `InMemorySchemaRegistry`, `RedisSchemaRegistry`.
    - `converters.py`: Bidirectional converters between JSON ↔ Avro and JSON ↔ Protobuf.
  - Hermetic unit tests in `tests/test_schema_registry.py` and `tests/test_schema_converters.py`.

- **Plan 05-02 (Wave 2):** Routing Engine Integration & DLQ Fallback
  - Integrate schema transforms into `Router.route()` and `PostOffice.add_route()`.
  - Wire DLQ fallback for schema validation and deserialization errors.
  - End-to-end integration tests in `tests/test_schema_routing.py`.
