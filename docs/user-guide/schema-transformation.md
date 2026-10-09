# Dynamic Schema Registry & Serialization

[← Back to User Guide](README.md) | [← Back to Project README](../../README.md)

PostOffice enables runtime schema validation and bidirectional payload translation across JSON, Apache Avro (`fastavro`), and Protocol Buffers (`protobuf`), with full support for the standard Confluent Schema Registry 5-byte wire format.

---

## Table of Contents
- [Supported Formats & Framing](#supported-formats--framing)
- [Schema Registry Architecture](#schema-registry-architecture)
  - [In-Memory Registry (Default)](#in-memory-registry-default)
  - [Distributed Redis Registry](#distributed-redis-registry)
- [Registering Schemas](#registering-schemas)
  - [Registering Apache Avro Schemas](#registering-apache-avro-schemas)
  - [Registering Protocol Buffer Schemas](#registering-protocol-buffer-schemas)
- [Configuring Route Transformations](#configuring-route-transformations)
- [Confluent Wire Format Support](#confluent-wire-format-support)
- [Failure Handling & DLQ Routing](#failure-handling--dlq-routing)

---

## Supported Formats & Framing

PostOffice translates seamlessly between any pair of supported formats:

| Format Name | Identifier | Engine | Typical Use Case |
|---|---|---|---|
| **JSON** | `"json"` | Standard `json` | Edge MQTT sensors, REST integrations |
| **Apache Avro** | `"avro"` | `fastavro` | Kafka event streams, BigQuery / Data lake ingestion |
| **Protocol Buffers** | `"protobuf"` | `google.protobuf` | High-efficiency microservice RPC, compact telemetry |

### Confluent Wire Format
When communicating with Kafka topics using Schema Registry, messages are prepended with a 5-byte header:
- **Byte 0:** Magic Byte (`0x00`)
- **Bytes 1–4:** 4-byte big-endian unsigned integer representing the numeric Schema ID
- **Bytes 5+:** Binary Avro or Protobuf payload

PostOffice automatically encodes and decodes this header when `wire_format=True`.

---

## Schema Registry Architecture

PostOffice provides pluggable registry backends implementing `BaseSchemaRegistry`:

### In-Memory Registry (Default)
`InMemorySchemaRegistry` is local, thread-safe, and caches parsed schemas directly in memory. Ideal for standalone scripts and hermetic testing.

### Distributed Redis Registry
`RedisSchemaRegistry` stores schema definitions and ID counters in Redis hashes, synchronizing schemas across distributed worker instances while maintaining an in-memory cache on each worker.

---

## Registering Schemas

You can register schemas directly on the `PostOffice` facade or through a `BaseSchemaRegistry` instance.

### Registering Apache Avro Schemas

Pass a Python dictionary or JSON string conforming to the Apache Avro specification:

```python
from postoffice.app import PostOffice

po = PostOffice()

sensor_avro_schema = {
    "type": "record",
    "name": "SensorReading",
    "fields": [
        {"name": "sensor_id", "type": "string"},
        {"name": "temperature", "type": "double"},
        {"name": "humidity", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ]
}

# Registers schema and assigns a numeric Schema ID (e.g. 1)
schema_id = po.register_schema("SensorReading", sensor_avro_schema, schema_type="AVRO")
print(f"Registered SensorReading as schema ID: {schema_id}")
```

### Registering Protocol Buffer Schemas

You can register compiled Protobuf `Message` classes, or dictionary definitions dynamically synthesized at runtime without compiling `.proto` files:

```python
sensor_proto_schema = {
    "package": "telemetry",
    "message_name": "SensorReadingProto",
    "fields": [
        {"name": "sensor_id", "type": "string", "number": 1},
        {"name": "temperature", "type": "double", "number": 2},
        {"name": "humidity", "type": "double", "number": 3},
        {"name": "timestamp", "type": "int64", "number": 4}
    ]
}

po.register_schema("SensorReadingProto", sensor_proto_schema, schema_type="PROTOBUF")
```

---

## Configuring Route Transformations

Configure input and output formats and schemas in `add_route(...)`:

### Example 1: MQTT JSON to Kafka Avro (Confluent Wire Format)

```python
po.add_route(
    source_broker="mqtt_edge",
    source_topic="sensors/+/readings",
    target_broker="kafka_main",
    target_topic="analytics.readings.avro",
    input_format="json",
    output_format="avro",
    output_schema="SensorReading",
    wire_format=True  # Prepends 5-byte magic byte + schema_id header
)
```

### Example 2: Kafka Avro to RabbitMQ JSON

```python
po.add_route(
    source_broker="kafka_main",
    source_topic="analytics.readings.avro",
    target_broker="rabbit_mq",
    target_topic="telemetry.alerts",
    input_format="avro",
    input_schema="SensorReading",
    output_format="json",
    wire_format=True  # Automatically strips 5-byte header and decodes schema
)
```

### Example 3: Cross-Format Fanout
A single MQTT JSON message can be routed to Kafka as Avro, to an analytics broker as Protobuf, and to a dashboard as raw JSON simultaneously:

```python
# 1. Kafka receives Avro with Confluent header
po.add_route("mqtt_edge", "metrics", "kafka", "metrics.avro",
             input_format="json", output_format="avro", output_schema="SensorReading", wire_format=True)

# 2. RPC broker receives Protobuf
po.add_route("mqtt_edge", "metrics", "grpc_queue", "metrics.proto",
             input_format="json", output_format="protobuf", output_schema="SensorReadingProto")

# 3. Notification broker receives original JSON
po.add_route("mqtt_edge", "metrics", "dashboard", "metrics.json")
```

---

## Confluent Wire Format Support

Directional wire framing is handled automatically:
- When routing **from JSON to Avro/Protobuf** with `wire_format=True`, only the outgoing message receives the 5-byte wire header.
- When routing **from Avro/Protobuf to JSON** with `wire_format=True`, the 5-byte wire header is parsed and stripped from the incoming message.
- If you need explicit directional control, use `wire_format_in=True` or `wire_format_out=True`.

---

## Failure Handling & DLQ Routing

If an incoming message violates schema validation (e.g. wrong type, missing required field, corrupted JSON), PostOffice:
1. Logs the exact validation failure.
2. Increments `postoffice_routing_errors_total{error_type="schema_validation_error"}`.
3. Packages the unparseable original payload and exception trace into a JSON Dead Letter Queue envelope.
4. Publishes the envelope to the designated DLQ broker.
5. Issues upstream `ack_fn()` so the malformed poison-pill message does not block ingress queues.
