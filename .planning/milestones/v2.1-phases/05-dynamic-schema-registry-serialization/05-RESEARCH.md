# Phase 5: Dynamic Schema Registry & Serialization — Research & Design

**Phase:** 05-dynamic-schema-registry-serialization
**Date:** 2026-10-08
**Tags:** avro, protobuf, fastavro, schema-registry, serialization, json

---

## 1. Fastavro Schema Parsing & Wire Format

### 1.1 Fastavro In-Memory Serialization
`fastavro` provides `schemaless_writer` and `schemaless_reader`, which serialize individual record dictionaries against a parsed Avro schema without attaching the full Avro container header to each message. This is essential for low-overhead message bus routing:

```python
import io
import fastavro

# 1. Parse JSON/dict schema
schema = fastavro.parse_schema({
    "type": "record",
    "name": "SensorReading",
    "fields": [
        {"name": "sensor_id", "type": "string"},
        {"name": "temperature", "type": "double"},
        {"name": "timestamp", "type": "long"}
    ]
})

# 2. Serialize Dict -> Avro Bytes
buffer = io.BytesIO()
record = {"sensor_id": "sensor-01", "temperature": 23.4, "timestamp": 1728390000}
fastavro.schemaless_writer(buffer, schema, record)
avro_bytes = buffer.getvalue()

# 3. Deserialize Avro Bytes -> Dict
read_buffer = io.BytesIO(avro_bytes)
restored = fastavro.schemaless_reader(read_buffer, schema)
```

### 1.2 Confluent Avro Wire Format
In Confluent Kafka ecosystems, messages serialized with Schema Registry use a 5-byte header prefix:
- **Byte 0:** Magic Byte (`0x00`).
- **Bytes 1–4:** 4-byte unsigned big-endian integer (`>I`) encoding the Schema ID.
- **Bytes 5+:** Schemaless binary Avro payload.

To support Confluent wire format:
- `encode_wire_format(payload: bytes, schema_id: int) -> bytes`:
  `b'\x00' + struct.pack('>I', schema_id) + payload`
- `decode_wire_format(data: bytes) -> tuple[int, bytes]`:
  Verifies magic byte `0x00`, unpacks `schema_id = struct.unpack('>I', data[1:5])[0]`, and returns `(schema_id, data[5:])`.

---

## 2. Dynamic Protocol Buffers & JSON Format

### 2.1 JSON ↔ Protobuf Bidirectional Conversion
Google's official `protobuf` library provides `google.protobuf.json_format`:
- `json_format.MessageToJson(message, preserving_proto_field_name=True)`
- `json_format.Parse(json_str, message_instance)` or `json_format.ParseDict(dict_obj, message_instance)`

### 2.2 Runtime Dynamic Protobuf Descriptors
To allow schemas to be registered dynamically at runtime without requiring an external `protoc` build step ahead of time, PostOffice can construct `FileDescriptorProto` structures and register them into `google.protobuf.descriptor_pool.DescriptorPool`. Alternatively, users can register compiled `Message` classes or `.proto` schema strings parsed dynamically.

```python
from google.protobuf import descriptor_pb2
from google.protobuf import descriptor_pool
from google.protobuf import message_factory
from google.protobuf import json_format

# Dynamically building a proto descriptor
file_proto = descriptor_pb2.FileDescriptorProto()
file_proto.name = "sensor.proto"
file_proto.package = "postoffice"

msg_type = file_proto.message_type.add()
msg_type.name = "SensorReading"

f1 = msg_type.field.add()
f1.name = "sensor_id"
f1.number = 1
f1.type = descriptor_pb2.FieldDescriptorProto.TYPE_STRING

f2 = msg_type.field.add()
f2.name = "temperature"
f2.number = 2
f2.type = descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE

pool = descriptor_pool.DescriptorPool()
pool.Add(file_proto)
descriptor = pool.FindMessageTypeByName("postoffice.SensorReading")
factory = message_factory.MessageFactory(pool)
message_cls = factory.GetPrototype(descriptor)
```

---

## 3. Pluggable Schema Registry Architecture

### 3.1 Registry Contract (`BaseSchemaRegistry`)
```python
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, Union

class SchemaEntry:
    def __init__(self, schema_id: int, name: str, schema_type: str, definition: Any, parsed_schema: Any):
        self.schema_id = schema_id
        self.name = name
        self.schema_type = schema_type.upper()  # 'AVRO' or 'PROTOBUF'
        self.definition = definition            # Raw dict or proto def
        self.parsed_schema = parsed_schema      # fastavro parsed schema or proto message class

class BaseSchemaRegistry(ABC):
    @abstractmethod
    def register_schema(self, name: str, definition: Any, schema_type: str = "AVRO") -> int:
        pass

    @abstractmethod
    def get_schema(self, name: str) -> Optional[SchemaEntry]:
        pass

    @abstractmethod
    def get_schema_by_id(self, schema_id: int) -> Optional[SchemaEntry]:
        pass
```

### 3.2 Implementations
1. **`InMemorySchemaRegistry`:**
   - Thread-safe dictionary storing `name -> SchemaEntry` and `id -> SchemaEntry`.
   - Default for local development and unit tests.
2. **`RedisSchemaRegistry`:**
   - Serializes schema definitions into Redis hashes (`postoffice:schemas:{name}`) and global ID counters (`postoffice:schemas:next_id`).
   - Caches parsed schemas in local memory to avoid re-parsing on every message route.
3. **`ConfluentSchemaRegistryClient`:**
   - Optional HTTP client querying a Confluent Schema Registry instance (`GET /subjects/{subject}/versions/latest`).

---

## 4. Routing Engine Integration & Dead Letter Queue Fallback

### 4.1 Route Definition
In `Router.add_route` / `PostOffice.add_route`:
```python
add_route(
    source_client="mqtt_edge",
    source_topic="telemetry/temp",
    target_client="kafka_main",
    target_topic="analytics.temperatures",
    input_format="json",
    input_schema="SensorReading",
    output_format="avro",
    output_schema="SensorReading",
    wire_format=True
)
```

### 4.2 Pipeline Flow in `Router.route()`
1. Ingress payload bytes received.
2. If route specifies `input_format` & `output_format`:
   - Step 1: Decode incoming bytes to Python dictionary via `SchemaConverter.deserialize(payload, input_format, input_schema, wire_format)`.
   - Step 2: Encode Python dictionary to target bytes via `SchemaConverter.serialize(record_dict, output_format, output_schema, wire_format)`.
3. If any step raises `SchemaValidationError`, `ValueError`, or `KeyError`:
   - Catch error and invoke `self._route_to_dlq(source_client, source_topic, message, error=f"Schema transform error: {e}")`.
   - Record metric `self.metrics.record_error(source_client, "schema_transform_error")`.
   - Release upstream message via `ack_fn()` (poison-pill quarantined).
4. If transform succeeds: pass transformed bytes to `target_client.publish(target_topic, transformed_payload, ...)`.
