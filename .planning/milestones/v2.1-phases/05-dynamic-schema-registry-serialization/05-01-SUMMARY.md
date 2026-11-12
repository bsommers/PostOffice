---
phase: 05-dynamic-schema-registry-serialization
plan: 01
subsystem: schema
tags:
  - schema-registry
  - avro
  - protobuf
  - fastavro
  - serialization
  - unittest

requires: []
provides:
  - BaseSchemaRegistry interface with InMemorySchemaRegistry and RedisSchemaRegistry
  - SchemaConverter supporting bidirectional JSON, Avro, and Protobuf translation
  - Confluent wire format framing encoder and decoder
  - Hermetic unit tests in tests/test_schema_registry.py and tests/test_schema_converters.py
affects:
  - 05-02-PLAN.md
  - Router
  - PostOffice

tech-stack:
  added:
    - fastavro 1.13.1
    - protobuf 7.36.2
  patterns:
    - In-memory compiled schema caching for high-throughput routing
    - Confluent 5-byte wire format (magic byte 0x00 + big-endian schema ID) framing
    - Dynamic Protobuf FileDescriptorProto synthesis without protoc compilation step

key-files:
  created:
    - src/postoffice/schema/__init__.py
    - src/postoffice/schema/registry.py
    - src/postoffice/schema/converters.py
    - tests/test_schema_registry.py
    - tests/test_schema_converters.py
  modified:
    - requirements.txt

key-decisions:
  - "Implemented InMemorySchemaRegistry with local dicts and thread safety for standalone testing and routing"
  - "Implemented RedisSchemaRegistry persisting metadata into Redis hashes while maintaining local compiled cache"
  - "Used fastavro schemaless_writer and schemaless_reader for low-overhead Avro message streaming"
  - "Used google.protobuf.message_factory.GetMessageClass and dynamic FileDescriptorProto for zero-build Protobuf support"
  - "Created encode_wire_format and decode_wire_format supporting standard Confluent 5-byte wire framing"

patterns-established:
  - "SchemaConverter translates payloads between arbitrary pairs of formats (JSON, Avro, Protobuf)"
  - "Wire format framing automatically resolves schema ID from byte header when schema_name is omitted"
  - "SchemaValidationError encapsulates schema lookup, parsing, and serialization failures"

requirements-completed:
  - SCH-01 (Part 1: Schema Registry & Format Engines)

coverage:
  - id: S1
    description: "Schema registry interface and in-memory/Redis implementations"
    requirement: "SCH-01"
    verification:
      - kind: unit
        ref: "tests/test_schema_registry.py"
        status: pass
    human_judgment: false
  - id: S2
    description: "Bidirectional JSON, Avro, and Protobuf converters with Confluent wire format"
    requirement: "SCH-01"
    verification:
      - kind: unit
        ref: "tests/test_schema_converters.py"
        status: pass
    human_judgment: false
---

# Plan 05-01 Summary

Implemented core Schema Registry implementations (`InMemorySchemaRegistry`, `RedisSchemaRegistry`) in `src/postoffice/schema/registry.py` and bidirectional serialization engines (`SchemaConverter`) supporting JSON, Apache Avro (`fastavro`), and Protocol Buffers (`protobuf`) with Confluent wire format framing in `src/postoffice/schema/converters.py`.

## Key Changes
1. **`src/postoffice/schema/registry.py`:**
   - Defined `SchemaEntry` with schema ID, name, format type, raw definition, and compiled schema representation.
   - Defined `BaseSchemaRegistry` contract (`register_schema`, `get_schema`, `get_schema_by_id`).
   - Implemented `InMemorySchemaRegistry` with automatic ID generation, versioning, and compiled schema caching.
   - Implemented `RedisSchemaRegistry` persisting schema metadata to Redis hashes with local memory caching.
   - Added `_build_dynamic_proto_message_class` to dynamically construct Protobuf message classes from dictionary definitions without requiring `protoc` code generation.
2. **`src/postoffice/schema/converters.py`:**
   - Implemented `encode_wire_format` and `decode_wire_format` for Confluent 5-byte wire format (magic byte `0x00` + 4-byte big-endian Schema ID).
   - Implemented `SchemaConverter` supporting `serialize`, `deserialize`, and `convert` between JSON, Avro, and Protobuf.
   - Defined `SchemaValidationError` for clean error containment.
3. **`tests/test_schema_registry.py`:**
   - 6 unit tests verifying registration, retrieval by name/ID, version incrementing, invalid types, missing schemas, and mock Redis integration.
4. **`tests/test_schema_converters.py`:**
   - 8 unit tests verifying wire format encoding/decoding, JSON ↔ Avro, JSON ↔ Protobuf, cross-format roundtrips (JSON -> Avro -> Protobuf -> JSON), and error handling.

## Verification
- `PYTHONPATH=src .venv/bin/python -m unittest tests/test_schema_registry.py tests/test_schema_converters.py` (14 tests in 0.001s, OK)
- `PYTHONPATH=src .venv/bin/python -m unittest discover tests/` (57 tests in 0.527s, OK)
