---
phase: 05-dynamic-schema-registry-serialization
plan: 02
subsystem: routing-schema
tags:
  - schema-transformation
  - avro
  - protobuf
  - dlq
  - router
  - facade
  - unittest

requires:
  - 05-01-PLAN.md
provides:
  - Dynamic payload schema transformation in Router.route
  - Dead Letter Queue routing on schema validation/deserialization failure
  - Prometheus schema_validation_error metric recording
  - Schema configuration and registry management in PostOffice facade
  - Hermetic end-to-end integration tests in tests/test_schema_routing.py
affects:
  - Router
  - PostOffice

tech-stack:
  added: []
  patterns:
    - Route-level format and schema transformation (JSON <-> Avro <-> Protobuf)
    - Confluent wire format directional detection based on format type
    - Poison-pill quarantine with upstream ACK upon routing transform failures to DLQ

key-files:
  created:
    - tests/test_schema_routing.py
  modified:
    - src/postoffice/router.py
    - src/postoffice/app.py
    - src/postoffice/schema/__init__.py

key-decisions:
  - "Initialized SchemaConverter directly within Router, defaulting to InMemorySchemaRegistry if unspecified"
  - "Automatically determined wire format direction: wire_format_in applies only to non-JSON input, wire_format_out only to non-JSON output"
  - "Excluded schema and transformation metadata from raw_kwargs passed into broker client publish()"
  - "On schema transformation failure, intercepted error, recorded schema_validation_error metric, dispatched to DLQ, and ACKed upstream message"

patterns-established:
  - "PostOffice exposes register_schema, get_schema, get_schema_by_id delegating to Router schema registry"
  - "Fanout routes can deliver different formats to different targets from a single ingress message"

requirements-completed:
  - SCH-01

coverage:
  - id: S3
    description: "Router and PostOffice dynamic schema transformation and DLQ failure routing"
    requirement: "SCH-01"
    verification:
      - kind: unit
        ref: "tests/test_schema_routing.py"
        status: pass
    human_judgment: false
---

# Plan 05-02 Summary

Integrated dynamic schema transformation into `Router` and `PostOffice`, enabling automatic cross-format conversion (JSON ↔ Apache Avro ↔ Protocol Buffers) across messaging brokers, with DLQ fallback on validation failures, verified with 6 integration tests in `tests/test_schema_routing.py`.

## Key Changes
1. **`src/postoffice/router.py`:**
   - Accepted `schema_registry` in `Router.__init__` and initialized `self.converter = SchemaConverter(...)`.
   - In `route()`, inspected `route_config` for `input_format`, `input_schema`, `output_format`, `output_schema`, and `wire_format`.
   - Converted incoming payloads dynamically before calling `clients[target_client].publish(...)`.
   - On transformation failure, recorded metric `schema_validation_error`, dispatched failed message to DLQ with error details, and released upstream messages via `ack_fn()`.
2. **`src/postoffice/app.py`:**
   - Accepted `schema_registry` parameter in `PostOffice.__init__` and preserved it across `reset()`.
   - Added `register_schema`, `get_schema`, and `get_schema_by_id` facade methods.
   - Cleanly passed schema parameters through `add_route()`.
3. **`tests/test_schema_routing.py`:**
   - 6 integration tests verifying JSON -> Avro (Confluent wire format), JSON -> Protobuf, Avro -> JSON, DLQ fallback on validation error, heterogeneous multi-format fanout routing, and facade schema registry methods.

## Verification
- `PYTHONPATH=src .venv/bin/python -m unittest tests/test_schema_routing.py` (6 tests in 0.003s, OK)
- `PYTHONPATH=src .venv/bin/python -m unittest discover tests/` (63 tests in 0.531s, OK)
