# Troubleshooting & FAQ

[← Back to User Guide](README.md) | [← Back to Project README](../../README.md)

Common issues, diagnostics, error messages, and their solutions when developing and deploying PostOffice.

---

## Table of Contents
- [Diagnostic Checklist](#diagnostic-checklist)
- [Common Error Messages](#common-error-messages)
  - [`NameError: name 'Optional' is not defined` or Missing Imports](#missing-imports)
  - [`Schema transformation failed: Schema ID X not found in registry`](#schema-id-not-found)
  - [`Invalid wire format magic byte: expected 0x00`](#invalid-magic-byte)
  - [`DLQ recursion detected: message originated from DLQ`](#dlq-recursion-detected)
  - [`Target client X not registered`](#target-client-not-registered)
  - [`CircuitBreakerOpenError: Circuit breaker for broker 'X' is OPEN`](#circuit-breaker-open)
  - [`BufferOverflowError: Publish buffer for client 'X' is full`](#publish-buffer-overflow)
  - [`Redis connection error in update listener: Retrying in 1.0s...`](#redis-connection-error)
- [Frequently Asked Questions](#frequently-asked-questions)

---

## Diagnostic Checklist

1. **Verify Python Path:** Ensure `PYTHONPATH=src` is set before running any script or test command:
   ```bash
   export PYTHONPATH=src
   ```
2. **Inspect Prometheus Metrics:** Check routing error counters and durations:
   ```bash
   curl -s http://localhost:9100/metrics | grep postoffice_
   ```
3. **Inspect Dead Letter Queue:** If messages are not arriving at their destination, check if they are being routed to your configured DLQ broker and topic.
4. **Run Hermetic Unit Tests:** Confirm that the local environment and dependencies are functioning:
   ```bash
   PYTHONPATH=src .venv/bin/python -m unittest discover tests/
   ```

---

## Common Error Messages

### Missing Imports
- **Symptom:** `NameError: name 'Optional' is not defined` or `ModuleNotFoundError: No module named 'postoffice.schema'`.
- **Cause:** Missing import in file or running python without `PYTHONPATH=src`.
- **Solution:** Execute scripts with `PYTHONPATH=src .venv/bin/python <script.py>` and verify imports from `typing` or `postoffice.*`.

### Schema ID Not Found
- **Symptom:** `Schema transformation error: Schema ID 42 from wire format header not found in registry`.
- **Cause:** An incoming message contains Confluent wire format framing pointing to a schema ID that has not been registered in the local or Redis schema registry.
- **Solution:** Register the corresponding schema using `postoffice.register_schema()` before starting routes, or use `RedisSchemaRegistry` connected to a shared schema database.

### Invalid Magic Byte
- **Symptom:** `Schema transformation error: Invalid wire format magic byte: expected 0x00, got 0x7b`.
- **Cause:** `wire_format_in=True` or `wire_format=True` was configured on a route whose incoming payload is raw JSON (`0x7b` is ASCII `{`) instead of Confluent wire format.
- **Solution:** Set `wire_format=False` or omit it. When routing from JSON to Avro/Protobuf with `wire_format=True`, PostOffice automatically only applies wire formatting to the outgoing message.

### DLQ Recursion Detected
- **Symptom:** `DLQ recursion detected: message originated from DLQ <broker>/<topic>. Dropping.`
- **Cause:** A message originated from the exact same broker and topic as the designated Dead Letter Queue destination, and failed routing. Forwarding it would cause an infinite recursive storm.
- **Solution:** PostOffice drops the message and calls `nack_fn(requeue=False)` to prevent infinite loops. Inspect the message payload at the source to resolve formatting or routing bugs.

### Target Client Not Registered
- **Symptom:** `Target client 'kafka_main' not registered`.
- **Cause:** `add_route()` was called referencing a target broker name that was not registered with `add_broker()`.
- **Solution:** Ensure `po.add_broker("kafka_main", ...)` is called before `add_route()`.

### Circuit Breaker Open
- **Symptom:** `CircuitBreakerOpenError: Circuit breaker for broker 'kafka_main' is OPEN` (and `postoffice_circuit_breaker_state{target_broker="kafka_main"} 1.0`).
- **Cause:** Consecutive publish failures to `kafka_main` reached `failure_threshold`, tripping its egress circuit breaker to `OPEN`, and either `enable_publish_buffer=False` or the client's publish buffer reached capacity.
- **Solution:** Enable in-flight buffering on the target broker (`enable_publish_buffer=True`, `max_publish_buffer_size=2000`, `buffer_overflow_policy="drop_oldest"`) so messages queue in memory during short outages and flush automatically when the `HALF_OPEN` probe succeeds.

### Publish Buffer Overflow
- **Symptom:** `BufferOverflowError: Publish buffer for client 'mqtt_edge' is full (max_size=1000)`.
- **Cause:** The broker is disconnected (or its circuit breaker is `OPEN`), `enable_publish_buffer=True` with `buffer_overflow_policy="reject"`, and buffered messages exceeded `max_publish_buffer_size`.
- **Solution:** Increase `max_publish_buffer_size` or switch to `buffer_overflow_policy="drop_oldest"` if retaining the freshest telemetry is preferred over rejecting new messages to DLQ.

### Redis Connection Error
- **Symptom:** `Redis connection error in update listener: Connection refused. Retrying in 1.0s...`
- **Cause:** Data Plane worker cannot reach Redis on `localhost:6379`.
- **Solution:** Start the local Redis broker stack using `docker compose up -d redis` or specify the correct Redis host and port when launching `DataPlane(redis_host="...", redis_port=...)`.

---

## Frequently Asked Questions

### Does PostOffice require Docker to run unit tests?
**No.** All 106 unit tests in the `tests/` directory hermetically mock external brokers and Redis offline. Tests execute in ~0.4s without any external services running.

### How fast is the TopicTrie matching engine?
The `TopicTrie` operates in $O(k)$ time relative to topic depth $k$ rather than route count $N$. At 10,000 registered routes, `TopicTrie` lookups take ~26 microseconds (over 648x faster than linear scanning).

### How does PostOffice handle poison-pill messages?
When an unroutable message or malformed payload arrives, PostOffice routes the original payload and error forensic details into a Dead Letter Queue. Once safely persisted in the DLQ, PostOffice issues an upstream acknowledgment (`ack_fn()`) to remove the poison pill from the ingress queue.

