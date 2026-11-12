---
phase: 02-semantic-translation-delivery-guarantees
plan: 02
subsystem: plugins
tags:
  - kafka
  - amqp
  - mqtt
  - delivery-guarantees
  - tests
  - unittest

requires:
  - 02-01-PLAN.md
provides:
  - Kafka delivery report confirmation and error callbacks
  - AMQP publish confirm and consumer ack/nack callbacks
  - MQTT publish confirm and QoS metadata propagation
  - Complete hermetic unit test suite in tests/test_delivery_guarantees.py
affects:
  - plugins
  - tests
  - ROADMAP.md
  - REQUIREMENTS.md

tech-stack:
  added: []
  patterns:
    - Broker callback bridging to uniform on_confirm/on_error protocol
    - Multi-target fanout countdown verification in unit tests

key-files:
  created:
    - tests/test_delivery_guarantees.py
  modified:
    - src/postoffice/plugins/kafka_client.py
    - src/postoffice/plugins/amqp_client.py
    - src/postoffice/plugins/mqtt_client.py

key-decisions:
  - "Bridged Kafka produce delivery_report callback to invoke on_confirm on success and on_error on failure"
  - "Wired AMQP consumer auto_ack=False mode to provide thread-safe basic_ack and basic_nack callbacks to on_message"
  - "Propagated MQTT msg.qos into on_message extra kwargs to trigger automatic Router semantic translation"

patterns-established:
  - "Broker plugins invoke on_confirm upon successful publish and on_error when publish fails"
  - "Hermetic unit tests verify confirmation callbacks, fanout countdown latches, and QoS <-> delivery_mode translation"

requirements-completed:
  - SEM-01
  - SEM-02

coverage:
  - id: D1
    description: "End-to-end delivery confirmation coordination between ingress and egress protocols"
    requirement: "SEM-01"
    verification:
      - kind: unit
        ref: "tests/test_delivery_guarantees.py"
        status: pass
    human_judgment: false
  - id: D2
    description: "Automatic translation between MQTT QoS and AMQP delivery_mode with explicit override support"
    requirement: "SEM-02"
    verification:
      - kind: unit
        ref: "tests/test_delivery_guarantees.py"
        status: pass
    human_judgment: false
---

# Plan 02-02 Summary

Wired delivery confirmation callbacks across `KafkaClient`, `AmqpClient`, and `MqttClient`, and implemented comprehensive hermetic unit tests verifying delivery guarantees and semantic translations.

## Key Changes
1. **`src/postoffice/plugins/kafka_client.py`:**
   - Updated `KafkaClient.publish` to accept `on_confirm` and `on_error` and wire them into `delivery_report`.
   - Updated `_consume_loop` to provide `ack_fn` that commits message offset asynchronously upon downstream acknowledgement.
2. **`src/postoffice/plugins/amqp_client.py`:**
   - Updated `AmqpClient.publish` to accept and execute `on_confirm` / `on_error`.
   - Updated `_on_message` to supply thread-safe `ack_fn` (`basic_ack`) and `nack_fn` (`basic_nack`) when `auto_ack=False`.
3. **`src/postoffice/plugins/mqtt_client.py`:**
   - Updated `MqttClient.publish` to accept and invoke `on_confirm` / `on_error`.
   - Updated `_on_message` to forward `msg.qos` to `Router.route`.
4. **`tests/test_delivery_guarantees.py`:**
   - Created 11 hermetic unit tests verifying:
     - Single-target `ack_fn` and `nack_fn` propagation
     - Multi-target fanout latch ("All-Succeed" policy: `ack_fn` fired only after all targets confirm)
     - Multi-target error short-circuit (`nack_fn` fired on any target failure)
     - Fire-and-forget backward compatibility
     - MQTT QoS 0/1/2 <-> AMQP `delivery_mode` 1/2 bidirectional auto-translation
     - Explicit route kwargs overrides
     - Plugin-specific confirmation and error handling for Kafka, AMQP, and MQTT.

## Verification
- `PYTHONPATH=src .venv/bin/python -m unittest tests/test_delivery_guarantees.py` (11 tests in 0.005s, OK)
- `PYTHONPATH=src .venv/bin/python -m unittest discover tests/` (22 tests in 0.019s, OK)
