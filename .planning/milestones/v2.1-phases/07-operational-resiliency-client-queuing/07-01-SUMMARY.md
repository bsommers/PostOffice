---
phase: 07-operational-resiliency-client-queuing
plan: 01
subsystem: client-queuing
tags: [resiliency, queuing, mqtt, nanomq, backpressure]
requires: []
provides:
  - "BaseClient thread-safe subscription registry (_subscriptions) and bounded publish buffer (_publish_buffer)"
  - "BufferedPublish dataclass and BufferOverflowError exception with 'reject' and 'drop_oldest' policies"
  - "MqttClient and NanoMqClient offline subscription kwargs preservation (qos) and publish buffer flush on connect"
affects:
  - src/postoffice/base_client.py
  - src/postoffice/plugins/mqtt_client.py
  - src/postoffice/plugins/nanomq_client.py
  - tests/test_client_queuing.py
tech-stack:
  added: []
  patterns:
    - "Bounded collections.deque with RLock for thread-safe in-flight publish buffering"
    - "Subscription parameter snapshot replay across reconnect lifecycles"
key-files:
  created:
    - tests/test_client_queuing.py
  modified:
    - src/postoffice/base_client.py
    - src/postoffice/plugins/mqtt_client.py
decisions:
  - "Default enable_publish_buffer=False on BaseClient to preserve backward compatibility with immediate error callbacks while allowing opt-in bounded buffering per broker."
  - "Support 'reject' and 'drop_oldest' overflow policies, invoking on_error(BufferOverflowError) on rejected or evicted messages."
metrics:
  duration: 10m
  completed_date: "2026-10-17"
requirements:
  - RES-01
---

# Phase 7 Plan 01: BaseClient Queuing Engine & MQTT/NanoMQ Integration Summary

## Accomplishments
- Implemented `BufferedPublish`, `BufferOverflowError`, thread-safe subscription registry (`record_subscription`, `get_subscriptions`), and bounded `deque` publish buffer (`buffer_publish`, `flush_publish_buffer`) in `BaseClient`.
- Updated `MqttClient` and `NanoMqClient` to preserve subscription `qos` and kwargs when queued offline and replayed in `_on_connect`.
- Integrated automatic FIFO publish buffer flushing in `MqttClient._on_connect`.
- Created hermetic unit tests in `tests/test_client_queuing.py` covering `BaseClient` policies and MQTT/NanoMQ replay.
