---
phase: 07-operational-resiliency-client-queuing
plan: 02
subsystem: client-queuing
tags: [resiliency, queuing, amqp, kafka, router, fanout]
requires:
  - 07-01
provides:
  - "AmqpClient offline subscription queuing and replay on channel creation"
  - "AmqpClient and KafkaClient bounded publish buffering and automatic flush on connect"
  - "KafkaClient pre-connect subscription queuing before consumer initialization"
  - "End-to-end Router + _FanoutCoordinator deferred acknowledgement and overflow-to-DLQ verification"
affects:
  - src/postoffice/plugins/amqp_client.py
  - src/postoffice/plugins/kafka_client.py
  - tests/test_client_queuing.py
tech-stack:
  added: []
  patterns:
    - "Deferred upstream ACK across offline target broker buffer flush via _FanoutCoordinator"
key-files:
  created: []
  modified:
    - src/postoffice/plugins/amqp_client.py
    - src/postoffice/plugins/kafka_client.py
    - tests/test_client_queuing.py
decisions:
  - "Replay AMQP subscriptions and flush publish buffer inside _consume_loop immediately after channel establishment and before setting _connected_event."
  - "Allow KafkaClient.subscribe() to queue into _pending_subscriptions before connect() creates the Consumer instance."
metrics:
  duration: 10m
  completed_date: "2026-10-17"
requirements:
  - RES-01
---

# Phase 7 Plan 02: AMQP & Kafka Offline Queuing, Reconnect Replay & E2E Mesh Verification Summary

## Accomplishments
- Updated `AmqpClient` to queue subscriptions registered before `connect()` and replay them with full `exchange`, `exchange_type`, `queue`, and `auto_ack` parameters when `_consume_loop` opens the channel.
- Enabled bounded publish buffering and automatic buffer flushing on `AmqpClient` and `KafkaClient`.
- Updated `KafkaClient.subscribe()` to record and queue subscriptions even when `self.consumer is None` (pre-start).
- Verified end-to-end `Router` + `_FanoutCoordinator` integration: upstream `ack_fn` is held while messages are buffered for an offline target broker and triggered once the target connects and flushes, while `BufferOverflowError` routes cleanly to the Dead Letter Queue.
