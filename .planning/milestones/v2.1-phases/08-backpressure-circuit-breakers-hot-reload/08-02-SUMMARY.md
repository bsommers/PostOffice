---
phase: 08-backpressure-circuit-breakers-hot-reload
plan: 02
subsystem: security-hot-reload
tags: [security, tls, mtls, sasl, hot-reload, control-plane, data-plane]
requires:
  - 08-01
provides:
  - "BaseClient.reload_security() contract implemented across MqttClient, NanoMqClient, AmqpClient, and KafkaClient"
  - "PostOffice.reload_broker_security() facade method"
  - "ControlPlane.update_broker_security() Redis persistence and Pub/Sub broadcast"
  - "DataPlane live 'update_broker_security' event handler"
affects:
  - src/postoffice/base_client.py
  - src/postoffice/plugins/mqtt_client.py
  - src/postoffice/plugins/nanomq_client.py
  - src/postoffice/plugins/amqp_client.py
  - src/postoffice/plugins/kafka_client.py
  - src/postoffice/app.py
  - src/postoffice/control_plane.py
  - src/postoffice/data_plane.py
  - tests/test_credential_reload.py
tech-stack:
  added: []
  patterns:
    - "Zero-downtime in-flight TLS/credential rotation with automatic subscription and publish buffer preservation"
key-files:
  created:
    - tests/test_credential_reload.py
  modified:
    - src/postoffice/base_client.py
    - src/postoffice/plugins/mqtt_client.py
    - src/postoffice/plugins/nanomq_client.py
    - src/postoffice/plugins/amqp_client.py
    - src/postoffice/plugins/kafka_client.py
    - src/postoffice/app.py
    - src/postoffice/control_plane.py
    - src/postoffice/data_plane.py
decisions:
  - "Define DEFAULT_PLAINTEXT_PORT and DEFAULT_TLS_PORT on MqttClient (1883/8883) and NanoMqClient (1884/8884) so hot-reloading TLS dynamically shifts default ports."
  - "Preserve all active subscriptions and buffered publishes across live security reloads on all four broker adapters."
metrics:
  duration: 10m
  completed_date: "2026-10-18"
requirements:
  - RES-03
---

# Phase 8 Plan 02: Dynamic TLS Certificate & Credential Hot-Reload Summary

## Accomplishments
- Implemented `reload_security()` across `BaseClient`, `MqttClient`, `NanoMqClient`, `AmqpClient`, and `KafkaClient` with automatic port shifting and subscription/buffer preservation.
- Added `PostOffice.reload_broker_security()`, `ControlPlane.update_broker_security()`, and `DataPlane` live `"update_broker_security"` Pub/Sub handling.
- Verified with 5 hermetic unit and distributed integration tests in `tests/test_credential_reload.py`.
