# IoT and Edge Computing

[← Use cases](README.md) | [← Project README](../../README.md)

## Table of Contents
- [The situation](#the-situation)
- [Scenario 1: device fleet ingestion](#scenario-1-device-fleet-ingestion)
- [Scenario 2: edge gateway with an unreliable uplink](#scenario-2-edge-gateway-with-an-unreliable-uplink)
- [Scenario 3: commands and configuration to devices](#scenario-3-commands-and-configuration-to-devices)
- [Scenario 4: rotating device-broker credentials without downtime](#scenario-4-rotating-device-broker-credentials-without-downtime)
- [Scenario 5: protocol migration](#scenario-5-protocol-migration)
- [Benefits summary](#benefits-summary)

## The situation

Devices want lightweight protocols (MQTT). Back-end services want durable queues and logs (AMQP, Kafka). Edge locations have limited CPU and flaky links. PostOffice runs as a small Python process (or fleet of workers) that:

- subscribes to a device broker such as NanoMQ, built for edge use;
- forwards upstream with the right format and delivery semantics;
- keeps going when the upstream is unavailable.

## Scenario 1: device fleet ingestion

Thousands of devices publish to `fleet/<device-id>/telemetry`. A single route handles the whole fleet.

```python
po = PostOffice(metrics_port=9100, dlq_broker="kafka_core", dlq_topic="deadletter.fleet")
po.add_broker("edge_mqtt", "nanomq", host="localhost", port=1883)
po.add_broker("kafka_core", "kafka", bootstrap_servers="kafka.cloud:9092")

po.subscribe("edge_mqtt", "fleet/+/telemetry", qos=1)
po.add_route("edge_mqtt", "fleet/+/telemetry", "kafka_core", "fleet.telemetry")
po.start()
```

New devices need no router change. The matching engine is a trie, so lookup cost depends on topic depth rather than on the number of routes; see [routing](../user-guide/routing-and-wildcards.md).

## Scenario 2: edge gateway with an unreliable uplink

A site gateway runs a local broker and PostOffice. When the cellular link drops, uplink publishes fail.

```python
po.add_broker(
    "kafka_core", "kafka", bootstrap_servers="kafka.cloud:9092",
    enable_publish_buffer=True, max_publish_buffer_size=20000,
    buffer_overflow_policy="drop_oldest",
    circuit_breaker_threshold=3, circuit_breaker_timeout=60.0,
)
```

While the breaker is open, messages queue in memory up to the limit. On recovery the buffer flushes. Size the buffer from your outage length times message rate, and remember it is **not** persistent across a restart. For long outages or critical data, pair it with a persistent local queue; see [fit and limits](fit-and-limits.md).

## Scenario 3: commands and configuration to devices

Back-end services publish commands to RabbitMQ; PostOffice delivers them to the device's topic.

```python
po.add_broker("core_rabbit", "amqp", host="rabbit.cloud")
po.subscribe("core_rabbit", "fleet.commands", exchange="devices", exchange_type="topic")
po.add_route("core_rabbit", "fleet.commands", "edge_mqtt", "fleet/commands", qos=1)
```

Use a fixed topic string per route for non-MQTT sources; see [topic dialects](fit-and-limits.md#topic-dialects).

## Scenario 4: rotating device-broker credentials without downtime

Security teams rotate passwords and certificates. PostOffice can hot-reload them on a live client:

```python
import os

from postoffice.security import SecurityConfig

po.reload_broker_security("edge_mqtt", SecurityConfig(username="gateway", password=os.environ["NEW_SECRET"]))
```

Across many workers, the Control Plane can broadcast `update_broker_security` through Redis. Treat Redis as a privileged channel and secure it (password, TLS, network isolation); see [security guide](../user-guide/security-and-auth.md) and the [security scan](../../.security-reports/scan-2026-10-25.md).

## Scenario 5: protocol migration

Moving a fleet from MQTT to Kafka-first, or from RabbitMQ to Kafka, takes months. Run PostOffice as the compatibility layer: old consumers keep their queue while new consumers read the Kafka topic, and a route feeds both.

```python
po.add_route("edge_mqtt", "fleet/+/telemetry", "kafka_core", "fleet.telemetry")
po.add_route("edge_mqtt", "fleet/+/telemetry", "core_rabbit", "legacy.telemetry")
```

Both routes fire for each message (fan-out), and each can have its own DLQ.

## Benefits summary

| Pain | With PostOffice |
|---|---|
| Heavy cloud SDKs on small devices | Devices stay on MQTT; bridging happens at the edge |
| Link drops lose data | Circuit breaker + bounded buffer ride out short outages |
| Credential rotation means restarts | Hot-reload on live clients |
| Big-bang protocol migration | Fan-out to old and new during the transition |
