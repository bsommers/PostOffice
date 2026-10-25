# Industrial IoT and Manufacturing

[← Use cases](README.md) | [← Project README](../../README.md)

## Table of Contents
- [The situation](#the-situation)
- [Scenario 1: plant telemetry to the data platform](#scenario-1-plant-telemetry-to-the-data-platform)
- [Scenario 2: alarms to the maintenance workflow](#scenario-2-alarms-to-the-maintenance-workflow)
- [Scenario 3: setpoints and recipes back to the line](#scenario-3-setpoints-and-recipes-back-to-the-line)
- [Scenario 4: multi-plant consolidation](#scenario-4-multi-plant-consolidation)
- [Scenario 5: surviving a network or broker outage](#scenario-5-surviving-a-network-or-broker-outage)
- [Benefits summary](#benefits-summary)

## The situation

A plant floor produces a steady stream of small messages: temperatures, vibration, counters, cycle times, alarms. Machines and gateways publish them on **MQTT** (often to a local NanoMQ or Mosquitto broker). Meanwhile:

- the **data platform** wants them in **Kafka**, in a compact typed format;
- **maintenance and MES systems** work through **RabbitMQ** queues;
- each plant has different topic conventions;
- the plant network is not always reliable.

## Scenario 1: plant telemetry to the data platform

Every machine publishes `plant1/line3/press7/temperature`. One wildcard route collects all of them and converts JSON to Avro for Kafka.

```python
import os

from postoffice.app import PostOffice

po = PostOffice(metrics_port=9100, dlq_broker="kafka_bus", dlq_topic="deadletter.plant1")

po.add_broker("plant_mqtt", "nanomq", host="plant-broker.local", port=1883)
po.add_broker("kafka_bus", "kafka", bootstrap_servers="kafka.corp:9092")

po.register_schema("MachineReading", {
    "type": "record", "name": "MachineReading",
    "fields": [
        {"name": "value", "type": "double"},
        {"name": "unit", "type": "string"},
    ],
}, schema_type="AVRO")

po.subscribe("plant_mqtt", "plant1/+/+/temperature", qos=1)
po.add_route(
    source_broker="plant_mqtt",
    source_topic="plant1/+/+/temperature",
    target_broker="kafka_bus",
    target_topic="plant1.temperature",
    input_format="json",
    output_format="avro",
    output_schema="MachineReading",
    wire_format=True,
)
po.start()
```

**Why it helps:** adding line 4 or press 8 needs no change. The `+` wildcards cover them. Readings that fail schema conversion (for example a sensor sending text) land in the DLQ instead of poisoning the analytics topic.

## Scenario 2: alarms to the maintenance workflow

Alarms are rare but important. Route them to RabbitMQ, where the maintenance system consumes from a durable queue.

```python
po.add_broker("maint_rabbit", "amqp", host="rabbit.corp", port=5672,
              user="svc_postoffice", password=os.environ["AMQP_PASSWORD"])
po.subscribe("plant_mqtt", "plant1/+/+/alarm", qos=2)
po.add_route("plant_mqtt", "plant1/+/+/alarm", "maint_rabbit", "maintenance.alarms")
```

Read the credentials from the environment in real deployments rather than writing them in code; see [security and auth](../user-guide/security-and-auth.md) for `SecurityConfig` and TLS.

The router maps MQTT QoS to AMQP delivery mode, so an alarm sent at QoS 1 or 2 becomes a persistent AMQP message. See [delivery guarantees](../user-guide/delivery-guarantees.md).

## Scenario 3: setpoints and recipes back to the line

Traffic is not only upward. A planning system publishes a recipe change to RabbitMQ; PostOffice delivers it to the line's MQTT topic.

```python
po.subscribe("maint_rabbit", "commands.line3.#", exchange="mes", exchange_type="topic")
po.add_route("maint_rabbit", "commands.line3.#", "plant_mqtt", "plant1/line3/commands", qos=1)
```

Note that AMQP routing keys use `.` while MQTT uses `/`. PostOffice's wildcard matching speaks MQTT; for non-MQTT sources use patterns that match the exact topic strings the source delivers. See [fit and limits](fit-and-limits.md#topic-dialects).

## Scenario 4: multi-plant consolidation

Each plant runs its own broker. One PostOffice (or one per region) bridges all of them into a single Kafka cluster, tagging data by topic prefix.

```python
for plant in ("plant1", "plant2", "plant3"):
    po.add_broker(f"{plant}_mqtt", "nanomq", host=f"{plant}-broker.local", port=1883)
    po.subscribe(f"{plant}_mqtt", f"{plant}/#", qos=1)
    po.add_route(f"{plant}_mqtt", f"{plant}/#", "kafka_bus", f"{plant}.raw")
```

For many sites, run **Data Plane workers** per region and manage routes centrally with the **Control Plane**; see [distributed plane](../user-guide/distributed-plane.md).

## Scenario 5: surviving a network or broker outage

Turn on circuit breakers and a bounded publish buffer so a Kafka outage does not stall the plant bridge or lose a burst of readings.

```python
po.add_broker(
    "kafka_bus", "kafka", bootstrap_servers="kafka.corp:9092",
    enable_publish_buffer=True, max_publish_buffer_size=50000,
    buffer_overflow_policy="drop_oldest",
    circuit_breaker_threshold=5, circuit_breaker_timeout=30.0,
)
```

After five consecutive failures the breaker opens, new messages are held in the buffer, and the bridge probes for recovery every 30 seconds. `drop_oldest` favours fresh telemetry; use `reject` where every message matters and you would rather push back on the source. The buffer lives in memory, so it does not survive a process restart. See [resiliency](../user-guide/resiliency-and-circuit-breakers.md) and [fit and limits](fit-and-limits.md).

## Benefits summary

| Pain | With PostOffice |
|---|---|
| One bridge script per machine type | One wildcard route per signal family |
| Raw JSON bloating the data lake | Avro/Protobuf conversion at the boundary |
| Silent loss when Kafka is down | Breaker + buffer + DLQ, plus metrics to see it |
| Plant-by-plant configuration drift | Central routes pushed to workers through Redis |
| Operators blind to bridge health | Prometheus metrics per broker |
