# Other Industries

[← Use cases](README.md) | [← Project README](../../README.md)

The patterns repeat. Each row names a domain, what flows, and which pattern from the [index](README.md#reusable-patterns) applies. Snippets use the same API as the other guides.

## Table of Contents
- [Energy and utilities](#energy-and-utilities)
- [Logistics and fleet](#logistics-and-fleet)
- [Smart buildings and facilities](#smart-buildings-and-facilities)
- [Healthcare devices](#healthcare-devices)
- [Retail and point of sale](#retail-and-point-of-sale)
- [Agriculture and environment](#agriculture-and-environment)
- [Finance and trading operations](#finance-and-trading-operations)
- [Software platforms and SaaS](#software-platforms-and-saas)

## Energy and utilities

Smart meters and substations publish on MQTT. The utility's billing system uses RabbitMQ and the grid analytics team uses Kafka.

```python
po.subscribe("meters_mqtt", "grid/+/+/reading", qos=1)
po.add_route("meters_mqtt", "grid/+/+/reading", "kafka_bus", "grid.readings",
             input_format="json", output_format="avro", output_schema="MeterReading", wire_format=True)
po.add_route("meters_mqtt", "grid/+/+/reading", "billing_rabbit", "billing.readings")
```
Patterns: fan-in, fan-out, format gateway.

## Logistics and fleet

Vehicle trackers send position on MQTT. Dispatch needs it in a queue; analytics in Kafka. Commands (re-route) return over the command path.

```python
po.subscribe("tracker_mqtt", "fleet/+/position", qos=0)
po.add_route("tracker_mqtt", "fleet/+/position", "dispatch_rabbit", "dispatch.positions")
po.subscribe("dispatch_rabbit", "dispatch.reroute", exchange="dispatch", exchange_type="topic")
po.add_route("dispatch_rabbit", "dispatch.reroute", "tracker_mqtt", "fleet/commands/reroute", qos=1)
```
Patterns: fan-in, command return path. Use QoS 0 for high-frequency positions where occasional loss is acceptable.

## Smart buildings and facilities

HVAC, lighting and access systems publish on MQTT; the facilities platform consumes from Kafka; alarms go to the on-call queue.

```python
po.add_route("bms_mqtt", "building/+/floor/+/hvac", "kafka_bus", "facilities.hvac")
po.add_route("bms_mqtt", "building/+/floor/+/alarm", "oncall_rabbit", "oncall.facilities")
```
Patterns: fan-in, alert routing.

## Healthcare devices

Bedside monitors and wearables publish telemetry; a clinical platform consumes it. Use TLS/mTLS everywhere; payloads may be sensitive.

```python
from postoffice.security import SecurityConfig
sec = SecurityConfig(tls_enabled=True)   # add CA/cert/key paths for mTLS
po.add_broker("device_mqtt", "mqtt", host="devices.hospital.local", port=8883, security_config=sec)
```
Patterns: shield, quarantine. Regulated data needs your own compliance review; PostOffice logs no payload bodies at INFO and above, but a DLQ stores full payloads. Protect the DLQ topic accordingly. See [security guide](../user-guide/security-and-auth.md).

## Retail and point of sale

Stores publish sales events locally; head office wants them centralised, converted to a common schema, with store outages tolerated.

```python
po.add_broker("store_kafka", "kafka", bootstrap_servers="hq-kafka:9092",
              enable_publish_buffer=True, max_publish_buffer_size=10000,
              circuit_breaker_threshold=3)
po.add_route("store_mqtt", "store/+/sale", "store_kafka", "retail.sales",
             input_format="json", output_format="avro", output_schema="Sale", wire_format=True)
```
Patterns: shield, format gateway.

## Agriculture and environment

Field sensors report soil moisture and weather over long-range links, aggregated by a gateway broker. Bridge to the cloud with a small buffer; drop oldest on long outages.
Patterns: shield, fan-in.

## Finance and trading operations

Use for *operational* feeds (reference-data updates, trade-event notifications to back-office queues, risk alerts), not as the transaction path. Make consumers idempotent and read [fit and limits](fit-and-limits.md) first.
Patterns: fan-out, quarantine.

## Software platforms and SaaS

Bridge customer-facing MQTT/AMQP integrations to your internal Kafka event backbone. Per-tenant topic prefixes (`tenant/<id>/events`) with a wildcard route onboard tenants without code changes.

```python
po.add_route("tenant_mqtt", "tenant/+/events", "kafka_bus", "platform.tenant.events")
```
Patterns: fan-in, control-plane rollout.
