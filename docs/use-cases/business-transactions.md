# Business Transactions

[← Use cases](README.md) | [← Project README](../../README.md)

## Table of Contents
- [The situation](#the-situation)
- [Important: delivery semantics first](#important-delivery-semantics-first)
- [Scenario 1: order events across systems](#scenario-1-order-events-across-systems)
- [Scenario 2: inventory and fulfilment updates](#scenario-2-inventory-and-fulfilment-updates)
- [Scenario 3: payment and settlement notifications](#scenario-3-payment-and-settlement-notifications)
- [Scenario 4: notifications to customers and partners](#scenario-4-notifications-to-customers-and-partners)
- [Scenario 5: legacy system integration](#scenario-5-legacy-system-integration)
- [Benefits summary](#benefits-summary)

## The situation

Orders, payments, stock changes and shipping events flow between a storefront, an ERP, warehouse systems and a data warehouse. These are usually on different brokers because they were chosen at different times by different teams. Business transactions care about **not losing events** and **seeing what failed**.

## Important: delivery semantics first

PostOffice targets **at-least-once** delivery. It acknowledges a source message only after routing, and it diverts failures to a DLQ. That means:

- **Consumers must be idempotent** (use order IDs or event IDs to deduplicate). Duplicates are possible after retries or restarts.
- It does **not** provide exactly-once or distributed transactions.
- Review [fit and limits](fit-and-limits.md) before using it for money movement. Some adapter confirmation work is still on the roadmap.

Use PostOffice to **propagate events about** transactions. Keep the transaction of record in the system that owns it.

## Scenario 1: order events across systems

The storefront publishes `OrderPlaced` to RabbitMQ. The ERP, warehouse and analytics all need it.

```python
po = PostOffice(metrics_port=9100, dlq_broker="rabbit_core", dlq_topic="dlq.orders")
po.add_broker("rabbit_core", "amqp", host="rabbit.corp")
po.add_broker("kafka_bus", "kafka", bootstrap_servers="kafka.corp:9092")

po.subscribe("rabbit_core", "orders.placed", exchange="commerce", exchange_type="topic",
             queue="postoffice.orders", auto_ack=False)
po.add_route("rabbit_core", "orders.placed", "kafka_bus", "orders.placed",
             dlq_broker="rabbit_core", dlq_topic="dlq.orders")
po.start()
```

`auto_ack=False` (the default in current code) means RabbitMQ keeps the message until the router has finished with it.

## Scenario 2: inventory and fulfilment updates

Warehouse scanners publish stock moves on MQTT. Route them to the ERP queue and the analytics stream:

```python
po.subscribe("wh_mqtt", "warehouse/+/stock", qos=1)
po.add_route("wh_mqtt", "warehouse/+/stock", "rabbit_core", "inventory.updates")
po.add_route("wh_mqtt", "warehouse/+/stock", "kafka_bus", "inventory.stream")
```

Add a circuit breaker on `rabbit_core` so an ERP maintenance window buffers updates rather than failing scanners.

## Scenario 3: payment and settlement notifications

Payment providers emit status events. Route them from Kafka to a RabbitMQ queue consumed by finance:

```python
po.subscribe("kafka_bus", "payments.status")
po.add_route("kafka_bus", "payments.status", "rabbit_core", "finance.payments",
             dlq_broker="rabbit_core", dlq_topic="dlq.payments")
```

Anything that cannot be delivered ends up in `dlq.payments` with the reason. Finance can inspect and replay. Do not rely on PostOffice for the authoritative payment record.

## Scenario 4: notifications to customers and partners

Fan out business events to channels that already have broker-facing integrations (email service, SMS gateway, partner webhooks fed from a queue).

```python
po.add_route("kafka_bus", "orders.shipped", "rabbit_core", "notify.email")
po.add_route("kafka_bus", "orders.shipped", "rabbit_core", "notify.sms")
```

## Scenario 5: legacy system integration

A legacy ERP only reads from a RabbitMQ queue; new services publish to Kafka. PostOffice bridges them, optionally converting formats, so neither side changes:

```python
po.subscribe("kafka_bus", "erp.outbound")
po.add_route("kafka_bus", "erp.outbound", "rabbit_core", "legacy.erp.in",
             input_format="avro", output_format="json")
```

## Benefits summary

| Pain | With PostOffice |
|---|---|
| Point-to-point integrations between each pair of systems | A single routing layer |
| Failed events vanish into logs | DLQ with the original payload and error |
| New consumer means producer changes | Add a route instead |
| Legacy systems block modernisation | Bridge and convert formats |
