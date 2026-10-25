# Business Management

[← Use cases](README.md) | [← Project README](../../README.md)

## Table of Contents
- [The situation](#the-situation)
- [Scenario 1: an operational event bus](#scenario-1-an-operational-event-bus)
- [Scenario 2: KPI and alert feeds](#scenario-2-kpi-and-alert-feeds)
- [Scenario 3: cross-department workflow triggers](#scenario-3-cross-department-workflow-triggers)
- [Scenario 4: governed change of routing rules](#scenario-4-governed-change-of-routing-rules)
- [Scenario 5: bridge health as a business metric](#scenario-5-bridge-health-as-a-business-metric)
- [Benefits summary](#benefits-summary)

## The situation

Management needs timely, trustworthy signals: orders per hour, line downtime, stock-outs, SLA breaches. Those signals originate in different systems. PostOffice acts as the **connecting layer**, so each department publishes events in its own way while managers' dashboards and alert engines receive a consistent stream.

## Scenario 1: an operational event bus

Sales, production, logistics and support each publish business events. Normalise them onto one Kafka namespace.

```python
po.add_route("sales_rabbit", "sales.#", "kafka_bus", "biz.sales")
po.add_route("prod_mqtt",    "factory/+/+/summary", "kafka_bus", "biz.production")
po.add_route("logi_rabbit",  "shipments.#", "kafka_bus", "biz.logistics")
```

BI tools and alerting consume `biz.*` and never need to know which system produced each event.

## Scenario 2: KPI and alert feeds

Route threshold events to the channel that the on-call or management system watches:

```python
po.add_route("prod_mqtt", "factory/+/+/downtime", "ops_rabbit", "alerts.downtime",
             dlq_broker="ops_rabbit", dlq_topic="dlq.alerts")
```

If alert delivery fails, the event is preserved in `dlq.alerts` rather than lost, and the DLQ counter flags the problem.

## Scenario 3: cross-department workflow triggers

A completed production batch should trigger quality inspection and invoicing prep. Fan out one event to both departments' queues:

```python
po.add_route("prod_mqtt", "factory/+/batch/complete", "qa_rabbit", "qa.inspect")
po.add_route("prod_mqtt", "factory/+/batch/complete", "fin_rabbit", "finance.invoice.prep")
```

## Scenario 4: governed change of routing rules

Business rules change (a new region, a new reporting stream). With the Control Plane, an administrator adds a route centrally and every worker applies it live; no deployments are needed.

```python
from postoffice.control_plane import ControlPlane

cp = ControlPlane(redis_host="redis.corp")
cp.add_route("prod_mqtt", "factory/+/+/scrap", "kafka_bus", "biz.scrap")
```

Redis holds the route table, so it is also the audit point: restrict who can write to it. See [distributed plane](../user-guide/distributed-plane.md) and secure Redis (password, TLS, private network).

## Scenario 5: bridge health as a business metric

If integration is business-critical, its health should be visible to managers. Enable metrics and scrape them with Prometheus:

```python
po = PostOffice(metrics_port=9100)
```

Alert on the DLQ counter and on breaker state; see [observability](../user-guide/observability-and-dlq.md). By default the metrics server listens on all interfaces without authentication, so keep it on a private network (a loopback default is a tracked improvement).

## Benefits summary

| Pain | With PostOffice |
|---|---|
| Dashboards need bespoke feeds per department | One namespace on Kafka |
| Alerts lost when the notifier is down | DLQ plus counter |
| Rule changes need a release | Control Plane pushes routes live |
| Nobody knows if integration is healthy | Metrics for rates, errors and DLQ volume |
