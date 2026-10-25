# Data Systems and Processing

[← Use cases](README.md) | [← Project README](../../README.md)

## Table of Contents
- [The situation](#the-situation)
- [Scenario 1: governed ingestion into Kafka](#scenario-1-governed-ingestion-into-kafka)
- [Scenario 2: one stream, many consumers](#scenario-2-one-stream-many-consumers)
- [Scenario 3: quarantine and replay of bad data](#scenario-3-quarantine-and-replay-of-bad-data)
- [Scenario 4: feeding ML and feature pipelines](#scenario-4-feeding-ml-and-feature-pipelines)
- [Scenario 5: shared schemas across workers](#scenario-5-shared-schemas-across-workers)
- [Where PostOffice stops](#where-postoffice-stops)
- [Benefits summary](#benefits-summary)

## The situation

Data teams get inputs from many producers that never agreed on a format. Ingestion code ends up full of "parse, check, convert, retry, give up" logic, and a single malformed message can stall a pipeline. PostOffice moves that work to the boundary:

- **convert** between JSON, Avro and Protobuf (with Confluent wire framing, so Schema Registry-style consumers work);
- **quarantine** failures in a dead-letter topic with the reason attached;
- **route** by topic pattern so new sources join without code changes.

## Scenario 1: governed ingestion into Kafka

Producers send JSON over MQTT or AMQP. Downstream consumers expect Avro with a registered schema.

```python
po = PostOffice(dlq_broker="kafka_bus", dlq_topic="deadletter.ingest")
po.add_broker("mqtt_in", "mqtt", host="ingest.local", port=1883)
po.add_broker("kafka_bus", "kafka", bootstrap_servers="kafka:9092")

po.register_schema("Event", {
    "type": "record", "name": "Event",
    "fields": [{"name": "id", "type": "string"}, {"name": "ts", "type": "long"}],
}, schema_type="AVRO")

po.subscribe("mqtt_in", "events/#", qos=1)
po.add_route("mqtt_in", "events/#", "kafka_bus", "events.avro",
             input_format="json", output_format="avro",
             output_schema="Event", wire_format=True)
po.start()
```

Only valid, schema-conformant records reach `events.avro`; consumers need no defensive parsing.

## Scenario 2: one stream, many consumers

The same source feeds a lake sink, a real-time dashboard and an audit queue, each in its own format:

```python
po.add_route("mqtt_in", "events/#", "kafka_bus", "events.avro",
             input_format="json", output_format="avro", output_schema="Event", wire_format=True)
po.add_route("mqtt_in", "events/#", "kafka_bus", "events.json")             # raw copy
po.add_route("mqtt_in", "events/#", "rabbit_audit", "audit.events")         # queue for audit
```

Each route is independent. Failure of one target is handled per route (its own DLQ if you set `dlq_broker`/`dlq_topic` on the route).

## Scenario 3: quarantine and replay of bad data

A producer ships a bad release and starts sending malformed JSON. Instead of failing the pipeline:

1. The route's conversion fails.
2. The original payload, source topic, and error message are wrapped in a DLQ envelope and published to `deadletter.ingest`.
3. The `dlq_messages` Prometheus counter rises, which alerting can watch.
4. After the producer is fixed, replay from the DLQ topic with your own consumer.

See [observability and DLQ](../user-guide/observability-and-dlq.md) for the envelope format.

## Scenario 4: feeding ML and feature pipelines

Feature pipelines need stable, typed inputs. A Protobuf route gives training and serving code one contract:

```python
po.add_route("mqtt_in", "features/+/raw", "kafka_bus", "features.proto",
             input_format="json", output_format="protobuf",
             output_schema="FeatureVector", wire_format=True)
```

Register `FeatureVector` first with `schema_type="PROTOBUF"`. See [schema transformation](../user-guide/schema-transformation.md).

## Scenario 5: shared schemas across workers

When several workers run, schemas should be shared. `RedisSchemaRegistry` stores them in Redis:

```python
import redis
from postoffice.schema.registry import RedisSchemaRegistry

po = PostOffice(schema_registry=RedisSchemaRegistry(redis.Redis(decode_responses=True)))
```

Library users can do this today. The standalone Data Plane worker currently uses an in-memory registry (a tracked improvement; see [fit and limits](fit-and-limits.md)).

## Where PostOffice stops

It is a **router and converter**, not a stream processor. It does not join, window or aggregate. Put Kafka Streams, Flink, Spark or SQL engines after Kafka for that work. PostOffice's job is to deliver clean, typed events to them.

## Benefits summary

| Pain | With PostOffice |
|---|---|
| Format sprawl across producers | Conversion at the edge of the platform |
| One bad message blocks the pipeline | DLQ quarantine with reason |
| Ad-hoc ingestion scripts | Declarative routes |
| No ingestion visibility | Prometheus counters per broker and DLQ |
