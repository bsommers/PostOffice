# PostOffice Architecture & Semantic Mapping

PostOffice acts as a multi-protocol messaging router and translator, effectively acting as "glue" between MQTT, AMQP (RabbitMQ), Kafka, and other potential systems.

## The Challenge: Semantic Mismatches

Different messaging protocols have distinct semantics, especially regarding message delivery guarantees, routing, and durability.

### 1. MQTT (Pub/Sub)
* **Design**: Lightweight, topic-based pub/sub for IoT/edge devices.
* **QoS**:
  * QoS 0 (At most once)
  * QoS 1 (At least once)
  * QoS 2 (Exactly once)
* **Durability**: Optional (via Clean Session = False and Persistent Messages).
* **Routing**: Exact topic match and wildcards (`+`, `#`).

### 2. AMQP (RabbitMQ)
* **Design**: Advanced routing, enterprise messaging, queuing.
* **QoS/Delivery**: Acknowledgements (acks/nacks), persistent messages, publisher confirms.
* **Durability**: Durable queues, persistent messages.
* **Routing**: Exchanges (Direct, Topic, Fanout, Headers) and Bindings.

### 3. Kafka (Event Streaming)
* **Design**: Distributed commit log, high-throughput event streaming.
* **QoS/Delivery**: "At least once" by default. "Exactly once" via transactional APIs. Relies on consumer offsets.
* **Durability**: Inherently durable (retained on disk based on time/size limits).
* **Routing**: Topics and Partitions (Key-based routing).

## Semantic Translation Mapping

When routing messages from one system to another, PostOffice must translate these semantics appropriately.

### MQTT -> AMQP
* **Routing**: MQTT topics map directly to AMQP Routing Keys (using Topic Exchanges). MQTT wildcards (`+`, `#`) need to be translated to AMQP wildcards (`*`, `#`).
* **Delivery**:
  * MQTT QoS 0 -> Transient AMQP message (delivery_mode=1), no publisher confirms needed.
  * MQTT QoS 1/2 -> Persistent AMQP message (delivery_mode=2). PostOffice must use Publisher Confirms in AMQP and wait for the ack before acknowledging the MQTT publish (PUBACK/PUBCOMP).

### MQTT -> Kafka
* **Routing**: MQTT topic maps to Kafka Topic. Kafka doesn't natively support wildcards on publish. The MQTT topic string can also be embedded in the Kafka message headers or payload if a generic Kafka topic is used.
* **Delivery**:
  * MQTT QoS 0 -> Kafka `acks=0`.
  * MQTT QoS 1/2 -> Kafka `acks=all`. PostOffice must wait for the Kafka producer callback before sending MQTT PUBACK.

### AMQP -> MQTT
* **Routing**: AMQP Routing Key maps to MQTT Topic.
* **Delivery**:
  * Transient AMQP -> MQTT QoS 0.
  * Persistent AMQP -> MQTT QoS 1. PostOffice acks the AMQP message only after receiving MQTT PUBACK.

### Kafka -> MQTT
* **Routing**: Kafka topic maps to MQTT topic. Kafka keys can be mapped to sub-topics (e.g., `topic/key`).
* **Delivery**: Kafka offset commit should be tied to successful MQTT delivery (QoS 1). PostOffice consumes Kafka message, publishes to MQTT, waits for PUBACK, and *then* commits the Kafka offset.

## Future Plan

1. **Robust Configuration Management**: Move routing definitions from code to a configuration file (YAML/JSON) or a dynamic configuration store (like Redis or etcd).
2. **Schema Registry & Message Transformation**: Implement a transformation engine. Messages from IoT (JSON) might need to be converted to Avro/Protobuf for Kafka.
3. **Dead Letter Queues (DLQ)**: Implement handling for unroutable messages or failed deliveries.
4. **Stateful Routing (The Router)**: Enhance the `Router` class to be aware of message states (e.g., waiting for AMQP confirm before acking MQTT). Currently, it's a simple fire-and-forget.
5. **Pluggable Architecture**: Implement dynamically loadable modules for new protocols (e.g., Redis PubSub, NATS, WebSockets).
6. **Scalability & High Availability**: PostOffice itself needs to be stateless or use a distributed state backend to allow multiple instances to run concurrently without duplicating messages.
7. **Observability**: Integrate Prometheus metrics (messages routed, latencies, error rates) and OpenTelemetry tracing.
