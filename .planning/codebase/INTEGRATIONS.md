# External Integrations

**Analysis Date:** 2026-10-05

## Message Brokers & Protocols

**MQTT Broker (Eclipse Mosquitto):**
- Purpose: Primary central MQTT broker for lightweight topic-based pub/sub
- Protocol: MQTT 3.1.1 / 5.0
- Client: `paho.mqtt.client` (v2 callback API)
- Default Port: `1883` (TCP), `9001` (WebSocket)
- Authentication: Configurable; default anonymous for development (`configs/mosquitto.conf`)
- Routing Model: Hierarchical topic strings with single-level (`+`) and multi-level (`#`) wildcards
- Delivery Semantics: QoS 0 (at most once), QoS 1 (at least once), QoS 2 (exactly once)

**Edge Broker (EMQX NanoMQ):**
- Purpose: Lightweight edge MQTT broker for edge/IoT gateways
- Protocol: MQTT 3.1.1 / 5.0
- Client: `NanoMqClient` (subclass of `MqttClient` in `src/postoffice/plugins/nanomq_client.py`)
- Default Port: `1884` (mapped to container port `1883`)
- Capabilities: Shared subscriptions (`$share/<group>/<topic>`) for horizontal load-balancing across edge data plane workers

**AMQP Broker (RabbitMQ):**
- Purpose: Enterprise message queuing and exchange-based routing
- Protocol: AMQP 0-9-1
- Client: `pika.BlockingConnection` with thread-safe callbacks
- Default Ports: `5672` (AMQP), `15672` (Management UI)
- Authentication: Plain credentials (default `user`/`password` in `docker-compose.yml`)
- Routing Model: Exchanges (direct, topic, fanout, headers) bound to Queues via routing keys
- Delivery Semantics: Delivery mode 1 (transient) vs 2 (persistent), publisher confirms, message ACKs

**Distributed Event Streaming (Apache Kafka):**
- Purpose: Distributed commit log and event streaming backbone
- Protocol: Kafka binary protocol
- Client: `confluent_kafka.Producer` and `confluent_kafka.Consumer`
- Default Port: `9092` (internal `29092`)
- Dependency: Apache Zookeeper on port `2181`
- Routing Model: Topics partitioned across brokers, message keys for partition affinity
- Delivery Semantics: Consumer group tracking (`auto.offset.reset: earliest`), delivery report callbacks

## Data Storage & Coordination

**Distributed State Store (Redis):**
- Purpose: Central configuration registry and live update broadcasting channel for Control Plane and Data Plane workers
- Client: `redis.Redis` (Python client)
- Default Port: `6379`
- Key Schema:
  - `postoffice:brokers` (Hash): Stores broker definition payloads JSON-encoded `{ "protocol": ..., "kwargs": ... }` keyed by broker identifier
  - `postoffice:routes` (Hash): Stores routing definitions JSON-encoded keyed by `{source_broker}:{source_topic}->{target_broker}:{target_topic}`
  - `postoffice:subscriptions` (Hash): Stores edge subscription parameters JSON-encoded keyed by `{broker_name}:{topic}`
  - `postoffice:config_updates` (Pub/Sub Channel): Real-time broadcast channel emitting update events (`add_broker`, `add_route`, `add_subscription`, `clear_state`) to synchronized workers

---

*Integrations analysis: 2026-10-05*
