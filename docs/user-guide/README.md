# PostOffice User Guide

[← Back to Project README](../../README.md)

PostOffice is a high-throughput, multi-protocol messaging system and semantic translation bridge. It routes and transforms messaging payloads and delivery guarantees across MQTT, AMQP (RabbitMQ), Apache Kafka, and NanoMQ edge brokers with zero message loss.

---

## Table of Contents

- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Two-Minute Quickstart](#two-minute-quickstart)
- [Architecture & Core Concepts](#architecture--core-concepts)
- [Feature Guides](#feature-guides)
  - [Topic Routing & Wildcards](routing-and-wildcards.md)
  - [Delivery Guarantees & Semantic Translation](delivery-guarantees.md)
  - [Dynamic Schema Registry & Serialization](schema-transformation.md)
  - [Operational Resiliency, Queuing & Circuit Breakers](resiliency-and-circuit-breakers.md)
  - [Observability & Dead Letter Queues](observability-and-dlq.md)
  - [Distributed Control Plane & Data Plane](distributed-plane.md)
  - [Production Security, TLS/mTLS & Authentication](security-and-auth.md)
- [Complete API Reference](#complete-api-reference)
  - [`PostOffice` Facade](#postoffice-facade)
  - [Security Configuration (`SecurityConfig`)](#security-configuration-securityconfig)
  - [Route Parameters by Protocol](#route-parameters-by-protocol)
- [Troubleshooting & FAQ](troubleshooting.md)

---

## Getting Started

### Prerequisites

- **Python Runtime:** Python 3.12+
- **Package Manager:** `uv` (recommended) or `pip`
- **Docker & Docker Compose:** Required only for running the optional local broker stack (Mosquitto, RabbitMQ, Kafka, NanoMQ, Redis). Not required for unit tests or standalone in-memory routing.

### Installation

1. **Clone the repository:**
   ```bash
   git clone https://github.com/bsommers/PostOffice.git
   cd PostOffice
   ```

2. **Create virtual environment and install dependencies:**
   Using `uv`:
   ```bash
   uv venv
   source .venv/bin/activate
   uv pip install -r requirements.txt
   ```
   Or using standard `pip`:
   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Verify installation via hermetic test suite & coverage gate:**
   All 106 unit tests mock network clients offline and run in ~0.4s without requiring Docker:
   ```bash
   PYTHONPATH=src .venv/bin/coverage run -m unittest discover tests/ && .venv/bin/coverage report
   ```

---

## Two-Minute Quickstart

Here is a complete, standalone script demonstrating PostOffice routing messages from an edge MQTT sensor topic to an enterprise Kafka topic, complete with Prometheus metrics and Dead Letter Queue protection:

```python
import time
from postoffice.app import PostOffice

# 1. Initialize PostOffice with Prometheus metrics and Dead Letter Queue
po = PostOffice(
    metrics_port=9100,           # Exposes Prometheus /metrics on port 9100
    dlq_broker="kafka_bus",      # Failed messages divert to Kafka
    dlq_topic="system.deadletter"# Dead-letter quarantine topic
)

# 2. Register broker connections
po.add_broker("mqtt_edge", "mqtt", host="localhost", port=1883)
po.add_broker("kafka_bus", "kafka", bootstrap_servers="localhost:9092")

# 3. Configure high-speed wildcard route with QoS translation
po.add_route(
    source_broker="mqtt_edge",
    source_topic="facilities/+/temperatures",
    target_broker="kafka_bus",
    target_topic="telemetry.facilities",
    key=b"sensor_reading"
)

# 4. Subscribe edge broker to topic pattern
po.subscribe("mqtt_edge", "facilities/+/temperatures", qos=1)

# 5. Start connections and metrics HTTP server
po.start()
print("PostOffice routing active. Scrape metrics at http://localhost:9100/metrics")

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    po.stop()
    print("PostOffice stopped cleanly.")
```

---

## Architecture & Core Concepts

PostOffice uses a strict three-tier architecture:

```
+-------------------------------------------------------------------+
|                        PostOffice Facade                         |
| add_broker() | add_route() | configure_circuit_breaker() | start()|
+---------------------------------+---------------------------------+
                                  |
                                  v
+---------------------------------+---------------------------------+
|                       Router Engine Core                          |
|  TopicTrie O(k)  |  _FanoutCoordinator  |  CircuitBreaker         |
|  SchemaConverter |  Dead Letter Queue   |  MetricsManager         |
+---------------------------------+---------------------------------+
                                  |
        +-------------------------+-------------------------+
        |                         |                         |
        v                         v                         v
+---------------+         +---------------+         +---------------+
|  MQTT Plugin  |         |  AMQP Plugin  |         | Kafka Plugin  |
|  (paho-mqtt)  |         |    (pika)     |         |(confluent-k.) |
+---------------+         +---------------+         +---------------+
```

1. **Facade Layer (`src/postoffice/app.py`):** Provides a uniform programmatic contract for applications, decoupling user code from specific broker client drivers.
2. **Router Core (`src/postoffice/router.py`):**
   - **`TopicTrie`:** $O(k)$ topic prefix matching replacing linear $O(N)$ route scans.
   - **`_FanoutCoordinator`:** Atomic latch coordinating multi-target delivery confirmations.
   - **`CircuitBreaker`:** Per-target state machine (`CLOSED`, `OPEN`, `HALF_OPEN`) with publish buffer failover before DLQ drop.
   - **`_translate_semantics`:** Maps MQTT QoS levels to AMQP persistence modes.
   - **`SchemaConverter`:** Translates between JSON, Avro, and Protobuf on the fly.
3. **Pluggable Broker Clients (`src/postoffice/plugins/`):** Driver plugins implementing `BaseClient` interface (with thread-safe subscription registries, bounded publish buffers, and live `reload_security()`) for MQTT, AMQP, Kafka, and NanoMQ.
4. **Distributed Coordination:** Redis-backed `ControlPlane` and `DataPlane` workers for multi-node deployments.

---

## Feature Guides

Detailed walkthroughs and recipes for each major capability:

- **[Topic Routing & Wildcards](routing-and-wildcards.md):** Learn how MQTT-style `+` and `#` wildcards work and how the $O(k)$ TopicTrie delivers 600x+ speedups.
- **[Delivery Guarantees & Semantic Translation](delivery-guarantees.md):** Understand end-to-end acknowledgement propagation across MQTT QoS 1/2, AMQP publisher confirms, and Kafka offset commits.
- **[Dynamic Schema Registry & Serialization](schema-transformation.md):** Register Avro and Protobuf schemas, convert payloads dynamically, and format Confluent wire headers.
- **[Operational Resiliency, Queuing & Circuit Breakers](resiliency-and-circuit-breakers.md):** Configure offline subscription replay, bounded publish buffers (`reject` / `drop_oldest`), egress circuit breakers, and live credential hot-reload.
- **[Observability & Dead Letter Queues](observability-and-dlq.md):** Configure Prometheus scraping, track routing histograms, and isolate poison pills via DLQ JSON envelopes.
- **[Distributed Control Plane & Data Plane](distributed-plane.md):** Deploy horizontal DataPlane workers synchronized via Redis Pub/Sub, sign and rate-limit control events, and distribute ingress rules.
- **[Ingress Firewall, Sender Ledger & In-Flight Limits](ingress-firewall.md):** Reject oversized, malformed or abusive traffic before routing, track misbehaving senders, apply back-pressure, and cap retries.
- **[Production Security, TLS/mTLS & Authentication](security-and-auth.md):** Configure TLS socket encryption, client certificates (mTLS), SASL authentication, and zero-downtime credential rotation across all brokers.

---

## Complete API Reference

### `PostOffice` Facade

`from postoffice.app import PostOffice`

#### Constructor
```python
PostOffice(
    metrics_port: Optional[int] = None,
    dlq_broker: Optional[str] = None,
    dlq_topic: Optional[str] = None,
    schema_registry: Optional[BaseSchemaRegistry] = None
)
```
- `metrics_port`: TCP port to run Prometheus HTTP `/metrics` scrape server. If `None`, HTTP server is disabled.
- `dlq_broker`: Default broker identifier to receive failed or unroutable messages.
- `dlq_topic`: Default topic/queue identifier for dead letter messages.
- `schema_registry`: Custom `BaseSchemaRegistry` instance. Defaults to `InMemorySchemaRegistry()`.

#### Methods
- `add_broker(name: str, protocol: str, security_config: Optional[Union[SecurityConfig, dict]] = None, circuit_breaker_threshold: Optional[int] = None, circuit_breaker_timeout: float = 30.0, **kwargs) -> None`
  Registers a messaging broker with optional TLS/mTLS or SASL security settings, client publish buffer kwargs (`enable_publish_buffer`, `max_publish_buffer_size`, `buffer_overflow_policy`), and egress circuit breaker parameters. Protocol must be `'mqtt'`, `'amqp'`, `'kafka'`, or `'nanomq'`.
- `configure_circuit_breaker(broker_name: str, failure_threshold: int = 5, recovery_timeout: float = 30.0) -> CircuitBreaker`
  Configures or updates an egress `CircuitBreaker` for a registered target broker.
- `reload_broker_security(broker_name: str, security_config: Union[SecurityConfig, dict]) -> None`
  Hot-reloads TLS/mTLS certificates or SASL credentials on a registered broker in-flight without losing subscriptions or buffered messages.
- `add_route(source_broker: str, source_topic: str, target_broker: str, target_topic: str, **target_kwargs) -> None`
  Configures a routing rule with optional protocol parameters and schema transformations.
- `subscribe(broker_name: str, topic: str, **kwargs) -> None`
  Subscribes a registered broker to a topic (preserving `kwargs` across reconnects).
- `register_schema(name: str, definition: Any, schema_type: str = "AVRO") -> int`
  Registers a schema and returns its unique integer Schema ID.
- `get_schema(name: str) -> Optional[SchemaEntry]`
  Retrieves a registered schema entry by name.
- `get_schema_by_id(schema_id: int) -> Optional[SchemaEntry]`
  Retrieves a registered schema entry by ID.
- `set_dlq(broker_name: str, topic: str) -> None`
  Updates the global default Dead Letter Queue destination.
- `set_metrics_port(port: Optional[int]) -> None`
  Updates the Prometheus metrics HTTP port.
- `start() -> None`
  Connects all brokers and starts the metrics server.
- `stop() -> None`
  Disconnects all brokers and shuts down the metrics server.
- `reset() -> None`
  Tears down all connections, routes, and brokers, resetting the instance to an unconfigured state while preserving registry configurations.

---

### Security Configuration (`SecurityConfig`)

`from postoffice.security import SecurityConfig`

```python
SecurityConfig(
    tls_enabled: bool = False,
    ca_cert: Optional[str] = None,
    client_cert: Optional[str] = None,
    client_key: Optional[str] = None,
    client_key_password: Optional[str] = None,
    insecure_skip_verify: bool = False,
    username: Optional[str] = None,
    password: Optional[str] = None,
    sasl_mechanism: Optional[str] = None
)
```
- `tls_enabled`: Enables TLS encryption. Automatically switches default ports (MQTT to 8883, AMQP to 5671, NanoMQ to 8884).
- `ca_cert`: Path to Root CA certificate file for server verification.
- `client_cert` / `client_key`: Mutual TLS client certificate and private key paths.
- `insecure_skip_verify`: Bypasses TLS hostname and certificate checks (development only).
- `username` / `password`: Basic credentials for MQTT/AMQP or SASL authentication for Kafka.
- `sasl_mechanism`: Kafka SASL mechanism (`"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`). Automatically configures Kafka `security.protocol` to `SASL_SSL` or `SASL_PLAINTEXT`.

---

### Route Parameters by Protocol

When calling `add_route(...)`, protocol-specific publish parameters are passed via keyword arguments:

| Protocol | Keyword Arguments | Types & Defaults | Description |
|---|---|---|---|
| **MQTT / NanoMQ** | `qos` | `int` (0, 1, 2) | MQTT Quality of Service level |
| | `retain` | `bool` (default: `False`) | Retain message flag on broker |
| **AMQP (RabbitMQ)** | `exchange` | `str` (default: `""`) | Target AMQP exchange name |
| | `delivery_mode` | `int` (1=Transient, 2=Persistent) | Message persistence mode |
| **Apache Kafka** | `key` | `bytes` or `str` | Message key for partition hashing |
| | `partition` | `int` | Explicit target partition index |

#### Schema Transformation Parameters
Passed to `add_route(...)` to enable runtime payload conversions:

| Parameter | Type | Default | Description |
|---|---|---|---|
| `input_format` | `str` | `"json"` | Format of incoming payload (`"json"`, `"avro"`, `"protobuf"`) |
| `input_schema` | `str` | `None` | Registered schema name for input deserialization |
| `output_format` | `str` | `"json"` | Format of outgoing payload (`"json"`, `"avro"`, `"protobuf"`) |
| `output_schema` | `str` | `None` | Registered schema name for output serialization |
| `wire_format` | `bool` | `False` | Enables Confluent 5-byte wire format framing |
| `wire_format_in` | `bool` | `wire_format` | Overrides wire format framing for input only |
| `wire_format_out`| `bool` | `wire_format` | Overrides wire format framing for output only |
| `dlq_broker` | `str` | Global DLQ | Overrides dead letter queue destination broker for this route |
| `dlq_topic` | `str` | Global DLQ | Overrides dead letter queue topic for this route |
