# Production Security, TLS/mTLS & Authentication

[← Back to User Guide](README.md)

PostOffice provides an end-to-end security model securing data-in-transit across all protocol adapters (MQTT, AMQP, Apache Kafka, and NanoMQ edge brokers). The architecture features a unified configuration interface (`SecurityConfig`), strict certificate validation by default, mutual TLS (mTLS) authentication, and enterprise SASL support.

---

## Table of Contents

- [Overview](#overview)
- [Unified Security Configuration (`SecurityConfig`)](#unified-security-configuration-securityconfig)
- [Protocol-Specific Security Recipes](#protocol-specific-security-recipes)
  - [MQTT & NanoMQ (TLS & mTLS)](#mqtt--nanomq-tls--mtls)
  - [AMQP / RabbitMQ (TLS & PlainCredentials)](#amqp--rabbitmq-tls--plaincredentials)
  - [Apache Kafka (SSL & SASL SCRAM)](#apache-kafka-ssl--sasl-scram)
- [Distributed Security Sync (ControlPlane & DataPlane)](#distributed-security-sync-controlplane--dataplane)
- [Testing & Development (Skipping Verification)](#testing--development-skipping-verification)

---

## Overview

In enterprise deployments, different messaging systems enforce disparate authentication and transport encryption mechanisms:
- **MQTT / NanoMQ:** TLS socket encryption via OpenSSL and username/password credentials.
- **AMQP / RabbitMQ:** SSL context encryption wrapped in `pika.SSLOptions` with `PlainCredentials`.
- **Apache Kafka:** `librdkafka` properties resolving to `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL` with SCRAM or PLAIN mechanisms.

PostOffice unifies these disparate interfaces behind `SecurityConfig`. When a broker is registered, PostOffice automatically translates the unified configuration into driver-native primitives.

---

## Unified Security Configuration (`SecurityConfig`)

Import `SecurityConfig` from `postoffice.security`:

```python
from postoffice.security import SecurityConfig

sec = SecurityConfig(
    tls_enabled=True,
    ca_cert="/etc/ssl/certs/ca.pem",
    client_cert="/etc/ssl/certs/client-cert.pem",
    client_key="/etc/ssl/private/client-key.pem",
    client_key_password=None,
    insecure_skip_verify=False,
    username="service_account",
    password="super_secret_password",
    sasl_mechanism="SCRAM-SHA-512"
)
```

### Configuration Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `tls_enabled` | `bool` | `False` | Enables TLS transport encryption. |
| `ca_cert` | `Optional[str]` | `None` | Path to the Root CA certificate file for server validation. |
| `client_cert` | `Optional[str]` | `None` | Path to client certificate file (required for mTLS). |
| `client_key` | `Optional[str]` | `None` | Path to client private key file (required for mTLS). |
| `client_key_password` | `Optional[str]` | `None` | Optional password for encrypted private key files. |
| `insecure_skip_verify` | `bool` | `False` | Disables certificate and hostname verification (testing only). |
| `username` | `Optional[str]` | `None` | Username for basic auth or SASL credentials. |
| `password` | `Optional[str]` | `None` | Password for basic auth or SASL credentials. |
| `sasl_mechanism` | `Optional[str]` | `None` | Kafka SASL mechanism (`"PLAIN"`, `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`). |

> [!IMPORTANT]
> If `client_cert` is provided, `client_key` must also be provided (and vice versa). `SecurityConfig` enforces this constraint during validation.

---

## Protocol-Specific Security Recipes

### MQTT & NanoMQ (TLS & mTLS)

When `tls_enabled=True`, PostOffice automatically shifts the default port from `1883` to `8883` (or `1884` to `8884` for NanoMQ):

```python
from postoffice.app import PostOffice
from postoffice.security import SecurityConfig

po = PostOffice()

# Mutual TLS (mTLS) with Mosquitto or NanoMQ
sec_mqtt = SecurityConfig(
    tls_enabled=True,
    ca_cert="/certs/ca.pem",
    client_cert="/certs/client.pem",
    client_key="/certs/client.key",
    username="edge_device_01",
    password="edge_secret_token"
)

po.add_broker("mqtt_edge", "mqtt", host="mqtt.corp.internal", security_config=sec_mqtt)
```

### AMQP / RabbitMQ (TLS & PlainCredentials)

When `tls_enabled=True`, PostOffice shifts the default AMQP port from `5672` to `5671` and configures `pika.SSLOptions` with an authenticated `ssl.SSLContext`:

```python
sec_amqp = SecurityConfig(
    tls_enabled=True,
    ca_cert="/certs/ca.pem",
    client_cert="/certs/client.pem",
    client_key="/certs/client.key",
    username="rabbit_app_user",
    password="rabbit_app_password"
)

po.add_broker("rabbit_cluster", "amqp", host="amqp.corp.internal", security_config=sec_amqp)
```

### Apache Kafka (SSL & SASL SCRAM)

PostOffice dynamically determines the `security.protocol` based on your settings:
- `tls_enabled=True` and `sasl_mechanism` set -> `SASL_SSL`
- `tls_enabled=True` and no SASL -> `SSL`
- `tls_enabled=False` and `sasl_mechanism` set -> `SASL_PLAINTEXT`
- Neither set -> `PLAINTEXT`

```python
sec_kafka = SecurityConfig(
    tls_enabled=True,
    ca_cert="/certs/ca.pem",
    username="kafka_producer",
    password="scram_password",
    sasl_mechanism="SCRAM-SHA-512"
)

po.add_broker("kafka_corp", "kafka", bootstrap_servers="kafka.corp.internal:9094", security_config=sec_kafka)
```

---

## Distributed Security Sync (ControlPlane & DataPlane)

In distributed architectures, security configurations are injected into the centralized `ControlPlane` and broadcasted to worker `DataPlane` nodes over Redis:

```python
from postoffice.control_plane import ControlPlane
from postoffice.security import SecurityConfig

cp = ControlPlane(redis_host="localhost", redis_port=6379)

# Register secure broker
cp.register_broker(
    name="secure_kafka",
    protocol="kafka",
    bootstrap_servers="kafka.internal:9094",
    security_config=SecurityConfig(
        tls_enabled=True,
        ca_cert="/etc/ssl/certs/ca.pem",
        username="worker_node",
        password="worker_secret",
        sasl_mechanism="SCRAM-SHA-256"
    )
)
```

`ControlPlane` serializes `SecurityConfig` to a plain JSON dictionary stored in Redis hash `postoffice:brokers`. Any connected `DataPlane` worker receives the update message and reconstructs the secure broker client seamlessly.

---

## Testing & Development (Skipping Verification)

For local development or testing with self-signed certificates without mounting CA bundles, set `insecure_skip_verify=True`:

```python
dev_sec = SecurityConfig(
    tls_enabled=True,
    insecure_skip_verify=True
)

po.add_broker("local_secure_mqtt", "mqtt", host="localhost", port=8883, security_config=dev_sec)
```

> [!WARNING]
> Never set `insecure_skip_verify=True` in production environments, as it disables server identity validation and makes connections vulnerable to man-in-the-middle attacks.
