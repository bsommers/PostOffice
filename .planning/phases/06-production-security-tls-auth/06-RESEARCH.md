# Phase 6: Production Security, TLS/mTLS & Authentication — Research & Driver Specifications

**Phase:** 06-production-security-tls-auth
**Date:** 2026-10-13
**Tags:** security, tls, mtls, sasl, mqtt, amqp, kafka, pika, paho-mqtt, confluent-kafka

---

## 1. Unified Security Configuration Model

To ensure consistency across heterogeneous protocols, PostOffice introduces `SecurityConfig`:

```python
from dataclasses import dataclass
from typing import Optional, Dict, Any
import ssl

@dataclass
class SecurityConfig:
    tls_enabled: bool = False
    ca_cert: Optional[str] = None               # Path to CA certificate
    client_cert: Optional[str] = None           # Path to client certificate (mTLS)
    client_key: Optional[str] = None            # Path to client private key (mTLS)
    client_key_password: Optional[str] = None   # Optional private key passphrase
    insecure_skip_verify: bool = False          # Skip certificate validation (testing only)
    username: Optional[str] = None              # Auth username
    password: Optional[str] = None              # Auth password
    sasl_mechanism: Optional[str] = None        # PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
```

---

## 2. Plugin Driver Configurations

### 2.1 MQTT & NanoMQ (`paho-mqtt`)
- **Username / Password:**
  `client.username_pw_set(security.username, security.password)`
- **TLS & mTLS:**
  ```python
  if security.tls_enabled:
      cert_reqs = ssl.CERT_NONE if security.insecure_skip_verify else ssl.CERT_REQUIRED
      client.tls_set(
          ca_certs=security.ca_cert,
          certfile=security.client_cert,
          keyfile=security.client_key,
          cert_reqs=cert_reqs,
          tls_version=ssl.PROTOCOL_TLS_CLIENT
      )
      if security.insecure_skip_verify:
          client.tls_insecure_set(True)
  ```
- **Port Defaults:** Standard TLS port for MQTT is `8883` (vs `1883` plaintext).

### 2.2 AMQP / RabbitMQ (`pika`)
- **Credentials:**
  `credentials = pika.PlainCredentials(security.username, security.password)`
- **SSLOptions & SSLContext:**
  ```python
  ssl_options = None
  if security.tls_enabled:
      context = ssl.create_default_context(cafile=security.ca_cert)
      if security.client_cert and security.client_key:
          context.load_cert_chain(
              certfile=security.client_cert,
              keyfile=security.client_key,
              password=security.client_key_password
          )
      if security.insecure_skip_verify:
          context.check_hostname = False
          context.verify_mode = ssl.CERT_NONE

      ssl_options = pika.SSLOptions(context, server_hostname=host)
  ```
- **Port Defaults:** Standard TLS port for AMQP is `5671` (vs `5672` plaintext).

### 2.3 Apache Kafka (`confluent-kafka`)
`confluent-kafka` uses `librdkafka` configuration properties:
- **Protocol Resolution:**
  ```python
  has_tls = security.tls_enabled
  has_sasl = bool(security.username and security.sasl_mechanism)

  if has_tls and has_sasl:
      conf["security.protocol"] = "SASL_SSL"
  elif has_tls:
      conf["security.protocol"] = "SSL"
  elif has_sasl:
      conf["security.protocol"] = "SASL_PLAINTEXT"
  else:
      conf["security.protocol"] = "PLAINTEXT"
  ```
- **TLS Configuration:**
  - `ssl.ca.location = security.ca_cert`
  - `ssl.certificate.location = security.client_cert`
  - `ssl.key.location = security.client_key`
  - `ssl.key.password = security.client_key_password`
  - `enable.ssl.certificate.verification = not security.insecure_skip_verify`
- **SASL Configuration:**
  - `sasl.mechanism = security.sasl_mechanism.upper()`
  - `sasl.username = security.username`
  - `sasl.password = security.password`

---

## 3. Hermetic Testing Strategy

In unit tests, generating live OpenSSL cert files or testing against remote brokers with self-signed certificates adds flakiness.
Instead:
1. `tests/test_security_config.py`: Verifies `SecurityConfig` validation, dictionary serialization, and default value behavior.
2. `tests/test_plugin_security.py`: Verifies that `MqttClient`, `AmqpClient`, `KafkaClient`, and `NanoMqClient` invoke the correct driver methods (`tls_set`, `SSLOptions`, `security.protocol`, `sasl.mechanism`) when supplied with a `SecurityConfig`.
3. `tests/test_secure_mesh.py`: Tests `PostOffice` and `ControlPlane` wiring with security configurations across multiple brokers.
