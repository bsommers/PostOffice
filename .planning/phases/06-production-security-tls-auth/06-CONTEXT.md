# Phase 6: Production Security, TLS/mTLS & Authentication — Context & Decisions

**Phase:** 06-production-security-tls-auth
**Milestone:** v2.0
**Requirements:** SEC-01
**Created:** 2026-10-13

---

## 1. Background & Scope

PostOffice connects heterogeneous messaging protocols across perimeter boundaries: IoT edge devices in the field (MQTT/NanoMQ), internal enterprise queues (RabbitMQ/AMQP), and centralized streaming data lakes (Apache Kafka). Production readiness requires encrypted transport (TLS 1.2+), mutual certificate verification (mTLS), and standardized credential authentication (Username/Password, SASL PLAIN, and SASL SCRAM).

Phase 6 hardens PostOffice with unified security configuration, strict default verification policies, and driver-specific translation for all broker plugins.

---

## 2. Locked Decisions

### Decision 1: Unified `SecurityConfig` Contract
- **Choice:** Unified `SecurityConfig` dataclass and dictionary specification in `src/postoffice/security.py`.
- **Fields:**
  - `tls_enabled: bool = False`
  - `ca_cert: Optional[str] = None` (Path to CA certificate bundle or PEM string)
  - `client_cert: Optional[str] = None` (Path to client public certificate for mTLS)
  - `client_key: Optional[str] = None` (Path to client private key for mTLS)
  - `client_key_password: Optional[str] = None` (Optional passphrase for private key)
  - `insecure_skip_verify: bool = False` (Explicit opt-in to disable CA/hostname checks in testing)
  - `username: Optional[str] = None`
  - `password: Optional[str] = None`
  - `sasl_mechanism: Optional[str] = None` (e.g. `'PLAIN'`, `'SCRAM-SHA-256'`, `'SCRAM-SHA-512'`)
- **Translation:** Each client plugin translates `SecurityConfig` into its native driver arguments (`paho-mqtt` `tls_set`/`username_pw_set`, `pika` `ssl_options`/`PlainCredentials`, `confluent-kafka` configuration dictionaries).

### Decision 2: Authentication Scope
- **MQTT / NanoMQ:** Username/Password credentials and mTLS client certificates.
- **AMQP (RabbitMQ):** `pika.PlainCredentials` (or `EXTERNAL` for mTLS) with `ssl.SSLContext`.
- **Kafka:** Standard production matrix:
  - `SSL` (mTLS client certificates)
  - `SASL_PLAINTEXT` (Username/Password via PLAIN or SCRAM)
  - `SASL_SSL` (TLS transport + SASL PLAIN, SCRAM-SHA-256, or SCRAM-SHA-512)

### Decision 3: Strict Verification Policy by Default
- **Policy:** CA verification and hostname checks are strictly enabled by default whenever TLS is active.
- **Opt-Out:** Bypassing verification (e.g. for self-signed certificates in local staging) requires explicitly setting `insecure_skip_verify=True`. When set, PostOffice logs a security warning.

---

## 3. Plan Decomposition

- **Plan 06-01 (Wave 1):** Security Configuration & Plugin Driver Hardening
  - Create `src/postoffice/security.py` (`SecurityConfig`, TLS validation helpers).
  - Update `MqttClient`, `NanoMqClient`, `AmqpClient`, `KafkaClient` to apply `SecurityConfig`.
  - Hermetic unit tests in `tests/test_security_config.py` and `tests/test_plugin_security.py`.

- **Plan 06-02 (Wave 2):** Facade Integration, Control Plane Persistence & End-to-End Tests
  - Update `PostOffice.add_broker()` to accept `security_config` directly or via kwargs.
  - Update `ControlPlane` and `DataPlane` to persist and synchronize security parameters through Redis.
  - Comprehensive end-to-end integration tests in `tests/test_secure_mesh.py`.
