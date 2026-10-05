# Technology Stack

**Analysis Date:** 2026-10-05

## Languages

**Primary:**
- Python 3.12+ (tested with Python 3.13/3.14) - All application, plugin, router, and test code

**Secondary:**
- Bash / Shell - Startup and local execution scripts
- YAML - Docker Compose orchestration configuration (`docker-compose.yml`)
- INI / Conf - Mosquitto broker configuration (`configs/mosquitto.conf`)

## Runtime

**Environment:**
- Python 3.12+ runtime
- Docker engine & Docker Compose for multi-broker infrastructure (Kafka, Zookeeper, RabbitMQ, Mosquitto, NanoMQ, Redis)

**Package Manager:**
- `uv` (recommended) or `pip`
- Manifest: `requirements.txt`
- Virtual environment: `.venv` (standard local venv)

## Frameworks

**Core:**
- Built-in `abc` (Abstract Base Classes) for `BaseClient` interface definition
- Built-in `threading` for background worker consumer loops and thread-safe callbacks
- Custom pluggable registry pattern via `ClientRegistry` decorator

**Testing:**
- Standard library `unittest`
- Standard library `unittest.mock` (`MagicMock`, `patch`) for mocking broker network connections

**Build/Dev:**
- `uv` for lightning-fast virtual environment management and dependency resolution

## Key Dependencies

**Critical:**
- `paho-mqtt` (2.1.0) - MQTT client library (v2 callback API) for Mosquitto and NanoMQ edge broker connectivity
- `pika` (1.4.4) - RabbitMQ / AMQP 0-9-1 client using `BlockingConnection` with thread-safe callbacks
- `confluent-kafka` (2.15.1) - High-performance Apache Kafka client (C-based `librdkafka` bindings) for Producer and Consumer
- `redis` (8.1.0) - Redis client providing cluster state storage (hashes) and real-time Pub/Sub configuration event propagation

## Configuration

**Environment:**
- `REDIS_HOST` - Hostname/IP of Redis server for distributed Control/Data plane coordination (default: `localhost`)
- `PYTHONPATH` - Requires `src` directory on path (`PYTHONPATH=src`)

**Service Configurations:**
- `docker-compose.yml` - Defines multi-broker service cluster:
  - `zookeeper` on port `2181`
  - `kafka` on port `9092` (internal `29092`)
  - `rabbitmq` on ports `5672` (AMQP) and `15672` (Management UI)
  - `mosquitto` on ports `1883` (MQTT) and `9001` (WebSockets)
  - `nanomq` on port `1884` (mapped to container port `1883`)
  - `redis` on port `6379`
- `configs/mosquitto.conf` - Listener configuration allowing anonymous access for local development

## Platform Requirements

**Development:**
- Linux / macOS / Windows with Docker installed
- Python 3.12+ with `requirements.txt` installed

**Production:**
- Containerized deployment (Docker / Kubernetes) with network access to messaging broker clusters and Redis

---

*Stack analysis: 2026-10-05*
