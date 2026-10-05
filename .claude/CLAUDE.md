<!-- GSD:project-start source:PROJECT.md -->

## Project

**PostOffice**

PostOffice is a multi-protocol messaging system and translation bridge that routes data between MQTT, AMQP (RabbitMQ), Apache Kafka, and NanoMQ edge brokers. It provides a uniform programmatic interface for local applications alongside a distributed architecture with a Redis-backed Control Plane and horizontally scalable Data Plane workers.

**Core Value:** Seamlessly route and translate messaging payloads and delivery semantics across heterogeneous pub/sub, streaming, and queueing protocols with zero message loss.

### Constraints

- Runtime: Python 3.12+
- Dependencies: `paho-mqtt`, `pika`, `confluent-kafka`, `redis`
- Hermetic testability: Unit tests must execute cleanly without external network broker dependencies via mocking.

---
*Last updated: 2026-10-05*
<!-- GSD:project-end -->

<!-- GSD:stack-start source:codebase/STACK.md -->

## Technology Stack

## Languages

- Python 3.12+ (tested with Python 3.13/3.14) - All application, plugin, router, and test code
- Bash / Shell - Startup and local execution scripts
- YAML - Docker Compose orchestration configuration (`docker-compose.yml`)
- INI / Conf - Mosquitto broker configuration (`configs/mosquitto.conf`)

## Runtime

- Python 3.12+ runtime
- Docker engine & Docker Compose for multi-broker infrastructure (Kafka, Zookeeper, RabbitMQ, Mosquitto, NanoMQ, Redis)
- `uv` (recommended) or `pip`
- Manifest: `requirements.txt`
- Virtual environment: `.venv` (standard local venv)

## Frameworks

- Built-in `abc` (Abstract Base Classes) for `BaseClient` interface definition
- Built-in `threading` for background worker consumer loops and thread-safe callbacks
- Custom pluggable registry pattern via `ClientRegistry` decorator
- Standard library `unittest`
- Standard library `unittest.mock` (`MagicMock`, `patch`) for mocking broker network connections
- `uv` for lightning-fast virtual environment management and dependency resolution

## Key Dependencies

- `paho-mqtt` (2.1.0) - MQTT client library (v2 callback API) for Mosquitto and NanoMQ edge broker connectivity
- `pika` (1.4.4) - RabbitMQ / AMQP 0-9-1 client using `BlockingConnection` with thread-safe callbacks
- `confluent-kafka` (2.15.1) - High-performance Apache Kafka client (C-based `librdkafka` bindings) for Producer and Consumer
- `redis` (8.1.0) - Redis client providing cluster state storage (hashes) and real-time Pub/Sub configuration event propagation

## Configuration

- `REDIS_HOST` - Hostname/IP of Redis server for distributed Control/Data plane coordination (default: `localhost`)
- `PYTHONPATH` - Requires `src` directory on path (`PYTHONPATH=src`)
- `docker-compose.yml` - Defines multi-broker service cluster:
- `configs/mosquitto.conf` - Listener configuration allowing anonymous access for local development

## Platform Requirements

- Linux / macOS / Windows with Docker installed
- Python 3.12+ with `requirements.txt` installed
- Containerized deployment (Docker / Kubernetes) with network access to messaging broker clusters and Redis

<!-- GSD:stack-end -->

<!-- GSD:conventions-start source:CONVENTIONS.md -->

## Conventions

## Naming Patterns

- Source files: `snake_case.py` (e.g., `base_client.py`, `mqtt_client.py`)
- Test files: `test_<module>.py` in `tests/` directory (e.g., `test_postoffice.py`, `test_wildcards.py`)
- Configuration files: standard lower-case or specific formats (`docker-compose.yml`, `requirements.txt`, `mosquitto.conf`)
- `PascalCase` for all classes (`BaseClient`, `ClientRegistry`, `PostOffice`, `Router`, `ControlPlane`, `DataPlane`)
- No special prefixes/suffixes for interfaces; abstract classes inherit from `abc.ABC`
- `snake_case()` for all public methods and functions (`add_broker`, `add_route`, `subscribe`, `publish`, `on_message`, `connect`, `disconnect`)
- `_snake_case()` with leading underscore for internal helpers and thread worker loops (`_topic_match`, `_consume_loop`, `_publish_update`, `_listen_for_updates`)
- `snake_case` for local variables and attributes (`source_broker`, `target_topic`, `matching_routes`)
- `UPPER_SNAKE_CASE` for global constants or environment variable keys (`REDIS_HOST`, `PYTHONPATH`)

## Code Style & Formatting

- Standard PEP 8 conventions (4 spaces indentation, no tabs)
- Clean, readable line lengths (~88-100 chars)
- Double quotes used for strings and docstrings, single quotes for dict keys or simple literals
- Broadly utilized from `typing` module (`Dict`, `List`, `Any`, `Optional`, `Type`, `Callable`)
- Method signatures annotated for public APIs

## Import Organization

- Blank line separating standard library, third-party, and first-party modules.

## Error Handling & Concurrency

- Validation errors: raise standard `ValueError` with descriptive message (e.g. `raise ValueError(f"Broker with name '{name}' already exists.")`)
- Connection/Network failures: caught and logged via `logger.error(...)` to prevent unhandled process crashes where possible
- Thread-safe scheduling: In `AmqpClient`, use `self.connection.add_callback_threadsafe(...)` for cross-thread channel operations
- Daemon threads for background loops (`thread.daemon = True`)
- Clean shutdown via `threading.Event()` (`self._stop_event.set()` and `thread.join(timeout=2)`)
- Mutex locks (`threading.Lock()`) protecting shared queues/lists across threads (e.g., in `KafkaClient`)

<!-- GSD:conventions-end -->

<!-- GSD:architecture-start source:ARCHITECTURE.md -->

## Architecture

## Pattern Overview

- Protocol-agnostic routing core with MQTT-style topic matching and wildcards (`+`, `#`).
- Three-tier indirection: Facade (`PostOffice`) -> Routing & Factory Engine (`Router`, `ClientRegistry`) -> Pluggable Protocol Adapters (`BaseClient` plugins).
- Distributed split: Administrative Control Plane writes to Redis and broadcasts real-time configuration events; horizontally scalable Data Plane workers ingest state and route traffic without cluster restarts.

## Layers

- Purpose: Unified public API for configuring, starting, and stopping multi-protocol bridges.
- Contains: `PostOffice` class.
- Depends on: `Router`, `ClientRegistry`, and registered plugins.
- Used by: Single-process scripts, embedded applications, and `DataPlane` worker instances.
- Purpose: Maintains routing tables and performs topic matching between source and destination endpoints.
- Contains: `Router` class with wildcard matching logic (`_topic_match`).
- Depends on: Protocol adapter interfaces.
- Used by: `PostOffice` facade and `BaseClient` callbacks.
- Purpose: Abstract base class for messaging clients and a decorator-based factory registry (`@ClientRegistry.register`).
- Contains:
- Depends on: Underlying protocol client SDKs (`paho-mqtt`, `pika`, `confluent-kafka`).
- Used by: `PostOffice` and `Router`.
- Purpose: Decouples routing configuration management from message forwarding execution.
- Contains:
- Depends on: Redis and `PostOffice`.
- Used by: Ops/DevOps via `scripts/admin.py` or automated orchestrators.

## Data Flow

## Key Abstractions

- Purpose: Defines standard interface contract for any messaging protocol.
- Pattern: Abstract Template Method / Strategy.
- Location: `src/postoffice/base_client.py`.
- Purpose: Decouples plugin registration and creation from the application facade.
- Pattern: Factory Method & Decorator Registry.
- Location: `src/postoffice/registry.py`.
- Purpose: In-memory mapping and topic matching engine.
- Pattern: Mediator / Router.
- Location: `src/postoffice/router.py`.

## Entry Points

- **Library Facade:** `from postoffice.app import PostOffice`
- **Data Plane Worker:** `python src/postoffice/worker.py` (reads `REDIS_HOST` environment variable)
- **Admin Script:** `python scripts/admin.py` (populates Redis rules)

## Cross-Cutting Concerns

- **Threading & Concurrency:** Background consumer threads in `AmqpClient` (`_consume_loop`), `KafkaClient` (`_consume_loop`), and `DataPlane` (`_listen_for_updates`).
- **Thread Safety:** `pika.BlockingConnection` requires `connection.add_callback_threadsafe(...)` for cross-thread calls.
- **Logging:** Standard library `logging` configured across all modules.

<!-- GSD:architecture-end -->

<!-- GSD:skills-start source:skills/ -->

## Project Skills

No project skills found. Add skills to any of: `.claude/skills/`, `.agents/skills/`, `.cursor/skills/`, `.github/skills/`, or `.codex/skills/` with a `SKILL.md` index file.
<!-- GSD:skills-end -->

<!-- GSD:workflow-start source:GSD defaults -->

## GSD Workflow Enforcement

Before using Edit, Write, or other file-changing tools, start work through a GSD command so planning artifacts and execution context stay in sync.

Use these entry points:

- `/gsd-quick` for small fixes, doc updates, and ad-hoc tasks
- `/gsd-debug` for investigation and bug fixing
- `/gsd-execute-phase` for planned phase work

Do not make direct repo edits outside a GSD workflow unless the user explicitly asks to bypass it.
<!-- GSD:workflow-end -->

<!-- GSD:profile-start -->

## Developer Profile

> Profile not yet configured. Run `/gsd-profile-user` to generate your developer profile.
> This section is managed by `generate-claude-profile` -- do not edit manually.
<!-- GSD:profile-end -->
