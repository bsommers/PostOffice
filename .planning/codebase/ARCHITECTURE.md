# Architecture

**Analysis Date:** 2026-10-05

## Pattern Overview

**Overall:** Decoupled Multi-Protocol Routing Facade with Pluggable Adapters and Distributed Control/Data Plane Architecture.

**Key Characteristics:**
- Protocol-agnostic routing core with MQTT-style topic matching and wildcards (`+`, `#`).
- Three-tier indirection: Facade (`PostOffice`) -> Routing & Factory Engine (`Router`, `ClientRegistry`) -> Pluggable Protocol Adapters (`BaseClient` plugins).
- Distributed split: Administrative Control Plane writes to Redis and broadcasts real-time configuration events; horizontally scalable Data Plane workers ingest state and route traffic without cluster restarts.

## Layers

**1. Facade Layer (`src/postoffice/app.py`):**
- Purpose: Unified public API for configuring, starting, and stopping multi-protocol bridges.
- Contains: `PostOffice` class.
- Depends on: `Router`, `ClientRegistry`, and registered plugins.
- Used by: Single-process scripts, embedded applications, and `DataPlane` worker instances.

**2. Routing Engine Layer (`src/postoffice/router.py`):**
- Purpose: Maintains routing tables and performs topic matching between source and destination endpoints.
- Contains: `Router` class with wildcard matching logic (`_topic_match`).
- Depends on: Protocol adapter interfaces.
- Used by: `PostOffice` facade and `BaseClient` callbacks.

**3. Plugin & Registry Layer (`src/postoffice/registry.py`, `src/postoffice/base_client.py`, `src/postoffice/plugins/`):**
- Purpose: Abstract base class for messaging clients and a decorator-based factory registry (`@ClientRegistry.register`).
- Contains:
  - `BaseClient`: Abstract methods `connect()`, `disconnect()`, `subscribe()`, `publish()`, and common `on_message()` callback dispatch.
  - `ClientRegistry`: Plugin lookup and instantiation factory.
  - Concrete plugins: `MqttClient`, `AmqpClient`, `KafkaClient`, `NanoMqClient`.
- Depends on: Underlying protocol client SDKs (`paho-mqtt`, `pika`, `confluent-kafka`).
- Used by: `PostOffice` and `Router`.

**4. Distributed Coordination Layer (`src/postoffice/control_plane.py`, `src/postoffice/data_plane.py`, `src/postoffice/worker.py`):**
- Purpose: Decouples routing configuration management from message forwarding execution.
- Contains:
  - `ControlPlane`: Administrative client writing configurations (brokers, routes, subscriptions) into Redis and broadcasting change events.
  - `DataPlane`: Horizontally scalable worker that boots up an internal `PostOffice` instance, syncs state from Redis, and listens for live config broadcasts.
  - `worker.py`: CLI entry point running `DataPlane.run()`.
- Depends on: Redis and `PostOffice`.
- Used by: Ops/DevOps via `scripts/admin.py` or automated orchestrators.

## Data Flow

**Message Routing Flow:**
1. **Ingress:** An external broker delivers a message to a connected `BaseClient` implementation (via background thread, consumer poll, or callback).
2. **Standardization:** Concrete client converts broker message to bytes and calls `self.on_message(topic, message, **kwargs)`.
3. **Dispatch:** `BaseClient.on_message` delegates to `Router.route(source_client, topic, message, **kwargs)`.
4. **Matching:** `Router` evaluates all registered routes for `source_client` against incoming topic using `_topic_match(route_topic, message_topic)`.
5. **Egress:** For every match, `Router` extracts target broker and publishes via `target_client.publish(target_topic, message, **target_kwargs)`.

**Configuration Update Flow:**
1. Administrator calls `ControlPlane.add_route(...)` or registers a broker.
2. `ControlPlane` updates Redis hashes (`postoffice:routes`, `postoffice:brokers`).
3. `ControlPlane` publishes a JSON payload to `postoffice:config_updates`.
4. Connected `DataPlane` workers receive the event via Redis Pub/Sub thread.
5. Workers dynamically apply updates to their internal `PostOffice` instances without dropping connections.

## Key Abstractions

**`BaseClient`:**
- Purpose: Defines standard interface contract for any messaging protocol.
- Pattern: Abstract Template Method / Strategy.
- Location: `src/postoffice/base_client.py`.

**`ClientRegistry`:**
- Purpose: Decouples plugin registration and creation from the application facade.
- Pattern: Factory Method & Decorator Registry.
- Location: `src/postoffice/registry.py`.

**`Router`:**
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

---

*Architecture analysis: 2026-10-05*
