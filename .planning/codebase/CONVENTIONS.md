# Coding Conventions

**Analysis Date:** 2026-10-05

## Naming Patterns

**Files:**
- Source files: `snake_case.py` (e.g., `base_client.py`, `mqtt_client.py`)
- Test files: `test_<module>.py` in `tests/` directory (e.g., `test_postoffice.py`, `test_wildcards.py`)
- Configuration files: standard lower-case or specific formats (`docker-compose.yml`, `requirements.txt`, `mosquitto.conf`)

**Classes:**
- `PascalCase` for all classes (`BaseClient`, `ClientRegistry`, `PostOffice`, `Router`, `ControlPlane`, `DataPlane`)
- No special prefixes/suffixes for interfaces; abstract classes inherit from `abc.ABC`

**Functions & Methods:**
- `snake_case()` for all public methods and functions (`add_broker`, `add_route`, `subscribe`, `publish`, `on_message`, `connect`, `disconnect`)
- `_snake_case()` with leading underscore for internal helpers and thread worker loops (`_topic_match`, `_consume_loop`, `_publish_update`, `_listen_for_updates`)

**Variables & Constants:**
- `snake_case` for local variables and attributes (`source_broker`, `target_topic`, `matching_routes`)
- `UPPER_SNAKE_CASE` for global constants or environment variable keys (`REDIS_HOST`, `PYTHONPATH`)

## Code Style & Formatting

**Formatting Standards:**
- Standard PEP 8 conventions (4 spaces indentation, no tabs)
- Clean, readable line lengths (~88-100 chars)
- Double quotes used for strings and docstrings, single quotes for dict keys or simple literals

**Type Annotations:**
- Broadly utilized from `typing` module (`Dict`, `List`, `Any`, `Optional`, `Type`, `Callable`)
- Method signatures annotated for public APIs

## Import Organization

**Order:**
1. Standard library modules (`os`, `sys`, `json`, `logging`, `threading`, `abc`, `typing`, `unittest`)
2. Third-party packages (`redis`, `pika`, `paho.mqtt.client`, `confluent_kafka`)
3. Internal application modules (`postoffice.base_client`, `postoffice.registry`, `postoffice.router`, `postoffice.plugins`)

**Grouping:**
- Blank line separating standard library, third-party, and first-party modules.

## Error Handling & Concurrency

**Error Handling:**
- Validation errors: raise standard `ValueError` with descriptive message (e.g. `raise ValueError(f"Broker with name '{name}' already exists.")`)
- Connection/Network failures: caught and logged via `logger.error(...)` to prevent unhandled process crashes where possible
- Thread-safe scheduling: In `AmqpClient`, use `self.connection.add_callback_threadsafe(...)` for cross-thread channel operations

**Concurrency & Threading:**
- Daemon threads for background loops (`thread.daemon = True`)
- Clean shutdown via `threading.Event()` (`self._stop_event.set()` and `thread.join(timeout=2)`)
- Mutex locks (`threading.Lock()`) protecting shared queues/lists across threads (e.g., in `KafkaClient`)

---

*Conventions analysis: 2026-10-05*
