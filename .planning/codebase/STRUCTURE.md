# Codebase Structure

**Analysis Date:** 2026-10-05

## Directory Layout

```
postoffice/
├── configs/                  # External service configuration files
│   └── mosquitto.conf        # Mosquitto MQTT broker configuration
├── docs/                     # Documentation files
│   ├── ARCHITECTURE.md       # High-level architecture and semantic mapping documentation
│   └── USER_GUIDE.md         # Setup, run, and usage instructions
├── scripts/                  # Operational and administrative utilities
│   └── admin.py              # Example admin script injecting routes via ControlPlane
├── src/                      # Application source code
│   └── postoffice/           # Main Python package
│       ├── __init__.py       # Package init
│       ├── app.py            # PostOffice facade class
│       ├── base_client.py    # Abstract base client interface
│       ├── control_plane.py  # Redis-backed admin control plane
│       ├── data_plane.py     # Worker synchronization engine
│       ├── registry.py       # Plugin registration and factory registry
│       ├── router.py         # Topic matching and route dispatcher
│       ├── worker.py         # Data plane worker CLI executable
│       └── plugins/          # Protocol adapter plugins
│           ├── __init__.py   # Plugin exports
│           ├── amqp_client.py    # RabbitMQ / AMQP adapter
│           ├── kafka_client.py   # Apache Kafka adapter
│           ├── mqtt_client.py    # Eclipse Mosquitto / generic MQTT adapter
│           └── nanomq_client.py  # NanoMQ edge MQTT adapter
├── tests/                    # Automated test suite
│   ├── test_postoffice.py    # Unit tests for facade, registry, and routing dispatch
│   └── test_wildcards.py     # Unit tests for MQTT wildcard topic matching
├── docker-compose.yml        # Multi-broker local development environment
├── LICENSE                   # Apache 2.0 license
├── README.md                 # Project landing page & quick links
└── requirements.txt          # Python package dependencies
```

## Directory Purposes

**`src/postoffice/`:**
- Purpose: Core application package containing the router, registry, facade, and distributed components.
- Key files: `app.py` (facade), `router.py` (routing engine), `base_client.py` (client interface), `control_plane.py` (Redis admin), `data_plane.py` (worker engine), `worker.py` (worker CLI).

**`src/postoffice/plugins/`:**
- Purpose: Protocol-specific adapters implementing `BaseClient`.
- Key files: `mqtt_client.py` (MQTT), `nanomq_client.py` (NanoMQ), `amqp_client.py` (RabbitMQ), `kafka_client.py` (Kafka).

**`tests/`:**
- Purpose: Test suite using Python `unittest` and `unittest.mock`.
- Key files: `test_postoffice.py`, `test_wildcards.py`.

**`scripts/`:**
- Purpose: Operational scripts for administering and exercising the cluster.
- Key files: `admin.py`.

**`configs/`:**
- Purpose: Broker and infrastructure configuration templates.
- Key files: `mosquitto.conf`.

**`docs/`:**
- Purpose: Architecture decisions, semantic mapping specs, and user setup guides.
- Key files: `ARCHITECTURE.md`, `USER_GUIDE.md`.

## Key File Locations

**Entry Points:**
- `src/postoffice/app.py`: Facade for programmatic embedding.
- `src/postoffice/worker.py`: Standalone CLI executable for distributed data plane workers.
- `scripts/admin.py`: Standalone administrative CLI for configuring Redis routing rules.

**Configuration:**
- `docker-compose.yml`: Multi-broker container stack definition.
- `configs/mosquitto.conf`: Mosquitto daemon configuration.
- `requirements.txt`: Python package requirements.

**Tests:**
- `tests/test_postoffice.py`: Mocked broker routing tests.
- `tests/test_wildcards.py`: Topic wildcard pattern validation.

## Naming Conventions

**Files:**
- Snake_case (`snake_case.py`) for all Python source and test files.
- Uppercase (`README.md`, `LICENSE`, `ARCHITECTURE.md`) for core documentation.

**Classes & Functions:**
- `PascalCase` for classes (`PostOffice`, `Router`, `BaseClient`, `MqttClient`, `AmqpClient`, `KafkaClient`, `NanoMqClient`, `ControlPlane`, `DataPlane`).
- `snake_case()` for methods, functions, and variables.
- `_leading_underscore()` for private or internal methods (`_topic_match`, `_consume_loop`, `_publish_update`).

---

*Structure analysis: 2026-10-05*
