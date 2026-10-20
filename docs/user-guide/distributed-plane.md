# Distributed Control Plane & Data Plane

[← Back to User Guide](README.md) | [← Back to Project README](../../README.md)

PostOffice scales horizontally by separating administrative configuration management (**Control Plane**) from real-time message routing workers (**Data Plane**), synchronized via Redis.

---

## Table of Contents
- [Architecture Overview](#architecture-overview)
- [Running Data Plane Workers](#running-data-plane-workers)
- [Administering the Control Plane](#administering-the-control-plane)
  - [Registering Brokers & Routes](#registering-brokers--routes)
  - [Broadcasting Dynamic Updates](#broadcasting-dynamic-updates)
  - [Clean State Resets](#clean-state-resets)
- [Resiliency & Connection Loss Recovery](#resiliency--connection-loss-recovery)

---

## Architecture Overview

```
                      +-----------------------------+
                      |   Control Plane (Admin)     |
                      |   scripts/admin.py or API   |
                      +--------------+--------------+
                                     |
                           (Writes state & PubSub)
                                     v
                      +-----------------------------+
                      |         Redis Hash          |
                      |  postoffice:brokers         |
                      |  postoffice:routes          |
                      |  postoffice:subscriptions   |
                      +--------------+--------------+
                                     |
                           (PubSub config_updates)
             +-----------------------+-----------------------+
             |                                               |
             v                                               v
+-------------------------+                     +-------------------------+
|   Data Plane Worker 1   |                     |   Data Plane Worker 2   |
|   src/postoffice/       |                     |   src/postoffice/       |
|   worker.py             |                     |   worker.py             |
+-------------------------+                     +-------------------------+
```

1. **State Persistence:** Brokers, routes, and subscriptions are persisted in Redis hashes.
2. **Real-Time Notification:** When configuration changes occur, the Control Plane publishes an update event on the Redis PubSub channel `postoffice:config_updates`.
3. **Stateless Workers:** Data Plane workers subscribe to `postoffice:config_updates`, dynamically updating their local in-memory `PostOffice` and `Router` instances without restarting.

---

## Running Data Plane Workers

Start any number of worker processes across containers or virtual machines:

```bash
# Set PYTHONPATH and launch worker
PYTHONPATH=src .venv/bin/python src/postoffice/worker.py
```

Workers immediately:
1. Connect to Redis (default: `localhost:6379`).
2. Sync all existing brokers, routes, and subscriptions from Redis hashes into their local `PostOffice` instance.
3. Start all broker network connections.
4. Listen on `postoffice:config_updates` for real-time changes.

---

## Administering the Control Plane

You can inject configuration programmatically using the `ControlPlane` Python API:

```python
from postoffice.control_plane import ControlPlane
from postoffice.security import SecurityConfig

# Connect to Redis control cluster
cp = ControlPlane(redis_host="localhost", redis_port=6379)

# 1. Register Brokers
cp.register_broker("mqtt_edge", "mqtt", host="10.0.0.1", port=1883)
cp.register_broker("kafka_main", "kafka", bootstrap_servers="10.0.0.2:9092")

# 2. Register Routes
cp.register_route(
    source_broker="mqtt_edge",
    source_topic="telemetry/+",
    target_broker="kafka_main",
    target_topic="telemetry.raw",
)

# 3. Register Subscriptions
cp.register_subscription("mqtt_edge", "telemetry/+", qos=1)

# 4. Hot-Reload Broker Security Credentials Live Across Workers
cp.update_broker_security(
    "kafka_main",
    SecurityConfig(
        tls_enabled=True,
        ca_cert="/etc/ssl/certs/ca.pem",
        username="rotated_user",
        password="rotated_password",
        sasl_mechanism="SCRAM-SHA-256",
    ),
)
```

Every method call automatically writes the configuration to Redis and broadcasts an update message, causing all active Data Plane workers to reconfigure dynamically.

### Executing the Bundled Admin Script
```bash
PYTHONPATH=src .venv/bin/python scripts/admin.py
```

### Clean State Resets
To completely wipe cluster state and reconfigure all workers from scratch:

```python
# Clears Redis hashes and broadcasts 'clear_state' signal
cp.clear_state()
```

Workers receiving `clear_state` invoke `PostOffice.reset()`, disconnecting all active brokers and clearing all routing tables cleanly in memory.

---

## Resiliency & Connection Loss Recovery

In production environments, network partitions or Redis restarts can disrupt the worker's connection to the Control Plane.

`DataPlane` incorporates an automated reconnection loop:
- When the Redis connection drops, the worker catches the error, logs a diagnostic warning, and enters a retry loop.
- It attempts to reconnect with backoff delays, preventing thrashing.
- Once Redis becomes reachable again, the worker automatically resynchronizes cluster state and resumes real-time Pub/Sub listening.
