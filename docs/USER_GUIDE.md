# PostOffice User Guide

PostOffice is a multi-protocol messaging router designed to act as a bridge between diverse message brokers, such as MQTT, NanoMQ, Kafka, and RabbitMQ (AMQP).

## Prerequisites

- **Python 3.12+**
- **Docker & Docker Compose** (for running the local broker stack)

## Installation & Setup

1. **Start the Message Brokers & Redis:**
   We provide a standard `docker-compose.yml` that provisions Zookeeper, Kafka, RabbitMQ, Mosquitto, NanoMQ, and Redis.
   ```bash
   docker compose up -d
   ```

2. **Install Python Dependencies:**
   Install the required libraries to interface with the respective brokers and Redis.
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure PYTHONPATH:**
   Ensure the `src` directory is on your Python path to execute scripts properly.
   ```bash
   export PYTHONPATH=$PYTHONPATH:$(pwd)/src
   ```

## Distributed Execution (Scalable Setup)

PostOffice scales horizontally via a Data Plane / Control Plane split.

### 1. Run Data Plane Workers
You can run any number of worker processes. They will connect to Redis and wait for configuration payloads.
```bash
# Start a worker instance
python src/postoffice/worker.py
```

### 2. Configure via the Control Plane
Instead of hardcoding routes, administrators use the Control Plane API to inject routing rules into the Redis cluster. The workers receive these changes in real-time.

```bash
# Run the example admin script to populate Redis with rules
python scripts/admin.py
```

## Working with the Native Interface

If you wish to embed the single-node PostOffice application directly into a script (bypassing Redis), it exposes a uniform interface (in `src/postoffice/app.py`):

```python
from postoffice.app import PostOffice

po = PostOffice()

# Register any number of supported brokers
po.add_broker("mqtt_local", "mqtt", host="localhost", port=1883)
po.add_broker("kafka_cluster", "kafka", bootstrap_servers="localhost:9092")

# Subscribe to topics
po.subscribe("mqtt_local", "sensor/data", qos=1)

# Route messages from 'mqtt_local' to the 'kafka_cluster'
po.add_route(
    source_broker="mqtt_local",
    source_topic="sensor/data",
    target_broker="kafka_cluster",
    target_topic="telemetry",
    key=b"iot_sensor",
    partition=1
)

# Start connections
po.start()
```

### Supported Publish Route Parameters
- **MQTT**: `qos` (int), `retain` (bool)
- **AMQP**: `exchange` (str), `delivery_mode` (int: 1=Transient, 2=Persistent)
- **Kafka**: `key` (bytes), `partition` (int)

## Running Tests

Tests are located in the `tests/` directory and mock the connections to external brokers to ensure the internal routing logic executes safely.

```bash
python -m unittest discover tests/
```
