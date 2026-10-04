# PostOffice User Guide

PostOffice is a multi-protocol messaging router designed to act as a bridge between diverse message brokers, such as MQTT, NanoMQ, Kafka, and RabbitMQ (AMQP).

## Prerequisites

- **Python 3.12+**
- **Docker & Docker Compose** (for running the local broker stack)

## Installation & Setup

1. **Start the Message Brokers:**
   We provide a standard `docker-compose.yml` that provisions Zookeeper, Kafka, RabbitMQ, and Mosquitto.
   ```bash
   docker compose up -d
   ```

2. **Install Python Dependencies:**
   Install the required libraries to interface with the respective brokers.
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure PYTHONPATH:**
   Ensure the `src` directory is on your Python path to execute scripts properly.
   ```bash
   export PYTHONPATH=$PYTHONPATH:$(pwd)/src
   ```

## Running the Router

You can run the demonstration script that establishes connections to all local brokers and sets up mock subscriptions:

```bash
python src/postoffice/main.py
```

## Working with the Interface

The PostOffice application exposes a single uniform interface (in `src/postoffice/app.py`). You do not need to instantiate specific broker clients manually.

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
