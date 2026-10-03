# PostOffice User Guide

PostOffice is a multi-protocol messaging router designed to act as a bridge between diverse message brokers, such as MQTT, Kafka, and RabbitMQ (AMQP).

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

## Adding New Routes

The `Router` class (in `src/postoffice/router.py`) handles the forwarding of messages from one client/topic pair to another. It supports specific broker kwargs during publishing by passing them directly in `add_route`.

To configure a new route, edit the routing rules in `main.py`:

```python
# Route messages from the 'mqtt_1' client on 'home/temperature'
# to the 'kafka_1' client on the 'telemetry' topic on partition 1.
router.add_route(
    source_client="mqtt_1",
    source_topic="home/temperature",
    target_client="kafka_1",
    target_topic="telemetry",
    key=b"iot_sensor",
    partition=1
)

# Route Kafka telemetry to AMQP using a fanout exchange with persistent delivery
router.add_route(
    source_client="kafka_1",
    source_topic="telemetry",
    target_client="amqp_1",
    target_topic="",
    exchange="events_fanout",
    delivery_mode=2
)
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
