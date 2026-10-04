import time
import logging
from postoffice.router import Router
from postoffice.registry import ClientRegistry

# Import plugins to trigger registration
import postoffice.plugins

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting PostOffice...")

    # Initialize the Router
    router = Router()

    # Initialize Clients via Registry
    mqtt_client = ClientRegistry.create_client("mqtt", "mqtt_1", router, host="localhost")
    nanomq_client = ClientRegistry.create_client("nanomq", "nano_1", router, host="localhost")
    amqp_client = ClientRegistry.create_client("amqp", "amqp_1", router, host="localhost")
    kafka_client = ClientRegistry.create_client("kafka", "kafka_1", router, bootstrap_servers="localhost:9092")

    # Register clients with router
    router.register_client(mqtt_client)
    router.register_client(nanomq_client)
    router.register_client(amqp_client)
    router.register_client(kafka_client)

    # Connect clients
    mqtt_client.connect()
    nanomq_client.connect()
    amqp_client.connect()
    kafka_client.connect()

    # Setup subscriptions
    try:
        # Subscribe to MQTT with QoS 1
        mqtt_client.subscribe("sensor/data", qos=1)
    except Exception as e:
        logger.error(f"MQTT subscribe error: {e}")

    try:
        # Subscribe to NanoMQ local edge
        nanomq_client.subscribe("edge/device/+", qos=0)
    except Exception as e:
        logger.error(f"NanoMQ subscribe error: {e}")

    try:
        # Subscribe to AMQP using a fanout exchange
        amqp_client.subscribe("broadcast.events", exchange="events_fanout", exchange_type="fanout")
    except Exception as e:
        logger.error(f"AMQP subscribe error: {e}")

    try:
        # Subscribe to a generic Kafka topic
        kafka_client.subscribe("kafka_topic")
    except Exception as e:
        logger.error(f"Kafka subscribe error: {e}")

    # Setup routing rules
    # Route NanoMQ edge devices to central MQTT
    router.add_route(
        source_client="nano_1",
        source_topic="edge/device/+",
        target_client="mqtt_1",
        target_topic="central/telemetry",
        qos=1
    )

    # Route MQTT to a specific AMQP topic exchange with persistent delivery
    router.add_route(
        source_client="mqtt_1",
        source_topic="sensor/data",
        target_client="amqp_1",
        target_topic="sensor.telemetry",
        exchange="iot_topic_exchange",
        delivery_mode=2
    )

    # Route Kafka to AMQP fanout
    router.add_route(
        source_client="kafka_1",
        source_topic="kafka_topic",
        target_client="amqp_1",
        target_topic="",
        exchange="events_fanout"
    )

    # Route AMQP back to Kafka on a specific partition with a specific key
    router.add_route(
        source_client="amqp_1",
        source_topic="broadcast.events",
        target_client="kafka_1",
        target_topic="processed_events",
        key=b"amqp_source",
        partition=0
    )

    logger.info("PostOffice running. Press Ctrl+C to stop.")

    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down PostOffice...")
    finally:
        mqtt_client.disconnect()
        nanomq_client.disconnect()
        amqp_client.disconnect()
        kafka_client.disconnect()
        logger.info("PostOffice stopped.")

if __name__ == "__main__":
    main()
