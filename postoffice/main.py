import time
import logging
from postoffice.router import Router
from postoffice.mqtt_client import MqttClient
from postoffice.amqp_client import AmqpClient
from postoffice.kafka_client import KafkaClient

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting PostOffice...")

    # Initialize the Router
    router = Router()

    # Initialize Clients
    mqtt_client = MqttClient("mqtt_1", router, host="localhost")
    amqp_client = AmqpClient("amqp_1", router, host="localhost")
    kafka_client = KafkaClient("kafka_1", router, bootstrap_servers="localhost:9092")

    # Register clients with router
    router.register_client(mqtt_client)
    router.register_client(amqp_client)
    router.register_client(kafka_client)

    # Connect clients
    mqtt_client.connect()
    amqp_client.connect()
    kafka_client.connect()

    # Setup subscriptions (only if connected successfully, we mock for robustness during error)
    try:
        mqtt_client.subscribe("sensor/data")
    except Exception as e:
        logger.error(f"MQTT subscribe error: {e}")

    try:
        amqp_client.subscribe("amqp_queue")
    except Exception as e:
        logger.error(f"AMQP subscribe error: {e}")

    try:
        kafka_client.subscribe("kafka_topic")
    except Exception as e:
        logger.error(f"Kafka subscribe error: {e}")

    # Setup routing rules
    # Route MQTT to Kafka
    router.add_route("mqtt_1", "sensor/data", "kafka_1", "sensor_events")
    # Route Kafka to AMQP
    router.add_route("kafka_1", "kafka_topic", "amqp_1", "amqp_queue")

    logger.info("PostOffice running. Press Ctrl+C to stop.")

    try:
        # Keep the main thread alive
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down PostOffice...")
    finally:
        mqtt_client.disconnect()
        amqp_client.disconnect()
        kafka_client.disconnect()
        logger.info("PostOffice stopped.")

if __name__ == "__main__":
    main()
