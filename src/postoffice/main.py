import time
import logging
from postoffice.app import PostOffice

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    logger.info("Starting PostOffice...")

    # Initialize the Uniform Interface Layer (Facade)
    po = PostOffice()

    # 1. Register Brokers (Abstracted)
    po.add_broker("mqtt_1", "mqtt", host="localhost", port=1883)
    po.add_broker("nano_1", "nanomq", host="localhost", port=1884)
    po.add_broker("amqp_1", "amqp", host="localhost")
    po.add_broker("kafka_1", "kafka", bootstrap_servers="localhost:9092")

    # 2. Connect to all systems
    po.start()

    # 3. Setup Subscriptions
    po.subscribe("mqtt_1", "sensor/data", qos=1)
    po.subscribe("nano_1", "edge/device/+", qos=0)
    po.subscribe("amqp_1", "broadcast.events", exchange="events_fanout", exchange_type="fanout")
    po.subscribe("kafka_1", "kafka_topic")

    # 4. Setup Routing Rules

    # Route NanoMQ edge devices to central MQTT
    po.add_route(
        source_broker="nano_1",
        source_topic="edge/device/+",
        target_broker="mqtt_1",
        target_topic="central/telemetry",
        qos=1
    )

    # Route MQTT to a specific AMQP topic exchange with persistent delivery
    po.add_route(
        source_broker="mqtt_1",
        source_topic="sensor/data",
        target_broker="amqp_1",
        target_topic="sensor.telemetry",
        exchange="iot_topic_exchange",
        delivery_mode=2
    )

    # Route Kafka to AMQP fanout
    po.add_route(
        source_broker="kafka_1",
        source_topic="kafka_topic",
        target_broker="amqp_1",
        target_topic="",
        exchange="events_fanout"
    )

    # Route AMQP back to Kafka on a specific partition with a specific key
    po.add_route(
        source_broker="amqp_1",
        source_topic="broadcast.events",
        target_broker="kafka_1",
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
        po.stop()
        logger.info("PostOffice stopped.")

if __name__ == "__main__":
    main()
