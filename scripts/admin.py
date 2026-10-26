import sys
import os
import logging

# Ensure we can import postoffice
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'src'))
from postoffice.control_plane import ControlPlane

logging.basicConfig(level=logging.INFO, format='%(message)s')

def inject_test_config():
    cp = ControlPlane(
        redis_host=os.environ.get("REDIS_HOST", "localhost"),
        redis_port=int(os.environ.get("REDIS_PORT", "6379")),
        redis_password=os.environ.get("REDIS_PASSWORD") or None,
        redis_tls=os.environ.get("REDIS_TLS") == "1",
    )

    print("Clearing old state (running workers ignore this unless started with ALLOW_CLEAR_STATE=1)...")
    cp.clear_state()

    print("\n--- Injecting Brokers ---")
    cp.register_broker("mqtt_1", "mqtt", host="localhost", port=1883)
    cp.register_broker("nano_1", "nanomq", host="localhost", port=1884)
    cp.register_broker("amqp_1", "amqp", host="localhost")
    cp.register_broker("kafka_1", "kafka", bootstrap_servers="localhost:9092")

    print("\n--- Injecting Subscriptions ---")
    cp.add_subscription("mqtt_1", "sensor/data", qos=1)
    cp.add_subscription("nano_1", "$share/workers/edge/device/+", qos=1) # Scalable Shared Subscription Example
    cp.add_subscription("amqp_1", "broadcast.events", exchange="events_fanout", exchange_type="fanout")
    cp.add_subscription("kafka_1", "kafka_topic")

    print("\n--- Injecting Routes ---")
    # Route NanoMQ edge devices to central MQTT
    cp.add_route(
        source_broker="nano_1",
        source_topic="edge/device/+",
        target_broker="mqtt_1",
        target_topic="central/telemetry",
        qos=1
    )

    # Route MQTT to a specific AMQP topic exchange with persistent delivery
    cp.add_route(
        source_broker="mqtt_1",
        source_topic="sensor/data",
        target_broker="amqp_1",
        target_topic="sensor.telemetry",
        exchange="iot_topic_exchange",
        delivery_mode=2
    )

if __name__ == "__main__":
    inject_test_config()
