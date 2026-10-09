import unittest
import json
from unittest.mock import MagicMock
from postoffice.router import Router
from postoffice.app import PostOffice
from postoffice.schema import InMemorySchemaRegistry, decode_wire_format

class TestSchemaRouting(unittest.TestCase):
    def setUp(self):
        self.registry = InMemorySchemaRegistry()
        self.router = Router(schema_registry=self.registry)

        # Register Avro schema
        self.avro_schema = {
            "type": "record",
            "name": "SensorTelemetry",
            "fields": [
                {"name": "sensor_id", "type": "string"},
                {"name": "temperature", "type": "double"},
                {"name": "status", "type": "string"}
            ]
        }
        self.registry.register_schema("SensorTelemetry", self.avro_schema, "AVRO")

        # Register Protobuf schema
        self.proto_schema = {
            "package": "iot",
            "message_name": "DeviceEvent",
            "fields": [
                {"name": "sensor_id", "type": "string", "number": 1},
                {"name": "temperature", "type": "double", "number": 2},
                {"name": "status", "type": "string", "number": 3}
            ]
        }
        self.registry.register_schema("DeviceEvent", self.proto_schema, "PROTOBUF")

    def test_json_to_avro_wire_format_routing(self):
        mqtt_client = MagicMock()
        mqtt_client.name = "mqtt_ingress"
        kafka_client = MagicMock()
        kafka_client.name = "kafka_egress"

        self.router.register_client(mqtt_client)
        self.router.register_client(kafka_client)

        def mock_publish(topic, msg, on_confirm=None, **kwargs):
            if on_confirm:
                on_confirm()

        kafka_client.publish.side_effect = mock_publish

        self.router.add_route(
            source_client="mqtt_ingress",
            source_topic="sensors/temperature",
            target_client="kafka_egress",
            target_topic="telemetry.avro",
            input_format="json",
            output_format="avro",
            output_schema="SensorTelemetry",
            wire_format=True
        )

        ack_fn = MagicMock()
        raw_json = b'{"sensor_id": "sensor-1", "temperature": 26.2, "status": "OK"}'

        self.router.route(
            "mqtt_ingress",
            "sensors/temperature",
            raw_json,
            ack_fn=ack_fn
        )

        kafka_client.publish.assert_called_once()
        topic, published_bytes = kafka_client.publish.call_args[0][:2]
        self.assertEqual(topic, "telemetry.avro")

        # Verify published bytes are Confluent wire format
        schema_id, payload = decode_wire_format(published_bytes)
        self.assertEqual(schema_id, 1)

        # Deserialize to verify content integrity
        restored = self.router.converter.deserialize(published_bytes, "avro", wire_format=True)
        self.assertEqual(restored["sensor_id"], "sensor-1")
        self.assertAlmostEqual(restored["temperature"], 26.2)
        self.assertEqual(restored["status"], "OK")

        ack_fn.assert_called_once()

    def test_json_to_protobuf_routing(self):
        amqp_client = MagicMock()
        amqp_client.name = "amqp_ingress"
        kafka_client = MagicMock()
        kafka_client.name = "kafka_egress"

        self.router.register_client(amqp_client)
        self.router.register_client(kafka_client)

        self.router.add_route(
            source_client="amqp_ingress",
            source_topic="events/device",
            target_client="kafka_egress",
            target_topic="events.proto",
            input_format="json",
            output_format="protobuf",
            output_schema="DeviceEvent"
        )

        raw_json = b'{"sensor_id": "dev-42", "temperature": 18.5, "status": "ACTIVE"}'
        self.router.route("amqp_ingress", "events/device", raw_json)

        kafka_client.publish.assert_called_once()
        topic, published_bytes = kafka_client.publish.call_args[0][:2]
        self.assertEqual(topic, "events.proto")

        # Deserialize Protobuf and verify
        restored = self.router.converter.deserialize(published_bytes, "protobuf", schema_name="DeviceEvent")
        self.assertEqual(restored["sensor_id"], "dev-42")
        self.assertAlmostEqual(restored["temperature"], 18.5)
        self.assertEqual(restored["status"], "ACTIVE")

    def test_avro_to_json_routing(self):
        kafka_client = MagicMock()
        kafka_client.name = "kafka_ingress"
        mqtt_client = MagicMock()
        mqtt_client.name = "mqtt_egress"

        self.router.register_client(kafka_client)
        self.router.register_client(mqtt_client)

        self.router.add_route(
            source_client="kafka_ingress",
            source_topic="raw.avro",
            target_client="mqtt_egress",
            target_topic="notifications/temp",
            input_format="avro",
            input_schema="SensorTelemetry",
            output_format="json"
        )

        # Prepare Avro payload
        avro_bytes = self.router.converter.serialize(
            {"sensor_id": "sensor-5", "temperature": 31.0, "status": "WARN"},
            "avro",
            "SensorTelemetry"
        )

        self.router.route("kafka_ingress", "raw.avro", avro_bytes)

        mqtt_client.publish.assert_called_once()
        topic, published_bytes = mqtt_client.publish.call_args[0][:2]
        self.assertEqual(topic, "notifications/temp")

        # Verify valid JSON
        result = json.loads(published_bytes.decode('utf-8'))
        self.assertEqual(result["sensor_id"], "sensor-5")
        self.assertEqual(result["status"], "WARN")

    def test_schema_validation_error_routes_to_dlq(self):
        mqtt_client = MagicMock()
        mqtt_client.name = "mqtt_ingress"
        kafka_client = MagicMock()
        kafka_client.name = "kafka_egress"
        dlq_client = MagicMock()
        dlq_client.name = "dlq_broker"

        self.router.register_client(mqtt_client)
        self.router.register_client(kafka_client)
        self.router.register_client(dlq_client)

        self.router.set_dlq("dlq_broker", "system/dlq")
        self.router.add_route(
            source_client="mqtt_ingress",
            source_topic="sensors/strict",
            target_client="kafka_egress",
            target_topic="telemetry.avro",
            input_format="json",
            output_format="avro",
            output_schema="SensorTelemetry"
        )

        ack_fn = MagicMock()
        # Invalid payload: temperature is a string rather than double, and status is missing
        malformed_json = b'{"sensor_id": "sensor-1", "temperature": "NOT_A_FLOAT"}'

        self.router.route(
            "mqtt_ingress",
            "sensors/strict",
            malformed_json,
            ack_fn=ack_fn
        )

        # Target publish should not be called
        kafka_client.publish.assert_not_called()

        # DLQ should receive failure envelope
        dlq_client.publish.assert_called_once()
        dlq_topic, dlq_bytes = dlq_client.publish.call_args[0][:2]
        self.assertEqual(dlq_topic, "system/dlq")
        envelope = json.loads(dlq_bytes.decode('utf-8'))
        self.assertIn("Schema transformation error", envelope["error"])

        # Metric should be recorded
        self.assertIn(
            'postoffice_routing_errors_total{error_type="schema_validation_error",source_broker="mqtt_ingress"} 1.0',
            self.router.metrics.get_metrics_text()
        )

        # Upstream message acknowledged (poison pill quarantined)
        ack_fn.assert_called_once()

    def test_fanout_with_heterogeneous_formats(self):
        mqtt_client = MagicMock()
        mqtt_client.name = "mqtt_ingress"
        kafka_client = MagicMock()
        kafka_client.name = "kafka_egress"
        amqp_client = MagicMock()
        amqp_client.name = "amqp_egress"

        self.router.register_client(mqtt_client)
        self.router.register_client(kafka_client)
        self.router.register_client(amqp_client)

        # Route 1: transforms to Avro for Kafka
        self.router.add_route(
            "mqtt_ingress", "data/fanout",
            "kafka_egress", "kafka.avro",
            input_format="json", output_format="avro", output_schema="SensorTelemetry"
        )

        # Route 2: keeps as JSON for AMQP
        self.router.add_route(
            "mqtt_ingress", "data/fanout",
            "amqp_egress", "amqp.json"
        )

        raw_json = b'{"sensor_id": "s1", "temperature": 20.0, "status": "OK"}'
        self.router.route("mqtt_ingress", "data/fanout", raw_json)

        kafka_client.publish.assert_called_once()
        amqp_client.publish.assert_called_once()

        # Kafka received Avro
        kafka_bytes = kafka_client.publish.call_args[0][1]
        deserialized_avro = self.router.converter.deserialize(kafka_bytes, "avro", "SensorTelemetry")
        self.assertEqual(deserialized_avro["sensor_id"], "s1")

        # AMQP received original JSON
        amqp_bytes = amqp_client.publish.call_args[0][1]
        deserialized_json = json.loads(amqp_bytes.decode('utf-8'))
        self.assertEqual(deserialized_json["sensor_id"], "s1")

    def test_postoffice_facade_schema_integration(self):
        po = PostOffice()
        schema_id = po.register_schema("FacadeSchema", self.avro_schema, "AVRO")
        self.assertEqual(schema_id, 1)

        entry = po.get_schema("FacadeSchema")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.schema_id, 1)

        entry_by_id = po.get_schema_by_id(1)
        self.assertEqual(entry_by_id, entry)

        po.reset()
        # Schema registry should be preserved across reset
        self.assertIsNotNone(po.get_schema("FacadeSchema"))


if __name__ == "__main__":
    unittest.main()
