import unittest
import json
from postoffice.schema.registry import InMemorySchemaRegistry
from postoffice.schema.converters import (
    SchemaConverter,
    SchemaValidationError,
    encode_wire_format,
    decode_wire_format,
)

class TestSchemaConverters(unittest.TestCase):
    def setUp(self):
        self.registry = InMemorySchemaRegistry()
        self.converter = SchemaConverter(self.registry)

        self.avro_schema = {
            "type": "record",
            "name": "Telemetry",
            "fields": [
                {"name": "device_id", "type": "string"},
                {"name": "voltage", "type": "double"},
                {"name": "active", "type": "boolean"}
            ]
        }
        self.registry.register_schema("TelemetryAvro", self.avro_schema, "AVRO")

        self.proto_schema = {
            "package": "telemetry",
            "message_name": "TelemetryProto",
            "fields": [
                {"name": "device_id", "type": "string", "number": 1},
                {"name": "voltage", "type": "double", "number": 2},
                {"name": "active", "type": "bool", "number": 3}
            ]
        }
        self.registry.register_schema("TelemetryProto", self.proto_schema, "PROTOBUF")

    def test_wire_format_framing(self):
        payload = b"sensor_binary_payload"
        framed = encode_wire_format(payload, schema_id=42)

        self.assertEqual(framed[0], 0)
        schema_id, extracted_payload = decode_wire_format(framed)
        self.assertEqual(schema_id, 42)
        self.assertEqual(extracted_payload, payload)

    def test_wire_format_corrupted(self):
        # Too short (< 5 bytes)
        with self.assertRaises(SchemaValidationError):
            decode_wire_format(b"\x00\x01")

        # Wrong magic byte
        with self.assertRaises(SchemaValidationError):
            decode_wire_format(b"\x01\x00\x00\x00\x05data")

    def test_json_to_avro_and_back(self):
        orig_record = {"device_id": "sensor-99", "voltage": 12.4, "active": True}
        json_bytes = json.dumps(orig_record).encode('utf-8')

        # JSON -> Avro
        avro_bytes = self.converter.convert(
            json_bytes,
            input_format="json",
            output_format="avro",
            output_schema="TelemetryAvro"
        )
        self.assertIsInstance(avro_bytes, bytes)

        # Avro -> JSON
        restored_json_bytes = self.converter.convert(
            avro_bytes,
            input_format="avro",
            output_format="json",
            input_schema="TelemetryAvro"
        )
        restored = json.loads(restored_json_bytes.decode('utf-8'))
        self.assertEqual(restored["device_id"], "sensor-99")
        self.assertAlmostEqual(restored["voltage"], 12.4)
        self.assertTrue(restored["active"])

    def test_avro_with_confluent_wire_format(self):
        orig_record = {"device_id": "sensor-100", "voltage": 3.3, "active": False}
        json_bytes = json.dumps(orig_record).encode('utf-8')

        # JSON -> Avro with wire format
        framed_avro = self.converter.convert(
            json_bytes,
            input_format="json",
            output_format="avro",
            output_schema="TelemetryAvro",
            wire_format_out=True
        )
        self.assertEqual(framed_avro[0], 0)

        # Avro with wire format -> JSON (automatically resolving schema ID from header)
        restored_json = self.converter.convert(
            framed_avro,
            input_format="avro",
            output_format="json",
            wire_format_in=True
        )
        restored = json.loads(restored_json.decode('utf-8'))
        self.assertEqual(restored["device_id"], "sensor-100")
        self.assertAlmostEqual(restored["voltage"], 3.3)
        self.assertFalse(restored["active"])

    def test_json_to_protobuf_and_back(self):
        orig_record = {"device_id": "node-42", "voltage": 5.0, "active": True}
        json_bytes = json.dumps(orig_record).encode('utf-8')

        # JSON -> Protobuf
        proto_bytes = self.converter.convert(
            json_bytes,
            input_format="json",
            output_format="protobuf",
            output_schema="TelemetryProto"
        )
        self.assertIsInstance(proto_bytes, bytes)

        # Protobuf -> JSON
        restored_json = self.converter.convert(
            proto_bytes,
            input_format="protobuf",
            output_format="json",
            input_schema="TelemetryProto"
        )
        restored = json.loads(restored_json.decode('utf-8'))
        self.assertEqual(restored["device_id"], "node-42")
        self.assertAlmostEqual(restored["voltage"], 5.0)
        self.assertTrue(restored["active"])

    def test_end_to_end_cross_protocol_roundtrip(self):
        # JSON -> Avro (Wire) -> Protobuf (Wire) -> JSON
        raw_json = b'{"device_id": "edge-gate", "voltage": 220.0, "active": true}'

        # Step 1: JSON -> Avro wire format
        avro_wire = self.converter.convert(
            raw_json, "json", "avro",
            output_schema="TelemetryAvro", wire_format_out=True
        )

        # Step 2: Avro wire -> Protobuf wire format
        proto_wire = self.converter.convert(
            avro_wire, "avro", "protobuf",
            output_schema="TelemetryProto",
            wire_format_in=True, wire_format_out=True
        )

        # Step 3: Protobuf wire -> JSON
        final_json = self.converter.convert(
            proto_wire, "protobuf", "json",
            wire_format_in=True
        )

        result = json.loads(final_json.decode('utf-8'))
        self.assertEqual(result["device_id"], "edge-gate")
        self.assertAlmostEqual(result["voltage"], 220.0)
        self.assertTrue(result["active"])

    def test_validation_error_missing_schema(self):
        with self.assertRaises(SchemaValidationError):
            self.converter.convert(b"{}", "json", "avro")  # output_schema missing

    def test_validation_error_malformed_payload(self):
        with self.assertRaises(SchemaValidationError):
            self.converter.convert(
                b"{not valid json}",
                input_format="json",
                output_format="avro",
                output_schema="TelemetryAvro"
            )


if __name__ == "__main__":
    unittest.main()
