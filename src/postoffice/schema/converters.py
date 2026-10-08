import io
import json
import struct
from typing import Optional, Tuple, Any, Dict
import fastavro
from google.protobuf import json_format
from postoffice.schema.registry import BaseSchemaRegistry, SchemaEntry


class SchemaValidationError(Exception):
    """Raised when payload fails schema validation, serialization, or deserialization."""
    pass


def encode_wire_format(payload: bytes, schema_id: int) -> bytes:
    """
    Encodes payload with the Confluent Schema Registry 5-byte wire format:
    Byte 0: Magic byte 0x00
    Bytes 1-4: 4-byte big-endian unsigned integer representing schema_id
    Bytes 5+: Serialized message payload
    """
    return b'\x00' + struct.pack('>I', schema_id) + payload


def decode_wire_format(data: bytes) -> Tuple[int, bytes]:
    """
    Decodes Confluent Schema Registry 5-byte wire format header.
    Returns (schema_id, payload_bytes).
    """
    if len(data) < 5:
        raise SchemaValidationError(f"Payload too short for wire format framing (expected >= 5 bytes, got {len(data)})")
    if data[0] != 0:
        raise SchemaValidationError(f"Invalid wire format magic byte: expected 0x00, got {hex(data[0])}")

    schema_id = struct.unpack('>I', data[1:5])[0]
    payload = data[5:]
    return schema_id, payload


class SchemaConverter:
    """
    Bidirectional translation engine between JSON, Apache Avro, and Protocol Buffers.
    Supports raw payloads and Confluent 5-byte wire format framing.
    """

    def __init__(self, registry: BaseSchemaRegistry):
        self.registry = registry

    def _resolve_schema(self, schema_name_or_id: Optional[Any], expected_type: Optional[str] = None) -> SchemaEntry:
        if schema_name_or_id is None:
            raise SchemaValidationError("Schema name or ID must be provided for binary serialization formats")

        if isinstance(schema_name_or_id, int):
            entry = self.registry.get_schema_by_id(schema_name_or_id)
        else:
            entry = self.registry.get_schema(str(schema_name_or_id))

        if entry is None:
            raise SchemaValidationError(f"Schema '{schema_name_or_id}' not found in schema registry")

        if expected_type and entry.schema_type != expected_type.upper():
            raise SchemaValidationError(
                f"Schema '{schema_name_or_id}' is of type '{entry.schema_type}', expected '{expected_type.upper()}'"
            )

        return entry

    def deserialize(
        self,
        payload: bytes,
        format_type: str,
        schema_name: Optional[str] = None,
        wire_format: bool = False
    ) -> Dict[str, Any]:
        """
        Deserializes binary payload into a canonical Python dictionary.
        """
        norm_format = format_type.lower()
        extracted_id = None

        if wire_format:
            extracted_id, payload = decode_wire_format(payload)
            if schema_name is None and extracted_id is not None:
                entry = self.registry.get_schema_by_id(extracted_id)
                if not entry:
                    raise SchemaValidationError(f"Schema ID {extracted_id} from wire format header not found in registry")
                schema_name = entry.name

        try:
            if norm_format == "json":
                return json.loads(payload.decode('utf-8'))

            elif norm_format == "avro":
                entry = self._resolve_schema(schema_name or extracted_id, "AVRO")
                stream = io.BytesIO(payload)
                return fastavro.schemaless_reader(stream, entry.parsed_schema)

            elif norm_format == "protobuf":
                entry = self._resolve_schema(schema_name or extracted_id, "PROTOBUF")
                msg = entry.parsed_schema()
                msg.ParseFromString(payload)
                return json_format.MessageToDict(msg, preserving_proto_field_name=True)

            else:
                raise SchemaValidationError(f"Unsupported input format: '{format_type}'. Supported: 'json', 'avro', 'protobuf'")

        except Exception as e:
            if isinstance(e, SchemaValidationError):
                raise
            raise SchemaValidationError(f"Failed to deserialize {format_type} payload: {e}") from e

    def serialize(
        self,
        record: Dict[str, Any],
        format_type: str,
        schema_name: Optional[str] = None,
        wire_format: bool = False
    ) -> bytes:
        """
        Serializes a Python dictionary into binary payload of the requested format.
        """
        norm_format = format_type.lower()
        schema_id = 0

        try:
            if norm_format == "json":
                body = json.dumps(record).encode('utf-8')
                if schema_name:
                    entry = self.registry.get_schema(schema_name)
                    if entry:
                        schema_id = entry.schema_id

            elif norm_format == "avro":
                entry = self._resolve_schema(schema_name, "AVRO")
                schema_id = entry.schema_id
                stream = io.BytesIO()
                fastavro.schemaless_writer(stream, entry.parsed_schema, record)
                body = stream.getvalue()

            elif norm_format == "protobuf":
                entry = self._resolve_schema(schema_name, "PROTOBUF")
                schema_id = entry.schema_id
                msg = entry.parsed_schema()
                json_format.ParseDict(record, msg)
                body = msg.SerializeToString()

            else:
                raise SchemaValidationError(f"Unsupported output format: '{format_type}'. Supported: 'json', 'avro', 'protobuf'")

            if wire_format:
                return encode_wire_format(body, schema_id)
            return body

        except Exception as e:
            if isinstance(e, SchemaValidationError):
                raise
            raise SchemaValidationError(f"Failed to serialize payload to {format_type}: {e}") from e

    def convert(
        self,
        payload: bytes,
        input_format: str,
        output_format: str,
        input_schema: Optional[str] = None,
        output_schema: Optional[str] = None,
        wire_format_in: bool = False,
        wire_format_out: bool = False
    ) -> bytes:
        """
        Converts payload directly from input_format to output_format.
        """
        record = self.deserialize(payload, input_format, input_schema, wire_format=wire_format_in)
        return self.serialize(record, output_format, output_schema, wire_format=wire_format_out)
