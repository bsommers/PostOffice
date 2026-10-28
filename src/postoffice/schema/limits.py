"""
Caps on dynamically compiled schema definitions.

Definitions arrive from Redis (``RedisSchemaRegistry.get_schema``), so they are untrusted: an
oversized or pathological one would otherwise be compiled by protobuf or fastavro as-is. Errors
name the limit that was hit, never the definition's contents.
"""
import json
import os
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional

from google.protobuf import descriptor_pb2
from google.protobuf.message import Message

from postoffice.jsonscan import scan_json

IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}\Z")
PACKAGE = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,63}(\.[A-Za-z_][A-Za-z0-9_]{0,63}){0,7}\Z")
MAX_SCHEMA_NAME = 128
MAX_FIELD_NUMBER = 2 ** 29 - 1
RESERVED_NUMBERS = range(19000, 20000)  # reserved for the protobuf implementation
LABELS = (None, "optional", "repeated")


class SchemaDefinitionError(ValueError):
    """A schema definition exceeds a configured cap or is structurally unsound."""


@dataclass(frozen=True)
class SchemaLimits:
    max_bytes: int = 64 * 1024   # serialized definition
    max_fields: int = 256        # Protobuf fields in total
    max_depth: int = 32          # Avro JSON nesting (a record costs three levels)

    def __post_init__(self):
        if min(self.max_bytes, self.max_fields, self.max_depth) <= 0:
            raise ValueError("schema limits must be > 0")

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None) -> "SchemaLimits":
        env = os.environ if env is None else env
        values = {}
        for attr, var in (("max_bytes", "POSTOFFICE_SCHEMA_MAX_BYTES"),
                          ("max_fields", "POSTOFFICE_SCHEMA_MAX_FIELDS"),
                          ("max_depth", "POSTOFFICE_SCHEMA_MAX_DEPTH")):
            raw = env.get(var)
            if raw is not None and raw.strip() != "":
                try:
                    values[attr] = int(raw)
                except ValueError:
                    raise ValueError(f"{var} must be an integer") from None
        return cls(**values)


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SchemaDefinitionError(message)


def _check_field_number(number: Any) -> None:
    _require(isinstance(number, int) and not isinstance(number, bool), "field number must be an integer")
    _require(1 <= number <= MAX_FIELD_NUMBER, f"field number outside 1..{MAX_FIELD_NUMBER}")
    _require(number not in RESERVED_NUMBERS, "field number in the reserved range 19000-19999")


def _json_size(definition: Any, limits: SchemaLimits) -> None:
    try:
        size = len(json.dumps(definition))
    except (TypeError, ValueError):
        raise SchemaDefinitionError("definition is not JSON-serializable") from None
    _require(size <= limits.max_bytes, f"definition is larger than {limits.max_bytes} bytes")


def _check_depth(definition: Any, limits: SchemaLimits) -> None:
    """Iterative, and bounded by a node budget so a self-referencing structure terminates."""
    budget = limits.max_bytes
    stack = [(definition, 1)]
    while stack:
        node, depth = stack.pop()
        budget -= 1
        _require(budget > 0, f"definition has too many nodes or is cyclic (limit {limits.max_bytes})")
        if isinstance(node, dict):
            _require(depth <= limits.max_depth, f"definition is nested deeper than {limits.max_depth}")
            stack.extend((child, depth + 1) for child in node.values())
        elif isinstance(node, list):
            _require(depth <= limits.max_depth, f"definition is nested deeper than {limits.max_depth}")
            stack.extend((child, depth + 1) for child in node)


def _validate_avro(definition: Any, limits: SchemaLimits) -> Any:
    """Returns the definition (parsed when it was a JSON string) once it is within the caps."""
    if isinstance(definition, (str, bytes)):
        raw = definition.encode("utf-8") if isinstance(definition, str) else definition
        _require(len(raw) <= limits.max_bytes, f"definition is larger than {limits.max_bytes} bytes")
        problem = scan_json(raw, limits.max_depth, limits.max_bytes, limits.max_bytes, limits.max_bytes)
        _require(problem is None, f"definition is not acceptable ({problem})")
        try:
            definition = json.loads(raw)
        except ValueError:
            raise SchemaDefinitionError("definition is not valid JSON") from None
    _check_depth(definition, limits)
    _json_size(definition, limits)
    return definition


def _validate_proto_dict(definition: Any, limits: SchemaLimits) -> None:
    from postoffice.schema.registry import PROTO_FIELD_TYPES  # registry imports this module
    _check_depth(definition, limits)
    _json_size(definition, limits)
    _require(isinstance(definition, dict), "Protobuf definition must be an object")
    package = definition.get("package", "postoffice.dynamic")
    _require(package == "" or (isinstance(package, str) and PACKAGE.match(package) is not None),
             "package is not a valid dotted identifier")
    message_name = definition.get("message_name")
    _require(message_name is None or (isinstance(message_name, str) and IDENTIFIER.match(message_name)),
             "message_name is not a valid identifier")
    fields = definition.get("fields", [])
    _require(isinstance(fields, list), "fields must be a list")
    _require(len(fields) <= limits.max_fields, f"more than {limits.max_fields} fields")
    names, numbers = set(), set()
    for index, field in enumerate(fields, start=1):
        _require(isinstance(field, dict), "each field must be an object")
        name = field.get("name")
        _require(isinstance(name, str) and IDENTIFIER.match(name) is not None, "field name is not a valid identifier")
        _require(name not in names, "duplicate field name")
        names.add(name)
        number = field.get("number", index)
        _check_field_number(number)
        _require(number not in numbers, "duplicate field number")
        numbers.add(number)
        ftype = field.get("type", "string")
        _require(isinstance(ftype, str) and ftype.lower() in PROTO_FIELD_TYPES, "unknown field type")
        label = field.get("label")
        _require(label in LABELS, "label must be 'optional' or 'repeated'")


def _validate_file_descriptor(fd: descriptor_pb2.FileDescriptorProto, limits: SchemaLimits) -> None:
    _require(fd.ByteSize() <= limits.max_bytes, f"definition is larger than {limits.max_bytes} bytes")
    _require(len(fd.message_type) >= 1, "file descriptor has no message type")
    total, stack = 0, list(fd.message_type)
    while stack:
        message = stack.pop()
        total += len(message.field)
        _require(total <= limits.max_fields, f"more than {limits.max_fields} fields")
        for field in message.field:
            _check_field_number(field.number)
        stack.extend(message.nested_type)


def validate_schema_definition(name: Any, definition: Any, schema_type: str, limits: SchemaLimits) -> Any:
    """
    Raises SchemaDefinitionError unless the definition is within the caps. Returns the definition to
    compile (an Avro JSON string comes back parsed). Compiled ``Message`` classes are code, not data.
    """
    _require(isinstance(name, str) and 0 < len(name) <= MAX_SCHEMA_NAME and "\x00" not in name,
             f"schema name must be 1-{MAX_SCHEMA_NAME} characters without NUL")
    kind = schema_type.upper()
    if kind == "AVRO":
        return _validate_avro(definition, limits)
    if kind == "PROTOBUF":
        if isinstance(definition, type) and issubclass(definition, Message):
            return definition
        if isinstance(definition, descriptor_pb2.FileDescriptorProto):
            _validate_file_descriptor(definition, limits)
        else:
            _validate_proto_dict(definition, limits)
        return definition
    raise SchemaDefinitionError(f"Unsupported schema type: {schema_type}")
