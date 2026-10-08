import abc
import json
import logging
import threading
from typing import Dict, Any, Optional, Union
import fastavro
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.message import Message

logger = logging.getLogger(__name__)

PROTO_FIELD_TYPES = {
    "double": descriptor_pb2.FieldDescriptorProto.TYPE_DOUBLE,
    "float": descriptor_pb2.FieldDescriptorProto.TYPE_FLOAT,
    "int64": descriptor_pb2.FieldDescriptorProto.TYPE_INT64,
    "uint64": descriptor_pb2.FieldDescriptorProto.TYPE_UINT64,
    "int32": descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
    "fixed64": descriptor_pb2.FieldDescriptorProto.TYPE_FIXED64,
    "fixed32": descriptor_pb2.FieldDescriptorProto.TYPE_FIXED32,
    "bool": descriptor_pb2.FieldDescriptorProto.TYPE_BOOL,
    "string": descriptor_pb2.FieldDescriptorProto.TYPE_STRING,
    "bytes": descriptor_pb2.FieldDescriptorProto.TYPE_BYTES,
    "uint32": descriptor_pb2.FieldDescriptorProto.TYPE_UINT32,
    "sfixed32": descriptor_pb2.FieldDescriptorProto.TYPE_SFIXED32,
    "sfixed64": descriptor_pb2.FieldDescriptorProto.TYPE_SFIXED64,
    "sint32": descriptor_pb2.FieldDescriptorProto.TYPE_SINT32,
    "sint64": descriptor_pb2.FieldDescriptorProto.TYPE_SINT64,
}


class SchemaEntry:
    """Holds metadata and compiled runtime descriptors for a registered schema."""
    def __init__(
        self,
        schema_id: int,
        name: str,
        schema_type: str,
        definition: Any,
        parsed_schema: Any,
        version: int = 1
    ):
        self.schema_id = schema_id
        self.name = name
        self.schema_type = schema_type.upper()  # 'AVRO' or 'PROTOBUF'
        self.definition = definition            # Original definition (dict, str, or FileDescriptorProto)
        self.parsed_schema = parsed_schema      # fastavro parsed schema or protobuf Message class
        self.version = version

    def __repr__(self) -> str:
        return f"<SchemaEntry id={self.schema_id} name='{self.name}' type='{self.schema_type}' version={self.version}>"


def _build_dynamic_proto_message_class(name: str, definition: Union[dict, descriptor_pb2.FileDescriptorProto]):
    """Dynamically creates a Protobuf Message class from a dict spec or FileDescriptorProto."""
    if isinstance(definition, descriptor_pb2.FileDescriptorProto):
        file_proto = definition
        pkg = file_proto.package
        msg_name = file_proto.message_type[0].name
        full_name = f"{pkg}.{msg_name}" if pkg else msg_name
    elif isinstance(definition, dict):
        file_proto = descriptor_pb2.FileDescriptorProto()
        pkg = definition.get("package", "postoffice.dynamic")
        file_proto.name = f"{name}.proto"
        file_proto.package = pkg

        msg_type = file_proto.message_type.add()
        msg_name = definition.get("message_name", name)
        msg_type.name = msg_name

        fields = definition.get("fields", [])
        for idx, field in enumerate(fields, start=1):
            f_name = field["name"]
            f_type_str = field.get("type", "string").lower()
            f_number = field.get("number", idx)

            f = msg_type.field.add()
            f.name = f_name
            f.number = f_number
            f.type = PROTO_FIELD_TYPES.get(f_type_str, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
            if field.get("label") == "repeated":
                f.label = descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED
            else:
                f.label = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL

        full_name = f"{pkg}.{msg_name}" if pkg else msg_name
    else:
        raise ValueError(f"Unsupported Protobuf schema definition type: {type(definition)}")

    pool = descriptor_pool.DescriptorPool()
    pool.Add(file_proto)
    desc = pool.FindMessageTypeByName(full_name)
    return message_factory.GetMessageClass(desc)


class BaseSchemaRegistry(abc.ABC):
    """Abstract interface for schema registration and lookup."""

    @abc.abstractmethod
    def register_schema(self, name: str, definition: Any, schema_type: str = "AVRO") -> int:
        """Registers a new schema and returns its unique integer Schema ID."""
        pass

    @abc.abstractmethod
    def get_schema(self, name: str) -> Optional[SchemaEntry]:
        """Retrieves a schema entry by its logical name."""
        pass

    @abc.abstractmethod
    def get_schema_by_id(self, schema_id: int) -> Optional[SchemaEntry]:
        """Retrieves a schema entry by its numeric Schema ID."""
        pass


class InMemorySchemaRegistry(BaseSchemaRegistry):
    """Thread-safe, local in-memory schema registry."""

    def __init__(self):
        self._by_name: Dict[str, SchemaEntry] = {}
        self._by_id: Dict[int, SchemaEntry] = {}
        self._next_id: int = 1
        self._lock = threading.Lock()

    def register_schema(self, name: str, definition: Any, schema_type: str = "AVRO") -> int:
        normalized_type = schema_type.upper()
        if normalized_type not in ("AVRO", "PROTOBUF"):
            raise ValueError(f"Unsupported schema type: {schema_type}. Supported: 'AVRO', 'PROTOBUF'")

        # Parse and compile the schema
        if normalized_type == "AVRO":
            if isinstance(definition, str):
                definition = json.loads(definition)
            parsed = fastavro.parse_schema(definition)
        elif normalized_type == "PROTOBUF":
            if isinstance(definition, type) and issubclass(definition, Message):
                parsed = definition
            else:
                parsed = _build_dynamic_proto_message_class(name, definition)

        with self._lock:
            if name in self._by_name:
                existing = self._by_name[name]
                schema_id = existing.schema_id
                version = existing.version + 1
            else:
                schema_id = self._next_id
                self._next_id += 1
                version = 1

            entry = SchemaEntry(
                schema_id=schema_id,
                name=name,
                schema_type=normalized_type,
                definition=definition,
                parsed_schema=parsed,
                version=version
            )
            self._by_name[name] = entry
            self._by_id[schema_id] = entry
            logger.info(f"Registered schema '{name}' as ID {schema_id} (type={normalized_type}, v={version})")
            return schema_id

    def get_schema(self, name: str) -> Optional[SchemaEntry]:
        with self._lock:
            return self._by_name.get(name)

    def get_schema_by_id(self, schema_id: int) -> Optional[SchemaEntry]:
        with self._lock:
            return self._by_id.get(schema_id)


class RedisSchemaRegistry(BaseSchemaRegistry):
    """
    Distributed Redis-backed schema registry synchronized across workers.
    Caches parsed schema representations locally in memory to optimize routing throughput.
    """

    def __init__(self, redis_client: Any, prefix: str = "postoffice:schemas"):
        self.redis = redis_client
        self.prefix = prefix
        self.local_cache = InMemorySchemaRegistry()
        self._lock = threading.Lock()

    def register_schema(self, name: str, definition: Any, schema_type: str = "AVRO") -> int:
        normalized_type = schema_type.upper()
        meta_key = f"{self.prefix}:meta:{name}"
        id_map_key = f"{self.prefix}:id_to_name"
        next_id_key = f"{self.prefix}:next_id"

        # Check existing
        existing_meta = self.redis.hgetall(meta_key)
        if existing_meta:
            schema_id = int(existing_meta.get(b"schema_id", existing_meta.get("schema_id")))
            version = int(existing_meta.get(b"version", existing_meta.get("version", 1))) + 1
        else:
            schema_id = self.redis.incr(next_id_key)
            version = 1

        def_str = json.dumps(definition) if isinstance(definition, (dict, list)) else str(definition)

        self.redis.hset(meta_key, mapping={
            "schema_id": str(schema_id),
            "name": name,
            "schema_type": normalized_type,
            "definition": def_str,
            "version": str(version)
        })
        self.redis.hset(id_map_key, mapping={str(schema_id): name})

        # Update local cache
        self.local_cache.register_schema(name, definition, schema_type=normalized_type)
        return schema_id

    def get_schema(self, name: str) -> Optional[SchemaEntry]:
        cached = self.local_cache.get_schema(name)
        if cached:
            return cached

        meta_key = f"{self.prefix}:meta:{name}"
        data = self.redis.hgetall(meta_key)
        if not data:
            return None

        # Normalize bytes vs str from redis
        schema_type = data.get("schema_type") or data.get(b"schema_type")
        if isinstance(schema_type, bytes):
            schema_type = schema_type.decode("utf-8")

        def_raw = data.get("definition") or data.get(b"definition")
        if isinstance(def_raw, bytes):
            def_raw = def_raw.decode("utf-8")

        try:
            definition = json.loads(def_raw)
        except Exception:
            definition = def_raw

        self.local_cache.register_schema(name, definition, schema_type=schema_type)
        return self.local_cache.get_schema(name)

    def get_schema_by_id(self, schema_id: int) -> Optional[SchemaEntry]:
        cached = self.local_cache.get_schema_by_id(schema_id)
        if cached:
            return cached

        id_map_key = f"{self.prefix}:id_to_name"
        name = self.redis.hget(id_map_key, str(schema_id))
        if not name:
            return None
        if isinstance(name, bytes):
            name = name.decode("utf-8")

        return self.get_schema(name)
