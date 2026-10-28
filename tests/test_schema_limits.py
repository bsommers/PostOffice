"""D9c: caps on dynamically compiled Protobuf / Avro definitions (hermetic)."""
import json
import os
import unittest
from unittest.mock import MagicMock, patch

from google.protobuf import descriptor_pb2

from postoffice.schema.limits import SchemaDefinitionError, SchemaLimits, validate_schema_definition
from postoffice.schema.registry import InMemorySchemaRegistry, RedisSchemaRegistry


def proto(fields=None, **extra):
    return {"package": "t", "message_name": "M",
            "fields": fields if fields is not None else [{"name": "a", "type": "string", "number": 1}], **extra}


def avro_nested(depth):
    schema = "string"
    for i in range(depth):
        schema = {"type": "record", "name": f"R{i}", "fields": [{"name": "f", "type": schema}]}
    return schema


class TestProtobufCaps(unittest.TestCase):
    def setUp(self):
        self.reg = InMemorySchemaRegistry()

    def rejected(self, definition, **kw):
        with self.assertRaises(SchemaDefinitionError):
            self.reg.register_schema("S", definition, "PROTOBUF")
        self.assertIsNone(self.reg.get_schema("S"))

    def test_valid_definition_still_registers(self):
        self.assertEqual(self.reg.register_schema("S", proto(), "PROTOBUF"), 1)

    def test_too_many_fields(self):
        fields = [{"name": f"f{i}", "type": "int32", "number": i + 1} for i in range(257)]
        self.rejected(proto(fields))

    def test_exactly_the_field_limit_is_allowed(self):
        fields = [{"name": f"f{i}", "type": "int32", "number": i + 1} for i in range(256)]
        self.reg.register_schema("S", proto(fields), "PROTOBUF")

    def test_oversize_definition(self):
        self.rejected(proto(package="p" * 70000))

    def test_duplicate_field_numbers(self):
        self.rejected(proto([{"name": "a", "number": 1}, {"name": "b", "number": 1}]))

    def test_duplicate_field_names(self):
        self.rejected(proto([{"name": "a", "number": 1}, {"name": "a", "number": 2}]))

    def test_field_numbers_out_of_range(self):
        for number in (0, -1, 2 ** 29, 2 ** 40, "1", 1.5, True, None):
            with self.subTest(number=number):
                self.rejected(proto([{"name": "a", "number": number}]))

    def test_reserved_number_range(self):
        for number in (19000, 19500, 19999):
            with self.subTest(number=number):
                self.rejected(proto([{"name": "a", "number": number}]))
        for number in (18999, 20000, 2 ** 29 - 1):
            with self.subTest(number=number):
                InMemorySchemaRegistry().register_schema("S", proto([{"name": "a", "number": number}]), "PROTOBUF")

    def test_identifier_patterns(self):
        for bad in ("", "1abc", "a b", "a-b", "a.b", "é", "x" * 65, 5, None):
            with self.subTest(field=bad):
                self.rejected(proto([{"name": bad, "number": 1}]))
        self.rejected(proto(message_name="bad name"))
        self.rejected(proto(package="a..b"))
        self.rejected(proto(package="9a"))

    def test_unknown_type_is_rejected_not_silently_string(self):
        self.rejected(proto([{"name": "a", "type": "strng", "number": 1}]))
        self.rejected(proto([{"name": "a", "type": ["string"], "number": 1}]))

    def test_every_supported_type_is_accepted(self):
        from postoffice.schema.registry import PROTO_FIELD_TYPES
        for i, t in enumerate(PROTO_FIELD_TYPES, start=1):
            with self.subTest(type=t):
                InMemorySchemaRegistry().register_schema("S", proto([{"name": "a", "type": t, "number": i}]),
                                                         "PROTOBUF")

    def test_label_must_be_known(self):
        self.rejected(proto([{"name": "a", "number": 1, "label": "required-ish"}]))
        InMemorySchemaRegistry().register_schema("S", proto([{"name": "a", "number": 1, "label": "repeated"}]),
                                                 "PROTOBUF")

    def test_structure_must_be_well_formed(self):
        self.rejected(proto(fields="not a list"))
        self.rejected(proto(["not a dict"]))
        self.rejected(proto([{"type": "string"}]))
        self.rejected("just a string")

    def test_schema_name_is_bounded(self):
        for bad in ("", "x" * 200, "bad\x00name"):
            with self.subTest(name=bad):
                with self.assertRaises(SchemaDefinitionError):
                    self.reg.register_schema(bad, proto(), "PROTOBUF")

    def test_file_descriptor_is_checked_too(self):
        fd = descriptor_pb2.FileDescriptorProto(name="x.proto", package="t")
        msg = fd.message_type.add(name="M")
        for i in range(300):
            msg.field.add(name=f"f{i}", number=i + 1, type=descriptor_pb2.FieldDescriptorProto.TYPE_INT32,
                          label=descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL)
        self.rejected(fd)

    def test_empty_file_descriptor_is_rejected(self):
        self.rejected(descriptor_pb2.FileDescriptorProto(name="x.proto"))

    def test_limits_are_configurable(self):
        reg = InMemorySchemaRegistry(limits=SchemaLimits(max_fields=2))
        with self.assertRaises(SchemaDefinitionError):
            reg.register_schema("S", proto([{"name": f"f{i}", "number": i + 1} for i in range(3)]), "PROTOBUF")


class TestAvroCaps(unittest.TestCase):
    def setUp(self):
        self.reg = InMemorySchemaRegistry()

    def test_ordinary_schema_registers(self):
        self.reg.register_schema("S", avro_nested(3), "AVRO")

    def test_depth_cap(self):
        with self.assertRaises(SchemaDefinitionError):
            self.reg.register_schema("S", avro_nested(40), "AVRO")
        self.assertIsNone(self.reg.get_schema("S"))

    def test_depth_cap_applies_to_json_strings_before_parsing(self):
        text = json.dumps(avro_nested(40))
        with patch("postoffice.schema.limits.json.loads", side_effect=AssertionError("parsed")):
            with self.assertRaises(SchemaDefinitionError):
                validate_schema_definition("S", text, "AVRO", SchemaLimits())

    def test_pathological_nesting_does_not_recurse(self):
        text = "[" * 100000 + "]" * 100000
        with self.assertRaises(SchemaDefinitionError):
            self.reg.register_schema("S", text, "AVRO")

    def test_size_cap(self):
        schema = {"type": "record", "name": "R", "doc": "d" * 70000, "fields": []}
        with self.assertRaises(SchemaDefinitionError):
            self.reg.register_schema("S", schema, "AVRO")

    def test_oversize_string_is_rejected_before_parsing(self):
        with patch("postoffice.schema.limits.json.loads", side_effect=AssertionError("parsed")):
            with self.assertRaises(SchemaDefinitionError):
                validate_schema_definition("S", "x" * 70000, "AVRO", SchemaLimits())

    def test_self_referencing_python_structure_terminates(self):
        loop = {"type": "record", "name": "R", "fields": []}
        loop["fields"].append(loop)
        with self.assertRaises(SchemaDefinitionError):
            self.reg.register_schema("S", loop, "AVRO")

    def test_invalid_json_string_is_a_definition_error(self):
        with self.assertRaises(SchemaDefinitionError):
            self.reg.register_schema("S", "{not json", "AVRO")


class FakeRedis:
    def __init__(self):
        self.store = {}

    def hgetall(self, key):
        return dict(self.store.get(key, {}))

    def hset(self, key, field=None, value=None, mapping=None):
        self.store.setdefault(key, {}).update(mapping or {field: value})

    def hget(self, key, field):
        return self.store.get(key, {}).get(field)

    def incr(self, key):
        self.store[key] = self.store.get(key, 0) + 1
        return self.store[key]


class TestRedisPaths(unittest.TestCase):
    def test_register_rejects_before_writing_anything(self):
        redis_client = FakeRedis()
        reg = RedisSchemaRegistry(redis_client)
        with self.assertRaises(SchemaDefinitionError):
            reg.register_schema("S", proto([{"name": "a", "number": 1}, {"name": "b", "number": 1}]), "PROTOBUF")
        self.assertEqual(redis_client.store, {})

    def plant(self, redis_client, name, definition, schema_type="PROTOBUF"):
        redis_client.hset(f"postoffice:schemas:meta:{name}", mapping={
            "schema_id": "7", "name": name, "schema_type": schema_type, "definition": definition, "version": "1"})
        redis_client.hset("postoffice:schemas:id_to_name", mapping={"7": name})

    def test_hostile_definition_in_redis_is_treated_as_missing(self):
        redis_client = FakeRedis()
        fields = [{"name": f"f{i}", "type": "int32", "number": i + 1} for i in range(1000)]
        self.plant(redis_client, "evil", json.dumps(proto(fields)))
        reg = RedisSchemaRegistry(redis_client)
        with self.assertLogs("postoffice.schema.registry", level="ERROR"):
            self.assertIsNone(reg.get_schema("evil"))
        self.assertIsNone(reg.get_schema_by_id(7))
        self.assertIsNone(reg.local_cache.get_schema("evil"))

    def test_oversize_raw_definition_is_not_parsed(self):
        redis_client = FakeRedis()
        self.plant(redis_client, "huge", "[" * 200000)
        reg = RedisSchemaRegistry(redis_client)
        with patch("postoffice.schema.registry.json.loads", side_effect=AssertionError("parsed")):
            self.assertIsNone(reg.get_schema("huge"))

    def test_valid_definition_in_redis_still_loads(self):
        redis_client = FakeRedis()
        self.plant(redis_client, "ok", json.dumps(proto()))
        self.assertIsNotNone(RedisSchemaRegistry(redis_client).get_schema("ok"))

    def test_error_messages_do_not_echo_definition_contents(self):
        secret = "hunter2-do-not-log"
        with self.assertRaises(SchemaDefinitionError) as ctx:
            validate_schema_definition("S", proto([{"name": secret + " bad", "number": 1}]), "PROTOBUF",
                                       SchemaLimits())
        self.assertNotIn(secret, str(ctx.exception))


class TestLimitsConfig(unittest.TestCase):
    def test_defaults(self):
        lim = SchemaLimits()
        self.assertEqual((lim.max_bytes, lim.max_fields, lim.max_depth), (64 * 1024, 256, 32))

    def test_from_env(self):
        env = {"POSTOFFICE_SCHEMA_MAX_BYTES": "1000", "POSTOFFICE_SCHEMA_MAX_FIELDS": "5",
               "POSTOFFICE_SCHEMA_MAX_DEPTH": "4"}
        with patch.dict(os.environ, env):
            lim = SchemaLimits.from_env()
        self.assertEqual((lim.max_bytes, lim.max_fields, lim.max_depth), (1000, 5, 4))

    def test_invalid_limits_are_rejected(self):
        for kw in ({"max_bytes": 0}, {"max_fields": -1}, {"max_depth": 0}):
            with self.subTest(kw=kw), self.assertRaises(ValueError):
                SchemaLimits(**kw)
        with patch.dict(os.environ, {"POSTOFFICE_SCHEMA_MAX_FIELDS": "lots"}):
            with self.assertRaises(ValueError):
                SchemaLimits.from_env()

    def test_default_registry_picks_up_the_environment(self):
        with patch.dict(os.environ, {"POSTOFFICE_SCHEMA_MAX_FIELDS": "1"}):
            reg = InMemorySchemaRegistry()
        with self.assertRaises(SchemaDefinitionError):
            reg.register_schema("S", proto([{"name": "a", "number": 1}, {"name": "b", "number": 2}]), "PROTOBUF")


if __name__ == "__main__":
    unittest.main()
