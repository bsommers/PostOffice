import unittest
import json
from unittest.mock import MagicMock
from postoffice.schema.registry import (
    InMemorySchemaRegistry,
    RedisSchemaRegistry,
    SchemaEntry,
)

class TestSchemaRegistry(unittest.TestCase):
    def test_in_memory_avro_registration(self):
        registry = InMemorySchemaRegistry()
        schema_dict = {
            "type": "record",
            "name": "User",
            "fields": [
                {"name": "username", "type": "string"},
                {"name": "age", "type": "int"}
            ]
        }

        schema_id = registry.register_schema("UserSchema", schema_dict, schema_type="AVRO")
        self.assertEqual(schema_id, 1)

        entry_by_name = registry.get_schema("UserSchema")
        self.assertIsNotNone(entry_by_name)
        self.assertEqual(entry_by_name.schema_id, 1)
        self.assertEqual(entry_by_name.name, "UserSchema")
        self.assertEqual(entry_by_name.schema_type, "AVRO")
        self.assertEqual(entry_by_name.version, 1)

        entry_by_id = registry.get_schema_by_id(1)
        self.assertEqual(entry_by_id, entry_by_name)

    def test_in_memory_protobuf_registration(self):
        registry = InMemorySchemaRegistry()
        proto_dict = {
            "package": "events",
            "message_name": "LoginEvent",
            "fields": [
                {"name": "user_id", "type": "string", "number": 1},
                {"name": "success", "type": "bool", "number": 2}
            ]
        }

        schema_id = registry.register_schema("LoginEvent", proto_dict, schema_type="PROTOBUF")
        self.assertEqual(schema_id, 1)

        entry = registry.get_schema("LoginEvent")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.schema_type, "PROTOBUF")
        # Ensure it generated a valid callable message class
        msg_instance = entry.parsed_schema(user_id="u123", success=True)
        self.assertEqual(msg_instance.user_id, "u123")
        self.assertTrue(msg_instance.success)

    def test_schema_version_increment(self):
        registry = InMemorySchemaRegistry()
        s1 = {"type": "record", "name": "V", "fields": [{"name": "f1", "type": "string"}]}
        s2 = {"type": "record", "name": "V", "fields": [{"name": "f1", "type": "string"}, {"name": "f2", "type": "int"}]}

        id1 = registry.register_schema("Versioned", s1)
        self.assertEqual(registry.get_schema("Versioned").version, 1)

        id2 = registry.register_schema("Versioned", s2)
        self.assertEqual(id1, id2)
        self.assertEqual(registry.get_schema("Versioned").version, 2)

    def test_unsupported_schema_type(self):
        registry = InMemorySchemaRegistry()
        with self.assertRaises(ValueError):
            registry.register_schema("Invalid", {}, schema_type="THRIFT")

    def test_missing_schema_returns_none(self):
        registry = InMemorySchemaRegistry()
        self.assertIsNone(registry.get_schema("NonExistent"))
        self.assertIsNone(registry.get_schema_by_id(999))

    def test_redis_schema_registry(self):
        mock_redis = MagicMock()
        storage = {}

        def mock_incr(key):
            storage[key] = storage.get(key, 0) + 1
            return storage[key]

        def mock_hset(key, field=None, value=None, mapping=None):
            if key not in storage:
                storage[key] = {}
            if mapping:
                storage[key].update(mapping)
            if field is not None and value is not None:
                storage[key][field] = value
            return 1

        def mock_hgetall(key):
            return storage.get(key, {})

        def mock_hget(key, field):
            return storage.get(key, {}).get(field)

        mock_redis.incr.side_effect = mock_incr
        mock_redis.hset.side_effect = mock_hset
        mock_redis.hgetall.side_effect = mock_hgetall
        mock_redis.hget.side_effect = mock_hget

        redis_reg = RedisSchemaRegistry(mock_redis, prefix="test:schemas")
        schema_dict = {
            "type": "record",
            "name": "Metric",
            "fields": [{"name": "cpu", "type": "float"}]
        }

        schema_id = redis_reg.register_schema("MetricSchema", schema_dict, "AVRO")
        self.assertEqual(schema_id, 1)

        # Retrieve by name and verify
        entry = redis_reg.get_schema("MetricSchema")
        self.assertIsNotNone(entry)
        self.assertEqual(entry.name, "MetricSchema")
        self.assertEqual(entry.schema_id, 1)

        # Retrieve by ID and verify
        entry_by_id = redis_reg.get_schema_by_id(1)
        self.assertIsNotNone(entry_by_id)
        self.assertEqual(entry_by_id.name, "MetricSchema")
        self.assertIn("MetricSchema", repr(entry_by_id))

        # Re-register existing schema in Redis to test version increment
        schema_id_v2 = redis_reg.register_schema("MetricSchema", schema_dict, "AVRO")
        self.assertEqual(schema_id_v2, 1)

        # Instantiate a second RedisSchemaRegistry (cold local_cache) against the same Redis storage
        # to exercise cold-cache Redis hydration in get_schema() and get_schema_by_id()
        cold_reg_1 = RedisSchemaRegistry(mock_redis, prefix="test:schemas")
        hydrated_by_name = cold_reg_1.get_schema("MetricSchema")
        self.assertIsNotNone(hydrated_by_name)
        self.assertEqual(hydrated_by_name.name, "MetricSchema")
        self.assertIsNone(cold_reg_1.get_schema("UnknownSchema"))

        cold_reg_2 = RedisSchemaRegistry(mock_redis, prefix="test:schemas")
        hydrated_by_id = cold_reg_2.get_schema_by_id(1)
        self.assertIsNotNone(hydrated_by_id)
        self.assertEqual(hydrated_by_id.name, "MetricSchema")
        self.assertIsNone(cold_reg_2.get_schema_by_id(999))

    def test_avro_json_string_and_protobuf_repeated_fields(self):
        registry = InMemorySchemaRegistry()
        avro_json_str = json.dumps({
            "type": "record",
            "name": "StrSchema",
            "fields": [{"name": "id", "type": "string"}]
        })
        sid = registry.register_schema("StrSchema", avro_json_str, "AVRO")
        self.assertEqual(sid, 1)

        proto_repeated = {
            "package": "pkg",
            "message_name": "Batch",
            "fields": [{"name": "tags", "type": "string", "number": 1, "label": "repeated"}]
        }
        pid = registry.register_schema("Batch", proto_repeated, "PROTOBUF")
        self.assertEqual(pid, 2)
        entry = registry.get_schema("Batch")
        # Register using already compiled Message class directly
        pid2 = registry.register_schema("BatchDirect", entry.parsed_schema, "PROTOBUF")
        self.assertEqual(pid2, 3)

        with self.assertRaises(ValueError):
            registry.register_schema("BadProto", 12345, "PROTOBUF")


if __name__ == "__main__":
    unittest.main()
