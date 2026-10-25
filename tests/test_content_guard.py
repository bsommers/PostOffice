import json
import time
import unittest
from unittest.mock import MagicMock, patch

from postoffice.app import PostOffice
from postoffice.ingress import Action, ContentGuard, Firewall, MessageContext
from postoffice.schema.converters import SchemaConverter
from postoffice.schema.registry import InMemorySchemaRegistry

READING = {"type": "record", "name": "Reading", "fields": [{"name": "v", "type": "int"}]}


def ctx(payload, topic="fleet/d1/telemetry", broker="src"):
    return MessageContext(broker=broker, topic=topic, size=len(payload) if payload else 0,
                          payload=payload)


def js(obj):
    return json.dumps(obj).encode()


def guard(**kw):
    kw.setdefault("topic", "fleet/+/telemetry")
    return ContentGuard(**kw)


class TestScope(unittest.TestCase):
    def test_non_matching_topic_is_skipped_without_parsing(self):
        g = guard()
        with patch("postoffice.ingress.content.json.loads") as loads:
            self.assertEqual(g.check(ctx(b"\xff garbage", topic="other/topic")).action, Action.ALLOW)
        loads.assert_not_called()

    def test_broker_scope(self):
        g = guard(brokers=["only"])
        self.assertEqual(g.check(ctx(b"not json", broker="src")).action, Action.ALLOW)
        self.assertEqual(g.check(ctx(b"not json", broker="only")).action, Action.REJECT)

    def test_missing_payload_cannot_be_inspected_and_is_allowed(self):
        self.assertEqual(guard().check(ctx(None)).action, Action.ALLOW)

    def test_valid_json_passes(self):
        self.assertEqual(guard().check(ctx(js({"a": [1, 2], "b": {"c": "x"}}))).action, Action.ALLOW)


class TestStructuralLimits(unittest.TestCase):
    def reject(self, g, payload, needle):
        v = g.check(ctx(payload))
        self.assertEqual((v.action, v.category, v.rule), (Action.REJECT, "malformed", "content_guard"))
        self.assertIn(needle, v.reason)

    def test_deep_nesting_rejected_without_recursion_error(self):
        self.reject(guard(max_depth=6), b"[" * 100000 + b"]" * 100000, "depth")

    def test_depth_at_limit_allowed(self):
        self.assertEqual(guard(max_depth=3).check(ctx(js([[[1]]]))).action, Action.ALLOW)

    def test_too_many_keys(self):
        self.reject(guard(max_keys=5), js({str(i): i for i in range(6)}), "keys")

    def test_keys_counted_across_nesting(self):
        self.reject(guard(max_keys=3), js({"a": {"b": 1, "c": {"d": 2}}}), "keys")

    def test_long_array(self):
        self.reject(guard(max_array_length=10), js(list(range(11))), "array")

    def test_array_at_limit_allowed(self):
        self.assertEqual(guard(max_array_length=10).check(ctx(js(list(range(10))))).action,
                         Action.ALLOW)

    def test_long_string(self):
        self.reject(guard(max_string_length=20), js({"k": "x" * 21}), "string")

    def test_invalid_utf8(self):
        self.reject(guard(), b'{"k": "\xff\xfe"}', "utf-8")

    def test_invalid_json(self):
        self.reject(guard(), b'{"k": ', "json")

    def test_scan_is_linear_on_unterminated_strings(self):
        start = time.perf_counter()
        guard().check(ctx(b'"\\' * 50000))
        self.assertLess(time.perf_counter() - start, 1.0)


class TestBusinessRules(unittest.TestCase):
    def mismatch(self, g, obj, needle):
        v = g.check(ctx(js(obj)))
        self.assertEqual((v.action, v.category), (Action.REJECT, "schema_mismatch"))
        self.assertIn(needle, v.reason)

    def test_required_field_missing(self):
        self.mismatch(guard(required_fields=["id", "meta.site"]), {"id": 1, "meta": {}}, "meta.site")

    def test_required_fields_present(self):
        g = guard(required_fields=["id", "meta.site"])
        self.assertEqual(g.check(ctx(js({"id": 1, "meta": {"site": "a"}}))).action, Action.ALLOW)

    def test_range_above_max_and_below_min(self):
        g = guard(field_ranges={"qty": {"min": 0, "max": 10000}})
        self.mismatch(g, {"qty": 10001}, "qty")
        self.mismatch(g, {"qty": -1}, "qty")
        self.assertEqual(g.check(ctx(js({"qty": 5}))).action, Action.ALLOW)

    def test_range_on_non_number(self):
        self.mismatch(guard(field_ranges={"qty": {"max": 10}}), {"qty": "lots"}, "qty")

    def test_range_rejects_bool_as_number(self):
        self.mismatch(guard(field_ranges={"qty": {"max": 10}}), {"qty": True}, "qty")

    def test_enum(self):
        g = guard(enums={"status": ["ok", "fail"]})
        self.mismatch(g, {"status": "weird"}, "status")
        self.assertEqual(g.check(ctx(js({"status": "ok"}))).action, Action.ALLOW)

    def test_optional_absent_field_skipped(self):
        g = guard(field_ranges={"qty": {"max": 10}}, enums={"status": ["ok"]})
        self.assertEqual(g.check(ctx(js({"other": 1}))).action, Action.ALLOW)

    def test_non_object_root_with_field_rules(self):
        self.mismatch(guard(required_fields=["id"]), [1, 2], "id")

    def test_action_override(self):
        g = guard(required_fields=["id"], action="drop")
        self.assertEqual(g.check(ctx(js({}))).action, Action.DROP)


class TestSchemaBinding(unittest.TestCase):
    def setUp(self):
        self.registry = InMemorySchemaRegistry()
        self.registry.register_schema("Reading", READING, "AVRO")
        self.converter = SchemaConverter(self.registry)
        self.valid = self.converter.serialize({"v": 7}, "avro", "Reading")

    def bound(self, **kw):
        g = guard(format="avro", schema="Reading", **kw)
        g.bind_converter(self.converter)
        return g

    def test_valid_avro_passes(self):
        self.assertEqual(self.bound().check(ctx(self.valid)).action, Action.ALLOW)

    def test_garbage_avro_is_schema_mismatch(self):
        v = self.bound().check(ctx(b"\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff"))
        self.assertEqual((v.action, v.category), (Action.REJECT, "schema_mismatch"))

    def test_binding_is_per_topic_pattern(self):
        self.registry.register_schema(
            "Other", {"type": "record", "name": "Other",
                      "fields": [{"name": "s", "type": "string"}]}, "AVRO")
        a = guard(topic="plant1/+/temperature", format="avro", schema="Reading")
        b = guard(topic="plant1/+/status", format="avro", schema="Other")
        for g in (a, b):
            g.bind_converter(self.converter)
        fw = Firewall([a, b])
        self.assertEqual(fw.evaluate(ctx(self.valid, topic="plant1/m1/temperature")).action,
                         Action.ALLOW)
        self.assertEqual(fw.evaluate(ctx(self.valid, topic="plant1/m1/status")).category,
                         "schema_mismatch")

    def test_unbound_converter_fails_closed_via_rule_error(self):
        g = guard(format="avro", schema="Reading")
        v = Firewall([g]).evaluate(ctx(self.valid))
        self.assertEqual((v.action, v.category), (Action.REJECT, "rule_error"))

    def test_oversized_avro_is_rejected_before_decoding(self):
        g = self.bound(max_bytes=10)
        self.converter.deserialize = MagicMock()
        self.assertEqual(g.check(ctx(b"x" * 11)).category, "oversize")
        self.converter.deserialize.assert_not_called()


class TestConfig(unittest.TestCase):
    def test_invalid_config(self):
        for kw in ({"format": "xml"}, {"max_depth": 0}, {"format": "json", "schema": "S"},
                   {"format": "avro"}, {"field_ranges": {"q": {"bogus": 1}}}):
            with self.assertRaises(ValueError, msg=str(kw)):
                guard(**kw)

    def test_from_dict_and_router_binds_converter(self):
        po = PostOffice()
        po.register_schema("Reading", READING, "AVRO")
        po.configure_ingress({"rules": [
            {"type": "content_guard", "topic": "fleet/+/telemetry", "format": "avro",
             "schema": "Reading"}]})
        nack, ack = MagicMock(), MagicMock()
        po.router.route("src", "fleet/d1/telemetry", b"\xff" * 11, ack_fn=ack, nack_fn=nack)
        nack.assert_called_once_with(requeue=False)
        ack.assert_not_called()

    def test_violations_feed_the_sender_ledger(self):
        po = PostOffice()
        po.configure_ingress({
            "rules": [{"type": "content_guard", "topic": "fleet/{id}/telemetry".replace("{id}", "+"),
                       "required_fields": ["id"]}],
            "ledger": {"identity_template": "fleet/{id}/telemetry"},
        })
        po.router.route("src", "fleet/d5/telemetry", b"{}", nack_fn=MagicMock())
        row = po.sender_report()[0]
        self.assertEqual((row["sender"], row["categories"]), ("d5", {"schema_mismatch": 1}))


if __name__ == "__main__":
    unittest.main()
