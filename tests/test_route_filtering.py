"""
Hermetic unit and differential tests for Dynamic Content-Based Route Filtering (`FLT-01`).
"""

import json
import unittest
from unittest.mock import MagicMock

from postoffice.filtering import evaluate_filter, validate_filter_spec
from postoffice.native import NativeEngine, native_validate_action
from postoffice.router import Router
from postoffice.shadow import ShadowHarness
from postoffice.validation import PayloadError, validate_action


class TestFilterValidationAndEvaluation(unittest.TestCase):
    def test_all_operators_and_nested_paths(self):
        payload = json.dumps(
            {
                "device": "sensor-eu-01",
                "active": True,
                "count": 1,
                "zero": 0,
                "disabled": False,
                "null_field": None,
                "tags": ["edge", "critical", 42],
                "sensor": {
                    "readings": [
                        {"temp": 19.5, "unit": "C"},
                        {"temp": 28.4, "unit": "C"},
                    ]
                },
                "matrix": [[10, 20], [30, 40]],
            }
        ).encode("utf-8")
        kwargs = {
            "qos": 1,
            "headers": {"x-region": "eu-west-1", "priority": 5},
        }

        cases = [
            ({"path": "$.device", "op": "==", "value": "sensor-eu-01"}, True),
            ({"path": "device", "op": "eq", "value": "sensor-eu-01"}, True),
            ({"path": "$.device", "op": "!=", "value": "sensor-us-01"}, True),
            ({"path": "$.device", "op": "ne", "value": "sensor-eu-01"}, False),
            ({"path": "$.missing", "op": "!=", "value": "anything"}, False),
            ({"path": "$.sensor.readings[1].temp", "op": ">", "value": 25}, True),
            ({"path": "$.sensor.readings[1].temp", "op": "gt", "value": 28.4}, False),
            ({"path": "$.sensor.readings[1].temp", "op": ">=", "value": 28.4}, True),
            ({"path": "$.sensor.readings[0].temp", "op": "gte", "value": 20}, False),
            ({"path": "$.sensor.readings[0].temp", "op": "<", "value": 20.0}, True),
            ({"path": "$.sensor.readings[0].temp", "op": "lt", "value": 19.5}, False),
            ({"path": "$.sensor.readings[0].temp", "op": "<=", "value": 19.5}, True),
            ({"path": "$.sensor.readings[0].temp", "op": "lte", "value": 19.0}, False),
            ({"path": "$.device", "op": ">", "value": "sensor-a"}, True),
            ({"path": "$.device", "op": "<", "value": "sensor-z"}, True),
            ({"path": "$.device", "op": ">", "value": 10}, False),
            ({"path": "$.matrix[1][0]", "op": "==", "value": 30}, True),
            ({"path": "$.tags", "op": "contains", "value": "critical"}, True),
            ({"path": "$.tags", "op": "contains", "value": 42}, True),
            ({"path": "$.tags", "op": "contains", "value": True}, False),
            ({"path": "$.device", "op": "contains", "value": "-eu-"}, True),
            ({"path": "$.device", "op": "in", "value": ["sensor-us-01", "sensor-eu-01"]}, True),
            ({"path": "$.device", "op": "in", "value": "cluster:sensor-eu-01:main"}, True),
            ({"path": "$.device", "op": "prefix", "value": "sensor-eu"}, True),
            ({"path": "$.device", "op": "startswith", "value": "sensor-us"}, False),
            ({"path": "$.device", "op": "suffix", "value": "-01"}, True),
            ({"path": "$.device", "op": "endswith", "value": "-02"}, False),
            ({"path": "$.sensor.readings[1].temp", "op": "exists"}, True),
            ({"path": "$.sensor.readings[1].temp", "op": "exists", "value": True}, True),
            ({"path": "$.sensor.readings[1].temp", "op": "exists", "value": False}, False),
            ({"path": "$.missing.field", "op": "exists", "value": False}, True),
            ({"path": "$.null_field", "op": "exists", "value": True}, False),
            ({"path": "$.null_field", "op": "exists", "value": False}, True),
            ({"path": "$kwargs.qos", "op": "==", "value": 1}, True),
            ({"path": "$headers.x-region", "op": "==", "value": "eu-west-1"}, True),
            ({"path": "$headers.priority", "op": ">=", "value": 3}, True),
            ({"path": "$headers.missing", "op": "exists", "value": False}, True),
        ]

        for spec, expected in cases:
            with self.subTest(spec=spec):
                validate_filter_spec(spec)
                self.assertEqual(evaluate_filter(spec, payload, kwargs), expected)

    def test_root_array_indexing(self):
        payload = json.dumps([{"id": "a0"}, {"id": "a1"}]).encode("utf-8")
        spec = {"path": "$[1].id", "op": "==", "value": "a1"}
        validate_filter_spec(spec)
        self.assertTrue(evaluate_filter(spec, payload))

    def test_bool_vs_int_strictness(self):
        payload = json.dumps(
            {"flag_true": True, "flag_false": False, "num_one": 1, "num_zero": 0}
        ).encode("utf-8")

        self.assertFalse(
            evaluate_filter({"path": "$.flag_true", "op": "==", "value": 1}, payload)
        )
        self.assertFalse(
            evaluate_filter({"path": "$.flag_false", "op": "==", "value": 0}, payload)
        )
        self.assertFalse(
            evaluate_filter({"path": "$.num_one", "op": "==", "value": True}, payload)
        )
        self.assertFalse(
            evaluate_filter({"path": "$.num_zero", "op": "==", "value": False}, payload)
        )
        self.assertTrue(
            evaluate_filter({"path": "$.flag_true", "op": "!=", "value": 1}, payload)
        )
        self.assertFalse(
            evaluate_filter({"path": "$.flag_true", "op": "in", "value": [1, 2, 3]}, payload)
        )
        self.assertTrue(
            evaluate_filter({"path": "$.flag_true", "op": "in", "value": [1, True]}, payload)
        )

    def test_combinators_all_any_not(self):
        payload = json.dumps({"env": "prod", "severity": 4, "region": "us-east"}).encode("utf-8")
        spec = {
            "all": [
                {"path": "$.env", "op": "==", "value": "prod"},
                {
                    "any": [
                        {"path": "$.severity", "op": ">=", "value": 5},
                        {"path": "$.region", "op": "prefix", "value": "us-"},
                    ]
                },
                {"not": {"path": "$.muted", "op": "==", "value": True}},
            ]
        }
        validate_filter_spec(spec)
        self.assertTrue(evaluate_filter(spec, payload))

        payload_blocked = json.dumps(
            {"env": "prod", "severity": 2, "region": "eu-central", "muted": False}
        ).encode("utf-8")
        self.assertFalse(evaluate_filter(spec, payload_blocked))

    def test_non_json_payload_handling(self):
        raw = b"\x89PNG\r\n\x1a\n\x00\x00binary-non-json"
        self.assertFalse(
            evaluate_filter({"path": "$.a", "op": "==", "value": 1}, raw)
        )
        self.assertFalse(
            evaluate_filter({"path": "$.a", "op": "!=", "value": 1}, raw)
        )
        self.assertFalse(
            evaluate_filter({"path": "$.a", "op": "exists", "value": False}, raw)
        )
        # Metadata filters on $kwargs / $headers still work on binary payloads
        self.assertTrue(
            evaluate_filter(
                {"path": "$headers.content-type", "op": "==", "value": "image/png"},
                raw,
                {"headers": {"content-type": "image/png"}},
            )
        )

    def test_invalid_filter_specs_rejected(self):
        invalid_specs = [
            None,
            {},
            [],
            "not-a-dict",
            {"all": []},
            {"any": "not-a-list"},
            {"all": [{"path": "$.a", "op": "==", "value": 1}], "extra": 1},
            {"path": "", "op": "==", "value": 1},
            {"path": "$", "op": "==", "value": 1},
            {"path": "$.", "op": "==", "value": 1},
            {"path": "$kwargs.", "op": "==", "value": 1},
            {"path": "$unknown.a", "op": "==", "value": 1},
            {"path": "$.a..b", "op": "==", "value": 1},
            {"path": "$.a[]", "op": "==", "value": 1},
            {"path": "$.a[-1]", "op": "==", "value": 1},
            {"path": "$.a[abc]", "op": "==", "value": 1},
            {"path": "$.a[0", "op": "==", "value": 1},
            {"path": "$.a", "op": "regex", "value": ".*"},
            {"path": "$.a", "op": "=="},
            {"path": "$.a", "op": ">", "value": True},
            {"path": "$.a", "op": ">", "value": [1, 2]},
            {"path": "$.a", "op": "in", "value": 123},
            {"path": "$.a", "op": "prefix", "value": 123},
            {"path": "$.a", "op": "exists", "value": "yes"},
            {"path": "$.a", "op": "==", "value": 1, "unknown_key": True},
        ]
        for bad in invalid_specs:
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    validate_filter_spec(bad)

        # Exceeding max_depth=8
        deep = {"path": "$.a", "op": "==", "value": 1}
        for _ in range(8):
            deep = {"not": deep}
        with self.assertRaises(ValueError):
            validate_filter_spec(deep, max_depth=8)

    def test_router_and_control_plane_validation(self):
        router = Router()
        with self.assertRaises(ValueError):
            router.add_route(
                "src", "t/in", "dst", "t/out", filter={"path": "$.a", "op": "bad_op", "value": 1}
            )

        with self.assertRaises(PayloadError):
            validate_action(
                "add_route",
                {
                    "source_broker": "src",
                    "source_topic": "t/in",
                    "target_broker": "dst",
                    "target_topic": "t/out",
                    "target_kwargs": {"filter": {"path": "$.a", "op": "bad_op", "value": 1}},
                },
            )

        self.assertFalse(
            native_validate_action(
                "add_route",
                {
                    "source_broker": "src",
                    "source_topic": "t/in",
                    "target_broker": "dst",
                    "target_topic": "t/out",
                    "target_kwargs": {"filter": {"path": "$.a", "op": "bad_op", "value": 1}},
                },
            )
        )
        self.assertTrue(
            native_validate_action(
                "add_route",
                {
                    "source_broker": "src",
                    "source_topic": "t/in",
                    "target_broker": "dst",
                    "target_topic": "t/out",
                    "target_kwargs": {"filter": {"path": "$.a", "op": "==", "value": 1}},
                },
            )
        )
        eng = NativeEngine()
        eng.register_broker("src", "mqtt")
        eng.register_broker("dst", "mqtt")
        eng.add_route(
            "src",
            "t/in",
            "dst",
            "t/out",
            {"filter": {"path": "$.a", "op": "bad_op", "value": 1}},
        )
        out = eng.route("src", "t/in", b'{"a": 1}')
        self.assertEqual(len(out["published"]), 0)
        self.assertTrue(out["nacked"])

    def test_router_content_based_fanout_and_exclusion(self):
        router = Router()
        src = MagicMock()
        src.name = "src"
        hot_dst = MagicMock()
        hot_dst.name = "hot_dst"
        hot_dst.publish.side_effect = (
            lambda *a, **kw: kw["on_confirm"]() if kw.get("on_confirm") else None
        )
        cold_dst = MagicMock()
        cold_dst.name = "cold_dst"
        audit_dst = MagicMock()
        audit_dst.name = "audit_dst"

        router.register_client(src)
        router.register_client(hot_dst)
        router.register_client(cold_dst)
        router.register_client(audit_dst)

        router.add_route(
            "src",
            "telemetry/+",
            "hot_dst",
            "alerts/hot",
            filter={"path": "$.temp", "op": ">=", "value": 30},
            qos=1,
        )
        router.add_route(
            "src",
            "telemetry/+",
            "cold_dst",
            "alerts/cold",
            filter={"path": "$.temp", "op": "<", "value": 0},
        )
        router.add_route(
            "src",
            "telemetry/+",
            "audit_dst",
            "audit/all",
            filter={"path": "$.audit", "op": "==", "value": True},
        )

        # 1. Matches hot_dst only
        acked = []
        router.route(
            "src",
            "telemetry/s1",
            b'{"temp": 35.2, "audit": false}',
            ack_fn=lambda: acked.append(True),
        )
        self.assertEqual(len(acked), 1)
        hot_dst.publish.assert_called_once()
        _, pub_kwargs = hot_dst.publish.call_args
        self.assertNotIn("filter", pub_kwargs)
        self.assertEqual(pub_kwargs.get("qos"), 1)
        cold_dst.publish.assert_not_called()
        audit_dst.publish.assert_not_called()

        # 2. Matches no filter -> still acked cleanly without DLQ/nack
        hot_dst.publish.reset_mock()
        acked.clear()
        nacked = []
        router.route(
            "src",
            "telemetry/s1",
            b'{"temp": 20.0, "audit": false}',
            ack_fn=lambda: acked.append(True),
            nack_fn=lambda **_kw: nacked.append(True),
        )
        self.assertEqual(len(acked), 1)
        self.assertEqual(len(nacked), 0)
        hot_dst.publish.assert_not_called()
        cold_dst.publish.assert_not_called()
        audit_dst.publish.assert_not_called()


class TestShadowParityRouteFiltering(unittest.TestCase):
    def test_shadow_harness_250_messages_zero_divergence(self):
        harness = ShadowHarness(unroutable_policy="dlq")
        harness.apply_action({"action": "set_dlq", "broker": "dlq_b", "topic": "dlq/main"})

        # Route 1: High temperature alerts in eu-west or us-east
        harness.apply_action(
            {
                "action": "add_route",
                "source_broker": "ingest",
                "source_topic": "sensors/+/data",
                "target_broker": "alert_broker",
                "target_topic": "alerts/high_temp",
                "target_kwargs": {
                    "qos": 1,
                    "filter": {
                        "all": [
                            {"path": "$.readings[0].temp", "op": ">=", "value": 30.0},
                            {"path": "$headers.region", "op": "in", "value": ["eu-west", "us-east"]},
                            {"not": {"path": "$.maintenance", "op": "==", "value": True}},
                        ]
                    },
                },
            }
        )

        # Route 2: Battery warning or explicit error code
        harness.apply_action(
            {
                "action": "add_route",
                "source_broker": "ingest",
                "source_topic": "sensors/+/data",
                "target_broker": "ops_broker",
                "target_topic": "ops/battery",
                "target_kwargs": {
                    "filter": {
                        "any": [
                            {"path": "$.battery", "op": "<=", "value": 15},
                            {"path": "$.tags", "op": "contains", "value": "urgent"},
                            {"path": "$.device_id", "op": "prefix", "value": "crit-"},
                        ]
                    }
                },
            }
        )

        # Route 3: Audit route checking exists & suffix & kwargs
        harness.apply_action(
            {
                "action": "add_route",
                "source_broker": "ingest",
                "source_topic": "sensors/+/data",
                "target_broker": "audit_broker",
                "target_topic": "audit/stream",
                "target_kwargs": {
                    "filter": {
                        "all": [
                            {"path": "$.firmware", "op": "exists", "value": False},
                            {"path": "$.device_id", "op": "suffix", "value": "-prod"},
                            {"path": "$kwargs.qos", "op": "==", "value": 1},
                        ]
                    }
                },
            }
        )

        regions = ["eu-west", "us-east", "ap-south", "sa-east"]
        messages = []
        for i in range(250):
            if i % 25 == 0:
                # Non-JSON payload
                payload = f"raw-binary-frame-{i}".encode("utf-8")
            elif i % 17 == 0:
                # Bool vs int edge case payload
                payload = json.dumps(
                    {
                        "device_id": f"dev-{i}-prod",
                        "readings": [{"temp": True if i % 2 == 0 else 35.0}],
                        "maintenance": 1 if i % 3 == 0 else False,
                        "battery": False if i % 5 == 0 else 10,
                        "tags": [1, True, "normal"],
                    }
                ).encode("utf-8")
            else:
                temp = 15.0 + (i % 30)
                battery = (i * 7) % 100
                maint = (i % 11) == 0
                dev_prefix = "crit-" if (i % 13 == 0) else "std-"
                dev_suffix = "-prod" if (i % 2 == 0) else "-dev"
                tags = ["urgent", "iot"] if (i % 9 == 0) else ["iot"]
                payload = json.dumps(
                    {
                        "device_id": f"{dev_prefix}{i}{dev_suffix}",
                        "readings": [{"temp": temp}],
                        "maintenance": maint,
                        "battery": battery,
                        "tags": tags,
                    }
                ).encode("utf-8")

            topic = "sensors/s1/data" if (i % 20 != 0) else "unmatched/topic"
            messages.append(
                {
                    "source_broker": "ingest",
                    "topic": topic,
                    "payload": payload,
                    "kwargs": {
                        "qos": i % 2,
                        "headers": {"region": regions[i % len(regions)]},
                    },
                }
            )

        report = harness.run_soak(messages)
        self.assertTrue(
            report.is_clean,
            f"Expected 0 shadow diffs across 250 filtered messages, got: {report.diffs[:5]}",
        )
        self.assertEqual(report.total_messages, 250)


if __name__ == "__main__":
    unittest.main()
