import json
import unittest

from postoffice.shadow import ShadowHarness


class TestPhase3ShadowMode(unittest.TestCase):
    def _base_actions(self, unroutable_policy: str = "nack") -> list[dict]:
        avro_schema = json.dumps(
            {
                "type": "record",
                "name": "SensorReading",
                "fields": [
                    {"name": "device", "type": "string"},
                    {"name": "temp", "type": "double"},
                ],
            }
        )
        proto_schema = json.dumps(
            {
                "name": "AlertMsg",
                "fields": [
                    {"name": "code", "type": "string", "number": 1},
                    {"name": "severity", "type": "int64", "number": 2},
                ],
            }
        )
        return [
            {
                "action": "set_dlq",
                "target_broker": "dlq_brk",
                "topic": "system/dlq",
                "rate_per_sec": 100.0,
                "burst": 50,
                "dedupe_window_sec": 60.0,
                "max_payload_bytes": 64,
            },
            {
                "action": "register_schema",
                "name": "SensorReading",
                "schema_str": avro_schema,
                "schema_type": "AVRO",
                "version": 1,
                "compatibility": "BACKWARD",
            },
            {
                "action": "register_schema",
                "name": "AlertMsg",
                "schema_str": proto_schema,
                "schema_type": "PROTOBUF",
                "version": 1,
                "compatibility": "BACKWARD",
            },
            {
                "action": "set_ingress",
                "rules": [
                    {
                        "type": "max_payload_bytes",
                        "max_bytes": 128,
                        "action": "drop",
                    },
                    {
                        "type": "topic_shape",
                        "deny": ["sensors/banned/temp"],
                        "action": "drop",
                    },
                    {
                        "type": "content_guard",
                        "topic": "#",
                        "format": "json",
                        "max_depth": 6,
                        "action": "drop",
                    },
                    {
                        "type": "rate_limit",
                        "rate": 1000.0,
                        "burst": 100.0,
                        "action": "drop",
                    },
                ],
            },
            {
                "action": "add_route",
                "source_broker": "mqtt_edge",
                "source_topic": "sensors/+/temp",
                "target_broker": "kafka_core",
                "target_topic": "telemetry.avro",
                "kwargs": {
                    "input_format": "json",
                    "output_format": "avro",
                    "output_schema": "SensorReading",
                    "confluent_framing": True,
                    "key": "sensor-key",
                },
            },
            {
                "action": "add_route",
                "source_broker": "mqtt_edge",
                "source_topic": "alerts/#",
                "target_broker": "amqp_core",
                "target_topic": "alerts_q",
                "kwargs": {
                    "input_format": "json",
                    "output_format": "protobuf",
                    "output_schema": "AlertMsg",
                    "qos": 1,
                },
            },
        ]

    def test_shadow_soak_zero_diff_across_routes_schemas_firewall_and_dlq(self):
        harness = ShadowHarness(unroutable_policy="dlq")
        for act in self._base_actions():
            harness.apply_action(act)

        messages = [
            # 1. Valid JSON -> Avro (Confluent framed) + semantic kwargs
            {
                "source_broker": "mqtt_edge",
                "topic": "sensors/s1/temp",
                "payload": b'{"device":"s1","temp":23.5}',
                "kwargs": {"qos": 1},
                "now_ns": 1_000_000_000,
            },
            # 2. Valid JSON -> Protobuf + QoS 1 -> delivery_mode=2 translation
            {
                "source_broker": "mqtt_edge",
                "topic": "alerts/fire",
                "payload": b'{"code":"FIRE","severity":5}',
                "kwargs": {"qos": 1},
                "now_ns": 1_000_000_100,
            },
            # 3. Firewall drop: denied topic
            {
                "source_broker": "mqtt_edge",
                "topic": "sensors/banned/temp",
                "payload": b'{"device":"banned","temp":10.0}',
                "kwargs": {},
                "now_ns": 1_000_000_200,
            },
            # 4. Firewall drop: malformed JSON
            {
                "source_broker": "mqtt_edge",
                "topic": "sensors/s1/temp",
                "payload": b"{not-valid-json",
                "kwargs": {},
                "now_ns": 1_000_000_300,
            },
            # 5. Firewall drop: oversize payload (> 128 bytes)
            {
                "source_broker": "mqtt_edge",
                "topic": "sensors/s1/temp",
                "payload": b'{"device":"' + (b"x" * 140) + b'","temp":1.0}',
                "kwargs": {},
                "now_ns": 1_000_000_400,
            },
            # 6. Schema transformation failure -> dispatched to DLQ
            {
                "source_broker": "mqtt_edge",
                "topic": "sensors/s1/temp",
                "payload": b'{"device":"s1","temp":"NOT_A_NUMBER"}',
                "kwargs": {},
                "now_ns": 2_000_000_000,
            },
            # 7. Duplicate schema failure within dedupe window -> DLQ suppressed in both
            {
                "source_broker": "mqtt_edge",
                "topic": "sensors/s1/temp",
                "payload": b'{"device":"s1","temp":"NOT_A_NUMBER"}',
                "kwargs": {},
                "now_ns": 2_500_000_000,
            },
            # 8. Unroutable message under policy="dlq" with payload > max_payload_bytes (64B) -> truncated DLQ envelope
            {
                "source_broker": "mqtt_edge",
                "topic": "unrouted/stream",
                "payload": b'{"padding":"' + (b"z" * 70) + b'"}',
                "kwargs": {},
                "now_ns": 3_000_000_000,
            },
        ]

        report = harness.run_soak(messages)
        self.assertEqual(report.total_messages, len(messages))
        self.assertEqual(report.diffs, [], f"Unexpected shadow diffs: {report.diffs}")
        self.assertTrue(report.is_clean)

    def test_shadow_broker_outage_and_resync_drill(self):
        harness = ShadowHarness(unroutable_policy="nack")
        for act in self._base_actions():
            harness.apply_action(act)

        # Simulate target broker outage on kafka_core: both engines must nack(requeue=True)
        step_outage = harness.step(
            source_broker="mqtt_edge",
            topic="sensors/s2/temp",
            payload=b'{"device":"s2","temp":19.0}',
            kwargs={"qos": 1},
            now_ns=5_000_000_000,
            egress_failures=["kafka_core"],
        )
        self.assertEqual(step_outage, [])

        # Clear state & resync cleanly in both engines
        harness.apply_action({"action": "clear_state"})
        for act in self._base_actions():
            harness.apply_action(act)

        # Broker recovered -> both engines publish and ack identically
        step_recovered = harness.step(
            source_broker="mqtt_edge",
            topic="sensors/s2/temp",
            payload=b'{"device":"s2","temp":19.0}',
            kwargs={"qos": 1},
            now_ns=6_000_000_000,
        )
        self.assertEqual(step_recovered, [])

    def test_shadow_harness_detects_injected_divergence(self):
        harness = ShadowHarness(unroutable_policy="nack")
        for act in self._base_actions():
            harness.apply_action(act)

        # Mutate ONLY the native engine by adding a registered extra broker & route so outputs diverge
        harness.native.register_broker("extra_brk", "mqtt", {})
        harness.native.apply_action(
            {
                "action": "add_route",
                "source_broker": "mqtt_edge",
                "source_topic": "sensors/+/temp",
                "target_broker": "extra_brk",
                "target_topic": "shadow/extra",
                "kwargs": {},
            }
        )
        diffs = harness.step(
            source_broker="mqtt_edge",
            topic="sensors/s1/temp",
            payload=b'{"device":"s1","temp":20.0}',
            kwargs={},
            now_ns=7_000_000_000,
        )
        self.assertGreaterEqual(len(diffs), 1)
        self.assertEqual(diffs[0].field, "egress")


if __name__ == "__main__":
    unittest.main()
