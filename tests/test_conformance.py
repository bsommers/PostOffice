import json
import pathlib
import unittest

from postoffice.conformance import (
    EXPECTED_SUITES,
    PlatformReferenceClient,
    PlatformReferenceServer,
    generate_all_vectors,
    run_conformance,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
VECTORS_DIR = REPO_ROOT / "conformance" / "vectors"


class TestPhase1Conformance(unittest.TestCase):
    def test_generator_is_100_percent_deterministic(self):
        first = generate_all_vectors()
        second = generate_all_vectors()
        self.assertEqual(set(first.keys()), set(EXPECTED_SUITES))
        self.assertEqual(
            json.dumps(first, sort_keys=True),
            json.dumps(second, sort_keys=True),
        )

    def test_committed_vector_files_match_generator(self):
        generated = generate_all_vectors()
        for suite_name, expected_data in generated.items():
            path = VECTORS_DIR / f"{suite_name}.json"
            self.assertTrue(path.is_file(), f"Missing vector file: {path}")
            on_disk = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual(
                on_disk,
                expected_data,
                f"Vector drift in {suite_name}.json",
            )

    def test_python_reference_passes_100_percent_of_conformance_vectors(self):
        passed, failed, errors = run_conformance(VECTORS_DIR)
        self.assertEqual(failed, 0, f"Conformance failures: {errors}")
        self.assertEqual(errors, [])
        self.assertGreaterEqual(passed, 45)

    def test_platform_reference_client_and_server_round_trip(self):
        server = PlatformReferenceServer(bearer_token="secret-token")
        bad_client = PlatformReferenceClient(server, bearer_token="wrong-token")
        with self.assertRaises(PermissionError):
            bad_client.register_broker("mqtt_a", "mock", {})

        client = PlatformReferenceClient(server, bearer_token="secret-token")
        ack1 = client.register_broker("mqtt_a", "mock", {})
        self.assertTrue(ack1["ok"])
        self.assertEqual(ack1["schema_version"], 1)

        ack2 = client.register_broker("amqp_b", "mock", {})
        self.assertTrue(ack2["ok"])

        ack3 = client.add_route(
            source_broker="mqtt_a",
            source_topic="sensors/+",
            target_broker="amqp_b",
            target_topic="ingest/sensors",
            semantic_translation=True,
        )
        self.assertTrue(ack3["ok"])

        sub_id = client.subscribe("amqp_b", "ingest/#")
        pub = client.publish(
            broker="mqtt_a",
            topic="sensors/temp",
            payload=b'{"c": 21.5}',
            sender_id="dev-1",
            metadata={"qos": 1},
        )
        self.assertTrue(pub["accepted"])
        self.assertEqual(pub["firewall_verdict"], "allow")
        self.assertEqual(pub["targets_delivered"], 1)

        frames = client.poll_subscription(sub_id)
        self.assertEqual(len(frames), 1)
        self.assertEqual(frames[0]["source_topic"], "ingest/sensors")
        ack_res = client.ack_delivery(sub_id, frames[0]["delivery_id"])
        self.assertTrue(ack_res["acked"])
        nack_res = client.nack_delivery(sub_id, frames[0]["delivery_id"], requeue=False)
        self.assertTrue(nack_res["nacked"])

        # Exercise remaining Admin & Observe operations
        self.assertTrue(client.add_subscription("mqtt_a", "sensors/#")["ok"])
        self.assertTrue(
            client.set_ingress(
                {"rules": [{"type": "max_payload_bytes", "max_bytes": 1024}]}
            )["ok"]
        )
        self.assertTrue(
            client.set_dlq("amqp_b", "dlq/main", budget={"max_message_bytes": 4096})["ok"]
        )
        self.assertTrue(
            client.register_schema(
                1,
                "AVRO",
                {
                    "type": "record",
                    "name": "S",
                    "fields": [{"name": "c", "type": "double"}],
                },
            )["ok"]
        )
        self.assertTrue(client.get_schema(1)["found"])
        self.assertFalse(client.get_schema(999)["found"])
        self.assertIn("offenders", client.get_sender_report())

        health = client.get_health()
        self.assertEqual(health["status"], "ok")
        ready = client.get_ready()
        self.assertTrue(ready["ready"])
        stats = client.get_stats()
        self.assertEqual(stats["active_brokers"], 2)
        metrics_resp = client.get_metrics()
        self.assertIn("postoffice_messages_routed_total", metrics_resp["prometheus_text"])

        self.assertTrue(
            client.remove_route(
                source_broker="mqtt_a",
                source_topic="sensors/+",
                target_broker="amqp_b",
                target_topic="ingest/sensors",
            )["ok"]
        )
        self.assertTrue(client.remove_broker("mqtt_a")["ok"])
        self.assertTrue(client.clear_state()["ok"])


if __name__ == "__main__":
    unittest.main()
