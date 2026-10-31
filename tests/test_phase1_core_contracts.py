import json
import pathlib
import unittest
from unittest.mock import MagicMock, patch

from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane
from postoffice.dlq import format_dlq_payload
from postoffice.ingress.rules import topic_matches
from postoffice.trie import TopicTrie

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
CONTRACTS_DIR = REPO_ROOT / "docs" / "contracts"


class TestPhase1CoreContracts(unittest.TestCase):
    def test_topic_trie_dollar_prefix_isolated_from_root_wildcards(self):
        trie = TopicTrie()
        trie.insert("#", "all_multi")
        trie.insert("+/broker/clients", "root_plus")
        trie.insert("$SYS/#", "sys_multi")
        trie.insert("$SYS/+/clients", "sys_plus")

        matched = trie.match("$SYS/broker/clients")
        self.assertEqual(set(matched), {"sys_multi", "sys_plus"})

        normal = trie.match("telemetry/broker/clients")
        self.assertEqual(set(normal), {"all_multi", "root_plus"})

    def test_ingress_topic_matches_dollar_prefix_rule(self):
        self.assertFalse(topic_matches("#", "$SYS/broker/load"))
        self.assertFalse(topic_matches("+/broker/load", "$SYS/broker/load"))
        self.assertTrue(topic_matches("$SYS/#", "$SYS/broker/load"))
        self.assertTrue(topic_matches("$SYS/+/load", "$SYS/broker/load"))

    def test_format_dlq_payload_accepts_deterministic_timestamp(self):
        fixed_ts = "2026-10-31T00:00:00+00:00"
        raw = format_dlq_payload(
            b'{"temp":21.5}',
            "mqtt_edge",
            "sensors/temp",
            "Target timeout",
            metadata={"attempt": 1},
            timestamp=fixed_ts,
        )
        decoded = json.loads(raw.decode("utf-8"))
        self.assertEqual(decoded["timestamp"], fixed_ts)
        self.assertFalse(decoded["is_base64"])

    @patch("postoffice.control_plane.redis.Redis")
    @patch("postoffice.data_plane.redis.Redis")
    def test_control_plane_and_data_plane_schema_version_contract(
        self, mock_dp_redis, mock_cp_redis
    ):
        cp_r = MagicMock()
        cp_r.incr.return_value = 1
        mock_cp_redis.return_value = cp_r

        cp = ControlPlane(include_schema_version=True)
        cp._publish_update("clear_state", {})
        published_raw = cp_r.publish.call_args[0][1]
        published_obj = json.loads(published_raw)
        self.assertEqual(published_obj["schema_version"], 1)

        dp = DataPlane()
        admitted = dp._admit(published_raw)
        self.assertIsNotNone(admitted)
        self.assertEqual(admitted["schema_version"], 1)

        # Unsupported integer schema_version rejected
        bad_v2 = json.dumps(
            {"action": "clear_state", "data": {}, "version": 2, "schema_version": 2}
        )
        self.assertIsNone(dp._admit(bad_v2))

        # Non-int / bool schema_version rejected
        bad_bool = json.dumps(
            {"action": "clear_state", "data": {}, "version": 3, "schema_version": True}
        )
        self.assertIsNone(dp._admit(bad_bool))

    def test_contracts_documentation_and_schemas_exist(self):
        required_docs = (
            "README.md",
            "control-plane-redis.md",
            "dlq-envelope.md",
            "metrics.md",
            "routing-and-protocols.md",
            "firewall-schema.json",
            "route-schema.json",
        )
        for filename in required_docs:
            path = CONTRACTS_DIR / filename
            self.assertTrue(path.is_file(), f"Missing contract file: {path}")
            if filename.endswith(".json"):
                schema = json.loads(path.read_text(encoding="utf-8"))
                self.assertIn("$schema", schema)
                self.assertEqual(schema.get("type"), "object")


if __name__ == "__main__":
    unittest.main()
