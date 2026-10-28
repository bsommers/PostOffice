"""D9d: ingress rules distributed over the Control Plane (hermetic)."""
import json
import os
import sys
import threading
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
from fakes_redis import BROKERS, FakeBus, event  # noqa: E402

from postoffice.control_plane import ControlPlane  # noqa: E402
from postoffice.data_plane import DataPlane  # noqa: E402
from postoffice.ingress import Firewall  # noqa: E402
from postoffice.signing import HmacSigner  # noqa: E402
from postoffice.validation import ACTIONS, PayloadError, validate_action  # noqa: E402

INGRESS = "postoffice:ingress"

RULES_A = {"rules": [{"type": "max_payload_bytes", "max_bytes": 100}]}
RULES_B = {"rules": [{"type": "max_payload_bytes", "max_bytes": 200}], "inflight": {"b1": {"max_inflight": 10}}}


class PlaneCase(unittest.TestCase):
    def build(self, signer=None, **dp_kwargs):
        self.bus = FakeBus()
        self.bus.delete = lambda *keys: None
        patches = [patch("postoffice.control_plane.redis.Redis", return_value=self.bus),
                   patch("postoffice.data_plane.redis.Redis", return_value=self.bus),
                   patch("postoffice.plugins.mqtt_client.mqtt.Client")]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.cp = ControlPlane(signer=signer)
        self.dp = DataPlane(signer=signer, **dp_kwargs)
        self.dp._version = 0
        self.dp._stop_event.wait = lambda delay=None: False
        self.addCleanup(self.dp.po.stop)

    def deliver(self, raw):
        self.dp._process_update_message({"type": "message", "data": raw})

    def count(self, result):
        return self.dp.po.router.metrics.registry.get_sample_value(
            "postoffice_control_events_total", {"result": result}) or 0.0

    def limit(self):
        return self.dp.po.firewall.rules[0].max_bytes if self.dp.po.firewall else None


class TestValidation(unittest.TestCase):
    def test_action_is_registered(self):
        self.assertIn("set_ingress", ACTIONS)

    def test_valid_configs_pass(self):
        validate_action("set_ingress", RULES_A)
        validate_action("set_ingress", RULES_B)
        validate_action("set_ingress", {})

    def test_rejects_bad_shapes(self):
        bad = [
            {"unexpected": 1},
            {"rules": "nope"},
            {"rules": [1]},
            {"rules": [{"type": "max_payload_bytes"}] * 65},
            {"on_rule_error": 5},
            {"ledger": []},
            {"inflight": []},
            {"inflight": {"bad name!": {"max_inflight": 5}}},
            {"inflight": {"b1": {"max_inflight": 0}}},
            {"inflight": {"b1": {"max_inflight": True}}},
            {"inflight": {"b1": {"max_inflight": 5, "low_water": 5}}},
            {"inflight": {"b1": {"max_inflight": 5, "lease_timeout": -1}}},
            {"inflight": {"b1": {"max_inflight": 5, "other": 1}}},
        ]
        for config in bad:
            with self.subTest(config=config), self.assertRaises(PayloadError):
                validate_action("set_ingress", config)


class TestControlPlane(PlaneCase):
    def test_set_ingress_stores_and_publishes(self):
        self.build()
        self.dp.pubsub = self.bus.pubsub()
        self.dp.pubsub.subscribe("c")
        self.cp.set_ingress(RULES_A)
        self.assertEqual(json.loads(self.bus.hashes[INGRESS]["rules"]), RULES_A)
        message = json.loads(self.dp.pubsub.queue.popleft())
        self.assertEqual((message["action"], message["data"]), ("set_ingress", RULES_A))

    def test_clear_state_also_wipes_ingress(self):
        self.build()
        deleted = []
        self.bus.delete = lambda *keys: deleted.extend(keys)
        self.cp.clear_state()
        self.assertIn(INGRESS, deleted)


class TestLiveEvents(PlaneCase):
    def test_event_installs_firewall(self):
        self.build()
        self.deliver(event("set_ingress", RULES_A, 1))
        self.assertIsInstance(self.dp.po.firewall, Firewall)
        self.assertEqual(self.limit(), 100)
        self.assertEqual(self.count("applied"), 1.0)

    def test_event_replaces_rules_and_applies_inflight(self):
        self.build()
        self.deliver(event("set_ingress", RULES_A, 1))
        self.deliver(event("set_ingress", RULES_B, 2))
        self.assertEqual(self.limit(), 200)
        self.assertEqual(self.dp.po._inflight_limits["b1"]["max_inflight"], 10)

    def test_dropped_inflight_entry_is_removed(self):
        self.build()
        self.deliver(event("set_ingress", RULES_B, 1))
        self.deliver(event("set_ingress", RULES_A, 2))
        self.assertNotIn("b1", self.dp.po._inflight_limits)

    def test_unbuildable_config_keeps_old_firewall_and_counts_invalid(self):
        self.build()
        self.deliver(event("set_ingress", RULES_A, 1))
        old = self.dp.po.firewall
        self.deliver(event("set_ingress", {"rules": [{"type": "no_such_rule"}]}, 2))
        self.assertIs(self.dp.po.firewall, old)
        self.assertEqual(self.limit(), 100)
        self.assertEqual(self.count("invalid"), 1.0)

    def test_bad_rule_arguments_keep_old_firewall(self):
        self.build()
        self.deliver(event("set_ingress", RULES_A, 1))
        old = self.dp.po.firewall
        self.deliver(event("set_ingress", {"rules": [{"type": "max_payload_bytes", "bogus": 1}]}, 2))
        self.assertIs(self.dp.po.firewall, old)
        self.assertEqual(self.count("invalid"), 1.0)

    def test_invalid_shape_never_reaches_apply(self):
        self.build()
        with patch.object(Firewall, "from_dict", side_effect=AssertionError("built")):
            self.deliver(event("set_ingress", {"inflight": {"b1": {"max_inflight": 0}}}, 1))
        self.assertIsNone(self.dp.po.firewall)
        self.assertEqual(self.count("invalid"), 1.0)

    def test_identical_config_is_not_reapplied(self):
        self.build()
        self.deliver(event("set_ingress", RULES_A, 1))
        first = self.dp.po.firewall
        self.deliver(event("set_ingress", RULES_A, 2))
        self.assertIs(self.dp.po.firewall, first)

    def test_forbidden_when_not_in_allow_list(self):
        self.build(allowed_actions={"add_broker"})
        self.deliver(event("set_ingress", RULES_A, 1))
        self.assertIsNone(self.dp.po.firewall)
        self.assertEqual(self.count("forbidden_action"), 1.0)

    def test_concurrent_readers_see_old_or_new_never_partial(self):
        self.build()
        self.deliver(event("set_ingress", RULES_A, 1))
        seen, stop = set(), threading.Event()

        def reader():
            while not stop.is_set():
                firewall = self.dp.po.firewall
                seen.add(firewall.rules[0].max_bytes if firewall.rules else None)

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            for i in range(2, 60):
                config = RULES_A if i % 2 else {"rules": [{"type": "max_payload_bytes", "max_bytes": 200}]}
                self.deliver(event("set_ingress", config, i))
        finally:
            stop.set()
            thread.join()
        self.assertLessEqual(seen, {100, 200})


class TestSnapshot(PlaneCase):
    def test_snapshot_restores_rules_after_restart(self):
        self.build()
        self.cp.set_ingress(RULES_B)
        fresh = DataPlane()
        self.addCleanup(fresh.po.stop)
        fresh.sync_state()
        self.assertEqual(fresh.po.firewall.rules[0].max_bytes, 200)
        self.assertEqual(fresh.po._inflight_limits["b1"]["max_inflight"], 10)

    def test_bad_stored_entry_is_skipped_and_counted(self):
        self.build()
        self.bus.hashes[INGRESS] = {"rules": json.dumps({"rules": [{"type": "no_such_rule"}]})}
        self.dp.sync_state()
        self.assertIsNone(self.dp.po.firewall)
        self.assertEqual(self.count("invalid"), 1.0)

    def test_resync_keeps_firewall_instance_when_unchanged(self):
        self.build()
        self.cp.set_ingress(RULES_A)
        self.dp.sync_state()
        first = self.dp.po.firewall
        self.dp.sync_state()
        self.assertIs(self.dp.po.firewall, first)

    def test_only_the_rules_field_is_read(self):
        self.build()
        self.bus.hashes[INGRESS] = {"other": json.dumps(RULES_A)}
        self.dp.sync_state()
        self.assertIsNone(self.dp.po.firewall)


class TestSigned(PlaneCase):
    def test_signed_round_trip(self):
        signer = HmacSigner({"k1": "s" * 32})
        self.build(signer=signer)
        self.cp.set_ingress(RULES_A)
        fresh = DataPlane(signer=signer)
        self.addCleanup(fresh.po.stop)
        fresh.sync_state()
        self.assertEqual(fresh.po.firewall.rules[0].max_bytes, 100)

    def test_unsigned_ingress_written_straight_into_redis_is_rejected(self):
        signer = HmacSigner({"k1": "s" * 32})
        self.build(signer=signer)
        self.bus.hashes[INGRESS] = {"rules": json.dumps(RULES_A)}
        self.dp.sync_state()
        self.assertIsNone(self.dp.po.firewall)
        self.assertEqual(self.count("unsigned"), 1.0)


if __name__ == "__main__":
    unittest.main()
