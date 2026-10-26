import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(__file__))
from fakes_redis import BROKERS, ROUTES, VERSION, FakeBus, broker_cfg, route_cfg  # noqa: E402

from postoffice.control_plane import ControlPlane  # noqa: E402
from postoffice.data_plane import DataPlane  # noqa: E402
from postoffice.signing import HmacSigner, sign_event, sign_state  # noqa: E402

KEY_A = "a" * 32
KEY_B = "b" * 32
VERSION_SIGS = "postoffice:version_sigs"


def signer(**keys):
    return HmacSigner(keys or {"k1": KEY_A})


class TestHmacSigner(unittest.TestCase):
    def test_roundtrip_and_tamper_detection(self):
        s = signer()
        kid, sig = s.sign(b"hello")
        self.assertTrue(s.verify(kid, sig, b"hello"))
        self.assertFalse(s.verify(kid, sig, b"hellp"))

    def test_unknown_kid_and_wrong_key_fail(self):
        kid, sig = signer().sign(b"m")
        self.assertFalse(signer(k1=KEY_B).verify(kid, sig, b"m"))
        self.assertFalse(signer(other=KEY_A).verify(kid, sig, b"m"))

    def test_garbage_signature_fails_cleanly(self):
        self.assertFalse(signer().verify("k1", "zz-not-hex", b"m"))
        self.assertFalse(signer().verify("k1", None, b"m"))

    def test_rotation_signs_with_first_key_verifies_with_all(self):
        old, new = HmacSigner({"old": KEY_A}), HmacSigner({"new": KEY_B})
        both = HmacSigner({"new": KEY_B, "old": KEY_A})
        self.assertEqual(both.sign(b"m")[0], "new")
        for s in (old, new):
            kid, sig = s.sign(b"m")
            self.assertTrue(both.verify(kid, sig, b"m"))

    def test_signature_uses_constant_time_compare(self):
        with patch("postoffice.signing.hmac.compare_digest", return_value=True) as cmp:
            signer().verify("k1", "00", b"m")
        cmp.assert_called()

    def test_weak_or_empty_keys_rejected(self):
        for keys in ({}, {"k": "short"}, {"": KEY_A}):
            with self.assertRaises(ValueError, msg=str(keys)):
                HmacSigner(keys)

    def test_from_env(self):
        self.assertIsNone(HmacSigner.from_env({}))
        s = HmacSigner.from_env({"POSTOFFICE_EVENT_KEYS": f"new:{KEY_B}, old:{KEY_A}"})
        self.assertEqual(s.sign(b"m")[0], "new")
        self.assertTrue(s.verify(*HmacSigner({"old": KEY_A}).sign(b"m"), b"m"))
        with self.assertRaises(ValueError):
            HmacSigner.from_env({"POSTOFFICE_EVENT_KEYS": "no-colon"})

    def test_events_and_state_are_domain_separated(self):
        s = signer()
        env = json.loads(sign_event(s, '{"x": 1}'))
        state_msg = json.loads(sign_state(s, "postoffice:brokers", "b1", '{"x": 1}'))
        self.assertNotEqual(env["sig"], state_msg["sig"])


def make_cp(s=None):
    bus = FakeBus()
    published = []
    bus.publish = lambda channel, payload: published.append(payload)
    bus.delete = lambda *keys: None
    with patch("postoffice.control_plane.redis.Redis", return_value=bus):
        cp = ControlPlane(signer=s)
    return cp, bus, published


class TestControlPlaneSigning(unittest.TestCase):
    def test_events_are_sealed_with_a_verifiable_signature(self):
        cp, bus, published = make_cp(signer())
        cp.add_route("b1", "in", "b2", "out")
        env = json.loads(published[0])
        self.assertEqual(set(env), {"kid", "sig", "payload"})
        self.assertTrue(signer().verify(env["kid"], env["sig"], b"event\x00" + env["payload"].encode()))
        body = json.loads(env["payload"])
        self.assertEqual((body["action"], body["version"]), ("add_route", 1))

    def test_stored_state_is_signed_and_bound_to_its_field(self):
        cp, bus, _ = make_cp(signer())
        cp.register_broker("b1", "mqtt", host="h")
        env = json.loads(bus.hashes[BROKERS]["b1"])
        msg = b"state\x00" + BROKERS.encode() + b"\x00b1\x00" + env["payload"].encode()
        self.assertTrue(signer().verify(env["kid"], env["sig"], msg))
        self.assertFalse(signer().verify(env["kid"], env["sig"],
                                         b"state\x00" + BROKERS.encode() + b"\x00b2\x00" + env["payload"].encode()))

    def test_each_version_gets_a_signature_and_old_ones_are_trimmed(self):
        cp, bus, _ = make_cp(signer())
        for i in range(1005):
            cp.add_subscription("b1", f"t{i}")
        self.assertIn("1005", bus.hashes[VERSION_SIGS])
        self.assertNotIn("1", bus.hashes[VERSION_SIGS])
        self.assertLessEqual(len(bus.hashes[VERSION_SIGS]), 1001)

    def test_update_broker_security_works_on_a_signed_entry(self):
        cp, bus, published = make_cp(signer())
        cp.register_broker("b1", "mqtt", host="h")
        cp.update_broker_security("b1", {"username": "u"})
        stored = json.loads(json.loads(bus.hashes[BROKERS]["b1"])["payload"])
        self.assertEqual(stored["kwargs"]["security_config"], {"username": "u"})

    def test_without_a_signer_the_legacy_wire_format_is_unchanged(self):
        cp, bus, published = make_cp(None)
        cp.add_route("b1", "in", "b2", "out")
        self.assertEqual(set(json.loads(published[0])), {"action", "data", "version"})
        self.assertNotIn("payload", json.loads(bus.hashes[ROUTES]["b1:in->b2:out"]))
        self.assertNotIn(VERSION_SIGS, bus.hashes)


class PlaneBase(unittest.TestCase):
    def build(self, cp_signer, dp_signer, mode=None):
        self.bus = FakeBus()
        self.published = []
        self.bus.delete = lambda *keys: None
        patches = [patch("postoffice.control_plane.redis.Redis", return_value=self.bus),
                   patch("postoffice.data_plane.redis.Redis", return_value=self.bus),
                   patch("postoffice.plugins.mqtt_client.mqtt.Client")]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.cp = ControlPlane(signer=cp_signer)
        self.dp = DataPlane(signer=dp_signer, signing_mode=mode)
        self.dp._stop_event.wait = lambda delay=None: False
        self.addCleanup(self.dp.po.stop)
        self.bus.publish = lambda channel, payload: self.published.append(payload)

    def deliver(self, raw):
        self.dp._process_update_message({"type": "message", "data": raw})

    def count(self, result):
        return self.dp.po.router.metrics.registry.get_sample_value(
            "postoffice_control_events_total", {"result": result})

    def routes(self):
        return self.dp.po.router.routes.get(("b1", "in"), [])


class TestEventVerification(PlaneBase):
    def setUp(self):
        self.build(signer(), signer())
        self.cp.register_broker("b1", "mqtt", host="h")
        self.cp.register_broker("b2", "mqtt", host="h")
        self.dp.sync_state()
        self.published.clear()
        self.cp.add_route("b1", "in", "b2", "out")
        self.good = self.published[-1]

    def test_valid_signed_event_applies(self):
        self.deliver(self.good)
        self.assertEqual(len(self.routes()), 1)
        self.assertEqual(self.count("applied"), 1.0)

    def test_tampered_event_rejected_and_counted(self):
        env = json.loads(self.good)
        env["payload"] = env["payload"].replace('"out"', '"evil"')
        self.deliver(json.dumps(env))
        self.assertEqual(self.routes(), [])
        self.assertEqual(self.count("bad_signature"), 1.0)

    def test_unsigned_event_rejected_in_enforce(self):
        self.deliver(json.dumps({"action": "add_route", "version": 3,
                                 "data": json.loads(route_cfg())}))
        self.assertEqual(self.routes(), [])
        self.assertEqual(self.count("unsigned"), 1.0)

    def test_event_signed_with_another_key_rejected(self):
        env = json.loads(sign_event(HmacSigner({"k1": KEY_B}), json.dumps(
            {"action": "add_route", "data": json.loads(route_cfg()), "version": 3})))
        self.deliver(json.dumps(env))
        self.assertEqual(self.routes(), [])
        self.assertEqual(self.count("bad_signature"), 1.0)

    def test_replayed_state_signature_is_not_a_valid_event(self):
        payload = json.dumps({"action": "add_route", "data": json.loads(route_cfg()), "version": 3})
        env = json.loads(sign_state(signer(), "postoffice:routes", "x", payload))
        self.deliver(json.dumps(env))
        self.assertEqual(self.routes(), [])

    def test_rejections_never_log_payload_contents(self):
        secret = "S3CRET-PASSWORD"
        env = json.loads(self.good)
        env["payload"] = env["payload"].replace('"out"', f'"{secret}"')
        with self.assertLogs("postoffice.data_plane", level="DEBUG") as logs:
            self.deliver(json.dumps(env))
            logging_noise = logs.output
        self.assertNotIn(secret, "\n".join(logging_noise))


class TestModes(PlaneBase):
    def legacy_event(self):
        return json.dumps({"action": "add_route", "version": 1, "data": json.loads(route_cfg())})

    def prepare(self, cp_signer, dp_signer, mode):
        self.build(cp_signer, dp_signer, mode)
        self.dp.po.add_broker("b1", "mqtt", host="h")
        self.dp.po.add_broker("b2", "mqtt", host="h")

    def test_observe_applies_unsigned_but_counts_it(self):
        self.prepare(None, signer(), "observe")
        self.deliver(self.legacy_event())
        self.assertEqual(len(self.routes()), 1)
        self.assertEqual(self.count("unsigned"), 1.0)

    def test_observe_applies_tampered_but_counts_it(self):
        self.prepare(signer(), signer(), "observe")
        self.cp.add_route("b1", "in", "b2", "out")
        env = json.loads(self.published[-1])
        env["sig"] = "0" * 64
        self.deliver(json.dumps(env))
        self.assertEqual(len(self.routes()), 1)
        self.assertEqual(self.count("bad_signature"), 1.0)

    def test_off_accepts_legacy_and_signed_events_for_rolling_upgrades(self):
        self.prepare(signer(), None, None)
        self.cp.add_route("b1", "in", "b2", "signed")
        self.deliver(self.published[-1])
        self.deliver(self.legacy_event())
        self.assertEqual(len(self.routes()), 2)

    def test_default_mode_is_enforce_with_a_key_and_off_without(self):
        self.prepare(None, signer(), None)
        self.assertEqual(self.dp.signing_mode, "enforce")
        self.prepare(None, None, None)
        self.assertEqual(self.dp.signing_mode, "off")

    def test_invalid_mode_rejected(self):
        with patch("postoffice.data_plane.redis.Redis"), self.assertRaises(ValueError):
            DataPlane(signer=signer(), signing_mode="sometimes")

    def test_unauthenticated_worker_warns_at_startup(self):
        with patch("postoffice.data_plane.redis.Redis"), \
                self.assertLogs("postoffice.data_plane", level="WARNING") as cm:
            DataPlane(signer=None)
        self.assertIn("not authenticated", cm.output[0])

    def test_rotation_both_keys_accepted(self):
        old, new = HmacSigner({"old": KEY_A}), HmacSigner({"new": KEY_B})
        self.prepare(old, HmacSigner({"new": KEY_B, "old": KEY_A}), "enforce")
        self.cp.add_route("b1", "in", "b2", "x")
        self.deliver(self.published[-1])
        self.cp.signer = new
        self.cp.add_route("b1", "in", "b2", "y")
        self.deliver(self.published[-1])
        self.assertEqual(len(self.routes()), 2)


class TestSnapshotVerification(PlaneBase):
    def setUp(self):
        self.build(signer(), signer())
        self.cp.register_broker("b1", "mqtt", host="h")
        self.cp.register_broker("b2", "mqtt", host="h")
        self.cp.add_route("b1", "in", "b2", "out")

    def test_signed_state_loads(self):
        self.dp.sync_state()
        self.assertEqual(sorted(self.dp.po.brokers), ["b1", "b2"])
        self.assertEqual(len(self.routes()), 1)
        self.assertEqual(self.dp._version, 3)

    def test_tampered_entry_is_skipped_and_counted(self):
        env = json.loads(self.bus.hashes[BROKERS]["b2"])
        env["payload"] = env["payload"].replace('"h"', '"evil-host"')
        self.bus.hashes[BROKERS]["b2"] = json.dumps(env)
        self.dp.sync_state(max_retries=1)
        self.assertNotIn("b2", self.dp.po.brokers)
        self.assertEqual(self.count("bad_signature"), 1.0)

    def test_entry_copied_to_another_field_is_rejected(self):
        self.bus.hashes[BROKERS]["b3"] = self.bus.hashes[BROKERS]["b2"]
        self.dp.sync_state()
        self.assertNotIn("b3", self.dp.po.brokers)

    def test_unsigned_entry_is_skipped_in_enforce(self):
        self.bus.hashes[BROKERS]["b3"] = broker_cfg()
        self.dp.sync_state()
        self.assertNotIn("b3", self.dp.po.brokers)
        self.assertEqual(self.count("unsigned"), 1.0)

    def test_forged_version_counter_is_not_trusted(self):
        self.bus.kv[VERSION] = 10 ** 6
        self.dp.sync_state()
        self.assertIsNone(self.dp._version)  # no gap detection, but events are not silently skipped
        self.assertEqual(self.count("bad_version"), 1.0)
        self.assertEqual(len(self.routes()), 1)

    def test_missing_version_signature_gets_a_short_grace_then_fails(self):
        del self.bus.hashes[VERSION_SIGS]["3"]
        self.dp.sync_state()
        self.assertIsNone(self.dp._version)

    def test_observe_mode_trusts_but_counts_a_forged_version(self):
        self.dp.signing_mode = "observe"
        self.bus.kv[VERSION] = 10 ** 6
        self.dp.sync_state()
        self.assertEqual(self.dp._version, 10 ** 6)
        self.assertEqual(self.count("bad_version"), 1.0)

    def test_empty_cluster_needs_no_version_signature(self):
        self.bus.hashes.clear()
        self.bus.kv.clear()
        self.dp.sync_state()
        self.assertEqual(self.dp._version, 0)

    def test_signed_events_and_state_from_a_different_key_are_all_rejected(self):
        dp2_bus = self.bus
        dp = DataPlane(signer=HmacSigner({"k1": KEY_B}))
        dp._stop_event.wait = lambda delay=None: False
        dp.sync_state(max_retries=1)
        self.assertEqual(dp.po.brokers, {})
        dp.po.stop()


if __name__ == "__main__":
    unittest.main()
