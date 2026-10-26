"""D9b: control-plane event limits, action allow-list and payload validation (hermetic)."""
import json
import logging
import os
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(__file__))
from fakes_redis import BROKERS, FakeBus, event  # noqa: E402

from postoffice.control_plane import ControlPlane  # noqa: E402
from postoffice.data_plane import DataPlane  # noqa: E402
from postoffice.ratelimit import TokenBucket  # noqa: E402
from postoffice.validation import PayloadError, validate_action  # noqa: E402

SECRET = "hunter2-do-not-log"


class FakeClock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now


class PlaneCase(unittest.TestCase):
    def build(self, **dp_kwargs):
        self.bus = FakeBus()
        self.bus.delete = lambda *keys: None
        self.clock = FakeClock()
        patches = [patch("postoffice.control_plane.redis.Redis", return_value=self.bus),
                   patch("postoffice.data_plane.redis.Redis", return_value=self.bus),
                   patch("postoffice.plugins.mqtt_client.mqtt.Client")]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.cp = ControlPlane()
        self.dp = DataPlane(**dp_kwargs)
        self.dp._clock = self.clock
        self.dp._version = 0
        self.dp._stop_event.wait = lambda delay=None: False
        self.addCleanup(self.dp.po.stop)

    def deliver(self, raw):
        self.dp._process_update_message({"type": "message", "data": raw})

    def count(self, result):
        return self.dp.po.router.metrics.registry.get_sample_value(
            "postoffice_control_events_total", {"result": result}) or 0.0

    def add_broker(self, name="b1", version=None, **kwargs):
        data = {"name": name, "protocol": "mqtt", "kwargs": {"host": "h", **kwargs}}
        self.deliver(event("add_broker", data, version))


class TestEventSize(PlaneCase):
    def test_oversize_event_is_dropped_before_parsing(self):
        self.build(max_event_bytes=1024)
        raw = event("add_broker", {"name": "b1", "protocol": "mqtt", "kwargs": {"host": "h" * 5000}})
        with patch("postoffice.data_plane.json.loads", side_effect=AssertionError("parsed")):
            self.deliver(raw)
        self.assertEqual(self.count("oversize"), 1.0)
        self.assertEqual(self.dp.po.brokers, {})

    def test_multibyte_payload_is_measured_in_bytes(self):
        self.build(max_event_bytes=1024)
        self.deliver(event("add_broker", {"name": "b1", "protocol": "mqtt", "kwargs": {"host": "é" * 600}}))
        self.assertEqual(self.count("oversize"), 1.0)

    def test_event_under_the_cap_applies(self):
        self.build(max_event_bytes=1024)
        self.add_broker()
        self.assertIn("b1", self.dp.po.brokers)
        self.assertEqual(self.count("oversize"), 0.0)

    def test_deeply_nested_event_is_rejected_without_recursion_error(self):
        self.build()
        deep = '{"action": "add_route", "data": ' + '{"a": ' * 5000 + "1" + "}" * 5000 + "}"
        self.deliver(deep)
        self.assertEqual(self.count("invalid"), 1.0)

    def test_oversize_state_entry_is_skipped_on_sync(self):
        self.build(max_event_bytes=1024)
        self.bus.hashes[BROKERS] = {
            "big": json.dumps({"protocol": "mqtt", "kwargs": {"host": "h" * 5000}}),
            "ok": json.dumps({"protocol": "mqtt", "kwargs": {"host": "h"}}),
        }
        self.dp.sync_state()
        self.assertEqual(list(self.dp.po.brokers), ["ok"])
        self.assertEqual(self.count("oversize"), 1.0)

    def test_limits_are_configurable_from_the_environment(self):
        with patch.dict(os.environ, {"POSTOFFICE_MAX_EVENT_BYTES": "2048", "POSTOFFICE_EVENT_RATE": "7",
                                     "POSTOFFICE_EVENT_BURST": "9"}):
            self.build()
        self.assertEqual(self.dp.max_event_bytes, 2048)
        self.assertEqual((self.dp._event_bucket.rate, self.dp._event_bucket.burst), (7, 9))

    def test_defaults(self):
        self.build()
        self.assertEqual(self.dp.max_event_bytes, 256 * 1024)
        self.assertEqual((self.dp._event_bucket.rate, self.dp._event_bucket.burst), (20, 100))


class TestRateLimit(PlaneCase):
    def setUp(self):
        self.build()
        self.dp._event_bucket = TokenBucket(1, 2, clock=self.clock)
        self.dp._version = 0

    def test_excess_events_are_shed_and_flag_a_resync(self):
        for i in range(1, 5):
            self.add_broker(f"b{i}", version=i)
        self.assertEqual(sorted(self.dp.po.brokers), ["b1", "b2"])
        self.assertEqual(self.count("rate_limited"), 2.0)
        self.assertTrue(self.dp._needs_resync)

    def test_tokens_refill_over_time(self):
        for i in range(1, 4):
            self.add_broker(f"b{i}", version=i)
        self.clock.now += 5
        self.add_broker("b9")
        self.assertIn("b9", self.dp.po.brokers)

    def test_a_shed_event_does_not_apply_even_when_it_is_the_next_version(self):
        self.add_broker("b1", version=1)
        self.add_broker("b2", version=2)
        self.add_broker("b3", version=3)
        self.assertNotIn("b3", self.dp.po.brokers)


class TestResyncBackoff(PlaneCase):
    def setUp(self):
        self.build()

    def test_first_resync_is_immediate_then_spacing_grows(self):
        self.assertTrue(self.dp._resync_due())
        self.dp.resync()
        self.assertFalse(self.dp._resync_due())
        waits = []
        for _ in range(4):
            waits.append(self.dp._next_resync_at - self.clock.now)
            self.clock.now = self.dp._next_resync_at
            self.assertTrue(self.dp._resync_due())
            self.dp.resync()
        self.assertEqual(waits, sorted(waits))
        self.assertGreater(waits[-1], waits[0])

    def test_spacing_is_capped(self):
        for _ in range(40):
            self.clock.now = self.dp._next_resync_at
            self.dp.resync()
        self.assertLessEqual(self.dp._next_resync_at - self.clock.now, 30.0)

    def test_a_quiet_period_resets_the_spacing(self):
        for _ in range(5):
            self.clock.now = self.dp._next_resync_at
            self.dp.resync()
        self.clock.now += 600
        self.dp.resync()
        self.assertLessEqual(self.dp._next_resync_at - self.clock.now, 1.0)

    def test_listener_defers_a_resync_that_is_not_due(self):
        self.dp.resync()
        self.dp._needs_resync = True
        with patch.object(self.dp, "sync_state") as sync:
            self.dp._maybe_resync()
            sync.assert_not_called()
            self.assertTrue(self.dp._needs_resync)
            self.clock.now = self.dp._next_resync_at
            self.dp._maybe_resync()
            sync.assert_called_once()
        self.assertFalse(self.dp._needs_resync)


class TestActionAllowList(PlaneCase):
    def test_clear_state_is_ignored_by_default(self):
        self.build()
        self.add_broker(version=1)
        self.deliver(event("clear_state", {}, 2))
        self.assertIn("b1", self.dp.po.brokers)
        self.assertEqual(self.count("forbidden_action"), 1.0)

    def test_clear_state_works_when_explicitly_enabled(self):
        self.build(allow_clear_state=True)
        self.add_broker(version=1)
        self.deliver(event("clear_state", {}, 2))
        self.assertEqual(self.dp.po.brokers, {})

    def test_clear_state_can_be_enabled_from_the_environment(self):
        with patch.dict(os.environ, {"ALLOW_CLEAR_STATE": "1"}):
            self.build()
        self.assertTrue(self.dp.allow_clear_state)

    def test_allow_list_restricts_actions(self):
        self.build(allowed_actions={"add_route"})
        self.add_broker()
        self.assertEqual(self.dp.po.brokers, {})
        self.assertEqual(self.count("forbidden_action"), 1.0)

    def test_allow_list_from_the_environment(self):
        with patch.dict(os.environ, {"POSTOFFICE_ALLOWED_ACTIONS": "add_route, add_subscription"}):
            self.build()
        self.assertEqual(self.dp.allowed_actions, {"add_route", "add_subscription"})

    def test_unknown_action_in_the_environment_is_a_startup_error(self):
        with patch.dict(os.environ, {"POSTOFFICE_ALLOWED_ACTIONS": "add_route,drop_tables"}):
            with self.assertRaises(ValueError):
                self.build()

    def test_unknown_action_is_forbidden(self):
        self.build()
        self.deliver(event("drop_tables", {}, 1))
        self.assertEqual(self.count("forbidden_action"), 1.0)

    def test_a_forbidden_event_still_advances_the_version(self):
        self.build()
        self.dp._version = 0
        self.deliver(event("clear_state", {}, 1))
        self.add_broker(version=2)
        self.assertEqual(self.dp._version, 2)
        self.assertFalse(self.dp._needs_resync)
        self.assertIn("b1", self.dp.po.brokers)

    def test_a_forbidden_clear_state_does_not_count_as_applied(self):
        self.build()
        self.deliver(event("clear_state", {}, 1))
        self.assertEqual(self.count("applied"), 0.0)


class TestPayloadValidation(PlaneCase):
    def setUp(self):
        self.build()
        self.add_broker("b1", version=1)
        self.add_broker("b2", version=2)
        self.version = 2

    def send(self, action, data):
        self.version += 1
        self.deliver(event(action, data, self.version))

    def assert_rejected(self):
        self.assertEqual(self.count("invalid"), 1.0)
        self.assertEqual(self.dp._version, self.version)  # sequence intact: no spurious resync
        self.assertFalse(self.dp._needs_resync)

    def test_bad_broker_names_are_rejected(self):
        for bad in ("", "has space", "a/b", "x" * 200, "..\\evil", "nul\x00", 5, None):
            with self.subTest(name=bad):
                self.build()
                self.version = 0
                self.send("add_broker", {"name": bad, "protocol": "mqtt", "kwargs": {"host": "h"}})
                self.assert_rejected()
                self.assertEqual(self.dp.po.brokers, {})

    def test_unknown_protocol_is_rejected(self):
        self.send("add_broker", {"name": "b3", "protocol": "ftp", "kwargs": {}})
        self.assert_rejected()
        self.assertNotIn("b3", self.dp.po.brokers)

    def test_non_string_protocol_is_rejected(self):
        self.send("add_broker", {"name": "b3", "protocol": ["mqtt"], "kwargs": {}})
        self.assert_rejected()

    def test_unexpected_broker_kwarg_is_rejected(self):
        self.send("add_broker", {"name": "b3", "protocol": "mqtt", "kwargs": {"host": "h", "__class__": "x"}})
        self.assert_rejected()
        self.assertNotIn("b3", self.dp.po.brokers)

    def test_wrong_kwarg_type_is_rejected(self):
        self.send("add_broker", {"name": "b3", "protocol": "mqtt", "kwargs": {"host": "h", "port": "1883"}})
        self.assert_rejected()

    def test_bool_is_not_an_int_port(self):
        self.send("add_broker", {"name": "b3", "protocol": "mqtt", "kwargs": {"host": "h", "port": True}})
        self.assert_rejected()

    def test_kwargs_must_be_an_object(self):
        self.send("add_broker", {"name": "b3", "protocol": "mqtt", "kwargs": ["host"]})
        self.assert_rejected()

    def test_security_config_must_be_an_object(self):
        self.send("add_broker", {"name": "b3", "protocol": "mqtt",
                                 "kwargs": {"host": "h", "security_config": "tls=on"}})
        self.assert_rejected()

    def test_base_client_and_add_broker_options_are_accepted(self):
        self.send("add_broker", {"name": "b3", "protocol": "mqtt", "kwargs": {
            "host": "h", "port": 1883, "enable_publish_buffer": True, "max_publish_buffer_size": 10,
            "circuit_breaker_threshold": 3, "circuit_breaker_timeout": 5.0,
            "security_config": {"username": "u"}}})
        self.assertEqual(self.count("invalid"), 0.0)
        self.assertIn("b3", self.dp.po.brokers)

    def test_every_registered_protocol_accepts_its_documented_kwargs(self):
        cases = {"mqtt": {"host": "h", "port": 1883}, "nanomq": {"host": "h"},
                 "amqp": {"host": "h", "user": "u", "password": "p"},
                 "kafka": {"bootstrap_servers": "k:9092"}}
        for protocol, kwargs in cases.items():
            with self.subTest(protocol=protocol):
                validate_action("add_broker", {"name": "x", "protocol": protocol, "kwargs": kwargs})

    def test_bad_route_is_rejected(self):
        bad = [
            {"source_broker": "b1", "source_topic": "in", "target_broker": "b2"},                       # missing key
            {"source_broker": "b1", "source_topic": "", "target_broker": "b2", "target_topic": "o",
             "target_kwargs": {}},                                                                       # empty topic
            {"source_broker": "b1", "source_topic": "a\x00b", "target_broker": "b2", "target_topic": "o",
             "target_kwargs": {}},                                                                       # NUL
            {"source_broker": "b1", "source_topic": "/".join(["x"] * 40), "target_broker": "b2",
             "target_topic": "o", "target_kwargs": {}},                                                  # too deep
            {"source_broker": "b1", "source_topic": "t" * 300, "target_broker": "b2", "target_topic": "o",
             "target_kwargs": {}},                                                                       # too long
            {"source_broker": "b1", "source_topic": "in", "target_broker": "b2", "target_topic": "o",
             "target_kwargs": "x"},                                                                      # not an object
            {"source_broker": 1, "source_topic": "in", "target_broker": "b2", "target_topic": "o",
             "target_kwargs": {}},                                                                       # not a string
        ]
        for data in bad:
            with self.subTest(data=data):
                self.build()
                self.version = 0
                self.send("add_route", data)
                self.assert_rejected()
                self.assertEqual(self.dp.po.router.routes, {})

    def test_valid_route_with_wildcards_is_accepted(self):
        self.send("add_route", {"source_broker": "b1", "source_topic": "sensors/+/temp",
                                "target_broker": "b2", "target_topic": "out/#", "target_kwargs": {"qos": 1}})
        self.assertEqual(self.count("invalid"), 0.0)
        self.assertEqual(len(self.dp.po.router.routes[("b1", "sensors/+/temp")]), 1)

    def test_bad_subscription_is_rejected(self):
        for data in ({"broker_name": "b1", "topic": "", "kwargs": {}},
                     {"broker_name": "b1", "topic": "t", "kwargs": []},
                     {"topic": "t", "kwargs": {}}):
            with self.subTest(data=data):
                self.build()
                self.add_broker("b1", version=1)
                self.version = 1
                self.send("add_subscription", data)
                self.assert_rejected()

    def test_bad_security_update_is_rejected(self):
        for data in ({"name": "b1", "security_config": "x"}, {"name": "b1"}, {"security_config": {}}):
            with self.subTest(data=data), patch.object(self.dp.po, "reload_broker_security") as reload:
                self.build()
                self.add_broker("b1", version=1)
                self.version = 1
                self.send("update_broker_security", data)
                self.assert_rejected()

    def test_non_object_data_is_rejected(self):
        self.send("add_route", ["not", "an", "object"])
        self.assert_rejected()

    def test_validation_error_never_echoes_values(self):
        with self.assertRaises(PayloadError) as ctx:
            validate_action("add_broker", {"name": "b", "protocol": "mqtt",
                                           "kwargs": {"host": "h", "password": SECRET, "evil_key": SECRET}})
        self.assertNotIn(SECRET, str(ctx.exception))

    def test_rejected_events_do_not_leak_values_into_logs(self):
        with self.assertLogs("postoffice", level=logging.DEBUG) as logs:
            self.send("add_broker", {"name": "b3", "protocol": "mqtt",
                                     "kwargs": {"host": "h", "evil": SECRET, "password": 5}})
        self.assertNotIn(SECRET, "\n".join(logs.output))

    def test_state_snapshot_is_validated_too(self):
        self.build()
        self.bus.hashes[BROKERS] = {
            "evil": json.dumps({"protocol": "mqtt", "kwargs": {"host": "h", "bogus": 1}}),
            "good": json.dumps({"protocol": "mqtt", "kwargs": {"host": "h"}}),
        }
        self.dp.sync_state()
        self.assertEqual(list(self.dp.po.brokers), ["good"])
        self.assertGreaterEqual(self.count("invalid"), 1.0)


class TestValidateActionUnit(unittest.TestCase):
    def test_clear_state_accepts_empty_data_only(self):
        validate_action("clear_state", {})
        with self.assertRaises(PayloadError):
            validate_action("clear_state", "x")

    def test_unknown_action(self):
        with self.assertRaises(PayloadError):
            validate_action("nope", {})


if __name__ == "__main__":
    unittest.main()
