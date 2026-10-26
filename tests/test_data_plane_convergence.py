import json
import unittest
from collections import deque
from unittest.mock import MagicMock, patch

import redis

from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane
from postoffice.router import Router

BROKERS, ROUTES, SUBS, VERSION = (
    "postoffice:brokers", "postoffice:routes", "postoffice:subscriptions", "postoffice:version")


class FakePubSub:
    def __init__(self, bus):
        self.bus = bus
        self.queue = deque()
        self.fail_listen = None

    def subscribe(self, channel):
        self.bus.calls.append("subscribe")
        self.bus.pubsubs.append(self)

    def listen(self):
        if self.fail_listen:
            raise self.fail_listen
        while self.queue:
            yield {"type": "message", "data": self.queue.popleft()}
        self.bus.on_listen_exhausted()


class FakeBus:
    """In-memory Redis stand-in whose publish() reaches every subscribed pubsub."""

    def __init__(self):
        self.hashes, self.kv, self.pubsubs, self.calls = {}, {}, [], []
        self.on_hgetall = None
        self.on_listen_exhausted = lambda: None
        self.fail_hgetall_once = {}

    def hset(self, name, key, value):
        self.hashes.setdefault(name, {})[key] = value

    def hgetall(self, name):
        self.calls.append(f"hgetall:{name}")
        if self.fail_hgetall_once.pop(name, False):
            raise redis.ConnectionError("blip")
        result = dict(self.hashes.get(name, {}))
        if self.on_hgetall:
            self.on_hgetall(name)
        return result

    def get(self, key):
        self.calls.append(f"get:{key}")
        return self.kv.get(key)

    def incr(self, key):
        self.kv[key] = int(self.kv.get(key, 0)) + 1
        return self.kv[key]

    def publish(self, channel, payload):
        for ps in self.pubsubs:
            ps.queue.append(payload)

    def pubsub(self):
        return FakePubSub(self)


def event(action, data, version=None):
    body = {"action": action, "data": data}
    if version is not None:
        body["version"] = version
    return json.dumps(body)


def broker_cfg(port=1883):
    return json.dumps({"protocol": "mqtt", "kwargs": {"host": "h", "port": port}})


def route_cfg(src="b1", st="in", dst="b2", dt="out"):
    return json.dumps({"source_broker": src, "source_topic": st, "target_broker": dst,
                       "target_topic": dt, "target_kwargs": {}})


class Base(unittest.TestCase):
    def setUp(self):
        self.bus = FakeBus()
        patches = [patch("postoffice.data_plane.redis.Redis", return_value=self.bus),
                   patch("postoffice.plugins.mqtt_client.mqtt.Client")]
        for p in patches:
            p.start()
            self.addCleanup(p.stop)
        self.dp = DataPlane()
        self.dp._stop_event.wait = lambda delay=None: False  # no real sleeping
        self.addCleanup(self.dp.po.stop)
        self.bus.on_listen_exhausted = self.dp._stop_event.set

    def seed(self, brokers=("b1", "b2"), routes=True):
        for name in brokers:
            self.bus.hset(BROKERS, name, broker_cfg())
        if routes:
            self.bus.hset(ROUTES, "r1", route_cfg())

    def route_count(self, key=("b1", "in")):
        return len(self.dp.po.router.routes.get(key, []))


class TestSubscribeBeforeSnapshot(Base):
    def test_run_subscribes_before_reading_state(self):
        self.seed()
        with patch.object(self.dp, "_listen_for_updates"):
            self.dp.run()
        self.assertLess(self.bus.calls.index("subscribe"), self.bus.calls.index(f"hgetall:{BROKERS}"))

    def test_event_published_during_snapshot_is_applied(self):
        self.seed(routes=False)
        self.dp._subscribe()

        def publish_mid_snapshot(name):
            if name == BROKERS:  # after brokers were read, before routes
                self.bus.hset(ROUTES, "late", route_cfg())
                self.bus.publish("c", event("add_route", json.loads(route_cfg()), version=1))
        self.bus.on_hgetall = publish_mid_snapshot
        self.dp.sync_state()
        self.dp._listen_for_updates()
        self.assertEqual(self.route_count(), 1)  # applied once: snapshot and event agree


class TestIdempotentSync(Base):
    def test_sync_retry_is_idempotent_after_partial_failure(self):
        self.seed()
        self.bus.fail_hgetall_once[ROUTES] = True
        self.dp.sync_state(retry_delay=0)
        self.assertEqual(sorted(self.dp.po.brokers), ["b1", "b2"])
        self.assertEqual(self.route_count(), 1)

    def test_repeated_full_sync_changes_nothing(self):
        self.seed()
        self.bus.hset(SUBS, "s", json.dumps({"broker_name": "b1", "topic": "t", "kwargs": {}}))
        self.dp.sync_state()
        client = self.dp.po.brokers["b1"]
        client.subscribe = MagicMock()
        self.dp.sync_state()
        self.assertIs(self.dp.po.brokers["b1"], client)
        client.subscribe.assert_not_called()
        self.assertEqual(self.route_count(), 1)

    def test_changed_broker_config_replaces_the_client(self):
        self.seed()
        self.dp.sync_state()
        old = self.dp.po.brokers["b1"]
        old.disconnect = MagicMock()
        self.bus.hset(BROKERS, "b1", broker_cfg(port=8883))
        self.dp.sync_state()
        new = self.dp.po.brokers["b1"]
        self.assertIsNot(new, old)
        self.assertEqual(new.port, 8883)
        old.disconnect.assert_called_once()
        self.assertIs(self.dp.po.router.clients["b1"], new)

    def test_replayed_add_route_does_not_duplicate(self):
        self.seed(routes=False)
        self.dp.sync_state()
        msg = {"type": "message", "data": event("add_route", json.loads(route_cfg()))}
        self.dp._process_update_message(msg)
        self.dp._process_update_message(msg)
        self.assertEqual(self.route_count(), 1)
        self.assertEqual(len(self.dp.po.router.client_tries["b1"].match("in")), 1)

    def test_router_add_route_is_idempotent_for_identical_routes(self):
        router = Router()
        router.add_route("s", "t", "d", "x", qos=1)
        router.add_route("s", "t", "d", "x", qos=1)
        router.add_route("s", "t", "d", "x", qos=2)  # different options: a distinct route
        self.assertEqual(len(router.routes[("s", "t")]), 2)


class TestResync(Base):
    def test_listener_reconnect_triggers_resync(self):
        self.seed(routes=False)
        self.dp.sync_state()
        first = FakePubSub(self.bus)
        first.fail_listen = redis.ConnectionError("dropped")
        self.dp.pubsub = first
        self.dp._pubsub_ready = True
        # State changed while the listener was disconnected; no event was ever delivered.
        self.bus.hset(ROUTES, "missed", route_cfg())
        self.dp._listen_for_updates()
        self.assertEqual(self.route_count(), 1)

    def test_first_listener_connection_after_run_does_not_resync(self):
        self.seed()
        self.dp._subscribe()
        self.dp.sync_state()
        before = self.bus.calls.count(f"hgetall:{BROKERS}")
        self.dp._listen_for_updates()
        self.assertEqual(self.bus.calls.count(f"hgetall:{BROKERS}"), before)

    def test_version_gap_triggers_resync(self):
        self.seed(routes=False)
        self.bus.kv[VERSION] = 3
        self.dp._subscribe()
        self.dp.sync_state()
        self.bus.hset(ROUTES, "v4", route_cfg())  # the v4 event was lost
        self.bus.publish("c", event("add_subscription", {"broker_name": "b1", "topic": "t", "kwargs": {}},
                                    version=5))
        self.dp._listen_for_updates()
        self.assertEqual(self.route_count(), 1)

    def test_stale_or_duplicate_version_is_ignored(self):
        self.seed(routes=False)
        self.bus.kv[VERSION] = 7
        self.dp.sync_state()
        self.dp._process_update_message(
            {"type": "message", "data": event("add_route", json.loads(route_cfg()), version=7)})
        self.assertEqual(self.route_count(), 0)

    def test_next_version_is_applied_and_advances(self):
        self.seed(routes=False)
        self.bus.kv[VERSION] = 7
        self.dp.sync_state()
        self.dp._process_update_message(
            {"type": "message", "data": event("add_route", json.loads(route_cfg()), version=8)})
        self.assertEqual(self.route_count(), 1)
        self.assertFalse(self.dp._needs_resync)

    def test_unversioned_events_from_older_control_planes_still_apply(self):
        self.seed(routes=False)
        self.dp.sync_state()
        self.dp._process_update_message(
            {"type": "message", "data": event("add_route", json.loads(route_cfg()))})
        self.assertEqual(self.route_count(), 1)

    def test_update_security_for_unknown_broker_reports_error(self):
        with self.assertLogs("postoffice.data_plane", level="ERROR") as cm:
            self.dp._process_update_message({"type": "message", "data": event(
                "update_broker_security", {"name": "ghost", "security_config": {"username": "u"}})})
        self.assertIn("ghost", cm.output[0])
        self.assertTrue(self.dp._needs_resync)
        value = self.dp.po.router.metrics.registry.get_sample_value(
            "postoffice_routing_errors_total",
            {"source_broker": "data_plane", "error_type": "security_update_unknown_broker"})
        self.assertEqual(value, 1.0)

    def test_security_update_does_not_cause_a_needless_client_replacement(self):
        self.seed()
        self.dp.sync_state()
        client = self.dp.po.brokers["b1"]
        client.reload_security = MagicMock()
        sec = {"username": "u", "password": "p"}
        self.dp._process_update_message({"type": "message", "data": event(
            "update_broker_security", {"name": "b1", "security_config": sec})})
        stored = json.loads(broker_cfg())
        stored["kwargs"]["security_config"] = sec  # what ControlPlane stores after the update
        self.bus.hset(BROKERS, "b1", json.dumps(stored))
        self.dp.sync_state()
        self.assertIs(self.dp.po.brokers["b1"], client)


class TestControlPlaneVersion(unittest.TestCase):
    def test_every_write_bumps_the_version_and_stamps_the_event(self):
        bus = FakeBus()
        published = []
        bus.publish = lambda channel, payload: published.append(json.loads(payload))
        bus.delete = lambda *keys: None
        with patch("postoffice.control_plane.redis.Redis", return_value=bus):
            cp = ControlPlane()
        cp.register_broker("b1", "mqtt", host="h")
        cp.add_route("b1", "in", "b2", "out")
        cp.add_subscription("b1", "in")
        cp.clear_state()
        self.assertEqual([p["version"] for p in published], [1, 2, 3, 4])
        self.assertEqual(bus.kv[VERSION], 4)


if __name__ == "__main__":
    unittest.main()
