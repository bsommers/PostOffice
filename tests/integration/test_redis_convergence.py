import os
import threading
import time
import unittest

import redis

from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane

TARGET = os.environ.get("POSTOFFICE_TEST_REDIS")
ENABLED = os.environ.get("POSTOFFICE_INTEGRATION") == "1" and TARGET

ROUTES = "postoffice:routes"
VERSION = "postoffice:version"


def wait_until(predicate, timeout=15.0, interval=0.1):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


@unittest.skipUnless(ENABLED, "set POSTOFFICE_INTEGRATION=1 and POSTOFFICE_TEST_REDIS=host:port (see README)")
class RedisCase(unittest.TestCase):
    def setUp(self):
        host, port = TARGET.rsplit(":", 1)
        self.host, self.port = host, int(port)
        self.admin = redis.Redis(host=self.host, port=self.port, decode_responses=True)
        self.admin.ping()
        self.admin.flushall()
        self._saved_limit = self.admin.config_get("client-output-buffer-limit")["client-output-buffer-limit"]
        self.cp = ControlPlane(redis_host=self.host, redis_port=self.port)
        self.planes = []
        self.addCleanup(self._restore)

    def _restore(self):
        for dp in self.planes:
            dp._stop_event.set()
            dp._buffering.clear()
            try:
                dp.pubsub.close()  # frees the server-side subscription for the next test
                dp.r.close()
            except Exception:
                pass
        # "normal 0 0 0 replica ... pubsub A B C": restore just the pubsub class
        parts = self._saved_limit.split()
        pubsub = parts[parts.index("pubsub"):parts.index("pubsub") + 4]
        self.admin.config_set("client-output-buffer-limit", " ".join(pubsub))
        self.admin.flushall()

    def data_plane(self):
        dp = DataPlane(redis_host=self.host, redis_port=self.port)
        self.planes.append(dp)
        return dp

    def pubsub_clients(self):
        return len(self.admin.client_list(_type="pubsub"))

    def seed_brokers(self):
        for name in ("b1", "b2"):
            self.cp.register_broker(name, "mqtt", host="127.0.0.1", port=1)

    def routes(self, dp, key=("b1", "in")):
        return dp.po.router.routes.get(key, [])


class TestSnapshotAndVersion(RedisCase):
    def test_snapshot_roundtrip_applies_state_and_tracks_the_real_version(self):
        self.seed_brokers()
        self.cp.add_route("b1", "in", "b2", "out")
        self.cp.add_subscription("b1", "in")
        dp = self.data_plane()
        dp._converge()
        self.assertEqual(sorted(dp.po.brokers), ["b1", "b2"])
        self.assertEqual(len(self.routes(dp)), 1)
        self.assertEqual(dp._version, int(self.admin.get(VERSION)))
        self.assertEqual(dp._version, 4)

    def test_repeated_converge_is_idempotent_against_real_redis(self):
        self.seed_brokers()
        self.cp.add_route("b1", "in", "b2", "out")
        dp = self.data_plane()
        dp._converge()
        client = dp.po.brokers["b1"]
        dp._converge()
        self.assertIs(dp.po.brokers["b1"], client)
        self.assertEqual(len(self.routes(dp)), 1)


class TestNoLostUpdates(RedisCase):
    def publish_after_routes_are_read(self, dp):
        """Publishes a route right after the snapshot read the routes hash: only the event carries it."""
        original = dp.r.hgetall
        done = []

        def hook(name):
            result = original(name)
            if name == ROUTES and not done:
                done.append(True)
                self.cp.add_route("b1", "late", "b2", "out")
            return result
        dp.r.hgetall = hook

    def test_naive_snapshot_without_subscription_loses_the_event(self):
        """Negative control: shows the race is real, so the next test proves something."""
        self.seed_brokers()
        dp = self.data_plane()
        self.publish_after_routes_are_read(dp)
        dp.sync_state()
        self.assertEqual(self.routes(dp, ("b1", "late")), [])

    def test_subscribe_buffer_snapshot_drain_keeps_the_event(self):
        self.seed_brokers()
        dp = self.data_plane()
        self.publish_after_routes_are_read(dp)
        dp._converge()
        self.assertEqual(len(self.routes(dp, ("b1", "late"))), 1)
        self.assertEqual(dp._version, int(self.admin.get(VERSION)))


class TestRecovery(RedisCase):
    def start_listener(self, dp):
        thread = threading.Thread(target=dp._listen_for_updates, daemon=True)
        thread.start()
        return thread

    def test_server_side_disconnect_triggers_reconnect_and_resync(self):
        self.seed_brokers()
        baseline = self.pubsub_clients()
        dp = self.data_plane()
        dp._converge()
        self.start_listener(dp)
        self.assertTrue(wait_until(lambda: self.pubsub_clients() == baseline + 1))
        self.admin.client_kill_filter(_type="pubsub")
        self.cp.add_route("b1", "in", "b2", "after-kill")  # may be published while no one listens
        self.assertTrue(wait_until(lambda: len(self.routes(dp)) == 1), "route never converged after reconnect")

    def test_live_events_flow_through_the_listener(self):
        self.seed_brokers()
        baseline = self.pubsub_clients()
        dp = self.data_plane()
        dp._converge()
        self.start_listener(dp)
        self.assertTrue(wait_until(lambda: self.pubsub_clients() == baseline + 1))
        self.cp.add_route("b1", "in", "b2", "live")
        self.assertTrue(wait_until(lambda: len(self.routes(dp)) == 1))
        self.assertEqual(dp._version, int(self.admin.get(VERSION)))

    def test_stalled_subscriber_cut_off_by_redis_recovers_via_resync(self):
        self.seed_brokers()
        dp = self.data_plane()
        dp._converge()
        before = int(self.admin.info("stats").get("client_output_buffer_limit_disconnections", 0))
        self.admin.config_set("client-output-buffer-limit", "pubsub 20000 20000 0")
        dp._subscribe_and_buffer()
        dp._stop_buffering()  # a consumer that has stopped reading: events pile up on the server
        for i in range(20):
            self.cp.add_route("b1", "in", "b2", f"out{i}", pad="x" * 5000)
        stats = self.admin.info("stats")
        if "client_output_buffer_limit_disconnections" in stats:
            self.assertGreater(int(stats["client_output_buffer_limit_disconnections"]), before,
                               "Redis did not cut off the stalled subscriber; the test is not exercising the limit")
        self.start_listener(dp)
        self.assertTrue(wait_until(lambda: len(self.routes(dp)) == 20), "state never converged after the cut-off")


if __name__ == "__main__":
    unittest.main()
