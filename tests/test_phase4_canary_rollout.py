import json
import unittest
from unittest.mock import MagicMock

from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane
from postoffice.native import NativeEngine
from postoffice.rollout import (
    CanaryPolicy,
    CanaryRolloutController,
    RolloutMetricsSample,
)
from postoffice.router import Router, canary_bucket
from postoffice.signing import HmacSigner
from postoffice.validation import PayloadError, validate_action


class _InMemoryRedis:
    """Hermetic in-memory Redis mock supporting hashes, strings, incr, and pubsub."""

    def __init__(self):
        self.kv = {}
        self.hashes = {}
        self.published = []

    def incr(self, key: str) -> int:
        val = int(self.kv.get(key, 0)) + 1
        self.kv[key] = str(val)
        return val

    def get(self, key: str):
        return self.kv.get(key)

    def hset(self, name: str, key: str, value: str):
        self.hashes.setdefault(name, {})[key] = value

    def hget(self, name: str, key: str):
        return self.hashes.get(name, {}).get(key)

    def hdel(self, name: str, key: str):
        self.hashes.get(name, {}).pop(key, None)

    def hgetall(self, name: str):
        return dict(self.hashes.get(name, {}))

    def delete(self, *names: str):
        for name in names:
            self.hashes.pop(name, None)
            self.kv.pop(name, None)

    def publish(self, channel: str, payload: str):
        self.published.append((channel, payload))


class TestPhase4CanaryRollout(unittest.TestCase):
    def test_validation_accepts_engine_tags_and_set_rollout(self):
        for eng in ("all", "python", "native", "canary"):
            validate_action(
                "add_route",
                {
                    "source_broker": "mqtt_in",
                    "source_topic": "sensors/#",
                    "target_broker": "kafka_out",
                    "target_topic": "telemetry",
                    "target_kwargs": {"engine": eng},
                },
            )
        with self.assertRaises(PayloadError):
            validate_action(
                "add_route",
                {
                    "source_broker": "mqtt_in",
                    "source_topic": "sensors/#",
                    "target_broker": "kafka_out",
                    "target_topic": "telemetry",
                    "target_kwargs": {"engine": "invalid_engine"},
                },
            )

        validate_action(
            "set_rollout",
            {
                "native_percentage": 25,
                "environment": "staging",
                "status": "active",
                "reason": "ramp_stage_2",
            },
        )
        with self.assertRaises(PayloadError):
            validate_action("set_rollout", {"native_percentage": 150})
        with self.assertRaises(PayloadError):
            validate_action("set_rollout", {"native_percentage": -5})
        with self.assertRaises(PayloadError):
            validate_action("set_rollout", {"native_percentage": 25, "status": "bogus"})

    def test_static_engine_tag_isolation_python_vs_native(self):
        py_router = Router()
        py_router.set_worker_engine("python")
        py_target = MagicMock()
        py_target.name = "kafka_out"
        py_target.publish.side_effect = lambda *a, **kw: kw.get("on_confirm", lambda: None)()
        py_router.register_client(py_target)

        # Route 1: engine=native (Python must skip, Native must route)
        # Route 2: engine=python (Python must route, Native must skip)
        py_router.add_route(
            "mqtt_in", "native/only", "kafka_out", "out.native", engine="native"
        )
        py_router.add_route(
            "mqtt_in", "python/only", "kafka_out", "out.python", engine="python"
        )

        nat = NativeEngine()
        nat.apply_action(
            "add_broker",
            {"name": "kafka_out", "protocol": "kafka", "kwargs": {"bootstrap_servers": "localhost:9092"}},
        )
        nat.apply_action(
            "add_route",
            {
                "source_broker": "mqtt_in",
                "source_topic": "native/only",
                "target_broker": "kafka_out",
                "target_topic": "out.native",
                "target_kwargs": {"engine": "native"},
            },
        )
        nat.apply_action(
            "add_route",
            {
                "source_broker": "mqtt_in",
                "source_topic": "python/only",
                "target_broker": "kafka_out",
                "target_topic": "out.python",
                "target_kwargs": {"engine": "python"},
            },
        )

        # Send to native/only
        acked = []
        py_router.route("mqtt_in", "native/only", b'{"v":1}', ack_fn=lambda: acked.append(True))
        self.assertEqual(len(acked), 1)
        py_target.publish.assert_not_called()

        nat_res1 = nat.route("mqtt_in", "native/only", b'{"v":1}')
        self.assertTrue(nat_res1["acked"])
        self.assertEqual(len(nat_res1["published"]), 1)
        self.assertEqual(nat_res1["published"][0]["topic"], "out.native")
        self.assertNotIn("engine", nat_res1["published"][0]["kwargs"])

        # Send to python/only
        py_router.route("mqtt_in", "python/only", b'{"v":2}', ack_fn=lambda: acked.append(True))
        self.assertEqual(len(acked), 2)
        self.assertEqual(py_target.publish.call_count, 1)
        self.assertEqual(py_target.publish.call_args[0], ("out.python", b'{"v":2}'))
        self.assertNotIn("engine", py_target.publish.call_args[1])

        nat_res2 = nat.route("mqtt_in", "python/only", b'{"v":2}')
        self.assertTrue(nat_res2["acked"])
        self.assertEqual(len(nat_res2["published"]), 0)

    def test_canary_percentage_split_complementarity_across_python_and_native(self):
        py_router = Router()
        py_router.set_worker_engine("python")
        py_published = []

        class _CaptureClient:
            name = "kafka_out"

            def publish(self, topic, message, **kwargs):
                py_published.append((topic, message, kwargs))

        py_router.register_client(_CaptureClient())
        py_router.add_route(
            "mqtt_in", "sensors/+/data", "kafka_out", "telemetry", engine="canary"
        )

        nat = NativeEngine()
        nat.apply_action(
            "add_broker",
            {"name": "kafka_out", "protocol": "kafka", "kwargs": {"bootstrap_servers": "localhost:9092"}},
        )
        nat.apply_action(
            "add_route",
            {
                "source_broker": "mqtt_in",
                "source_topic": "sensors/+/data",
                "target_broker": "kafka_out",
                "target_topic": "telemetry",
                "target_kwargs": {"engine": "canary"},
            },
        )

        for pct in (0, 5, 25, 100):
            py_published.clear()
            py_router.set_rollout_percentage(pct)
            nat.apply_action(
                "set_rollout",
                {"native_percentage": pct, "environment": "prod", "status": "active"},
            )

            nat_count = 0
            py_count = 0
            for i in range(200):
                payload = f'{{"seq":{i}}}'.encode("utf-8")
                topic = f"sensors/dev-{i % 10}/data"
                before_py = len(py_published)
                py_router.route("mqtt_in", topic, payload)
                py_handled = len(py_published) - before_py

                nat_out = nat.route("mqtt_in", topic, payload)
                nat_handled = len(nat_out["published"])

                # Every single message must be handled by EXACTLY one worker engine (no drops, no duplicates)
                self.assertEqual(
                    py_handled + nat_handled,
                    1,
                    f"Mismatch at pct={pct}, seq={i}: py={py_handled}, nat={nat_handled}",
                )
                py_count += py_handled
                nat_count += nat_handled

            if pct == 0:
                self.assertEqual(nat_count, 0)
                self.assertEqual(py_count, 200)
            elif pct == 100:
                self.assertEqual(nat_count, 200)
                self.assertEqual(py_count, 0)
            else:
                self.assertGreater(nat_count, 0)
                self.assertGreater(py_count, 0)

    def test_canary_rollout_controller_ramp_and_automatic_rollback(self):
        fake_redis = _InMemoryRedis()
        signer = HmacSigner({"k1": "super-secret-signing-key-32bytes!"})

        cp = ControlPlane.__new__(ControlPlane)
        cp.r = fake_redis
        cp.channel = "postoffice:config_updates"
        cp.signer = signer
        cp.include_schema_version = True

        dp = DataPlane.__new__(DataPlane)
        dp.r = fake_redis
        dp.po = MagicMock()
        dp.po.router = Router()
        dp.pubsub = MagicMock()
        dp.channel = "postoffice:config_updates"
        dp._stop_event = MagicMock()
        dp._stop_event.is_set.return_value = False
        dp._init_convergence_state()
        dp.signer = signer
        dp.signing_mode = "enforce"
        dp.allow_clear_state = True

        nat = NativeEngine()
        nat.configure_signing("k1:super-secret-signing-key-32bytes!", "enforce")
        nat.set_allow_clear_state(True)

        policy = CanaryPolicy(
            stages=(5, 25, 100),
            max_error_rate=0.02,
            max_lag_messages=500,
            max_p99_latency_ms=5.0,
            min_samples=100,
        )
        controller = CanaryRolloutController(
            environment="prod", policy=policy, control_plane=cp
        )

        # Start canary at 5%
        state0 = controller.start()
        self.assertEqual(state0.native_percentage, 5)
        self.assertEqual(state0.status, "active")

        # Sync event to both Python DataPlane and NativeEngine
        _, ev1 = fake_redis.published[-1]
        dp._process_update_message({"type": "message", "data": ev1})
        self.assertEqual(dp.po.router.native_percentage, 5)
        self.assertEqual(nat.apply_event(ev1, now_sec=1.0), "applied")

        # Low sample count holds at 5%
        hold = controller.evaluate_and_step(
            RolloutMetricsSample(routed_total=10, errors_total=0, consumer_lag=5, p99_latency_ms=0.5)
        )
        self.assertEqual(hold.native_percentage, 5)

        # Healthy sample at 5% -> promotes to 25%
        state1 = controller.evaluate_and_step(
            RolloutMetricsSample(
                routed_total=500,
                errors_total=1,
                consumer_lag=40,
                p99_latency_ms=1.4,
            )
        )
        self.assertEqual(state1.native_percentage, 25)
        self.assertEqual(state1.status, "active")
        _, ev2 = fake_redis.published[-1]
        dp._process_update_message({"type": "message", "data": ev2})
        self.assertEqual(dp.po.router.native_percentage, 25)
        self.assertEqual(nat.apply_event(ev2, now_sec=2.0), "applied")

        # Verify snapshot sync_state also loads postoffice:rollout
        dp2 = DataPlane.__new__(DataPlane)
        dp2.r = fake_redis
        dp2.po = MagicMock()
        dp2.po.router = Router()
        dp2.pubsub = MagicMock()
        dp2.channel = "postoffice:config_updates"
        dp2._stop_event = MagicMock()
        dp2._stop_event.is_set.return_value = False
        dp2._init_convergence_state()
        dp2.signer = signer
        dp2.signing_mode = "enforce"
        dp2.sync_state()
        self.assertEqual(dp2.po.router.native_percentage, 25)

        # Healthy sample at 25% -> promotes to 100%
        state2 = controller.evaluate_and_step(
            RolloutMetricsSample(
                routed_total=2000,
                errors_total=5,
                consumer_lag=120,
                p99_latency_ms=2.1,
            )
        )
        self.assertEqual(state2.native_percentage, 100)
        self.assertEqual(state2.status, "completed")
        _, ev3 = fake_redis.published[-1]
        dp._process_update_message({"type": "message", "data": ev3})
        self.assertEqual(dp.po.router.native_percentage, 100)
        self.assertEqual(nat.apply_event(ev3, now_sec=3.0), "applied")

        # Inject error-rate regression (5% > 2% threshold) -> automatic rollback to 0%
        state3 = controller.evaluate_and_step(
            RolloutMetricsSample(
                routed_total=1000,
                errors_total=50,
                consumer_lag=100,
                p99_latency_ms=2.0,
            )
        )
        self.assertEqual(state3.native_percentage, 0)
        self.assertEqual(state3.status, "rolled_back")
        self.assertIn("error_rate", state3.reason)
        _, ev4 = fake_redis.published[-1]
        dp._process_update_message({"type": "message", "data": ev4})
        self.assertEqual(dp.po.router.native_percentage, 0)
        self.assertEqual(nat.apply_event(ev4, now_sec=4.0), "applied")

        # Restart and test lag-regression and latency-regression rollbacks
        controller.start()
        state_lag = controller.evaluate_and_step(
            RolloutMetricsSample(
                routed_total=500,
                errors_total=0,
                consumer_lag=1200,  # > 500 max_lag_messages
                p99_latency_ms=1.5,
            )
        )
        self.assertEqual(state_lag.native_percentage, 0)
        self.assertEqual(state_lag.status, "rolled_back")
        self.assertIn("consumer_lag", state_lag.reason)

        controller.start()
        state_lat = controller.evaluate_and_step(
            RolloutMetricsSample(
                routed_total=500,
                errors_total=0,
                consumer_lag=10,
                p99_latency_ms=9.2,  # > 5.0ms max_p99_latency_ms
            )
        )
        self.assertEqual(state_lat.native_percentage, 0)
        self.assertEqual(state_lat.status, "rolled_back")
        self.assertIn("p99_latency", state_lat.reason)
        self.assertGreaterEqual(len(controller.history), 7)

        # Policy validation edge cases
        with self.assertRaises(ValueError):
            CanaryPolicy(stages=())
        with self.assertRaises(ValueError):
            CanaryPolicy(stages=(25, 5))
        with self.assertRaises(ValueError):
            CanaryPolicy(max_error_rate=1.5)
        with self.assertRaises(ValueError):
            CanaryPolicy(max_lag_messages=-1)
        with self.assertRaises(ValueError):
            CanaryPolicy(max_p99_latency_ms=0.0)
        with self.assertRaises(ValueError):
            CanaryPolicy(min_samples=0)


if __name__ == "__main__":
    unittest.main()
