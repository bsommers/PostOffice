"""
Live Containerized Broker Integration & Soak Suite.
Gated by POSTOFFICE_INTEGRATION=1 and POSTOFFICE_TEST_REDIS=host:port.
"""

from __future__ import annotations

import json
import os
import unittest
from typing import Any, Dict

import redis

from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane
from postoffice.native import NativeEngine
from postoffice.rollout import (
    CanaryPolicy,
    CanaryRolloutController,
    RolloutMetricsSample,
)
from postoffice.shadow import ShadowHarness

# Alias for soak harness semantics
ShadowModeRunner = ShadowHarness

TARGET = os.environ.get("POSTOFFICE_TEST_REDIS")
ENABLED = os.environ.get("POSTOFFICE_INTEGRATION") == "1" and TARGET


@unittest.skipUnless(
    ENABLED,
    "Integration test requires POSTOFFICE_INTEGRATION=1 and POSTOFFICE_TEST_REDIS=host:port",
)
class TestLiveBrokerSoak(unittest.TestCase):
    def setUp(self) -> None:
        assert TARGET is not None
        host, port = TARGET.rsplit(":", 1)
        self.host = host
        self.port = int(port)
        self.admin = redis.Redis(host=self.host, port=self.port, decode_responses=True)
        self.admin.ping()
        self.admin.flushall()
        self.cp = ControlPlane(redis_host=self.host, redis_port=self.port, use_streams=True)
        self.planes: list[DataPlane] = []
        self.addCleanup(self._cleanup)

    def _make_dp(self) -> DataPlane:
        dp = DataPlane(redis_host=self.host, redis_port=self.port)
        self.planes.append(dp)
        return dp

    def _cleanup(self) -> None:
        for dp in self.planes:
            dp._stop_event.set()
            dp._buffering.clear()
            try:
                if getattr(dp, "pubsub", None) is not None:
                    dp.pubsub.close()
                dp.r.close()
            except Exception:
                pass
        try:
            self.cp.r.close()
            self.admin.flushall()
            self.admin.close()
        except Exception:
            pass

    def test_live_redis_control_plane_convergence_python_and_native(self) -> None:
        self.cp.register_broker("mqtt_live", "mqtt", host="127.0.0.1", port=1883)
        self.cp.register_broker("kafka_live", "kafka", bootstrap_servers="127.0.0.1:9092")
        self.cp.add_route("mqtt_live", "sensors/+/temp", "kafka_live", "telemetry.avro")
        self.cp.add_subscription("mqtt_live", "sensors/+/temp")

        dp = self._make_dp()
        dp._converge()

        self.assertEqual(sorted(dp.po.brokers.keys()), ["kafka_live", "mqtt_live"])
        self.assertEqual(len(dp.po.router.routes.get(("mqtt_live", "sensors/+/temp"), [])), 1)
        self.assertEqual(dp._version, int(self.admin.get("postoffice:version")))

        native_eng = NativeEngine()
        sync_rep = native_eng.sync_from_redis(host=self.host, port=self.port)

        self.assertEqual(sync_rep["applied_version"], dp._version)
        self.assertEqual(sync_rep["active_brokers"], 2)
        self.assertEqual(sync_rep["active_routes"], 1)

    def test_live_redis_streams_xadd_xrange_catchup(self) -> None:
        self.cp.register_broker("b1", "mqtt", host="127.0.0.1", port=1883)
        self.cp.register_broker("b2", "kafka", bootstrap_servers="127.0.0.1:9092")
        self.cp.add_route("b1", "init/topic", "b2", "init.out")

        dp = self._make_dp()
        dp._converge()
        init_version = dp._version

        # Add more updates; ControlPlane with use_streams=True publishes to stream
        self.cp.add_route("b1", "stream/r1", "b2", "stream1.out")
        self.cp.add_route("b1", "stream/r2", "b2", "stream2.out")

        replayed = dp.replay_stream(after_version=init_version)
        self.assertEqual(replayed, 2)
        self.assertEqual(dp._version, int(self.admin.get("postoffice:version")))
        self.assertEqual(len(dp.po.router.routes.get(("b1", "stream/r1"), [])), 1)
        self.assertEqual(len(dp.po.router.routes.get(("b1", "stream/r2"), [])), 1)

        # Native engine catches up from Redis streams
        native_eng = NativeEngine()
        sync_rep = native_eng.sync_from_redis(host=self.host, port=self.port)
        self.assertEqual(sync_rep["applied_version"], dp._version)
        self.assertEqual(sync_rep["active_routes"], 3)

    def test_live_shadow_mode_runner_zero_divergence_parity(self) -> None:
        harness = ShadowModeRunner(unroutable_policy="dlq")
        avro_schema = json.dumps(
            {
                "type": "record",
                "name": "LiveMetric",
                "fields": [
                    {"name": "host", "type": "string"},
                    {"name": "val", "type": "double"},
                ],
            }
        )
        harness.apply_action(
            {
                "action": "set_dlq",
                "target_broker": "dlq",
                "topic": "dlq.out",
                "rate_per_sec": 1000.0,
                "burst": 200,
                "max_payload_bytes": 1024,
            }
        )
        harness.apply_action(
            {
                "action": "register_schema",
                "name": "LiveMetric",
                "schema_str": avro_schema,
                "schema_type": "AVRO",
                "version": 1,
            }
        )
        harness.apply_action(
            {
                "action": "set_ingress",
                "rules": [
                    {
                        "type": "topic_shape",
                        "deny": ["banned/#"],
                        "action": "drop",
                    }
                ],
            }
        )
        harness.apply_action(
            {
                "action": "add_route",
                "source_broker": "edge",
                "source_topic": "metrics/+",
                "target_broker": "cloud",
                "target_topic": "cloud.avro",
                "kwargs": {
                    "input_format": "json",
                    "output_format": "avro",
                    "output_schema": "LiveMetric",
                    "confluent_framing": True,
                },
            }
        )
        harness.apply_action(
            {
                "action": "add_route",
                "source_broker": "edge",
                "source_topic": "raw/+",
                "target_broker": "cloud",
                "target_topic": "cloud.raw",
                "kwargs": {},
            }
        )

        messages = []
        for i in range(1000):
            if i % 4 == 0:
                messages.append(
                    {
                        "source_broker": "edge",
                        "topic": "metrics/m1",
                        "payload": json.dumps({"host": f"srv-{i}", "val": float(i)}).encode("utf-8"),
                        "kwargs": {},
                    }
                )
            elif i % 4 == 1:
                messages.append(
                    {
                        "source_broker": "edge",
                        "topic": "banned/bad",
                        "payload": b"drop-me",
                        "kwargs": {},
                    }
                )
            elif i % 4 == 2:
                messages.append(
                    {
                        "source_broker": "edge",
                        "topic": "metrics/m1",
                        "payload": b'{"host": "srv", "val": "not_a_number"}',  # triggers DLQ
                        "kwargs": {},
                    }
                )
            else:
                messages.append(
                    {
                        "source_broker": "edge",
                        "topic": "raw/item",
                        "payload": f"item-{i}".encode("utf-8"),
                        "kwargs": {},
                    }
                )

        report = harness.run_soak(messages)
        self.assertTrue(report.is_clean, f"Divergences found: {report.diffs}")
        self.assertEqual(len(report.diffs), 0)
        self.assertEqual(report.total_messages, 1000)

    def test_live_canary_rollout_progression_and_automatic_rollback(self) -> None:
        policy = CanaryPolicy(
            stages=(5, 25, 100),
            max_error_rate=0.01,
            max_lag_messages=1000,
            max_p99_latency_ms=5.0,
            min_samples=100,
        )
        controller = CanaryRolloutController(
            environment="staging", policy=policy, control_plane=self.cp
        )
        dp = self._make_dp()
        dp._converge()
        native_eng = NativeEngine()
        native_eng.sync_from_redis(host=self.host, port=self.port)

        # Stage 0: 5%
        s0 = controller.start()
        self.assertEqual(s0.native_percentage, 5)
        self.assertEqual(s0.status, "active")
        dp._converge()
        sync_rep = native_eng.sync_from_redis(host=self.host, port=self.port)
        self.assertEqual(dp.po.router.native_percentage, 5)

        # Stage 1: Promoted to 25%
        s1 = controller.evaluate_and_step(
            RolloutMetricsSample(routed_total=500, errors_total=0, consumer_lag=5, p99_latency_ms=0.5)
        )
        self.assertEqual(s1.native_percentage, 25)
        self.assertEqual(s1.status, "active")
        dp._converge()
        self.assertEqual(dp.po.router.native_percentage, 25)

        # Stage 2: Promoted to 100%
        s2 = controller.evaluate_and_step(
            RolloutMetricsSample(routed_total=2000, errors_total=1, consumer_lag=10, p99_latency_ms=0.9)
        )
        self.assertEqual(s2.native_percentage, 100)
        self.assertEqual(s2.status, "completed")
        dp._converge()
        self.assertEqual(dp.po.router.native_percentage, 100)

        # Breach: Simulated error rate spike (6% > 1%) -> immediate rollback to 0%
        s3 = controller.evaluate_and_step(
            RolloutMetricsSample(routed_total=1000, errors_total=60, consumer_lag=10, p99_latency_ms=1.1)
        )
        self.assertEqual(s3.native_percentage, 0)
        self.assertEqual(s3.status, "rolled_back")
        self.assertIn("error_rate_regression", s3.reason)
        dp._converge()
        self.assertEqual(dp.po.router.native_percentage, 0)


if __name__ == "__main__":
    unittest.main()
