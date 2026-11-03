import io
import json
import logging
import pathlib
import subprocess
import unittest
import urllib.error
import urllib.request

from postoffice.logging_config import (
    StructuredJsonFormatter,
    configure_structured_logging,
)
from postoffice.metrics import MetricsManager
from postoffice.native import NativeEngine

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent


class TestPhase4OperationalParity(unittest.TestCase):
    def test_python_and_native_healthz_readyz_metrics_http_parity(self):
        py_metrics = MetricsManager()
        py_metrics.set_readiness_probe(lambda: (True, 42))
        py_metrics.record_routed("mqtt_in", "kafka_out", "success")
        py_metrics.start_server(0, addr="127.0.0.1")
        py_port = py_metrics.bound_port

        nat_engine = NativeEngine()
        nat_engine.register_broker("kafka_out", "kafka", {})
        nat_engine.add_route("mqtt_in", "sensors/+", "kafka_out", "telemetry", {})
        nat_engine.route("mqtt_in", "sensors/1", b'{"t":1}')
        nat_port = nat_engine.start_metrics_server("127.0.0.1", 0)

        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{py_port}/healthz", timeout=2.0
            ) as resp:
                py_health = json.loads(resp.read().decode("utf-8"))
            with urllib.request.urlopen(
                f"http://127.0.0.1:{nat_port}/healthz", timeout=2.0
            ) as resp:
                nat_health = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(py_health, {"status": "ok", "version": "1.0.0"})
            self.assertEqual(py_health, nat_health)

            with urllib.request.urlopen(
                f"http://127.0.0.1:{py_port}/readyz", timeout=2.0
            ) as resp:
                py_ready = json.loads(resp.read().decode("utf-8"))
            with urllib.request.urlopen(
                f"http://127.0.0.1:{nat_port}/readyz", timeout=2.0
            ) as resp:
                nat_ready = json.loads(resp.read().decode("utf-8"))
            self.assertEqual(py_ready, {"ready": True, "applied_version": 42})
            self.assertEqual(set(py_ready.keys()), set(nat_ready.keys()))
            self.assertTrue(nat_ready["ready"])

            # Verify 503 when Python worker is not yet synced
            py_metrics.set_readiness_probe(lambda: (False, 0))
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{py_port}/readyz", timeout=2.0
                )
            self.assertEqual(ctx.exception.code, 503)
            ctx.exception.close()

            with urllib.request.urlopen(
                f"http://127.0.0.1:{py_port}/metrics", timeout=2.0
            ) as resp:
                py_m_text = resp.read().decode("utf-8")
            with urllib.request.urlopen(
                f"http://127.0.0.1:{nat_port}/metrics", timeout=2.0
            ) as resp:
                nat_m_text = resp.read().decode("utf-8")
            self.assertIn("postoffice_messages_routed_total", py_m_text)
            self.assertIn("postoffice_messages_routed_total", nat_m_text)
        finally:
            py_metrics.stop_server()
            nat_engine.stop_metrics_server()

    def test_structured_json_logging_parity(self):
        stream = io.StringIO()
        logger = logging.getLogger("postoffice.test_structured")
        logger.handlers.clear()
        logger.propagate = False
        configure_structured_logging(
            level=logging.INFO,
            engine="python",
            worker_id="py-worker-1",
            stream=stream,
            target_logger=logger,
        )
        logger.info(
            "Route applied",
            extra={
                "source_broker": "mqtt_in",
                "target_broker": "kafka_out",
                "applied_version": 7,
            },
        )
        line = stream.getvalue().strip()
        record = json.loads(line)
        self.assertEqual(record["level"], "INFO")
        self.assertEqual(record["logger"], "postoffice.test_structured")
        self.assertEqual(record["message"], "Route applied")
        self.assertEqual(record["engine"], "python")
        self.assertEqual(record["worker_id"], "py-worker-1")
        self.assertEqual(record["source_broker"], "mqtt_in")
        self.assertEqual(record["target_broker"], "kafka_out")
        self.assertEqual(record["applied_version"], 7)
        self.assertIn("timestamp", record)

    def test_mixed_fleet_operational_artifacts_and_runbook(self):
        compose_path = REPO_ROOT / "deploy" / "docker-compose.mixed-fleet.yml"
        self.assertTrue(compose_path.is_file())
        compose_text = compose_path.read_text(encoding="utf-8")
        self.assertIn("postoffice-worker-python", compose_text)
        self.assertIn("postoffice-worker-native", compose_text)
        self.assertIn("/healthz", compose_text)
        self.assertIn("/readyz", compose_text)

        chart_dir = REPO_ROOT / "deploy" / "helm" / "postoffice"
        self.assertTrue((chart_dir / "Chart.yaml").is_file())
        self.assertTrue((chart_dir / "values.yaml").is_file())
        self.assertTrue((chart_dir / "templates" / "deployment-python.yaml").is_file())
        self.assertTrue((chart_dir / "templates" / "deployment-native.yaml").is_file())
        self.assertTrue((chart_dir / "templates" / "service.yaml").is_file())

        dash_path = REPO_ROOT / "deploy" / "dashboards" / "postoffice-mixed-fleet.json"
        self.assertTrue(dash_path.is_file())
        dash = json.loads(dash_path.read_text(encoding="utf-8"))
        self.assertEqual(dash["title"], "PostOffice Mixed-Fleet Canary & SLO Dashboard")
        panel_titles = {p["title"] for p in dash["panels"]}
        self.assertIn("Throughput by Engine (msg/s)", panel_titles)
        self.assertIn("Bridge-Added Latency p99 (< 5 ms SLO)", panel_titles)
        self.assertIn("Routing Error Rate & Canary Rollback Threshold", panel_titles)
        self.assertIn("DLQ Admissions & Refusals", panel_titles)

        runbook_path = REPO_ROOT / "docs" / "runbooks" / "mixed-fleet-rollout.md"
        self.assertTrue(runbook_path.is_file())
        runbook_text = runbook_path.read_text(encoding="utf-8")
        self.assertIn("5% -> 25% -> 100%", runbook_text)
        self.assertIn("Automatic Rollback", runbook_text)
        self.assertIn("/healthz", runbook_text)
        self.assertIn("/readyz", runbook_text)


if __name__ == "__main__":
    unittest.main()
