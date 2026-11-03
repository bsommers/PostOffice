import json
import pathlib
import socket
import subprocess
import threading
import unittest
import urllib.request

from postoffice.native import (
    NativeDlqBudget,
    NativeEngine,
    NativeSigner,
    native_format_dlq_payload,
    native_validate_action,
)

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
VECTORS_DIR = REPO_ROOT / "conformance" / "vectors"


class _MockResp2Server:
    """Minimal in-process Redis RESP2 server for testing subscribe-before-snapshot convergence."""

    def __init__(self, version: int = 2, mid_snapshot_event: str | None = None):
        self.version = version
        self.mid_snapshot_event = mid_snapshot_event
        self.brokers = {
            "mqtt_in": json.dumps(
                {"protocol": "mqtt", "kwargs": {"host": "127.0.0.1", "port": 1883}}
            ),
            "amqp_out": json.dumps(
                {"protocol": "amqp", "kwargs": {"host": "127.0.0.1", "port": 5672}}
            ),
        }
        self.routes = {
            "r1": json.dumps(
                {
                    "source_broker": "mqtt_in",
                    "source_topic": "sensors/+",
                    "target_broker": "amqp_out",
                    "target_topic": "enterprise/sensors",
                    "target_kwargs": {"semantic_translation": True},
                }
            )
        }
        self.subscriptions = {
            "mqtt_in:sensors/+": json.dumps(
                {"broker_name": "mqtt_in", "topic": "sensors/+", "kwargs": {"qos": 1}}
            )
        }
        self.ingress = {
            "rules": json.dumps(
                {
                    "rules": [{"type": "max_payload_bytes", "max_bytes": 4096, "action": "drop"}],
                    "on_rule_error": "reject",
                }
            )
        }
        self.subscribe_confirmed = False
        self.commands_seen: list[list[str]] = []
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self.port = self._sock.getsockname()[1]
        self._stop = threading.Event()
        self._pubsub_conns: list[socket.socket] = []
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass
        with self._lock:
            for c in self._pubsub_conns:
                try:
                    c.close()
                except OSError:
                    pass

    def _accept_loop(self) -> None:
        self._sock.settimeout(0.2)
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                continue
            t = threading.Thread(target=self._handle_conn, args=(conn,), daemon=True)
            t.start()

    @staticmethod
    def _encode_bulk(s: str | None) -> bytes:
        if s is None:
            return b"$-1\r\n"
        b = s.encode("utf-8")
        return f"${len(b)}\r\n".encode("ascii") + b + b"\r\n"

    @classmethod
    def _encode_array(cls, items: list) -> bytes:
        out = [f"*{len(items)}\r\n".encode("ascii")]
        for item in items:
            if isinstance(item, int):
                out.append(f":{item}\r\n".encode("ascii"))
            else:
                out.append(cls._encode_bulk(str(item)))
        return b"".join(out)

    def _handle_conn(self, conn: socket.socket) -> None:
        conn.settimeout(2.0)
        buf = b""
        try:
            while not self._stop.is_set():
                cmd, buf = self._read_command(conn, buf)
                if cmd is None:
                    break
                self.commands_seen.append(cmd)
                op = cmd[0].upper()
                if op == "SUBSCRIBE":
                    chan = cmd[1]
                    self.subscribe_confirmed = True
                    with self._lock:
                        self._pubsub_conns.append(conn)
                    conn.sendall(self._encode_array(["subscribe", chan, 1]))
                elif op == "GET":
                    key = cmd[1]
                    if key == "postoffice:version":
                        conn.sendall(self._encode_bulk(str(self.version)))
                    else:
                        conn.sendall(self._encode_bulk(None))
                elif op == "HGETALL":
                    key = cmd[1]
                    mapping = {
                        "postoffice:brokers": self.brokers,
                        "postoffice:routes": self.routes,
                        "postoffice:subscriptions": self.subscriptions,
                        "postoffice:ingress": self.ingress,
                    }.get(key, {})
                    flat: list[str] = []
                    for k, v in mapping.items():
                        flat.extend([k, v])
                    conn.sendall(self._encode_array(flat))
                    if key == "postoffice:routes" and self.mid_snapshot_event is not None:
                        ev = self.mid_snapshot_event
                        self.mid_snapshot_event = None
                        with self._lock:
                            for pub_conn in list(self._pubsub_conns):
                                try:
                                    pub_conn.sendall(
                                        self._encode_array(
                                            ["message", "postoffice:config_updates", ev]
                                        )
                                    )
                                except OSError:
                                    pass
                elif op == "PING":
                    conn.sendall(b"+PONG\r\n")
                else:
                    conn.sendall(b"+OK\r\n")
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass

    @staticmethod
    def _read_line(conn: socket.socket, buf: bytes) -> tuple[bytes | None, bytes]:
        while b"\r\n" not in buf:
            chunk = conn.recv(4096)
            if not chunk:
                return None, buf
            buf += chunk
        idx = buf.index(b"\r\n")
        return buf[:idx], buf[idx + 2 :]

    @classmethod
    def _read_command(cls, conn: socket.socket, buf: bytes) -> tuple[list[str] | None, bytes]:
        line, buf = cls._read_line(conn, buf)
        if line is None:
            return None, buf
        if not line.startswith(b"*"):
            return [line.decode("utf-8", "replace")], buf
        count = int(line[1:])
        args: list[str] = []
        for _ in range(count):
            hdr, buf = cls._read_line(conn, buf)
            if hdr is None or not hdr.startswith(b"$"):
                return None, buf
            length = int(hdr[1:])
            while len(buf) < length + 2:
                chunk = conn.recv(4096)
                if not chunk:
                    return None, buf
                buf += chunk
            args.append(buf[:length].decode("utf-8", "replace"))
            buf = buf[length + 2 :]
        return args, buf


class TestPhase3NativeWorkerCore(unittest.TestCase):
    def test_native_signing_and_validation_conformance(self):
        vec = json.loads((VECTORS_DIR / "control_plane_signing.json").read_text(encoding="utf-8"))
        signer = NativeSigner(vec["keyring"])
        event_body = vec["event_body"]

        signed_ev = json.loads(signer.sign_event(event_body))
        self.assertEqual(signed_ev, vec["signed_event_envelope"])
        self.assertTrue(signer.verify_event(json.dumps(signed_ev)))

        signed_st = json.loads(signer.sign_state("postoffice:routes", "route_1", event_body))
        self.assertEqual(signed_st, vec["signed_state_envelope"])
        self.assertTrue(
            signer.verify_state("postoffice:routes", "route_1", json.dumps(signed_st))
        )

        # Cross-domain replay must be rejected
        self.assertFalse(
            signer.verify_state("postoffice:routes", "route_1", json.dumps(signed_ev))
        )

        signed_ver = json.loads(signer.sign_version(7))
        self.assertEqual(signed_ver, vec["signed_version_envelope"])
        self.assertTrue(signer.verify_version(7, json.dumps(signed_ver)))
        self.assertFalse(signer.verify_version(8, json.dumps(signed_ver)))

        for vc in vec["validation_cases"]:
            actual = native_validate_action(vc["action"], vc["data"])
            self.assertEqual(actual, vc["actual_valid"], msg=vc["name"])

    def test_native_dlq_envelope_and_budget_conformance(self):
        vec = json.loads((VECTORS_DIR / "dlq_envelopes.json").read_text(encoding="utf-8"))

        utf8_env = json.loads(
            native_format_dlq_payload(
                b'{"sensor": 1}',
                "mqtt_edge",
                "sensors/1",
                "Broker down",
                metadata={"attempt": 1},
                timestamp="2026-10-31T00:00:00+00:00",
            ).decode("utf-8")
        )
        self.assertEqual(utf8_env, vec["utf8_envelope"])

        bin_env = json.loads(
            native_format_dlq_payload(
                bytes.fromhex("fffefd00"),
                "amqp_core",
                "bin/topic",
                "Decode error",
                metadata={},
                timestamp="2026-10-31T00:00:00+00:00",
            ).decode("utf-8")
        )
        self.assertEqual(bin_env, vec["binary_envelope"])

        budget = NativeDlqBudget(
            max_message_bytes=32,
            msgs_per_sec=10.0,
            bytes_per_sec=1024.0,
            dedupe_window=10.0,
        )
        now = 1000.0
        for step in vec["budget_steps"]:
            now += step["advance_sec"]
            payload = bytes.fromhex(step["payload_hex"])
            res = budget.admit(
                source_broker="mqtt_edge",
                source_topic="sensors/1",
                error="Timeout",
                payload=payload,
                metadata_only=step["metadata_only"],
                now_sec=now,
            )
            self.assertEqual(res["refusal"], step["refusal"], msg=step["step"])
            self.assertEqual(res["repeat_count"], step["repeat_count"], msg=step["step"])

    def test_native_engine_router_delivery_conformance(self):
        vec = json.loads((VECTORS_DIR / "router_delivery.json").read_text(encoding="utf-8"))
        for case in vec["cases"]:
            if case["case"].startswith("unroutable_policy_"):
                policy = case["policy"]
                engine = NativeEngine()
                engine.register_broker("src", "mqtt", {})
                engine.register_broker("dlq_b", "mqtt", {})
                engine.set_dlq("dlq_b", "dlq.topic")
                engine.set_unroutable_policy(policy)
                outcome = engine.route("src", "unmatched/topic", b"hello")
                self.assertEqual(outcome["acked"], case["acked"], msg=case["case"])
                self.assertEqual(outcome["nacked"], case["nacked"], msg=case["case"])
                self.assertEqual(len(outcome["dlq_published"]), case["dlq_count"], msg=case["case"])
            elif case["case"] == "fanout_with_semantic_translation":
                engine = NativeEngine()
                engine.register_broker("mqtt_src", "mqtt", {})
                engine.register_broker("amqp_t1", "amqp", {})
                engine.register_broker("kafka_t2", "kafka", {})
                engine.add_route(
                    "mqtt_src",
                    "telemetry/+",
                    "amqp_t1",
                    "amqp.telemetry",
                    {"semantic_translation": True},
                )
                engine.add_route(
                    "mqtt_src",
                    "telemetry/+",
                    "kafka_t2",
                    "kafka.telemetry",
                    {"semantic_translation": True},
                )
                outcome = engine.route(
                    "mqtt_src",
                    "telemetry/dev1",
                    b'{"v":1}',
                    source_kwargs={"qos": 1},
                )
                self.assertEqual(outcome["acked"], case["acked"])
                t1_pub = [
                    {"topic": p["topic"], "payload_hex": p["payload_hex"], "kwargs": p["kwargs"]}
                    for p in outcome["published"]
                    if p["target_broker"] == "amqp_t1"
                ]
                t2_pub = [
                    {"topic": p["topic"], "payload_hex": p["payload_hex"], "kwargs": p["kwargs"]}
                    for p in outcome["published"]
                    if p["target_broker"] == "kafka_t2"
                ]
                self.assertEqual(t1_pub, case["t1_published"])
                self.assertEqual(t2_pub, case["t2_published"])

    def test_native_engine_metrics_and_http_endpoint(self):
        engine = NativeEngine(max_label_values=2)
        engine.register_broker("b1", "mqtt", {})
        engine.register_broker("b2", "mqtt", {})
        engine.register_broker("b3", "mqtt", {})
        engine.add_route("b1", "t", "b2", "out", {})
        engine.add_route("b2", "t", "b3", "out", {})
        engine.add_route("b3", "t", "b1", "out", {})

        engine.route("b1", "t", b"1")
        engine.route("b2", "t", b"2")
        engine.route("b3", "t", b"3")  # 3rd source_broker overflows to "other"

        text = engine.metrics_text()
        self.assertIn('postoffice_messages_routed_total{source_broker="b1"', text)
        self.assertIn('postoffice_messages_routed_total{source_broker="b2"', text)
        self.assertIn('postoffice_messages_routed_total{source_broker="other"', text)

        port = engine.start_metrics_server("127.0.0.1", 0)
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=2.0) as resp:
                body = resp.read().decode("utf-8")
                self.assertIn("postoffice_messages_routed_total", body)
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/healthz", timeout=2.0) as resp:
                health = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(health["status"], "ok")
        finally:
            engine.stop_metrics_server()

    def test_native_redis_subscribe_before_snapshot_convergence(self):
        mid_event = json.dumps(
            {
                "action": "add_route",
                "data": {
                    "source_broker": "mqtt_in",
                    "source_topic": "alerts/#",
                    "target_broker": "amqp_out",
                    "target_topic": "enterprise/alerts",
                    "target_kwargs": {},
                },
                "version": 3,
                "schema_version": 1,
            }
        )
        server = _MockResp2Server(version=2, mid_snapshot_event=mid_event)
        try:
            engine = NativeEngine()
            sync_report = engine.sync_from_redis("127.0.0.1", server.port)
            # SUBSCRIBE must be the very first command sent on the pubsub socket before snapshot reads
            self.assertEqual(server.commands_seen[0][0].upper(), "SUBSCRIBE")
            self.assertEqual(sync_report["applied_version"], 3)
            self.assertEqual(sync_report["active_brokers"], 2)
            self.assertEqual(sync_report["active_routes"], 2)

            # Both snapshot route (sensors/+) and mid-snapshot buffered event route (alerts/#) must work
            r1 = engine.route("mqtt_in", "sensors/temp", b'{"t":20}')
            self.assertEqual(len(r1["published"]), 1)
            self.assertEqual(r1["published"][0]["topic"], "enterprise/sensors")

            r2 = engine.route("mqtt_in", "alerts/fire", b'{"a":1}')
            self.assertEqual(len(r2["published"]), 1)
            self.assertEqual(r2["published"][0]["topic"], "enterprise/alerts")

            # Also verify standalone postoffice-worker binary --sync-once mode
            worker_bin = (
                REPO_ROOT / "native" / "libpostoffice" / "target" / "release" / "postoffice-worker"
            )
            proc = subprocess.run(
                [
                    str(worker_bin),
                    "--redis-host",
                    "127.0.0.1",
                    "--redis-port",
                    str(server.port),
                    "--sync-once",
                ],
                capture_output=True,
                text=True,
                timeout=10,
                check=True,
            )
            cli_out = json.loads(proc.stdout.strip())
            self.assertEqual(cli_out["applied_version"], 2)
            self.assertEqual(cli_out["active_brokers"], 2)
        finally:
            server.close()


if __name__ == "__main__":
    unittest.main()
