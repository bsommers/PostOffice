import json
import socket
import threading
import unittest
from unittest.mock import MagicMock, patch

from postoffice.app import PostOffice
from postoffice.control_plane import ControlPlane
from postoffice.data_plane import DataPlane
from postoffice.native import NativeEngine
from postoffice.signing import HmacSigner


class _FakeRedisStreamsServer:
    """Minimal RESP2 TCP server supporting SUBSCRIBE, GET, HGETALL, and XRANGE for stream replay."""

    def __init__(self, stream_events):
        self.stream_events = list(stream_events)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("127.0.0.1", 0))
        self.sock.listen(8)
        self.port = self.sock.getsockname()[1]
        self._stop = False
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def close(self):
        self._stop = True
        try:
            self.sock.close()
        except Exception:
            pass

    @staticmethod
    def _bulk(s: str) -> bytes:
        b = s.encode("utf-8")
        return f"${len(b)}\r\n".encode("ascii") + b + b"\r\n"

    def _serve(self):
        while not self._stop:
            try:
                conn, _ = self.sock.accept()
            except Exception:
                break
            threading.Thread(target=self._handle, args=(conn,), daemon=True).start()

    def _handle(self, conn: socket.socket):
        f = conn.makefile("rb")
        try:
            while not self._stop:
                line = f.readline()
                if not line:
                    break
                if not line.startswith(b"*"):
                    continue
                argc = int(line[1:].strip())
                args = []
                for _ in range(argc):
                    sz_line = f.readline()
                    sz = int(sz_line[1:].strip())
                    data = f.read(sz)
                    f.read(2)
                    args.append(data.decode("utf-8"))
                cmd = args[0].upper()
                if cmd == "SUBSCRIBE":
                    chan = args[1]
                    resp = (
                        b"*3\r\n"
                        + self._bulk("subscribe")
                        + self._bulk(chan)
                        + b":1\r\n"
                    )
                    conn.sendall(resp)
                elif cmd == "GET":
                    # Snapshot is at version 0; stream carries versions 1 and 2
                    conn.sendall(self._bulk("0"))
                elif cmd == "HGETALL":
                    conn.sendall(b"*0\r\n")
                elif cmd == "XRANGE":
                    # Return array of [entry_id, ["version", v, "payload", p]]
                    out = [f"*{len(self.stream_events)}\r\n".encode("ascii")]
                    for idx, (ver, payload) in enumerate(self.stream_events, start=1):
                        entry_id = f"1700000000000-{idx}"
                        out.append(b"*2\r\n")
                        out.append(self._bulk(entry_id))
                        out.append(b"*4\r\n")
                        out.append(self._bulk("version"))
                        out.append(self._bulk(str(ver)))
                        out.append(self._bulk("payload"))
                        out.append(self._bulk(payload))
                    conn.sendall(b"".join(out))
        except Exception:
            pass
        finally:
            try:
                conn.close()
            except Exception:
                pass


class TestPhase5Consolidation(unittest.TestCase):
    @patch("redis.Redis")
    def test_redis_streams_control_plane_publish_and_data_plane_replay(
        self, mock_redis_cls
    ):
        mock_cp_r = MagicMock()
        mock_cp_r.incr.side_effect = [1, 2, 3]
        mock_dp_r = MagicMock()
        mock_redis_cls.side_effect = [mock_cp_r, mock_dp_r]

        signer = HmacSigner({"k1": "super-secret-signing-key-32bytes!"})
        cp = ControlPlane(signer=signer, use_streams=True)
        cp.register_broker("mqtt_edge", "mqtt", host="mqtt.internal", port=1883)
        cp.register_broker("kafka_core", "kafka", bootstrap_servers="kafka.internal:9092")
        cp.add_route("mqtt_edge", "sensors/+", "kafka_core", "telemetry")

        # Verify XADD was called 3 times on postoffice:config_stream
        self.assertEqual(mock_cp_r.xadd.call_count, 3)
        stream_records = []
        for idx, call in enumerate(mock_cp_r.xadd.call_args_list, start=1):
            stream_name, fields = call.args[0], call.args[1]
            self.assertEqual(stream_name, "postoffice:config_stream")
            stream_records.append((f"1700000000000-{idx}", fields))

        # Now configure DataPlane at version 0 and replay all 3 events from Redis Streams
        mock_dp_r.xrange.return_value = stream_records

        dp = DataPlane(signer=signer, signing_mode="enforce")
        dp._version = 0
        with patch.object(dp.po, "add_broker", wraps=dp.po.add_broker), patch(
            "postoffice.registry.ClientRegistry.create_client"
        ) as mock_create:
            mock_create.side_effect = lambda proto, name, router, **kw: MagicMock(
                name=name
            )
            replayed = dp.replay_stream(after_version=0)
            self.assertEqual(replayed, 3)
            self.assertEqual(dp._version, 3)
            self.assertIn("mqtt_edge", dp.po.brokers)
            self.assertIn("kafka_core", dp.po.brokers)
            matched = dp.po.router.client_tries["mqtt_edge"].match("sensors/temp")
            self.assertEqual(len(matched), 1)
            self.assertEqual(matched[0]["target_client"], "kafka_core")

    def test_native_engine_redis_streams_catchup_via_xrange(self):
        ev1 = json.dumps(
            {
                "action": "add_broker",
                "data": {"name": "mqtt_edge", "protocol": "mqtt", "kwargs": {}},
                "version": 1,
            }
        )
        ev2 = json.dumps(
            {
                "action": "add_broker",
                "data": {"name": "kafka_core", "protocol": "kafka", "kwargs": {}},
                "version": 2,
            }
        )
        ev3 = json.dumps(
            {
                "action": "add_route",
                "data": {
                    "source_broker": "mqtt_edge",
                    "source_topic": "sensors/+",
                    "target_broker": "kafka_core",
                    "target_topic": "telemetry",
                    "target_kwargs": {},
                },
                "version": 3,
            }
        )
        srv = _FakeRedisStreamsServer([(1, ev1), (2, ev2), (3, ev3)])
        try:
            eng = NativeEngine()
            rep = eng.sync_from_redis(host="127.0.0.1", port=srv.port, now_sec=100.0)
            self.assertEqual(rep["applied_version"], 3)
            self.assertEqual(rep["active_brokers"], 2)
            self.assertEqual(rep["active_routes"], 1)
            out = eng.route("mqtt_edge", "sensors/temp", b'{"v":1}', now_sec=101.0)
            self.assertTrue(out["acked"])
            self.assertEqual(len(out["published"]), 1)
            self.assertEqual(out["published"][0]["target_broker"], "kafka_core")
        finally:
            srv.close()

    @patch("postoffice.registry.ClientRegistry.create_client")
    def test_postoffice_facade_native_engine_consolidation_and_oracle_parity(
        self, mock_create_client
    ):
        def _make_client(protocol, name, router, **kwargs):
            m = MagicMock()
            m.name = name
            m.publish.side_effect = (
                lambda topic, message, on_confirm=None, on_error=None, **kw: (
                    on_confirm() if on_confirm else None
                )
            )
            return m

        mock_create_client.side_effect = _make_client

        po = PostOffice(on_unroutable="dlq", use_native_engine=True)
        self.assertIsNotNone(po.native_engine)
        po.add_broker("mqtt_in", "mqtt")
        po.add_broker("kafka_out", "kafka")
        po.add_broker("dlq_brk", "kafka")
        po.set_dlq("dlq_brk", "system.dlq")
        po.add_route("mqtt_in", "factory/+/telemetry", "kafka_out", "factory.events")
        po.configure_ingress(
            {
                "rules": [
                    {
                        "type": "max_payload_bytes",
                        "max_bytes": 32,
                        "action": "drop",
                    }
                ],
            }
        )

        # 1. Routed message matches between NativeEngine and Python reference router
        native_ok = po.route_native(
            "mqtt_in", "factory/line1/telemetry", b'{"ok":true}', now_sec=50.0
        )
        self.assertTrue(native_ok["acked"])
        self.assertEqual(len(native_ok["published"]), 1)
        self.assertEqual(native_ok["published"][0]["target_broker"], "kafka_out")

        acked = []
        po.router.route(
            "mqtt_in",
            "factory/line1/telemetry",
            b'{"ok":true}',
            ack_fn=lambda: acked.append(True),
        )
        self.assertEqual(acked, [True])

        # 2. Firewall drop matches between NativeEngine and Python reference router
        native_drop = po.route_native(
            "mqtt_in",
            "factory/line1/telemetry",
            b"x" * 64,
            now_sec=51.0,
        )
        self.assertTrue(native_drop["acked"])
        self.assertEqual(native_drop["firewall_verdict"], "drop")
        self.assertEqual(len(native_drop["published"]), 0)

        # 3. Unroutable -> DLQ matches between NativeEngine and Python reference router
        native_dlq = po.route_native(
            "mqtt_in", "unknown/topic", b'{"u":1}', now_sec=52.0
        )
        self.assertTrue(native_dlq["acked"])
        self.assertEqual(len(native_dlq["dlq_published"]), 1)
        self.assertEqual(native_dlq["dlq_published"][0]["target_broker"], "dlq_brk")


if __name__ == "__main__":
    unittest.main()
