import io
import json
import socket
import ssl
import threading
import time
import unittest
from unittest.mock import MagicMock, patch
import urllib.error
import urllib.request

import postoffice.plugins  # noqa: F401
from postoffice.plugins.http_client import HttpClient, WebhookClient
from postoffice.plugins.websocket_client import (
    OPCODE_BINARY,
    OPCODE_CLOSE,
    OPCODE_PING,
    OPCODE_PONG,
    OPCODE_TEXT,
    WebSocketClient,
    WebSocketServer,
    _PrefixedSocket,
    compute_accept_key,
    decode_ws_envelope,
    encode_ws_envelope,
    encode_ws_frame,
    read_ws_frame,
)
from postoffice.registry import ClientRegistry
from postoffice.router import Router
from postoffice.security import SecurityConfig
from postoffice.validation import PayloadError, plugin_kwargs, validate_action


class TestHttpAndWebSocketValidation(unittest.TestCase):
    def test_registry_lookup(self):
        self.assertIs(ClientRegistry.get_client("http"), HttpClient)
        self.assertIs(ClientRegistry.get_client("webhook"), WebhookClient)
        self.assertIs(ClientRegistry.get_client("websocket"), WebSocketClient)

    def test_validate_add_broker_http_webhook_websocket(self):
        validate_action(
            "add_broker",
            {
                "name": "http_in",
                "protocol": "http",
                "kwargs": {
                    "url": "http://127.0.0.1:9000/hook",
                    "listen_host": "127.0.0.1",
                    "listen_port": 0,
                    "timeout": 2.5,
                    "bearer_token": "tok123",
                    "verify_tls": False,
                },
            },
        )
        validate_action(
            "add_broker",
            {
                "name": "wh_out",
                "protocol": "webhook",
                "kwargs": {
                    "url": "http://127.0.0.1:9001/events",
                    "enable_publish_buffer": True,
                },
            },
        )
        validate_action(
            "add_broker",
            {
                "name": "ws_edge",
                "protocol": "websocket",
                "kwargs": {
                    "host": "127.0.0.1",
                    "port": 8765,
                    "path": "/ws",
                    "timeout": 3.0,
                    "bearer_token": "ws-sec",
                },
            },
        )

        # Reject invalid scalar types
        with self.assertRaises(PayloadError):
            validate_action(
                "add_broker",
                {
                    "name": "bad_http",
                    "protocol": "http",
                    "kwargs": {"listen_port": "not-an-int"},
                },
            )
        with self.assertRaises(PayloadError):
            validate_action(
                "add_broker",
                {
                    "name": "bad_ws",
                    "protocol": "websocket",
                    "kwargs": {"unknown_kwarg": 123},
                },
            )

        http_kw = plugin_kwargs("http")
        self.assertIs(http_kw["listen_port"], int)
        self.assertIs(http_kw["bearer_token"], str)
        ws_kw = plugin_kwargs("websocket")
        self.assertIs(ws_kw["port"], int)
        self.assertIs(ws_kw["path"], str)


class TestHttpClientLoopback(unittest.TestCase):
    def test_end_to_end_http_ingress_to_webhook_egress(self):
        router = Router()
        received_Event = threading.Event()
        delivered = []

        # Target HTTP listener that records received webhook deliveries
        sink_router = Router()
        sink_client = WebhookClient("sink_listener", sink_router, listen_host="127.0.0.1", listen_port=0)

        class _CaptureRouter:
            def route(self, broker_name, topic, message, ack_fn=None, nack_fn=None, **kwargs):
                delivered.append((topic, message))
                if ack_fn:
                    ack_fn()
                received_Event.set()

        sink_client.router = _CaptureRouter()
        sink_client.subscribe("out/#")
        sink_client.connect()

        egress_url = f"http://127.0.0.1:{sink_client.bound_port}/webhook"
        http_in = HttpClient("http_in", router, listen_host="127.0.0.1", listen_port=0)
        wh_out = WebhookClient("wh_out", router, url=egress_url)

        router.register_client(http_in)
        router.register_client(wh_out)
        router.add_route("http_in", "sensors/+", "wh_out", "out/telemetry")

        http_in.subscribe("sensors/+")
        http_in.connect()
        wh_out.connect()

        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{http_in.bound_port}/sensors/temp",
                data=b'{"c": 22.4}',
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=3.0) as resp:
                self.assertEqual(resp.status, 200)
                body = json.loads(resp.read().decode("utf-8"))
                self.assertEqual(body["status"], "routed")
                self.assertEqual(body["topic"], "sensors/temp")

            self.assertTrue(received_Event.wait(timeout=3.0))
            self.assertEqual(delivered, [("out/telemetry", b'{"c": 22.4}')])
        finally:
            http_in.disconnect()
            wh_out.disconnect()
            sink_client.disconnect()

    def test_bearer_auth_and_nack_503_and_header_topic(self):
        class _AuthRouter:
            def __init__(self):
                self.should_nack = False
                self.should_raise = False
                self.received = []

            def route(self, broker_name, topic, message, ack_fn=None, nack_fn=None, **kwargs):
                if self.should_raise:
                    raise RuntimeError("boom")
                self.received.append((topic, message))
                if self.should_nack and nack_fn:
                    nack_fn(False)
                elif ack_fn:
                    ack_fn()

        r = _AuthRouter()
        client = HttpClient(
            "http_sec",
            r,
            listen_host="127.0.0.1",
            listen_port=0,
            bearer_token="secret-token-99",
        )
        client.subscribe("orders/#")
        client.connect()

        try:
            url = f"http://127.0.0.1:{client.bound_port}/fallback/path"

            # 1. Missing Authorization header -> 401
            req_unauth = urllib.request.Request(url, data=b"x", method="POST")
            with self.assertRaises(urllib.error.HTTPError) as ctx:
                urllib.request.urlopen(req_unauth, timeout=2.0)
            self.assertEqual(ctx.exception.code, 401)
            ctx.exception.close()

            # 2. Invalid bearer token -> 401
            req_bad = urllib.request.Request(
                url,
                data=b"x",
                headers={"Authorization": "Bearer wrong-token"},
                method="POST",
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx2:
                urllib.request.urlopen(req_bad, timeout=2.0)
            self.assertEqual(ctx2.exception.code, 401)
            ctx2.exception.close()

            # 3. Valid bearer token + X-PostOffice-Topic header -> 200
            req_ok = urllib.request.Request(
                url,
                data=b"order-1",
                headers={
                    "Authorization": "Bearer secret-token-99",
                    "X-PostOffice-Topic": "orders/eu",
                },
                method="POST",
            )
            with urllib.request.urlopen(req_ok, timeout=2.0) as resp:
                self.assertEqual(resp.status, 200)
            self.assertEqual(r.received, [("orders/eu", b"order-1")])

            # 4. Nack triggers 503 Service Unavailable
            r.should_nack = True
            req_nack = urllib.request.Request(
                url,
                data=b"order-2",
                headers={"Authorization": "Bearer secret-token-99"},
                method="POST",
            )
            with self.assertRaises(urllib.error.HTTPError) as ctx3:
                urllib.request.urlopen(req_nack, timeout=2.0)
            self.assertEqual(ctx3.exception.code, 503)
            ctx3.exception.close()

            # 5. Unhandled routing exception triggers 503
            r.should_raise = True
            with self.assertRaises(urllib.error.HTTPError) as ctx4:
                urllib.request.urlopen(req_nack, timeout=2.0)
            self.assertEqual(ctx4.exception.code, 503)
            ctx4.exception.close()
        finally:
            client.disconnect()

    def test_offline_publish_buffering_and_reload_security(self):
        received = []
        done = threading.Event()

        class _SinkRouter:
            def route(self, broker_name, topic, message, ack_fn=None, nack_fn=None, **kwargs):
                received.append((topic, message))
                if ack_fn:
                    ack_fn()
                done.set()

        sink = HttpClient(
            "sink",
            _SinkRouter(),
            listen_host="127.0.0.1",
            listen_port=0,
            security_config={"username": "admin", "password": "pw1"},
        )
        sink.subscribe("events/#")
        sink.connect()

        try:
            target_url = f"http://127.0.0.1:{sink.bound_port}/events"
            # Disconnected publisher with buffering disabled -> calls on_error
            no_buf = HttpClient("no_buf", Router(), url=target_url, enable_publish_buffer=False)
            errors = []
            no_buf.publish("events/a", b"drop", on_error=errors.append)
            self.assertEqual(len(errors), 1)

            # Disconnected publisher with buffering enabled -> buffers and flushes on connect
            pub = HttpClient(
                "pub",
                Router(),
                url=target_url,
                enable_publish_buffer=True,
                verify_tls=False,
                security_config={"username": "admin", "password": "pw1"},
            )
            confirmed = []
            pub.publish("events/buffered", b"payload-buffered", on_confirm=lambda: confirmed.append(True))
            self.assertEqual(pub.publish_buffer_size, 1)

            # Reload security preserves subscriptions & buffer
            pub.subscribe("local/sub")
            pub.reload_security(SecurityConfig(tls_enabled=False, username="admin", password="pw1"))
            self.assertEqual(pub.publish_buffer_size, 1)
            self.assertIn("local/sub", pub.get_subscriptions())

            pub.connect()
            try:
                self.assertTrue(done.wait(timeout=3.0))
                self.assertEqual(pub.publish_buffer_size, 0)
                self.assertEqual(len(confirmed), 1)
                self.assertEqual(received, [("events/buffered", b"payload-buffered")])
            finally:
                pub.disconnect()
        finally:
            sink.disconnect()


class TestWebSocketClientAndServer(unittest.TestCase):
    def test_frame_encoding_and_envelopes(self):
        # Test small, 16-bit (126..65535), and 64-bit (>=65536) frame encodings
        self.assertEqual(
            compute_accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
        )
        env = encode_ws_envelope("t/1", b"\x00\x01\x02")
        self.assertEqual(decode_ws_envelope(env), ("t/1", b"\x00\x01\x02"))
        self.assertEqual(decode_ws_envelope(b'{"topic": "t/2", "payload": "hi"}'), ("t/2", b"hi"))
        self.assertEqual(decode_ws_envelope(b"raw/topic:hello"), ("raw/topic", b"hello"))
        self.assertEqual(decode_ws_envelope(b"plain_message"), ("default", b"plain_message"))
        self.assertEqual(decode_ws_envelope(b"\xff\xfe\xfd"), ("default", b"\xff\xfe\xfd"))

        med = b"A" * 300
        frame_med = encode_ws_frame(med, opcode=OPCODE_BINARY, mask=False)
        self.assertTrue(len(frame_med) > 300)

        large = b"B" * 66000
        frame_large = encode_ws_frame(large, opcode=OPCODE_BINARY, mask=False)
        self.assertTrue(len(frame_large) > 66000)

    def test_websocket_end_to_end_publish_and_subscribe_routing(self):
        server_received = threading.Event()
        client_routed = threading.Event()
        routed_messages = []

        def on_srv_msg(topic: str, data: bytes):
            server_received.set()

        srv = WebSocketServer(host="127.0.0.1", port=0, on_message=on_srv_msg, bearer_token="ws-tok")
        port = srv.start()

        class _CapturingRouter:
            def route(self, broker_name, topic, message, ack_fn=None, nack_fn=None, **kwargs):
                routed_messages.append((broker_name, topic, message))
                client_routed.set()

        router = _CapturingRouter()

        try:
            # 1. Invalid bearer token fails handshake
            bad_ws = WebSocketClient(
                "bad_ws",
                router,
                host="127.0.0.1",
                port=port,
                bearer_token="wrong-tok",
            )
            with self.assertRaises(ConnectionError):
                bad_ws.connect()

            # 2. Offline buffering before connect()
            ws = WebSocketClient(
                "ws_main",
                router,
                host="127.0.0.1",
                port=port,
                path="/ws",
                bearer_token="ws-tok",
                enable_publish_buffer=True,
            )
            ws.subscribe("commands/#")
            confirmed = []
            ws.publish("telemetry/cpu", b"usage=42", on_confirm=lambda: confirmed.append(True))
            self.assertEqual(ws.publish_buffer_size, 1)

            # Connect flushes buffered publish to WebSocketServer
            ws.connect()
            self.assertTrue(server_received.wait(timeout=3.0))
            self.assertEqual(len(confirmed), 1)
            self.assertEqual(srv.received_messages, [("telemetry/cpu", b"usage=42")])

            # Server sends PING -> client responds with PONG automatically
            srv.send_raw_frame(encode_ws_frame(b"ping-data", opcode=OPCODE_PING, mask=False))

            # Server broadcasts a message -> client receives and routes to Router
            srv.broadcast("commands/reboot", b'{"delay": 5}')
            self.assertTrue(client_routed.wait(timeout=3.0))
            self.assertEqual(routed_messages, [("ws_main", "commands/reboot", b'{"delay": 5}')])

            # Reload security while connected reconnects cleanly
            ws.reload_security(SecurityConfig(tls_enabled=False))
            self.assertTrue(ws._connected)
            self.assertIn("commands/#", ws.get_subscriptions())

            ws.disconnect()
        finally:
            srv.stop()

    def test_websocket_publish_disconnected_without_buffer(self):
        ws = WebSocketClient("ws_nobuf", Router(), enable_publish_buffer=False)
        errs = []
        ws.publish("t/1", b"msg", on_error=errs.append)
        self.assertEqual(len(errs), 1)

    def test_http_edge_cases_and_tls_config(self):
        with patch("ssl.create_default_context") as mock_ctx_fn:
            mock_ctx = MagicMock()
            mock_ctx_fn.return_value = mock_ctx
            c = HttpClient(
                "http_tls",
                Router(),
                bearer_token="tok-a",
                security_config={
                    "tls_enabled": True,
                    "ca_cert": "/tmp/ca.pem",
                    "client_cert": "/tmp/cert.pem",
                    "client_key": "/tmp/key.pem",
                    "insecure_skip_verify": True,
                },
            )
            mock_ctx.load_verify_locations.assert_called_once_with(cafile="/tmp/ca.pem")
            mock_ctx.load_cert_chain.assert_called_once()
            self.assertEqual(c._outbound_auth_header(), "Bearer tok-a")

        # Password-only security config -> Bearer <password>
        c_pw = HttpClient("http_pw", Router(), security_config={"password": "sec-pw"})
        self.assertEqual(c_pw._outbound_auth_header(), "Bearer sec-pw")
        self.assertTrue(c_pw._check_auth("Bearer sec-pw"))

        # Connect first, then subscribe starts server; root path "/" maps to "default" topic
        routed = []

        class _SimpleRouter:
            def route(self, broker_name, topic, message, ack_fn=None, nack_fn=None, **kwargs):
                routed.append((topic, message))

        c_late = HttpClient("http_late", _SimpleRouter(), listen_host="127.0.0.1", listen_port=0)
        c_late.connect()
        c_late.subscribe("default")
        c_late.subscribe("default")  # idempotent duplicate subscribe
        c_late._ensure_server_started()  # idempotent when already running
        try:
            req = urllib.request.Request(
                f"http://127.0.0.1:{c_late.bound_port}/",
                data=b"root-body",
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=2.0) as resp:
                self.assertEqual(resp.status, 200)
            self.assertEqual(routed, [("default", b"root-body")])

            # Publish failure invokes on_error
            pub_errs = []
            c_late.url = "http://127.0.0.1:1/unreachable"
            c_late.timeout = 0.2
            c_late.publish("t/fail", b"data", on_error=pub_errs.append)
            self.assertEqual(len(pub_errs), 1)
        finally:
            c_late.disconnect()

    def test_websocket_protocol_edge_cases(self):
        # 1. Decode broken JSON starting with '{' falls back to ':' or 'default'
        self.assertEqual(decode_ws_envelope(b"{broken:json"), ("{broken", b"json"))
        self.assertEqual(decode_ws_envelope(b"{broken_no_colon"), ("default", b"{broken_no_colon"))

        # 2. Read frames of 16-bit (126) and 64-bit (127) lengths over socketpair
        s1, s2 = socket.socketpair()
        try:
            med_payload = b"M" * 300
            large_payload = b"L" * 66000
            s1.sendall(encode_ws_frame(med_payload, opcode=OPCODE_BINARY, mask=True, masking_key=b"\x01\x02\x03\x04"))
            op1, pay1, masked1 = read_ws_frame(s2)
            self.assertEqual((op1, pay1, masked1), (OPCODE_BINARY, med_payload, True))

            t = threading.Thread(
                target=lambda: s1.sendall(encode_ws_frame(large_payload, opcode=OPCODE_BINARY, mask=False)),
                daemon=True,
            )
            t.start()
            op2, pay2, masked2 = read_ws_frame(s2)
            t.join(timeout=2.0)
            self.assertEqual((op2, pay2, masked2), (OPCODE_BINARY, large_payload, False))

            # PrefixedSocket drains prefix then reads from socket
            pref = _PrefixedSocket(s2, b"abc")
            self.assertEqual(pref.recv(2), b"ab")
            self.assertEqual(pref.recv(2), b"c")
            s1.sendall(b"xyz")
            self.assertEqual(pref.recv(3), b"xyz")

            # Closed socket raises ConnectionError
            s1.close()
            with self.assertRaises(ConnectionError):
                read_ws_frame(s2)
        finally:
            s1.close()
            s2.close()

        # 3. TLS context + auth header variants on WebSocketClient
        with patch("ssl.create_default_context") as mock_ctx_fn:
            mock_ctx = MagicMock()
            mock_ctx_fn.return_value = mock_ctx
            ws_tls = WebSocketClient(
                "ws_tls",
                Router(),
                path="custom_no_slash",
                security_config={
                    "tls_enabled": True,
                    "ca_cert": "/tmp/ca.pem",
                    "client_cert": "/tmp/cert.pem",
                    "client_key": "/tmp/key.pem",
                    "insecure_skip_verify": True,
                    "username": "u",
                    "password": "p",
                },
            )
            self.assertEqual(ws_tls.path, "/custom_no_slash")
            self.assertTrue(ws_tls._outbound_auth_header().startswith("Basic "))

        ws_pw = WebSocketClient("ws_pw", Router(), security_config={"password": "pw-only"})
        self.assertEqual(ws_pw._outbound_auth_header(), "Bearer pw-only")

        # 4. Server missing Sec-WebSocket-Key returns 400; client ping -> server pong; publish error handling
        srv = WebSocketServer(host="127.0.0.1", port=0)
        port = srv.start()
        try:
            # Missing Sec-WebSocket-Key -> 400 Bad Request
            raw = socket.create_connection(("127.0.0.1", port), timeout=2.0)
            raw.sendall(b"GET /ws HTTP/1.1\r\nHost: localhost\r\n\r\n")
            resp = raw.recv(1024)
            self.assertIn(b"400 Bad Request", resp)
            raw.close()

            # Client sends PING -> server replies PONG; client router raising error is caught
            class _ErrorRouter:
                def route(self, *args, **kwargs):
                    raise RuntimeError("router failure")

            ws_c = WebSocketClient("ws_c", _ErrorRouter(), host="127.0.0.1", port=port)
            ws_c.connect()
            ws_c._sock.sendall(encode_ws_frame(b"hello-ping", opcode=OPCODE_PING, mask=True))
            srv.broadcast("err/topic", b"trigger-error")
            time.sleep(0.05)

            # Simulate broken socket on publish -> invokes on_error
            pub_errors = []
            ws_c._sock.close()
            ws_c.publish("t/broken", b"payload", on_error=pub_errors.append)
            self.assertEqual(len(pub_errors), 1)
            ws_c.disconnect()
        finally:
            srv.stop()



if __name__ == "__main__":
    unittest.main()
