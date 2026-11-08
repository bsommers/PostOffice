import base64
import hmac
import http.server
import json
import logging
import ssl
import threading
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional, Union

from postoffice.base_client import BaseClient
from postoffice.registry import ClientRegistry
from postoffice.security import SecurityConfig, ensure_security_config

logger = logging.getLogger(__name__)


@ClientRegistry.register("http")
class HttpClient(BaseClient):
    """
    HTTP/Webhook protocol adapter supporting both HTTP POST egress publishing
    and an embedded ThreadingHTTPServer ingress listener for webhook subscriptions.
    """

    def __init__(
        self,
        name: str,
        router: Any,
        url: str = "http://localhost:8080/webhook",
        listen_host: str = "127.0.0.1",
        listen_port: int = 0,
        timeout: float = 5.0,
        bearer_token: Optional[str] = None,
        verify_tls: bool = True,
        security_config: Optional[Union[SecurityConfig, Dict[str, Any]]] = None,
        **kwargs
    ):
        super().__init__(name, router, **kwargs)
        self.url = url
        self.listen_host = listen_host
        self.listen_port = listen_port
        self.bound_port: int = listen_port
        self.timeout = timeout
        self.bearer_token = bearer_token
        self.verify_tls = verify_tls
        self.security = ensure_security_config(security_config)
        self.subscribed_topics: List[str] = []
        self._connected: bool = False
        self._server: Optional[http.server.ThreadingHTTPServer] = None
        self._server_thread: Optional[threading.Thread] = None
        self._server_lock = threading.Lock()
        self._ssl_context = self._build_ssl_context()

    def _build_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self.security.tls_enabled and self.verify_tls:
            return None
        ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        if self.security.ca_cert:
            ctx.load_verify_locations(cafile=self.security.ca_cert)
        if self.security.client_cert and self.security.client_key:
            ctx.load_cert_chain(
                certfile=self.security.client_cert,
                keyfile=self.security.client_key,
                password=self.security.client_key_password,
            )
        if self.security.insecure_skip_verify or not self.verify_tls:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _expected_auth_headers(self) -> List[str]:
        expected: List[str] = []
        if self.bearer_token:
            expected.append(f"Bearer {self.bearer_token}")
        if self.security.username and self.security.password is not None:
            raw = f"{self.security.username}:{self.security.password}".encode("utf-8")
            expected.append(f"Basic {base64.b64encode(raw).decode('ascii')}")
        elif self.security.password:
            expected.append(f"Bearer {self.security.password}")
        return expected

    def _check_auth(self, auth_header: Optional[str]) -> bool:
        expected = self._expected_auth_headers()
        if not expected:
            return True
        if not auth_header:
            return False
        return any(hmac.compare_digest(auth_header.strip(), exp) for exp in expected)

    def _outbound_auth_header(self) -> Optional[str]:
        if self.bearer_token:
            return f"Bearer {self.bearer_token}"
        if self.security.username and self.security.password is not None:
            raw = f"{self.security.username}:{self.security.password}".encode("utf-8")
            return f"Basic {base64.b64encode(raw).decode('ascii')}"
        if self.security.password:
            return f"Bearer {self.security.password}"
        return None

    def _ensure_server_started(self) -> None:
        with self._server_lock:
            if self._server is not None:
                return
            client_ref = self

            class _WebhookHandler(http.server.BaseHTTPRequestHandler):
                def do_POST(self):
                    if not client_ref._check_auth(self.headers.get("Authorization")):
                        self._send_json(401, {"error": "unauthorized"})
                        return

                    length = int(self.headers.get("Content-Length", "0") or 0)
                    body = self.rfile.read(length) if length > 0 else b""

                    topic = self.headers.get("X-PostOffice-Topic")
                    if not topic:
                        topic = self.path.split("?", 1)[0].lstrip("/")
                    if not topic:
                        topic = "default"

                    outcome = {"nacked": False}

                    def ack_fn():
                        outcome["nacked"] = False

                    def nack_fn(requeue: bool = False):
                        outcome["nacked"] = True

                    try:
                        client_ref.on_message(topic, body, ack_fn=ack_fn, nack_fn=nack_fn)
                    except Exception as exc:
                        logger.error(f"{client_ref.name}: Error routing HTTP ingress on {topic}: {exc}")
                        self._send_json(503, {"error": "routing_error"})
                        return

                    if outcome["nacked"]:
                        self._send_json(503, {"error": "nacked"})
                    else:
                        self._send_json(200, {"status": "routed", "topic": topic})

                def _send_json(self, status_code: int, payload: Dict[str, Any]) -> None:
                    raw = json.dumps(payload).encode("utf-8")
                    self.send_response(status_code)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(raw)))
                    self.end_headers()
                    self.wfile.write(raw)

                def log_message(self, format: str, *args: Any) -> None:
                    logger.debug("%s - %s", client_ref.name, format % args)

            server = http.server.ThreadingHTTPServer((self.listen_host, self.listen_port), _WebhookHandler)
            self._server = server
            self.bound_port = server.server_port
            thread = threading.Thread(
                target=server.serve_forever,
                name=f"postoffice-http-{self.name}",
                daemon=True,
            )
            self._server_thread = thread
            thread.start()

    def connect(self) -> None:
        self._connected = True
        if self.listen_port > 0 or self.subscribed_topics or self.get_subscriptions():
            self._ensure_server_started()
        self.flush_publish_buffer()

    def disconnect(self) -> None:
        self._connected = False
        with self._server_lock:
            server = self._server
            self._server = None
            self._server_thread = None
        if server is not None:
            try:
                server.shutdown()
            finally:
                server.server_close()

    def subscribe(self, topic: str, **kwargs) -> None:
        self.record_subscription(topic, **kwargs)
        if topic not in self.subscribed_topics:
            self.subscribed_topics.append(topic)
        if self._connected:
            self._ensure_server_started()

    def publish(
        self,
        topic: str,
        message: bytes,
        on_confirm: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        **kwargs
    ) -> None:
        if not self._connected:
            if self.buffer_publish(topic, message, on_confirm=on_confirm, on_error=on_error, **kwargs):
                logger.info(f"{self.name}: Client not connected. Message to {topic} buffered for publish on connect.")
                return
            err = RuntimeError(f"{self.name}: HTTP client is not connected.")
            logger.error(str(err))
            if on_error:
                on_error(err)
            return

        target_url = kwargs.get("url", self.url)
        content_type = kwargs.get("content_type", "application/octet-stream")
        headers = {
            "Content-Type": content_type,
            "X-PostOffice-Topic": topic,
        }
        auth_val = self._outbound_auth_header()
        if auth_val:
            headers["Authorization"] = auth_val

        payload = message if isinstance(message, (bytes, bytearray)) else str(message).encode("utf-8")
        req = urllib.request.Request(target_url, data=bytes(payload), headers=headers, method="POST")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self._ssl_context) as resp:
                status = getattr(resp, "status", 200)
                if 200 <= status < 300:
                    if on_confirm:
                        on_confirm()
                else:
                    err = RuntimeError(f"{self.name}: Unexpected HTTP status {status} for {topic}")
                    if on_error:
                        on_error(err)
        except Exception as exc:
            logger.error(f"{self.name}: HTTP publish error on {topic}: {exc}")
            if on_error:
                on_error(exc)

    def reload_security(self, security_config: Optional[Union[SecurityConfig, Dict[str, Any]]]) -> None:
        self.security = ensure_security_config(security_config)
        self._ssl_context = self._build_ssl_context()
        logger.info(f"{self.name}: Reloaded HTTP security configuration (tls_enabled={self.security.tls_enabled})")


@ClientRegistry.register("webhook")
class WebhookClient(HttpClient):
    """Alias adapter registered under protocol name 'webhook'."""
    pass
