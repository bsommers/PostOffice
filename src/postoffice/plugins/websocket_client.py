import base64
import hashlib
import hmac
import json
import logging
import os
import socket
import ssl
import struct
import threading
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from postoffice.base_client import BaseClient
from postoffice.registry import ClientRegistry
from postoffice.security import SecurityConfig, ensure_security_config

logger = logging.getLogger(__name__)

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

OPCODE_CONTINUATION = 0x0
OPCODE_TEXT = 0x1
OPCODE_BINARY = 0x2
OPCODE_CLOSE = 0x8
OPCODE_PING = 0x9
OPCODE_PONG = 0xA


def compute_accept_key(sec_websocket_key: str) -> str:
    digest = hashlib.sha1((sec_websocket_key.strip() + WS_GUID).encode("ascii")).digest()
    return base64.b64encode(digest).decode("ascii")


def encode_ws_frame(
    payload: bytes,
    opcode: int = OPCODE_TEXT,
    mask: bool = True,
    masking_key: Optional[bytes] = None,
) -> bytes:
    header = bytearray()
    header.append(0x80 | (opcode & 0x0F))
    mask_bit = 0x80 if mask else 0x00
    length = len(payload)
    if length < 126:
        header.append(mask_bit | length)
    elif length <= 0xFFFF:
        header.append(mask_bit | 126)
        header.extend(struct.pack("!H", length))
    else:
        header.append(mask_bit | 127)
        header.extend(struct.pack("!Q", length))

    if mask:
        mkey = masking_key if (masking_key is not None and len(masking_key) == 4) else os.urandom(4)
        header.extend(mkey)
        masked_payload = bytes(b ^ mkey[i & 3] for i, b in enumerate(payload))
        return bytes(header) + masked_payload
    return bytes(header) + payload


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("WebSocket connection closed while reading frame")
        buf.extend(chunk)
    return bytes(buf)


def read_ws_frame(sock: socket.socket) -> Tuple[int, bytes, bool]:
    first_two = _recv_exact(sock, 2)
    b1, b2 = first_two[0], first_two[1]
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", _recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", _recv_exact(sock, 8))[0]

    masking_key = _recv_exact(sock, 4) if masked else b""
    payload = _recv_exact(sock, length) if length > 0 else b""
    if masked and payload:
        payload = bytes(b ^ masking_key[i & 3] for i, b in enumerate(payload))
    return opcode, payload, masked


def encode_ws_envelope(topic: str, message: bytes) -> bytes:
    doc = {
        "topic": topic,
        "payload_b64": base64.b64encode(message).decode("ascii"),
    }
    return json.dumps(doc).encode("utf-8")


def decode_ws_envelope(raw: bytes) -> Tuple[str, bytes]:
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError:
        return ("default", raw)

    if text.startswith("{"):
        try:
            doc = json.loads(text)
            if isinstance(doc, dict) and "topic" in doc:
                topic = str(doc["topic"])
                if "payload_b64" in doc:
                    return (topic, base64.b64decode(doc["payload_b64"]))
                if "payload" in doc:
                    val = doc["payload"]
                    return (topic, val.encode("utf-8") if isinstance(val, str) else bytes(val))
        except Exception:
            pass

    if ":" in text:
        topic, rest = text.split(":", 1)
        return (topic, rest.encode("utf-8"))
    return ("default", raw)


class WebSocketServer:
    """
    Lightweight standard-library RFC 6455 WebSocket server for local broker testing
    and embedded WebSocket listener usage.
    """

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 0,
        on_message: Optional[Callable[[str, bytes], None]] = None,
        bearer_token: Optional[str] = None,
        ssl_context: Optional[ssl.SSLContext] = None,
    ):
        self.host = host
        self.port = port
        self.bound_port: int = port
        self.on_message = on_message
        self.bearer_token = bearer_token
        self.ssl_context = ssl_context
        self.received_messages: List[Tuple[str, bytes]] = []
        self._server_sock: Optional[socket.socket] = None
        self._clients: List[socket.socket] = []
        self._lock = threading.Lock()
        self._running = False
        self._accept_thread: Optional[threading.Thread] = None

    def start(self) -> int:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.host, self.port))
        sock.listen(16)
        self.bound_port = sock.getsockname()[1]
        self._server_sock = sock
        self._running = True
        self._accept_thread = threading.Thread(
            target=self._accept_loop,
            name=f"ws-server-{self.bound_port}",
            daemon=True,
        )
        self._accept_thread.start()
        return self.bound_port

    def stop(self) -> None:
        self._running = False
        if self._server_sock is not None:
            try:
                self._server_sock.close()
            except OSError:
                pass
            self._server_sock = None
        with self._lock:
            clients = list(self._clients)
            self._clients.clear()
        for c in clients:
            try:
                c.close()
            except OSError:
                pass

    def _accept_loop(self) -> None:
        while self._running and self._server_sock is not None:
            try:
                conn, _ = self._server_sock.accept()
            except OSError:
                break
            if self.ssl_context is not None:
                try:
                    conn = self.ssl_context.wrap_socket(conn, server_side=True)
                except Exception:
                    conn.close()
                    continue
            t = threading.Thread(target=self._handle_client, args=(conn,), daemon=True)
            t.start()

    def _handle_client(self, conn: socket.socket) -> None:
        try:
            req_data = b""
            while b"\r\n\r\n" not in req_data:
                chunk = conn.recv(4096)
                if not chunk:
                    conn.close()
                    return
                req_data += chunk

            header_text = req_data.split(b"\r\n\r\n", 1)[0].decode("latin-1")
            lines = header_text.split("\r\n")
            headers: Dict[str, str] = {}
            for line in lines[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            if self.bearer_token:
                auth = headers.get("authorization", "")
                expected = f"Bearer {self.bearer_token}"
                if not hmac.compare_digest(auth, expected):
                    conn.sendall(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")
                    conn.close()
                    return

            ws_key = headers.get("sec-websocket-key")
            if not ws_key:
                conn.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
                conn.close()
                return

            accept_val = compute_accept_key(ws_key)
            resp = (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\n"
                "Connection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept_val}\r\n\r\n"
            ).encode("ascii")
            conn.sendall(resp)

            with self._lock:
                self._clients.append(conn)

            while self._running:
                opcode, payload, _ = read_ws_frame(conn)
                if opcode == OPCODE_CLOSE:
                    try:
                        conn.sendall(encode_ws_frame(b"", opcode=OPCODE_CLOSE, mask=False))
                    except OSError:
                        pass
                    break
                if opcode == OPCODE_PING:
                    conn.sendall(encode_ws_frame(payload, opcode=OPCODE_PONG, mask=False))
                    continue
                if opcode in (OPCODE_TEXT, OPCODE_BINARY):
                    topic, msg_bytes = decode_ws_envelope(payload)
                    with self._lock:
                        self.received_messages.append((topic, msg_bytes))
                    if self.on_message is not None:
                        self.on_message(topic, msg_bytes)
        except Exception:
            pass
        finally:
            with self._lock:
                if conn in self._clients:
                    self._clients.remove(conn)
            try:
                conn.close()
            except OSError:
                pass

    def broadcast(self, topic: str, message: bytes) -> None:
        frame = encode_ws_frame(encode_ws_envelope(topic, message), opcode=OPCODE_TEXT, mask=False)
        self.send_raw_frame(frame)

    def send_raw_frame(self, frame_bytes: bytes) -> None:
        with self._lock:
            clients = list(self._clients)
        for c in clients:
            try:
                c.sendall(frame_bytes)
            except OSError:
                pass


@ClientRegistry.register("websocket")
class WebSocketClient(BaseClient):
    """
    RFC 6455 WebSocket protocol adapter using Python standard library sockets and SSL.
    """

    def __init__(
        self,
        name: str,
        router: Any,
        host: str = "127.0.0.1",
        port: int = 8765,
        path: str = "/ws",
        timeout: float = 5.0,
        bearer_token: Optional[str] = None,
        security_config: Optional[Union[SecurityConfig, Dict[str, Any]]] = None,
        **kwargs
    ):
        super().__init__(name, router, **kwargs)
        self.host = host
        self.port = port
        self.path = path if path.startswith("/") else f"/{path}"
        self.timeout = timeout
        self.bearer_token = bearer_token
        self.security = ensure_security_config(security_config)
        self.subscribed_topics: List[str] = []
        self._sock: Optional[socket.socket] = None
        self._connected: bool = False
        self._write_lock = threading.Lock()
        self._reader_thread: Optional[threading.Thread] = None
        self._ssl_context = self._build_ssl_context()

    def _build_ssl_context(self) -> Optional[ssl.SSLContext]:
        if not self.security.tls_enabled:
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
        if self.security.insecure_skip_verify:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        return ctx

    def _outbound_auth_header(self) -> Optional[str]:
        if self.bearer_token:
            return f"Bearer {self.bearer_token}"
        if self.security.username and self.security.password is not None:
            raw = f"{self.security.username}:{self.security.password}".encode("utf-8")
            return f"Basic {base64.b64encode(raw).decode('ascii')}"
        if self.security.password:
            return f"Bearer {self.security.password}"
        return None

    def connect(self) -> None:
        raw_sock = socket.create_connection((self.host, self.port), timeout=self.timeout)
        if self._ssl_context is not None:
            sock = self._ssl_context.wrap_socket(raw_sock, server_hostname=self.host)
        else:
            sock = raw_sock

        ws_key = base64.b64encode(os.urandom(16)).decode("ascii")
        lines = [
            f"GET {self.path} HTTP/1.1",
            f"Host: {self.host}:{self.port}",
            "Upgrade: websocket",
            "Connection: Upgrade",
            f"Sec-WebSocket-Key: {ws_key}",
            "Sec-WebSocket-Version: 13",
        ]
        auth_val = self._outbound_auth_header()
        if auth_val:
            lines.append(f"Authorization: {auth_val}")
        handshake_req = ("\r\n".join(lines) + "\r\n\r\n").encode("ascii")
        sock.sendall(handshake_req)

        resp_data = b""
        while b"\r\n\r\n" not in resp_data:
            chunk = sock.recv(4096)
            if not chunk:
                sock.close()
                raise ConnectionError(f"{self.name}: WebSocket handshake closed prematurely")
            resp_data += chunk

        header_block, leftover = resp_data.split(b"\r\n\r\n", 1)
        header_lines = header_block.decode("latin-1").split("\r\n")
        status_line = header_lines[0]
        if " 101 " not in status_line:
            sock.close()
            raise ConnectionError(f"{self.name}: WebSocket handshake rejected: {status_line}")

        resp_headers: Dict[str, str] = {}
        for line in header_lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                resp_headers[k.strip().lower()] = v.strip()

        expected_accept = compute_accept_key(ws_key)
        actual_accept = resp_headers.get("sec-websocket-accept", "")
        if actual_accept != expected_accept:
            sock.close()
            raise ConnectionError(f"{self.name}: Invalid Sec-WebSocket-Accept in handshake")

        sock.settimeout(None)
        self._sock = sock
        self._connected = True
        self._reader_thread = threading.Thread(
            target=self._read_loop,
            args=(sock, leftover),
            name=f"postoffice-ws-{self.name}",
            daemon=True,
        )
        self._reader_thread.start()
        self.flush_publish_buffer()

    def _read_loop(self, sock: socket.socket, initial_bytes: bytes = b"") -> None:
        # Wrap socket if leftover bytes were read after handshake header
        reader_sock = _PrefixedSocket(sock, initial_bytes) if initial_bytes else sock
        while self._connected and self._sock is sock:
            try:
                opcode, payload, _ = read_ws_frame(reader_sock)
            except Exception:
                break

            if opcode == OPCODE_CLOSE:
                break
            if opcode == OPCODE_PING:
                try:
                    with self._write_lock:
                        sock.sendall(encode_ws_frame(payload, opcode=OPCODE_PONG, mask=True))
                except OSError:
                    break
                continue
            if opcode in (OPCODE_TEXT, OPCODE_BINARY):
                topic, msg_bytes = decode_ws_envelope(payload)
                try:
                    self.on_message(topic, msg_bytes)
                except Exception as exc:
                    logger.error(f"{self.name}: Error routing WebSocket message on {topic}: {exc}")

        if self._sock is sock:
            self._connected = False

    def disconnect(self) -> None:
        self._connected = False
        sock = self._sock
        self._sock = None
        if sock is not None:
            try:
                with self._write_lock:
                    sock.sendall(encode_ws_frame(b"", opcode=OPCODE_CLOSE, mask=True))
            except OSError:
                pass
            try:
                sock.close()
            except OSError:
                pass

    def subscribe(self, topic: str, **kwargs) -> None:
        self.record_subscription(topic, **kwargs)
        if topic not in self.subscribed_topics:
            self.subscribed_topics.append(topic)

    def publish(
        self,
        topic: str,
        message: bytes,
        on_confirm: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        **kwargs
    ) -> None:
        if not self._connected or self._sock is None:
            if self.buffer_publish(topic, message, on_confirm=on_confirm, on_error=on_error, **kwargs):
                logger.info(f"{self.name}: WebSocket not connected. Message to {topic} buffered.")
                return
            err = RuntimeError(f"{self.name}: WebSocket client is not connected.")
            logger.error(str(err))
            if on_error:
                on_error(err)
            return

        payload = message if isinstance(message, (bytes, bytearray)) else str(message).encode("utf-8")
        envelope = encode_ws_envelope(topic, bytes(payload))
        frame = encode_ws_frame(envelope, opcode=OPCODE_TEXT, mask=True)
        try:
            with self._write_lock:
                self._sock.sendall(frame)
            if on_confirm:
                on_confirm()
        except Exception as exc:
            logger.error(f"{self.name}: WebSocket publish error on {topic}: {exc}")
            if on_error:
                on_error(exc)

    def reload_security(self, security_config: Optional[Union[SecurityConfig, Dict[str, Any]]]) -> None:
        was_connected = self._connected
        if was_connected:
            self.disconnect()
        self.security = ensure_security_config(security_config)
        self._ssl_context = self._build_ssl_context()
        logger.info(f"{self.name}: Reloaded WebSocket security configuration (tls_enabled={self.security.tls_enabled})")
        if was_connected:
            self.connect()


class _PrefixedSocket:
    """Helper socket wrapper that drains initial buffered bytes before reading from the underlying socket."""

    def __init__(self, sock: socket.socket, prefix: bytes):
        self._sock = sock
        self._buf = bytearray(prefix)

    def recv(self, n: int) -> bytes:
        if self._buf:
            take = min(n, len(self._buf))
            out = bytes(self._buf[:take])
            del self._buf[:take]
            return out
        return self._sock.recv(n)
