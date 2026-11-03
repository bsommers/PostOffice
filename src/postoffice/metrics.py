import logging
from typing import Callable, Optional, Tuple
import threading
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest, make_wsgi_app

logger = logging.getLogger(__name__)


class _QuietWSGIHandler(WSGIRequestHandler):
    def log_message(self, format: str, *args) -> None:
        pass


class _ReuseAddrWSGIServer(WSGIServer):
    allow_reuse_address = True


def start_http_server(
    port: int,
    addr: str = "127.0.0.1",
    registry: Optional[CollectorRegistry] = None,
) -> Tuple[WSGIServer, threading.Thread]:
    """
    Starts a background WSGI HTTP server serving `/metrics`, `/healthz`, and `/readyz`
    with exact operational parity between Python and Native workers.
    """
    prom_app = make_wsgi_app(registry) if registry is not None else make_wsgi_app()

    def _app(environ, start_response):
        path = environ.get("PATH_INFO", "/metrics")
        if path.startswith("/healthz"):
            body = b'{"status":"ok","version":"1.0.0"}\n'
            start_response(
                "200 OK",
                [
                    ("Content-Type", "application/json"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]
        if path.startswith("/readyz"):
            probe = getattr(registry, "_postoffice_readiness_fn", None)
            ready, applied_version = probe() if callable(probe) else (True, 0)
            status = "200 OK" if ready else "503 Service Unavailable"
            ready_str = "true" if ready else "false"
            body = f'{{"ready":{ready_str},"applied_version":{int(applied_version)}}}\n'.encode(
                "utf-8"
            )
            start_response(
                status,
                [
                    ("Content-Type", "application/json"),
                    ("Content-Length", str(len(body))),
                ],
            )
            return [body]
        return prom_app(environ, start_response)

    httpd = make_server(
        addr,
        port,
        _app,
        server_class=_ReuseAddrWSGIServer,
        handler_class=_QuietWSGIHandler,
    )
    thread = threading.Thread(target=httpd.serve_forever, daemon=True)
    thread.start()
    return httpd, thread

class LabelGuard:
    """
    Bounds the distinct values of a metric label. Values seen before the cap keep their own series;
    later ones collapse into ``other``, so hostile or buggy input cannot explode Prometheus series.
    """
    OVERFLOW = "other"

    def __init__(self, max_values: int):
        if max_values <= 0:
            raise ValueError("max_label_values must be > 0")
        self.max_values = max_values
        self._seen = set()
        self._lock = threading.Lock()

    def __call__(self, value: str) -> str:
        if value in self._seen:
            return value
        with self._lock:
            if value in self._seen:
                return value
            if len(self._seen) < self.max_values:
                self._seen.add(value)
                return value
        return self.OVERFLOW


class MetricsManager:
    """
    Manages Prometheus metrics instrumentation for PostOffice.
    Encapsulates counters, histograms, and optional HTTP metrics scraping server.
    """
    def __init__(self, registry: Optional[CollectorRegistry] = None, max_label_values: int = 100):
        self._guards = {}
        self._max_label_values = max_label_values
        LabelGuard(max_label_values)  # validate eagerly
        self.registry = registry or CollectorRegistry(auto_describe=True)
        self._readiness_fn: Callable[[], Tuple[bool, int]] = lambda: (True, 0)
        setattr(self.registry, "_postoffice_readiness_fn", lambda: self._readiness_fn())
        self._server_handle: Optional[Tuple[WSGIServer, threading.Thread]] = None
        self._server_lock = threading.Lock()

        # Metrics Definitions
        self.messages_routed = Counter(
            'postoffice_messages_routed_total',
            'Total number of messages routed through PostOffice',
            ['source_broker', 'target_broker', 'status'],
            registry=self.registry
        )

        self.routing_errors = Counter(
            'postoffice_routing_errors_total',
            'Total number of routing and delivery errors encountered',
            ['source_broker', 'error_type'],
            registry=self.registry
        )

        self.routing_duration = Histogram(
            'postoffice_routing_duration_seconds',
            'Time taken in seconds to route a message from ingress to egress dispatch',
            ['source_broker'],
            registry=self.registry
        )

        self.dlq_messages = Counter(
            'postoffice_dlq_messages_total',
            'Total number of messages routed to a Dead Letter Queue',
            ['source_broker', 'dlq_broker', 'dlq_topic'],
            registry=self.registry
        )

        self.ingress = Counter(
            'postoffice_ingress_total',
            'Messages stopped by the ingress firewall',
            ['source_broker', 'rule', 'action'],
            registry=self.registry
        )

        self.sender_events = Counter(
            'postoffice_sender_events_total',
            'Problem events attributed to senders, by category (per-sender detail: sender_report)',
            ['source_broker', 'category'],
            registry=self.registry
        )

        self.inflight = Gauge(
            'postoffice_inflight_messages',
            'Messages accepted from a source and not yet acknowledged or negatively acknowledged',
            ['source_broker'],
            registry=self.registry
        )

        self.control_events = Counter(
            'postoffice_control_events_total',
            'Control Plane events and state entries seen by a Data Plane worker, by outcome',
            ['result'],
            registry=self.registry
        )

        self.dlq_refused = Counter(
            'postoffice_dlq_refused_total',
            'Total number of Dead Letter Queue writes refused by the DLQ budget',
            ['source_broker', 'reason'],
            registry=self.registry
        )

    def _label(self, name: str, value: str) -> str:
        """Maps a label value through the per-label-name cardinality guard."""
        guard = self._guards.get(name)
        if guard is None:
            guard = self._guards.setdefault(name, LabelGuard(self._max_label_values))
        return guard(value)

    def set_inflight_source(self, source_broker: str, depth_fn: Callable[[], float]) -> None:
        """Reports in-flight depth for a source by sampling ``depth_fn`` at scrape time."""
        self.inflight.labels(source_broker=self._label("source_broker", source_broker)).set_function(depth_fn)

    def record_ingress(self, source_broker: str, rule: str, action: str) -> None:
        """Increments the ingress firewall counter."""
        self.ingress.labels(source_broker=self._label('source_broker', source_broker), rule=rule, action=action).inc()

    def record_sender_event(self, source_broker: str, category: str) -> None:
        """Increments the sender_events counter; no sender label, to keep cardinality bounded."""
        self.sender_events.labels(source_broker=self._label('source_broker', source_broker), category=category).inc()

    def record_control_event(self, result: str) -> None:
        """Counts a control-plane message outcome (applied, unsigned, bad_signature, ...)."""
        self.control_events.labels(result=result).inc()

    def record_dlq_refused(self, source_broker: str, reason: str) -> None:
        """Increments the dlq_refused counter."""
        self.dlq_refused.labels(source_broker=self._label('source_broker', source_broker), reason=reason).inc()

    def record_routed(self, source_broker: str, target_broker: str, status: str = "success") -> None:
        """Increments the messages_routed counter."""
        self.messages_routed.labels(
            source_broker=self._label('source_broker', source_broker),
            target_broker=self._label('target_broker', target_broker),
            status=status
        ).inc()

    def record_error(self, source_broker: str, error_type: str) -> None:
        """Increments the routing_errors counter."""
        self.routing_errors.labels(
            source_broker=self._label('source_broker', source_broker),
            error_type=error_type
        ).inc()

    def record_duration(self, source_broker: str, duration_seconds: float) -> None:
        """Observes the routing duration in the histogram."""
        self.routing_duration.labels(
            source_broker=self._label('source_broker', source_broker)
        ).observe(duration_seconds)

    def record_dlq(self, source_broker: str, dlq_broker: str, dlq_topic: str) -> None:
        """Increments the dlq_messages counter."""
        self.dlq_messages.labels(
            source_broker=self._label('source_broker', source_broker),
            dlq_broker=self._label('dlq_broker', dlq_broker),
            dlq_topic=self._label('dlq_topic', dlq_topic)
        ).inc()

    def set_readiness_probe(self, readiness_fn: Callable[[], Tuple[bool, int]]) -> None:
        """Configures the `(ready, applied_version)` callback queried by `/readyz`."""
        self._readiness_fn = readiness_fn
        setattr(self.registry, "_postoffice_readiness_fn", lambda: self._readiness_fn())

    @property
    def bound_port(self) -> Optional[int]:
        """Returns the bound TCP port of the running HTTP server, or None if stopped."""
        with self._server_lock:
            if self._server_handle is None:
                return None
            server, _ = self._server_handle
            return getattr(server, "server_port", None)

    def get_metrics_text(self) -> str:
        """Returns the Prometheus text-formatted metrics snapshot."""
        return generate_latest(self.registry).decode('utf-8')

    def start_server(self, port: int, addr: str = '127.0.0.1') -> None:
        """Starts a background HTTP server serving metrics; loopback only unless ``addr`` says otherwise."""
        with self._server_lock:
            if self._server_handle is not None:
                logger.warning(f"Metrics HTTP server is already running.")
                return

            server, thread = start_http_server(port, addr=addr, registry=self.registry)
            self._server_handle = (server, thread)
            logger.info(f"Started Prometheus metrics HTTP server on {addr}:{port}")

    def stop_server(self) -> None:
        """Stops the metrics HTTP server if currently running."""
        with self._server_lock:
            if self._server_handle is None:
                return

            server, thread = self._server_handle
            try:
                server.shutdown()
                server.server_close()
                thread.join(timeout=2.0)
            except Exception as e:
                logger.error(f"Error stopping metrics server: {e}")
            finally:
                self._server_handle = None
                logger.info("Stopped Prometheus metrics HTTP server.")
