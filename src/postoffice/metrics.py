import logging
from typing import Optional, Tuple
import threading
from wsgiref.simple_server import WSGIServer
from prometheus_client import CollectorRegistry, Counter, Histogram, generate_latest, start_http_server

logger = logging.getLogger(__name__)

class MetricsManager:
    """
    Manages Prometheus metrics instrumentation for PostOffice.
    Encapsulates counters, histograms, and optional HTTP metrics scraping server.
    """
    def __init__(self, registry: Optional[CollectorRegistry] = None):
        self.registry = registry or CollectorRegistry(auto_describe=True)
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

    def record_routed(self, source_broker: str, target_broker: str, status: str = "success") -> None:
        """Increments the messages_routed counter."""
        self.messages_routed.labels(
            source_broker=source_broker,
            target_broker=target_broker,
            status=status
        ).inc()

    def record_error(self, source_broker: str, error_type: str) -> None:
        """Increments the routing_errors counter."""
        self.routing_errors.labels(
            source_broker=source_broker,
            error_type=error_type
        ).inc()

    def record_duration(self, source_broker: str, duration_seconds: float) -> None:
        """Observes the routing duration in the histogram."""
        self.routing_duration.labels(
            source_broker=source_broker
        ).observe(duration_seconds)

    def record_dlq(self, source_broker: str, dlq_broker: str, dlq_topic: str) -> None:
        """Increments the dlq_messages counter."""
        self.dlq_messages.labels(
            source_broker=source_broker,
            dlq_broker=dlq_broker,
            dlq_topic=dlq_topic
        ).inc()

    def get_metrics_text(self) -> str:
        """Returns the Prometheus text-formatted metrics snapshot."""
        return generate_latest(self.registry).decode('utf-8')

    def start_server(self, port: int, addr: str = '0.0.0.0') -> None:
        """Starts a background HTTP server serving metrics on the specified port."""
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
