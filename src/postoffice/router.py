import logging
import threading
import time
from typing import Dict, Any, List, Optional, Callable
from postoffice.trie import TopicTrie
from postoffice.metrics import MetricsManager
from postoffice.dlq import format_dlq_payload

logger = logging.getLogger(__name__)

class _FanoutCoordinator:
    """Coordinates asynchronous delivery confirmations across multiple target broker publishes."""
    def __init__(
        self,
        expected_count: int,
        ack_fn: Optional[Callable[[], None]] = None,
        nack_fn: Optional[Callable[..., None]] = None,
        dlq_fn: Optional[Callable[[Optional[Exception]], bool]] = None
    ):
        self.remaining = expected_count
        self.ack_fn = ack_fn
        self.nack_fn = nack_fn
        self.dlq_fn = dlq_fn
        self.lock = threading.Lock()
        self.failed = False

        if self.remaining == 0 and self.ack_fn:
            self.ack_fn()

    def on_confirm(self):
        with self.lock:
            if self.failed:
                return
            self.remaining -= 1
            if self.remaining == 0 and self.ack_fn:
                self.ack_fn()

    def on_error(self, exc: Optional[Exception] = None):
        with self.lock:
            if not self.failed:
                self.failed = True
                if self.dlq_fn:
                    try:
                        handled = self.dlq_fn(exc)
                        if handled:
                            return
                    except Exception as e:
                        logger.error(f"Error executing DLQ fallback: {e}")
                if self.nack_fn:
                    try:
                        self.nack_fn(requeue=True)
                    except TypeError:
                        self.nack_fn()


def _translate_semantics(source_kwargs: Dict[str, Any], target_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Applies automatic semantic translation between MQTT QoS and AMQP delivery_mode.
    Explicit target kwargs take precedence over automatic defaults.
    """
    translated = dict(target_kwargs)

    # MQTT -> AMQP delivery_mode translation
    if "delivery_mode" not in translated and "qos" in source_kwargs:
        source_qos = source_kwargs.get("qos")
        if source_qos in (1, 2):
            translated["delivery_mode"] = 2  # persistent
        elif source_qos == 0:
            translated["delivery_mode"] = 1  # transient

    # AMQP -> MQTT qos translation
    if "qos" not in translated and "delivery_mode" in source_kwargs:
        source_dm = source_kwargs.get("delivery_mode")
        if source_dm == 2:
            translated["qos"] = 1  # at-least-once
        elif source_dm == 1:
            translated["qos"] = 0  # at-most-once

    return translated


class Router:
    def __init__(self, metrics_manager: Optional[MetricsManager] = None):
        # Maps (source_client_name, source_topic) to list of dictionaries describing target parameters (kept for inspection)
        self.routes: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        self.clients: Dict[str, Any] = {}
        # Per-client hierarchical TopicTrie for O(k) matching
        self.client_tries: Dict[str, TopicTrie] = {}
        # Metrics instrumentation
        self.metrics = metrics_manager or MetricsManager()
        # Global default Dead Letter Queue target
        self.default_dlq_broker: Optional[str] = None
        self.default_dlq_topic: Optional[str] = None

    def set_dlq(self, broker_name: str, topic: str) -> None:
        """Sets the global default Dead Letter Queue destination broker and topic."""
        self.default_dlq_broker = broker_name
        self.default_dlq_topic = topic
        logger.info(f"Configured default DLQ target: {broker_name}/{topic}")

    def _route_to_dlq(
        self,
        source_broker: str,
        source_topic: str,
        payload: bytes,
        error: str,
        dlq_broker: Optional[str] = None,
        dlq_topic: Optional[str] = None,
        ack_fn: Optional[Callable[[], None]] = None,
        nack_fn: Optional[Callable[..., None]] = None
    ) -> bool:
        """
        Dispatches a failed or unroutable message to the configured DLQ.
        Returns True if safely published to DLQ and acknowledged, False otherwise.
        """
        target_broker = dlq_broker or self.default_dlq_broker
        target_topic = dlq_topic or self.default_dlq_topic

        if not target_broker or not target_topic:
            return False

        # Anti-recursion protection: do not forward DLQ message to itself
        if source_broker == target_broker and source_topic == target_topic:
            logger.critical(f"DLQ recursion detected: message originated from DLQ {source_broker}/{source_topic}. Dropping.")
            self.metrics.record_error(source_broker, "dlq_recursion")
            if nack_fn:
                try:
                    nack_fn(requeue=False)
                except TypeError:
                    nack_fn()
            return True

        try:
            dlq_bytes = format_dlq_payload(payload, source_broker, source_topic, error)
            if target_broker in self.clients:
                self.clients[target_broker].publish(target_topic, dlq_bytes)
                self.metrics.record_dlq(source_broker, target_broker, target_topic)
                logger.info(f"Dispatched failed/unroutable message from {source_broker}/{source_topic} to DLQ {target_broker}/{target_topic}")
                if ack_fn:
                    ack_fn()
                return True
            else:
                logger.error(f"DLQ broker '{target_broker}' not registered")
                self.metrics.record_error(source_broker, "dlq_broker_unregistered")
                return False
        except Exception as e:
            logger.error(f"Error publishing to DLQ {target_broker}/{target_topic}: {e}")
            self.metrics.record_error(source_broker, "dlq_publish_failure")
            return False

    def register_client(self, client: Any):
        self.clients[client.name] = client
        logger.info(f"Registered client: {client.name}")

    def _topic_match(self, route_topic: str, message_topic: str) -> bool:
        """Helper to match MQTT style wildcards (+ for single level, # for multi-level). Kept for backward compatibility."""
        if route_topic == message_topic:
            return True

        route_parts = route_topic.split('/')
        msg_parts = message_topic.split('/')

        for i, part in enumerate(route_parts):
            if part == '#':
                return True
            if i >= len(msg_parts):
                return False
            if part != '+' and part != msg_parts[i]:
                return False

        return len(route_parts) == len(msg_parts)

    def add_route(self, source_client: str, source_topic: str, target_client: str, target_topic: str, **target_kwargs):
        key = (source_client, source_topic)
        if key not in self.routes:
            self.routes[key] = []

        route_config = {
            "target_client": target_client,
            "target_topic": target_topic,
            **target_kwargs
        }
        self.routes[key].append(route_config)

        if source_client not in self.client_tries:
            self.client_tries[source_client] = TopicTrie()
        self.client_tries[source_client].insert(source_topic, route_config)

        logger.info(f"Added route: {source_client}/{source_topic} -> {target_client}/{target_topic} with kwargs {target_kwargs}")

    def route(
        self,
        source_client: str,
        source_topic: str,
        message: bytes,
        ack_fn: Optional[Callable[[], None]] = None,
        nack_fn: Optional[Callable[..., None]] = None,
        **source_kwargs
    ):
        start_time = time.perf_counter()

        # Allow ack_fn and nack_fn to be passed via kwargs if needed
        if ack_fn is None and "ack_fn" in source_kwargs:
            ack_fn = source_kwargs.pop("ack_fn")
        if nack_fn is None and "nack_fn" in source_kwargs:
            nack_fn = source_kwargs.pop("nack_fn")

        logger.info(f"Received message from {source_client} on {source_topic}: {message}")

        # Find all matching routes in O(k) time via TopicTrie
        client_trie = self.client_tries.get(source_client)
        matching_routes = client_trie.match(source_topic) if client_trie else []

        if not matching_routes:
            logger.debug(f"No routes found for {source_client}/{source_topic}")
            self.metrics.record_error(source_client, "unroutable")
            self.metrics.record_duration(source_client, time.perf_counter() - start_time)

            dlq_configured = bool(self.default_dlq_broker and self.default_dlq_topic)
            dlq_handled = self._route_to_dlq(
                source_client,
                source_topic,
                message,
                error="Unroutable message: no matching routes",
                ack_fn=ack_fn,
                nack_fn=nack_fn
            )
            if not dlq_handled:
                if dlq_configured and nack_fn:
                    try:
                        nack_fn(requeue=True)
                    except TypeError:
                        nack_fn()
                elif ack_fn:
                    ack_fn()
            return

        def _handle_dlq_on_error(exc: Optional[Exception]) -> bool:
            err_msg = str(exc) if exc else "Publish failed"
            # Use per-route DLQ if configured on the route, else fallback to global DLQ
            route_dlq_broker = matching_routes[0].get("dlq_broker") if matching_routes else None
            route_dlq_topic = matching_routes[0].get("dlq_topic") if matching_routes else None
            return self._route_to_dlq(
                source_client,
                source_topic,
                message,
                error=err_msg,
                dlq_broker=route_dlq_broker,
                dlq_topic=route_dlq_topic,
                ack_fn=ack_fn,
                nack_fn=nack_fn
            )

        has_callbacks = ack_fn is not None or nack_fn is not None
        coordinator = _FanoutCoordinator(
            len(matching_routes),
            ack_fn=ack_fn,
            nack_fn=nack_fn,
            dlq_fn=_handle_dlq_on_error
        ) if has_callbacks else None

        for route_config in matching_routes:
            target_client = route_config["target_client"]
            target_topic = route_config["target_topic"]

            # Exclude internal routing and DLQ metadata to pass **kwargs to publish
            raw_kwargs = {
                k: v for k, v in route_config.items()
                if k not in ("target_client", "target_topic", "dlq_broker", "dlq_topic")
            }
            publish_kwargs = _translate_semantics(source_kwargs, raw_kwargs)

            if coordinator:
                publish_kwargs["on_confirm"] = coordinator.on_confirm
                publish_kwargs["on_error"] = coordinator.on_error

            if target_client in self.clients:
                logger.info(f"Routing to {target_client} on {target_topic} args: {publish_kwargs}")
                try:
                    self.clients[target_client].publish(target_topic, message, **publish_kwargs)
                    self.metrics.record_routed(source_client, target_client, status="success")
                except Exception as e:
                    logger.error(f"Error publishing to {target_client}: {e}")
                    self.metrics.record_routed(source_client, target_client, status="error")
                    self.metrics.record_error(source_client, "publish_failure")
                    if coordinator:
                        coordinator.on_error(e)
                    else:
                        self._route_to_dlq(
                            source_client,
                            source_topic,
                            message,
                            error=str(e),
                            dlq_broker=route_config.get("dlq_broker"),
                            dlq_topic=route_config.get("dlq_topic")
                        )
            else:
                logger.warning(f"Target client {target_client} not registered")
                self.metrics.record_error(source_client, "unregistered_target")
                exc = RuntimeError(f"Target client {target_client} not registered")
                if coordinator:
                    coordinator.on_error(exc)
                else:
                    self._route_to_dlq(
                        source_client,
                        source_topic,
                        message,
                        error=str(exc),
                        dlq_broker=route_config.get("dlq_broker"),
                        dlq_topic=route_config.get("dlq_topic")
                    )

        self.metrics.record_duration(source_client, time.perf_counter() - start_time)



