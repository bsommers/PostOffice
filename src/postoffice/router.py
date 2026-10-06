import logging
import threading
from typing import Dict, List, Any, Optional, Callable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class _FanoutCoordinator:
    """Coordinates asynchronous delivery confirmations across multiple target broker publishes."""
    def __init__(
        self,
        expected_count: int,
        ack_fn: Optional[Callable[[], None]] = None,
        nack_fn: Optional[Callable[..., None]] = None
    ):
        self.remaining = expected_count
        self.ack_fn = ack_fn
        self.nack_fn = nack_fn
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
    def __init__(self):
        # Maps (source_client_name, source_topic) to list of dictionaries describing target parameters
        self.routes: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        self.clients: Dict[str, Any] = {}

    def register_client(self, client: Any):
        self.clients[client.name] = client
        logger.info(f"Registered client: {client.name}")

    def _topic_match(self, route_topic: str, message_topic: str) -> bool:
        """Helper to match MQTT style wildcards (+ for single level, # for multi-level)."""
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
        # Allow ack_fn and nack_fn to be passed via kwargs if needed
        if ack_fn is None and "ack_fn" in source_kwargs:
            ack_fn = source_kwargs.pop("ack_fn")
        if nack_fn is None and "nack_fn" in source_kwargs:
            nack_fn = source_kwargs.pop("nack_fn")

        logger.info(f"Received message from {source_client} on {source_topic}: {message}")

        # Find all matching routes, processing wildcards
        matching_routes = []
        for (r_client, r_topic), routes in self.routes.items():
            if r_client == source_client and self._topic_match(r_topic, source_topic):
                matching_routes.extend(routes)

        if not matching_routes:
            logger.debug(f"No routes found for {source_client}/{source_topic}")
            if ack_fn:
                ack_fn()
            return

        has_callbacks = ack_fn is not None or nack_fn is not None
        coordinator = _FanoutCoordinator(len(matching_routes), ack_fn=ack_fn, nack_fn=nack_fn) if has_callbacks else None

        for route_config in matching_routes:
            target_client = route_config["target_client"]
            target_topic = route_config["target_topic"]

            # Exclude internal routing metadata to just pass **kwargs to publish
            raw_kwargs = {k: v for k, v in route_config.items() if k not in ("target_client", "target_topic")}
            publish_kwargs = _translate_semantics(source_kwargs, raw_kwargs)

            if coordinator:
                publish_kwargs["on_confirm"] = coordinator.on_confirm
                publish_kwargs["on_error"] = coordinator.on_error

            if target_client in self.clients:
                logger.info(f"Routing to {target_client} on {target_topic} args: {publish_kwargs}")
                try:
                    self.clients[target_client].publish(target_topic, message, **publish_kwargs)
                except Exception as e:
                    logger.error(f"Error publishing to {target_client}: {e}")
                    if coordinator:
                        coordinator.on_error(e)
            else:
                logger.warning(f"Target client {target_client} not registered")
                if coordinator:
                    coordinator.on_error(RuntimeError(f"Target client {target_client} not registered"))

