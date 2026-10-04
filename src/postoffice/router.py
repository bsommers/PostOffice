import logging
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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

    def route(self, source_client: str, source_topic: str, message: bytes, **source_kwargs):
        logger.info(f"Received message from {source_client} on {source_topic}: {message}")

        # Find all matching routes, processing wildcards
        matching_routes = []
        for (r_client, r_topic), routes in self.routes.items():
            if r_client == source_client and self._topic_match(r_topic, source_topic):
                matching_routes.extend(routes)

        if matching_routes:
            for route_config in matching_routes:
                target_client = route_config["target_client"]
                target_topic = route_config["target_topic"]

                # Exclude internal routing metadata to just pass **kwargs to publish
                publish_kwargs = {k: v for k, v in route_config.items() if k not in ("target_client", "target_topic")}

                if target_client in self.clients:
                    logger.info(f"Routing to {target_client} on {target_topic} args: {publish_kwargs}")
                    self.clients[target_client].publish(target_topic, message, **publish_kwargs)
                else:
                    logger.warning(f"Target client {target_client} not registered")
        else:
            logger.debug(f"No routes found for {source_client}/{source_topic}")
