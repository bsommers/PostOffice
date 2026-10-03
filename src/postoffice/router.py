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

        key = (source_client, source_topic)
        if key in self.routes:
            for route_config in self.routes[key]:
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
