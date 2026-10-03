import logging
from typing import Dict, List, Any

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Router:
    def __init__(self):
        # Maps (source_client_name, source_topic) to list of (target_client_name, target_topic)
        self.routes: Dict[tuple[str, str], List[tuple[str, str]]] = {}
        self.clients: Dict[str, Any] = {}

    def register_client(self, client: Any):
        self.clients[client.name] = client
        logger.info(f"Registered client: {client.name}")

    def add_route(self, source_client: str, source_topic: str, target_client: str, target_topic: str):
        key = (source_client, source_topic)
        if key not in self.routes:
            self.routes[key] = []
        self.routes[key].append((target_client, target_topic))
        logger.info(f"Added route: {source_client}/{source_topic} -> {target_client}/{target_topic}")

    def route(self, source_client: str, source_topic: str, message: bytes):
        logger.info(f"Received message from {source_client} on {source_topic}: {message}")

        key = (source_client, source_topic)
        if key in self.routes:
            for target_client, target_topic in self.routes[key]:
                if target_client in self.clients:
                    logger.info(f"Routing to {target_client} on {target_topic}")
                    self.clients[target_client].publish(target_topic, message)
                else:
                    logger.warning(f"Target client {target_client} not registered")
        else:
            logger.debug(f"No routes found for {source_client}/{source_topic}")
