import json
import redis
import logging

logger = logging.getLogger(__name__)

class ControlPlane:
    """
    Centralized admin service for managing cluster state.
    State is saved to Redis, and changes are broadcasted via Pub/Sub so that
    any number of DataPlane workers can sync rules without restarting.
    """
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.channel = "postoffice:config_updates"

    def _publish_update(self, action: str, data: dict):
        payload = json.dumps({"action": action, "data": data})
        self.r.publish(self.channel, payload)
        logger.info(f"Published update: {payload}")

    def register_broker(self, name: str, protocol: str, **kwargs):
        """Save a new broker configuration to Redis and alert the cluster."""
        config = {"protocol": protocol, "kwargs": kwargs}
        self.r.hset("postoffice:brokers", name, json.dumps(config))
        self._publish_update("add_broker", {"name": name, **config})

    def add_route(self, source_broker: str, source_topic: str, target_broker: str, target_topic: str, **target_kwargs):
        """Save a new routing rule and alert the cluster."""
        route_id = f"{source_broker}:{source_topic}->{target_broker}:{target_topic}"
        config = {
            "source_broker": source_broker,
            "source_topic": source_topic,
            "target_broker": target_broker,
            "target_topic": target_topic,
            "target_kwargs": target_kwargs
        }
        self.r.hset("postoffice:routes", route_id, json.dumps(config))
        self._publish_update("add_route", config)

    def add_subscription(self, broker_name: str, topic: str, **kwargs):
        """Register an edge subscription and alert the cluster."""
        sub_id = f"{broker_name}:{topic}"
        config = {"broker_name": broker_name, "topic": topic, "kwargs": kwargs}
        self.r.hset("postoffice:subscriptions", sub_id, json.dumps(config))
        self._publish_update("add_subscription", config)

    def clear_state(self):
        """Wipes the distributed state (dangerous)."""
        self.r.delete("postoffice:brokers", "postoffice:routes", "postoffice:subscriptions")
        self._publish_update("clear_state", {})
