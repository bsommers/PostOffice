import json
import redis
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class ControlPlane:
    """
    Centralized admin service for managing cluster state.
    State is saved to Redis, and changes are broadcasted via Pub/Sub so that
    any number of DataPlane workers can sync rules without restarting.
    """
    version_key = "postoffice:version"

    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379,
                 redis_password: Optional[str] = None, redis_tls: bool = False):
        self.r = redis.Redis(
            host=redis_host, port=redis_port, password=redis_password, ssl=redis_tls,
            decode_responses=True,
        )
        self.channel = "postoffice:config_updates"

    def _publish_update(self, action: str, data: dict):
        # State is already written; bump the version, then announce it so workers can detect gaps.
        version = self.r.incr(self.version_key)
        payload = json.dumps({"action": action, "data": data, "version": version})
        self.r.publish(self.channel, payload)
        # Log only the action and entity name: payloads can carry credentials (security_config).
        logger.info(f"Published update: {action} ({data.get('name') or data.get('broker_name') or data.get('source_broker') or '-'})")

    def _serialize_dict(self, data: dict) -> dict:
        result = {}
        for k, v in data.items():
            if hasattr(v, "to_dict") and callable(v.to_dict):
                result[k] = v.to_dict()
            else:
                result[k] = v
        return result

    def register_broker(self, name: str, protocol: str, **kwargs):
        """Save a new broker configuration to Redis and alert the cluster."""
        serialized_kwargs = self._serialize_dict(kwargs)
        config = {"protocol": protocol, "kwargs": serialized_kwargs}
        self.r.hset("postoffice:brokers", name, json.dumps(config))
        self._publish_update("add_broker", {"name": name, **config})

    def update_broker_security(self, name: str, security_config: dict | object):
        """Hot-reload a broker's security configuration in Redis and broadcast to active DataPlanes."""
        if hasattr(security_config, "to_dict") and callable(security_config.to_dict):
            sec_dict = security_config.to_dict()
        else:
            sec_dict = dict(security_config)

        existing_raw = self.r.hget("postoffice:brokers", name)
        if existing_raw:
            config = json.loads(existing_raw)
            config.setdefault("kwargs", {})["security_config"] = sec_dict
            self.r.hset("postoffice:brokers", name, json.dumps(config))

        self._publish_update(
            "update_broker_security",
            {"name": name, "security_config": sec_dict}
        )

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
