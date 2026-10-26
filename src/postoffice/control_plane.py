import json
import redis
import logging
from typing import Any, Dict, Optional
from postoffice.signing import HmacSigner, as_envelope, sign_event, sign_state, sign_version

logger = logging.getLogger(__name__)

class ControlPlane:
    """
    Centralized admin service for managing cluster state.
    State is saved to Redis, and changes are broadcasted via Pub/Sub so that
    any number of DataPlane workers can sync rules without restarting.
    """
    signer: Optional[HmacSigner] = None   # class default: planes built without __init__ stay unsigned
    version_key = "postoffice:version"
    version_sigs_key = "postoffice:version_sigs"
    VERSION_SIG_RETENTION = 1000

    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379,
                 redis_password: Optional[str] = None, redis_tls: bool = False,
                 signer: Optional[HmacSigner] = None):
        self.r = redis.Redis(
            host=redis_host, port=redis_port, password=redis_password, ssl=redis_tls,
            decode_responses=True,
        )
        self.channel = "postoffice:config_updates"
        # Explicit signer wins; otherwise POSTOFFICE_EVENT_KEYS (None = unsigned legacy wire format).
        self.signer = signer if signer is not None else HmacSigner.from_env()

    def _publish_update(self, action: str, data: dict):
        # State is already written; bump the version, then announce it so workers can detect gaps.
        version = self.r.incr(self.version_key)
        body = json.dumps({"action": action, "data": data, "version": version})
        if self.signer is not None:
            payload = sign_event(self.signer, body)
            self.r.hset(self.version_sigs_key, str(version), sign_version(self.signer, version))
            self.r.hdel(self.version_sigs_key, str(version - self.VERSION_SIG_RETENTION))
        else:
            payload = body
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

    def _put(self, hash_name: str, field: str, config: Dict[str, Any]) -> None:
        """Stores one state entry, signed (bound to its hash and field) when a signer is configured."""
        payload = json.dumps(config)
        value = sign_state(self.signer, hash_name, field, payload) if self.signer is not None else payload
        self.r.hset(hash_name, field, value)

    def _read_state(self, hash_name: str, field: str) -> Optional[Dict[str, Any]]:
        raw = self.r.hget(hash_name, field)
        if not raw:
            return None
        obj = json.loads(raw)
        envelope = as_envelope(obj)
        return json.loads(envelope["payload"]) if envelope else obj

    def register_broker(self, name: str, protocol: str, **kwargs):
        """Save a new broker configuration to Redis and alert the cluster."""
        serialized_kwargs = self._serialize_dict(kwargs)
        config = {"protocol": protocol, "kwargs": serialized_kwargs}
        self._put("postoffice:brokers", name, config)
        self._publish_update("add_broker", {"name": name, **config})

    def update_broker_security(self, name: str, security_config: dict | object):
        """Hot-reload a broker's security configuration in Redis and broadcast to active DataPlanes."""
        if hasattr(security_config, "to_dict") and callable(security_config.to_dict):
            sec_dict = security_config.to_dict()
        else:
            sec_dict = dict(security_config)

        config = self._read_state("postoffice:brokers", name)
        if config:
            config.setdefault("kwargs", {})["security_config"] = sec_dict
            self._put("postoffice:brokers", name, config)

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
        self._put("postoffice:routes", route_id, config)
        self._publish_update("add_route", config)

    def add_subscription(self, broker_name: str, topic: str, **kwargs):
        """Register an edge subscription and alert the cluster."""
        sub_id = f"{broker_name}:{topic}"
        config = {"broker_name": broker_name, "topic": topic, "kwargs": kwargs}
        self._put("postoffice:subscriptions", sub_id, config)
        self._publish_update("add_subscription", config)

    def clear_state(self):
        """Wipes the distributed state (dangerous)."""
        self.r.delete("postoffice:brokers", "postoffice:routes", "postoffice:subscriptions")
        self._publish_update("clear_state", {})
