from typing import Optional, Union, Dict, Any
from postoffice.plugins.mqtt_client import MqttClient
from postoffice.registry import ClientRegistry
from postoffice.security import SecurityConfig, ensure_security_config
import logging

logger = logging.getLogger(__name__)

@ClientRegistry.register("nanomq")
class NanoMqClient(MqttClient):
    """
    NanoMQ is a lightweight edge MQTT broker.
    It inherently supports MQTT 3.1.1 and 5.0 just like Mosquitto,
    but we can extend this client class with any NanoMQ specific SDK functions if needed.
    """
    def __init__(
        self,
        name: str,
        router: any,
        host: str = "localhost",
        port: Optional[int] = None,
        security_config: Optional[Union[SecurityConfig, Dict[str, Any]]] = None,
        **kwargs
    ):
        if port is None:
            sec = ensure_security_config(security_config)
            port = 8884 if sec.tls_enabled else 1884
        super().__init__(name, router, host=host, port=port, security_config=security_config, **kwargs)
        logger.info(f"Initialized NanoMQ Client targeting {host}:{self.port}")
