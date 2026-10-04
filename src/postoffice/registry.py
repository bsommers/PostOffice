import logging
from typing import Dict, Type, Any
from postoffice.base_client import BaseClient

logger = logging.getLogger(__name__)

class ClientRegistry:
    _registry: Dict[str, Type[BaseClient]] = {}

    @classmethod
    def register(cls, protocol_name: str):
        """Decorator to register a client class under a given protocol name."""
        def wrapper(client_cls: Type[BaseClient]):
            cls._registry[protocol_name] = client_cls
            logger.info(f"Registered plugin for protocol: {protocol_name}")
            return client_cls
        return wrapper

    @classmethod
    def get_client(cls, protocol_name: str) -> Type[BaseClient]:
        if protocol_name not in cls._registry:
            raise ValueError(f"No client registered for protocol '{protocol_name}'")
        return cls._registry[protocol_name]

    @classmethod
    def create_client(cls, protocol_name: str, name: str, router: Any, **kwargs) -> BaseClient:
        """Factory method to instantiate a registered client."""
        client_cls = cls.get_client(protocol_name)
        return client_cls(name=name, router=router, **kwargs)
