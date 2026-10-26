import logging
from typing import Dict, Any, List, Optional, Union
from postoffice.dlq import BudgetedDlq
from postoffice.ingress import Firewall
from postoffice.router import Router
from postoffice.registry import ClientRegistry
from postoffice.schema import BaseSchemaRegistry, SchemaEntry
from postoffice.security import SecurityConfig

# Import plugins to trigger registration
import postoffice.plugins

logger = logging.getLogger(__name__)

class PostOffice:
    """
    Uniform Interface Layer (Facade) for configuring and starting the multi-protocol router.
    Clients interact strictly with this class and do not need to know about specific broker implementations.
    """
    def __init__(
        self,
        metrics_port: Optional[int] = None,
        dlq_broker: Optional[str] = None,
        dlq_topic: Optional[str] = None,
        schema_registry: Optional[BaseSchemaRegistry] = None,
        on_unroutable: str = "nack",
        max_routes_per_topic: Optional[int] = 100,
        metrics_addr: str = "127.0.0.1",
    ):
        self.metrics_addr = metrics_addr
        self.max_routes_per_topic = max_routes_per_topic
        self.router = Router(schema_registry=schema_registry, max_routes_per_topic=max_routes_per_topic)
        self.router.set_unroutable_policy(on_unroutable)
        self.on_unroutable = on_unroutable
        self.dlq_budget: Optional[BudgetedDlq] = None
        self.firewall: Optional[Firewall] = None
        self._inflight_limits: Dict[str, Dict[str, Any]] = {}
        self._retry_policy: Optional[Dict[str, Any]] = None
        self.brokers: Dict[str, Any] = {}
        self.is_running = False
        self.metrics_port = metrics_port
        self.metrics_manager = self.router.metrics
        self.schema_registry = self.router.schema_registry
        self.dlq_broker = dlq_broker
        self.dlq_topic = dlq_topic
        if dlq_broker and dlq_topic:
            self.router.set_dlq(dlq_broker, dlq_topic)

    def register_schema(self, name: str, definition: Any, schema_type: str = "AVRO") -> int:
        """Registers a schema into the PostOffice schema registry."""
        return self.schema_registry.register_schema(name, definition, schema_type)

    def get_schema(self, name: str) -> Optional[SchemaEntry]:
        """Retrieves a schema entry by name."""
        return self.schema_registry.get_schema(name)

    def get_schema_by_id(self, schema_id: int) -> Optional[SchemaEntry]:
        """Retrieves a schema entry by numeric ID."""
        return self.schema_registry.get_schema_by_id(schema_id)

    def set_dlq(self, broker_name: str, topic: str) -> None:
        """Sets the global default Dead Letter Queue destination broker and topic."""
        self.dlq_broker = broker_name
        self.dlq_topic = topic
        self.router.set_dlq(broker_name, topic)

    def set_unroutable_policy(self, policy: str, broker: Optional[str] = None) -> None:
        """Sets what happens to messages matching no route: 'nack' (default), 'dlq' or 'drop'."""
        self.router.set_unroutable_policy(policy, broker=broker)
        if broker is None:
            self.on_unroutable = policy

    def set_dlq_budget(self, **limits) -> None:
        """Limits DLQ writes (max_message_bytes, msgs_per_sec, bytes_per_sec) per source broker."""
        self.dlq_budget = BudgetedDlq(**limits)
        self.router.set_dlq_budget(self.dlq_budget)

    def sender_report(self, top: int = 10) -> List[Dict[str, Any]]:
        """Noisiest senders seen by the ingress ledger (empty when no ledger is configured)."""
        ledger = getattr(self.firewall, "ledger", None)
        return ledger.sender_report(top) if ledger is not None else []

    def configure_ingress(self, config: Union[Firewall, Dict[str, Any], None]) -> None:
        """Installs the ingress firewall from a Firewall or a dict (same shape as a rules file); None disables."""
        self.firewall = Firewall.from_dict(config) if isinstance(config, dict) else config
        self.router.set_firewall(self.firewall)
        if isinstance(config, dict):
            for broker, limit in config.get("inflight", {}).items():
                self.set_inflight_limit(broker, **limit)

    def set_retry_policy(self, max_attempts: Optional[int] = 5, base_delay: float = 0.5, **options) -> None:
        """Caps requeue-driven redelivery of failing messages (max_attempts=None = unbounded)."""
        self._retry_policy = dict(max_attempts=max_attempts, base_delay=base_delay, **options)
        self.router.set_retry_policy(**self._retry_policy)

    def set_inflight_limit(
        self, broker: str, max_inflight: Optional[int], low_water: Optional[int] = None,
        lease_timeout: float = 60.0,
    ) -> None:
        """Caps un-confirmed messages from a source broker, applying native back-pressure (None removes)."""
        if max_inflight is None:
            self._inflight_limits.pop(broker, None)
        else:
            self._inflight_limits[broker] = dict(
                max_inflight=max_inflight, low_water=low_water, lease_timeout=lease_timeout)
        self.router.set_inflight_limit(broker, max_inflight, low_water=low_water, lease_timeout=lease_timeout)

    def set_metrics_port(self, port: Optional[int], addr: Optional[str] = None) -> None:
        """Sets the HTTP port for Prometheus metrics; ``addr`` (default loopback) must be widened explicitly."""
        self.metrics_port = port
        if addr is not None:
            self.metrics_addr = addr

    def reload_broker_security(
        self,
        name: str,
        security_config: Union[SecurityConfig, Dict[str, Any]],
    ) -> None:
        """Hot-reloads TLS certificates and credentials for an existing broker in-flight."""
        if name not in self.brokers:
            raise ValueError(f"Broker '{name}' not found.")
        self.brokers[name].reload_security(security_config)

    def configure_circuit_breaker(
        self,
        broker_name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
    ):
        """Configures an egress circuit breaker for a registered broker."""
        if broker_name not in self.brokers:
            raise ValueError(f"Broker '{broker_name}' not found.")
        return self.router.configure_circuit_breaker(
            broker_name,
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
        )

    def add_broker(
        self,
        name: str,
        protocol: str,
        security_config: Optional[Union[SecurityConfig, Dict[str, Any]]] = None,
        circuit_breaker_threshold: Optional[int] = None,
        circuit_breaker_timeout: float = 30.0,
        **kwargs
    ) -> None:
        """
        Registers a new messaging broker into the PostOffice network.

        :param name: Unique internal identifier for the broker (e.g. 'mqtt_1')
        :param protocol: The protocol type ('mqtt', 'amqp', 'kafka', 'nanomq')
        :param security_config: Optional TLS/mTLS and authentication settings (SecurityConfig or dict)
        :param circuit_breaker_threshold: Optional consecutive failure threshold to trip egress circuit breaker
        :param circuit_breaker_timeout: Seconds before an OPEN circuit breaker transitions to HALF_OPEN
        :param kwargs: Broker specific connection params (e.g. host, port, bootstrap_servers, user)
        """
        if name in self.brokers:
            raise ValueError(f"Broker with name '{name}' already exists.")

        if security_config is not None:
            kwargs["security_config"] = security_config

        logger.info(f"Adding broker '{name}' via protocol '{protocol}'")
        client = ClientRegistry.create_client(protocol, name, self.router, **kwargs)
        self.router.register_client(client)
        self.brokers[name] = client

        if circuit_breaker_threshold is not None:
            self.router.configure_circuit_breaker(
                name,
                failure_threshold=circuit_breaker_threshold,
                recovery_timeout=circuit_breaker_timeout,
            )

        # If the PostOffice has already started, immediately connect newly injected brokers
        if self.is_running:
            try:
                client.connect()
            except Exception as e:
                logger.error(f"Failed to dynamically connect broker '{name}': {e}")

    def remove_broker(self, name: str) -> bool:
        """Disconnects and unregisters a broker (routes naming it are kept). Returns False if unknown."""
        client = self.brokers.pop(name, None)
        if client is None:
            return False
        try:
            client.disconnect()
        except Exception as e:
            logger.error(f"Error disconnecting broker '{name}' during removal: {e}")
        self.router.clients.pop(name, None)
        return True

    def add_route(self, source_broker: str, source_topic: str, target_broker: str, target_topic: str, **target_kwargs) -> None:
        """
        Configures a route between two registered brokers.
        """
        if source_broker not in self.brokers:
            raise ValueError(f"Source broker '{source_broker}' not found.")
        if target_broker not in self.brokers:
            raise ValueError(f"Target broker '{target_broker}' not found.")

        self.router.add_route(
            source_client=source_broker,
            source_topic=source_topic,
            target_client=target_broker,
            target_topic=target_topic,
            **target_kwargs
        )

    def subscribe(self, broker_name: str, topic: str, **kwargs) -> None:
        """
        Subscribes a specific broker to a topic.
        """
        if broker_name not in self.brokers:
            raise ValueError(f"Broker '{broker_name}' not found.")

        try:
            self.brokers[broker_name].subscribe(topic, **kwargs)
        except Exception as e:
            logger.error(f"Failed to subscribe {broker_name} to {topic}: {e}")

    def start(self) -> None:
        """
        Starts connections for all registered brokers and metrics HTTP server if configured.
        """
        self.is_running = True
        if self.metrics_port is not None:
            self.router.metrics.start_server(self.metrics_port, addr=self.metrics_addr)

        logger.info("Starting all PostOffice broker connections...")
        for name, broker in self.brokers.items():
            try:
                broker.connect()
            except Exception as e:
                logger.error(f"Failed to connect broker '{name}': {e}")

    def stop(self) -> None:
        """
        Stops and cleans up connections for all registered brokers and metrics server.
        """
        logger.info("Stopping all PostOffice broker connections...")
        self.router.metrics.stop_server()
        for name, broker in self.brokers.items():
            try:
                broker.disconnect()
            except Exception as e:
                logger.error(f"Error disconnecting broker '{name}': {e}")

    def reset(self) -> None:
        """
        Stops and cleans up all active brokers, and clears all routes and client registrations.
        Resets the PostOffice instance to an unconfigured state.
        """
        logger.info("Resetting PostOffice state...")
        self.stop()
        self.brokers.clear()
        # Keep the MetricsManager so counters and the scrape endpoint survive a reset.
        self.router = Router(
            metrics_manager=self.metrics_manager,
            schema_registry=self.schema_registry,
            max_routes_per_topic=self.max_routes_per_topic,
        )
        if self.dlq_broker and self.dlq_topic:
            self.router.set_dlq(self.dlq_broker, self.dlq_topic)
        self.router.set_unroutable_policy(self.on_unroutable)
        if self.dlq_budget is not None:
            self.router.set_dlq_budget(self.dlq_budget)
        self.router.set_firewall(self.firewall)
        for broker, limit in self._inflight_limits.items():
            self.router.set_inflight_limit(broker, **limit)
        if self._retry_policy is not None:
            self.router.set_retry_policy(**self._retry_policy)
        self.is_running = False
