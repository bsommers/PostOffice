import logging
import threading
import time
from typing import Dict, Any, List, Optional, Callable
from postoffice.trie import TopicTrie
from postoffice.ingress import Action, Firewall, InflightLimiter, MessageContext
from postoffice.metrics import MetricsManager
from postoffice.dlq import BudgetedDlq, format_dlq_payload
from postoffice.resiliency import CircuitBreaker, CircuitBreakerOpenError
from postoffice.schema import (
    BaseSchemaRegistry,
    InMemorySchemaRegistry,
    SchemaConverter,
    SchemaValidationError,
)

logger = logging.getLogger(__name__)

class _FanoutCoordinator:
    """Coordinates asynchronous delivery confirmations across multiple target broker publishes."""
    def __init__(
        self,
        expected_count: int,
        ack_fn: Optional[Callable[[], None]] = None,
        nack_fn: Optional[Callable[..., None]] = None,
        dlq_fn: Optional[Callable[[Optional[Exception], Optional[Dict[str, Any]]], bool]] = None
    ):
        self.remaining = expected_count
        self.ack_fn = ack_fn
        self.nack_fn = nack_fn
        self.dlq_fn = dlq_fn
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

    def on_error(self, exc: Optional[Exception] = None, route: Optional[Dict[str, Any]] = None):
        """Records a failed target publish. `route` is the failing route's config, used to pick its DLQ."""
        with self.lock:
            if not self.failed:
                self.failed = True
                if self.dlq_fn:
                    try:
                        handled = self.dlq_fn(exc, route)
                        if handled:
                            return
                    except Exception as e:
                        logger.error(f"Error executing DLQ fallback: {e}")
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
    VALID_UNROUTABLE_POLICIES = ("nack", "dlq", "drop")

    def __init__(
        self,
        metrics_manager: Optional[MetricsManager] = None,
        schema_registry: Optional[BaseSchemaRegistry] = None
    ):
        # Maps (source_client_name, source_topic) to list of dictionaries describing target parameters (kept for inspection)
        self.routes: Dict[tuple[str, str], List[Dict[str, Any]]] = {}
        self.clients: Dict[str, Any] = {}
        # Per-client hierarchical TopicTrie for O(k) matching
        self.client_tries: Dict[str, TopicTrie] = {}
        # Metrics instrumentation
        self.metrics = metrics_manager or MetricsManager()
        # Schema registry and conversion engine
        self.schema_registry = schema_registry or InMemorySchemaRegistry()
        self.converter = SchemaConverter(self.schema_registry)
        # Global default Dead Letter Queue target
        self.default_dlq_broker: Optional[str] = None
        self.default_dlq_topic: Optional[str] = None
        # What to do with messages that match no route: "nack" (default), "dlq" or "drop"
        self.unroutable_policy: str = "nack"
        self._unroutable_overrides: Dict[str, str] = {}
        # Admission control for every DLQ write
        self.dlq_budget = BudgetedDlq()
        # Ingress firewall evaluated before routing (None = disabled)
        self.firewall: Optional[Firewall] = None
        # Per-source in-flight caps with protocol-native back-pressure
        self._inflight: Dict[str, InflightLimiter] = {}
        # Per-broker egress circuit breakers
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}

    def configure_circuit_breaker(
        self,
        broker_name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        time_fn: Callable[[], float] = time.monotonic,
    ) -> CircuitBreaker:
        """Configures an egress circuit breaker for a target broker."""
        cb = CircuitBreaker(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            time_fn=time_fn,
        )
        self.circuit_breakers[broker_name] = cb
        logger.info(
            f"Configured circuit breaker for '{broker_name}' "
            f"(threshold={failure_threshold}, timeout={recovery_timeout}s)"
        )
        return cb

    def set_dlq(self, broker_name: str, topic: str) -> None:
        """Sets the global default Dead Letter Queue destination broker and topic."""
        self.default_dlq_broker = broker_name
        self.default_dlq_topic = topic
        logger.info(f"Configured default DLQ target: {broker_name}/{topic}")

    def set_unroutable_policy(self, policy: str, broker: Optional[str] = None) -> None:
        """Sets the policy for unroutable messages, globally or for one source broker."""
        if policy not in self.VALID_UNROUTABLE_POLICIES:
            raise ValueError(
                f"Invalid unroutable policy '{policy}'. Must be one of {self.VALID_UNROUTABLE_POLICIES}."
            )
        if broker is None:
            self.unroutable_policy = policy
        else:
            self._unroutable_overrides[broker] = policy

    def set_firewall(self, firewall) -> None:
        """Installs (or with None removes) the ingress firewall evaluated before routing."""
        self.firewall = firewall
        for rule in getattr(firewall, "rules", []):
            if hasattr(rule, "bind_converter"):
                rule.bind_converter(self.converter)

    def set_dlq_budget(self, budget: BudgetedDlq) -> None:
        """Replaces the admission-control budget applied to DLQ writes."""
        self.dlq_budget = budget

    def _route_to_dlq(
        self,
        source_broker: str,
        source_topic: str,
        payload: bytes,
        error: str,
        dlq_broker: Optional[str] = None,
        dlq_topic: Optional[str] = None,
        ack_fn: Optional[Callable[[], None]] = None,
        nack_fn: Optional[Callable[..., None]] = None
    ) -> bool:
        """
        Dispatches a failed or unroutable message to the configured DLQ.
        Returns True if safely published to DLQ and acknowledged, False otherwise.
        """
        target_broker = dlq_broker or self.default_dlq_broker
        target_topic = dlq_topic or self.default_dlq_topic

        if not target_broker or not target_topic:
            return False

        # Anti-recursion protection: do not forward DLQ message to itself
        if source_broker == target_broker and source_topic == target_topic:
            logger.critical(f"DLQ recursion detected: message originated from DLQ {source_broker}/{source_topic}. Dropping.")
            self.metrics.record_error(source_broker, "dlq_recursion")
            if nack_fn:
                try:
                    nack_fn(requeue=False)
                except TypeError:
                    nack_fn()
            return True

        refusal = self.dlq_budget.check(source_broker, len(payload))
        if refusal:
            # Counted, not logged per message: a flood must not become a log flood too.
            self.metrics.record_dlq_refused(source_broker, refusal)
            logger.debug(f"DLQ budget refused message from {source_broker}/{source_topic}: {refusal}")
            return False

        try:
            dlq_bytes = format_dlq_payload(payload, source_broker, source_topic, error)
            if target_broker in self.clients:
                self.clients[target_broker].publish(target_topic, dlq_bytes)
                self.metrics.record_dlq(source_broker, target_broker, target_topic)
                logger.info(f"Dispatched failed/unroutable message from {source_broker}/{source_topic} to DLQ {target_broker}/{target_topic}")
                if ack_fn:
                    ack_fn()
                return True
            else:
                logger.error(f"DLQ broker '{target_broker}' not registered")
                self.metrics.record_error(source_broker, "dlq_broker_unregistered")
                return False
        except Exception as e:
            logger.error(f"Error publishing to DLQ {target_broker}/{target_topic}: {e}")
            self.metrics.record_error(source_broker, "dlq_publish_failure")
            return False

    def register_client(self, client: Any):
        self.clients[client.name] = client
        limiter = self._inflight.get(client.name)
        if limiter is not None:
            self._call_client(client.name, "set_inflight_limit", limiter.max_inflight)

    def _call_client(self, broker: str, hook: str, *args) -> None:
        method = getattr(self.clients.get(broker), hook, None)
        if method is not None:
            method(*args)

    def set_inflight_limit(
        self,
        broker: str,
        max_inflight: Optional[int],
        low_water: Optional[int] = None,
        lease_timeout: float = 60.0,
    ) -> None:
        """
        Caps un-confirmed messages from ``broker`` (None removes the cap). At the cap the client is
        paused (AMQP prefetch / Kafka pause) and resumed once in-flight falls to ``low_water``;
        sources that cannot be paused shed the excess.
        """
        previous = self._inflight.pop(broker, None)
        if max_inflight is None:
            if previous is not None:
                self._call_client(broker, "set_inflight_limit", None)
                if previous.paused:
                    self._call_client(broker, "resume_consumption")
            return
        self._inflight[broker] = InflightLimiter(
            max_inflight,
            low_water=low_water,
            lease_timeout=lease_timeout,
            on_saturate=lambda: self._call_client(broker, "pause_consumption"),
            on_drain=lambda: self._call_client(broker, "resume_consumption"),
        )
        self._call_client(broker, "set_inflight_limit", max_inflight)

    def inflight(self, broker: str) -> int:
        limiter = self._inflight.get(broker)
        return limiter.inflight if limiter is not None else 0
        logger.info(f"Registered client: {client.name}")

    def _topic_match(self, route_topic: str, message_topic: str) -> bool:
        """Helper to match MQTT style wildcards (+ for single level, # for multi-level). Kept for backward compatibility."""
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

        if source_client not in self.client_tries:
            self.client_tries[source_client] = TopicTrie()
        self.client_tries[source_client].insert(source_topic, route_config)

        logger.info(f"Added route: {source_client}/{source_topic} -> {target_client}/{target_topic} with kwargs {target_kwargs}")

    def _apply_ingress_verdict(self, source_client, verdict, ack_fn, nack_fn, start_time) -> None:
        self.metrics.record_ingress(source_client, verdict.rule or "unknown", verdict.action.value)
        if verdict.category and verdict.category != "rule_error":
            self.metrics.record_sender_event(source_client, verdict.category)
        self.metrics.record_duration(source_client, time.perf_counter() - start_time)
        logger.debug(f"Ingress {verdict.action.value} from {source_client} by {verdict.rule}: {verdict.reason}")
        if verdict.action == Action.DROP:
            if ack_fn:
                ack_fn()
        elif nack_fn:
            try:
                nack_fn(requeue=False)
            except TypeError:
                nack_fn()

    def route(
        self,
        source_client: str,
        source_topic: str,
        message: bytes,
        ack_fn: Optional[Callable[[], None]] = None,
        nack_fn: Optional[Callable[..., None]] = None,
        **source_kwargs
    ):
        limiter = self._inflight.get(source_client)
        if limiter is None:
            return self._route(source_client, source_topic, message, ack_fn, nack_fn, **source_kwargs)

        if ack_fn is None and "ack_fn" in source_kwargs:
            ack_fn = source_kwargs.pop("ack_fn")
        if nack_fn is None and "nack_fn" in source_kwargs:
            nack_fn = source_kwargs.pop("nack_fn")

        lease = limiter.acquire()
        if lease is None:
            self._shed_over_cap(source_client, nack_fn)
            return

        def _releasing(fn):
            if fn is None:
                return None

            def wrapped(*args, **kwargs):
                try:
                    return fn(*args, **kwargs)
                finally:
                    lease.release()
            return wrapped

        # With confirmation callbacks the slot is held until the message is acked or nacked;
        # without them (e.g. MQTT) it only covers the time spent inside route().
        has_callbacks = ack_fn is not None or nack_fn is not None
        try:
            return self._route(
                source_client, source_topic, message, _releasing(ack_fn), _releasing(nack_fn), **source_kwargs
            )
        except BaseException:
            lease.release()
            raise
        finally:
            if not has_callbacks:
                lease.release()

    def _shed_over_cap(self, source_client: str, nack_fn: Optional[Callable[..., None]]) -> None:
        self.metrics.record_error(source_client, "inflight_limit")
        logger.warning(f"In-flight cap reached for {source_client}; message not accepted")
        if nack_fn:
            # Requeue: the source is paused, so this redelivers once capacity frees.
            try:
                nack_fn(requeue=True)
            except TypeError:
                nack_fn()

    def _route(
        self,
        source_client: str,
        source_topic: str,
        message: bytes,
        ack_fn: Optional[Callable[[], None]] = None,
        nack_fn: Optional[Callable[..., None]] = None,
        **source_kwargs
    ):
        start_time = time.perf_counter()

        # Allow ack_fn and nack_fn to be passed via kwargs if needed
        if ack_fn is None and "ack_fn" in source_kwargs:
            ack_fn = source_kwargs.pop("ack_fn")
        if nack_fn is None and "nack_fn" in source_kwargs:
            nack_fn = source_kwargs.pop("nack_fn")

        logger.debug(f"Received message from {source_client} on {source_topic} ({len(message)} bytes)")

        if self.firewall is not None:
            verdict = self.firewall.evaluate(
                MessageContext(source_client, source_topic, len(message), message)
            )
            if verdict.action != Action.ALLOW:
                self._apply_ingress_verdict(source_client, verdict, ack_fn, nack_fn, start_time)
                return

        # Find all matching routes in O(k) time via TopicTrie
        client_trie = self.client_tries.get(source_client)
        matching_routes = client_trie.match(source_topic) if client_trie else []

        if not matching_routes:
            logger.debug(f"No routes found for {source_client}/{source_topic}")
            self.metrics.record_error(source_client, "unroutable")
            if self.firewall is not None:
                self.firewall.note(MessageContext(source_client, source_topic, len(message)), "unroutable")
                self.metrics.record_sender_event(source_client, "unroutable")
            self.metrics.record_duration(source_client, time.perf_counter() - start_time)

            policy = self._unroutable_overrides.get(source_client, self.unroutable_policy)
            if policy == "dlq" and self._route_to_dlq(
                source_client,
                source_topic,
                message,
                error="Unroutable message: no matching routes",
                ack_fn=ack_fn,
                nack_fn=nack_fn
            ):
                return
            if policy == "drop":
                if ack_fn:
                    ack_fn()
                self.metrics.record_error(source_client, "unroutable_dropped")
            elif nack_fn:
                # requeue=False: an unroutable message fails identically on redelivery.
                try:
                    nack_fn(requeue=False)
                except TypeError:
                    nack_fn()
                self.metrics.record_error(source_client, "unroutable_nacked")
            else:
                # Source cannot be pushed back (e.g. MQTT): the message is shed, but counted.
                self.metrics.record_error(source_client, "unroutable_dropped")
            return

        def _handle_dlq_on_error(exc: Optional[Exception], failed_route: Optional[Dict[str, Any]]) -> bool:
            err_msg = str(exc) if exc else "Publish failed"
            # Use the failing route's DLQ if configured on it, else fallback to global DLQ
            route_dlq_broker = failed_route.get("dlq_broker") if failed_route else None
            route_dlq_topic = failed_route.get("dlq_topic") if failed_route else None
            return self._route_to_dlq(
                source_client,
                source_topic,
                message,
                error=err_msg,
                dlq_broker=route_dlq_broker,
                dlq_topic=route_dlq_topic,
                ack_fn=ack_fn,
                nack_fn=nack_fn
            )

        has_callbacks = ack_fn is not None or nack_fn is not None
        coordinator = _FanoutCoordinator(
            len(matching_routes),
            ack_fn=ack_fn,
            nack_fn=nack_fn,
            dlq_fn=_handle_dlq_on_error
        ) if has_callbacks else None

        for route_config in matching_routes:
            target_client = route_config["target_client"]
            target_topic = route_config["target_topic"]

            # Exclude internal routing, DLQ, and schema metadata to pass clean **kwargs to publish
            excluded_keys = (
                "target_client", "target_topic", "dlq_broker", "dlq_topic",
                "input_format", "input_schema", "output_format", "output_schema",
                "wire_format", "wire_format_in", "wire_format_out"
            )
            raw_kwargs = {
                k: v for k, v in route_config.items()
                if k not in excluded_keys
            }
            publish_kwargs = _translate_semantics(source_kwargs, raw_kwargs)

            # Check if route specifies schema transformation
            input_format = route_config.get("input_format")
            output_format = route_config.get("output_format")
            input_schema = route_config.get("input_schema")
            output_schema = route_config.get("output_schema")
            wire_format = route_config.get("wire_format", False)
            wire_format_in = route_config.get("wire_format_in", wire_format if (input_format and input_format != "json") else False)
            wire_format_out = route_config.get("wire_format_out", wire_format if (output_format and output_format != "json") else False)

            needs_transform = bool(
                output_format or output_schema or
                (input_format and output_format and input_format != output_format)
            )

            out_message = message
            if needs_transform:
                in_fmt = input_format or "json"
                out_fmt = output_format or "json"
                try:
                    out_message = self.converter.convert(
                        message,
                        input_format=in_fmt,
                        output_format=out_fmt,
                        input_schema=input_schema,
                        output_schema=output_schema,
                        wire_format_in=wire_format_in,
                        wire_format_out=wire_format_out
                    )
                except Exception as e:
                    err = SchemaValidationError(f"Schema transformation error: {e}")
                    logger.error(f"Schema transformation failed for {source_client}/{source_topic} -> {target_client}/{target_topic}: {err}")
                    self.metrics.record_error(source_client, "schema_validation_error")
                    self.metrics.record_routed(source_client, target_client, status="error")
                    if coordinator:
                        coordinator.on_error(err, route_config)
                    else:
                        self._route_to_dlq(
                            source_client,
                            source_topic,
                            message,
                            error=str(err),
                            dlq_broker=route_config.get("dlq_broker"),
                            dlq_topic=route_config.get("dlq_topic"),
                            ack_fn=ack_fn,
                            nack_fn=nack_fn
                        )
                    continue

            if coordinator:
                publish_kwargs["on_confirm"] = coordinator.on_confirm
                publish_kwargs["on_error"] = (
                    lambda err, _c=coordinator, _rc=route_config: _c.on_error(err, _rc)
                )

            if target_client in self.clients:
                target_obj = self.clients[target_client]
                breaker = self.circuit_breakers.get(target_client)

                if breaker is not None and not breaker.allow_request():
                    self.metrics.record_error(source_client, "circuit_open")
                    # Attempt ring-buffer failover before dropping to DLQ
                    if hasattr(target_obj, "buffer_publish") and target_obj.buffer_publish(
                        target_topic,
                        out_message,
                        **publish_kwargs
                    ):
                        logger.warning(
                            f"Circuit breaker OPEN for {target_client}; buffered message to {target_topic} in publish buffer."
                        )
                        continue

                    exc = CircuitBreakerOpenError(f"Circuit breaker is OPEN for target broker '{target_client}'")
                    self.metrics.record_routed(source_client, target_client, status="error")
                    if coordinator:
                        coordinator.on_error(exc, route_config)
                    else:
                        self._route_to_dlq(
                            source_client,
                            source_topic,
                            message,
                            error=str(exc),
                            dlq_broker=route_config.get("dlq_broker"),
                            dlq_topic=route_config.get("dlq_topic")
                        )
                    continue

                orig_confirm = publish_kwargs.get("on_confirm")
                orig_error = publish_kwargs.get("on_error")
                error_recorded = [False]

                if breaker is not None:
                    def _wrapped_confirm(
                        _b=breaker, _t=target_obj, _oc=orig_confirm
                    ):
                        recovered = _b.record_success()
                        if _oc:
                            _oc()
                        if recovered and hasattr(_t, "flush_publish_buffer"):
                            _t.flush_publish_buffer()

                    def _wrapped_error(
                        err: Exception,
                        _b=breaker,
                        _oe=orig_error,
                        _er=error_recorded,
                        _rc=route_config
                    ):
                        _er[0] = True
                        _b.record_failure()
                        if _oe:
                            _oe(err)
                        else:
                            self._route_to_dlq(
                                source_client,
                                source_topic,
                                message,
                                error=str(err),
                                dlq_broker=_rc.get("dlq_broker"),
                                dlq_topic=_rc.get("dlq_topic")
                            )

                    publish_kwargs["on_confirm"] = _wrapped_confirm
                    publish_kwargs["on_error"] = _wrapped_error

                logger.debug(f"Routing to {target_client} on {target_topic} (publish args: {sorted(publish_kwargs)})")
                try:
                    target_obj.publish(target_topic, out_message, **publish_kwargs)
                    if not error_recorded[0]:
                        self.metrics.record_routed(source_client, target_client, status="success")
                    else:
                        self.metrics.record_routed(source_client, target_client, status="error")
                except Exception as e:
                    logger.error(f"Error publishing to {target_client}: {e}")
                    self.metrics.record_routed(source_client, target_client, status="error")
                    self.metrics.record_error(source_client, "publish_failure")
                    if breaker is not None and not error_recorded[0]:
                        breaker.record_failure()
                    if coordinator:
                        coordinator.on_error(e, route_config)
                    elif not error_recorded[0]:
                        self._route_to_dlq(
                            source_client,
                            source_topic,
                            message,
                            error=str(e),
                            dlq_broker=route_config.get("dlq_broker"),
                            dlq_topic=route_config.get("dlq_topic")
                        )
            else:
                logger.warning(f"Target client {target_client} not registered")
                self.metrics.record_error(source_client, "unregistered_target")
                exc = RuntimeError(f"Target client {target_client} not registered")
                if coordinator:
                    coordinator.on_error(exc, route_config)
                else:
                    self._route_to_dlq(
                        source_client,
                        source_topic,
                        message,
                        error=str(exc),
                        dlq_broker=route_config.get("dlq_broker"),
                        dlq_topic=route_config.get("dlq_topic")
                    )

        self.metrics.record_duration(source_client, time.perf_counter() - start_time)



