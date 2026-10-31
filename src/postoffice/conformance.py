import json
import pathlib
from typing import Any, Callable, Dict, List, Optional, Tuple

from postoffice.base_client import BaseClient
from postoffice.dlq import BudgetedDlq, dlq_fingerprint, format_dlq_payload
from postoffice.ingress.content import ContentGuard
from postoffice.ingress.firewall import Firewall
from postoffice.ingress.ledger import SenderLedger
from postoffice.ingress.rules import (
    Action,
    MaxPayloadBytes,
    MessageContext,
    RateLimit,
    TopicShape,
    topic_matches,
)
from postoffice.router import Router
from postoffice.schema.converters import (
    SchemaConverter,
    SchemaValidationError,
    decode_wire_format,
    encode_wire_format,
)
from postoffice.schema.registry import InMemorySchemaRegistry
from postoffice.signing import (
    HmacSigner,
    sign_event,
    sign_state,
    sign_version,
    verify_state,
)
from postoffice.trie import TopicTrie
from postoffice.validation import PayloadError, validate_action

EXPECTED_SUITES: Tuple[str, ...] = (
    "topic_matching",
    "firewall_verdicts",
    "dlq_envelopes",
    "wire_framing",
    "control_plane_signing",
    "router_delivery",
    "admin_api",
)

FIXED_TIMESTAMP = "2026-10-31T00:00:00+00:00"


class InMemoryBrokerClient(BaseClient):
    """Deterministic in-memory broker client for conformance vectors and reference server."""

    def __init__(self, name: str = "mock", fail_publish: bool = False, **kwargs: Any):
        self.name = name
        self.fail_publish = fail_publish
        self.kwargs = dict(kwargs)
        self.security_config = dict(kwargs.get("security_config") or {})
        self.connected = False
        self.published: List[Dict[str, Any]] = []
        self.subscriptions: Dict[str, Callable] = {}

    def connect(self) -> None:
        self.connected = True

    def disconnect(self) -> None:
        self.connected = False

    def publish(
        self,
        topic: str,
        payload: bytes,
        on_confirm: Optional[Callable[[], None]] = None,
        on_error: Optional[Callable[[Exception], None]] = None,
        **kwargs: Any,
    ) -> None:
        if self.fail_publish:
            err = RuntimeError(f"Simulated publish failure on {self.name}")
            if on_error is not None:
                on_error(err)
                return
            raise err
        clean_kwargs = {
            k: v for k, v in kwargs.items() if not callable(v)
        }
        self.published.append(
            {
                "topic": topic,
                "payload_hex": payload.hex(),
                "kwargs": clean_kwargs,
            }
        )
        if on_confirm is not None:
            on_confirm()

    def subscribe(self, topic: str, callback: Callable, **kwargs: Any) -> None:
        self.subscriptions[topic] = callback

    def reload_security(self, security_config: Dict[str, Any]) -> None:
        self.security_config = dict(security_config)


class PlatformReferenceServer:
    """In-memory reference server implementing the v1 Admin, Observe, and Stream contracts."""

    def __init__(self, bearer_token: str = ""):
        self.bearer_token = bearer_token
        self.version = 0
        self.schema_version = 1
        self.router = Router()
        self.brokers: Dict[str, InMemoryBrokerClient] = {}
        self.subscriptions: Dict[int, Dict[str, Any]] = {}
        self._next_sub_id = 1
        self._next_delivery_id = 1

    def check_auth(self, token: str) -> None:
        if self.bearer_token and token != self.bearer_token:
            raise PermissionError("Invalid bearer token")

    def _ack(self, message: str = "ok") -> Dict[str, Any]:
        self.version += 1
        return {
            "ok": True,
            "version": self.version,
            "schema_version": self.schema_version,
            "message": message,
        }

    def register_broker(
        self,
        name: str,
        broker_type: str,
        config: Dict[str, Any],
        security: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        client = InMemoryBrokerClient(
            name=name,
            broker_type=broker_type,
            security_config=security or {},
            **config,
        )
        client.connect()
        self.brokers[name] = client
        self.router.register_client(client)
        return self._ack(f"registered broker {name}")

    def remove_broker(self, name: str) -> Dict[str, Any]:
        client = self.brokers.pop(name, None)
        if client is not None:
            client.disconnect()
        self.router.clients.pop(name, None)
        return self._ack(f"removed broker {name}")

    def update_broker_security(
        self, name: str, security: Dict[str, Any]
    ) -> Dict[str, Any]:
        if name not in self.brokers:
            raise KeyError(f"Unknown broker: {name}")
        self.brokers[name].reload_security(security)
        return self._ack(f"updated security for {name}")

    def add_route(
        self,
        source_broker: str,
        source_topic: str,
        target_broker: Optional[str] = None,
        target_topic: Optional[str] = None,
        targets: Optional[List[Dict[str, str]]] = None,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        if targets:
            for t in targets:
                self.router.add_route(
                    source_broker,
                    source_topic,
                    t["broker"],
                    t["topic"],
                    **kwargs,
                )
        elif target_broker and target_topic:
            self.router.add_route(
                source_broker,
                source_topic,
                target_broker,
                target_topic,
                **kwargs,
            )
        else:
            raise ValueError("Either (target_broker, target_topic) or targets is required")
        return self._ack("route added")

    def remove_route(
        self,
        source_broker: str,
        source_topic: str,
        target_broker: Optional[str] = None,
        target_topic: Optional[str] = None,
    ) -> Dict[str, Any]:
        key = (source_broker, source_topic)
        if key in self.router.routes:
            if target_broker and target_topic:
                self.router.routes[key] = [
                    r
                    for r in self.router.routes[key]
                    if not (
                        r.get("target_client") == target_broker
                        and r.get("target_topic") == target_topic
                    )
                ]
            else:
                self.router.routes.pop(key, None)
        return self._ack("route removed")

    def add_subscription(
        self, broker: str, topic: str, qos: int = 0, auto_ack: bool = True
    ) -> Dict[str, Any]:
        if broker not in self.brokers:
            raise KeyError(f"Unknown broker: {broker}")
        self.brokers[broker].subscribe(
            topic,
            lambda t, p: self.router.route(broker, t, p),
            qos=qos,
            auto_ack=auto_ack,
        )
        return self._ack("subscription added")

    def set_ingress(self, config: Dict[str, Any]) -> Dict[str, Any]:
        fw = Firewall.from_dict(config)
        self.router.set_firewall(fw)
        return self._ack("ingress updated")

    def set_dlq(
        self, broker: str, topic: str, budget: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        self.router.set_dlq(broker, topic)
        if budget:
            self.router.set_dlq_budget(BudgetedDlq(**budget))
        return self._ack("dlq updated")

    def set_unroutable_policy(self, policy: str) -> Dict[str, Any]:
        self.router.set_unroutable_policy(policy)
        return self._ack("unroutable policy updated")

    def register_schema(
        self, schema_id: int, schema_type: str, definition: Any
    ) -> Dict[str, Any]:
        name = f"schema_{schema_id}"
        assigned_id = self.router.schema_registry.register_schema(
            name, definition, schema_type=schema_type
        )
        return self._ack(f"schema {assigned_id} registered")

    def get_schema(self, schema_id: int) -> Dict[str, Any]:
        entry = self.router.schema_registry.get_schema_by_id(schema_id)
        if entry is None:
            return {"schema_id": schema_id, "found": False}
        return {
            "schema_id": entry.schema_id,
            "schema_type": entry.schema_type,
            "definition": entry.definition,
            "found": True,
        }

    def clear_state(self) -> Dict[str, Any]:
        for client in self.brokers.values():
            client.disconnect()
        self.brokers.clear()
        self.router = Router()
        self.subscriptions.clear()
        return self._ack("state cleared")

    def get_health(self) -> Dict[str, Any]:
        return {"status": "ok", "uptime_ms": 1000, "version": "1.0.0"}

    def get_ready(self) -> Dict[str, Any]:
        return {
            "ready": True,
            "applied_version": self.version,
            "signing_mode": "permissive",
            "brokers": [
                {
                    "name": name,
                    "type": "mock",
                    "connected": client.connected,
                    "circuit_state": "CLOSED",
                }
                for name, client in sorted(self.brokers.items())
            ],
        }

    def get_sender_report(self, limit: int = 20) -> Dict[str, Any]:
        if self.router.firewall is None or self.router.firewall.ledger is None:
            return {"offenders": []}
        return {"offenders": self.router.firewall.ledger.sender_report(top=limit)}

    def get_stats(self) -> Dict[str, Any]:
        active_routes = sum(len(t) for t in self.router.client_tries.values())
        return {
            "applied_version": self.version,
            "active_brokers": len(self.brokers),
            "active_routes": active_routes,
            "memory_in_use_bytes": 0,
            "memory_budget_bytes": 67108864,
            "memory_shed_total": 0,
            "dlq_stats": {
                "tracked_sources": self.router.dlq_budget.tracked_sources,
                "tracked_fingerprints": self.router.dlq_budget.tracked_fingerprints,
            },
        }

    def get_metrics(self) -> Dict[str, Any]:
        return {
            "content_type": "text/plain; version=0.0.4; charset=utf-8",
            "prometheus_text": self.router.metrics.get_metrics_text(),
        }

    def subscribe(self, broker: str, topic_filter: str) -> int:
        sub_id = self._next_sub_id
        self._next_sub_id += 1
        self.subscriptions[sub_id] = {
            "broker": broker,
            "topic_filter": topic_filter,
            "queue": [],
            "acked": set(),
            "nacked": set(),
        }
        return sub_id

    def publish(
        self,
        broker: str,
        topic: str,
        payload: bytes,
        sender_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        meta = dict(metadata or {})
        if sender_id is not None:
            meta["sender"] = sender_id

        acked = [False]
        nacked = [False]

        before_counts = {
            name: len(c.published) for name, c in self.brokers.items()
        }
        self.router.route(
            broker,
            topic,
            payload,
            ack_fn=lambda: acked.__setitem__(0, True),
            nack_fn=lambda requeue=False: nacked.__setitem__(0, True),
            **meta,
        )
        after_counts = {
            name: len(c.published) for name, c in self.brokers.items()
        }
        delivered = sum(
            after_counts[n] - before_counts[n] for n in self.brokers
        )

        # Deliver to any matching stream subscriptions
        for name, client in self.brokers.items():
            new_msgs = client.published[before_counts[name] : after_counts[name]]
            for msg in new_msgs:
                for sub in self.subscriptions.values():
                    if sub["broker"] == name and topic_matches(
                        sub["topic_filter"], msg["topic"]
                    ):
                        did = self._next_delivery_id
                        self._next_delivery_id += 1
                        sub["queue"].append(
                            {
                                "delivery_id": did,
                                "source_broker": name,
                                "source_topic": msg["topic"],
                                "payload_hex": msg["payload_hex"],
                                "timestamp_ns": 1761868800000000000,
                                "metadata": msg["kwargs"],
                            }
                        )

        accepted = acked[0] and not nacked[0]
        return {
            "accepted": accepted,
            "firewall_verdict": "allow" if accepted else "reject",
            "rule": "",
            "reason": "",
            "targets_delivered": delivered,
            "targets_failed": 0 if accepted else 1,
            "dlq_recorded": False,
        }

    def poll_subscription(self, sub_id: int) -> List[Dict[str, Any]]:
        sub = self.subscriptions[sub_id]
        frames = list(sub["queue"])
        sub["queue"].clear()
        return frames

    def ack_delivery(self, sub_id: int, delivery_id: int) -> Dict[str, Any]:
        sub = self.subscriptions[sub_id]
        sub["acked"].add(delivery_id)
        return {"delivery_id": delivery_id, "acked": True}

    def nack_delivery(
        self, sub_id: int, delivery_id: int, requeue: bool = True
    ) -> Dict[str, Any]:
        sub = self.subscriptions[sub_id]
        sub["nacked"].add(delivery_id)
        return {"delivery_id": delivery_id, "nacked": True, "requeue": requeue}


class PlatformReferenceClient:
    """Reference client SDK wrapper over `PlatformReferenceServer`."""

    def __init__(self, server: PlatformReferenceServer, bearer_token: str = ""):
        self.server = server
        self.bearer_token = bearer_token

    def _auth(self) -> None:
        self.server.check_auth(self.bearer_token)

    def register_broker(
        self,
        name: str,
        broker_type: str,
        config: Dict[str, Any],
        security: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._auth()
        return self.server.register_broker(name, broker_type, config, security)

    def remove_broker(self, name: str) -> Dict[str, Any]:
        self._auth()
        return self.server.remove_broker(name)

    def update_broker_security(
        self, name: str, security: Dict[str, Any]
    ) -> Dict[str, Any]:
        self._auth()
        return self.server.update_broker_security(name, security)

    def add_route(self, **kwargs: Any) -> Dict[str, Any]:
        self._auth()
        return self.server.add_route(**kwargs)

    def remove_route(self, **kwargs: Any) -> Dict[str, Any]:
        self._auth()
        return self.server.remove_route(**kwargs)

    def add_subscription(
        self, broker: str, topic: str, qos: int = 0, auto_ack: bool = True
    ) -> Dict[str, Any]:
        self._auth()
        return self.server.add_subscription(broker, topic, qos=qos, auto_ack=auto_ack)

    def set_ingress(self, config: Dict[str, Any]) -> Dict[str, Any]:
        self._auth()
        return self.server.set_ingress(config)

    def set_dlq(
        self, broker: str, topic: str, budget: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        self._auth()
        return self.server.set_dlq(broker, topic, budget=budget)

    def set_unroutable_policy(self, policy: str) -> Dict[str, Any]:
        self._auth()
        return self.server.set_unroutable_policy(policy)

    def register_schema(
        self, schema_id: int, schema_type: str, definition: Any
    ) -> Dict[str, Any]:
        self._auth()
        return self.server.register_schema(schema_id, schema_type, definition)

    def get_schema(self, schema_id: int) -> Dict[str, Any]:
        self._auth()
        return self.server.get_schema(schema_id)

    def clear_state(self) -> Dict[str, Any]:
        self._auth()
        return self.server.clear_state()

    def get_health(self) -> Dict[str, Any]:
        return self.server.get_health()

    def get_ready(self) -> Dict[str, Any]:
        return self.server.get_ready()

    def get_sender_report(self, limit: int = 20) -> Dict[str, Any]:
        self._auth()
        return self.server.get_sender_report(limit=limit)

    def get_stats(self) -> Dict[str, Any]:
        self._auth()
        return self.server.get_stats()

    def get_metrics(self) -> Dict[str, Any]:
        return self.server.get_metrics()

    def subscribe(self, broker: str, topic_filter: str) -> int:
        self._auth()
        return self.server.subscribe(broker, topic_filter)

    def publish(
        self,
        broker: str,
        topic: str,
        payload: bytes,
        sender_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        self._auth()
        return self.server.publish(
            broker, topic, payload, sender_id=sender_id, metadata=metadata
        )

    def poll_subscription(self, sub_id: int) -> List[Dict[str, Any]]:
        self._auth()
        return self.server.poll_subscription(sub_id)

    def ack_delivery(self, sub_id: int, delivery_id: int) -> Dict[str, Any]:
        self._auth()
        return self.server.ack_delivery(sub_id, delivery_id)

    def nack_delivery(
        self, sub_id: int, delivery_id: int, requeue: bool = True
    ) -> Dict[str, Any]:
        self._auth()
        return self.server.nack_delivery(sub_id, delivery_id, requeue=requeue)


# ============================================================================
# Suite 1: Topic Matching Vectors
# ============================================================================


def _generate_topic_matching_vectors() -> Dict[str, Any]:
    patterns = [
        ("exact_temp", "sensors/room1/temp"),
        ("single_plus", "sensors/+/temp"),
        ("multi_hash", "sensors/#"),
        ("root_hash", "#"),
        ("root_plus_two", "+/+/temp"),
        ("sys_multi", "$SYS/#"),
        ("sys_plus", "$SYS/+/clients"),
        ("leading_slash", "/leading/+"),
        ("trailing_slash", "trailing/+/"),
    ]
    trie = TopicTrie()
    for route_id, pat in patterns:
        trie.insert(pat, route_id)

    probe_topics = [
        "sensors/room1/temp",
        "sensors/room2/temp",
        "sensors/room1/humidity",
        "sensors",
        "alerts/fire/temp",
        "$SYS/broker/clients",
        "$SYS/broker/load/1min",
        "$custom/x/temp",
        "/leading/seg",
        "trailing/seg/",
    ]
    trie_cases = []
    for topic in probe_topics:
        matched = sorted(trie.match(topic))
        trie_cases.append({"topic": topic, "matched_routes": matched})

    invalid_patterns = ["sensors/#/temp", "sensors/+room/temp", "sensors/#room"]
    invalid_cases = []
    for bad in invalid_patterns:
        try:
            t = TopicTrie()
            t.insert(bad, "x")
            valid = True
        except ValueError:
            valid = False
        invalid_cases.append({"pattern": bad, "valid": valid})

    pair_probes = [
        ("#", "sensors/temp"),
        ("#", "$SYS/broker/load"),
        ("+/broker/load", "$SYS/broker/load"),
        ("$SYS/#", "$SYS/broker/load"),
        ("$SYS/+/load", "$SYS/broker/load"),
        ("a/+/c", "a/b/c"),
        ("a/+/c", "a/b/d"),
        ("a/#", "a"),
    ]
    pair_cases = [
        {
            "pattern": pat,
            "topic": top,
            "matches": topic_matches(pat, top),
        }
        for pat, top in pair_probes
    ]

    return {
        "schema_version": 1,
        "suite": "topic_matching",
        "registered_patterns": [
            {"route_id": rid, "pattern": pat} for rid, pat in patterns
        ],
        "trie_cases": trie_cases,
        "invalid_patterns": invalid_cases,
        "pair_cases": pair_cases,
    }


# ============================================================================
# Suite 2: Firewall, ContentGuard & SenderLedger Vectors
# ============================================================================


def _generate_firewall_vectors() -> Dict[str, Any]:
    now = [1000.0]
    clock = lambda: now[0]

    ledger = SenderLedger(
        window=60.0,
        identity_template="devices/{device}/#",
        enforce=True,
        strike_threshold=3,
        cooldown_base=30.0,
        cooldown_factor=2.0,
        cooldown_cap=300.0,
        clock=clock,
    )
    rules = [
        MaxPayloadBytes(max_bytes=64, action=Action.REJECT),
        TopicShape(
            max_length=64,
            max_depth=5,
            deny=["$SYS/#", "+/admin"],
            action=Action.REJECT,
        ),
        RateLimit(
            rate=2.0,
            burst=2.0,
            key="identity",
            template="devices/{device}/#",
            action=Action.DROP,
            clock=clock,
        ),
        ContentGuard(
            topic="devices/+/telemetry",
            format="json",
            max_bytes=64,
            max_depth=4,
            max_keys=16,
            max_array_length=8,
            required_fields=["temp", "unit"],
            field_ranges={"temp": {"min": -40.0, "max": 125.0}},
            enums={"unit": ["C", "F"]},
            action=Action.REJECT,
        ),
    ]
    fw = Firewall(rules=rules, ledger=ledger)

    sequence = [
        {
            "step": "valid_message_1",
            "advance_sec": 0.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-a/telemetry",
            "payload_utf8": '{"temp": 22.5, "unit": "C"}',
        },
        {
            "step": "oversize_payload",
            "advance_sec": 0.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-b/telemetry",
            "payload_utf8": '{"temp": 22.5, "unit": "C", "pad": "' + ("x" * 50) + '"}',
        },
        {
            "step": "denied_topic_admin",
            "advance_sec": 0.0,
            "broker": "mqtt_edge",
            "topic": "devices/admin",
            "payload_utf8": '{"temp": 22.5, "unit": "C"}',
        },
        {
            "step": "denied_sys_topic",
            "advance_sec": 0.0,
            "broker": "mqtt_edge",
            "topic": "$SYS/broker/load",
            "payload_utf8": '{"temp": 22.5, "unit": "C"}',
        },
        {
            "step": "content_out_of_range",
            "advance_sec": 0.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-c/telemetry",
            "payload_utf8": '{"temp": 999.0, "unit": "C"}',
        },
        {
            "step": "rate_limit_first_allowed",
            "advance_sec": 0.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-r/telemetry",
            "payload_utf8": '{"temp": 20.0, "unit": "C"}',
        },
        {
            "step": "rate_limit_second_allowed",
            "advance_sec": 0.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-r/telemetry",
            "payload_utf8": '{"temp": 21.0, "unit": "C"}',
        },
        {
            "step": "rate_limit_exceeded_drop",
            "advance_sec": 0.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-r/telemetry",
            "payload_utf8": '{"temp": 22.0, "unit": "C"}',
        },
        {
            "step": "quarantine_strike_1",
            "advance_sec": 1.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-q/telemetry",
            "payload_utf8": "not-json",
        },
        {
            "step": "quarantine_strike_2",
            "advance_sec": 1.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-q/telemetry",
            "payload_utf8": "not-json",
        },
        {
            "step": "quarantine_strike_3_triggers_cooldown",
            "advance_sec": 1.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-q/telemetry",
            "payload_utf8": "not-json",
        },
        {
            "step": "quarantined_sender_blocked_even_on_valid_payload",
            "advance_sec": 1.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-q/telemetry",
            "payload_utf8": '{"temp": 22.5, "unit": "C"}',
        },
        {
            "step": "quarantine_expired_after_cooldown",
            "advance_sec": 35.0,
            "broker": "mqtt_edge",
            "topic": "devices/dev-q/telemetry",
            "payload_utf8": '{"temp": 22.5, "unit": "C"}',
        },
    ]

    results = []
    for item in sequence:
        now[0] += item["advance_sec"]
        raw = item["payload_utf8"].encode("utf-8")
        ctx = MessageContext(
            broker=item["broker"],
            topic=item["topic"],
            size=len(raw),
            payload=raw,
        )
        verdict = fw.evaluate(ctx)
        results.append(
            {
                "step": item["step"],
                "advance_sec": item["advance_sec"],
                "broker": item["broker"],
                "topic": item["topic"],
                "payload_utf8": item["payload_utf8"],
                "expected_verdict": {
                    "action": verdict.action.value,
                    "rule": verdict.rule,
                    "category": verdict.category,
                    "reason": verdict.reason,
                },
            }
        )

    return {
        "schema_version": 1,
        "suite": "firewall_verdicts",
        "steps": results,
        "final_sender_report": ledger.sender_report(top=10),
    }


# ============================================================================
# Suite 3: DLQ Envelopes, Fingerprints & BudgetedDlq Vectors
# ============================================================================


def _generate_dlq_vectors() -> Dict[str, Any]:
    utf8_env = json.loads(
        format_dlq_payload(
            b'{"sensor": 1}',
            "mqtt_edge",
            "sensors/1",
            "Broker down",
            metadata={"attempt": 1},
            timestamp=FIXED_TIMESTAMP,
        ).decode("utf-8")
    )
    bin_env = json.loads(
        format_dlq_payload(
            b"\xff\xfe\xfd\x00",
            "amqp_core",
            "bin/topic",
            "Decode error",
            metadata={},
            timestamp=FIXED_TIMESTAMP,
        ).decode("utf-8")
    )

    fp_cases = []
    for broker, topic, err, payload in [
        ("mqtt_edge", "sensors/1", "Broker down", b'{"sensor": 1}'),
        ("amqp_core", "bin/topic", "Decode error", b"\xff\xfe\xfd\x00"),
        ("kafka_stream", "orders", "Schema mismatch", b"A" * 5000),
    ]:
        fp_cases.append(
            {
                "source_broker": broker,
                "source_topic": topic,
                "error": err,
                "payload_hex": payload.hex(),
                "fingerprint": dlq_fingerprint(broker, topic, err, payload),
            }
        )

    now = [2000.0]
    budget = BudgetedDlq(
        max_message_bytes=32,
        msgs_per_sec=10.0,
        bytes_per_sec=100_000.0,
        dedupe_window=10.0,
        clock=lambda: now[0],
    )

    budget_steps = []
    for step_name, dt, payload, meta_only in [
        ("first_write_admitted", 0.0, b"small-payload", False),
        ("duplicate_within_window_suppressed", 1.0, b"small-payload", False),
        ("second_duplicate_suppressed", 1.0, b"small-payload", False),
        ("after_window_admitted_with_repeat_count", 11.0, b"small-payload", False),
        ("oversize_payload_refused", 1.0, b"X" * 64, False),
        ("oversize_metadata_only_admitted", 0.0, b"X" * 64, True),
    ]:
        now[0] += dt
        fp = dlq_fingerprint("mqtt_edge", "sensors/temp", "Timeout", payload)
        adm = budget.admit(
            "mqtt_edge",
            size=len(payload),
            fingerprint=fp,
            metadata_only=meta_only,
        )
        budget_steps.append(
            {
                "step": step_name,
                "advance_sec": dt,
                "payload_hex": payload.hex(),
                "metadata_only": meta_only,
                "refusal": adm.refusal,
                "repeat_count": adm.repeat_count,
            }
        )

    return {
        "schema_version": 1,
        "suite": "dlq_envelopes",
        "utf8_envelope": utf8_env,
        "binary_envelope": bin_env,
        "fingerprint_cases": fp_cases,
        "budget_steps": budget_steps,
    }


# ============================================================================
# Suite 4: Confluent Wire Framing & Schema Conversion Vectors
# ============================================================================


def _generate_wire_framing_vectors() -> Dict[str, Any]:
    reg = InMemorySchemaRegistry()
    conv = SchemaConverter(reg)
    avro_schema = {
        "type": "record",
        "name": "Reading",
        "fields": [
            {"name": "id", "type": "string"},
            {"name": "temp", "type": "double"},
        ],
    }
    proto_schema = {
        "name": "ReadingProto",
        "fields": [
            {"name": "id", "type": "string", "number": 1},
            {"name": "temp", "type": "double", "number": 2},
        ],
    }
    id_avro = reg.register_schema("reading_avro", avro_schema, "AVRO")
    id_proto = reg.register_schema("reading_proto", proto_schema, "PROTOBUF")

    sample_obj = {"id": "dev-1", "temp": 21.5}
    raw_json = json.dumps(sample_obj, sort_keys=True).encode("utf-8")
    framed_json = encode_wire_format(raw_json, 1)
    framed_avro = conv.serialize(
        sample_obj, "avro", schema_name="reading_avro", wire_format=True
    )
    framed_proto = conv.serialize(
        sample_obj, "protobuf", schema_name="reading_proto", wire_format=True
    )
    decoded_avro = conv.deserialize(framed_avro, "avro", wire_format=True)
    decoded_proto = conv.deserialize(framed_proto, "protobuf", wire_format=True)

    error_cases = []
    for name, raw in [
        ("too_short", b"\x00\x00\x01"),
        ("bad_magic_byte", b"\x01\x00\x00\x00\x01{}"),
    ]:
        try:
            decode_wire_format(raw)
            err = None
        except SchemaValidationError as exc:
            err = str(exc)
        error_cases.append(
            {"name": name, "raw_hex": raw.hex(), "raises_error": err is not None}
        )

    return {
        "schema_version": 1,
        "suite": "wire_framing",
        "schemas": [
            {"schema_id": id_avro, "name": "reading_avro", "type": "AVRO", "definition": avro_schema},
            {"schema_id": id_proto, "name": "reading_proto", "type": "PROTOBUF", "definition": proto_schema},
        ],
        "sample_object": sample_obj,
        "framed_json_hex": framed_json.hex(),
        "framed_avro_hex": framed_avro.hex(),
        "framed_proto_hex": framed_proto.hex(),
        "decoded_avro": decoded_avro,
        "decoded_proto": decoded_proto,
        "error_cases": error_cases,
    }


# ============================================================================
# Suite 5: Control-Plane D9 Signing & Action Validation Vectors
# ============================================================================


def _generate_control_plane_signing_vectors() -> Dict[str, Any]:
    keyring_str = "k2:second-secret-key,k1:first-secret-key"
    signer = HmacSigner.from_env({"POSTOFFICE_EVENT_KEYS": keyring_str})
    assert signer is not None
    event_body = json.dumps(
        {
            "action": "add_route",
            "data": {
                "source_broker": "mqtt_a",
                "source_topic": "sensors/+",
                "target_broker": "amqp_b",
                "target_topic": "ingest",
                "target_kwargs": {},
            },
            "version": 7,
            "schema_version": 1,
        },
        sort_keys=True,
    )
    signed_event = json.loads(sign_event(signer, event_body))
    signed_state = json.loads(
        sign_state(signer, "postoffice:routes", "route_1", event_body)
    )
    signed_ver = json.loads(sign_version(signer, 7))

    validation_cases = [
        {
            "name": "valid_add_broker",
            "action": "add_broker",
            "data": {"name": "mqtt_1", "type": "mqtt", "kwargs": {"host": "127.0.0.1"}},
            "valid": True,
        },
        {
            "name": "forbidden_broker_kwarg_on_message_callback",
            "action": "add_broker",
            "data": {
                "name": "mqtt_1",
                "type": "mqtt",
                "kwargs": {"on_message_callback": "evil"},
            },
            "valid": False,
        },
        {
            "name": "forbidden_tls_verify_disable",
            "action": "update_broker_security",
            "data": {
                "name": "mqtt_1",
                "security_config": {"tls": True, "tls_insecure": True},
            },
            "valid": False,
        },
        {
            "name": "valid_add_route",
            "action": "add_route",
            "data": {
                "source_broker": "mqtt_1",
                "source_topic": "sensors/+",
                "target_broker": "amqp_1",
                "target_topic": "ingest",
                "target_kwargs": {"semantic_translation": True},
            },
            "valid": True,
        },
    ]

    for vc in validation_cases:
        try:
            validate_action(vc["action"], vc["data"])
            actual_valid = True
        except PayloadError:
            actual_valid = False
        vc["actual_valid"] = actual_valid

    return {
        "schema_version": 1,
        "suite": "control_plane_signing",
        "keyring": "k2:second-secret-key,k1:first-secret-key",
        "event_body": event_body,
        "signed_event_envelope": signed_event,
        "signed_state_envelope": signed_state,
        "signed_version_envelope": signed_ver,
        "cross_domain_event_as_state_rejected": not verify_state(
            signer, "postoffice:routes", "route_1", signed_event
        ),
        "validation_cases": validation_cases,
    }


# ============================================================================
# Suite 6: Router End-to-End Delivery, Fan-Out & Unroutable Policy Vectors
# ============================================================================


def _generate_router_delivery_vectors() -> Dict[str, Any]:
    cases = []

    for policy in ("nack", "dlq", "drop"):
        router = Router()
        src = InMemoryBrokerClient("src")
        dlq_client = InMemoryBrokerClient("dlq_b")
        router.register_client(src)
        router.register_client(dlq_client)
        router.set_dlq("dlq_b", "dlq.topic")
        router.set_unroutable_policy(policy)
        acked = [False]
        nacked = [False]
        router.route(
            "src",
            "unmatched/topic",
            b"hello",
            ack_fn=lambda: acked.__setitem__(0, True),
            nack_fn=lambda requeue=False: nacked.__setitem__(0, True),
        )
        cases.append(
            {
                "case": f"unroutable_policy_{policy}",
                "policy": policy,
                "acked": acked[0],
                "nacked": nacked[0],
                "dlq_count": len(dlq_client.published),
            }
        )

    # Fan-out + semantic translation
    router = Router()
    src = InMemoryBrokerClient("mqtt_src")
    t1 = InMemoryBrokerClient("amqp_t1")
    t2 = InMemoryBrokerClient("kafka_t2")
    router.register_client(src)
    router.register_client(t1)
    router.register_client(t2)
    router.add_route(
        "mqtt_src",
        "telemetry/+",
        "amqp_t1",
        "amqp.telemetry",
        semantic_translation=True,
    )
    router.add_route(
        "mqtt_src",
        "telemetry/+",
        "kafka_t2",
        "kafka.telemetry",
        semantic_translation=True,
    )
    acked = [False]
    router.route(
        "mqtt_src",
        "telemetry/dev1",
        b'{"v":1}',
        ack_fn=lambda: acked.__setitem__(0, True),
        qos=1,
    )
    cases.append(
        {
            "case": "fanout_with_semantic_translation",
            "acked": acked[0],
            "t1_published": t1.published,
            "t2_published": t2.published,
        }
    )

    return {
        "schema_version": 1,
        "suite": "router_delivery",
        "cases": cases,
    }


# ============================================================================
# Suite 7: Platform API (`Admin`, `Observe`, `Stream`) Vectors
# ============================================================================


def _generate_admin_api_vectors() -> Dict[str, Any]:
    server = PlatformReferenceServer(bearer_token="conformance-token")
    client = PlatformReferenceClient(server, bearer_token="conformance-token")

    transcript = []
    transcript.append(
        {"op": "RegisterBroker:mqtt_in", "resp": client.register_broker("mqtt_in", "mock", {})}
    )
    transcript.append(
        {"op": "RegisterBroker:amqp_out", "resp": client.register_broker("amqp_out", "mock", {})}
    )
    transcript.append(
        {
            "op": "UpdateBrokerSecurity:mqtt_in",
            "resp": client.update_broker_security("mqtt_in", {"tls": {"enabled": True}}),
        }
    )
    transcript.append(
        {
            "op": "AddRoute",
            "resp": client.add_route(
                source_broker="mqtt_in",
                source_topic="Factory/+/temp",
                target_broker="amqp_out",
                target_topic="enterprise/temp",
                semantic_translation=True,
            ),
        }
    )
    transcript.append(
        {
            "op": "SetUnroutablePolicy",
            "resp": client.set_unroutable_policy("drop"),
        }
    )
    sub_id = client.subscribe("amqp_out", "enterprise/#")
    transcript.append(
        {
            "op": "Publish",
            "resp": client.publish(
                broker="mqtt_in",
                topic="Factory/line1/temp",
                payload=b'{"c":25.0}',
                sender_id="plc-1",
                metadata={"qos": 1},
            ),
        }
    )
    frames = client.poll_subscription(sub_id)
    transcript.append({"op": "PollSubscription", "resp": frames})
    transcript.append(
        {"op": "AckDelivery", "resp": client.ack_delivery(sub_id, frames[0]["delivery_id"])}
    )
    transcript.append({"op": "GetHealth", "resp": client.get_health()})
    transcript.append({"op": "GetReady", "resp": client.get_ready()})
    transcript.append({"op": "GetStats", "resp": client.get_stats()})

    return {
        "schema_version": 1,
        "suite": "admin_api",
        "transcript": transcript,
    }


def generate_all_vectors() -> Dict[str, Dict[str, Any]]:
    return {
        "topic_matching": _generate_topic_matching_vectors(),
        "firewall_verdicts": _generate_firewall_vectors(),
        "dlq_envelopes": _generate_dlq_vectors(),
        "wire_framing": _generate_wire_framing_vectors(),
        "control_plane_signing": _generate_control_plane_signing_vectors(),
        "router_delivery": _generate_router_delivery_vectors(),
        "admin_api": _generate_admin_api_vectors(),
    }


def write_vectors(output_dir: pathlib.Path) -> List[pathlib.Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: List[pathlib.Path] = []
    for suite_name, payload in generate_all_vectors().items():
        path = output_dir / f"{suite_name}.json"
        path.write_text(
            json.dumps(payload, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        written.append(path)
    return written


def run_conformance(vectors_dir: pathlib.Path) -> Tuple[int, int, List[str]]:
    """
    Executes the Python implementation against the committed JSON vectors in `vectors_dir`
    and verifies 100% behavioral parity.
    """
    passed = 0
    failed = 0
    errors: List[str] = []
    fresh = generate_all_vectors()

    for suite_name in EXPECTED_SUITES:
        path = vectors_dir / f"{suite_name}.json"
        if not path.is_file():
            failed += 1
            errors.append(f"Missing suite file: {path}")
            continue

        disk_data = json.loads(path.read_text(encoding="utf-8"))
        live_data = fresh[suite_name]

        for key in (
            "trie_cases",
            "invalid_patterns",
            "pair_cases",
            "steps",
            "fingerprint_cases",
            "budget_steps",
            "error_cases",
            "validation_cases",
            "cases",
            "transcript",
        ):
            if key in disk_data and isinstance(disk_data[key], list):
                for idx, expected_item in enumerate(disk_data[key]):
                    actual_item = live_data[key][idx]
                    if expected_item == actual_item:
                        passed += 1
                    else:
                        failed += 1
                        errors.append(
                            f"{suite_name}.{key}[{idx}] mismatch: expected {expected_item}, got {actual_item}"
                        )

        if disk_data != live_data:
            failed += 1
            errors.append(f"Top-level mismatch in suite {suite_name}")
        else:
            passed += 1

    return passed, failed, errors
