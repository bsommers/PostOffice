"""
Validation of Control Plane payloads before they reach plugin constructors or the router.

Events and stored state both arrive from Redis, so both are untrusted. Error messages name keys
and expected types only, never values: payloads can carry credentials (``security_config``).
"""
import inspect
import re
import typing
from typing import Any, Dict, Set

import postoffice.plugins  # noqa: F401  (registers the built-in protocols)
from postoffice.registry import ClientRegistry

ACTIONS = ("add_broker", "add_route", "add_subscription", "update_broker_security", "clear_state",
           "set_ingress")

NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:\-]{0,127}\Z")
KEY_PATTERN = re.compile(r"[A-Za-z_][A-Za-z0-9_\-]{0,63}\Z")
MAX_TOPIC_LENGTH = 256   # same defaults as the ingress TopicShape rule
MAX_TOPIC_DEPTH = 16

# Accepted by Router/PostOffice.add_broker itself rather than by the plugin constructor.
FACADE_KWARGS = {"security_config": dict, "circuit_breaker_threshold": int, "circuit_breaker_timeout": float}
_SKIPPED_PARAMS = {"self", "name", "router"}


class PayloadError(ValueError):
    """A Control Plane payload failed validation. The message never contains payload values."""


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise PayloadError(message)


def _check_dict(data: Any, what: str) -> Dict[str, Any]:
    _require(isinstance(data, dict), f"{what} must be an object")
    return data


def _check_keys(data: Dict[str, Any], required: Set[str], what: str) -> None:
    missing = sorted(required - data.keys())
    _require(not missing, f"{what} is missing: {', '.join(missing)}")


def _check_name(value: Any, what: str) -> str:
    _require(isinstance(value, str) and NAME_PATTERN.match(value) is not None, f"{what} is not a valid name")
    return value


def _check_topic(value: Any, what: str) -> str:
    _require(isinstance(value, str) and value != "", f"{what} must be a non-empty string")
    _require("\x00" not in value, f"{what} contains a null byte")
    _require(len(value) <= MAX_TOPIC_LENGTH, f"{what} is longer than {MAX_TOPIC_LENGTH}")
    _require(value.count("/") + 1 <= MAX_TOPIC_DEPTH, f"{what} is deeper than {MAX_TOPIC_DEPTH} levels")
    return value


def _check_options(options: Any, what: str) -> None:
    """Free-form per-route / per-subscription options: an object with identifier-like keys."""
    _check_dict(options, what)
    for key in options:
        _require(isinstance(key, str) and KEY_PATTERN.match(key) is not None, f"{what} has an invalid key")


def _scalar_type(param: inspect.Parameter) -> Any:
    """str/int/float/bool from the annotation (``Optional[int]`` counts as int) or the default; else None."""
    annotation = param.annotation
    if typing.get_origin(annotation) is typing.Union:
        args = [a for a in typing.get_args(annotation) if a is not type(None)]
        annotation = args[0] if len(args) == 1 else None
    if annotation in (str, int, float, bool):
        return annotation
    default = param.default
    return type(default) if isinstance(default, (str, int, float, bool)) else None


def plugin_kwargs(protocol: str) -> Dict[str, Any]:
    """
    The keyword arguments a protocol accepts, mapped to the type of their default (or None when the
    type is unconstrained). Derived from the constructor signatures along the class hierarchy, so a
    plugin author never maintains a second list.
    """
    accepted: Dict[str, Any] = {}
    for klass in ClientRegistry.get_client(protocol).__mro__:
        init = klass.__dict__.get("__init__")
        if init is None:
            continue
        for pname, param in inspect.signature(init).parameters.items():
            if pname in _SKIPPED_PARAMS or param.kind in (param.VAR_KEYWORD, param.VAR_POSITIONAL):
                continue
            accepted.setdefault(pname, _scalar_type(param))
    for key, expected in FACADE_KWARGS.items():
        accepted[key] = expected
    return accepted


def _type_ok(value: Any, expected: Any) -> bool:
    if value is None or expected is None:
        return True
    if expected is bool:
        return isinstance(value, bool)
    if isinstance(value, bool):
        return False  # True is not a port number
    if expected is float:
        return isinstance(value, (int, float))
    return isinstance(value, expected)


def _validate_broker(data: Dict[str, Any]) -> None:
    _check_keys(data, {"name", "protocol", "kwargs"}, "add_broker")
    _check_name(data["name"], "broker name")
    protocol = data["protocol"]
    _require(isinstance(protocol, str), "protocol must be a string")
    try:
        accepted = plugin_kwargs(protocol)
    except ValueError:
        raise PayloadError("protocol is not a registered plugin") from None
    kwargs = _check_dict(data["kwargs"], "broker kwargs")
    for key, value in kwargs.items():
        _require(key in accepted, f"broker kwarg {key!r} is not accepted by protocol {protocol!r}")
        _require(_type_ok(value, accepted[key]), f"broker kwarg {key!r} has the wrong type")


def _validate_route(data: Dict[str, Any]) -> None:
    _check_keys(data, {"source_broker", "source_topic", "target_broker", "target_topic", "target_kwargs"},
                "add_route")
    _check_name(data["source_broker"], "source_broker")
    _check_name(data["target_broker"], "target_broker")
    _check_topic(data["source_topic"], "source_topic")
    _check_topic(data["target_topic"], "target_topic")
    _check_options(data["target_kwargs"], "target_kwargs")


def _validate_subscription(data: Dict[str, Any]) -> None:
    _check_keys(data, {"broker_name", "topic", "kwargs"}, "add_subscription")
    _check_name(data["broker_name"], "broker_name")
    _check_topic(data["topic"], "topic")
    _check_options(data["kwargs"], "subscription kwargs")


def _validate_security_update(data: Dict[str, Any]) -> None:
    _check_keys(data, {"name", "security_config"}, "update_broker_security")
    _check_name(data["name"], "broker name")
    _check_dict(data["security_config"], "security_config")


INGRESS_KEYS = {"rules", "on_rule_error", "ledger", "inflight"}
MAX_INGRESS_RULES = 64
INFLIGHT_KEYS = {"max_inflight", "low_water", "lease_timeout"}


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _validate_inflight(inflight: Any) -> None:
    _check_dict(inflight, "inflight")
    for broker, limit in inflight.items():
        _check_name(broker, "inflight broker name")
        _check_dict(limit, "inflight limit")
        _require(set(limit) <= INFLIGHT_KEYS, "inflight limit has an unknown key")
        maximum = limit.get("max_inflight")
        _require(_is_int(maximum) and maximum > 0, "max_inflight must be a positive integer")
        low = limit.get("low_water")
        _require(low is None or (_is_int(low) and 0 <= low < maximum), "low_water must be in 0..max_inflight-1")
        lease = limit.get("lease_timeout")
        _require(lease is None or (isinstance(lease, (int, float)) and not isinstance(lease, bool) and lease > 0),
                 "lease_timeout must be a positive number")


def _validate_ingress(data: Dict[str, Any]) -> None:
    """Shape only: the worker builds the Firewall itself, which checks rule types and arguments."""
    _require(set(data) <= INGRESS_KEYS, "set_ingress has an unknown key")
    rules = data.get("rules", [])
    _require(isinstance(rules, list) and len(rules) <= MAX_INGRESS_RULES,
             f"rules must be a list of at most {MAX_INGRESS_RULES}")
    _require(all(isinstance(rule, dict) for rule in rules), "each rule must be an object")
    _require(isinstance(data.get("on_rule_error", "reject"), str), "on_rule_error must be a string")
    _require(isinstance(data.get("ledger", {}), dict), "ledger must be an object")
    _validate_inflight(data.get("inflight", {}))


_VALIDATORS = {
    "set_ingress": _validate_ingress,
    "add_broker": _validate_broker,
    "add_route": _validate_route,
    "add_subscription": _validate_subscription,
    "update_broker_security": _validate_security_update,
    "clear_state": lambda data: None,
}


def validate_action(action: Any, data: Any) -> None:
    """Raises PayloadError unless ``data`` is a well-formed payload for ``action``."""
    validator = _VALIDATORS.get(action) if isinstance(action, str) else None
    _require(validator is not None, "unknown action")
    validator(_check_dict(data, "data"))
