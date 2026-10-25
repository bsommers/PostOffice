import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Iterable, List, Optional

from postoffice.ratelimit import KeyedLimiter


class Action(str, Enum):
    ALLOW = "allow"
    DROP = "drop"      # acknowledge and discard
    REJECT = "reject"  # nack without requeue


@dataclass(frozen=True)
class MessageContext:
    broker: str
    topic: str
    size: int
    payload: Optional[bytes] = None
    sender: Optional[str] = None  # explicit identity (AMQP user_id, Kafka key) when known


@dataclass(frozen=True)
class Verdict:
    action: Action
    rule: Optional[str] = None
    category: Optional[str] = None  # oversize | malformed | rate_exceeded | forbidden_topic | cooled | rule_error
    reason: Optional[str] = None


ALLOW = Verdict(Action.ALLOW)


def parse_action(value) -> Action:
    try:
        return value if isinstance(value, Action) else Action(str(value).lower())
    except ValueError:
        raise ValueError(f"Invalid action '{value}'. Use one of: allow, drop, reject.")


def topic_matches(pattern: str, topic: str) -> bool:
    """MQTT-style match: '+' is one level, '#' is the remaining levels (including none)."""
    p, t = pattern.split("/"), topic.split("/")
    for i, seg in enumerate(p):
        if seg == "#":
            return True
        if i >= len(t) or (seg != "+" and seg != t[i]):
            return False
    return len(p) == len(t)


def extract_identity(template: str, topic: str) -> Optional[str]:
    """Returns the topic segment captured by the first ``{name}`` placeholder, or None."""
    t = topic.split("/")
    ident = None
    for i, seg in enumerate(template.split("/")):
        if seg == "#":
            return ident
        if i >= len(t):
            return None
        if seg.startswith("{") and seg.endswith("}"):
            ident = ident if ident is not None else t[i]
        elif seg != "+" and seg != t[i]:
            return None
    return ident


class Rule:
    name = "rule"

    def __init__(self, brokers: Optional[Iterable[str]] = None):
        self.brokers = frozenset(brokers) if brokers else None

    def applies(self, ctx: MessageContext) -> bool:
        return self.brokers is None or ctx.broker in self.brokers

    def check(self, ctx: MessageContext) -> Verdict:
        if not self.applies(ctx):
            return ALLOW
        return self._check(ctx)

    def _check(self, ctx: MessageContext) -> Verdict:
        raise NotImplementedError


class MaxPayloadBytes(Rule):
    """Rejects by declared size alone: the payload is never decoded or copied."""
    name = "max_payload_bytes"

    def __init__(self, max_bytes: int, action: Action = Action.REJECT, brokers=None):
        super().__init__(brokers)
        if max_bytes <= 0:
            raise ValueError("max_bytes must be > 0")
        self.max_bytes = max_bytes
        self.action = parse_action(action)

    def _check(self, ctx):
        if ctx.size > self.max_bytes:
            return Verdict(self.action, self.name, "oversize", f"{ctx.size} > {self.max_bytes} bytes")
        return ALLOW


class TopicShape(Rule):
    name = "topic_shape"

    def __init__(self, max_length: int = 256, max_depth: int = 16, deny: Optional[List[str]] = None,
                 action: Action = Action.REJECT, brokers=None):
        super().__init__(brokers)
        if max_length <= 0 or max_depth <= 0:
            raise ValueError("max_length and max_depth must be > 0")
        self.max_length = max_length
        self.max_depth = max_depth
        self.deny = list(deny or [])
        self.action = parse_action(action)

    def _bad(self, category, reason):
        return Verdict(self.action, self.name, category, reason)

    def _check(self, ctx):
        topic = ctx.topic
        if not topic:
            return self._bad("malformed", "empty topic")
        if "\x00" in topic:
            return self._bad("malformed", "null byte in topic")
        if len(topic) > self.max_length:
            return self._bad("malformed", f"topic length {len(topic)} > {self.max_length}")
        if topic.count("/") + 1 > self.max_depth:
            return self._bad("malformed", f"topic depth > {self.max_depth}")
        for pattern in self.deny:
            if topic_matches(pattern, topic):
                return self._bad("forbidden_topic", f"matches denied pattern {pattern}")
        return ALLOW


class RateLimit(Rule):
    name = "rate_limit"

    def __init__(self, rate: float, burst: Optional[float] = None, key: str = "broker",
                 template: Optional[str] = None, cost: str = "messages",
                 max_keys: int = 10000, ttl: float = 300.0, action: Action = Action.REJECT,
                 brokers=None, clock: Callable[[], float] = time.monotonic):
        super().__init__(brokers)
        if key not in ("broker", "topic", "identity"):
            raise ValueError("key must be 'broker', 'topic' or 'identity'")
        if cost not in ("messages", "bytes"):
            raise ValueError("cost must be 'messages' or 'bytes'")
        if key == "identity" and not template:
            raise ValueError("key='identity' requires a template")
        self.key, self.template, self.cost = key, template, cost
        self.action = parse_action(action)
        self._limiter = KeyedLimiter(rate, burst if burst is not None else rate,
                                     max_keys=max_keys, ttl=ttl, clock=clock)

    def _key(self, ctx):
        if self.key == "broker":
            return ctx.broker
        if self.key == "identity":
            ident = extract_identity(self.template, ctx.topic)
            return f"{ctx.broker}:{ident if ident is not None else ctx.topic}"
        return f"{ctx.broker}:{ctx.topic}"

    def _check(self, ctx):
        cost = float(ctx.size) if self.cost == "bytes" else 1.0
        if self._limiter.try_consume(self._key(ctx), cost):
            return ALLOW
        return Verdict(self.action, self.name, "rate_exceeded", f"{self.key} rate limit")
