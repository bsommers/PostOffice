import logging
from typing import Any, Dict, List

from postoffice.ingress.rules import (
    ALLOW, Action, MaxPayloadBytes, MessageContext, RateLimit, Rule, TopicShape, Verdict,
    parse_action,
)

logger = logging.getLogger(__name__)

RULE_TYPES = {
    "max_payload_bytes": MaxPayloadBytes,
    "topic_shape": TopicShape,
    "rate_limit": RateLimit,
}


class Firewall:
    """Ordered rule chain evaluated before routing. The first non-ALLOW verdict wins."""

    def __init__(self, rules: List[Rule], on_rule_error: Action = Action.REJECT):
        self.rules = list(rules)
        self.on_rule_error = parse_action(on_rule_error)

    def evaluate(self, ctx: MessageContext) -> Verdict:
        for rule in self.rules:
            try:
                verdict = rule.check(ctx)
            except Exception as e:
                name = getattr(rule, "name", type(rule).__name__)
                logger.error(f"Ingress rule '{name}' failed: {e}")
                # A broken rule must fail closed by default, never crash the callback thread.
                if self.on_rule_error == Action.ALLOW:
                    continue
                return Verdict(self.on_rule_error, name, "rule_error", str(e))
            if verdict.action != Action.ALLOW:
                return verdict
        return ALLOW

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "Firewall":
        """Builds a firewall from the JSON/YAML-shaped config shared by the API and rule files."""
        rules = []
        for spec in config.get("rules", []):
            spec = dict(spec)
            rule_type = spec.pop("type", None)
            if rule_type not in RULE_TYPES:
                raise ValueError(f"Unknown ingress rule type '{rule_type}'. Known: {sorted(RULE_TYPES)}")
            rules.append(RULE_TYPES[rule_type](**spec))
        return cls(rules, on_rule_error=parse_action(config.get("on_rule_error", "reject")))
