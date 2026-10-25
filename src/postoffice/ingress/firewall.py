import logging
from typing import Any, Dict, List, Optional

from postoffice.ingress.ledger import SenderLedger
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

    def __init__(self, rules: List[Rule], on_rule_error: Action = Action.REJECT,
                 ledger: Optional[SenderLedger] = None):
        self.rules = list(rules)
        self.on_rule_error = parse_action(on_rule_error)
        self.ledger = ledger

    def note(self, ctx: MessageContext, category: Optional[str]) -> None:
        """Attributes an event to its sender in the ledger; a ledger fault never affects routing."""
        if self.ledger is None or not category or category == "rule_error":
            return
        try:
            self.ledger.record(ctx.broker, self.ledger.key_for(ctx), category)
        except Exception as e:
            logger.error(f"Sender ledger failed to record '{category}': {e}")

    def _cooled_verdict(self, ctx: MessageContext) -> Optional[Verdict]:
        if self.ledger is None or not self.ledger.enforce:
            return None
        try:
            key = self.ledger.key_for(ctx)
            if not self.ledger.is_cooled(ctx.broker, key):
                return None
            until = self.ledger.cooled_until(ctx.broker, key)
        except Exception as e:
            logger.error(f"Sender ledger cool-down check failed: {e}")
            return None
        return Verdict(Action.DROP, "cooldown", "cooled", f"sender '{key}' cooled until {until:.0f}")

    def evaluate(self, ctx: MessageContext) -> Verdict:
        verdict = self._cooled_verdict(ctx)
        if verdict is None:
            verdict = self._run_rules(ctx)
        self.note(ctx, verdict.category)
        return verdict

    def _run_rules(self, ctx: MessageContext) -> Verdict:
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
        ledger_config = config.get("ledger")
        ledger = SenderLedger(**ledger_config) if ledger_config is not None else None
        return cls(rules, on_rule_error=parse_action(config.get("on_rule_error", "reject")), ledger=ledger)
