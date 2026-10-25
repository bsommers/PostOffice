import logging
import threading
import time
from collections import OrderedDict
from typing import Any, Callable, Dict, List, Optional, Tuple

from postoffice.backoff import ExponentialBackoff
from postoffice.ingress.rules import MessageContext, extract_identity

logger = logging.getLogger(__name__)

CATEGORIES = (
    "oversize", "malformed", "rate_exceeded", "unroutable", "schema_mismatch",
    "forbidden_topic", "cooled",
)
OTHER = "other"
_MIN_EVENTS_FOR_CAUSE = 5
_DOMINANT_SHARE = 0.9
_MIXED_CATEGORY_COUNT = 3


class _Entry:
    __slots__ = (
        "broker", "sender", "first_seen", "last_seen", "total", "categories", "win_start",
        "cur", "prev", "strikes", "cooled_until", "warned_window", "backoff",
    )

    def __init__(self, broker: str, sender: str, now: float, backoff: Optional[ExponentialBackoff]):
        self.broker = broker
        self.sender = sender
        self.first_seen = self.last_seen = self.win_start = now
        self.total = self.cur = self.prev = self.strikes = 0
        self.categories: Dict[str, int] = {}
        self.cooled_until: Optional[float] = None
        self.warned_window: Optional[float] = None
        self.backoff = backoff


class SenderLedger:
    """
    Per-sender record of ingress problems, bounded in memory and observe-only by default.

    Oversize, malformed or rate-exceeded traffic is as often a misconfigured or stuck device as
    an attack, so the ledger *tracks* first and offers ``likely_cause`` hints for diagnosis.
    With ``enforce=True`` a sender that crosses ``strike_threshold`` events inside one window
    is put in an exponentially growing cool-down (see ``Firewall``, which drops cooled senders).

    Keys beyond ``max_keys`` aggregate into a single ``other`` row rather than evicting tracked
    offenders, so key-cardinality floods cannot flush the evidence or grow memory.
    """

    def __init__(
        self,
        max_keys: int = 1000,
        ttl: float = 3600.0,
        window: float = 60.0,
        rise_factor: float = 2.0,
        rise_min_events: int = 10,
        widespread_senders: int = 10,
        identity_template: Optional[str] = None,
        enforce: bool = False,
        strike_threshold: int = 10,
        cooldown_base: float = 30.0,
        cooldown_factor: float = 2.0,
        cooldown_cap: float = 900.0,
        decay_after: float = 600.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        if max_keys <= 0 or ttl <= 0 or window <= 0:
            raise ValueError("max_keys, ttl and window must be > 0")
        if strike_threshold <= 0:
            raise ValueError("strike_threshold must be > 0")
        if enforce and ttl < cooldown_cap:
            # An entry that expires mid-cool-down would forget the offender.
            raise ValueError("ttl must be >= cooldown_cap when enforce is set")
        self.max_keys = max_keys
        self.ttl = ttl
        self.window = window
        self.rise_factor = rise_factor
        self.rise_min_events = rise_min_events
        self.widespread_senders = widespread_senders
        self.identity_template = identity_template
        self.enforce = enforce
        self.strike_threshold = strike_threshold
        self.decay_after = decay_after
        self._cooldown = dict(base=cooldown_base, factor=cooldown_factor, cap=cooldown_cap, jitter="none")
        if enforce:
            ExponentialBackoff(**self._cooldown)  # validate eagerly
        self._clock = clock
        self._entries: "OrderedDict[Tuple[str, str], _Entry]" = OrderedDict()
        self._other: Optional[_Entry] = None
        self.overflow_events = 0
        self._lock = threading.Lock()

    @property
    def tracked_keys(self) -> int:
        with self._lock:
            return len(self._entries)

    def key_for(self, ctx: MessageContext) -> str:
        """Most specific identity available: explicit sender, topic-template capture, else topic."""
        if ctx.sender:
            return ctx.sender
        if self.identity_template:
            ident = extract_identity(self.identity_template, ctx.topic)
            if ident is not None:
                return ident
        return ctx.topic

    def record(self, broker: str, sender: str, category: str) -> None:
        if category not in CATEGORIES:
            raise ValueError(f"Unknown ledger category '{category}'. Known: {CATEGORIES}")
        with self._lock:
            now = self._clock()
            self._prune(now)
            entry, tracked = self._entry_for(broker, sender, now)
            self._rotate(entry, now)
            if tracked and self.enforce and now - entry.last_seen > self.decay_after:
                # A quiet period forgives earlier strikes.
                entry.backoff.reset()
                entry.strikes = 0
            entry.last_seen = now
            entry.total += 1
            entry.cur += 1
            entry.categories[category] = entry.categories.get(category, 0) + 1
            if tracked and self.enforce and category != "cooled" and not self._cooled(entry, now):
                entry.strikes += 1
                if entry.strikes >= self.strike_threshold:
                    entry.cooled_until = now + entry.backoff.next()
                    entry.strikes = 0
            if self._rising(entry) and entry.warned_window != entry.win_start:
                entry.warned_window = entry.win_start
                logger.warning(
                    f"Sender '{entry.sender}' on '{entry.broker}' is misbehaving at a rising rate: "
                    f"{entry.cur} events this window (previous {entry.prev}), {entry.categories}"
                )

    def is_cooled(self, broker: str, sender: str) -> bool:
        with self._lock:
            entry = self._entries.get((broker, sender))
            return entry is not None and self._cooled(entry, self._clock())

    def cooled_until(self, broker: str, sender: str) -> Optional[float]:
        with self._lock:
            entry = self._entries.get((broker, sender))
            return entry.cooled_until if entry else None

    def sender_report(self, top: int = 10) -> List[Dict[str, Any]]:
        """Structured rows for the noisiest senders, busiest current window first."""
        with self._lock:
            now = self._clock()
            self._prune(now)
            entries = list(self._entries.values())
            if self._other is not None:
                entries.append(self._other)
            for entry in entries:
                self._rotate(entry, now)
            active = sum(1 for e in self._entries.values() if e.cur > 0)
            entries.sort(key=lambda e: (e.cur, e.total), reverse=True)
            return [self._row(e, active) for e in entries[:top]]

    def _entry_for(self, broker: str, sender: str, now: float) -> Tuple[_Entry, bool]:
        key = (broker, sender)
        entry = self._entries.pop(key, None)
        if entry is None:
            if len(self._entries) >= self.max_keys:
                self.overflow_events += 1
                if self._other is None:
                    self._other = _Entry("*", OTHER, now, None)
                return self._other, False
            backoff = ExponentialBackoff(**self._cooldown) if self.enforce else None
            entry = _Entry(broker, sender, now, backoff)
        self._entries[key] = entry  # most recently seen goes to the back
        return entry, True

    def _prune(self, now: float) -> None:
        while self._entries:
            oldest = next(iter(self._entries.values()))
            if now - oldest.last_seen <= self.ttl:
                break
            self._entries.popitem(last=False)

    def _rotate(self, entry: _Entry, now: float) -> None:
        elapsed = now - entry.win_start
        if elapsed < self.window:
            return
        periods = int(elapsed // self.window)
        entry.prev = entry.cur if periods == 1 else 0
        entry.cur = 0
        entry.strikes = 0
        entry.win_start += periods * self.window

    @staticmethod
    def _cooled(entry: _Entry, now: float) -> bool:
        return entry.cooled_until is not None and now < entry.cooled_until

    def _rising(self, entry: _Entry) -> bool:
        return entry.cur >= self.rise_min_events and entry.cur >= self.rise_factor * entry.prev

    def _cause(self, entry: _Entry, active: int) -> str:
        if active >= self.widespread_senders:
            return "widespread"
        if entry.total < _MIN_EVENTS_FOR_CAUSE:
            return "insufficient_data"
        if len(entry.categories) >= _MIXED_CATEGORY_COUNT and self._rising(entry):
            return "mixed_high_rate"
        if max(entry.categories.values()) >= _DOMINANT_SHARE * entry.total:
            return "repeated_identical_error"
        return "varied_errors"

    def _row(self, entry: _Entry, active: int) -> Dict[str, Any]:
        return {
            "broker": entry.broker,
            "sender": entry.sender,
            "total": entry.total,
            "categories": dict(entry.categories),
            "window": entry.cur,
            "previous_window": entry.prev,
            "rising": self._rising(entry),
            "first_seen": entry.first_seen,
            "last_seen": entry.last_seen,
            "cooled_until": entry.cooled_until,
            "likely_cause": self._cause(entry, active),
        }
