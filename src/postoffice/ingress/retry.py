import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Callable, Optional

import random

from postoffice.backoff import ExponentialBackoff


@dataclass(frozen=True)
class RetryDecision:
    attempt: int       # 1-based count of failed deliveries for this message so far
    exhausted: bool    # True when the attempt budget is spent: stop requeueing
    delay: float       # seconds to hold the message before requeueing it


class RetryTracker:
    """
    Caps redelivery of messages that keep failing, so a poison message cannot loop forever.

    ``max_attempts`` is the total number of delivery attempts: failure number ``max_attempts``
    is the last, and is reported as exhausted. Each requeue is delayed by exponential backoff.
    The key table is LRU+TTL bounded; an evicted key simply restarts its count, trading a little
    leniency for a hard memory bound.
    """

    def __init__(
        self,
        max_attempts: int = 5,
        base_delay: float = 0.5,
        factor: float = 2.0,
        cap: float = 30.0,
        jitter: str = "full",
        max_keys: int = 10000,
        ttl: float = 3600.0,
        clock: Callable[[], float] = time.monotonic,
        rng: Optional[random.Random] = None,
    ):
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if base_delay < 0:
            raise ValueError("base_delay must be >= 0")
        if max_keys <= 0 or ttl <= 0:
            raise ValueError("max_keys and ttl must be > 0")
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_keys = max_keys
        self.ttl = ttl
        self._clock = clock
        # base_delay == 0 means "requeue immediately", which ExponentialBackoff cannot express.
        self._backoff = (
            ExponentialBackoff(base=base_delay, factor=factor, cap=cap, jitter=jitter, rng=rng)
            if base_delay > 0 else None
        )
        self._counts: "OrderedDict[str, tuple]" = OrderedDict()  # key -> (failures, last_seen)
        self._lock = threading.Lock()

    @property
    def tracked_keys(self) -> int:
        with self._lock:
            return len(self._counts)

    def record_failure(self, key: str) -> RetryDecision:
        now = self._clock()
        with self._lock:
            while self._counts:
                _, (_, last_seen) = next(iter(self._counts.items()))
                if now - last_seen <= self.ttl:
                    break
                self._counts.popitem(last=False)
            failures = self._counts.pop(key, (0, now))[0] + 1
            self._counts[key] = (failures, now)
            while len(self._counts) > self.max_keys:
                self._counts.popitem(last=False)
        delay = self._backoff.delay_for(failures - 1) if self._backoff else 0.0
        return RetryDecision(failures, failures >= self.max_attempts, delay)

    def forget(self, key: str) -> None:
        with self._lock:
            self._counts.pop(key, None)
