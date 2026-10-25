import threading
from collections import OrderedDict
import time
from typing import Callable


class TokenBucket:
    """Thread-safe token bucket: refills at ``rate`` tokens/s up to ``burst``."""

    def __init__(self, rate: float, burst: float, clock: Callable[[], float] = time.monotonic):
        if rate <= 0:
            raise ValueError("rate must be > 0")
        if burst <= 0:
            raise ValueError("burst must be > 0")
        self.rate = rate
        self.burst = burst
        self._clock = clock
        self._tokens = burst
        self._updated = clock()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = self._clock()
        elapsed = max(0.0, now - self._updated)
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._updated = now

    def can_consume(self, cost: float = 1.0) -> bool:
        """True if ``cost`` tokens are available now; does not consume."""
        with self._lock:
            self._refill()
            return cost <= self._tokens

    def try_consume(self, cost: float = 1.0) -> bool:
        with self._lock:
            self._refill()
            if cost <= self._tokens:
                self._tokens -= cost
                return True
            return False


class KeyedLimiter:
    """Per-key token buckets with LRU and idle-TTL eviction, so memory stays bounded.

    An evicted key restarts with a full bucket; the cap trades a little leniency for a hard
    memory bound that key-cardinality attacks cannot exceed.
    """

    def __init__(
        self,
        rate: float,
        burst: float,
        max_keys: int = 10000,
        ttl: float = 300.0,
        clock: Callable[[], float] = time.monotonic,
    ):
        if max_keys <= 0:
            raise ValueError("max_keys must be > 0")
        if ttl <= 0:
            raise ValueError("ttl must be > 0")
        TokenBucket(rate, burst)  # validate eagerly
        self.rate = rate
        self.burst = burst
        self.max_keys = max_keys
        self.ttl = ttl
        self._clock = clock
        self._buckets: "OrderedDict[str, tuple]" = OrderedDict()
        self._lock = threading.Lock()

    @property
    def tracked_keys(self) -> int:
        with self._lock:
            return len(self._buckets)

    def try_consume(self, key: str, cost: float = 1.0) -> bool:
        now = self._clock()
        with self._lock:
            # Oldest entries sit at the front; drop idle ones, then enforce the cap.
            while self._buckets:
                oldest_key, (_, last_seen) = next(iter(self._buckets.items()))
                if now - last_seen <= self.ttl:
                    break
                del self._buckets[oldest_key]
            entry = self._buckets.pop(key, None)
            bucket = entry[0] if entry else TokenBucket(self.rate, self.burst, self._clock)
            self._buckets[key] = (bucket, now)
            while len(self._buckets) > self.max_keys:
                self._buckets.popitem(last=False)
        return bucket.try_consume(cost)
