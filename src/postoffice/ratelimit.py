import threading
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
