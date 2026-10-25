import logging
import threading
import time
from collections import OrderedDict
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class Lease:
    """One admitted in-flight message; ``release`` is idempotent and safe after reclamation."""

    __slots__ = ("_limiter", "deadline", "active")

    def __init__(self, limiter: "InflightLimiter", deadline: float):
        self._limiter = limiter
        self.deadline = deadline
        self.active = True

    def release(self) -> None:
        self._limiter._release(self)


class InflightLimiter:
    """
    Caps un-confirmed messages from one source and signals back-pressure with hysteresis.

    ``on_saturate`` fires once when the cap is reached and ``on_drain`` once when in-flight
    falls to ``low_water``, so a client is paused and resumed instead of flapping per message.
    A lease whose confirmation never arrives is reclaimed after ``lease_timeout``; otherwise a
    single lost callback would shrink capacity forever. Callbacks run outside the lock.
    """

    def __init__(
        self,
        max_inflight: int,
        low_water: Optional[int] = None,
        lease_timeout: float = 60.0,
        clock: Callable[[], float] = time.monotonic,
        on_saturate: Optional[Callable[[], None]] = None,
        on_drain: Optional[Callable[[], None]] = None,
    ):
        if max_inflight <= 0:
            raise ValueError("max_inflight must be > 0")
        low_water = max_inflight // 2 if low_water is None else low_water
        if not 0 <= low_water < max_inflight:
            raise ValueError("low_water must satisfy 0 <= low_water < max_inflight")
        if lease_timeout <= 0:
            raise ValueError("lease_timeout must be > 0")
        self.max_inflight = max_inflight
        self.low_water = low_water
        self.lease_timeout = lease_timeout
        self.reclaimed = 0
        self._clock = clock
        self._on_saturate = on_saturate
        self._on_drain = on_drain
        self._leases: "OrderedDict[int, Lease]" = OrderedDict()  # oldest (earliest deadline) first
        self._paused = False
        self._lock = threading.Lock()

    @property
    def inflight(self) -> int:
        with self._lock:
            events = self._reclaim_expired()
            count = len(self._leases)
        self._fire(events)
        return count

    @property
    def paused(self) -> bool:
        return self._paused

    def acquire(self) -> Optional[Lease]:
        """Returns a lease, or None when the cap is reached."""
        with self._lock:
            events = self._reclaim_expired()
            lease = None
            if len(self._leases) < self.max_inflight:
                lease = Lease(self, self._clock() + self.lease_timeout)
                self._leases[id(lease)] = lease
            if len(self._leases) >= self.max_inflight and not self._paused:
                self._paused = True
                events.append(self._on_saturate)
        self._fire(events)
        return lease

    def _release(self, lease: Lease) -> None:
        with self._lock:
            if not lease.active:
                return
            lease.active = False
            self._leases.pop(id(lease), None)
            events = [self._drain_event()]
        self._fire(events)

    def _reclaim_expired(self) -> list:
        now = self._clock()
        expired = False
        while self._leases:
            oldest = next(iter(self._leases.values()))
            if oldest.deadline > now:
                break
            oldest.active = False
            self._leases.popitem(last=False)
            self.reclaimed += 1
            expired = True
        if expired:
            logger.warning(f"Reclaimed expired in-flight leases (total reclaimed: {self.reclaimed})")
        return [self._drain_event()] if expired else []

    def _drain_event(self) -> Optional[Callable[[], None]]:
        if self._paused and len(self._leases) <= self.low_water:
            self._paused = False
            return self._on_drain
        return None

    @staticmethod
    def _fire(events: list) -> None:
        for callback in events:
            if callback is None:
                continue
            try:
                callback()
            except Exception as e:
                logger.error(f"In-flight limiter callback failed: {e}")
