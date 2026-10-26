import random
from typing import Optional


class ExponentialBackoff:
    """
    Exponential delay sequence with optional jitter, shared by every retry loop.

    ``jitter="full"`` draws each delay uniformly from [0, ceiling] so many callers
    recovering from the same outage do not retry in lockstep.
    """
    VALID_JITTER = ("none", "full")

    def __init__(
        self,
        base: float = 1.0,
        factor: float = 2.0,
        cap: float = 30.0,
        jitter: str = "full",
        rng: Optional[random.Random] = None,
    ):
        if base <= 0:
            raise ValueError("base must be > 0")
        if factor < 1.0:
            raise ValueError("factor must be >= 1")
        if cap < base:
            raise ValueError("cap must be >= base")
        if jitter not in self.VALID_JITTER:
            raise ValueError(f"jitter must be one of {self.VALID_JITTER}")
        self.base = base
        self.factor = factor
        self.cap = cap
        self.jitter = jitter
        self._rng = rng or random.Random()
        self._attempt = 0

    def next(self) -> float:
        """Returns the delay for the current attempt and advances the sequence."""
        ceiling = self.cap if self._ceiling_reached() else min(self.cap, self.base * self.factor ** self._attempt)
        if ceiling < self.cap:
            self._attempt += 1
        return self._rng.uniform(0.0, ceiling) if self.jitter == "full" else ceiling

    def delay_for(self, attempt: int) -> float:
        """Stateless delay for a 0-based attempt number, for callers that track attempts per key."""
        try:
            ceiling = min(self.cap, self.base * self.factor ** attempt)
        except OverflowError:
            ceiling = self.cap
        return self._rng.uniform(0.0, ceiling) if self.jitter == "full" else ceiling

    def reset(self) -> None:
        self._attempt = 0

    def _ceiling_reached(self) -> bool:
        # Stop growing once the cap is hit so the exponent cannot overflow on long outages.
        return self.base * self.factor ** self._attempt >= self.cap
