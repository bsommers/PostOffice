from enum import Enum
import threading
import time
from typing import Callable, Optional


class CircuitState(str, Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"


class CircuitBreakerOpenError(RuntimeError):
    """Raised when an operation is blocked by an open egress circuit breaker."""
    pass


class CircuitBreaker:
    """
    Thread-safe per-broker circuit breaker protecting egress publish paths.
    Transitions: CLOSED -> OPEN (after failure_threshold consecutive failures)
                 OPEN -> HALF_OPEN (after recovery_timeout seconds elapsed)
                 HALF_OPEN -> CLOSED (on probe success) or OPEN (on probe failure)
    """
    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 30.0,
        time_fn: Callable[[], float] = time.monotonic,
    ):
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout < 0:
            raise ValueError("recovery_timeout must be >= 0")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.time_fn = time_fn

        self._lock = threading.RLock()
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at: Optional[float] = None
        self._probe_in_flight = False

    @property
    def state(self) -> CircuitState:
        with self._lock:
            return self._state

    @property
    def failure_count(self) -> int:
        with self._lock:
            return self._failure_count

    def allow_request(self) -> bool:
        """
        Determines whether an egress publish attempt is permitted.
        In OPEN state, transitions to HALF_OPEN and allows a single probe if recovery_timeout has elapsed.
        """
        with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                now = self.time_fn()
                if self._opened_at is not None and (now - self._opened_at) >= self.recovery_timeout:
                    self._state = CircuitState.HALF_OPEN
                    self._probe_in_flight = True
                    return True
                return False

            if self._state == CircuitState.HALF_OPEN:
                if not self._probe_in_flight:
                    self._probe_in_flight = True
                    return True
                return False

            return False

    def record_success(self) -> bool:
        """
        Records a successful publish confirmation.
        Returns True if the circuit recovered from HALF_OPEN to CLOSED, False otherwise.
        """
        with self._lock:
            was_half_open = self._state == CircuitState.HALF_OPEN
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._opened_at = None
            self._probe_in_flight = False
            return was_half_open

    def record_failure(self) -> bool:
        """
        Records a failed publish attempt.
        Returns True if this failure tripped the circuit to OPEN.
        """
        with self._lock:
            self._failure_count += 1
            self._probe_in_flight = False
            if self._state == CircuitState.HALF_OPEN or self._failure_count >= self.failure_threshold:
                self._state = CircuitState.OPEN
                self._opened_at = self.time_fn()
                return True
            return False

    def reset(self) -> None:
        """Resets the circuit breaker to CLOSED state with zero failures."""
        with self._lock:
            self._state = CircuitState.CLOSED
            self._failure_count = 0
            self._opened_at = None
            self._probe_in_flight = False
