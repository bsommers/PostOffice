"""
Phase 4 Mixed-Fleet Canary Rollout Controller with Automatic Rollback.

Manages progressive canary promotion (5% -> 25% -> 100%) per environment and triggers
automatic rollback to 0% native (`engine="python"`) on error-rate, consumer-lag, or
bridge-added p99 latency regressions.
"""

from dataclasses import dataclass, field
from typing import Any, List, Optional, Tuple


@dataclass(frozen=True)
class CanaryPolicy:
    """SLO thresholds and ramp percentages for mixed-fleet canary rollout."""

    stages: Tuple[int, ...] = (5, 25, 100)
    max_error_rate: float = 0.01
    max_lag_messages: int = 1000
    max_p99_latency_ms: float = 5.0
    min_samples: int = 100

    def __post_init__(self) -> None:
        if not self.stages or any(not (0 < s <= 100) for s in self.stages):
            raise ValueError("stages must be non-empty integers in 1..100")
        if list(self.stages) != sorted(self.stages):
            raise ValueError("stages must be monotonically non-decreasing")
        if self.max_error_rate < 0.0 or self.max_error_rate > 1.0:
            raise ValueError("max_error_rate must be in 0.0..1.0")
        if self.max_lag_messages < 0:
            raise ValueError("max_lag_messages must be >= 0")
        if self.max_p99_latency_ms <= 0.0:
            raise ValueError("max_p99_latency_ms must be > 0")
        if self.min_samples <= 0:
            raise ValueError("min_samples must be > 0")


@dataclass(frozen=True)
class RolloutMetricsSample:
    """Observed metrics window evaluated by the canary controller."""

    routed_total: int
    errors_total: int
    consumer_lag: int = 0
    p99_latency_ms: float = 0.0

    @property
    def error_rate(self) -> float:
        if self.routed_total <= 0:
            return 1.0 if self.errors_total > 0 else 0.0
        return self.errors_total / float(self.routed_total)


@dataclass(frozen=True)
class RolloutState:
    """Current rollout state for an environment."""

    environment: str
    native_percentage: int
    stage_index: int
    status: str  # "active", "paused", "rolled_back", "completed"
    reason: str = ""


class CanaryRolloutController:
    """
    Coordinates mixed-fleet canary ramp (`5% -> 25% -> 100%`) and automatic rollback
    via ControlPlane (`postoffice:rollout` + `set_rollout` event).
    """

    def __init__(
        self,
        environment: str = "default",
        policy: Optional[CanaryPolicy] = None,
        control_plane: Optional[Any] = None,
    ) -> None:
        self.environment = environment
        self.policy = policy or CanaryPolicy()
        self.control_plane = control_plane
        self._state = RolloutState(
            environment=environment,
            native_percentage=0,
            stage_index=-1,
            status="paused",
            reason="initialized",
        )
        self.history: List[RolloutState] = [self._state]

    @property
    def state(self) -> RolloutState:
        return self._state

    def _publish(self, state: RolloutState) -> RolloutState:
        self._state = state
        self.history.append(state)
        if self.control_plane is not None:
            self.control_plane.set_rollout(
                native_percentage=state.native_percentage,
                environment=state.environment,
                status=state.status,
                reason=state.reason,
            )
        return state

    def start(self) -> RolloutState:
        """Starts or restarts the canary rollout at stage 0 (default 5%)."""
        first_pct = self.policy.stages[0]
        status = "completed" if len(self.policy.stages) == 1 and first_pct == 100 else "active"
        return self._publish(
            RolloutState(
                environment=self.environment,
                native_percentage=first_pct,
                stage_index=0,
                status=status,
                reason=f"started_stage_0_{first_pct}pct",
            )
        )

    def rollback(self, reason: str) -> RolloutState:
        """Immediately rolls back native traffic to 0% (`engine=python`)."""
        return self._publish(
            RolloutState(
                environment=self.environment,
                native_percentage=0,
                stage_index=-1,
                status="rolled_back",
                reason=reason[:256],
            )
        )

    def check_regression(self, sample: RolloutMetricsSample) -> Optional[str]:
        """Returns a descriptive breach reason if `sample` violates any SLO, else None."""
        if sample.error_rate > self.policy.max_error_rate:
            return (
                f"error_rate_regression: {sample.error_rate:.4f} > "
                f"{self.policy.max_error_rate:.4f}"
            )
        if sample.consumer_lag > self.policy.max_lag_messages:
            return (
                f"consumer_lag_regression: {sample.consumer_lag} > "
                f"{self.policy.max_lag_messages}"
            )
        if sample.p99_latency_ms > self.policy.max_p99_latency_ms:
            return (
                f"p99_latency_regression: {sample.p99_latency_ms:.2f}ms > "
                f"{self.policy.max_p99_latency_ms:.2f}ms"
            )
        return None

    def evaluate_and_step(self, sample: RolloutMetricsSample) -> RolloutState:
        """
        Evaluates `sample` against `CanaryPolicy`.
        - On any regression, triggers automatic rollback to 0% immediately (even at 100% or low sample count).
        - If sample count < `min_samples` or already rolled back/paused, holds current stage.
        - Otherwise advances to the next stage in `policy.stages`.
        """
        if self._state.status in ("rolled_back", "paused"):
            return self._state

        regression = self.check_regression(sample)
        if regression is not None:
            return self.rollback(regression)

        if sample.routed_total < self.policy.min_samples:
            return self._state

        next_idx = self._state.stage_index + 1
        if next_idx >= len(self.policy.stages):
            if self._state.status != "completed":
                return self._publish(
                    RolloutState(
                        environment=self.environment,
                        native_percentage=self._state.native_percentage,
                        stage_index=self._state.stage_index,
                        status="completed",
                        reason="rollout_complete",
                    )
                )
            return self._state

        next_pct = self.policy.stages[next_idx]
        is_final = next_idx == len(self.policy.stages) - 1 and next_pct == 100
        return self._publish(
            RolloutState(
                environment=self.environment,
                native_percentage=next_pct,
                stage_index=next_idx,
                status="completed" if is_final else "active",
                reason=f"promoted_stage_{next_idx}_{next_pct}pct",
            )
        )
