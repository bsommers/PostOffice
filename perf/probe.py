"""
Bridge-added nanosecond latency probe for `Router.route()` (`NATIVE_REWRITE_PLAN.md` §0.1).

Measures in-process bridge latency from ingress callback entry (`Router.route()`) to egress
client handoff completion (`target.publish()`), with zero overhead when disabled.
"""
import threading
from typing import Dict, List, Optional

from perf.oracle import PERCENTILES, percentile


class LatencyProbe:
    def __init__(self) -> None:
        self._samples: List[int] = []
        self._lock = threading.Lock()

    def record(self, duration_ns: int) -> None:
        with self._lock:
            self._samples.append(max(0, duration_ns))

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._samples)

    def reset(self) -> None:
        with self._lock:
            self._samples.clear()

    def summary(self) -> Optional[Dict[str, int]]:
        with self._lock:
            if not self._samples:
                return None
            ordered = sorted(self._samples)
        out = {"count": len(ordered), "min": ordered[0]}
        for p in PERCENTILES:
            out[f"p{p:g}"] = percentile(ordered, p)
        out["max"] = ordered[-1]
        return out
