"""
Message-accounting oracle (PERFORMANCE_TEST_PLAN §3).

Feed it what the generator sent and what the sink received; it reports loss, duplicates, per-producer
reordering, corruption and latency, and checks the books against PostOffice's own counters:

    sent == delivered + dlq + shed + dropped + in_flight

A run passes only if the books balance, nothing is corrupted, and no foreign/unexpected message arrived.
Duplicates and reordering are reported but allowed (at-least-once).
"""
import math
from collections import defaultdict
from typing import Dict, List, Optional, Set

from perf import header

PERCENTILES = (50, 90, 99, 99.9)
_LOST_SAMPLE = 20


def percentile(sorted_values: List[int], p: float) -> int:
    """Nearest-rank percentile of an ascending list."""
    rank = max(1, math.ceil(p / 100 * len(sorted_values)))
    return sorted_values[min(rank, len(sorted_values)) - 1]


class Oracle:
    def __init__(self):
        self._sent: Dict[int, Set[int]] = defaultdict(set)
        self._seen: Dict[int, Set[int]] = defaultdict(set)
        self._last: Dict[int, int] = {}
        self._latencies: List[int] = []
        self.duplicated = self.reordered = self.corrupted = self.foreign = self.unexpected = 0

    def sent(self, producer: int, seq: int) -> None:
        self._sent[producer].add(seq)

    def sent_range(self, producer: int, count: int) -> None:
        self._sent[producer].update(range(count))

    def received(self, message: bytes, recv_ts_ns: int) -> None:
        h = header.decode(message)
        if h is None:
            self.foreign += 1
            return
        if not h.intact:
            self.corrupted += 1
            return
        if h.seq not in self._sent.get(h.producer, ()):
            self.unexpected += 1
            return
        seen = self._seen[h.producer]
        if h.seq in seen:
            self.duplicated += 1
            return
        seen.add(h.seq)
        last = self._last.get(h.producer)
        if last is not None and h.seq < last:
            self.reordered += 1
        else:
            self._last[h.producer] = h.seq
        self._latencies.append(max(0, recv_ts_ns - h.send_ts_ns))

    def report(self, dlq: int = 0, shed: int = 0, dropped: int = 0, in_flight: int = 0) -> dict:
        sent = sum(len(s) for s in self._sent.values())
        delivered = sum(len(s) for s in self._seen.values())
        lost_sample = []
        for producer in sorted(self._sent):
            missing = sorted(self._sent[producer] - self._seen.get(producer, set()))
            lost_sample.extend([producer, s] for s in missing[: _LOST_SAMPLE - len(lost_sample)])
        balanced = sent == delivered + dlq + shed + dropped + in_flight
        lat = sorted(self._latencies)
        latency = None
        if lat:
            latency = {f"p{p:g}": percentile(lat, p) for p in PERCENTILES}
            latency["max"] = lat[-1]
        return {
            "sent": sent,
            "delivered": delivered,
            "lost": sent - delivered,
            "lost_sample": lost_sample,
            "accounted": {"dlq": dlq, "shed": shed, "dropped": dropped, "in_flight": in_flight},
            "duplicated": self.duplicated,
            "reordered": self.reordered,
            "corrupted": self.corrupted,
            "foreign": self.foreign,
            "unexpected": self.unexpected,
            "balanced": balanced,
            "passed": balanced and not (self.corrupted or self.foreign or self.unexpected),
            "latency_ns": latency,
        }
