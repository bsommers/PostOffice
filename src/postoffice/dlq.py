import base64
import hashlib
import json
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Optional, Dict, Any

from postoffice.ratelimit import TokenBucket

def format_dlq_payload(
    original_payload: bytes,
    source_broker: str,
    source_topic: str,
    error: str,
    metadata: Optional[Dict[str, Any]] = None
) -> bytes:
    """
    Wraps an original messaging payload and forensic failure details into a JSON envelope.
    Binary payloads that are not valid UTF-8 are Base64-encoded to ensure safe serialization.
    """
    try:
        payload_str = original_payload.decode('utf-8')
        is_base64 = False
    except (UnicodeDecodeError, AttributeError):
        payload_str = base64.b64encode(original_payload).decode('ascii')
        is_base64 = True

    envelope = {
        "source_broker": source_broker,
        "source_topic": source_topic,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error": str(error),
        "payload": payload_str,
        "is_base64": is_base64,
        "metadata": metadata or {}
    }

    return json.dumps(envelope).encode('utf-8')


@dataclass(frozen=True)
class Admission:
    refusal: Optional[str] = None  # None = allowed
    repeat_count: int = 0          # identical writes suppressed since the previous recorded one


def dlq_fingerprint(source_broker: str, source_topic: str, error: str, payload: bytes) -> str:
    """Stable identity of a dead-lettered message, for collapsing identical repeats."""
    h = hashlib.blake2b(digest_size=16)
    for part in (source_broker, source_topic, error):
        h.update(part.encode("utf-8", "replace") + b"\x00")
    h.update(payload)
    return h.hexdigest()


class BudgetedDlq:
    """
    Admission control for Dead Letter Queue writes, so a flood of bad or unroutable
    messages cannot turn the DLQ into a write-amplification sink.

    ``check`` returns None when a write is allowed, otherwise the refusal reason:
    ``oversize`` (payload above ``max_message_bytes``; never truncated or stored),
    ``rate`` (messages/s) or ``bytes`` (bytes/s). Budgets are tracked per source
    broker so one noisy source cannot exhaust another's, and the per-source table is
    LRU-bounded so it cannot itself be grown without limit.
    """

    def __init__(
        self,
        max_message_bytes: int = 256 * 1024,
        msgs_per_sec: float = 100.0,
        bytes_per_sec: float = 1024 * 1024,
        max_sources: int = 1024,
        dedupe_window: Optional[float] = None,
        max_fingerprints: int = 4096,
        clock: Callable[[], float] = time.monotonic,
    ):
        if max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be > 0")
        self.max_message_bytes = max_message_bytes
        self.msgs_per_sec = msgs_per_sec
        self.bytes_per_sec = bytes_per_sec
        self.max_sources = max_sources
        if dedupe_window is not None and dedupe_window <= 0:
            raise ValueError("dedupe_window must be > 0 (or None to disable)")
        if max_fingerprints <= 0:
            raise ValueError("max_fingerprints must be > 0")
        self.dedupe_window = dedupe_window
        self.max_fingerprints = max_fingerprints
        # fingerprint -> [first_recorded_at, suppressed_count]
        self._seen: "OrderedDict[str, list]" = OrderedDict()
        self._clock = clock
        self._lock = threading.Lock()
        self._buckets: "OrderedDict[str, tuple[TokenBucket, TokenBucket]]" = OrderedDict()

    @property
    def tracked_sources(self) -> int:
        return len(self._buckets)

    def _buckets_for(self, source_broker: str):
        entry = self._buckets.get(source_broker)
        if entry is None:
            entry = (
                TokenBucket(self.msgs_per_sec, self.msgs_per_sec, self._clock),
                # Burst must admit at least one maximum-size message.
                TokenBucket(self.bytes_per_sec, max(self.bytes_per_sec, self.max_message_bytes), self._clock),
            )
            self._buckets[source_broker] = entry
            while len(self._buckets) > self.max_sources:
                self._buckets.popitem(last=False)
        else:
            self._buckets.move_to_end(source_broker)
        return entry

    @property
    def tracked_fingerprints(self) -> int:
        return len(self._seen)

    def check(self, source_broker: str, size: int) -> Optional[str]:
        return self.admit(source_broker, size).refusal

    def admit(
        self,
        source_broker: str,
        size: int,
        fingerprint: Optional[str] = None,
        metadata_only: bool = False,
    ) -> Admission:
        """
        Decides whether a DLQ write may proceed. ``fingerprint`` enables identical-repeat collapsing
        (only when ``dedupe_window`` is set): a repeat inside the window is ``duplicate`` and costs no
        budget; the next write after the window reports how many were suppressed. ``metadata_only``
        skips the size limit for forensic records that carry no payload.
        """
        if size > self.max_message_bytes and not metadata_only:
            return Admission("oversize")
        with self._lock:
            now = self._clock()
            tracking = fingerprint is not None and self.dedupe_window is not None
            repeat_count = 0
            if tracking:
                entry = self._seen.get(fingerprint)
                if entry is not None:
                    if now - entry[0] <= self.dedupe_window:
                        entry[1] += 1
                        self._seen.move_to_end(fingerprint)
                        return Admission("duplicate")
                    repeat_count = entry[1]
            msgs, byte_bucket = self._buckets_for(source_broker)
            if not msgs.can_consume(1.0):
                return Admission("rate")
            if not byte_bucket.can_consume(float(size)):
                return Admission("bytes")
            msgs.try_consume(1.0)
            byte_bucket.try_consume(float(size))
            if tracking:
                self._seen[fingerprint] = [now, 0]
                self._seen.move_to_end(fingerprint)
                while len(self._seen) > self.max_fingerprints:
                    self._seen.popitem(last=False)
        return Admission(None, repeat_count)
