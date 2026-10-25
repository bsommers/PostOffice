import json
import base64
import threading
import time
from collections import OrderedDict
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
        clock: Callable[[], float] = time.monotonic,
    ):
        if max_message_bytes <= 0:
            raise ValueError("max_message_bytes must be > 0")
        self.max_message_bytes = max_message_bytes
        self.msgs_per_sec = msgs_per_sec
        self.bytes_per_sec = bytes_per_sec
        self.max_sources = max_sources
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

    def check(self, source_broker: str, size: int) -> Optional[str]:
        if size > self.max_message_bytes:
            return "oversize"
        with self._lock:
            msgs, byte_bucket = self._buckets_for(source_broker)
            if not msgs.can_consume(1.0):
                return "rate"
            if not byte_bucket.can_consume(float(size)):
                return "bytes"
            msgs.try_consume(1.0)
            byte_bucket.try_consume(float(size))
        return None
