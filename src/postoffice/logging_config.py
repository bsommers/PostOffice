"""
Structured JSON Logging Configuration (Phase 4 Operational Parity).

Emits single-line JSON log records with correlation fields (`timestamp`, `level`,
`logger`, `message`, `engine`, `worker_id`, `source_broker`, `target_broker`,
`topic`, `applied_version`, `event`) matching the native Rust worker.
"""

from datetime import datetime, timezone
import json
import logging
import os
import sys
from typing import Any, Optional, TextIO

_CORRELATION_FIELDS = (
    "source_broker",
    "target_broker",
    "topic",
    "applied_version",
    "event",
    "status",
    "error_type",
)


class StructuredJsonFormatter(logging.Formatter):
    """Formats Python `LogRecord` instances as single-line structured JSON."""

    def __init__(self, engine: str = "python", worker_id: Optional[str] = None) -> None:
        super().__init__()
        self.engine = engine
        self.worker_id = worker_id or os.environ.get("HOSTNAME", "postoffice-worker")

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        payload: dict[str, Any] = {
            "timestamp": ts,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "engine": getattr(record, "engine", self.engine),
            "worker_id": getattr(record, "worker_id", self.worker_id),
        }
        for key in _CORRELATION_FIELDS:
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))


def configure_structured_logging(
    level: int = logging.INFO,
    engine: str = "python",
    worker_id: Optional[str] = None,
    stream: Optional[TextIO] = None,
    target_logger: Optional[logging.Logger] = None,
) -> logging.Handler:
    """Installs `StructuredJsonFormatter` on `target_logger` (or the root logger)."""
    log = target_logger if target_logger is not None else logging.getLogger()
    log.setLevel(level)
    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setFormatter(StructuredJsonFormatter(engine=engine, worker_id=worker_id))
    log.addHandler(handler)
    return handler
