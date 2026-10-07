import json
import base64
from datetime import datetime, timezone
from typing import Optional, Dict, Any

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
