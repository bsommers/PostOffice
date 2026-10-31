"""
Perf message header, shared byte-for-byte with the native load generator and sink.

Layout (32 bytes, big-endian), followed by the body:

    0  magic       4s  b"POB1"
    4  producer    u32
    8  seq         u64  per-producer, starting at 0
   16  send_ts_ns  u64  CLOCK_REALTIME ns at the *intended* send time (open loop)
   24  crc32       u32  zlib.crc32 of the body
   28  reserved    u32  0
"""
import struct
import zlib
from typing import NamedTuple, Optional

MAGIC = b"POB1"
_FMT = struct.Struct(">4sIQQII")
HEADER_SIZE = _FMT.size  # 32


class Header(NamedTuple):
    producer: int
    seq: int
    send_ts_ns: int
    intact: bool  # crc matches the body
    body: bytes


def encode(producer: int, seq: int, send_ts_ns: int, body: bytes = b"", size: Optional[int] = None) -> bytes:
    """Builds a message; ``size`` pads the body with zeros so the whole message is exactly ``size`` bytes."""
    if size is not None:
        if size < HEADER_SIZE + len(body):
            raise ValueError(f"size must be >= {HEADER_SIZE + len(body)}")
        body = body + bytes(size - HEADER_SIZE - len(body))
    return _FMT.pack(MAGIC, producer, seq, send_ts_ns, zlib.crc32(body), 0) + body


def decode(message: bytes) -> Optional[Header]:
    """Returns None for messages that are not perf messages (wrong magic or too short)."""
    if len(message) < HEADER_SIZE or message[:4] != MAGIC:
        return None
    _, producer, seq, ts, crc, _ = _FMT.unpack_from(message)
    body = message[HEADER_SIZE:]
    return Header(producer, seq, ts, zlib.crc32(body) == crc, body)
