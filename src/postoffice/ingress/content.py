import json
import re
from typing import Any, Dict, Iterable, List, Optional, Tuple

from postoffice.ingress.rules import (
    ALLOW, Action, MessageContext, Rule, Verdict, parse_action, topic_matches,
)
from postoffice.schema.converters import SchemaValidationError

FORMATS = ("json", "avro", "protobuf")
RANGE_BOUNDS = ("min", "max")

# Every byte falls in exactly one group, and a string that starts always succeeds (an unterminated
# one, even ending in a lone backslash, consumes the rest of the input as one token). That keeps
# the scan a single linear pass; without the \\\Z alternative a crafted tail made it quadratic.
_TOKEN = re.compile(
    rb'(?P<ws>\s+)|(?P<str>"(?:[^"\\]++|\\.|\\\Z)*+(?:"|\Z))|(?P<punct>[\[\]{}:,])|(?P<other>[^\s\[\]{}:,"]+)',
    re.S,
)
_OPEN, _CLOSE = (ord("{"), ord("[")), (ord("}"), ord("]"))
_BRACKET, _COLON, _COMMA = ord("["), ord(":"), ord(",")


def scan_json(data: bytes, max_depth: int, max_keys: int, max_array_length: int,
              max_string_length: int) -> Optional[str]:
    """Bounds JSON in one linear pass *before* ``json.loads``. Returns the first violation or None."""
    depth = keys = 0
    stack: List[int] = []  # -1 for an object, else the comma count of an array
    for m in _TOKEN.finditer(data):
        kind = m.lastgroup
        if kind == "str":
            if m.end() - m.start() - 2 > max_string_length:
                return f"string length > {max_string_length}"
        elif kind == "punct":
            c = data[m.start()]
            if c in _OPEN or c == _BRACKET:
                depth += 1
                if depth > max_depth:
                    return f"depth > {max_depth}"
                stack.append(0 if c == _BRACKET else -1)
            elif c in _CLOSE:
                depth = max(0, depth - 1)
                if stack:
                    stack.pop()
            elif c == _COLON:
                keys += 1
                if keys > max_keys:
                    return f"keys > {max_keys}"
            elif c == _COMMA and stack and stack[-1] >= 0:
                stack[-1] += 1
                if stack[-1] >= max_array_length:
                    return f"array length > {max_array_length}"
    return None


class ContentGuard(Rule):
    """
    Application-aware payload checks for one topic pattern; other topics cost a pattern match.

    JSON is bounded by an iterative byte scan *before* ``json.loads``, so hostile nesting or
    size cannot cause ``RecursionError`` or memory blow-up. Avro/Protobuf payloads are decoded
    through the router's ``SchemaConverter`` (bound by ``Router.set_firewall``). Structural
    violations are ``malformed``/``oversize``; schema and business-rule violations are
    ``schema_mismatch``.
    """
    name = "content_guard"

    def __init__(
        self,
        topic: str,
        format: str = "json",
        schema: Optional[str] = None,
        max_bytes: Optional[int] = None,
        max_depth: int = 32,
        max_keys: int = 1000,
        max_array_length: int = 10000,
        max_string_length: int = 65536,
        required_fields: Optional[List[str]] = None,
        field_ranges: Optional[Dict[str, Dict[str, float]]] = None,
        enums: Optional[Dict[str, List[Any]]] = None,
        action: Action = Action.REJECT,
        brokers: Optional[Iterable[str]] = None,
    ):
        super().__init__(brokers)
        if format not in FORMATS:
            raise ValueError(f"format must be one of {FORMATS}")
        if format == "json" and schema:
            raise ValueError("schema applies to avro/protobuf; use required_fields/field_ranges/enums for json")
        if format != "json" and not schema:
            raise ValueError(f"format '{format}' requires a schema name")
        if min(max_depth, max_keys, max_array_length, max_string_length) <= 0 or (max_bytes is not None and max_bytes <= 0):
            raise ValueError("limits must be > 0")
        self.field_ranges = dict(field_ranges or {})
        for field, bounds in self.field_ranges.items():
            if not bounds or set(bounds) - set(RANGE_BOUNDS):
                raise ValueError(f"field_ranges['{field}'] must use only {RANGE_BOUNDS}")
        self.topic, self.format, self.schema = topic, format, schema
        self.max_bytes, self.max_depth, self.max_keys = max_bytes, max_depth, max_keys
        self.max_array_length, self.max_string_length = max_array_length, max_string_length
        self.required_fields = list(required_fields or [])
        self.enums = {k: list(v) for k, v in (enums or {}).items()}
        self.action = parse_action(action)
        self._converter = None

    def bind_converter(self, converter) -> None:
        self._converter = converter

    def _bad(self, category: str, reason: str) -> Verdict:
        return Verdict(self.action, self.name, category, reason)

    def _check(self, ctx: MessageContext) -> Verdict:
        if not topic_matches(self.topic, ctx.topic) or ctx.payload is None:
            return ALLOW
        payload = ctx.payload
        if self.max_bytes is not None and len(payload) > self.max_bytes:
            return self._bad("oversize", f"{len(payload)} > {self.max_bytes} bytes")
        if self.format == "json":
            record, problem = self._parse_json(payload)
            if problem:
                return self._bad("malformed", problem)
        else:
            if self._converter is None:
                raise RuntimeError("ContentGuard has no schema converter bound")
            try:
                record = self._converter.deserialize(payload, self.format, self.schema)
            except SchemaValidationError as e:
                return self._bad("schema_mismatch", str(e))
        problem = self._business_problem(record)
        return self._bad("schema_mismatch", problem) if problem else ALLOW

    def _parse_json(self, payload: bytes) -> Tuple[Any, Optional[str]]:
        problem = self._scan(payload)
        if problem:
            return None, problem
        try:
            return json.loads(payload.decode("utf-8")), None
        except UnicodeDecodeError:
            return None, "invalid utf-8"
        except (ValueError, RecursionError) as e:
            return None, f"invalid json: {e}"

    def _scan(self, data: bytes) -> Optional[str]:
        return scan_json(data, self.max_depth, self.max_keys, self.max_array_length, self.max_string_length)

    @staticmethod
    def _lookup(record: Any, path: str) -> Tuple[bool, Any]:
        for part in path.split("."):
            if not isinstance(record, dict) or part not in record:
                return False, None
            record = record[part]
        return True, record

    def _business_problem(self, record: Any) -> Optional[str]:
        for path in self.required_fields:
            if not self._lookup(record, path)[0]:
                return f"required field '{path}' missing"
        for path, bounds in self.field_ranges.items():
            found, value = self._lookup(record, path)
            if not found:
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                return f"field '{path}' is not a number"
            if ("min" in bounds and value < bounds["min"]) or ("max" in bounds and value > bounds["max"]):
                return f"field '{path}'={value} outside {bounds}"
        for path, allowed in self.enums.items():
            found, value = self._lookup(record, path)
            if found and value not in allowed:
                return f"field '{path}'={value!r} not in {allowed}"
        return None
