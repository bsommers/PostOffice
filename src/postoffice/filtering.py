"""
Dynamic content-based route filtering (`FLT-01`).

Evaluates declarative JSONPath + metadata predicates and boolean combinators
(`all`, `any`, `not`) against message payloads and ingress `source_kwargs`.
Maintains bit-for-bit parity with `native/libpostoffice/src/engine.rs`.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional, Tuple, Union

VALID_OPS = frozenset(
    {
        "==",
        "eq",
        "!=",
        "ne",
        ">",
        "gt",
        ">=",
        "gte",
        "<",
        "lt",
        "<=",
        "lte",
        "in",
        "contains",
        "exists",
        "prefix",
        "startswith",
        "suffix",
        "endswith",
    }
)

ORDERING_OPS = frozenset({">", "gt", ">=", "gte", "<", "lt", "<=", "lte"})
PREFIX_SUFFIX_OPS = frozenset({"prefix", "startswith", "suffix", "endswith"})
COMBINATOR_KEYS = frozenset({"all", "any", "not"})
LEAF_KEYS = frozenset({"path", "op", "value"})

Step = Tuple[str, Union[str, int]]  # ("key", str) or ("index", int)


def _is_number(val: Any) -> bool:
    return isinstance(val, (int, float)) and not isinstance(val, bool)


def _is_valid_json_value(val: Any, depth: int = 0) -> bool:
    if depth > 16:
        return False
    if val is None or isinstance(val, (bool, str)) or _is_number(val):
        return True
    if isinstance(val, list):
        return all(_is_valid_json_value(item, depth + 1) for item in val)
    if isinstance(val, dict):
        return all(
            isinstance(k, str) and _is_valid_json_value(v, depth + 1)
            for k, v in val.items()
        )
    return False


def _parse_path(path: str) -> Tuple[str, List[Step]]:
    """
    Parses a filter path into `(target, steps)` where `target` is one of
    `"payload"`, `"kwargs"`, or `"headers"`.
    Raises `ValueError` on malformed paths.
    """
    if not isinstance(path, str) or not path or len(path) > 256:
        raise ValueError("filter path must be a non-empty string (<= 256 chars)")

    if path.startswith("$kwargs."):
        target = "kwargs"
        rel = path[len("$kwargs.") :]
    elif path.startswith("$headers."):
        target = "headers"
        rel = path[len("$headers.") :]
    elif path.startswith("$."):
        target = "payload"
        rel = path[2:]
    elif path.startswith("$["):
        target = "payload"
        rel = path[1:]
    elif path.startswith("$"):
        raise ValueError(f"unsupported filter path root in {path!r}")
    else:
        target = "payload"
        rel = path

    if not rel or rel.startswith(".") or rel.endswith(".") or ".." in rel:
        raise ValueError(f"invalid filter path syntax: {path!r}")

    steps: List[Step] = []
    for token in rel.split("."):
        if not token:
            raise ValueError(f"empty path segment in {path!r}")
        if "[" not in token and "]" not in token:
            steps.append(("key", token))
            continue

        bracket_pos = token.find("[")
        if bracket_pos == -1 or ("]" in token[:bracket_pos]):
            raise ValueError(f"malformed bracket in path segment {token!r}")

        prefix = token[:bracket_pos]
        if prefix:
            steps.append(("key", prefix))

        pos = bracket_pos
        n = len(token)
        while pos < n:
            if token[pos] != "[":
                raise ValueError(f"unexpected characters after ']' in {token!r}")
            close_pos = token.find("]", pos + 1)
            if close_pos == -1:
                raise ValueError(f"unclosed '[' in {token!r}")
            idx_str = token[pos + 1 : close_pos]
            if not idx_str or not idx_str.isdecimal() or not idx_str.isascii():
                raise ValueError(f"array index must be non-negative digits in {token!r}")
            steps.append(("index", int(idx_str)))
            pos = close_pos + 1

    if not steps:
        raise ValueError(f"filter path produced no lookup steps: {path!r}")
    return target, steps


def validate_filter_spec(spec: Any, max_depth: int = 8) -> None:
    """
    Validates a filter specification structure, operators, paths, and nesting depth.
    Raises `ValueError` on any invalid specification.
    """
    if max_depth < 1:
        raise ValueError("filter nesting exceeds max_depth")
    if not isinstance(spec, dict) or not spec:
        raise ValueError("filter spec must be a non-empty object")

    keys = set(spec.keys())
    comb = keys & COMBINATOR_KEYS
    if comb:
        if len(keys) != 1:
            raise ValueError("combinator filter must contain exactly one key")
        key = next(iter(comb))
        sub = spec[key]
        if key in ("all", "any"):
            if not isinstance(sub, list) or not sub or len(sub) > 64:
                raise ValueError(f"'{key}' combinator must be a non-empty list (<= 64 items)")
            for item in sub:
                validate_filter_spec(item, max_depth - 1)
        else:  # "not"
            validate_filter_spec(sub, max_depth - 1)
        return

    if not keys <= LEAF_KEYS:
        raise ValueError("filter predicate contains unknown keys")
    if "path" not in spec or "op" not in spec:
        raise ValueError("filter predicate requires 'path' and 'op'")

    _parse_path(spec["path"])

    op = spec["op"]
    if not isinstance(op, str) or op not in VALID_OPS:
        raise ValueError(f"unsupported filter operator: {op!r}")

    if op == "exists":
        if "value" in spec and not isinstance(spec["value"], bool):
            raise ValueError("'exists' operator requires a boolean value")
        return

    if "value" not in spec:
        raise ValueError(f"operator {op!r} requires a 'value'")

    val = spec["value"]
    if not _is_valid_json_value(val):
        raise ValueError("filter value must be a valid JSON value")

    if op in ORDERING_OPS:
        if not (_is_number(val) or isinstance(val, str)):
            raise ValueError(f"operator {op!r} requires a numeric or string value")
    elif op == "in":
        if not isinstance(val, (list, str)):
            raise ValueError("operator 'in' requires a list or string value")
    elif op in PREFIX_SUFFIX_OPS:
        if not isinstance(val, str):
            raise ValueError(f"operator {op!r} requires a string value")


def _json_eq(a: Any, b: Any) -> bool:
    """Strict JSON equality where `True != 1` and `False != 0`."""
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, bool) or isinstance(b, bool):
        return isinstance(a, bool) and isinstance(b, bool) and a == b
    if _is_number(a) and _is_number(b):
        return float(a) == float(b)
    if isinstance(a, str) and isinstance(b, str):
        return a == b
    if isinstance(a, list) and isinstance(b, list):
        return len(a) == len(b) and all(_json_eq(x, y) for x, y in zip(a, b))
    if isinstance(a, dict) and isinstance(b, dict):
        return a.keys() == b.keys() and all(_json_eq(a[k], b[k]) for k in a)
    return False


def _resolve_steps(root: Any, steps: List[Step]) -> Tuple[bool, Any]:
    cur = root
    for kind, key_or_idx in steps:
        if kind == "key":
            if not isinstance(cur, dict) or key_or_idx not in cur:
                return False, None
            cur = cur[key_or_idx]
        else:
            if not isinstance(cur, list) or key_or_idx >= len(cur):
                return False, None
            cur = cur[key_or_idx]
    return True, cur


def _eval_op(op: str, found: bool, extracted: Any, expected: Any) -> bool:
    if op == "exists":
        want = True if expected is None else bool(expected)
        present = found and (extracted is not None)
        return present == want

    if not found:
        return False

    if op in ("==", "eq"):
        return _json_eq(extracted, expected)
    if op in ("!=", "ne"):
        return not _json_eq(extracted, expected)

    if op in ORDERING_OPS:
        if _is_number(extracted) and _is_number(expected):
            lhs, rhs = float(extracted), float(expected)
        elif isinstance(extracted, str) and isinstance(expected, str):
            lhs, rhs = extracted, expected
        else:
            return False
        if op in (">", "gt"):
            return lhs > rhs
        if op in (">=", "gte"):
            return lhs >= rhs
        if op in ("<", "lt"):
            return lhs < rhs
        return lhs <= rhs

    if op == "in":
        if isinstance(expected, list):
            return any(_json_eq(extracted, item) for item in expected)
        if isinstance(expected, str) and isinstance(extracted, str):
            return extracted in expected
        return False

    if op == "contains":
        if isinstance(extracted, list):
            return any(_json_eq(item, expected) for item in extracted)
        if isinstance(extracted, str) and isinstance(expected, str):
            return expected in extracted
        return False

    if op in ("prefix", "startswith"):
        return isinstance(extracted, str) and isinstance(expected, str) and extracted.startswith(expected)

    if op in ("suffix", "endswith"):
        return isinstance(extracted, str) and isinstance(expected, str) and extracted.endswith(expected)

    return False


class _PayloadCache:
    __slots__ = ("raw", "parsed", "valid")

    def __init__(self, raw: bytes) -> None:
        self.raw = raw
        self.parsed: Any = None
        self.valid: Optional[bool] = None

    def get(self) -> Tuple[bool, Any]:
        if self.valid is None:
            try:
                text = self.raw.decode("utf-8")
                val = json.loads(text)
                if isinstance(val, (dict, list)):
                    self.parsed = val
                    self.valid = True
                else:
                    self.valid = False
            except Exception:
                self.valid = False
        return bool(self.valid), self.parsed


def _evaluate_inner(
    spec: Any,
    cache: _PayloadCache,
    source_kwargs: Dict[str, Any],
    depth: int,
) -> bool:
    if depth <= 0 or not isinstance(spec, dict) or not spec:
        return False

    if "all" in spec and len(spec) == 1:
        sub = spec["all"]
        if not isinstance(sub, list) or not sub:
            return False
        return all(_evaluate_inner(item, cache, source_kwargs, depth - 1) for item in sub)

    if "any" in spec and len(spec) == 1:
        sub = spec["any"]
        if not isinstance(sub, list) or not sub:
            return False
        return any(_evaluate_inner(item, cache, source_kwargs, depth - 1) for item in sub)

    if "not" in spec and len(spec) == 1:
        return not _evaluate_inner(spec["not"], cache, source_kwargs, depth - 1)

    path = spec.get("path")
    op = spec.get("op")
    if not isinstance(path, str) or not isinstance(op, str) or op not in VALID_OPS:
        return False
    if op != "exists" and "value" not in spec:
        return False

    try:
        target, steps = _parse_path(path)
    except ValueError:
        return False

    if target == "payload":
        ok, root = cache.get()
        if not ok:
            return False
    elif target == "kwargs":
        root = source_kwargs
    else:  # "headers"
        headers = source_kwargs.get("headers")
        if not isinstance(headers, dict):
            return _eval_op(op, False, None, spec.get("value", True if op == "exists" else None))
        root = headers

    found, extracted = _resolve_steps(root, steps)
    expected = spec.get("value", True) if op == "exists" else spec.get("value")
    return _eval_op(op, found, extracted, expected)


def evaluate_filter(
    spec: Any,
    payload: bytes,
    source_kwargs: Optional[Dict[str, Any]] = None,
) -> bool:
    """
    Evaluates `spec` against `payload` and `source_kwargs`.
    Returns `True` if the message matches the filter, `False` otherwise.
    """
    if spec is None:
        return True
    cache = _PayloadCache(payload if isinstance(payload, (bytes, bytearray)) else b"")
    kwargs = source_kwargs if isinstance(source_kwargs, dict) else {}
    return _evaluate_inner(spec, cache, kwargs, 8)
