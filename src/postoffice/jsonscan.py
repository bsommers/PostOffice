import re
from typing import List, Optional

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
