# Phase 3: Route Engine Trie Optimization - Research

**Researched:** 2026-10-06  
**Domain:** Hierarchical Topic Trees, MQTT Wildcard Matching Algorithms, Radix/Prefix Tries  
**Confidence:** HIGH  

<user_constraints>
## User Constraints (from 03-CONTEXT.md)

### Locked Decisions
- **D-01:** Implement the Trie in a dedicated module `src/postoffice/trie.py` defining `TrieNode` and `TopicTrie`. `Router` imports and delegates to `TopicTrie`.
- **D-02:** Node structure:
  - `children`: Dict mapping segment string to child `TrieNode`.
  - `wildcard_single`: Optional child `TrieNode` for `+`.
  - `wildcard_multi`: Optional list for `#` matching zero or more trailing segments.
  - `routes`: List of route configurations terminating at this node.
- **D-03:** Multi-client indexing: Use a per-client Trie map in `Router`: `self.client_tries: Dict[str, TopicTrie] = {}`.
  - Lookup begins with $O(1)$ client dictionary lookup, followed by $O(k)$ segment traversal on the client's specific Trie.
- **D-04:** Topic segmentation: Segment incoming topics by `/`.
- **D-05:** Matching traversal rules:
  - Exact match: Traverse into `children[segment]`.
  - Single-level wildcard `+`: Traverse into `wildcard_single` child node for any non-empty segment.
  - Multi-level wildcard `#`: If present at a node, matches this and all remaining segments (including matching when no further segments exist).
- **D-06:** Overlap & Fanout: When multiple patterns match (e.g. `sensor/temp` and `sensor/+` and `#`), collect all terminal `routes` across matching branches and dispatch to all targets.
- **D-07:** Create hermetic unit tests in `tests/test_trie.py` verifying exact match, `+`, `#`, mixed patterns, and edge cases.
- **D-08:** Create `tests/benchmark_trie.py` benchmarking linear scan vs Trie lookup across 10, 100, 1,000, and 10,000 routes.

### Claude's Discretion
- Internal recursive vs iterative DFS traversal implementation in `TopicTrie.match()`.
- Trie node data attributes and clean-up / route deletion mechanics.
- Backward compatibility wrapper on `Router._topic_match`.

</user_constraints>

<architectural_responsibility_map>
## Architectural Responsibility Map

| Component | Responsibility | Performance Target |
|-----------|----------------|-------------------|
| `TrieNode` (`src/postoffice/trie.py`) | Individual segment node holding exact children, `+` child, `#` payloads, and terminal routes | Minimal memory overhead |
| `TopicTrie` (`src/postoffice/trie.py`) | Route insertion, topic matching with `+` and `#` traversal, clear/reset operations | $O(k)$ where $k$ is topic depth |
| `Router` (`src/postoffice/router.py`) | Per-client Trie routing delegation, confirmation coordination, semantic translation | $O(1)$ client lookup + $O(k)$ route matching |
| `tests/test_trie.py` | Isolated unit testing of `TopicTrie` (wildcards, edge cases, overlap) | 100% test coverage |
| `tests/benchmark_trie.py` | Scaling benchmark comparing linear scan vs Trie lookup (10 to 10,000 routes) | Documented speedup curve |

</architectural_responsibility_map>

<research_summary>
## Algorithmic Deep Dive: Topic Trie Matching for MQTT

### 1. Topic Segmentation
An MQTT topic is a UTF-8 string delimited by forward slash (`/`).
Example: `home/groundfloor/livingroom/temperature` has 4 segments: `['home', 'groundfloor', 'livingroom', 'temperature']`.
Special cases:
- `/leading/slash` -> `['', 'leading', 'slash']`
- `trailing/slash/` -> `['trailing', 'slash', '']`
- Exact empty segment `''` between slashes must be preserved according to MQTT 3.1.1 / 5.0 specifications.

### 2. Node Schema
```python
class TrieNode:
    def __init__(self):
        self.children = {}          # Dict[str, TrieNode] for literal segments
        self.wildcard_single = None # Optional[TrieNode] for '+'
        self.wildcard_multi = []    # List[Any] for '#' terminal payloads
        self.routes = []            # List[Any] for exact terminal payloads
```

### 3. Insertion Logic (`TopicTrie.insert(topic_pattern, route_data)`)
Validate MQTT syntax:
- `#` cannot appear before the last segment (e.g. `a/#/b` is invalid).
- If pattern ends with `#`:
  - Traverse to the parent node of `#`.
  - Append `route_data` to parent `wildcard_multi`.
- If segment is `+`:
  - Traverse into `node.wildcard_single` (create if None).
- Else:
  - Traverse into `node.children[segment]` (create if not present).
- At final segment (if not ending in `#`), append to `node.routes`.

### 4. Matching Logic (`TopicTrie.match(topic) -> List[Any]`)
Split topic by `/` into `segments`.
Use depth-first traversal (DFS):
```python
results = []
def _match(node, idx):
    # 1. Any '#' registered at this node matches all remaining segments (zero or more)
    if node.wildcard_multi:
        results.extend(node.wildcard_multi)

    # 2. If we reached end of topic segments, collect exact terminal routes
    if idx == len(segments):
        if node.routes:
            results.extend(node.routes)
        return

    seg = segments[idx]

    # 3. Literal segment branch
    child = node.children.get(seg)
    if child is not None:
        _match(child, idx + 1)

    # 4. Single-level wildcard branch
    if node.wildcard_single is not None:
        _match(node.wildcard_single, idx + 1)

_match(self.root, 0)
return results
```
Complexity:
- In the absence of wildcards: strictly $O(k)$ where $k$ is number of topic levels (typically 2 to 6).
- With wildcards: explores only matching branches, independent of the total number of registered routes $N$.

### 5. Benchmark Expectations
- Linear scan: At $N = 10,000$ routes, every lookup requires 10,000 string comparisons and regex/split checks, taking ~5-15 milliseconds per message (~70-200 lookups/sec).
- Topic Trie: At $N = 10,000$ routes, lookup requires $k$ dictionary lookups (e.g. $k=4$), taking ~2-5 microseconds per message (~200,000-500,000 lookups/sec).
- Expected speedup: **100x to 1000x** at 10,000 routes.

</research_summary>

<verification_strategy>
## Verification Strategy

1. **Unit Tests (`tests/test_trie.py`):**
   - Exact topic matching (single-level, multi-level).
   - Single-level wildcard `+` matching at beginning, middle, and end.
   - Multi-level wildcard `#` matching single level, multiple levels, and zero levels.
   - Mixed wildcards (`sensor/+/room/#`).
   - Root wildcards (`+` and `#`).
   - Non-matching topics.
   - Invalid pattern rejection (e.g. `foo/#/bar` raises `ValueError`).
   - Route removal / clear state.

2. **Integration & Regression Tests:**
   - Run existing `tests/test_wildcards.py`, `tests/test_postoffice.py`, `tests/test_delivery_guarantees.py`. All 22 tests must pass with zero changes to test files.

3. **Benchmark Script (`tests/benchmark_trie.py`):**
   - Measure lookup throughput for Linear vs Trie at $N \in \{10, 100, 1000, 10000\}$.
   - Verify scaling is $O(k)$ vs $O(N)$.

</verification_strategy>
