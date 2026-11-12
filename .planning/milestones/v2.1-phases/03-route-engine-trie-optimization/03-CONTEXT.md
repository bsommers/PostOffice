# Phase 3: Route Engine Trie Optimization - Context

**Gathered:** 2026-10-06  
**Status:** Ready for planning  

<domain>
## Phase Boundary

Phase 3 replaces the linear $O(N)$ topic route matching scan in PostOffice with an MQTT-compliant hierarchical Radix/Prefix Trie matching engine:
1. Designing and implementing a dedicated `TopicTrie` and `TrieNode` module in `src/postoffice/trie.py`.
2. Integrating the Trie into `Router` using per-client indexing (`self.client_tries[source_client] = TopicTrie()`).
3. Ensuring 100% compliance with MQTT topic wildcard rules (`+` single-level, `#` multi-level, exact matches).
4. Retaining full fanout routing semantics: if multiple routes match an incoming topic, all matching routes are triggered.
5. Providing benchmark and regression verification demonstrating $O(k)$ time complexity scaling up to 10,000 routes.

</domain>

<decisions>
## Implementation Decisions

### Module Location & Structure
- **D-01:** Implement the Trie in a dedicated module `src/postoffice/trie.py` defining `TrieNode` and `TopicTrie`. `Router` imports and delegates to `TopicTrie`.
- **D-02:** Node structure:
  - `children`: Dict mapping segment string to child `TrieNode`.
  - `wildcard_single`: Optional child `TrieNode` for `+`.
  - `wildcard_multi`: Optional list/entry for `#` matching zero or more trailing segments.
  - `routes`: List of route configurations terminating at this node.

### Multi-Client Indexing
- **D-03:** Use a per-client Trie map in `Router`: `self.client_tries: Dict[str, TopicTrie] = {}`.
  - Lookup begins with $O(1)$ client dictionary lookup, followed by $O(k)$ segment traversal on the client's specific Trie.

### Wildcard & Fanout Matching Semantics
- **D-04:** Topic segmentation: Segment incoming topics by `/`.
- **D-05:** Matching traversal rules:
  - Exact match: Traverse into `children[segment]`.
  - Single-level wildcard `+`: Traverse into `wildcard_single` child node for any non-empty segment.
  - Multi-level wildcard `#`: If present at a node, matches this and all remaining segments (including matching when no further segments exist).
- **D-06:** Overlap & Fanout: When multiple patterns match (e.g. `sensor/temp` and `sensor/+` and `#`), collect all terminal `routes` across matching branches and dispatch to all targets.

### Verification & Performance Benchmarking
- **D-07:** Create hermetic unit tests in `tests/test_trie.py` verifying exact match, `+`, `#`, mixed patterns, and edge cases (leading/trailing `/`, empty segments).
- **D-08:** Create `tests/benchmark_trie.py` benchmarking linear scan vs Trie lookup across 10, 100, 1,000, and 10,000 routes, measuring lookups/second and latency distribution.

### Claude's Discretion
- Internal recursive vs iterative DFS traversal implementation in `TopicTrie.match()`.
- Trie node data attributes and clean-up / route deletion mechanics.

</decisions>

<canonical_refs>
## Canonical References

### Source Files
- `src/postoffice/router.py` — Current linear route matching implementation
- `tests/test_wildcards.py` — Baseline MQTT wildcard verification tests
- `tests/test_postoffice.py` — Facade and integration route tests
- `tests/test_delivery_guarantees.py` — Confirmation callbacks across routes

### Architecture & Requirements
- `.planning/REQUIREMENTS.md` — Requirement `ROUT-01`
- `.planning/ROADMAP.md` — Phase 3 success criteria
- `.planning/research/LANGUAGE_REWRITE_SPIKE.md` — Algorithmic optimization rationale

</canonical_refs>

<threat_model>
## Security & Correctness Boundary

1. **Denial of Service via Degenerate Wildcards:** Malicious or malformed topics with thousands of `/` segments must not cause RecursionError or stack overflow; implement defensive depth checks.
2. **Wildcard Spec Conformance:** In MQTT:
   - `+` must match exactly one level (e.g. `a/+/b` matches `a/x/b` but not `a/x/y/b` or `a/b`).
   - `#` must only appear as the last character in a topic pattern (or alone). If `#` appears mid-topic, raise `ValueError` on route registration.
3. **Thread Safety:** Ensure route addition and route matching do not cause dictionary mutation runtime errors during concurrent publish/routing.

</threat_model>
