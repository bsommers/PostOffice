# Phase 3: Route Engine Trie Optimization - Discussion Log

**Date:** 2026-10-06  
**Participants:** User & Antigravity  

### Questions & Locked Decisions

1. **Where should the Trie data structure live?**
   - **Decision:** Dedicated module `src/postoffice/trie.py` with separate `TopicTrie` and `TrieNode` classes imported into `Router`.
   - **Rationale:** Decouples topic prefix matching from broker registry and confirmation coordinating logic, making it independently testable and benchmarkable.

2. **How should multi-client routing be indexed in the Trie?**
   - **Decision:** Per-client Trie dictionary (`self.client_tries[source_client] = TopicTrie()`) for $O(1)$ client lookup followed by $O(k)$ topic traversal.
   - **Rationale:** Avoids prefix contamination across client topics and preserves clean modular isolation per client broker.

3. **How should overlapping wildcard and exact routes be resolved?**
   - **Decision:** Maintain full fanout semantics: a message matches and dispatches to ALL matching routes (exact, `+`, and `#`).
   - **Rationale:** Aligns with standard pub/sub routing semantics where multiple subscriptions from different consumers are independently fulfilled.

4. **What verification benchmark should be established for ROUT-01?**
   - **Decision:** Include a dedicated benchmark script (`tests/benchmark_trie.py`) and unit tests comparing Trie vs linear search scaling up to 10,000 routes.
   - **Rationale:** Provides empirical, measurable proof that route matching scaling moves from $O(N)$ to $O(k)$.
