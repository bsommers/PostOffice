from typing import Dict, List, Any, Optional

class TrieNode:
    """A node in the TopicTrie representing a single topic path segment."""
    def __init__(self):
        self.children: Dict[str, TrieNode] = {}
        self.wildcard_single: Optional[TrieNode] = None  # for single-level wildcard '+'
        self.wildcard_multi: List[Any] = []             # for multi-level wildcard '#'
        self.routes: List[Any] = []                     # exact terminal route payloads


class TopicTrie:
    """
    Hierarchical Radix/Prefix Trie optimized for MQTT-style topic matching.
    Supports exact matching, single-level wildcards ('+'), and multi-level wildcards ('#').
    """
    def __init__(self):
        self.root = TrieNode()
        self._count = 0

    def insert(self, topic_pattern: str, route_data: Any) -> None:
        """
        Inserts a topic pattern and associated route configuration into the trie.
        Raises ValueError if MQTT wildcard placement rules are violated.
        """
        segments = topic_pattern.split('/')
        for i, seg in enumerate(segments):
            if seg == '#' and i != len(segments) - 1:
                raise ValueError(f"Multi-level wildcard '#' must be the last topic segment: '{topic_pattern}'")
            if '+' in seg and seg != '+':
                raise ValueError(f"Single-level wildcard '+' must occupy the entire segment: '{topic_pattern}'")
            if '#' in seg and seg != '#':
                raise ValueError(f"Multi-level wildcard '#' must occupy the entire segment: '{topic_pattern}'")

        node = self.root
        for seg in segments:
            if seg == '#':
                node.wildcard_multi.append(route_data)
                self._count += 1
                return
            elif seg == '+':
                if node.wildcard_single is None:
                    node.wildcard_single = TrieNode()
                node = node.wildcard_single
            else:
                if seg not in node.children:
                    node.children[seg] = TrieNode()
                node = node.children[seg]

        node.routes.append(route_data)
        self._count += 1

    def match(self, topic: str) -> List[Any]:
        """
        Matches a concrete message topic against all registered patterns in O(k) time,
        where k is the number of segments in the topic.
        Returns all matching route configurations across exact and wildcard branches.
        """
        segments = topic.split('/')
        results: List[Any] = []

        def _dfs(node: TrieNode, idx: int) -> None:
            # 1. Any multi-level wildcard '#' registered at this level matches all remaining segments
            if node.wildcard_multi:
                results.extend(node.wildcard_multi)

            # 2. Reached the end of message topic segments; collect exact terminal routes
            if idx == len(segments):
                if node.routes:
                    results.extend(node.routes)
                return

            seg = segments[idx]

            # 3. Exact matching segment branch
            child = node.children.get(seg)
            if child is not None:
                _dfs(child, idx + 1)

            # 4. Single-level wildcard '+' branch
            if node.wildcard_single is not None:
                _dfs(node.wildcard_single, idx + 1)

        _dfs(self.root, 0)
        return results

    def clear(self) -> None:
        """Clears all routes from the trie."""
        self.root = TrieNode()
        self._count = 0

    def __len__(self) -> int:
        return self._count
