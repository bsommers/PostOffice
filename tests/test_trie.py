import unittest
from postoffice.trie import TopicTrie

class TestTopicTrie(unittest.TestCase):
    def setUp(self):
        self.trie = TopicTrie()

    def test_exact_match(self):
        self.trie.insert("sensor/temperature", {"id": 1})
        self.trie.insert("sensor/humidity", {"id": 2})

        matches = self.trie.match("sensor/temperature")
        self.assertEqual(len(matches), 1)
        self.assertEqual(matches[0]["id"], 1)

        matches_hum = self.trie.match("sensor/humidity")
        self.assertEqual(len(matches_hum), 1)
        self.assertEqual(matches_hum[0]["id"], 2)

        # Non-matching topic
        self.assertEqual(self.trie.match("sensor/pressure"), [])

    def test_single_level_wildcard(self):
        # '+' at middle
        self.trie.insert("sport/+/player", {"id": "middle_plus"})
        # '+' at start
        self.trie.insert("+/tennis/ranking", {"id": "start_plus"})
        # '+' at end
        self.trie.insert("devices/status/+", {"id": "end_plus"})

        # Matches
        self.assertEqual(self.trie.match("sport/tennis/player"), [{"id": "middle_plus"}])
        self.assertEqual(self.trie.match("atp/tennis/ranking"), [{"id": "start_plus"}])
        self.assertEqual(self.trie.match("devices/status/online"), [{"id": "end_plus"}])

        # Non-matches (wrong depth)
        self.assertEqual(self.trie.match("sport/tennis/extra/player"), [])
        self.assertEqual(self.trie.match("sport/player"), [])
        self.assertEqual(self.trie.match("devices/status/online/more"), [])

    def test_multi_level_wildcard(self):
        self.trie.insert("sport/tennis/#", {"id": "tennis_multi"})

        # Matches multi-level
        self.assertEqual(self.trie.match("sport/tennis/player1"), [{"id": "tennis_multi"}])
        self.assertEqual(self.trie.match("sport/tennis/player1/ranking"), [{"id": "tennis_multi"}])

        # MQTT standard: '#' matches parent level itself (zero trailing segments)
        self.assertEqual(self.trie.match("sport/tennis"), [{"id": "tennis_multi"}])

        # Non-matches
        self.assertEqual(self.trie.match("sport/football/player1"), [])
        self.assertEqual(self.trie.match("other/sport/tennis"), [])

    def test_root_wildcards(self):
        trie_root_multi = TopicTrie()
        trie_root_multi.insert("#", {"id": "all"})
        self.assertEqual(trie_root_multi.match("any/topic/here"), [{"id": "all"}])
        self.assertEqual(trie_root_multi.match("single"), [{"id": "all"}])

        trie_root_single = TopicTrie()
        trie_root_single.insert("+", {"id": "single_level"})
        self.assertEqual(trie_root_single.match("device1"), [{"id": "single_level"}])
        self.assertEqual(trie_root_single.match("device1/status"), [])

    def test_combined_wildcards(self):
        self.trie.insert("building/+/floor/#", {"id": "combo"})

        self.assertEqual(self.trie.match("building/hq/floor/1"), [{"id": "combo"}])
        self.assertEqual(self.trie.match("building/hq/floor/1/room/101"), [{"id": "combo"}])
        self.assertEqual(self.trie.match("building/hq/floor"), [{"id": "combo"}])

        # Non-matching
        self.assertEqual(self.trie.match("building/floor/1"), [])

    def test_overlapping_route_fanout(self):
        self.trie.insert("sensor/temp", {"type": "exact"})
        self.trie.insert("sensor/+", {"type": "single"})
        self.trie.insert("sensor/#", {"type": "multi"})
        self.trie.insert("#", {"type": "catchall"})

        matches = self.trie.match("sensor/temp")
        matched_types = {m["type"] for m in matches}
        self.assertEqual(matched_types, {"exact", "single", "multi", "catchall"})
        self.assertEqual(len(matches), 4)

    def test_invalid_wildcard_patterns(self):
        # '#' must only be the final segment
        with self.assertRaises(ValueError):
            self.trie.insert("sport/#/player", {"id": 1})

        with self.assertRaises(ValueError):
            self.trie.insert("#/sport", {"id": 1})

        # '+' and '#' must occupy entire segments
        with self.assertRaises(ValueError):
            self.trie.insert("sport/ten+nis", {"id": 1})

        with self.assertRaises(ValueError):
            self.trie.insert("sport/tennis#", {"id": 1})

    def test_leading_and_trailing_slashes(self):
        self.trie.insert("/leading/topic", {"id": "leading"})
        self.trie.insert("trailing/topic/", {"id": "trailing"})

        self.assertEqual(self.trie.match("/leading/topic"), [{"id": "leading"}])
        self.assertEqual(self.trie.match("trailing/topic/"), [{"id": "trailing"}])
        self.assertEqual(self.trie.match("leading/topic"), [])

    def test_trie_clear_and_length(self):
        self.assertEqual(len(self.trie), 0)
        self.trie.insert("a/b", 1)
        self.trie.insert("a/c", 2)
        self.trie.insert("a/#", 3)
        self.assertEqual(len(self.trie), 3)

        self.trie.clear()
        self.assertEqual(len(self.trie), 0)
        self.assertEqual(self.trie.match("a/b"), [])

if __name__ == '__main__':
    unittest.main()
