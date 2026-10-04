import unittest
from postoffice.router import Router

class TestWildcardRouting(unittest.TestCase):
    def test_topic_match(self):
        router = Router()

        # Exact match
        self.assertTrue(router._topic_match("sensor/data", "sensor/data"))
        self.assertFalse(router._topic_match("sensor/data", "sensor/other"))

        # Single level wildcard (+)
        self.assertTrue(router._topic_match("sensor/+/data", "sensor/123/data"))
        self.assertFalse(router._topic_match("sensor/+/data", "sensor/123/other"))
        self.assertFalse(router._topic_match("sensor/+/data", "sensor/123/data/extra"))

        # Multi-level wildcard (#)
        self.assertTrue(router._topic_match("sensor/#", "sensor/123/data"))
        self.assertTrue(router._topic_match("sensor/123/#", "sensor/123/data/extra"))
        self.assertFalse(router._topic_match("sensor/123/#", "sensor/456/data"))

if __name__ == '__main__':
    unittest.main()