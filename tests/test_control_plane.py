import unittest
import json
from unittest.mock import patch
from postoffice.control_plane import ControlPlane

class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.published_messages = []

    def hset(self, name, key, value):
        if name not in self.hashes:
            self.hashes[name] = {}
        self.hashes[name][key] = value
        return 1

    def hgetall(self, name):
        return self.hashes.get(name, {})

    def delete(self, *names):
        deleted_count = 0
        for name in names:
            if name in self.hashes:
                del self.hashes[name]
                deleted_count += 1
        return deleted_count

    def publish(self, channel, message):
        self.published_messages.append((channel, message))
        return 1

class TestControlPlane(unittest.TestCase):
    def setUp(self):
        self.fake_redis = FakeRedis()
        self.patcher = patch('postoffice.control_plane.redis.Redis', return_value=self.fake_redis)
        self.patcher.start()
        self.cp = ControlPlane(redis_host='mock_host', redis_port=6379)

    def tearDown(self):
        self.patcher.stop()

    def test_register_broker(self):
        self.cp.register_broker("mqtt_1", "mqtt", host="localhost", port=1883)

        # Check hash persistence
        brokers = self.fake_redis.hgetall("postoffice:brokers")
        self.assertIn("mqtt_1", brokers)
        broker_data = json.loads(brokers["mqtt_1"])
        self.assertEqual(broker_data["protocol"], "mqtt")
        self.assertEqual(broker_data["kwargs"]["host"], "localhost")
        self.assertEqual(broker_data["kwargs"]["port"], 1883)

        # Check PubSub message
        self.assertEqual(len(self.fake_redis.published_messages), 1)
        channel, payload_str = self.fake_redis.published_messages[0]
        self.assertEqual(channel, "postoffice:config_updates")
        payload = json.loads(payload_str)
        self.assertEqual(payload["action"], "add_broker")
        self.assertEqual(payload["data"]["name"], "mqtt_1")
        self.assertEqual(payload["data"]["protocol"], "mqtt")
        self.assertEqual(payload["data"]["kwargs"]["port"], 1883)

    def test_add_route(self):
        self.cp.add_route(
            source_broker="mqtt_1",
            source_topic="sensor/temperature",
            target_broker="kafka_1",
            target_topic="telemetry",
            key=b"device_1".decode("latin-1"),
            partition=0
        )

        routes = self.fake_redis.hgetall("postoffice:routes")
        route_id = "mqtt_1:sensor/temperature->kafka_1:telemetry"
        self.assertIn(route_id, routes)
        route_data = json.loads(routes[route_id])
        self.assertEqual(route_data["source_broker"], "mqtt_1")
        self.assertEqual(route_data["target_broker"], "kafka_1")
        self.assertEqual(route_data["target_kwargs"]["partition"], 0)

        # Check PubSub message
        self.assertEqual(len(self.fake_redis.published_messages), 1)
        channel, payload_str = self.fake_redis.published_messages[0]
        self.assertEqual(channel, "postoffice:config_updates")
        payload = json.loads(payload_str)
        self.assertEqual(payload["action"], "add_route")
        self.assertEqual(payload["data"]["source_topic"], "sensor/temperature")

    def test_add_subscription(self):
        self.cp.add_subscription("mqtt_1", "sensor/+", qos=1)

        subscriptions = self.fake_redis.hgetall("postoffice:subscriptions")
        sub_id = "mqtt_1:sensor/+"
        self.assertIn(sub_id, subscriptions)
        sub_data = json.loads(subscriptions[sub_id])
        self.assertEqual(sub_data["broker_name"], "mqtt_1")
        self.assertEqual(sub_data["topic"], "sensor/+")
        self.assertEqual(sub_data["kwargs"]["qos"], 1)

        # Check PubSub message
        self.assertEqual(len(self.fake_redis.published_messages), 1)
        channel, payload_str = self.fake_redis.published_messages[0]
        self.assertEqual(channel, "postoffice:config_updates")
        payload = json.loads(payload_str)
        self.assertEqual(payload["action"], "add_subscription")
        self.assertEqual(payload["data"]["broker_name"], "mqtt_1")

    def test_clear_state(self):
        # Prepopulate fake Redis
        self.fake_redis.hset("postoffice:brokers", "b1", "{}")
        self.fake_redis.hset("postoffice:routes", "r1", "{}")
        self.fake_redis.hset("postoffice:subscriptions", "s1", "{}")

        self.cp.clear_state()

        self.assertEqual(len(self.fake_redis.hgetall("postoffice:brokers")), 0)
        self.assertEqual(len(self.fake_redis.hgetall("postoffice:routes")), 0)
        self.assertEqual(len(self.fake_redis.hgetall("postoffice:subscriptions")), 0)

        # Check PubSub message
        self.assertEqual(len(self.fake_redis.published_messages), 1)
        channel, payload_str = self.fake_redis.published_messages[0]
        self.assertEqual(channel, "postoffice:config_updates")
        payload = json.loads(payload_str)
        self.assertEqual(payload["action"], "clear_state")

if __name__ == '__main__':
    unittest.main()
