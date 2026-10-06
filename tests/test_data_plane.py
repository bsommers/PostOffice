import unittest
import json
import threading
from unittest.mock import patch, MagicMock
import redis
from postoffice.data_plane import DataPlane

class FakePubSub:
    def __init__(self, messages=None):
        self.messages = list(messages or [])
        self.subscribed_channels = []

    def subscribe(self, channel):
        self.subscribed_channels.append(channel)

    def listen(self):
        for msg in self.messages:
            yield msg

class FakeRedis:
    def __init__(self, pubsub_messages=None):
        self.hashes = {}
        self.published_messages = []
        self._pubsub_messages = pubsub_messages or []

    def hset(self, name, key, value):
        if name not in self.hashes:
            self.hashes[name] = {}
        self.hashes[name][key] = value

    def hgetall(self, name):
        return self.hashes.get(name, {})

    def pubsub(self):
        return FakePubSub(self._pubsub_messages)

class TestDataPlane(unittest.TestCase):
    @patch('postoffice.plugins.mqtt_client.mqtt.Client')
    @patch('postoffice.plugins.amqp_client.pika.BlockingConnection')
    @patch('postoffice.plugins.kafka_client.Consumer')
    @patch('postoffice.plugins.kafka_client.Producer')
    def setUp(self, MockProducer, MockConsumer, MockPika, MockMqtt):
        self.fake_redis = FakeRedis()
        self.patcher = patch('postoffice.data_plane.redis.Redis', return_value=self.fake_redis)
        self.patcher.start()
        self.dp = DataPlane(redis_host='mock_host', redis_port=6379)

    def tearDown(self):
        self.dp._stop_event.set()
        self.dp.po.stop()
        self.patcher.stop()

    def test_sync_state(self):
        # 1. Populate fake Redis state
        self.fake_redis.hset("postoffice:brokers", "mqtt_1", json.dumps({"protocol": "mqtt", "kwargs": {"host": "localhost", "port": 1883}}))
        self.fake_redis.hset("postoffice:brokers", "kafka_1", json.dumps({"protocol": "kafka", "kwargs": {"bootstrap_servers": "localhost:9092"}}))
        self.fake_redis.hset("postoffice:routes", "mqtt_1:sensor->kafka_1:telemetry", json.dumps({
            "source_broker": "mqtt_1",
            "source_topic": "sensor",
            "target_broker": "kafka_1",
            "target_topic": "telemetry",
            "target_kwargs": {"partition": 1}
        }))
        self.fake_redis.hset("postoffice:subscriptions", "mqtt_1:sensor", json.dumps({
            "broker_name": "mqtt_1",
            "topic": "sensor",
            "kwargs": {"qos": 1}
        }))

        # 2. Run sync_state
        self.dp.sync_state()

        # 3. Assertions
        self.assertIn("mqtt_1", self.dp.po.brokers)
        self.assertIn("kafka_1", self.dp.po.brokers)
        self.assertIn(("mqtt_1", "sensor"), self.dp.po.router.routes)
        self.assertEqual(len(self.dp.po.brokers["mqtt_1"].subscribed_topics), 1)
        self.assertEqual(self.dp.po.brokers["mqtt_1"].subscribed_topics[0], "sensor")

    def test_process_update_message_add_broker_and_route(self):
        msg_broker = {
            'type': 'message',
            'data': json.dumps({
                'action': 'add_broker',
                'data': {'name': 'mqtt_edge', 'protocol': 'mqtt', 'kwargs': {'host': 'edge', 'port': 1883}}
            })
        }
        self.dp._process_update_message(msg_broker)
        self.assertIn("mqtt_edge", self.dp.po.brokers)

        msg_sub = {
            'type': 'message',
            'data': json.dumps({
                'action': 'add_subscription',
                'data': {'broker_name': 'mqtt_edge', 'topic': 'devices/+', 'kwargs': {'qos': 1}}
            })
        }
        self.dp._process_update_message(msg_sub)
        self.assertIn("devices/+", self.dp.po.brokers["mqtt_edge"].subscribed_topics)

        msg_route = {
            'type': 'message',
            'data': json.dumps({
                'action': 'add_route',
                'data': {
                    'source_broker': 'mqtt_edge',
                    'source_topic': 'devices/+',
                    'target_broker': 'mqtt_edge',
                    'target_topic': 'internal/devices',
                    'target_kwargs': {}
                }
            })
        }
        self.dp._process_update_message(msg_route)
        self.assertIn(("mqtt_edge", "devices/+"), self.dp.po.router.routes)

    def test_process_update_message_clear_state(self):
        # Add broker and route
        self.dp.po.add_broker("mqtt_temp", "mqtt", host="localhost", port=1883)
        self.dp.po.add_route("mqtt_temp", "t1", "mqtt_temp", "t2")
        self.assertEqual(len(self.dp.po.brokers), 1)
        self.assertEqual(len(self.dp.po.router.routes), 1)

        # Send clear_state message
        msg_clear = {
            'type': 'message',
            'data': json.dumps({'action': 'clear_state', 'data': {}})
        }
        self.dp._process_update_message(msg_clear)

        # Assert full teardown
        self.assertEqual(len(self.dp.po.brokers), 0)
        self.assertEqual(len(self.dp.po.router.routes), 0)
        self.assertEqual(len(self.dp.po.router.clients), 0)
        self.assertFalse(self.dp.po.is_running)

    def test_process_update_message_malformed(self):
        # Malformed JSON should not crash
        bad_msg = {'type': 'message', 'data': 'not valid json'}
        self.dp._process_update_message(bad_msg)

    def test_listen_for_updates_reconnect_loop(self):
        # Create a mock Redis that fails on first pubsub listen attempt, then succeeds
        mock_pubsub = MagicMock()
        first_call = True

        def mock_listen():
            nonlocal first_call
            if first_call:
                first_call = False
                raise redis.exceptions.ConnectionError("Simulated Redis disconnect")
            else:
                self.dp._stop_event.set()
                return []

        mock_pubsub.listen.side_effect = mock_listen
        self.dp.r.pubsub = MagicMock(return_value=mock_pubsub)

        # Intercept wait on _stop_event to prevent actual sleep delay
        original_wait = self.dp._stop_event.wait
        self.dp._stop_event.wait = MagicMock(side_effect=lambda timeout=None: original_wait(0.01))

        # Run listener
        self.dp._listen_for_updates()

        self.assertTrue(mock_pubsub.subscribe.called)
        self.assertGreaterEqual(mock_pubsub.listen.call_count, 1)

if __name__ == '__main__':
    unittest.main()
