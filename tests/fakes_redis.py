"""Shared in-memory Redis stand-ins for the Control/Data Plane tests."""
import json
from collections import deque

import redis

BROKERS, ROUTES, SUBS, VERSION = (
    "postoffice:brokers", "postoffice:routes", "postoffice:subscriptions", "postoffice:version")


class FakePubSub:
    def __init__(self, bus, ack_delay=0):
        self.bus = bus
        self.queue = deque()
        self.fail_listen = None
        self.ack_delay = ack_delay  # get_message() polls that return nothing before the server confirms
        self._acked = False

    def subscribe(self, channel):
        self.bus.calls.append("subscribe")
        self.bus.pubsubs.append(self)

    def get_message(self, timeout=0, ignore_subscribe_messages=False):
        if not self._acked:
            if self.ack_delay > 0:
                self.ack_delay -= 1
                return None
            self._acked = True
            self.bus.calls.append("ack")
            return {"type": "subscribe"}
        if self.queue:
            return {"type": "message", "data": self.queue.popleft()}
        return None

    def listen(self):
        if self.fail_listen:
            raise self.fail_listen
        while self.queue:
            yield {"type": "message", "data": self.queue.popleft()}
        self.bus.on_listen_exhausted()


class FakeBus:
    """In-memory Redis stand-in whose publish() reaches every subscribed pubsub."""

    def __init__(self):
        self.hashes, self.kv, self.pubsubs, self.calls = {}, {}, [], []
        self.on_hgetall = None
        self.on_listen_exhausted = lambda: None
        self.fail_hgetall_once = {}
        self.ack_delay = 0

    def hset(self, name, key, value):
        self.hashes.setdefault(name, {})[key] = value

    def hgetall(self, name):
        self.calls.append(f"hgetall:{name}")
        if self.fail_hgetall_once.pop(name, False):
            raise redis.ConnectionError("blip")
        result = dict(self.hashes.get(name, {}))
        if self.on_hgetall:
            self.on_hgetall(name)
        return result

    def hget(self, name, field):
        return self.hashes.get(name, {}).get(field)

    def hdel(self, name, *fields):
        for f in fields:
            self.hashes.get(name, {}).pop(f, None)

    def get(self, key):
        self.calls.append(f"get:{key}")
        return self.kv.get(key)

    def incr(self, key):
        self.kv[key] = int(self.kv.get(key, 0)) + 1
        return self.kv[key]

    def publish(self, channel, payload):
        for ps in self.pubsubs:
            ps.queue.append(payload)

    def pubsub(self):
        return FakePubSub(self, ack_delay=self.ack_delay)


def event(action, data, version=None):
    body = {"action": action, "data": data}
    if version is not None:
        body["version"] = version
    return json.dumps(body)


def broker_cfg(port=1883):
    return json.dumps({"protocol": "mqtt", "kwargs": {"host": "h", "port": port}})


def route_cfg(src="b1", st="in", dst="b2", dt="out"):
    return json.dumps({"source_broker": src, "source_topic": st, "target_broker": dst,
                       "target_topic": dt, "target_kwargs": {}})
