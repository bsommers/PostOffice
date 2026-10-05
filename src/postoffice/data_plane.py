import json
import redis
import logging
import threading
from postoffice.app import PostOffice

logger = logging.getLogger(__name__)

class DataPlane:
    """
    Worker class that executes routing instructions.
    Downloads state from Redis and listens for live updates.
    Many DataPlanes can run concurrently to scale out horizontally.
    """
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379):
        self.r = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
        self.po = PostOffice()
        self.pubsub = self.r.pubsub()
        self.channel = "postoffice:config_updates"
        self._stop_event = threading.Event()

    def sync_state(self):
        """Fetches the latest state from Redis to catch up."""
        logger.info("Syncing initial state from Control Plane...")

        # 1. Sync Brokers
        brokers = self.r.hgetall("postoffice:brokers")
        for name, config_str in brokers.items():
            config = json.loads(config_str)
            self.po.add_broker(name, config["protocol"], **config["kwargs"])

        # 2. Sync Routes
        routes = self.r.hgetall("postoffice:routes")
        for _, config_str in routes.items():
            config = json.loads(config_str)
            self.po.add_route(
                config["source_broker"], config["source_topic"],
                config["target_broker"], config["target_topic"],
                **config["target_kwargs"]
            )

        # 3. Sync Subscriptions
        subs = self.r.hgetall("postoffice:subscriptions")
        for _, config_str in subs.items():
            config = json.loads(config_str)
            self.po.subscribe(config["broker_name"], config["topic"], **config["kwargs"])

    def _listen_for_updates(self):
        self.pubsub.subscribe(self.channel)
        logger.info(f"Subscribed to {self.channel} for live config updates.")

        for message in self.pubsub.listen():
            if self._stop_event.is_set():
                break

            if message['type'] == 'message':
                try:
                    payload = json.loads(message['data'])
                    action = payload.get("action")
                    data = payload.get("data", {})

                    if action == "add_broker":
                        self.po.add_broker(data["name"], data["protocol"], **data["kwargs"])
                    elif action == "add_route":
                        self.po.add_route(
                            data["source_broker"], data["source_topic"],
                            data["target_broker"], data["target_topic"],
                            **data["target_kwargs"]
                        )
                    elif action == "add_subscription":
                        self.po.subscribe(data["broker_name"], data["topic"], **data["kwargs"])
                    elif action == "clear_state":
                        logger.warning("Control plane issued clear_state. A restart is recommended.")

                except Exception as e:
                    logger.error(f"Error processing control plane update: {e}")

    def run(self):
        """Starts the worker, syncing state and listening for changes indefinitely."""
        self.sync_state()
        self.po.start()

        listener_thread = threading.Thread(target=self._listen_for_updates)
        listener_thread.daemon = True
        listener_thread.start()

        try:
            listener_thread.join()
        except KeyboardInterrupt:
            logger.info("Shutting down worker...")
        finally:
            self._stop_event.set()
            self.po.stop()
