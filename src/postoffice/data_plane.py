import json
import redis
import logging
from typing import Any, Dict, Optional, Set, Tuple
import threading
from postoffice.app import PostOffice

logger = logging.getLogger(__name__)

class DataPlane:
    """
    Worker class that executes routing instructions.
    Downloads state from Redis and listens for live updates.
    Many DataPlanes can run concurrently to scale out horizontally.

    Convergence: the worker subscribes *before* reading the snapshot so nothing published while it
    reads is lost, applies everything idempotently (brokers keyed by name, identical routes and
    subscriptions are no-ops), tracks the Control Plane's monotonically increasing version, and
    re-reads the snapshot after a listener reconnect or a version gap.
    """
    def __init__(self, redis_host: str = 'localhost', redis_port: int = 6379,
                 redis_password: Optional[str] = None, redis_tls: bool = False):
        self.r = redis.Redis(
            host=redis_host, port=redis_port, password=redis_password, ssl=redis_tls,
            decode_responses=True,
        )
        self.po = PostOffice()
        self.pubsub = self.r.pubsub()
        self.channel = "postoffice:config_updates"
        self._stop_event = threading.Event()
        self._init_convergence_state()

    version_key = "postoffice:version"

    def _init_convergence_state(self) -> None:
        self._version: Optional[int] = None       # last Control Plane version reflected locally
        self._needs_resync = False
        self._pubsub_ready = False                # run() already subscribed this pubsub
        self._synced_once = False
        self._brokers_applied: Dict[str, str] = {}
        self._subs_applied: Set[Tuple[str, str, str]] = set()

    # -- idempotent appliers -------------------------------------------------------------------

    def _apply_broker(self, name: str, config: Dict[str, Any]) -> None:
        normalized = json.dumps({"protocol": config["protocol"], "kwargs": config["kwargs"]}, sort_keys=True)
        if self._brokers_applied.get(name) == normalized:
            return
        if name in self.po.brokers:
            # Same name, different (or unrecorded) configuration: the newest definition wins.
            logger.info(f"Replacing broker '{name}' (configuration changed)")
            self.po.remove_broker(name)
            self._subs_applied = {s for s in self._subs_applied if s[0] != name}
        self.po.add_broker(name, config["protocol"], **config["kwargs"])
        self._brokers_applied[name] = normalized

    def _apply_subscription(self, broker: str, topic: str, kwargs: Dict[str, Any]) -> None:
        key = (broker, topic, json.dumps(kwargs, sort_keys=True))
        if key in self._subs_applied:
            return
        self.po.subscribe(broker, topic, **kwargs)
        self._subs_applied.add(key)

    def _record_security(self, name: str, security_config: Dict[str, Any]) -> None:
        """Keeps the applied-config record in step with a hot security reload (no needless replace)."""
        applied = self._brokers_applied.get(name)
        if applied is not None:
            config = json.loads(applied)
            config["kwargs"]["security_config"] = security_config
            self._brokers_applied[name] = json.dumps(config, sort_keys=True)

    # -- snapshot ------------------------------------------------------------------------------

    def _read_version(self) -> int:
        raw = self.r.get(self.version_key)
        return int(raw) if raw is not None else 0

    def _subscribe(self) -> None:
        """Opens the Pub/Sub subscription; call before sync_state so mid-snapshot events queue up."""
        self.pubsub = self.r.pubsub()
        self.pubsub.subscribe(self.channel)
        self._pubsub_ready = True

    def sync_state(self, max_retries: int = 5, retry_delay: float = 1.0):
        """Fetches the latest state from Redis to catch up. Safe to repeat: applying is idempotent."""
        logger.info("Syncing initial state from Control Plane...")
        attempt = 0
        while not self._stop_event.is_set():
            try:
                # Read the version first: events after it are replayed (idempotently) by the listener.
                snapshot_version = self._read_version()

                # 1. Sync Brokers
                brokers = self.r.hgetall("postoffice:brokers")
                for name, config_str in brokers.items():
                    self._apply_broker(name, json.loads(config_str))

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
                    self._apply_subscription(config["broker_name"], config["topic"], config["kwargs"])
                self._version = snapshot_version
                self._synced_once = True
                break
            except (redis.ConnectionError, redis.TimeoutError) as e:
                attempt += 1
                if attempt >= max_retries or self._stop_event.is_set():
                    logger.error(f"Failed to sync state from Redis after {attempt} attempts: {e}")
                    raise
                logger.warning(f"Failed to sync state ({e}). Retrying in {retry_delay}s (attempt {attempt}/{max_retries})...")
                self._stop_event.wait(retry_delay)

    def resync(self) -> None:
        """Re-reads the full snapshot (after a reconnect, a version gap or an unknown-entity update)."""
        self._needs_resync = False
        logger.info("Resyncing state from Control Plane...")
        self.sync_state()

    # -- live updates --------------------------------------------------------------------------

    def _accept_version(self, version: Optional[int]) -> bool:
        """Applies Control Plane ordering: skip stale events, resync on a gap. Unversioned events apply."""
        if version is None or self._version is None:
            return True
        if version <= self._version:
            logger.debug(f"Ignoring already-applied update v{version} (at v{self._version})")
            return False
        if version > self._version + 1:
            logger.warning(f"Missed control plane updates (have v{self._version}, got v{version}); resyncing")
            self._needs_resync = True
            return False
        self._version = version
        return True

    def _process_update_message(self, message):
        """Processes an individual Pub/Sub configuration update message."""
        if message and message.get('type') == 'message':
            try:
                payload = json.loads(message['data'])
                action = payload.get("action")
                data = payload.get("data", {})
                if not self._accept_version(payload.get("version")):
                    return

                if action == "add_broker":
                    self._apply_broker(data["name"], data)
                elif action == "add_route":
                    self.po.add_route(
                        data["source_broker"], data["source_topic"],
                        data["target_broker"], data["target_topic"],
                        **data["target_kwargs"]
                    )
                elif action == "add_subscription":
                    self._apply_subscription(data["broker_name"], data["topic"], data["kwargs"])
                elif action == "update_broker_security":
                    name = data["name"]
                    if name not in self.po.brokers:
                        # We missed the add_broker: the snapshot already carries the new security config.
                        logger.error(f"update_broker_security for unknown broker '{name}'; resyncing")
                        self.po.router.metrics.record_error("data_plane", "security_update_unknown_broker")
                        self._needs_resync = True
                        return
                    self.po.reload_broker_security(name, data["security_config"])
                    self._record_security(name, data["security_config"])
                elif action == "clear_state":
                    logger.warning("Control plane issued clear_state. Tearing down existing brokers and routes...")
                    was_running = self.po.is_running
                    self.po.reset()
                    self._brokers_applied.clear()
                    self._subs_applied.clear()
                    if was_running:
                        # reset() leaves the facade stopped; restart so brokers added next connect.
                        self.po.start()

            except Exception as e:
                logger.error(f"Error processing control plane update: {e}")

    def _listen_for_updates(self):
        """Listens for live updates; every reconnect re-subscribes first, then resyncs the snapshot."""
        delay = 1.0
        max_delay = 30.0

        while not self._stop_event.is_set():
            try:
                if self._pubsub_ready:
                    self._pubsub_ready = False  # run() subscribed and synced already
                else:
                    self._subscribe()
                    self._pubsub_ready = False
                    if self._synced_once:
                        self.resync()  # events may have been missed while disconnected
                logger.info(f"Subscribed to {self.channel} for live config updates.")
                delay = 1.0  # Reset delay on successful connection

                for message in self.pubsub.listen():
                    if self._stop_event.is_set():
                        break
                    self._process_update_message(message)
                    if self._needs_resync:
                        self.resync()
            except (redis.ConnectionError, redis.TimeoutError) as e:
                if self._stop_event.is_set():
                    break
                logger.warning(f"Redis connection error in update listener: {e}. Retrying in {delay}s...")
                self._stop_event.wait(delay)
                delay = min(delay * 2, max_delay)
            except Exception as e:
                if self._stop_event.is_set():
                    break
                logger.error(f"Unexpected error in update listener: {e}. Retrying in {delay}s...")
                self._stop_event.wait(delay)
                delay = min(delay * 2, max_delay)

    def run(self):
        """Starts the worker, syncing state and listening for changes indefinitely."""
        self._subscribe()  # before the snapshot, so nothing published during it is lost
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
