import json
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
import logging
import time
from collections import deque
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
        # No client-side retry: redis-py would otherwise reconnect and re-subscribe a dropped Pub/Sub
        # connection silently, hiding the gap. We want the error so we re-subscribe (confirmed) and
        # resync; snapshot reads have their own retry loop in sync_state.
        self.r = redis.Redis(
            host=redis_host, port=redis_port, password=redis_password, ssl=redis_tls,
            decode_responses=True, retry=Retry(NoBackoff(), 0), retry_on_error=[],
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
        self.max_buffered = 10000                 # events held while the snapshot is read
        self._buffer: deque = deque()
        self._buffer_overflow = False
        self._buffering = threading.Event()
        self._buffer_thread: Optional[threading.Thread] = None
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

    def _await_subscribe_ack(self, timeout: float) -> None:
        """Blocks until the server confirms the SUBSCRIBE; only then do events published after it reach us."""
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise redis.TimeoutError(f"No subscribe confirmation for {self.channel} within {timeout}s")
            message = self.pubsub.get_message(timeout=min(remaining, 0.1))
            if message is not None and message.get("type") == "subscribe":
                return

    def _subscribe_and_buffer(self, ack_timeout: float = 5.0) -> None:
        """
        Opens the Pub/Sub subscription, waits for the server to confirm it, then keeps reading it
        into a bounded in-memory buffer. Call before the snapshot: anything published from this
        point on is either in the snapshot or in the buffer. Reading continuously also keeps
        Redis from disconnecting us for exceeding its subscriber output-buffer limit.
        """
        self.pubsub = self.r.pubsub()
        self.pubsub.subscribe(self.channel)
        self._await_subscribe_ack(ack_timeout)
        self._buffer = deque()
        self._buffer_overflow = False
        self._buffering.set()
        self._buffer_thread = threading.Thread(target=self._buffer_reader, daemon=True)
        self._buffer_thread.start()
        self._pubsub_ready = True

    def _buffer_message(self, message) -> None:
        if message is None or message.get("type") != "message":
            return
        if len(self._buffer) >= self.max_buffered:
            self._buffer_overflow = True  # shed; the caller resyncs from the snapshot instead
            return
        self._buffer.append(message)

    def _buffer_reader(self) -> None:
        while self._buffering.is_set():
            try:
                message = self.pubsub.get_message(timeout=0.05)
            except Exception as e:
                logger.warning(f"Event buffer reader stopped: {e}")
                self._buffer_overflow = True  # unknown what was missed: force a resync
                return
            if message is None:
                time.sleep(0.001)
            else:
                self._buffer_message(message)

    def _stop_buffering(self) -> None:
        self._buffering.clear()
        thread, self._buffer_thread = self._buffer_thread, None
        if thread is not None:
            thread.join(timeout=2.0)
        try:
            # Sweep what already reached the socket but the reader had not picked up. Bounded: a
            # stream arriving as fast as we read must not keep us here forever; treat it as overflow.
            for _ in range(self.max_buffered + 1):
                message = self.pubsub.get_message(timeout=0)
                if message is None:
                    break
                self._buffer_message(message)
            else:
                self._buffer_overflow = True
        except Exception as e:
            logger.warning(f"Error draining event buffer: {e}")
            self._buffer_overflow = True

    def _drain_buffer(self) -> None:
        """Applies the events buffered during the snapshot, in order (or flags a resync if any were shed)."""
        self._stop_buffering()
        if self._buffer_overflow:
            logger.warning("Event buffer overflowed while syncing; falling back to a full resync")
            self._buffer.clear()
            self._needs_resync = True
            return
        while self._buffer:
            self._process_update_message(self._buffer.popleft())

    def _converge(self, max_retries: int = 5) -> None:
        """subscribe -> snapshot -> buffered events. The order is what prevents lost updates."""
        self._subscribe_and_buffer()
        try:
            self.sync_state(max_retries=max_retries)
        except BaseException:
            self._stop_buffering()
            raise
        self._drain_buffer()
        if self._needs_resync:
            self.resync()

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
                elif self._synced_once:
                    self._converge()  # events may have been missed while disconnected
                    self._pubsub_ready = False
                else:
                    self._subscribe_and_buffer()
                    self._drain_buffer()
                    self._pubsub_ready = False
                if self._needs_resync:
                    self.resync()
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
        self._converge()  # subscribe (confirmed) before the snapshot, so nothing published during it is lost
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
