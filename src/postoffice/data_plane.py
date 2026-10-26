import json
import os
import redis
from redis.backoff import NoBackoff
from redis.retry import Retry
import logging
import time
from collections import deque
from typing import Any, Dict, Optional, Set, Tuple
import threading
from postoffice.app import PostOffice
from postoffice.signing import HmacSigner, as_envelope, verify_event, verify_state, verify_version

logger = logging.getLogger(__name__)

SIGNING_MODES = ("off", "observe", "enforce")
LOG_INTERVAL = 10.0  # seconds between warnings of the same kind: rejections must not become a log flood

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
                 redis_password: Optional[str] = None, redis_tls: bool = False,
                 signer: Optional[HmacSigner] = None, signing_mode: Optional[str] = None):
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
        # Explicit signer wins; otherwise POSTOFFICE_EVENT_KEYS. A key means enforce unless told otherwise.
        self.signer = signer if signer is not None else HmacSigner.from_env()
        mode = signing_mode or os.environ.get("POSTOFFICE_EVENT_SIGNING_MODE") or (
            "enforce" if self.signer is not None else "off")
        if mode not in SIGNING_MODES:
            raise ValueError(f"signing_mode must be one of {SIGNING_MODES}")
        if mode != "off" and self.signer is None:
            raise ValueError(f"signing_mode '{mode}' requires keys (POSTOFFICE_EVENT_KEYS)")
        self.signing_mode = mode
        if self.signer is None:
            logger.warning(
                "Control-plane events are not authenticated (set POSTOFFICE_EVENT_KEYS to enable signing)")

    signer: Optional[HmacSigner] = None   # class defaults: planes built without __init__ stay unsigned
    signing_mode = "off"
    version_key = "postoffice:version"
    version_sigs_key = "postoffice:version_sigs"

    def _init_convergence_state(self) -> None:
        self._last_logged: Dict[str, float] = {}
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

    # -- authentication ------------------------------------------------------------------------

    def _control_event(self, result: str, what: str = "") -> None:
        """Counts an outcome; logs rejections (never payloads) at most once per LOG_INTERVAL per kind."""
        self.po.router.metrics.record_control_event(result)
        if result in ("applied", "stale", "gap"):
            return
        now = time.monotonic()
        if now - self._last_logged.get(result, -LOG_INTERVAL) >= LOG_INTERVAL:
            self._last_logged[result] = now
            logger.warning(f"Control-plane message {result}{' (' + what + ')' if what else ''} "
                           f"[mode={self.signing_mode}]")

    def _open_signed(self, raw: str, kind: str, hash_name: str = "", field: str = "") -> Optional[Dict[str, Any]]:
        """
        Returns the decoded message if it may be applied, else None (the rejection is counted).
        ``off`` accepts signed and unsigned alike (rolling upgrades); ``observe`` counts failures but
        still applies; ``enforce`` drops them.
        """
        obj = json.loads(raw)
        env = as_envelope(obj)
        what = f"{hash_name}/{field}" if hash_name else "event"
        if env is None:
            if self.signing_mode == "off":
                return obj
            self._control_event("unsigned", what)
            return obj if self.signing_mode == "observe" else None
        if self.signing_mode == "off":
            return json.loads(env["payload"])
        valid = verify_state(self.signer, hash_name, field, env) if kind == "state" else verify_event(self.signer, env)
        if valid:
            return json.loads(env["payload"])
        self._control_event("bad_signature", what)
        return json.loads(env["payload"]) if self.signing_mode == "observe" else None

    def _load_hash(self, hash_name: str) -> Dict[str, Dict[str, Any]]:
        """Reads a state hash, dropping entries that fail verification or cannot be parsed."""
        entries: Dict[str, Dict[str, Any]] = {}
        for field, raw in self.r.hgetall(hash_name).items():
            try:
                config = self._open_signed(raw, "state", hash_name, field)
            except (ValueError, KeyError) as e:
                logger.error(f"Unreadable state entry {hash_name}/{field}: {type(e).__name__}")
                self._control_event("invalid")
                continue
            if config is not None:
                entries[field] = config
        return entries

    # -- snapshot ------------------------------------------------------------------------------

    def _read_version(self) -> Optional[int]:
        """Current Control Plane version; None when signing is on and the counter cannot be trusted."""
        raw = self.r.get(self.version_key)
        version = int(raw) if raw is not None else 0
        if version == 0 or self.signing_mode == "off":
            return version
        entry = None
        for _ in range(3):  # the writer INCRs before it stores the signature: allow a brief gap
            entry = self.r.hget(self.version_sigs_key, str(version))
            if entry is not None:
                break
            self._stop_event.wait(0.05)
        if entry is not None and verify_version(self.signer, version, entry):
            return version
        self._control_event("bad_version")
        return version if self.signing_mode == "observe" else None

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

                # One bad entry (e.g. a rejected broker that a route refers to) must not block the rest.
                for name, config in self._load_hash("postoffice:brokers").items():
                    self._apply_entry("broker", name, lambda: self._apply_broker(name, config))
                for field, config in self._load_hash("postoffice:routes").items():
                    self._apply_entry("route", field, lambda: self.po.add_route(
                        config["source_broker"], config["source_topic"],
                        config["target_broker"], config["target_topic"],
                        **config["target_kwargs"]))
                for field, config in self._load_hash("postoffice:subscriptions").items():
                    self._apply_entry("subscription", field, lambda: self._apply_subscription(
                        config["broker_name"], config["topic"], config["kwargs"]))
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

    @staticmethod
    def _apply_entry(kind: str, name: str, apply) -> None:
        try:
            apply()
        except (redis.ConnectionError, redis.TimeoutError):
            raise
        except Exception as e:
            logger.error(f"Skipping {kind} '{name}' during sync: {e}")

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
            self._control_event("stale")
            return False
        if version > self._version + 1:
            logger.warning(f"Missed control plane updates (have v{self._version}, got v{version}); resyncing")
            self._control_event("gap")
            self._needs_resync = True
            return False
        self._version = version
        return True

    def _process_update_message(self, message):
        """Processes an individual Pub/Sub configuration update message."""
        if message and message.get('type') == 'message':
            try:
                payload = self._open_signed(message['data'], "event")
                if payload is None:
                    return
                action = payload.get("action")
                data = payload.get("data", {})
                if not self._accept_version(payload.get("version")):
                    return
                self._control_event("applied")

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
