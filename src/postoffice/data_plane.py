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
from postoffice.ingress import Firewall
from postoffice.backoff import ExponentialBackoff
from postoffice.jsonscan import scan_json
from postoffice.ratelimit import TokenBucket
from postoffice.signing import HmacSigner, as_envelope, verify_event, verify_state, verify_version
from postoffice.validation import ACTIONS, PayloadError, validate_action

logger = logging.getLogger(__name__)

SIGNING_MODES = ("off", "observe", "enforce")
LOG_INTERVAL = 10.0  # seconds between warnings of the same kind: rejections must not become a log flood
DEFAULT_MAX_EVENT_BYTES = 256 * 1024
DEFAULT_EVENT_RATE = 20.0    # events/s a worker will process; the rest are shed and recovered by resync
DEFAULT_EVENT_BURST = 100.0
RESYNC_BACKOFF_CAP = 30.0
RESYNC_QUIET = 2 * RESYNC_BACKOFF_CAP  # no resync for this long resets the spacing
# Structural bounds for a control message, checked by a linear scan before json.loads.
MAX_MESSAGE_DEPTH = 16
MAX_MESSAGE_KEYS = 10000
MAX_MESSAGE_ARRAY = 10000


def _env_number(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError:
        raise ValueError(f"{name} must be a number") from None

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
                 signer: Optional[HmacSigner] = None, signing_mode: Optional[str] = None,
                 max_event_bytes: Optional[int] = None, event_rate: Optional[float] = None,
                 event_burst: Optional[float] = None, allowed_actions: Optional[Set[str]] = None,
                 allow_clear_state: Optional[bool] = None):
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
        self._init_limits(max_event_bytes, event_rate, event_burst, allowed_actions, allow_clear_state)
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

    def _init_limits(self, max_event_bytes: Optional[int] = None, event_rate: Optional[float] = None,
                     event_burst: Optional[float] = None, allowed_actions: Optional[Set[str]] = None,
                     allow_clear_state: Optional[bool] = None) -> None:
        """Explicit arguments win over the environment, which wins over the defaults."""
        self.max_event_bytes = int(max_event_bytes if max_event_bytes is not None else
                                   _env_number("POSTOFFICE_MAX_EVENT_BYTES", DEFAULT_MAX_EVENT_BYTES))
        rate = event_rate if event_rate is not None else _env_number("POSTOFFICE_EVENT_RATE", DEFAULT_EVENT_RATE)
        burst = event_burst if event_burst is not None else _env_number("POSTOFFICE_EVENT_BURST", DEFAULT_EVENT_BURST)
        if self.max_event_bytes <= 0:
            raise ValueError("max_event_bytes must be > 0")
        self._clock = time.monotonic
        self._event_bucket = TokenBucket(rate, burst, clock=lambda: self._clock())
        if allowed_actions is None and os.environ.get("POSTOFFICE_ALLOWED_ACTIONS", "").strip():
            allowed_actions = {a.strip() for a in os.environ["POSTOFFICE_ALLOWED_ACTIONS"].split(",") if a.strip()}
        self.allowed_actions = set(ACTIONS) if allowed_actions is None else set(allowed_actions)
        unknown = self.allowed_actions - set(ACTIONS)
        if unknown:
            raise ValueError(f"Unknown control-plane actions: {', '.join(sorted(unknown))}")
        # clear_state tears every broker down, so it needs its own opt-in on top of the allow-list.
        self.allow_clear_state = (os.environ.get("ALLOW_CLEAR_STATE") == "1"
                                  if allow_clear_state is None else allow_clear_state)

    def _init_convergence_state(self) -> None:
        self._init_limits()
        self._resync_backoff = ExponentialBackoff(base=1.0, factor=2.0, cap=RESYNC_BACKOFF_CAP, jitter="none")
        self._next_resync_at = 0.0
        self._last_resync_at = float("-inf")
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
        self._ingress_applied: Optional[str] = None
        self._ingress_inflight: Set[str] = set()

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

    def _build_ingress(self, config: Dict[str, Any]) -> Optional[Firewall]:
        """Builds the new firewall before anything is swapped; None when this exact config is already live."""
        if json.dumps(config, sort_keys=True) == self._ingress_applied:
            return None
        try:
            return Firewall.from_dict(config)
        except Exception as e:  # unknown rule type, bad rule arguments, bad ledger settings
            raise PayloadError(f"ingress config rejected ({type(e).__name__})") from None

    def _install_ingress(self, config: Dict[str, Any], firewall: Optional[Firewall]) -> None:
        """Swaps the firewall (one reference assignment) and reconciles the in-flight caps it carries."""
        if firewall is None:
            return
        self.po.configure_ingress(firewall)
        inflight = config.get("inflight", {})
        for broker in self._ingress_inflight - set(inflight):
            self.po.set_inflight_limit(broker, None)
        for broker, limit in inflight.items():
            self.po.set_inflight_limit(broker, **limit)
        self._ingress_inflight = set(inflight)
        self._ingress_applied = json.dumps(config, sort_keys=True)

    def _apply_ingress(self, config: Dict[str, Any]) -> None:
        self._install_ingress(config, self._build_ingress(config))

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
            what = what[:120].replace("\n", " ").replace("\r", " ")  # may name attacker-chosen keys
            logger.warning(f"Control-plane message {result}{' (' + what + ')' if what else ''} "
                           f"[mode={self.signing_mode}]")

    # -- size and structure --------------------------------------------------------------------

    def _too_big(self, raw) -> bool:
        if isinstance(raw, str):
            if len(raw) > self.max_event_bytes:
                return True
            if len(raw) * 4 <= self.max_event_bytes:
                return False
            return len(raw.encode("utf-8")) > self.max_event_bytes
        return len(raw) > self.max_event_bytes

    def _parse(self, text) -> Any:
        """json.loads behind a linear structural scan, so hostile nesting cannot reach the parser."""
        data = text.encode("utf-8") if isinstance(text, str) else text
        problem = scan_json(data, MAX_MESSAGE_DEPTH, MAX_MESSAGE_KEYS, MAX_MESSAGE_ARRAY, self.max_event_bytes)
        if problem:
            raise ValueError(problem)
        return json.loads(text)

    def _open_signed(self, raw: str, kind: str, hash_name: str = "", field: str = "") -> Optional[Dict[str, Any]]:
        """
        Returns the decoded message if it may be applied, else None (the rejection is counted).
        ``off`` accepts signed and unsigned alike (rolling upgrades); ``observe`` counts failures but
        still applies; ``enforce`` drops them.
        """
        obj = self._parse(raw)
        env = as_envelope(obj)
        what = f"{hash_name}/{field}" if hash_name else "event"
        if env is None:
            if self.signing_mode == "off":
                return obj
            self._control_event("unsigned", what)
            return obj if self.signing_mode == "observe" else None
        if self.signing_mode == "off":
            return self._parse(env["payload"])
        valid = verify_state(self.signer, hash_name, field, env) if kind == "state" else verify_event(self.signer, env)
        if valid:
            return self._parse(env["payload"])
        self._control_event("bad_signature", what)
        return self._parse(env["payload"]) if self.signing_mode == "observe" else None

    def _load_hash(self, hash_name: str) -> Dict[str, Dict[str, Any]]:
        """Reads a state hash, dropping entries that fail verification or cannot be parsed."""
        entries: Dict[str, Dict[str, Any]] = {}
        for field, raw in self.r.hgetall(hash_name).items():
            if self._too_big(raw):
                self._control_event("oversize", f"{hash_name}/{field}")
                continue
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
                    data = {**config, "name": name} if isinstance(config, dict) else config
                    self._apply_entry("broker", name, "add_broker", data, lambda: self._apply_broker(name, config))
                for field, config in self._load_hash("postoffice:routes").items():
                    self._apply_entry("route", field, "add_route", config, lambda: self.po.add_route(
                        config["source_broker"], config["source_topic"],
                        config["target_broker"], config["target_topic"],
                        **config["target_kwargs"]))
                for field, config in self._load_hash("postoffice:subscriptions").items():
                    self._apply_entry("subscription", field, "add_subscription", config,
                                      lambda: self._apply_subscription(
                                          config["broker_name"], config["topic"], config["kwargs"]))
                for field, config in self._load_hash("postoffice:ingress").items():
                    if field == "rules":
                        self._apply_entry("ingress", field, "set_ingress", config,
                                          lambda: self._apply_ingress(config))
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

    def _apply_entry(self, kind: str, name: str, action: str, data: Any, apply) -> None:
        try:
            validate_action(action, data)
            apply()
        except (redis.ConnectionError, redis.TimeoutError):
            raise
        except PayloadError as e:
            self._control_event("invalid", f"{kind} '{name}': {e}")
            logger.error(f"Skipping {kind} '{name[:80]}' during sync: {e}")
        except Exception as e:
            logger.error(f"Skipping {kind} '{name}' during sync: {e}")

    def resync(self) -> None:
        """Re-reads the full snapshot (after a reconnect, a version gap or an unknown-entity update)."""
        self._needs_resync = False
        now = self._clock()
        if now - self._last_resync_at > RESYNC_QUIET:
            self._resync_backoff.reset()
        self._last_resync_at = now
        self._next_resync_at = now + self._resync_backoff.next()
        logger.info("Resyncing state from Control Plane...")
        self.sync_state()

    def _resync_due(self) -> bool:
        return self._clock() >= self._next_resync_at

    def _maybe_resync(self) -> None:
        """
        Resyncs if one is wanted and the spacing allows it. A flood of bad or shed events therefore
        costs at most one snapshot read per (growing) interval. A deferred resync is retried on the
        next message; the snapshot is authoritative, so waiting never loses state, it only delays it.
        """
        if self._needs_resync and self._resync_due():
            self.resync()

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

    def _action_allowed(self, action: Any) -> bool:
        if action not in self.allowed_actions:
            return False
        return action != "clear_state" or self.allow_clear_state

    def _admit(self, raw) -> Optional[Dict[str, Any]]:
        """
        Size, rate, signature and shape checks, cheapest first. Returns the decoded event, or None
        when it was dropped (counted). Size and rate come before any parsing or HMAC work.
        """
        if self._too_big(raw):
            self._control_event("oversize")
            return None
        if not self._event_bucket.try_consume():
            # Shedding leaves a hole in the version sequence: recover it from the snapshot.
            self._control_event("rate_limited")
            self._needs_resync = True
            return None
        try:
            payload = self._open_signed(raw, "event")
        except ValueError:
            self._control_event("invalid", "undecodable event")
            return None
        if payload is None:
            return None
        if not isinstance(payload, dict):
            self._control_event("invalid", "event is not an object")
            return None
        return payload

    def _process_update_message(self, message):
        """Processes an individual Pub/Sub configuration update message."""
        if message and message.get('type') == 'message':
            try:
                payload = self._admit(message['data'])
                if payload is None:
                    return
                action = payload.get("action")
                data = payload.get("data", {})
                # Ordering first: an event we refuse below still holds its place in the sequence.
                if not self._accept_version(payload.get("version")):
                    return
                if not self._action_allowed(action):
                    self._control_event("forbidden_action", action if action in ACTIONS else "unknown")
                    return
                try:
                    validate_action(action, data)
                except PayloadError as e:
                    self._control_event("invalid", f"{action}: {e}")
                    return
                prepared = None
                if action == "set_ingress":
                    try:
                        prepared = self._build_ingress(data)
                    except PayloadError as e:
                        self._control_event("invalid", f"{action}: {e}")
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
                elif action == "set_ingress":
                    self._install_ingress(data, prepared)
                elif action == "clear_state":
                    logger.warning("Control plane issued clear_state. Tearing down existing brokers and routes...")
                    was_running = self.po.is_running
                    self.po.reset()
                    self._brokers_applied.clear()
                    self._subs_applied.clear()
                    self.po.configure_ingress(None)
                    self._ingress_applied, self._ingress_inflight = None, set()
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
                self._maybe_resync()
                logger.info(f"Subscribed to {self.channel} for live config updates.")
                delay = 1.0  # Reset delay on successful connection

                for message in self.pubsub.listen():
                    if self._stop_event.is_set():
                        break
                    self._process_update_message(message)
                    self._maybe_resync()
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
