# Phase 1: Test Suite & Distributed Plane Hardening - Research

**Researched:** 2026-10-05
**Domain:** Python Redis Testing, Fault Tolerance, Event-Driven State Resets
**Confidence:** HIGH

<user_constraints>
## User Constraints (from CONTEXT.md)

### Locked Decisions
- **D-01:** Use a custom in-memory fake Redis client with `unittest.mock.MagicMock` rather than introducing `fakeredis` as an external dependency. Preserves zero-dependency hermetic testing consistent with existing broker mocks in `tests/test_postoffice.py`.
- **D-02:** Mock Redis data structures (`hset`, `hgetall`, `delete`, `publish`) and `pubsub` listener (`listen()`, `subscribe()`) to thoroughly test sync and update processing paths.
- **D-03:** In `DataPlane`, wrap Redis initial connection and Pub/Sub listener loop in an exponential backoff reconnect loop (starting at 1s, doubling up to a 30s ceiling).
- **D-04:** Log warnings during reconnect attempts and maintain worker thread liveness until Redis connectivity is re-established.
- **D-05:** Upon receiving a `clear_state` event in `DataPlane._listen_for_updates()`, execute a full teardown: disconnect all active brokers, clear internal route dictionaries and client registries on `PostOffice`, and reset the worker state cleanly without requiring a process restart.
- **D-06:** Create two dedicated test files matching source file structure:
  - `tests/test_control_plane.py`: Verifies `register_broker`, `add_route`, `add_subscription`, and `clear_state` against mock Redis hashes and Pub/Sub.
  - `tests/test_data_plane.py`: Verifies `sync_state`, `_listen_for_updates`, dynamic event application, `clear_state` teardown, and reconnection error handling.

### Claude's Discretion
- Backoff timing calculation, mock helper implementation specifics, and unit test assertions.

### Deferred Ideas (OUT OF SCOPE)
- None.

</user_constraints>

<architectural_responsibility_map>
## Architectural Responsibility Map

| Capability | Primary Tier | Secondary Tier | Rationale |
|------------|-------------|----------------|-----------|
| Control Plane Unit Tests | Test Suite (`tests/test_control_plane.py`) | Mock Layer | Validates admin API writes to Redis hashes and broadcasts events |
| Data Plane Unit Tests | Test Suite (`tests/test_data_plane.py`) | Mock Layer | Validates worker sync, pubsub listening, and dynamic routing updates |
| Reconnect Backoff Loop | Data Plane Worker (`src/postoffice/data_plane.py`) | Network / Driver | Ensures worker resiliency during Redis outages or restarts |
| State Reset Teardown | Data Plane Worker (`src/postoffice/data_plane.py`) & Facade (`src/postoffice/app.py`) | Lifecycle | Completely flushes brokers and routes on `clear_state` |

</architectural_responsibility_map>

<research_summary>
## Summary

Phase 1 hardens PostOffice's distributed execution layer and establishes 100% hermetic unit test coverage for the Control Plane and Data Plane components.

The research establishes:
1. **Mocking Redis without Third-Party Dependencies:** A lightweight `FakeRedis` helper simulating `hset`, `hgetall`, `delete`, and `pubsub` using standard in-memory dictionaries and generator queues, allowing tests to run in sub-millisecond time with zero external network dependencies.
2. **Exponential Backoff Reconnect:** A clean loop pattern in `DataPlane.run()` and `DataPlane._listen_for_updates()` catching `redis.exceptions.ConnectionError` and `redis.exceptions.TimeoutError`, retrying with exponential backoff (`min(initial_delay * (2 ** attempt), max_delay)`) and jitter, while honoring the worker's `_stop_event`.
3. **Clean State Reset Teardown:** Adding a `reset()` method to `PostOffice` (or updating `DataPlane._handle_clear_state()`) that stops all running brokers, empties `self.brokers` and `self.router.routes`, and resets internal state so new configuration can be ingested cleanly.

</research_summary>

<implementation_patterns>
## Implementation Patterns

### Pattern 1: In-Memory Fake Redis
```python
class FakePubSub:
    def __init__(self, messages=None):
        self.messages = messages or []
        self.subscribed_channels = []

    def subscribe(self, channel):
        self.subscribed_channels.append(channel)

    def listen(self):
        for msg in self.messages:
            yield msg

class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.published_messages = []
        self.pubsub_instance = FakePubSub()

    def hset(self, name, key, value):
        if name not in self.hashes:
            self.hashes[name] = {}
        self.hashes[name][key] = value

    def hgetall(self, name):
        return self.hashes.get(name, {})

    def delete(self, *names):
        for name in names:
            self.hashes.pop(name, None)

    def publish(self, channel, message):
        self.published_messages.append((channel, message))

    def pubsub(self):
        return self.pubsub_instance
```

### Pattern 2: Reconnection Backoff in Data Plane
```python
def _listen_for_updates(self):
    delay = 1.0
    max_delay = 30.0

    while not self._stop_event.is_set():
        try:
            self.pubsub.subscribe(self.channel)
            logger.info(f"Subscribed to {self.channel} for live config updates.")
            delay = 1.0  # Reset on successful connect

            for message in self.pubsub.listen():
                if self._stop_event.is_set():
                    break
                self._process_message(message)
        except (redis.ConnectionError, redis.TimeoutError) as e:
            if self._stop_event.is_set():
                break
            logger.warning(f"Redis connection lost: {e}. Retrying in {delay}s...")
            self._stop_event.wait(delay)
            delay = min(delay * 2, max_delay)
```

### Pattern 3: Complete State Teardown
```python
# In PostOffice facade:
def reset(self) -> None:
    """Stops all brokers and clears all routes and clients."""
    self.stop()
    self.brokers.clear()
    self.router.routes.clear()
    self.router.clients.clear()
    self.is_running = False
```

</implementation_patterns>

<pitfalls>
## Common Pitfalls & Anti-Patterns

1. **Infinite Reconnect Blocking on Shutdown:** The reconnection loop must check `self._stop_event.is_set()` before and after sleep, using `self._stop_event.wait(delay)` rather than `time.sleep(delay)`.
2. **Leaking Subprocess Threads:** When `clear_state` is received, running broker connections must be stopped (`broker.disconnect()`) before clearing dictionaries to prevent orphaned threads in `AmqpClient` or `KafkaClient`.
3. **Mocking Static Methods vs Object Instances:** `redis.Redis(...)` should be patched at the module level in `postoffice.control_plane.redis.Redis` and `postoffice.data_plane.redis.Redis`.

</pitfalls>

<verification_strategy>
## Verification Strategy

1. **Unit Tests:**
   ```bash
   PYTHONPATH=src .venv/bin/python -m unittest discover tests/
   ```
2. **Specific Test Files:**
   - `tests/test_control_plane.py`: Verify broker registration, route configuration, subscription additions, and state wiping.
   - `tests/test_data_plane.py`: Verify initial sync from Redis hashes, real-time message handling, backoff reconnect behavior on simulated connection error, and clean teardown on `clear_state`.
3. **No Regressions:**
   - `tests/test_postoffice.py` and `tests/test_wildcards.py` must remain 100% green.

</verification_strategy>

---
*Research completed: 2026-10-05*
