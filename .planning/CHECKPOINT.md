# Checkpoint — Milestone v2.1 Operational Resiliency & Test Coverage Gate — 2026-10-18

**Status:** COMPLETE
**Branch:** `main` (release `v2.1.0` tagged and pushed to `origin` + user guide sync)
**Working Tree:** Clean

## 1. Goal & Context
Deliver Milestone v2.1 (`RES-01` client subscription & publish queuing, `RES-02` egress circuit breakers & backpressure, `RES-03` live mTLS/SASL credential hot-reload), close all P0 and P1 test coverage gaps identified during the coverage audit, enforce a `>= 90%` combined statement + branch coverage gate via `.coveragerc`, record the full audit in `docs/TEST_COVERAGE.md`, and synchronize `docs/user-guide/`.

## 2. Done (With Evidence)
- **Milestone v2.1 (Phases 7 & 8 — tag `v2.1.0`):**
  - `RES-01` (`src/postoffice/base_client.py`, `src/postoffice/plugins/{mqtt,amqp,kafka,nanomq}_client.py`): Thread-safe `_subscriptions` registry preserving subscription `kwargs` (`qos`, `exchange`, `queue`) across reconnects and bounded `deque` `_publish_buffer` supporting `"reject"` (`BufferOverflowError`) and `"drop_oldest"` overflow policies (`tests/test_client_queuing.py` -> 16 passed).
  - `RES-02` (`src/postoffice/resiliency.py`, `src/postoffice/router.py`, `src/postoffice/metrics.py`): Per-broker `CircuitBreaker` (`CLOSED -> OPEN -> HALF_OPEN -> CLOSED`), automatic fallback into target publish ring buffer while `OPEN`, DLQ diversion on buffer overflow or disabled buffer, and buffered message flush on `HALF_OPEN -> CLOSED` probe recovery (`tests/test_circuit_breaker.py` -> 10 passed).
  - `RES-03` (`src/postoffice/plugins/*`, `src/postoffice/app.py`, `src/postoffice/control_plane.py`, `src/postoffice/data_plane.py`): Live `reload_security()` across all 4 broker adapters and `ControlPlane.update_broker_security()` -> Redis `postoffice:brokers` hash + `postoffice:config_updates` Pub/Sub synchronization without losing subscriptions or buffered publishes (`tests/test_hot_reload.py` -> 10 passed).
- **Test Coverage Audit & P0/P1 Remediation (`d1d0ff0`):**
  - Created `tests/test_plugin_ingress_callbacks.py` and expanded `tests/test_plugin_security.py`, `tests/test_schema_registry.py`, `tests/test_schema_converters.py`, `tests/test_circuit_breaker.py`, `tests/test_data_plane.py`, and `tests/test_postoffice.py`.
  - Wired `.coveragerc` (`branch = True`, `source = src/postoffice`, `fail_under = 90`) and added `coverage` to `requirements.txt`, `AGENTS.md`, and `GEMINI.md`.
  - Updated `docs/TEST_COVERAGE.md` with before/after metrics:
    - Combined statement + branch coverage: **`79.0% -> 90.0%`** (`1,803 / 1,984` points)
    - Statement coverage: **`82.7% -> 92.9%`** (`1,392 / 1,498` statements)
    - Function reachability: **`92.1% -> 99.3%`** (`138 / 139` functions; `100.0%` of concrete methods)
  - Verified gate command:
    `PYTHONPATH=src .venv/bin/coverage run -m unittest discover tests/ && .venv/bin/coverage report && .venv/bin/coverage erase` -> `Ran 106 tests in 1.014s — OK — TOTAL 90%`.
- **User Guide Synchronization:**
  - Created `docs/user-guide/resiliency-and-circuit-breakers.md` and synchronized `README.md` and `docs/user-guide/{README,security-and-auth,observability-and-dlq,distributed-plane,troubleshooting}.md`.

## 3. Failed / Blocked (With Root Cause)
- None. All 8 phases (16 plans) across Milestones v1.0, v2.0, and v2.1 plus the P0/P1 coverage remediation are complete and verified.

## 4. Bugs / Gotchas Discovered This Session (Do Not Reintroduce)
- **`RedisSchemaRegistry` test false-positive cache hits:** Testing `RedisSchemaRegistry.get_schema()` and `get_schema_by_id()` on the same instance that called `register_schema()` only exercises `self.local_cache` (`70%` module coverage). Always instantiate a second `RedisSchemaRegistry` sharing the mock Redis backend (cold `local_cache`) to verify Redis `hgetall`/`hget` hydration (`lines 218-252` in `src/postoffice/schema/registry.py`).
- **`AmqpClient._on_message` thread-safe ACK/NACK dispatch:** `pika` connections are not thread-safe; `_ack` and `_nack` closures schedule `ch.basic_ack` / `ch.basic_nack` via `self.connection.add_callback_threadsafe(...)` when `self.connection.is_open` is `True`, falling back to direct channel calls only when closed/mocked.
- **`Router.route` when `CircuitBreaker` is `OPEN`:** When a target broker's circuit breaker is `OPEN`, `Router.route` attempts `target_client._buffer_publish(target_topic, transformed_payload, **publish_kwargs)` before raising `CircuitBreakerOpenError`. Only when `enable_publish_buffer=False` or `BufferOverflowError` is raised does it trip `coordinator.on_error(exc)` / DLQ routing.

## 5. State That May Have Changed
- `coverage==7.16.2` is installed in `.venv/` and listed in `requirements.txt`.
- `.coveragerc` is configured at repo root enforcing `branch = True` and `fail_under = 90`.
- Temporary `.coverage` SQLite database was erased via `.venv/bin/coverage erase` (and is ignored by `.gitignore`).
- No background daemons or Docker containers were left running.

## 6. Explicit Authorizations & Scope
- Local unit tests must remain 100% hermetic (`unittest.mock` for `paho-mqtt`, `pika`, `confluent-kafka`, and `redis`).
- Conserve GitHub Actions CI minutes (`RULE[user_global]` #9): execute all test and coverage gates locally before pushing or opening PRs.

## 7. Next Steps (Exact Commands In Order)
1. Verify repository state and branch:
   ```bash
   git status -s && git log -n 3 --oneline
   ```
2. Run the hermetic unit test suite and 90% branch+statement coverage gate:
   ```bash
   PYTHONPATH=src .venv/bin/coverage run -m unittest discover tests/ && .venv/bin/coverage report && .venv/bin/coverage erase
   ```
   *(Expect: `Ran 106 tests` -> `OK`, `TOTAL 1498 stmts, 106 miss, 486 branch, 75 brpart, 90%`)*
3. Review `.planning/ROADMAP.md`, `.planning/STATE.md`, and `docs/TEST_COVERAGE.md` when scoping the next milestone (`v2.2` or `v3.0`).
