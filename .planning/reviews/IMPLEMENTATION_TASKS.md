# Architecture Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close the delivery-guarantee, control-plane and hygiene gaps from the 2026-10-25 architecture review, the security scan and the dependency audit.

**Architecture:** Keep the Facade → Router → Plugin layering. Fix adapter correctness first (real confirms, reconnect, Kafka commits), then refactor `Router.route()` under the existing test net, then make the Control/Data Plane convergent. Every task is TDD with hermetic mocked tests; opt-in integration tests are a separate tier.

**Tech Stack:** Python 3.12+, paho-mqtt 2.1, pika 1.4.4, confluent-kafka 2.15, redis 8.1, unittest + unittest.mock.

**Spec:** `.planning/reviews/ARCHITECTURE_REVIEW_PLAN.md` (finding IDs R*, M*, A*, K*, D*, S*, B*, F* refer to it), `.security-reports/scan-2026-10-25.md`, `docs/DEPENDENCY_AUDIT.md`.

## Global Constraints

- Run tests with `PYTHONPATH=src .venv/bin/python -m unittest discover tests/`; baseline is **119 tests OK**. Each task ends with the full suite green.
- Default suite stays hermetic: no network, brokers or Redis.
- Public contracts kept: `PostOffice.reset()` returns the facade to the unconfigured **and stopped** state (asserted by `test_facade_lifecycle_and_validation_guards`).
- No secrets or payload bodies in logs at INFO or above.
- One atomic commit per task. **Commits and pushes need explicit user approval**; none are authorized yet.
- Project CLAUDE.md asks for GSD workflow for edits; update `.planning/STATE.md` when a phase closes.

## Review Focus

- Source ack must never precede broker acceptance (MQTT QoS≥1, AMQP unroutable, Kafka delivery report).
- Broker down at startup, then up later: adapter must connect and flush its buffer without a process restart.
- Kafka consumer killed mid-batch: no offset past an unacknowledged message.
- Worker restarts or Redis blips between snapshot and subscribe: no lost route/broker events, no duplicate routes.
- Same `add_route`/`add_broker` replayed: idempotent, no duplicate delivery.
- Poison message with no DLQ must not requeue forever.

## Status of earlier work

| Item | State |
|---|---|
| P0.1 reset/clear_state, P0.2 per-route DLQ, P0.6 log redaction + AMQP `auto_ack` default | DONE, uncommitted, in `tests/test_p0_hardening.py` |
| Security scan | DONE → `.security-reports/scan-2026-10-25.md` (grade B; 2 Medium, 2 Low) |
| Dependency audit | DONE → `docs/DEPENDENCY_AUDIT.md` (no CVE verified; unpinned deps) |

---

## Phase 0: Land the finished work

### Task 0: Commit P0.1/P0.2/P0.6 (needs user approval)

**Files:** Modify `src/postoffice/{app,control_plane,data_plane,router,security}.py`, `src/postoffice/plugins/amqp_client.py`; add `tests/test_p0_hardening.py`, `.planning/reviews/*`, `.security-reports/*`, `docs/DEPENDENCY_AUDIT.md`.

- [ ] **Step 1:** Run the full suite; expect `Ran 119 tests ... OK`.
- [ ] **Step 2:** Add `graphify-out/` to `.gitignore` (folds in P2.7) and confirm `git status` shows no generated files staged.
- [ ] **Step 3:** After approval, commit code+tests as `fix(router,dataplane): P0.1/P0.2/P0.6 hardening`, and reports as a separate `docs:` commit.

---

## Phase P0: Delivery guarantees

### Task 1: Real publish confirms for MQTT (M1, M3)

**Files:** Modify `src/postoffice/plugins/mqtt_client.py` (`publish`, `_on_connect`, add `_on_publish`); Test `tests/test_mqtt_confirms.py`.

**Interfaces:** Produces `MqttClient._inflight: dict[int, tuple[Callable|None, Callable|None]]` keyed by paho `mid`, guarded by an existing lock.

- [ ] **Step 1:** Write failing tests: `test_qos0_confirms_immediately`; `test_qos1_confirm_waits_for_on_publish` (assert `on_confirm` not called after `publish`, called after `_on_publish(client, None, mid, ...)`); `test_rc_failure_calls_on_error_not_confirm`; `test_connect_refused_reason_code_skips_subscribe`.
- [ ] **Step 2:** Run `-k mqtt_confirms`; expect FAIL.
- [ ] **Step 3:** Implement per plan P0.3: check `info.rc`, track `mid` for QoS≥1, resolve in `_on_publish`; in `_on_connect` return early when `reason_code.is_failure`.
- [ ] **Step 4:** Run the new tests then the full suite; expect all OK.
- [ ] **Step 5:** Commit `fix(mqtt): confirm publishes only after broker acceptance`.

### Task 2: Publisher confirms and unroutable handling for AMQP (A1, A4, A5)

**Files:** Modify `src/postoffice/plugins/amqp_client.py`; Test `tests/test_amqp_confirms.py`.

**Interfaces:** Consumes `pika.exceptions.UnroutableError`, `NackError`. Produces constructor kwargs `virtual_host: str = "/"`; credentials default to `None` and raise `ValueError` if absent (scan finding, Low).

- [ ] **Step 1:** Write failing tests: `test_confirm_delivery_enabled_on_channel_open`; `test_publish_uses_mandatory_and_confirms_after_success`; `test_unroutable_calls_on_error`; `test_missing_credentials_raise_value_error`; `test_queue_declared_once_per_queue_not_per_publish`; `test_requeue_nack_without_dlq_is_capped` (requeue only if redelivered flag is false).
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement: `channel.confirm_delivery()` after open, `basic_publish(..., mandatory=True)`, cache declared queues in a set, remove `user`/`password` fallbacks, honour `virtual_host`.
- [ ] **Step 4:** Run full suite; expect OK. Update docs that mention default credentials.
- [ ] **Step 5:** Commit `fix(amqp): publisher confirms, no default credentials, bounded requeue`.

### Task 3: Reconnect for MQTT and AMQP (M2, A2, A6, B1)

**Files:** Modify `mqtt_client.py` (`connect`), `amqp_client.py` (`_consume_loop`, `connect`), `base_client.py` (`flush_publish_buffer`); Test `tests/test_reconnect.py`.

**Interfaces:** Produces `BaseClient.flush_publish_buffer()` that re-buffers the failing item at the head and invokes `on_error` if retries are exhausted, never dropping silently.

- [ ] **Step 1:** Write failing tests: `test_mqtt_first_connect_failure_uses_connect_async_and_loop_start`; `test_amqp_loop_retries_with_backoff_after_connect_error` (patch the stop-event wait to record delays 1, 2, 4 …, cap 30); `test_amqp_clears_channel_on_failure_so_publish_buffers`; `test_amqp_resubscribes_and_flushes_after_reconnect`; `test_flush_requeues_item_when_publish_raises`.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement: MQTT `connect_async` + `loop_start` + `reconnect_delay_set(1, 30)`; AMQP backoff loop that nulls `connection`/`channel`, then calls `_apply_subscription` for each recorded subscription and `flush_publish_buffer`; fix B1 ordering.
- [ ] **Step 4:** Full suite OK.
- [ ] **Step 5:** Commit `fix(plugins): reconnect with backoff for MQTT and AMQP`.

### Task 4: Kafka callbacks and offset commits (K1–K5)

**Files:** Modify `src/postoffice/plugins/kafka_client.py`; Test `tests/test_kafka_delivery.py`.

**Interfaces:** Produces `KafkaClient._pending: dict[tuple[str,int], SortedSet[int]]` tracking completed offsets, and `_commit_contiguous(topic, partition)`.

- [ ] **Step 1:** Write failing tests: `test_consumer_config_disables_auto_commit`; `test_producer_polled_without_new_publish` (a poll thread or consume-loop poll is invoked); `test_offset_not_committed_past_gap` (ack 5 and 7 with 6 outstanding → commit 6 only after 6 completes, then 8); `test_bad_config_raises_handled_error_not_uncaught` (`KafkaException`/`ValueError` caught in `connect`); `test_reload_security_clears_handles_so_publish_buffers`; `test_publish_error_not_double_reported`.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement per P0.5; `enable.auto.commit=false`, `enable.auto.offset.store=false`.
- [ ] **Step 4:** Full suite OK. Manual check (documented, not in default suite): `docker compose up -d`, kill the consumer mid-batch, confirm no offset advanced past an unacked message.
- [ ] **Step 5:** Commit `fix(kafka): deterministic delivery callbacks and contiguous offset commits`.

### Task 5: Security-scan fixes (Medium/Low)

**Status: DONE.** `start_server`/`set_metrics_port` default to `127.0.0.1` (`PostOffice(metrics_addr=...)`, `set_metrics_port(port, addr=...)`); `ControlPlane`/`DataPlane` take `redis_password`/`redis_tls`; `worker.py` and `scripts/admin.py` read `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_TLS=1`; all three plugins log a WARNING when `insecure_skip_verify` is set. Behaviour change: metrics were reachable on all interfaces and now bind loopback unless widened explicitly. Tests: `tests/test_security_hardening.py`. No compose file starts a worker with metrics, so none needed updating.

**Files:** Modify `metrics.py`, `app.py` (`set_metrics_port`), `control_plane.py`, `data_plane.py`, `worker.py`, the three plugins (warning on skip-verify); Test `tests/test_security_hardening.py`.

**Interfaces:** Produces `MetricsManager.start_server(port, addr="127.0.0.1")`, `PostOffice.set_metrics_port(port, addr="127.0.0.1")`, and env vars `REDIS_PORT`, `REDIS_PASSWORD`, `REDIS_TLS` ("1" enables), read in `worker.py` and passed to `DataPlane`/`ControlPlane` constructors as `redis_password: str | None`, `redis_tls: bool`.

- [ ] **Step 1:** Write failing tests: `test_metrics_binds_loopback_by_default`; `test_metrics_addr_override`; `test_redis_client_receives_password_and_ssl` (assert kwargs passed to patched `redis.Redis`); `test_insecure_skip_verify_logs_warning` for each plugin.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement the above; update `docker-compose.yml`/docs where the metrics port must be reachable (set addr `0.0.0.0` explicitly).
- [ ] **Step 4:** Full suite OK.
- [ ] **Step 5:** Commit `fix(security): loopback metrics default, Redis auth/TLS options, skip-verify warnings`.

---

## Phase P1: Refactoring and convergence

### Task 6: Typed `Route`, `EgressClient` Protocol, decompose `route()` (P1.1, P1.2, P1.5, R3, R5, R6)

**Files:** Modify `src/postoffice/router.py`, `base_client.py`; Create `src/postoffice/routing_types.py`; Test: existing router tests unchanged plus `tests/test_router_pipeline.py`.

**Interfaces:** Produces `@dataclass(frozen=True) Route(target_client, target_topic, dlq_broker, dlq_topic, schema, publish_kwargs)`, `class EgressClient(Protocol)` with `publish`, `buffer_publish`, `flush_publish_buffer`, `Router.remove_route(source_broker, source_topic, target_broker, target_topic) -> bool`, `TopicTrie.remove(...)`, and a single `Router._fail(delivery, route, exc)`.

- [ ] **Step 1:** Write failing tests: `test_add_route_is_idempotent`; `test_unknown_route_kwarg_raises_value_error_at_registration`; `test_remove_route_stops_delivery`; `test_dlq_publish_outside_coordinator_lock`; `test_nack_fn_always_receives_requeue_keyword`.
- [ ] **Step 2:** Run; expect FAIL; run the full suite to record the pre-refactor baseline.
- [ ] **Step 3:** Refactor behind the existing 119 tests: extract `_prepare`, `_check_egress`, `_dispatch`, `_fail`; delete the three `except TypeError` fallbacks; remove `Router.routes` or derive it from the trie.
- [ ] **Step 4:** Full suite OK with no existing test edited (except those pinned to removed internals; list any in the commit body).
- [ ] **Step 5:** Commit in two parts: `refactor(router): typed Route and pipeline stages`, `feat(router): remove_route, idempotent add_route`.

### Task 7: Convergent Control/Data Plane (P1.3, D1–D3, D6, D7)

**Status: DONE** (with only the idempotent-`add_route` slice of Task 6, which T7 consumes; the rest of T6 is still open). `DataPlane.run()` subscribes before the snapshot and listens on that subscription, so events published mid-snapshot are applied after it; sync is idempotent (brokers keyed by name and replaced only when their config changed via `PostOffice.remove_broker`, identical subscriptions skipped, `Router.add_route` ignores an identical route); every listener reconnect re-subscribes then `resync()`s; `ControlPlane` INCRs `postoffice:version` on each write and stamps events with `version`, the worker skips stale versions and resyncs on a gap (unversioned events from older control planes still apply); `update_broker_security` for an unknown broker logs an error, counts `security_update_unknown_broker` and resyncs; security reloads update the applied-config record so a later resync does not needlessly replace the client. Deviation from the written plan: no separate `_subscribe_and_buffer`/`_drain_buffer` methods; the pre-subscribed socket queue is the buffer. Known limits: a resync never removes entities deleted from Redis (the Control Plane has no delete API besides `clear_state`); concurrent Control Plane writers can publish out of version order, which costs a spurious resync, not a wrong state. Tests: `tests/test_data_plane_convergence.py`; existing tests that build planes by hand call `_init_convergence_state()`.

**Files:** Modify `data_plane.py`, `control_plane.py`, `app.py` (idempotent `add_broker` option); Test `tests/test_data_plane_convergence.py`.

**Interfaces:** Consumes Task 6 idempotent `add_route`. Produces `DataPlane._subscribe_and_buffer()`, `_drain_buffer()`, `resync()`; Redis key `postoffice:version` incremented by every `ControlPlane` write.

- [ ] **Step 1:** Write failing tests: `test_event_published_during_snapshot_is_applied` (event arrives between hgetall calls); `test_sync_retry_is_idempotent_after_partial_failure`; `test_listener_reconnect_triggers_resync`; `test_replayed_add_route_does_not_duplicate`; `test_version_gap_triggers_resync`; `test_update_security_for_unknown_broker_reports_error`.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement: subscribe first, buffer, snapshot, drain; replace-on-conflict for brokers keyed by name; resync on reconnect and on version gap.
- [ ] **Step 4:** Full suite OK.
- [ ] **Step 5:** Commit `fix(dataplane): subscribe-before-snapshot, idempotent sync, resync on reconnect`.

### Task 8: Schema registry wiring and atomic register (P1.6, S1–S4)

**Files:** Modify `schema/registry.py`, `schema/converters.py`, `data_plane.py`; Test `tests/test_schema_distributed.py`.

**Interfaces:** `DataPlane` builds `PostOffice(schema_registry=RedisSchemaRegistry(self.r))`.

- [ ] **Step 1:** Write failing tests: `test_dataplane_uses_redis_registry`; `test_concurrent_register_yields_distinct_versions` (threaded, fake Redis with a lock); `test_cache_refreshes_when_version_bumped`; `test_unknown_protobuf_type_raises_value_error`.
- [ ] **Step 2:** Run; expect FAIL.
- [ ] **Step 3:** Implement with `WATCH/MULTI` or a Lua script; version-checked cache reads; `ValueError` for unknown proto types.
- [ ] **Step 4:** Full suite OK.
- [ ] **Step 5:** Commit `feat(schema): distributed registry wiring and atomic register`.

### Task 9: Observable facade failures (P1.7, F2)

**Files:** Modify `app.py`, `data_plane.py`; Test `tests/test_facade_errors.py`.

**Interfaces:** `PostOffice.start(strict: bool = False) -> dict[str, Exception | None]`; `strict=True` raises an aggregate `RuntimeError`. `subscribe()` raises `ValueError` for unknown broker.

- [ ] **Step 1:** Write failing tests for each behavior above plus `test_default_start_keeps_current_log_and_continue_contract`.
- [ ] **Step 2–4:** Run to fail; implement; full suite OK.
- [ ] **Step 5:** Commit `feat(app): per-broker start results and strict mode`.

---

## Phase P2: Hygiene

### Task 10: Packaging and dependency pinning (P2.4, P2.7, P2.9; dependency audit)

**Files:** Create `pyproject.toml`, `requirements-dev.txt`; Modify `requirements.txt`, `.gitignore`, `docker-compose.yml`, `scripts/admin.py`, `src/postoffice/worker.py`; Test `tests/test_packaging.py`.

- [ ] **Step 1:** Write failing test `test_console_entry_points_importable` (import `postoffice.worker:main`, `scripts` equivalent exposed as `postoffice.admin:main`).
- [ ] **Step 2–3:** Implement `pyproject.toml` (src layout, `requires-python = ">=3.12"`, bounded deps from `docs/DEPENDENCY_AUDIT.md`), move `coverage` to dev, drop `sys.path` hacks, remove compose `version:`, add healthchecks.
- [ ] **Step 4:** Rebuild a fresh venv with `uv sync`/`uv pip install -e .`, run the full suite; expect OK. Run `uv run --with pip-audit pip-audit -r requirements.txt` and record the result in `docs/DEPENDENCY_AUDIT.md`.
- [ ] **Step 5:** Commit `build: pyproject, bounded dependencies, dev split`.

### Task 11: Small cleanups (P2.1, P2.2, P2.3, P2.6, P2.8, metrics import)

**Files:** `router.py` (remove `_topic_match`), `tests/test_wildcards.py` (port to `TopicTrie`), `security.py` (validate `sasl_mechanism`), `resiliency.py` (probe timeout), `metrics.py` (drop `WSGIServer` import); Test additions in the corresponding files.

- [ ] **Step 1:** Failing tests: `test_half_open_probe_times_out_and_reopens`; `test_invalid_sasl_mechanism_raises_value_error`.
- [ ] **Step 2–4:** Implement; full suite OK.
- [ ] **Step 5:** Commit `chore: remove dead matcher, probe timeout, validation`.

### Task 12: Opt-in integration tier (P2.10)

**Files:** Create `tests/integration/` (skipped unless `POSTOFFICE_INTEGRATION=1`), `tests/integration/README.md`.

- [ ] **Step 1:** Write tests: MQTT QoS1 confirm, AMQP unroutable → error, Kafka restart offset ordering, reconnect after broker restart.
- [ ] **Step 2:** Run default suite; expect integration tests reported as skipped and count unchanged otherwise.
- [ ] **Step 3:** Run with `docker compose up -d` and `POSTOFFICE_INTEGRATION=1`; record results.
- [ ] **Step 4:** Commit `test: opt-in integration tier`.

---

## Phase P3 (roadmap, not scheduled)

P3.1 handler registry for plane events; P3.2 Redis Streams; P3.3 bounded per-target egress queues; P3.4 topic-dialect adapters; P3.5 durable buffer + `max_age`; P3.6 buffer/breaker/confirm-latency metrics (count success at confirm, R2); P3.7 plugin entry points. Each needs its own brainstorm and plan before work starts.

## Self-Review

- Spec coverage: P0.3→T1/T2, P0.4→T3, P0.5→T4, P1.1/1.2/1.5→T6, P1.3→T7, P1.6→T8, P1.7→T9, P2.x→T10–12; scan findings→T2 (creds), T5 (metrics, Redis auth, skip-verify); dependency audit→T10. R2/R4 (unroutable policy) are deliberately left to P3.6 and a separate decision: R4 needs a user decision on `on_unroutable`.
- Order: T1–T5 independent; T6 before T7; T10 any time. Suggested: T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10–T12.
