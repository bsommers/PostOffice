# Session Checkpoint — 2026-10-25

## Goal (user's words)
1. Original: run `/code-architecture-review` → plan saved at `.planning/reviews/ARCHITECTURE_REVIEW_PLAN.md` (DONE).
2. Then: "do all of those in order - P0.1, .2, .6; then /security-scan ; then /dependency-audit ; break the plan into tasks with /plan"
3. Mid-turn addition: "write up a set of documentation on use cases and purpose. give readers good ideas how using PostOffice can benefit them in their use cases -- think of IIoT, IoT, manufacturing, data systems, processing, business transactions, business management, others"

## Status
| Step | State |
|---|---|
| P0.1 / P0.2 / P0.6 fixes | DONE, uncommitted. Suite: 119 tests OK (106 old + 13 new in `tests/test_p0_hardening.py`) |
| /security-scan | IN PROGRESS. Phase 1-2 done; greps found no hardcoded creds/eval/shell/pickle. CVE DB exists (`~/.claude/security/cve-db/index/packages.json`); index has NO entries for paho-mqtt, prometheus-client; npm-oriented matches only for others (name collisions, e.g. protobufjs, not protobuf). Report not yet written to `.security-reports/scan-2026-10-25.md` |
| /dependency-audit | NOT STARTED |
| /plan (task breakdown) | NOT STARTED |
| Use-cases docs | NOT STARTED (new request) |

## Decisions / corrections
- `PostOffice.reset()` contract kept: returns facade to unconfigured AND stopped state (existing test `test_facade_lifecycle_and_validation_guards` asserts it). P0.1 fix lives in `DataPlane` clear_state: restart `po` if it was running. `reset()` now reuses the existing `MetricsManager`.
- P0.2: `_FanoutCoordinator.on_error(exc, route=None)`, `dlq_fn(exc, route)`; per-route `on_error` lambda bound in `Router.route`.
- P0.6: per-message router logs moved to DEBUG, payload body removed; ControlPlane logs action+name only; `SecurityConfig.password/client_key_password` `repr=False`; AMQP `auto_ack` default now `False`.
- Dead end: first tried restarting inside `reset()`; broke existing lifecycle test; moved to DataPlane.

## Working tree (branch `main`, no stashes, no background processes)
Modified: app.py, control_plane.py, data_plane.py, plugins/amqp_client.py, router.py, security.py.
Untracked: tests/test_p0_hardening.py, .planning/reviews/ (plan + this file), graphify-out/ (generated; should be gitignored, plan item P2.7), `.security-reports/` (empty dir, created by scan).
Nothing committed. User has NOT asked to commit. Project CLAUDE.md wants GSD workflow for edits; user explicitly directed this work.

## Authorizations
Edit src/tests/plan files, run graphify, run test suite: given. Commits/pushes: NOT given.

## Installed deps (for dependency-audit)
confluent-kafka 2.15.1, coverage 7.16.2, fastavro 1.13.1, paho-mqtt 2.1.0, pika 1.4.4, prometheus-client 0.26.0, protobuf 7.36.2, redis 8.1.0. `requirements.txt` is unpinned.

## Next command
Finish scan: write `.security-reports/scan-2026-10-25.md` (findings so far: Medium - metrics server binds 0.0.0.0 unauthenticated `metrics.py:81`; Medium - unauthenticated Redis control channel + plaintext creds in Redis; Low - AMQP default creds `user/password` `amqp_client.py:28`; Low - `insecure_skip_verify` available in 3 plugins; positives: TLS verify default on, env-based REDIS_HOST, validation in SecurityConfig/Trie, no eval/shell/pickle). Then invoke Skill `dependency-audit`, then `/plan`, then write use-case docs (suggest `docs/use-cases/` with README index + per-vertical pages; see skill `writing-user-guides`/`documentation`).

Baseline check: `PYTHONPATH=src .venv/bin/python -m unittest discover tests/` → 119 OK.
