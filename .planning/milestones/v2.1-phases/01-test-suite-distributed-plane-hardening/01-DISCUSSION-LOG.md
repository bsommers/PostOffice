# Phase 1: Test Suite & Distributed Plane Hardening - Discussion Log

> **Audit trail only.** Do not use as input to planning, research, or execution agents.
> Decisions are captured in CONTEXT.md — this log preserves the alternatives considered.

**Date:** 2026-10-05
**Phase:** 1-Test Suite & Distributed Plane Hardening
**Areas discussed:** Mocking Strategy, Reconnection Policy, Clear State Action, Test Suite Structure

---

## Mocking Strategy

| Option | Description | Selected |
|--------|-------------|----------|
| Custom in-memory fake with unittest.mock | Zero external dependencies, consistent with existing broker mocks | ✓ |
| fakeredis package | Accurate Redis behavior simulation but adds third-party test dependency | |
| You decide | Use whichever provides best test reliability with minimal overhead | |

**User's choice:** Custom in-memory fake with unittest.mock
**Notes:** Avoid adding extraneous test dependencies like fakeredis to keep the environment lightweight and hermetic.

---

## Reconnection Policy

| Option | Description | Selected |
|--------|-------------|----------|
| Exponential backoff reconnect loop | 1s up to max 30s — resilient to temporary Redis outages/restarts | ✓ |
| Fixed retries then terminate | Fail-fast for orchestrator container restart | |
| Configurable reconnect policy | Environment variables (RECONNECT_ATTEMPTS, RECONNECT_DELAY) | |

**User's choice:** Exponential backoff reconnect loop (e.g. 1s up to max 30s)
**Notes:** Allows workers to survive transient Redis network hiccups or cluster rollouts.

---

## Clear State Action

| Option | Description | Selected |
|--------|-------------|----------|
| Full teardown | Disconnect all brokers and reset routing tables to empty state cleanly without restarting process | ✓ |
| Clear routes & subscriptions only | Preserve active broker connections, wipe routing table in memory | |
| You decide | Whichever cleanly resets state without connection leaks | |

**User's choice:** Full teardown
**Notes:** Completely resets state to pristine condition upon receiving `clear_state`.

---

## Test Suite Structure

| Option | Description | Selected |
|--------|-------------|----------|
| Dedicated test files | tests/test_control_plane.py and tests/test_data_plane.py (1:1 mapping with src modules) | ✓ |
| Single consolidated test file | tests/test_distributed.py (combines ControlPlane and DataPlane tests) | |
| You decide | Match codebase conventions | |

**User's choice:** Dedicated test files (tests/test_control_plane.py and tests/test_data_plane.py)
**Notes:** Clean separation of concerns matching the 1:1 layout of source modules.

---

## Claude's Discretion

- Backoff timing calculation and mock helper implementations.

## Deferred Ideas

- None.
