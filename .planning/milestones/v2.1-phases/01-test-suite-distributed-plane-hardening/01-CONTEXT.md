# Phase 1: Test Suite & Distributed Plane Hardening - Context

**Gathered:** 2026-10-05
**Status:** Ready for planning

<domain>
## Phase Boundary

Phase 1 focuses on hardening the distributed control and data plane subsystems:
1. Writing hermetic unit tests for `ControlPlane` and `DataPlane` using standard library `unittest` and `unittest.mock`.
2. Implementing an exponential backoff reconnect loop in `DataPlane` for resilient Redis connectivity.
3. Implementing clean state teardown in `DataPlane` when handling `clear_state` events from `ControlPlane`.

</domain>

<decisions>
## Implementation Decisions

### Mocking Strategy
- **D-01:** Use a custom in-memory fake Redis client with `unittest.mock.MagicMock` rather than introducing `fakeredis` as an external dependency. This preserves zero-dependency hermetic testing consistent with existing broker mocks in `tests/test_postoffice.py`.
- **D-02:** Mock Redis data structures (`hset`, `hgetall`, `delete`, `publish`) and `pubsub` listener (`listen()`, `subscribe()`) to thoroughly test sync and update processing paths.

### Reconnection Policy
- **D-03:** In `DataPlane`, wrap Redis initial connection and Pub/Sub listener loop in an exponential backoff reconnect loop (starting at 1s, doubling up to a 30s ceiling).
- **D-04:** Log warnings during reconnect attempts and maintain worker thread liveness until Redis connectivity is re-established.

### Clear State Handling
- **D-05:** Upon receiving a `clear_state` event in `DataPlane._listen_for_updates()`, execute a full teardown: disconnect all active brokers, clear internal route dictionaries and client registries on `PostOffice`, and reset the worker state to empty cleanly without requiring a process restart.

### Test Suite Structure
- **D-06:** Create two dedicated test files matching source file structure:
  - `tests/test_control_plane.py`: Verifies `register_broker`, `add_route`, `add_subscription`, and `clear_state` against mock Redis hashes and Pub/Sub.
  - `tests/test_data_plane.py`: Verifies `sync_state`, `_listen_for_updates`, dynamic event application (`add_broker`, `add_route`, `add_subscription`), `clear_state` teardown, and reconnection error handling.

### Claude's Discretion
- Implementation details of the backoff math, mock Redis class helper functions, and specific test case assertions are left to builder discretion.

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Architecture & Specs
- `docs/ARCHITECTURE.md` — Section "Horizontal Scalability (Control Plane vs Data Plane)" detailing Redis keys and synchronization
- `docs/USER_GUIDE.md` — Section "Distributed Execution (Scalable Setup)" describing worker commands and script usage

### Source Contracts
- `src/postoffice/control_plane.py` — Admin methods (`register_broker`, `add_route`, `add_subscription`, `clear_state`)
- `src/postoffice/data_plane.py` — Worker methods (`sync_state`, `_listen_for_updates`, `run`)

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `tests/test_postoffice.py` — Established pattern for patching external network clients with `unittest.mock.patch` and verifying routing behaviors with `MagicMock`.

### Established Patterns
- Standard library `unittest` runner: `PYTHONPATH=src .venv/bin/python -m unittest discover tests/`
- Daemon background threads with `threading.Event()` for clean lifecycle management.

### Integration Points
- `DataPlane.po` — Embedded `PostOffice` instance managed by the worker.
- Redis channels: `postoffice:config_updates`, and Redis hashes: `postoffice:brokers`, `postoffice:routes`, `postoffice:subscriptions`.

</code_context>

<specifics>
## Specific Ideas

- Test both happy-path configuration synchronization and error-recovery paths (e.g. malformed JSON payloads in Pub/Sub, Redis connection exceptions).

</specifics>

<deferred>
## Deferred Ideas

- None — discussion stayed strictly within Phase 1 scope.

</deferred>

---

*Phase: 1-Test Suite & Distributed Plane Hardening*
*Context gathered: 2026-10-05*
