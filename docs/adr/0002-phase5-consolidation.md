# ADR 0002: Phase 5 Consolidation — Component Ownership & Reference Oracle

- **Status:** Accepted
- **Date:** 2026-11-04
- **Context:** Phase 5 of `.planning/reviews/NATIVE_REWRITE_PLAN.md` requires deciding per component whether to retire the Python Data Plane or retain it as the reference implementation and executable test oracle, while consolidating P3 items (per-target bounded queues, opt-in disk-backed store-and-forward, and Redis Streams control-plane replay) natively.

## Decision

1. **Native Rust (`libpostoffice` + `postoffice-worker`) owns production Data Plane execution and P3 durability:**
   - **Data Plane Worker (`native/libpostoffice/src/bin/postoffice-worker.rs`):** Default production Data Plane for high-throughput (100k+ msg/s) and edge (`< 64 MB` idle RSS, single-binary) deployments.
   - **Per-Target Bounded Queues (`po_target_queue_t` in `native/libpostoffice/src/queue.rs`):** Enforces `max_messages`, `max_bytes`, `max_age_sec`, and `overflow_policy` (`"reject"` or `"drop_oldest"`).
   - **Edge Disk-Backed Store-and-Forward (`po_disk_store_t` in `native/libpostoffice/src/queue.rs`):** Opt-in (`§7 Decision 5`), size-capped (`size_cap_bytes`), wear-aware batched (`batch_max_messages`, `batch_max_bytes`, `flush_interval_sec`), CRC32-framed binary log with automatic crash truncation of torn trailing writes.
   - **Redis Streams Catch-Up (`postoffice:config_stream` in `native/libpostoffice/src/redis_sync.rs`):** Replays missed control-plane events via `XRANGE` alongside T7 `SUBSCRIBE`-before-snapshot convergence.

2. **Python owns the Control Plane, Admin CLI, Embedder Facade, and Executable Test Oracle:**
   - **Control Plane & Rollout (`src/postoffice/control_plane.py`, `src/postoffice/rollout.py`, `scripts/admin.py`):** Authoritative cluster configuration writer (supporting both Pub/Sub `postoffice:config_updates` and Redis Streams `postoffice:config_stream` via `use_streams=True` / `POSTOFFICE_REDIS_STREAMS=1`) and canary rollout controller (`5% -> 25% -> 100%` with automatic rollback).
   - **Library Facade (`src/postoffice/app.py`):** Embedders can either run in-process Python routing or attach `NativeEngine` (`PostOffice(..., use_native_engine=True)` / `route_native(...)`) via the C ABI bridge (`src/postoffice/native.py`).
   - **Reference Implementation & Conformance Oracle (`src/postoffice/router.py`, `src/postoffice/data_plane.py`, `conformance/`):** Retained in-tree and tested continuously via `conformance/run.py --both` (71/71 vectors in both `python` and `native` modes) and `ShadowHarness` (`src/postoffice/shadow.py`).
