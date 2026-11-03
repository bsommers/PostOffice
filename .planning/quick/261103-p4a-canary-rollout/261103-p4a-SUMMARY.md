# Quick Summary `261103-p4a`: Phase 4 Slice 1 — Control-Plane Route Tagging & Canary Rollout Controller

## Completed
1. **Control-Plane Route Tagging & Rollout Validation ([`src/postoffice/validation.py`](../../../src/postoffice/validation.py), [`src/postoffice/control_plane.py`](../../../src/postoffice/control_plane.py), [`native/libpostoffice/src/signing.rs`](../../../native/libpostoffice/src/signing.rs)):**
   - Added `VALID_ENGINES = ("all", "python", "native", "canary")` validation to `add_route` (`engine` tag in `target_kwargs` or top-level).
   - Added `"set_rollout"` action (`native_percentage` in `0..100`, `environment`, `status`, `reason`) and `ControlPlane.set_rollout(...)` persisting to Redis hash `postoffice:rollout` and broadcasting over `postoffice:config_updates`.
2. **Deterministic Canary Bucket & Mixed-Fleet Filtering ([`src/postoffice/router.py`](../../../src/postoffice/router.py), [`src/postoffice/data_plane.py`](../../../src/postoffice/data_plane.py), [`native/libpostoffice/src/engine.rs`](../../../native/libpostoffice/src/engine.rs), [`native/libpostoffice/src/redis_sync.rs`](../../../native/libpostoffice/src/redis_sync.rs)):**
   - Implemented `canary_bucket(source_broker, source_topic, payload)` using SHA-256 (`b"canary\x00..."` first 8 bytes big-endian `% 100`) bit-for-bit identical in Python and Rust.
   - Filtered matched routes by `worker_engine` (`"python"` vs `"native"`) and `native_percentage`, ensuring zero duplicate deliveries and zero dropped messages across `0% -> 5% -> 25% -> 100%` splits.
3. **Canary Rollout Controller with Automatic Rollback ([`src/postoffice/rollout.py`](../../../src/postoffice/rollout.py)):**
   - Implemented `CanaryPolicy`, `RolloutMetricsSample`, `RolloutState`, and `CanaryRolloutController` ramping `5% -> 25% -> 100%` and automatically rolling back to `0%` (`status="rolled_back"`) on error-rate, consumer-lag, or p99 latency regressions.
4. **Tests ([`tests/test_phase4_canary_rollout.py`](../../../tests/test_phase4_canary_rollout.py)):**
   - 4 hermetic tests verifying validation, static `engine="native"`/`engine="python"` isolation, 200-message x 4-stage (`0%`, `5%`, `25%`, `100%`) complementarity between Python and Rust, and automatic rollback drills.
