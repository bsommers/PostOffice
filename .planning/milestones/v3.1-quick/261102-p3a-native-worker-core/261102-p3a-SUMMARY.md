# Quick Task 261102-p3a Summary: Native Data Plane Worker Core (P3-1)

## Completed
- Implemented D9 HMAC-SHA256 signing & verification (`HmacSigner`, `sha256`, `hmac_sha256`, domain separation `event\0`, `state\0`, `version\0`) and Control-Plane action validation (`validate_action_json`) in [`native/libpostoffice/src/signing.rs`](../../../native/libpostoffice/src/signing.rs).
- Implemented the Native Data Plane Engine (`Engine`, `EngineState`, `BudgetedDlq`, `format_dlq_payload`, `LabelGuard`, `MetricsRegistry`, and HTTP `/metrics`, `/healthz`, `/readyz` server) in [`native/libpostoffice/src/engine.rs`](../../../native/libpostoffice/src/engine.rs).
- Implemented Redis RESP2 wire client and T7 Subscribe-Before-Snapshot convergence (`converge_from_redis`) in [`native/libpostoffice/src/redis_sync.rs`](../../../native/libpostoffice/src/redis_sync.rs) and standalone binary [`native/libpostoffice/src/bin/postoffice-worker.rs`](../../../native/libpostoffice/src/bin/postoffice-worker.rs).
- Extended C ABI ([`include/postoffice.h`](../../../include/postoffice.h), [`native/libpostoffice/src/lib.rs`](../../../native/libpostoffice/src/lib.rs)) and Python FFI bridge ([`src/postoffice/native.py`](../../../src/postoffice/native.py)) with `NativeSigner`, `NativeDlqBudget`, `NativeEngine`, `native_format_dlq_payload`, and `native_validate_action`.
- Verified with 5 test methods in [`tests/test_phase3_native_worker_core.py`](../../../tests/test_phase3_native_worker_core.py) and `conformance/run.py --both` (`71/71` in both modes).
