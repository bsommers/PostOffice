# Mixed-Fleet Canary Rollout & Rollback Runbook

## 1. Overview
PostOffice runs Python (`postoffice.worker`) and Native Rust (`postoffice-worker`) Data Plane workers side-by-side against the same Redis Control Plane and broker consumer groups.
- **Route Tagging:** Each route carries an optional `engine` selector (`"all"`, `"python"`, `"native"`, or `"canary"`).
- **Canary Traffic Split:** Routes tagged `engine="canary"` use deterministic SHA-256 hashing (`canary_bucket(source_broker, source_topic, payload)`) so every message is processed by **exactly one** engine (`bucket < native_percentage` -> `native`; `bucket >= native_percentage` -> `python`).
- **Ramp Stages:** `5% -> 25% -> 100%` per environment (`dev`, `staging`, `prod`).

## 2. Pre-Flight Health & Readiness Checks
Both Python and Native workers expose identical HTTP endpoints on `:9100`:
- **Liveness (`/healthz`):**
  ```bash
  curl -fsS http://127.0.0.1:9100/healthz
  # {"status":"ok","version":"1.0.0"}
  ```
- **Readiness (`/readyz`):**
  ```bash
  curl -fsS http://127.0.0.1:9100/readyz
  # {"ready":true,"applied_version":42}
  ```
- **Prometheus Metrics (`/metrics`):**
  ```bash
  curl -fsS http://127.0.0.1:9100/metrics
  ```

## 3. Progressive Rollout (`5% -> 25% -> 100%`)
Use `CanaryRolloutController` (`src/postoffice/rollout.py`) or `ControlPlane.set_rollout`:
1. **Stage 0 (`5%`):**
   ```python
   cp.set_rollout(native_percentage=5, environment="prod", status="active", reason="canary_start")
   ```
2. **Stage 1 (`25%`):** Promote after `>= 100` samples with `error_rate <= 0.01`, `consumer_lag <= 1000`, and `p99_latency_ms <= 5.0`.
3. **Stage 2 (`100%`):** Promote to full native cutover (`status="completed"`).

## 4. Automatic Rollback & Manual Emergency Rollback
### Automatic Rollback
`CanaryRolloutController.evaluate_and_step(sample)` automatically sets `native_percentage=0` (`status="rolled_back"`) and broadcasts `set_rollout` over `postoffice:config_updates` when any SLO threshold is breached:
- `error_rate > max_error_rate` (`1%`)
- `consumer_lag > max_lag_messages` (`1000` messages)
- `p99_latency_ms > max_p99_latency_ms` (`5.0 ms`)

### Manual Emergency Rollback
To immediately shift 100% of canary traffic back to Python workers without restarting pods:
```python
from postoffice.control_plane import ControlPlane
cp = ControlPlane(redis_host="localhost", redis_port=6379)
cp.set_rollout(native_percentage=0, environment="prod", status="rolled_back", reason="manual_operator_rollback")
```
Verify `/readyz` `applied_version` increments on all workers within 1 second.
