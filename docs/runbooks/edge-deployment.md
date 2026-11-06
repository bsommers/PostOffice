# Edge Binary Packaging & Deployment Runbook

## 1. Overview & Edge Specifications

PostOffice provides an optimized **Edge Profile** designed for resource-constrained edge gateways, industrial IoT nodes, and embedded Linux appliances (Raspberry Pi, NVIDIA Jetson, Siemens IOT2050, Moxa gateways).

In accordance with `.planning/reviews/NATIVE_REWRITE_PLAN.md` (§0.1, §0.2, §7 Decision 2 & Decision 5):
- **Binary Footprint:** Stripped static/minimal binary `< 1 MB` (`postoffice-worker`), stripped shared library `< 1 MB` (`libpostoffice.so`).
- **Memory Budgets:**
  - **Idle / Sync Peak RSS:** `< 64 MB` (`67,108,864` bytes), typically `~14 MB`.
  - **Loaded Peak RSS:** `< 128 MB` (`134,217,728` bytes), typically `~34 MB` under sustained load.
- **Throughput SLO:** `>= 1,000 msg/s` (single core, 1 KB payloads), achieving `> 15,000 msg/s` in local native benchmarks.
- **Durability Strategy:** In-memory queue by default; opt-in disk-backed store-and-forward with wear-aware batching to protect embedded eMMC/NAND flash from write wear.

---

## 2. Building Container Image (`Dockerfile.native`)

PostOffice provides a hardened multi-stage Docker build producing an unprivileged, minimal runtime container.

### Build Command
```bash
docker build -t postoffice-edge:latest -f Dockerfile.native .
```

### Container Architecture & Security Invariants
- **Multi-Stage Build:** Rust compiler stage compiles edge profile (`--no-default-features --features edge`) and strips binaries; stripped artifacts are copied into a clean `debian:bookworm-slim` runtime stage.
- **Unprivileged User:** Runs as non-root user `postoffice` (UID `10001`, GID `10001`).
- **Persistent Volume:** `/var/lib/postoffice` with `0750` permissions for opt-in store-and-forward storage.
- **Healthcheck:** Probes `/healthz` on `:9100` via curl every 10s.
- **Entrypoint:** `/usr/local/bin/postoffice-worker`.

### Running Container in Edge Environments
```bash
docker run -d \
  --name postoffice-worker \
  --restart unless-stopped \
  --user 10001:10001 \
  -v /var/data/postoffice:/var/lib/postoffice \
  -p 9100:9100 \
  -e REDIS_HOST="192.168.1.50" \
  -e REDIS_PORT="6379" \
  postoffice-edge:latest
```

---

## 3. Cross-Compiling for Embedded ARM Architectures

To run directly on bare-metal edge Linux without Docker, cross-compile using `cargo` and cross-toolchains or `cross` (`cross-rs`).

### Target Architectures
1. **`armv7-unknown-linux-gnueabihf`**: 32-bit ARMv7 (Raspberry Pi 2/3, Cortex-A7/A8/A9).
2. **`aarch64-unknown-linux-gnu` / `aarch64-unknown-linux-musl`**: 64-bit ARMv8 (Jetson Nano/Orin, Raspberry Pi 4/5, AWS Graviton).

### Cross-Compilation with `cross` (Recommended)
`cross` handles target sysroots, cross-compilers, and C libraries inside hermetic Docker build environments:

```bash
# 1. Install cross tool
cargo install cross --git https://github.com/cross-rs/cross

# 2. Build for 32-bit ARMv7 Edge Gateway
cross build \
  --release \
  --target armv7-unknown-linux-gnueabihf \
  --no-default-features \
  --features edge \
  --manifest-path native/libpostoffice/Cargo.toml

# 3. Build for 64-bit AArch64 Edge Node
cross build \
  --release \
  --target aarch64-unknown-linux-gnu \
  --no-default-features \
  --features edge \
  --manifest-path native/libpostoffice/Cargo.toml
```

### Strip and Deploy
Always strip debugging symbols before copying binaries to embedded targets:
```bash
# Strip 32-bit ARM binary
arm-linux-gnueabihf-strip target/armv7-unknown-linux-gnueabihf/release/postoffice-worker
arm-linux-gnueabihf-strip target/armv7-unknown-linux-gnueabihf/release/libpostoffice.so

# Strip 64-bit AArch64 binary
aarch64-linux-gnu-strip target/aarch64-unknown-linux-gnu/release/postoffice-worker
aarch64-linux-gnu-strip target/aarch64-unknown-linux-gnu/release/libpostoffice.so
```

---

## 4. Configuring Opt-In `DiskStoreAndForward`

### Flash Wear Mitigation Principles (§7 Decision 5)
Direct synchronous writes on every incoming message rapidly exhaust the write-endurance of eMMC and SD card flash media. PostOffice implements wear-aware batching:
1. **Opt-In Only:** Default routing is RAM-only.
2. **Strict Size Cap:** Enforces hard boundary (e.g., `4 MB` or `64 MB`) so disk never fills up.
3. **Wear-Aware Batching:** Buffers records until `batch_max_messages` or `batch_max_bytes` accumulates, or until `flush_interval_sec` elapses.
4. **Crash Resilience:** Each stored record is framed with a CRC32 integrity checksum.

### Configuration Parameters
When instantiating `DiskStoreAndForward` (`po_disk_store_t` via C ABI or Python FFI):
- `path`: `/var/lib/postoffice/store_and_forward.db`
- `size_cap_bytes`: `4194304` (`4 MB`) for constrained flash, or up to `67108864` (`64 MB`).
- `batch_max_messages`: `32` (batches up to 32 items per disk transaction).
- `batch_max_bytes`: `65536` (`64 KB` write page alignment).
- `flush_interval_sec`: `1.0` (forces background fsync every second).
- `overflow_policy`: `"drop_oldest"` (FIFO ring buffer) or `"reject"`.

```python
from postoffice.native import NativeDiskStoreAndForward

store = NativeDiskStoreAndForward(
    path="/var/lib/postoffice/store_and_forward.db",
    size_cap_bytes=4 * 1024 * 1024,
    batch_max_messages=32,
    batch_max_bytes=64 * 1024,
    flush_interval_sec=1.0,
    overflow_policy="drop_oldest",
)
```

---

## 5. Verification of Memory Budgets & SLO Gates

Run the verification suite to prove compliance with memory and throughput SLOs.

### Local Verification Run
```bash
PYTHONPATH=src .venv/bin/python scripts/verify_edge_profile.py
```

### Hermetic Unit Test Suite
```bash
PYTHONPATH=src .venv/bin/python -m unittest tests/test_edge_packaging.py
```

### JSON Benchmark Report
Verification results are recorded in `docs/benchmarks/edge_profile_report.json`:
- `idle_rss_mb < 64.0`: Verified via ephemeral RESP2 convergence (`~14.1 MB`).
- `loaded_rss_mb < 128.0`: Verified under 1 KB payload pipeline (`~34.3 MB`).
- `throughput_msg_per_sec >= 1000.0`: Verified (`~15,300 msg/s`).
- `all_gates_passed`: `true`.
