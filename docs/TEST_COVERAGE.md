# Test Suite, Conformance & Coverage Report — `PostOffice`

**Date:** `2026-11-05` (`v3.0.0`)
**Tooling used:**
- Python unit & integration suite: `coverage.py` (`--branch --source=src/postoffice`, enforced via `.coveragerc` `fail_under = 90`) + `unittest`
- Dual-engine conformance suite: `conformance/run.py --both` (`71` language-neutral JSON test vectors)
- Native Rust unit & fuzz suite: `cargo test --release --manifest-path native/libpostoffice/Cargo.toml`

---

## 1. Test Suite Inventory

| Dimension | Milestone v2.1 (`2026-10-18`) | Milestone v3.0 (`2026-11-05`) |
|---|---|---|
| Python test files (`tests/**/test_*.py`) | `15` | **`53`** |
| Python hermetic unit tests | `106` | **`586`** (`0` failures) |
| Dual-engine conformance vectors (`conformance/vectors/*.json`) | `0` | **`71 / 71`** (`python` mode) + **`71 / 71`** (`native` mode) |
| Native Rust unit & property/fuzz tests | `0` | **`7 / 7`** (`cargo test --release`) |
| Threshold gate enforced? | `Yes` (`>= 90%`) | `Yes` (`.coveragerc` `fail_under = 90` + `AGENTS.md` / `GEMINI.md` gate) |

---

## 2. Coverage Summary (`v3.0.0`)

| Metric | Milestone v2.1 | Milestone v3.0 (`2026-11-05`) | Source |
|---|---|---|---|
| Total statements (`src/postoffice/`) | `1,498` | **`5,189`** (`4,889` covered, `300` missed) | `coverage.py` |
| Total branches (`src/postoffice/`) | `486` | **`1,582`** (`1,381` covered, `201` partial) | `coverage.py --branch` |
| Combined statement + branch coverage | `90.0%` | **`92.0%`** (exceeds `90.0%` gate) | `coverage.py --branch` |

### Per-Module Coverage Breakdown

| Module | Stmts | Miss | Branch | BrPart | Cover |
|---|---:|---:|---:|---:|---:|
| `src/postoffice/__init__.py` | 0 | 0 | 0 | 0 | **100%** |
| `src/postoffice/control_plane.py` | 92 | 0 | 16 | 0 | **100%** |
| `src/postoffice/ingress/__init__.py` | 7 | 0 | 0 | 0 | **100%** |
| `src/postoffice/ingress/content.py` | 89 | 0 | 44 | 0 | **100%** |
| `src/postoffice/ingress/retry.py` | 49 | 0 | 12 | 0 | **100%** |
| `src/postoffice/plugins/__init__.py` | 5 | 0 | 0 | 0 | **100%** |
| `src/postoffice/registry.py` | 22 | 0 | 2 | 0 | **100%** |
| `src/postoffice/schema/__init__.py` | 3 | 0 | 0 | 0 | **100%** |
| `src/postoffice/trie.py` | 70 | 0 | 34 | 0 | **100%** |
| `src/postoffice/ingress/ledger.py` | 150 | 1 | 48 | 1 | **99%** |
| `src/postoffice/ingress/concurrency.py` | 91 | 2 | 24 | 0 | **98%** |
| `src/postoffice/jsonscan.py` | 33 | 0 | 24 | 1 | **98%** |
| `src/postoffice/signing.py` | 73 | 2 | 18 | 0 | **98%** |
| `src/postoffice/base_client.py` | 159 | 5 | 48 | 2 | **97%** |
| `src/postoffice/dlq.py` | 99 | 2 | 32 | 2 | **97%** |
| `src/postoffice/schema/converters.py` | 99 | 1 | 44 | 3 | **97%** |
| `src/postoffice/validation.py` | 145 | 2 | 40 | 4 | **97%** |
| `src/postoffice/backoff.py` | 34 | 2 | 10 | 0 | **95%** |
| `src/postoffice/ratelimit.py` | 64 | 2 | 16 | 2 | **95%** |
| `src/postoffice/security.py` | 33 | 1 | 10 | 1 | **95%** |
| `src/postoffice/governor.py` | 118 | 6 | 52 | 5 | **94%** |
| `src/postoffice/logging_config.py` | 28 | 1 | 6 | 1 | **94%** |
| `src/postoffice/metrics.py` | 125 | 6 | 20 | 3 | **94%** |
| `src/postoffice/plugins/kafka_client.py` | 187 | 6 | 84 | 11 | **94%** |
| `src/postoffice/plugins/nanomq_client.py` | 16 | 0 | 2 | 1 | **94%** |
| `src/postoffice/resiliency.py` | 72 | 3 | 16 | 2 | **94%** |
| `src/postoffice/schema/limits.py` | 125 | 6 | 30 | 2 | **94%** |
| `src/postoffice/ingress/rules.py` | 126 | 7 | 50 | 6 | **93%** |
| `src/postoffice/data_plane.py` | 503 | 40 | 180 | 15 | **92%** |
| `src/postoffice/conformance.py` | 435 | 28 | 80 | 15 | **90%** |
| `src/postoffice/ingress/firewall.py` | 77 | 6 | 28 | 2 | **90%** |
| `src/postoffice/rollout.py` | 86 | 6 | 32 | 4 | **90%** |
| `src/postoffice/schema/registry.py` | 162 | 13 | 36 | 6 | **90%** |
| `src/postoffice/native.py` | 669 | 41 | 118 | 45 | **89%** |
| `src/postoffice/shadow.py` | 179 | 17 | 50 | 9 | **89%** |
| `src/postoffice/plugins/amqp_client.py` | 172 | 15 | 62 | 11 | **88%** |
| `src/postoffice/router.py` | 484 | 44 | 194 | 31 | **88%** |
| `src/postoffice/app.py` | 170 | 23 | 62 | 4 | **86%** |
| `src/postoffice/plugins/mqtt_client.py` | 124 | 10 | 54 | 10 | **85%** |
| `src/postoffice/worker.py` | 14 | 2 | 4 | 2 | **78%** |
| **TOTAL** | **5,189** | **300** | **1,582** | **201** | **92%** |

---

## 3. Verification Gate Commands

Run all three verification tiers locally:

```bash
# 1. Python hermetic unit suite + >= 90% branch & statement coverage gate
PYTHONPATH=src .venv/bin/coverage run -m unittest discover tests/ && .venv/bin/coverage report

# 2. Dual-engine 71-vector conformance suite (Python reference vs. Native Rust)
PYTHONPATH=src .venv/bin/python conformance/run.py --both

# 3. Native Rust unit and property/fuzz test suite
cargo test --release --manifest-path native/libpostoffice/Cargo.toml
```
