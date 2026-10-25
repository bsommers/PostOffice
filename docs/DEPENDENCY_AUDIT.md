# Dependency Security & Supply Chain Audit Report

**Date:** 2026-10-25
**Ecosystem:** Python (`requirements.txt`, `.venv` via uv)
**Total Dependencies Audited:** 8 direct (transitive not enumerated)
**Security Status:** No known vulnerabilities found in the local CVE index. A live scan was **not run**: `pip-audit` is not installed and was not installed without approval.

---

## 1. Vulnerability Summary (CVEs)
- Local index (`~/.claude/security/cve-db`) has no applicable entries for any of the 8 packages (only npm name collisions, e.g. protobufjs).
- Treat this as "not verified", not "clean". To confirm: `uv run --with pip-audit pip-audit -r requirements.txt`.

---

## 2. Outdated Dependencies & Migration Roadmap
| Package | Installed | Latest | SemVer Gap | Recommendation |
| :--- | :---: | :---: | :---: | :--- |
| `confluent-kafka` | 2.15.1 | 2.16.0 | Minor | Update, then rerun suite |
| `coverage` | 7.16.2 | 7.17.0 | Minor | Dev-only; safe to update |
| `paho-mqtt` | 2.1.0 | current | — | None |
| `pika` | 1.4.4 | not checked | — | Check upstream; 1.x line is slow-moving |
| `redis`, `fastavro`, `protobuf`, `prometheus-client` | 8.1.0 / 1.13.1 / 7.36.2 / 0.26.0 | current | — | None |

No package is more than one major version behind.

### Supply-chain hygiene (main finding)
- `requirements.txt` lists bare names: **no versions, no lockfile, no hashes**. Builds are not reproducible and a compromised or breaking release is picked up automatically.
- `coverage` (dev-only) is mixed into runtime requirements.
- No `pyproject.toml`, so the declared Python 3.12+ floor is not machine-enforced.
- Remediation: pin to the tested versions with compatible-release bounds, add a lockfile (`uv pip compile requirements.in -o requirements.txt --generate-hashes` or `uv lock`), split dev requirements, and run `pip-audit` in CI.

Suggested `requirements.txt`:
```
paho-mqtt>=2.1,<3
pika>=1.4.4,<2
confluent-kafka>=2.15.1,<3
redis>=8.1,<9
prometheus-client>=0.26,<1
fastavro>=1.13.1,<2
protobuf>=7.36.2,<8
```
plus `requirements-dev.txt` with `coverage`.

---

## 3. License Compliance Scorecard
| Package | License | Class |
| :--- | :--- | :--- |
| confluent-kafka | Apache-2.0 | Permissive |
| coverage | Apache-2.0 | Permissive |
| fastavro | MIT | Permissive |
| paho-mqtt | EPL-2.0 OR BSD-3-Clause | Permissive (choose BSD-3-Clause) |
| pika | BSD-3-Clause | Permissive |
| prometheus-client | Apache-2.0 AND BSD-2-Clause | Permissive |
| protobuf | BSD-3-Clause | Permissive |
| redis | MIT | Permissive |

No copyleft risk among direct dependencies. `confluent-kafka` bundles librdkafka (BSD-2-Clause); not separately verified.

## 4. Maintenance
All packages are actively released; none are archived. `pika` 1.x releases are infrequent, so watch it.
