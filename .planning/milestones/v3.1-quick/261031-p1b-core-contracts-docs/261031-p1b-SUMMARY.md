---
status: complete
---
# Phase 1 slice P1-2: Freeze Core Contracts, MQTT `$` Topic Rule, `schema_version`, and `docs/contracts/`

- Updated `TopicTrie.match` (`src/postoffice/trie.py`) and `topic_matches` (`src/postoffice/ingress/rules.py`) to enforce MQTT v3.1.1/v5.0 §4.7.2: root wildcards `+` and `#` do not match `$`-prefixed topics, while explicit `$SYS/#` and `$SYS/+` patterns match.
- Added optional deterministic `timestamp` parameter to `format_dlq_payload` (`src/postoffice/dlq.py`).
- Added `EVENT_SCHEMA_VERSION = 1` support in `ControlPlane` (`src/postoffice/control_plane.py`) and strict `schema_version` validation in `DataPlane._admit` (`src/postoffice/data_plane.py`).
- Published frozen v1 contract specifications in `docs/contracts/` (`README.md`, `control-plane-redis.md`, `dlq-envelope.md`, `metrics.md`, `routing-and-protocols.md`, `firewall-schema.json`, `route-schema.json`).
- Verified with `tests/test_phase1_core_contracts.py`.
