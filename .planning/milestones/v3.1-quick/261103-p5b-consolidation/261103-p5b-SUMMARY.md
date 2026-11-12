# Summary 261103-p5b: Redis Streams Control-Plane Transport & Facade/Oracle Consolidation

- Added `postoffice:config_stream` (`XADD` / `XRANGE`) support to `src/postoffice/control_plane.py`, `src/postoffice/data_plane.py` (`replay_stream`), and `native/libpostoffice/src/redis_sync.rs` (`xrange_payloads`).
- Consolidated `PostOffice` facade (`src/postoffice/app.py`) with `use_native_engine=True` and `route_native(...)` while preserving the Python Data Plane as the executable reference oracle (`conformance/run.py --both`).
- Recorded ADR 0002 (`docs/adr/0002-phase5-consolidation.md`) and added 3 hermetic unit tests in `tests/test_phase5_consolidation.py`.
