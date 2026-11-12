---
status: complete
---
# Phase 1 slice P1-1: Polyglot Client Interface IDL, OpenAPI 3.1, and C ABI Header

- Authored `api/postoffice/v1/postoffice.proto` (Protobuf v3, `package postoffice.v1`) defining `AdminService` (12 RPCs), `ObserveService` (5 RPCs), and `StreamService` (2 RPCs) with additive-only v1 field numbering rules.
- Authored `api/openapi.json` (OpenAPI 3.1.0) mirroring all 19 Admin, Observe, and Stream operations over `/v1/admin/*`, `/v1/observe/*`, and `/v1/stream/*` with `bearerAuth` and `mtlsAuth` security schemes.
- Authored `include/postoffice.h` (`PO_ABI_VERSION == 1`) with opaque handles (`po_engine_t`, `po_trie_t`, `po_firewall_t`, `po_governor_t`, `po_codec_t`, `po_client_t`), `po_status_t`, `po_slice_t`, `po_owned_buf_t`, and zero-leak `po_buf_free` ownership semantics.
- Added `tests/test_phase1_idl_contracts.py` (5 hermetic tests including `gcc -std=c99` and `g++ -std=c++17` `-Wall -Wextra -Werror -pedantic` verification).
