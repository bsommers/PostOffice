---
status: complete
---
# Phase 2 slice P2-1: Rust `libpostoffice` Core & C ABI (`native/libpostoffice/`)

- Created `native/libpostoffice/` zero-dependency Rust crate (`cdylib`, `staticlib`, `rlib`) with `server` (default) and `edge` Cargo profiles.
- Implemented all `include/postoffice.h` C ABI entry points with panic containment (`catch_unwind`):
  - `TopicTrie` (`po_trie_*`) with MQTT `+`/`#` wildcards, `$` system topic isolation, and idempotent route management.
  - `MemoryGovernor` (`po_governor_*`) with atomic byte budget, 80% shed threshold, and cgroup v2/v1 + `/proc/meminfo` host RAM detection.
  - `Firewall` + `ContentGuard` + `SenderLedger` + `TokenBucket`/`KeyedLimiter` (`po_firewall_*`).
  - `Confluent` 5-byte wire framing, Avro/Protobuf binary schema codecs, and RFC 7693 `BLAKE2b-128` `po_dlq_fingerprint` (`po_codec_*`).
  - `po_client_*` C ABI handle and `po_buf_free` ownership contract.
- Verified 100% conformance vector pass and C99 binary linkage in `tests/test_phase2_c_abi.py`.
