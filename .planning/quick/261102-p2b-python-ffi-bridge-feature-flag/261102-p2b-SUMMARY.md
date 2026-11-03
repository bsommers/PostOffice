---
status: complete
---
# Phase 2 slice P2-2: Python FFI Bridge (`src/postoffice/native.py`), `MemoryGovernor` (`src/postoffice/governor.py`), and `POSTOFFICE_NATIVE=1` Feature Flag

- Implemented `src/postoffice/native.py` FFI wrappers (`NativeTrie`, `NativeFirewall`, `NativeCodec`, `NativeClient`, `native_dlq_fingerprint`, `native_encode_confluent`, `native_decode_confluent`, `is_native_enabled`) over `libpostoffice.so`.
- Implemented `src/postoffice/governor.py` (`MemoryGovernor` and `detect_host_ram_bytes`) with Linux cgroup v2 (`memory.max`), cgroup v1 (`memory.limit_in_bytes`), and `/proc/meminfo` host RAM detection, 50% default budget, and 80% overload shedding in both Python and `POSTOFFICE_NATIVE=1` modes.
- Integrated `POSTOFFICE_NATIVE=1` behind `TopicTrie`, `Firewall.from_dict`, `dlq_fingerprint`, `encode_wire_format`, `decode_wire_format`, and `conformance/run.py --both` while keeping the Python path as the default (`POSTOFFICE_NATIVE=0` or unset).
- Verified all 71 conformance vectors pass in both `python` and `native` modes (`142/142` checks).
