"""
Byte-accounted, cgroup-aware `MemoryGovernor` (`NATIVE_REWRITE_PLAN.md` §0.1 & §4 Phase 2).

Defaults to a soft budget of `0.5 * host RAM` (respecting Linux cgroup v2 `memory.max` and
cgroup v1 `memory.limit_in_bytes` when lower than `/proc/meminfo`) and sheds load at `80%`
of the budget so the process never OOMs under 10x overload.
When `POSTOFFICE_NATIVE=1` or `use_native=True`, delegates to `po_governor_t` in `libpostoffice`.
"""

import ctypes
import pathlib
import threading
from typing import Iterable, Optional

from postoffice.native import (
    PO_OK,
    get_libpostoffice,
    is_native_enabled,
)

DEFAULT_MEMORY_FRACTION = 0.50
DEFAULT_SHED_RATIO = 0.80
FALLBACK_RAM_BYTES = 1024 * 1024 * 1024  # 1 GiB
DEFAULT_CGROUP_PATHS = (
    "/sys/fs/cgroup/memory.max",
    "/sys/fs/cgroup/memory/memory.limit_in_bytes",
)


def detect_host_ram_bytes(
    cgroup_paths: Iterable[str] = DEFAULT_CGROUP_PATHS,
    meminfo_path: str = "/proc/meminfo",
) -> int:
    """Returns effective RAM ceiling in bytes, taking cgroup limits and `/proc/meminfo` into account."""
    proc_mem = _read_meminfo_bytes(meminfo_path) or FALLBACK_RAM_BYTES
    cg_limit = _read_cgroup_bytes(cgroup_paths)
    if cg_limit is not None and 0 < cg_limit < proc_mem:
        return cg_limit
    return proc_mem


def _read_cgroup_bytes(paths: Iterable[str]) -> Optional[int]:
    for p in paths:
        path = pathlib.Path(p)
        if not path.is_file():
            continue
        try:
            raw = path.read_text(encoding="utf-8").strip()
            if raw != "max":
                val = int(raw)
                if 0 < val < (1 << 60):
                    return val
        except (OSError, ValueError):
            continue
    return None


def _read_meminfo_bytes(meminfo_path: str) -> Optional[int]:
    path = pathlib.Path(meminfo_path)
    if not path.is_file():
        return None
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("MemTotal:"):
                parts = line.split()
                if len(parts) >= 2:
                    return int(parts[1]) * 1024
    except (OSError, ValueError):
        return None
    return None


class MemoryGovernor:
    """
    Thread-safe byte-accounted memory budget with 80% overload shedding.
    """

    def __init__(
        self,
        budget_bytes: Optional[int] = None,
        memory_fraction: float = DEFAULT_MEMORY_FRACTION,
        shed_ratio: float = DEFAULT_SHED_RATIO,
        cgroup_paths: Iterable[str] = DEFAULT_CGROUP_PATHS,
        use_native: Optional[bool] = None,
    ) -> None:
        if not (0.0 < memory_fraction <= 1.0):
            raise ValueError("memory_fraction must be in (0.0, 1.0]")
        if not (0.0 < shed_ratio <= 1.0):
            raise ValueError("shed_ratio must be in (0.0, 1.0]")

        if budget_bytes is None:
            host_ram = detect_host_ram_bytes(cgroup_paths=cgroup_paths)
            budget_bytes = max(4096, int(host_ram * memory_fraction))
        if budget_bytes <= 0:
            raise ValueError("budget_bytes must be > 0")

        self.budget_bytes = int(budget_bytes)
        self.shed_ratio = float(shed_ratio)
        self.shed_limit_bytes = max(1, int(round(self.budget_bytes * self.shed_ratio)))

        self._use_native = is_native_enabled() if use_native is None else bool(use_native)
        self._lock = threading.Lock()
        self._in_use = 0
        self._peak = 0
        self._shed_total = 0
        self._native_handle = ctypes.c_void_p()
        self._lib = None

        if self._use_native:
            self._lib = get_libpostoffice()
            rc = self._lib.po_governor_new(
                self.budget_bytes,
                self.shed_ratio,
                ctypes.byref(self._native_handle),
            )
            if rc != PO_OK:
                raise RuntimeError(f"po_governor_new failed with status {rc}")

    def __del__(self) -> None:
        handle = getattr(self, "_native_handle", None)
        lib = getattr(self, "_lib", None)
        if lib is not None and handle and handle.value:
            lib.po_governor_free(handle)
            self._native_handle = ctypes.c_void_p()

    def try_acquire(self, num_bytes: int) -> bool:
        if num_bytes < 0:
            raise ValueError("num_bytes must be >= 0")
        if self._use_native and self._lib is not None:
            rc = self._lib.po_governor_try_acquire(self._native_handle, int(num_bytes))
            if rc == PO_OK:
                cur = int(self._lib.po_governor_in_use_bytes(self._native_handle))
                if cur > self._peak:
                    self._peak = cur
                return True
            return False

        with self._lock:
            nxt = self._in_use + int(num_bytes)
            if nxt > self.shed_limit_bytes:
                self._shed_total += 1
                return False
            self._in_use = nxt
            if nxt > self._peak:
                self._peak = nxt
            return True

    def release(self, num_bytes: int) -> None:
        if num_bytes < 0:
            raise ValueError("num_bytes must be >= 0")
        if self._use_native and self._lib is not None:
            self._lib.po_governor_release(self._native_handle, int(num_bytes))
            return
        with self._lock:
            self._in_use = max(0, self._in_use - int(num_bytes))

    @property
    def in_use_bytes(self) -> int:
        if self._use_native and self._lib is not None:
            return int(self._lib.po_governor_in_use_bytes(self._native_handle))
        with self._lock:
            return self._in_use

    @property
    def peak_bytes(self) -> int:
        with self._lock:
            return self._peak

    @property
    def shed_total(self) -> int:
        if self._use_native and self._lib is not None:
            return int(self._lib.po_governor_shed_total(self._native_handle))
        with self._lock:
            return self._shed_total
