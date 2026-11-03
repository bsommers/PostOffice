//! `MemoryGovernor` (`po_governor_t`) — atomic byte-accounted memory budget with cgroup-aware
//! host RAM detection and 80% overload shedding (`NATIVE_REWRITE_PLAN.md` §0.1 & §4 Phase 2).

use std::fs;
use std::sync::atomic::{AtomicU64, Ordering};

pub const DEFAULT_MEMORY_FRACTION: f64 = 0.50;
pub const DEFAULT_SHED_RATIO: f64 = 0.80;
const FALLBACK_RAM_BYTES: u64 = 1024 * 1024 * 1024; // 1 GiB conservative fallback

pub struct MemoryGovernor {
    budget_bytes: u64,
    shed_limit_bytes: u64,
    in_use_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    shed_total: AtomicU64,
}

impl MemoryGovernor {
    pub fn new(budget_bytes: u64, shed_ratio: f64) -> Result<Self, String> {
        if budget_bytes == 0 {
            return Err("budget_bytes must be > 0".to_string());
        }
        if !(0.0 < shed_ratio && shed_ratio <= 1.0) || shed_ratio.is_nan() {
            return Err("shed_ratio must be in (0.0, 1.0]".to_string());
        }
        let shed_limit_bytes = ((budget_bytes as f64) * shed_ratio).round() as u64;
        Ok(Self {
            budget_bytes,
            shed_limit_bytes: shed_limit_bytes.max(1),
            in_use_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            shed_total: AtomicU64::new(0),
        })
    }

    pub fn from_host_default() -> Self {
        let host_ram = detect_host_ram_bytes();
        let budget = ((host_ram as f64) * DEFAULT_MEMORY_FRACTION) as u64;
        Self::new(budget.max(4096), DEFAULT_SHED_RATIO).unwrap()
    }

    #[inline]
    pub fn try_acquire(&self, bytes: u64) -> bool {
        let mut cur = self.in_use_bytes.load(Ordering::Relaxed);
        loop {
            let Some(next) = cur.checked_add(bytes) else {
                self.shed_total.fetch_add(1, Ordering::Relaxed);
                return false;
            };
            if next > self.shed_limit_bytes {
                self.shed_total.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            match self.in_use_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.peak_bytes.fetch_max(next, Ordering::Relaxed);
                    return true;
                }
                Err(actual) => cur = actual,
            }
        }
    }

    #[inline]
    pub fn release(&self, bytes: u64) {
        let mut cur = self.in_use_bytes.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_sub(bytes);
            match self.in_use_bytes.compare_exchange_weak(
                cur,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(actual) => cur = actual,
            }
        }
    }

    #[inline]
    pub fn budget_bytes(&self) -> u64 {
        self.budget_bytes
    }

    #[inline]
    pub fn shed_limit_bytes(&self) -> u64 {
        self.shed_limit_bytes
    }

    #[inline]
    pub fn in_use_bytes(&self) -> u64 {
        self.in_use_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn peak_bytes(&self) -> u64 {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn shed_total(&self) -> u64 {
        self.shed_total.load(Ordering::Relaxed)
    }
}

/// Detects effective memory ceiling taking Linux cgroup v2 (`memory.max`),
/// cgroup v1 (`memory.limit_in_bytes`), and `/proc/meminfo` (`MemTotal`) into account.
pub fn detect_host_ram_bytes() -> u64 {
    let proc_mem = read_proc_meminfo_bytes().unwrap_or(FALLBACK_RAM_BYTES);
    let cgroup_limit = read_cgroup_limit_bytes();
    match cgroup_limit {
        Some(limit) if limit > 0 && limit < proc_mem => limit,
        _ => proc_mem,
    }
}

fn read_cgroup_limit_bytes() -> Option<u64> {
    for path in [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ] {
        if let Ok(raw) = fs::read_to_string(path) {
            let trimmed = raw.trim();
            if trimmed != "max" {
                if let Ok(val) = trimmed.parse::<u64>() {
                    // Ignore sentinel values near u64::MAX / page-aligned i64::MAX
                    if val < (1u64 << 60) {
                        return Some(val);
                    }
                }
            }
        }
    }
    None
}

fn read_proc_meminfo_bytes() -> Option<u64> {
    let content = fs::read_to_string("/proc/meminfo").ok()?;
    for line in content.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb_str = rest.split_whitespace().next()?;
            let kb = kb_str.parse::<u64>().ok()?;
            return Some(kb.saturating_mul(1024));
        }
    }
    None
}
