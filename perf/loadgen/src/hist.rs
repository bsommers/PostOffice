//! Logarithmic HdrHistogram (sub-bucket base-2 latency histogram, 1 ns .. ~68 s)
//! and exact sample reservoir for small runs, with coordinated-omission awareness.

const SUB_BUCKET_BITS: u32 = 10; // 1024 sub-buckets per octave (~0.1% relative precision)
const SUB_BUCKET_COUNT: usize = 1 << SUB_BUCKET_BITS;
const SUB_BUCKET_MASK: u64 = (SUB_BUCKET_COUNT as u64) - 1;
const UNIT_MAGNITUDE: u32 = 36; // 2^36 ns ~= 68.7 seconds max tracked bucket

pub struct HdrHistogram {
    buckets: Vec<u64>,
    count: u64,
    min_ns: u64,
    max_ns: u64,
    sum_ns: u128,
}

impl Default for HdrHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl HdrHistogram {
    pub fn new() -> Self {
        let total_buckets = ((UNIT_MAGNITUDE - SUB_BUCKET_BITS + 2) as usize) * SUB_BUCKET_COUNT;
        Self {
            buckets: vec![0; total_buckets],
            count: 0,
            min_ns: u64::MAX,
            max_ns: 0,
            sum_ns: 0,
        }
    }

    #[inline]
    fn index_for(value: u64) -> usize {
        let clamped = value.min((1u64 << UNIT_MAGNITUDE) - 1);
        if clamped < SUB_BUCKET_COUNT as u64 {
            return clamped as usize;
        }
        let msb = 63 - clamped.leading_zeros();
        let octave = msb - SUB_BUCKET_BITS + 1;
        let sub = ((clamped >> (msb - SUB_BUCKET_BITS)) & SUB_BUCKET_MASK) as usize;
        (octave as usize) * SUB_BUCKET_COUNT + sub
    }

    #[inline]
    fn value_from_index(idx: usize) -> u64 {
        let octave = idx / SUB_BUCKET_COUNT;
        let sub = (idx % SUB_BUCKET_COUNT) as u64;
        if octave == 0 {
            sub
        } else {
            let shift = (octave as u32) - 1;
            ((1u64 << SUB_BUCKET_BITS) | sub) << shift
        }
    }

    pub fn record(&mut self, value_ns: u64) {
        let idx = Self::index_for(value_ns);
        self.buckets[idx] += 1;
        self.count += 1;
        self.min_ns = self.min_ns.min(value_ns);
        self.max_ns = self.max_ns.max(value_ns);
        self.sum_ns += value_ns as u128;
    }

    /// Coordinated-omission backfill: if a stall of `latency_ns` exceeds `expected_interval_ns`,
    /// also record the synthesized delayed arrivals at steps of `expected_interval_ns`.
    pub fn record_corrected(&mut self, latency_ns: u64, expected_interval_ns: u64) {
        self.record(latency_ns);
        if expected_interval_ns > 0 && latency_ns > expected_interval_ns {
            let mut step = latency_ns - expected_interval_ns;
            loop {
                self.record(step);
                if step < 2 * expected_interval_ns {
                    break;
                }
                step -= expected_interval_ns;
            }
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn max(&self) -> u64 {
        if self.count == 0 { 0 } else { self.max_ns }
    }

    pub fn min(&self) -> u64 {
        if self.count == 0 { 0 } else { self.min_ns }
    }

    pub fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        if p >= 100.0 {
            return self.max_ns;
        }
        let target = ((p / 100.0) * (self.count as f64)).ceil().max(1.0) as u64;
        let mut cumulative = 0u64;
        for (idx, &c) in self.buckets.iter().enumerate() {
            cumulative += c;
            if cumulative >= target {
                return Self::value_from_index(idx).min(self.max_ns);
            }
        }
        self.max_ns
    }

    pub fn summary_json(&self) -> String {
        if self.count == 0 {
            return "null".to_string();
        }
        format!(
            "{{\"p50\":{},\"p90\":{},\"p99\":{},\"p99.9\":{},\"max\":{}}}",
            self.percentile(50.0),
            self.percentile(90.0),
            self.percentile(99.0),
            self.percentile(99.9),
            self.max()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hdr_percentiles_and_precision() {
        let mut h = HdrHistogram::new();
        for v in 1..=100 {
            h.record(v * 1_000);
        }
        assert_eq!(h.count(), 100);
        assert_eq!(h.min(), 1_000);
        assert_eq!(h.max(), 100_000);
        let p50 = h.percentile(50.0);
        let p99 = h.percentile(99.0);
        assert!((49_900..=50_100).contains(&p50), "p50={p50}");
        assert!((98_800..=99_200).contains(&p99), "p99={p99}");
    }

    #[test]
    fn test_coordinated_omission_correction() {
        let mut raw = HdrHistogram::new();
        let mut corrected = HdrHistogram::new();
        let interval = 1_000_000; // 1 ms schedule
        for _ in 0..99 {
            raw.record(500_000);
            corrected.record_corrected(500_000, interval);
        }
        // One 20 ms stall (blocks 20 scheduled intervals)
        raw.record(20_000_000);
        corrected.record_corrected(20_000_000, interval);
        assert!(corrected.count() > raw.count());
        assert!(corrected.percentile(95.0) > raw.percentile(95.0));
    }
}
