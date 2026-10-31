//! Message-accounting oracle matching `perf/oracle.py`.
//!
//! Reconciles `sent == delivered + dlq + shed + dropped + in_flight`
//! and tracks `duplicated`, `reordered`, `corrupted`, `foreign`, `unexpected`, and latency.

use std::collections::{BTreeMap, BTreeSet};

use crate::header::{self, Header};
use crate::hist::HdrHistogram;

const LOST_SAMPLE_CAP: usize = 20;

#[derive(Default)]
pub struct Oracle {
    sent: BTreeMap<u32, BTreeSet<u64>>,
    seen: BTreeMap<u32, BTreeSet<u64>>,
    last_seq: BTreeMap<u32, u64>,
    pub latency: HdrHistogram,
    pub duplicated: u64,
    pub reordered: u64,
    pub corrupted: u64,
    pub foreign: u64,
    pub unexpected: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub sent: u64,
    pub delivered: u64,
    pub lost: u64,
    pub lost_sample: Vec<(u32, u64)>,
    pub dlq: u64,
    pub shed: u64,
    pub dropped: u64,
    pub in_flight: u64,
    pub duplicated: u64,
    pub reordered: u64,
    pub corrupted: u64,
    pub foreign: u64,
    pub unexpected: u64,
    pub balanced: bool,
    pub passed: bool,
    pub latency_json: String,
}

impl Report {
    pub fn to_json(&self) -> String {
        let sample_items: Vec<String> = self
            .lost_sample
            .iter()
            .map(|(p, s)| format!("[{p},{s}]"))
            .collect();
        format!(
            "{{\"sent\":{},\"delivered\":{},\"lost\":{},\"lost_sample\":[{}],\"accounted\":{{\"dlq\":{},\"shed\":{},\"dropped\":{},\"in_flight\":{}}},\"duplicated\":{},\"reordered\":{},\"corrupted\":{},\"foreign\":{},\"unexpected\":{},\"balanced\":{},\"passed\":{},\"latency_ns\":{}}}",
            self.sent,
            self.delivered,
            self.lost,
            sample_items.join(","),
            self.dlq,
            self.shed,
            self.dropped,
            self.in_flight,
            self.duplicated,
            self.reordered,
            self.corrupted,
            self.foreign,
            self.unexpected,
            self.balanced,
            self.passed,
            self.latency_json
        )
    }
}

impl Oracle {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn sent(&mut self, producer: u32, seq: u64) {
        self.sent.entry(producer).or_default().insert(seq);
    }

    pub fn sent_range(&mut self, producer: u32, count: u64) {
        let entry = self.sent.entry(producer).or_default();
        for s in 0..count {
            entry.insert(s);
        }
    }

    pub fn received(&mut self, message: &[u8], recv_ts_ns: u64) {
        let Some(Header {
            producer,
            seq,
            send_ts_ns,
            intact,
            ..
        }) = header::decode(message)
        else {
            self.foreign += 1;
            return;
        };
        if !intact {
            self.corrupted += 1;
            return;
        }
        let Some(sent_set) = self.sent.get(&producer) else {
            self.unexpected += 1;
            return;
        };
        if !sent_set.contains(&seq) {
            self.unexpected += 1;
            return;
        }
        let seen_set = self.seen.entry(producer).or_default();
        if !seen_set.insert(seq) {
            self.duplicated += 1;
            return;
        }
        if let Some(&last) = self.last_seq.get(&producer) {
            if seq < last {
                self.reordered += 1;
            } else {
                self.last_seq.insert(producer, seq);
            }
        } else {
            self.last_seq.insert(producer, seq);
        }
        let delta = recv_ts_ns.saturating_sub(send_ts_ns);
        self.latency.record(delta);
    }

    pub fn report(&self, dlq: u64, shed: u64, dropped: u64, in_flight: u64) -> Report {
        let sent: u64 = self.sent.values().map(|s| s.len() as u64).sum();
        let delivered: u64 = self.seen.values().map(|s| s.len() as u64).sum();
        let mut lost_sample = Vec::new();
        for (&producer, sent_set) in &self.sent {
            let seen_set = self.seen.get(&producer);
            for &seq in sent_set {
                let is_seen = seen_set.map_or(false, |s| s.contains(&seq));
                if !is_seen {
                    lost_sample.push((producer, seq));
                    if lost_sample.len() >= LOST_SAMPLE_CAP {
                        break;
                    }
                }
            }
            if lost_sample.len() >= LOST_SAMPLE_CAP {
                break;
            }
        }
        let balanced = sent == delivered + dlq + shed + dropped + in_flight;
        let passed = balanced && self.corrupted == 0 && self.foreign == 0 && self.unexpected == 0;
        Report {
            sent,
            delivered,
            lost: sent.saturating_sub(delivered),
            lost_sample,
            dlq,
            shed,
            dropped,
            in_flight,
            duplicated: self.duplicated,
            reordered: self.reordered,
            corrupted: self.corrupted,
            foreign: self.foreign,
            unexpected: self.unexpected,
            balanced,
            passed,
            latency_json: self.latency.summary_json(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_oracle_clean_and_loss() {
        let mut o = Oracle::new();
        o.sent_range(1, 5);
        for s in 0..5 {
            let msg = header::encode(1, s, 1000, b"x", None).unwrap();
            o.received(&msg, 1500);
        }
        let r = o.report(0, 0, 0, 0);
        assert!(r.balanced && r.passed);
        assert_eq!(r.delivered, 5);

        let mut o2 = Oracle::new();
        o2.sent_range(1, 3);
        let msg0 = header::encode(1, 0, 1000, b"x", None).unwrap();
        o2.received(&msg0, 1500);
        assert!(!o2.report(0, 0, 0, 0).passed);
        assert!(o2.report(1, 1, 0, 0).passed);
    }
}
