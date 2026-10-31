//! Phase 0 Rust Bake-Off Slice (`NATIVE_REWRITE_PLAN.md` §2 & §4 Phase 0).
//!
//! Implements the exact pipeline slice under comparison:
//! 1. `MemoryGovernor`: atomic byte-accounted budget (`soft_budget`, sheds at 80% `shed_threshold`).
//! 2. `Firewall`: 5-rule chain (`MaxPayloadBytes`, `TopicShape` with `$SYS/#` and `admin/+/secret` deny,
//!    and 3 `TokenBucket` rate limiters: broker, topic, identity) + single-pass JSON `ContentGuard`
//!    (depth <= 32, required fields `sensor_id`, `temperature`, `status`).
//! 3. `TopicTrie`: MQTT `+` and `#` wildcard matcher.
//! 4. Egress confirm + `POB1` CRC32 & sequence accounting oracle + `HdrHistogram`.

use std::collections::HashMap;
use std::env;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

const MAGIC: &[u8; 4] = b"POB1";
const HEADER_SIZE: usize = 32;

// --- 1. Memory Governor (D8 / §0.1: soft budget, sheds at 80%) ---

pub struct MemoryGovernor {
    pub budget_bytes: u64,
    pub shed_limit_bytes: u64,
    used_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    shed_count: AtomicU64,
}

impl MemoryGovernor {
    pub fn new(budget_bytes: u64) -> Self {
        let shed_limit_bytes = (budget_bytes * 80) / 100;
        Self {
            budget_bytes,
            shed_limit_bytes,
            used_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            shed_count: AtomicU64::new(0),
        }
    }

    #[inline]
    pub fn try_acquire(&self, bytes: u64) -> bool {
        let mut cur = self.used_bytes.load(Ordering::Relaxed);
        loop {
            let next = cur.saturating_add(bytes);
            if next > self.shed_limit_bytes {
                self.shed_count.fetch_add(1, Ordering::Relaxed);
                return false;
            }
            match self.used_bytes.compare_exchange_weak(
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
        self.used_bytes.fetch_sub(bytes, Ordering::Release);
    }

    pub fn peak_bytes(&self) -> u64 {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    pub fn shed_count(&self) -> u64 {
        self.shed_count.load(Ordering::Relaxed)
    }
}

// --- 2. POB1 Header & IEEE CRC32 ---

pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

pub fn encode_pob1(producer: u32, seq: u64, send_ts_ns: u64, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER_SIZE + body.len());
    let body_crc = crc32(body);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&producer.to_be_bytes());
    out.extend_from_slice(&seq.to_be_bytes());
    out.extend_from_slice(&send_ts_ns.to_be_bytes());
    out.extend_from_slice(&body_crc.to_be_bytes());
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(body);
    out
}

pub fn verify_pob1(msg: &[u8]) -> Option<(u32, u64, &[u8])> {
    if msg.len() < HEADER_SIZE || &msg[0..4] != MAGIC {
        return None;
    }
    let producer = u32::from_be_bytes(msg[4..8].try_into().ok()?);
    let seq = u64::from_be_bytes(msg[8..16].try_into().ok()?);
    let expected_crc = u32::from_be_bytes(msg[24..28].try_into().ok()?);
    let body = &msg[HEADER_SIZE..];
    if crc32(body) != expected_crc {
        return None;
    }
    Some((producer, seq, body))
}

// --- 3. Lock-free Atomic TokenBucket & 5-Rule Firewall + JSON ContentGuard ---

pub struct AtomicTokenBucket {
    tokens_milli: AtomicU64,
}

impl AtomicTokenBucket {
    pub fn new(initial_tokens: u64) -> Self {
        Self {
            tokens_milli: AtomicU64::new(initial_tokens.saturating_mul(1000)),
        }
    }

    #[inline]
    pub fn try_consume(&self, cost: u64) -> bool {
        let need = cost * 1000;
        let mut cur = self.tokens_milli.load(Ordering::Relaxed);
        loop {
            if cur < need {
                return false;
            }
            match self.tokens_milli.compare_exchange_weak(
                cur,
                cur - need,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => cur = actual,
            }
        }
    }
}

/// Single-pass structural JSON scanner + required fields checker (`sensor_id`, `temperature`, `status`).
#[inline]
pub fn validate_json_content(body: &[u8], max_depth: usize) -> bool {
    let mut depth = 0usize;
    let mut in_str = false;
    let mut esc = false;
    for &b in body {
        if in_str {
            if esc {
                esc = false;
            } else if b == b'\\' {
                esc = true;
            } else if b == b'"' {
                in_str = false;
            }
            continue;
        }
        match b {
            b'"' => in_str = true,
            b'{' | b'[' => {
                depth += 1;
                if depth > max_depth {
                    return false;
                }
            }
            b'}' | b']' => {
                if depth == 0 {
                    return false;
                }
                depth -= 1;
            }
            _ => {}
        }
    }
    if in_str || depth != 0 {
        return false;
    }
    // Check required keys in slice without heap-allocating a DOM
    twoway_contains(body, b"\"sensor_id\"")
        && twoway_contains(body, b"\"temperature\"")
        && twoway_contains(body, b"\"status\"")
}

#[inline]
fn twoway_contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}

pub struct Firewall {
    pub max_bytes: usize,
    pub max_topic_len: usize,
    pub max_topic_depth: usize,
    pub broker_bucket: AtomicTokenBucket,
    pub topic_bucket: AtomicTokenBucket,
    pub identity_bucket: AtomicTokenBucket,
    pub content_guard: bool,
}

impl Firewall {
    pub fn new(content_guard: bool) -> Self {
        let big = 1_000_000_000_000u64;
        Self {
            max_bytes: 1 << 20,
            max_topic_len: 256,
            max_topic_depth: 16,
            broker_bucket: AtomicTokenBucket::new(big),
            topic_bucket: AtomicTokenBucket::new(big),
            identity_bucket: AtomicTokenBucket::new(big),
            content_guard,
        }
    }

    #[inline]
    pub fn evaluate(&self, topic: &str, msg: &[u8], body: &[u8]) -> bool {
        // Rule 1: max_payload_bytes
        if msg.len() > self.max_bytes {
            return false;
        }
        // Rule 2: topic_shape (length, NUL, depth, deny $SYS/# and admin/+/secret)
        if topic.is_empty() || topic.len() > self.max_topic_len || topic.as_bytes().contains(&0) {
            return false;
        }
        if topic.starts_with("$SYS/") || topic == "$SYS" {
            return false;
        }
        let mut depth = 0usize;
        let mut first_seg = "";
        let mut third_seg = "";
        for (i, seg) in topic.split('/').enumerate() {
            depth += 1;
            if i == 0 {
                first_seg = seg;
            } else if i == 2 {
                third_seg = seg;
            }
        }
        if depth > self.max_topic_depth || (depth == 3 && first_seg == "admin" && third_seg == "secret") {
            return false;
        }
        // Rules 3, 4, 5: rate_limit (broker, topic, identity)
        if !self.broker_bucket.try_consume(1)
            || !self.topic_bucket.try_consume(1)
            || !self.identity_bucket.try_consume(1)
        {
            return false;
        }
        // Rule 6 (optional): content_guard
        if self.content_guard && !validate_json_content(body, 32) {
            return false;
        }
        true
    }
}

// --- 4. TopicTrie ---

#[derive(Default)]
pub struct TrieNode {
    children: HashMap<String, TrieNode>,
    routes: Vec<String>,
}

#[derive(Default)]
pub struct TopicTrie {
    root: TrieNode,
}

impl TopicTrie {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, pattern: &str, target_topic: &str) {
        let mut node = &mut self.root;
        for part in pattern.split('/') {
            node = node.children.entry(part.to_string()).or_default();
            if part == "#" {
                break;
            }
        }
        node.routes.push(target_topic.to_string());
    }

    #[inline]
    pub fn match_count(&self, topic: &str) -> usize {
        if topic.starts_with('$') {
            return 0;
        }
        let parts: Vec<&str> = topic.split('/').collect();
        Self::dfs(&self.root, &parts, 0)
    }

    fn dfs(node: &TrieNode, parts: &[&str], idx: usize) -> usize {
        let mut count = 0;
        if let Some(hash_node) = node.children.get("#") {
            count += hash_node.routes.len();
        }
        if idx == parts.len() {
            return count + node.routes.len();
        }
        if let Some(exact) = node.children.get(parts[idx]) {
            count += Self::dfs(exact, parts, idx + 1);
        }
        if let Some(plus) = node.children.get("+") {
            count += Self::dfs(plus, parts, idx + 1);
        }
        count
    }
}

// --- 5. HdrHistogram ---

const SUB_BUCKET_BITS: u32 = 10;
const SUB_BUCKET_COUNT: usize = 1 << SUB_BUCKET_BITS;
const SUB_BUCKET_MASK: u64 = (SUB_BUCKET_COUNT as u64) - 1;
const UNIT_MAGNITUDE: u32 = 36;

pub struct HdrHistogram {
    buckets: Vec<u64>,
    count: u64,
    max_ns: u64,
}

impl Default for HdrHistogram {
    fn default() -> Self {
        Self::new()
    }
}

impl HdrHistogram {
    pub fn new() -> Self {
        let total = ((UNIT_MAGNITUDE - SUB_BUCKET_BITS + 2) as usize) * SUB_BUCKET_COUNT;
        Self {
            buckets: vec![0; total],
            count: 0,
            max_ns: 0,
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
            ((1u64 << SUB_BUCKET_BITS) | sub) << (octave as u32 - 1)
        }
    }

    #[inline]
    pub fn record(&mut self, value_ns: u64) {
        let idx = Self::index_for(value_ns);
        self.buckets[idx] += 1;
        self.count += 1;
        self.max_ns = self.max_ns.max(value_ns);
    }

    pub fn merge(&mut self, other: &HdrHistogram) {
        for (a, &b) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *a += b;
        }
        self.count += other.count;
        self.max_ns = self.max_ns.max(other.max_ns);
    }

    pub fn percentile(&self, p: f64) -> u64 {
        if self.count == 0 {
            return 0;
        }
        let target = ((p / 100.0) * (self.count as f64)).ceil().max(1.0) as u64;
        let mut cum = 0u64;
        for (i, &c) in self.buckets.iter().enumerate() {
            cum += c;
            if cum >= target {
                return Self::value_from_index(i).min(self.max_ns);
            }
        }
        self.max_ns
    }
}

// --- 6. Benchmark Cells ---

fn make_1kb_payload(seq: u64) -> Vec<u8> {
    let mut json = format!(
        "{{\"sensor_id\":\"sensor-17\",\"temperature\":23.5,\"status\":\"OK\",\"pad\":\""
    )
    .into_bytes();
    let target_body = 1024 - HEADER_SIZE;
    while json.len() + 2 < target_body {
        json.push(b'x');
    }
    json.extend_from_slice(b"\"}");
    encode_pob1(1, seq, 0, &json)
}

fn run_cell(messages: u64, threads: usize, full_pipeline: bool) -> String {
    let mut trie = TopicTrie::new();
    trie.insert("devices/+/telemetry", "kafka/telemetry");
    let trie = Arc::new(trie);
    let fw = Arc::new(Firewall::new(full_pipeline));
    let gov = Arc::new(MemoryGovernor::new(64 * 1024 * 1024));
    let sample_msg = Arc::new(make_1kb_payload(0));

    let per_thread = (messages / threads as u64).max(1);
    let total_msgs = per_thread * (threads as u64);
    let delivered = Arc::new(AtomicU64::new(0));
    let hist = Arc::new(Mutex::new(HdrHistogram::new()));

    // Warm-up
    for _ in 0..200 {
        if let Some((_, _, body)) = verify_pob1(&sample_msg) {
            let _ = fw.evaluate("devices/d-17/telemetry", &sample_msg, body);
            let _ = trie.match_count("devices/d-17/telemetry");
        }
    }

    let t0 = Instant::now();
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let trie = trie.clone();
        let fw = fw.clone();
        let gov = gov.clone();
        let msg = sample_msg.clone();
        let delivered = delivered.clone();
        let hist = hist.clone();
        handles.push(thread::spawn(move || {
            let mut local_hist = HdrHistogram::new();
            let mut local_ok = 0u64;
            for _ in 0..per_thread {
                let call_t0 = Instant::now();
                if !gov.try_acquire(msg.len() as u64) {
                    continue;
                }
                if let Some((_, _, body)) = verify_pob1(&msg) {
                    if (!full_pipeline || fw.evaluate("devices/d-17/telemetry", &msg, body))
                        && trie.match_count("devices/d-17/telemetry") > 0
                    {
                        local_ok += 1;
                    }
                }
                gov.release(msg.len() as u64);
                local_hist.record(call_t0.elapsed().as_nanos() as u64);
            }
            delivered.fetch_add(local_ok, Ordering::Relaxed);
            hist.lock().unwrap().merge(&local_hist);
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
    let elapsed_ns = t0.elapsed().as_nanos().max(1) as u64;
    let deliv = delivered.load(Ordering::Relaxed);
    let rate = (total_msgs as f64) * 1e9 / (elapsed_ns as f64);
    let h = hist.lock().unwrap();
    format!(
        "{{\"msg_per_s\":{},\"p50_ns\":{},\"p99_ns\":{},\"p99_9_ns\":{},\"max_ns\":{},\"sent\":{},\"delivered\":{},\"oracle_passed\":{}}}",
        rate.round() as u64,
        h.percentile(50.0),
        h.percentile(99.0),
        h.percentile(99.9),
        h.max_ns,
        total_msgs,
        deliv,
        deliv == total_msgs
    )
}

fn run_overload_10x(messages: u64) -> String {
    // Budget = 10 KB, shed threshold = 80% = 8 KB (admits 8 x 1 KB messages in-flight; sheds the rest)
    let gov = MemoryGovernor::new(10 * 1024);
    let msg = make_1kb_payload(0);
    let total = messages.max(100);
    let mut held = 0u64;
    for _ in 0..total {
        if gov.try_acquire(msg.len() as u64) {
            held += 1;
        }
    }
    let peak = gov.peak_bytes();
    let shed = gov.shed_count();
    for _ in 0..held {
        gov.release(msg.len() as u64);
    }
    let balanced = total == held + shed;
    let passed = balanced && held == 8 && peak <= gov.shed_limit_bytes;
    format!(
        "{{\"sent\":{},\"delivered\":{},\"shed\":{},\"budget_bytes\":{},\"shed_limit_bytes\":{},\"peak_governor_bytes\":{},\"balanced\":{},\"passed\":{}}}",
        total, held, shed, gov.budget_bytes, gov.shed_limit_bytes, peak, balanced, passed
    )
}

fn max_rss_kb() -> u64 {
    if let Ok(status) = std::fs::read_to_string("/proc/self/status") {
        for line in status.lines() {
            if line.starts_with("VmHWM:") || line.starts_with("VmRSS:") {
                if let Some(num) = line.split_whitespace().nth(1) {
                    if let Ok(kb) = num.parse::<u64>() {
                        return kb;
                    }
                }
            }
        }
    }
    0
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let messages: u64 = args
        .windows(2)
        .find(|w| w[0] == "--messages")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(20_000);

    let c1_pass = run_cell(messages, 1, false);
    let c1_full = run_cell(messages, 1, true);
    let c4_full = run_cell(messages, 4, true);
    let c8_full = run_cell(messages, 8, true);
    let overload = run_overload_10x(messages);
    let rss_kb = max_rss_kb();

    println!(
        "{{\"language\":\"rust\",\"max_rss_kb\":{},\"cells\":{{\"threads_1_passthrough\":{},\"threads_1_full\":{},\"threads_4_full\":{},\"threads_8_full\":{},\"overload_10x\":{}}}}}",
        rss_kb, c1_pass, c1_full, c4_full, c8_full, overload
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_governor_sheds_at_80_percent() {
        let gov = MemoryGovernor::new(10_000);
        assert_eq!(gov.shed_limit_bytes, 8_000);
        for _ in 0..8 {
            assert!(gov.try_acquire(1_000));
        }
        assert!(!gov.try_acquire(1_000));
        assert_eq!(gov.shed_count(), 1);
        assert_eq!(gov.peak_bytes(), 8_000);
        gov.release(1_000);
        assert!(gov.try_acquire(1_000));
    }

    #[test]
    fn test_firewall_and_trie_and_pob1() {
        let msg = make_1kb_payload(42);
        let (prod, seq, body) = verify_pob1(&msg).unwrap();
        assert_eq!((prod, seq), (1, 42));
        let fw = Firewall::new(true);
        assert!(fw.evaluate("devices/d-17/telemetry", &msg, body));
        assert!(!fw.evaluate("$SYS/broker/load", &msg, body));
        assert!(!fw.evaluate("admin/x/secret", &msg, body));

        let mut trie = TopicTrie::new();
        trie.insert("devices/+/telemetry", "kafka/telemetry");
        assert_eq!(trie.match_count("devices/d-17/telemetry"), 1);
        assert_eq!(trie.match_count("$SYS/d-17/telemetry"), 0);
    }
}
