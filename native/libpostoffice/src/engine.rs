//! Native Data Plane Engine: Router Core, BudgetedDlq, MetricsRegistry, and HTTP Endpoint.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::codec::{dlq_fingerprint, CodecRegistry};
use crate::firewall::{Action, Firewall, MessageContext, TokenBucket};
use crate::governor::MemoryGovernor;
use crate::json::{parse_json, scan_json, JsonValue};
use crate::signing::{escape_str_inner, extract_envelope, validate_action_json, HmacSigner};
use crate::trie::TopicTrie;

const BASE64_CHARS: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(((input.len() + 2) / 3) * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;

        out.push(BASE64_CHARS[((triple >> 18) & 0x3f) as usize] as char);
        out.push(BASE64_CHARS[((triple >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(BASE64_CHARS[((triple >> 6) & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
        if chunk.len() > 2 {
            out.push(BASE64_CHARS[(triple & 0x3f) as usize] as char);
        } else {
            out.push('=');
        }
    }
    out
}

pub fn format_dlq_payload(
    original_payload: &[u8],
    source_broker: &str,
    source_topic: &str,
    error: &str,
    metadata: Option<JsonValue>,
    timestamp: Option<&str>,
) -> Vec<u8> {
    let (payload_str, is_base64) = match std::str::from_utf8(original_payload) {
        Ok(s) => (s.to_string(), false),
        Err(_) => (base64_encode(original_payload), true),
    };
    let ts = timestamp.unwrap_or("2026-10-31T00:00:00+00:00");
    let meta_val = metadata.unwrap_or_else(|| JsonValue::Object(Vec::new()));

    JsonValue::Object(vec![
        (
            "source_broker".to_string(),
            JsonValue::Str(source_broker.to_string()),
        ),
        (
            "source_topic".to_string(),
            JsonValue::Str(source_topic.to_string()),
        ),
        ("timestamp".to_string(), JsonValue::Str(ts.to_string())),
        ("error".to_string(), JsonValue::Str(error.to_string())),
        ("payload".to_string(), JsonValue::Str(payload_str)),
        ("is_base64".to_string(), JsonValue::Bool(is_base64)),
        ("metadata".to_string(), meta_val),
    ])
    .to_json_string()
    .into_bytes()
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Admission {
    pub refusal: Option<String>,
    pub repeat_count: u64,
}

pub struct BudgetedDlq {
    pub max_message_bytes: usize,
    pub msgs_per_sec: f64,
    pub bytes_per_sec: f64,
    pub max_sources: usize,
    pub dedupe_window: Option<f64>,
    pub max_fingerprints: usize,
    seen_order: Vec<String>,
    seen: HashMap<String, (f64, u64)>,
    bucket_order: Vec<String>,
    buckets: HashMap<String, (TokenBucket, TokenBucket)>,
}

impl BudgetedDlq {
    pub fn new(
        max_message_bytes: usize,
        msgs_per_sec: f64,
        bytes_per_sec: f64,
        max_sources: usize,
        dedupe_window: Option<f64>,
        max_fingerprints: usize,
    ) -> Result<Self, String> {
        if max_message_bytes == 0 {
            return Err("max_message_bytes must be > 0".to_string());
        }
        if let Some(w) = dedupe_window {
            if w <= 0.0 {
                return Err("dedupe_window must be > 0".to_string());
            }
        }
        if max_fingerprints == 0 {
            return Err("max_fingerprints must be > 0".to_string());
        }
        Ok(Self {
            max_message_bytes,
            msgs_per_sec,
            bytes_per_sec,
            max_sources: max_sources.max(1),
            dedupe_window,
            max_fingerprints,
            seen_order: Vec::new(),
            seen: HashMap::new(),
            bucket_order: Vec::new(),
            buckets: HashMap::new(),
        })
    }

    pub fn default_budget() -> Self {
        Self::new(256 * 1024, 100.0, 1024.0 * 1024.0, 1024, None, 4096).unwrap()
    }

    fn buckets_for(&mut self, source_broker: &str, now_sec: f64) -> &mut (TokenBucket, TokenBucket) {
        if self.buckets.contains_key(source_broker) {
            if let Some(pos) = self.bucket_order.iter().position(|k| k == source_broker) {
                let k = self.bucket_order.remove(pos);
                self.bucket_order.push(k);
            }
        } else {
            let msg_b = TokenBucket::new(self.msgs_per_sec, self.msgs_per_sec, now_sec).unwrap();
            let byte_burst = self.bytes_per_sec.max(self.max_message_bytes as f64);
            let byte_b = TokenBucket::new(self.bytes_per_sec, byte_burst, now_sec).unwrap();
            self.buckets
                .insert(source_broker.to_string(), (msg_b, byte_b));
            self.bucket_order.push(source_broker.to_string());
            while self.bucket_order.len() > self.max_sources {
                let evicted = self.bucket_order.remove(0);
                self.buckets.remove(&evicted);
            }
        }
        self.buckets.get_mut(source_broker).unwrap()
    }

    pub fn admit(
        &mut self,
        source_broker: &str,
        size: usize,
        fingerprint: Option<&str>,
        metadata_only: bool,
        now_sec: f64,
    ) -> Admission {
        if size > self.max_message_bytes && !metadata_only {
            return Admission {
                refusal: Some("oversize".to_string()),
                repeat_count: 0,
            };
        }
        let mut repeat_count = 0u64;
        if let (Some(fp), Some(window)) = (fingerprint, self.dedupe_window) {
            if let Some(entry) = self.seen.get_mut(fp) {
                if now_sec - entry.0 <= window {
                    entry.1 += 1;
                    if let Some(pos) = self.seen_order.iter().position(|k| k == fp) {
                        let k = self.seen_order.remove(pos);
                        self.seen_order.push(k);
                    }
                    return Admission {
                        refusal: Some("duplicate".to_string()),
                        repeat_count: 0,
                    };
                }
                repeat_count = entry.1;
            }
        }

        let (msgs, byte_bucket) = self.buckets_for(source_broker, now_sec);
        if !msgs.can_consume(1.0, now_sec) {
            return Admission {
                refusal: Some("rate".to_string()),
                repeat_count: 0,
            };
        }
        if !byte_bucket.can_consume(size as f64, now_sec) {
            return Admission {
                refusal: Some("bytes".to_string()),
                repeat_count: 0,
            };
        }
        msgs.try_consume(1.0, now_sec);
        byte_bucket.try_consume(size as f64, now_sec);

        if let (Some(fp), Some(_)) = (fingerprint, self.dedupe_window) {
            self.seen.insert(fp.to_string(), (now_sec, 0));
            if let Some(pos) = self.seen_order.iter().position(|k| k == fp) {
                self.seen_order.remove(pos);
            }
            self.seen_order.push(fp.to_string());
            while self.seen_order.len() > self.max_fingerprints {
                let evicted = self.seen_order.remove(0);
                self.seen.remove(&evicted);
            }
        }

        Admission {
            refusal: None,
            repeat_count,
        }
    }
}

pub struct LabelGuard {
    max_values: usize,
    seen: HashSet<String>,
}

impl LabelGuard {
    pub const OVERFLOW: &'static str = "other";

    pub fn new(max_values: usize) -> Self {
        Self {
            max_values: max_values.max(1),
            seen: HashSet::new(),
        }
    }

    pub fn guard(&mut self, value: &str) -> String {
        if self.seen.contains(value) {
            return value.to_string();
        }
        if self.seen.len() < self.max_values {
            self.seen.insert(value.to_string());
            return value.to_string();
        }
        Self::OVERFLOW.to_string()
    }
}

pub struct MetricsRegistry {
    max_label_values: usize,
    guards: HashMap<String, LabelGuard>,
    pub messages_routed: BTreeMap<(String, String, String), u64>,
    pub routing_errors: BTreeMap<(String, String), u64>,
    pub dlq_messages: BTreeMap<(String, String, String), u64>,
    pub dlq_refused: BTreeMap<(String, String), u64>,
    pub ingress: BTreeMap<(String, String, String), u64>,
    pub sender_events: BTreeMap<(String, String), u64>,
    pub control_events: BTreeMap<String, u64>,
}

impl MetricsRegistry {
    pub fn new(max_label_values: usize) -> Self {
        Self {
            max_label_values: max_label_values.max(1),
            guards: HashMap::new(),
            messages_routed: BTreeMap::new(),
            routing_errors: BTreeMap::new(),
            dlq_messages: BTreeMap::new(),
            dlq_refused: BTreeMap::new(),
            ingress: BTreeMap::new(),
            sender_events: BTreeMap::new(),
            control_events: BTreeMap::new(),
        }
    }

    fn label(&mut self, dim: &str, val: &str) -> String {
        let max_v = self.max_label_values;
        let guard = self
            .guards
            .entry(dim.to_string())
            .or_insert_with(|| LabelGuard::new(max_v));
        guard.guard(val)
    }

    pub fn record_routed(&mut self, source_broker: &str, target_broker: &str, status: &str) {
        let sb = self.label("source_broker", source_broker);
        let tb = self.label("target_broker", target_broker);
        *self
            .messages_routed
            .entry((sb, tb, status.to_string()))
            .or_insert(0) += 1;
    }

    pub fn record_error(&mut self, source_broker: &str, error_type: &str) {
        let sb = self.label("source_broker", source_broker);
        *self
            .routing_errors
            .entry((sb, error_type.to_string()))
            .or_insert(0) += 1;
    }

    pub fn record_dlq(&mut self, source_broker: &str, dlq_broker: &str, dlq_topic: &str) {
        let sb = self.label("source_broker", source_broker);
        let db = self.label("dlq_broker", dlq_broker);
        let dt = self.label("dlq_topic", dlq_topic);
        *self.dlq_messages.entry((sb, db, dt)).or_insert(0) += 1;
    }

    pub fn record_dlq_refused(&mut self, source_broker: &str, reason: &str) {
        let sb = self.label("source_broker", source_broker);
        *self
            .dlq_refused
            .entry((sb, reason.to_string()))
            .or_insert(0) += 1;
    }

    pub fn record_ingress(&mut self, source_broker: &str, rule: &str, action: &str) {
        let sb = self.label("source_broker", source_broker);
        *self
            .ingress
            .entry((sb, rule.to_string(), action.to_string()))
            .or_insert(0) += 1;
    }

    pub fn record_sender_event(&mut self, source_broker: &str, category: &str) {
        let sb = self.label("source_broker", source_broker);
        *self
            .sender_events
            .entry((sb, category.to_string()))
            .or_insert(0) += 1;
    }

    pub fn record_control_event(&mut self, result: &str) {
        *self.control_events.entry(result.to_string()).or_insert(0) += 1;
    }

    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        out.push_str("# HELP postoffice_messages_routed_total Total number of messages routed through PostOffice\n");
        out.push_str("# TYPE postoffice_messages_routed_total counter\n");
        for ((sb, tb, st), count) in &self.messages_routed {
            out.push_str(&format!(
                "postoffice_messages_routed_total{{source_broker=\"{}\",status=\"{}\",target_broker=\"{}\"}} {}.0\n",
                escape_str_inner(sb),
                escape_str_inner(st),
                escape_str_inner(tb),
                count
            ));
        }

        out.push_str("# HELP postoffice_routing_errors_total Total number of routing and delivery errors encountered\n");
        out.push_str("# TYPE postoffice_routing_errors_total counter\n");
        for ((sb, err), count) in &self.routing_errors {
            out.push_str(&format!(
                "postoffice_routing_errors_total{{error_type=\"{}\",source_broker=\"{}\"}} {}.0\n",
                escape_str_inner(err),
                escape_str_inner(sb),
                count
            ));
        }

        out.push_str("# HELP postoffice_dlq_messages_total Total number of messages routed to a Dead Letter Queue\n");
        out.push_str("# TYPE postoffice_dlq_messages_total counter\n");
        for ((sb, db, dt), count) in &self.dlq_messages {
            out.push_str(&format!(
                "postoffice_dlq_messages_total{{dlq_broker=\"{}\",dlq_topic=\"{}\",source_broker=\"{}\"}} {}.0\n",
                escape_str_inner(db),
                escape_str_inner(dt),
                escape_str_inner(sb),
                count
            ));
        }

        out.push_str("# HELP postoffice_dlq_refused_total Total number of Dead Letter Queue writes refused by the DLQ budget\n");
        out.push_str("# TYPE postoffice_dlq_refused_total counter\n");
        for ((sb, reason), count) in &self.dlq_refused {
            out.push_str(&format!(
                "postoffice_dlq_refused_total{{reason=\"{}\",source_broker=\"{}\"}} {}.0\n",
                escape_str_inner(reason),
                escape_str_inner(sb),
                count
            ));
        }

        out.push_str("# HELP postoffice_ingress_total Messages stopped by the ingress firewall\n");
        out.push_str("# TYPE postoffice_ingress_total counter\n");
        for ((sb, rule, act), count) in &self.ingress {
            out.push_str(&format!(
                "postoffice_ingress_total{{action=\"{}\",rule=\"{}\",source_broker=\"{}\"}} {}.0\n",
                escape_str_inner(act),
                escape_str_inner(rule),
                escape_str_inner(sb),
                count
            ));
        }

        out.push_str("# HELP postoffice_control_events_total Control Plane events and state entries seen by a Data Plane worker, by outcome\n");
        out.push_str("# TYPE postoffice_control_events_total counter\n");
        for (res, count) in &self.control_events {
            out.push_str(&format!(
                "postoffice_control_events_total{{result=\"{}\"}} {}.0\n",
                escape_str_inner(res),
                count
            ));
        }

        out
    }
}

#[derive(Clone, Debug)]
pub struct BrokerRegistration {
    pub name: String,
    pub protocol: String,
    pub kwargs: Vec<(String, JsonValue)>,
    pub connected: bool,
    pub fail_publish: bool,
}

pub struct EngineState {
    pub brokers: BTreeMap<String, BrokerRegistration>,
    pub tries: HashMap<String, TopicTrie>,
    pub routes_count: usize,
    pub subscriptions: BTreeMap<String, JsonValue>,
    pub firewall: Option<Firewall>,
    pub codec: CodecRegistry,
    pub governor: MemoryGovernor,
    pub dlq_budget: BudgetedDlq,
    pub default_dlq_broker: Option<String>,
    pub default_dlq_topic: Option<String>,
    pub unroutable_policy: String,
    pub unroutable_overrides: HashMap<String, String>,
    pub metrics: MetricsRegistry,
    pub applied_version: Option<i64>,
    pub signer: Option<HmacSigner>,
    pub signing_mode: String,
    pub allow_clear_state: bool,
    pub max_event_bytes: usize,
    pub event_bucket: TokenBucket,
    pub needs_resync: bool,
    pub worker_engine: String,
    pub native_percentage: u8,
}

pub fn canary_bucket(source_broker: &str, source_topic: &str, payload: &[u8]) -> u8 {
    let mut msg = Vec::with_capacity(9 + source_broker.len() + source_topic.len() + payload.len());
    msg.extend_from_slice(b"canary\x00");
    msg.extend_from_slice(source_broker.as_bytes());
    msg.push(0);
    msg.extend_from_slice(source_topic.as_bytes());
    msg.push(0);
    msg.extend_from_slice(payload);
    let digest = crate::signing::sha256(&msg);
    let val = u64::from_be_bytes([
        digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
    ]);
    (val % 100) as u8
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FilterTarget {
    Payload,
    Kwargs,
    Headers,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum FilterStep {
    Key(String),
    Index(usize),
}

fn parse_filter_path(path: &str) -> Result<(FilterTarget, Vec<FilterStep>), String> {
    if path.is_empty() || path.len() > 256 {
        return Err("filter path must be a non-empty string (<= 256 chars)".to_string());
    }

    let (target, rel) = if let Some(rest) = path.strip_prefix("$kwargs.") {
        (FilterTarget::Kwargs, rest)
    } else if let Some(rest) = path.strip_prefix("$headers.") {
        (FilterTarget::Headers, rest)
    } else if let Some(rest) = path.strip_prefix("$.") {
        (FilterTarget::Payload, rest)
    } else if path.starts_with("$[") {
        (FilterTarget::Payload, &path[1..])
    } else if path.starts_with('$') {
        return Err(format!("unsupported filter path root in '{path}'"));
    } else {
        (FilterTarget::Payload, path)
    };

    if rel.is_empty() || rel.starts_with('.') || rel.ends_with('.') || rel.contains("..") {
        return Err(format!("invalid filter path syntax: '{path}'"));
    }

    let mut steps = Vec::new();
    for token in rel.split('.') {
        if token.is_empty() {
            return Err(format!("empty path segment in '{path}'"));
        }
        if !token.contains('[') && !token.contains(']') {
            steps.push(FilterStep::Key(token.to_string()));
            continue;
        }
        let Some(bracket_pos) = token.find('[') else {
            return Err(format!("malformed bracket in path segment '{token}'"));
        };
        if token[..bracket_pos].contains(']') {
            return Err(format!("malformed bracket in path segment '{token}'"));
        }
        let prefix = &token[..bracket_pos];
        if !prefix.is_empty() {
            steps.push(FilterStep::Key(prefix.to_string()));
        }

        let bytes = token.as_bytes();
        let mut pos = bracket_pos;
        let n = bytes.len();
        while pos < n {
            if bytes[pos] != b'[' {
                return Err(format!("unexpected characters after ']' in '{token}'"));
            }
            let Some(rel_close) = token[pos + 1..].find(']') else {
                return Err(format!("unclosed '[' in '{token}'"));
            };
            let close_pos = pos + 1 + rel_close;
            let idx_str = &token[pos + 1..close_pos];
            if idx_str.is_empty() || !idx_str.bytes().all(|b| b.is_ascii_digit()) {
                return Err(format!(
                    "array index must be non-negative digits in '{token}'"
                ));
            }
            let idx: usize = idx_str
                .parse()
                .map_err(|_| format!("invalid array index in '{token}'"))?;
            steps.push(FilterStep::Index(idx));
            pos = close_pos + 1;
        }
    }

    if steps.is_empty() {
        return Err(format!("filter path produced no lookup steps: '{path}'"));
    }
    Ok((target, steps))
}

fn is_valid_filter_op(op: &str) -> bool {
    matches!(
        op,
        "==" | "eq"
            | "!="
            | "ne"
            | ">"
            | "gt"
            | ">="
            | "gte"
            | "<"
            | "lt"
            | "<="
            | "lte"
            | "in"
            | "contains"
            | "exists"
            | "prefix"
            | "startswith"
            | "suffix"
            | "endswith"
    )
}

fn is_ordering_op(op: &str) -> bool {
    matches!(op, ">" | "gt" | ">=" | "gte" | "<" | "lt" | "<=" | "lte")
}

fn is_prefix_suffix_op(op: &str) -> bool {
    matches!(op, "prefix" | "startswith" | "suffix" | "endswith")
}

pub fn validate_filter_json(spec: &JsonValue, max_depth: usize) -> Result<(), String> {
    if max_depth < 1 {
        return Err("filter nesting exceeds max_depth".to_string());
    }
    let Some(entries) = spec.as_object() else {
        return Err("filter spec must be a non-empty object".to_string());
    };
    if entries.is_empty() {
        return Err("filter spec must be a non-empty object".to_string());
    }

    let has_comb = entries
        .iter()
        .any(|(k, _)| matches!(k.as_str(), "all" | "any" | "not"));
    if has_comb {
        if entries.len() != 1 {
            return Err("combinator filter must contain exactly one key".to_string());
        }
        let (key, sub) = &entries[0];
        match key.as_str() {
            "all" | "any" => {
                let Some(arr) = sub.as_array() else {
                    return Err(format!(
                        "'{key}' combinator must be a non-empty list (<= 64 items)"
                    ));
                };
                if arr.is_empty() || arr.len() > 64 {
                    return Err(format!(
                        "'{key}' combinator must be a non-empty list (<= 64 items)"
                    ));
                }
                for item in arr {
                    validate_filter_json(item, max_depth - 1)?;
                }
                return Ok(());
            }
            "not" => {
                return validate_filter_json(sub, max_depth - 1);
            }
            _ => unreachable!(),
        }
    }

    for (k, _) in entries {
        if !matches!(k.as_str(), "path" | "op" | "value") {
            return Err("filter predicate contains unknown keys".to_string());
        }
    }

    let Some(path) = spec.get("path").and_then(|v| v.as_str()) else {
        return Err("filter predicate requires 'path' and 'op'".to_string());
    };
    let Some(op) = spec.get("op").and_then(|v| v.as_str()) else {
        return Err("filter predicate requires 'path' and 'op'".to_string());
    };

    parse_filter_path(path)?;
    if !is_valid_filter_op(op) {
        return Err(format!("unsupported filter operator: '{op}'"));
    }

    if op == "exists" {
        if let Some(val) = spec.get("value") {
            if val.as_bool().is_none() {
                return Err("'exists' operator requires a boolean value".to_string());
            }
        }
        return Ok(());
    }

    let Some(val) = spec.get("value") else {
        return Err(format!("operator '{op}' requires a 'value'"));
    };

    if is_ordering_op(op) {
        if !matches!(val, JsonValue::Number(_, _) | JsonValue::Str(_)) {
            return Err(format!(
                "operator '{op}' requires a numeric or string value"
            ));
        }
    } else if op == "in" {
        if !matches!(val, JsonValue::Array(_) | JsonValue::Str(_)) {
            return Err("operator 'in' requires a list or string value".to_string());
        }
    } else if is_prefix_suffix_op(op) && !matches!(val, JsonValue::Str(_)) {
        return Err(format!("operator '{op}' requires a string value"));
    }

    Ok(())
}

fn json_eq(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Null, JsonValue::Null) => true,
        (JsonValue::Bool(x), JsonValue::Bool(y)) => x == y,
        (JsonValue::Number(x, _), JsonValue::Number(y, _)) => x == y,
        (JsonValue::Str(x), JsonValue::Str(y)) => x == y,
        (JsonValue::Array(x), JsonValue::Array(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(xi, yi)| json_eq(xi, yi))
        }
        (JsonValue::Object(x), JsonValue::Object(y)) => {
            if x.len() != y.len() {
                return false;
            }
            for (k, xv) in x {
                let Some(yv) = b.get(k) else {
                    return false;
                };
                if !json_eq(xv, yv) {
                    return false;
                }
            }
            true
        }
        _ => false,
    }
}

fn resolve_filter_steps<'a>(root: &'a JsonValue, steps: &[FilterStep]) -> Option<&'a JsonValue> {
    let mut cur = root;
    for step in steps {
        match step {
            FilterStep::Key(k) => {
                cur = cur.get(k)?;
            }
            FilterStep::Index(idx) => {
                let arr = cur.as_array()?;
                cur = arr.get(*idx)?;
            }
        }
    }
    Some(cur)
}

fn eval_filter_op(op: &str, extracted: Option<&JsonValue>, expected: Option<&JsonValue>) -> bool {
    if op == "exists" {
        let want = expected.and_then(|v| v.as_bool()).unwrap_or(true);
        let present = matches!(extracted, Some(v) if !matches!(v, JsonValue::Null));
        return present == want;
    }

    let (Some(ext), Some(exp)) = (extracted, expected) else {
        return false;
    };

    if matches!(op, "==" | "eq") {
        return json_eq(ext, exp);
    }
    if matches!(op, "!=" | "ne") {
        return !json_eq(ext, exp);
    }

    if is_ordering_op(op) {
        match (ext, exp) {
            (JsonValue::Number(lhs, _), JsonValue::Number(rhs, _)) => {
                return match op {
                    ">" | "gt" => lhs > rhs,
                    ">=" | "gte" => lhs >= rhs,
                    "<" | "lt" => lhs < rhs,
                    _ => lhs <= rhs,
                };
            }
            (JsonValue::Str(lhs), JsonValue::Str(rhs)) => {
                return match op {
                    ">" | "gt" => lhs > rhs,
                    ">=" | "gte" => lhs >= rhs,
                    "<" | "lt" => lhs < rhs,
                    _ => lhs <= rhs,
                };
            }
            _ => return false,
        }
    }

    if op == "in" {
        return match exp {
            JsonValue::Array(items) => items.iter().any(|item| json_eq(ext, item)),
            JsonValue::Str(haystack) => {
                if let Some(needle) = ext.as_str() {
                    haystack.contains(needle)
                } else {
                    false
                }
            }
            _ => false,
        };
    }

    if op == "contains" {
        return match ext {
            JsonValue::Array(items) => items.iter().any(|item| json_eq(item, exp)),
            JsonValue::Str(haystack) => {
                if let Some(needle) = exp.as_str() {
                    haystack.contains(needle)
                } else {
                    false
                }
            }
            _ => false,
        };
    }

    if matches!(op, "prefix" | "startswith") {
        return match (ext.as_str(), exp.as_str()) {
            (Some(s), Some(p)) => s.starts_with(p),
            _ => false,
        };
    }

    if matches!(op, "suffix" | "endswith") {
        return match (ext.as_str(), exp.as_str()) {
            (Some(s), Some(sfx)) => s.ends_with(sfx),
            _ => false,
        };
    }

    false
}

enum LazyPayload<'a> {
    Unparsed(&'a [u8]),
    Invalid,
    Valid(JsonValue),
}

impl<'a> LazyPayload<'a> {
    fn get(&mut self) -> Option<&JsonValue> {
        if let LazyPayload::Unparsed(raw) = *self {
            let parsed = std::str::from_utf8(raw)
                .ok()
                .and_then(|s| parse_json(s).ok())
                .filter(|v| matches!(v, JsonValue::Object(_) | JsonValue::Array(_)));
            *self = match parsed {
                Some(v) => LazyPayload::Valid(v),
                None => LazyPayload::Invalid,
            };
        }
        match self {
            LazyPayload::Valid(ref v) => Some(v),
            _ => None,
        }
    }
}

fn evaluate_filter_inner(
    spec: &JsonValue,
    cache: &mut LazyPayload<'_>,
    source_kwargs: &JsonValue,
    depth: usize,
) -> bool {
    if depth == 0 {
        return false;
    }
    let Some(entries) = spec.as_object() else {
        return false;
    };
    if entries.is_empty() {
        return false;
    }

    if entries.len() == 1 {
        let (k, sub) = &entries[0];
        if k == "all" {
            let Some(arr) = sub.as_array() else {
                return false;
            };
            if arr.is_empty() {
                return false;
            }
            return arr
                .iter()
                .all(|item| evaluate_filter_inner(item, cache, source_kwargs, depth - 1));
        }
        if k == "any" {
            let Some(arr) = sub.as_array() else {
                return false;
            };
            if arr.is_empty() {
                return false;
            }
            return arr
                .iter()
                .any(|item| evaluate_filter_inner(item, cache, source_kwargs, depth - 1));
        }
        if k == "not" {
            return !evaluate_filter_inner(sub, cache, source_kwargs, depth - 1);
        }
    }

    let Some(path) = spec.get("path").and_then(|v| v.as_str()) else {
        return false;
    };
    let Some(op) = spec.get("op").and_then(|v| v.as_str()) else {
        return false;
    };
    if !is_valid_filter_op(op) {
        return false;
    }
    let expected = spec.get("value");
    if op != "exists" && expected.is_none() {
        return false;
    }

    let Ok((target, steps)) = parse_filter_path(path) else {
        return false;
    };

    let extracted = match target {
        FilterTarget::Payload => {
            let Some(root) = cache.get() else {
                return false;
            };
            resolve_filter_steps(root, &steps)
        }
        FilterTarget::Kwargs => resolve_filter_steps(source_kwargs, &steps),
        FilterTarget::Headers => {
            if let Some(headers) = source_kwargs.get("headers") {
                if headers.as_object().is_some() {
                    resolve_filter_steps(headers, &steps)
                } else {
                    None
                }
            } else {
                None
            }
        }
    };

    eval_filter_op(op, extracted, expected)
}

pub fn evaluate_filter(spec: &JsonValue, payload: &[u8], source_kwargs: &JsonValue) -> bool {
    if matches!(spec, JsonValue::Null) {
        return true;
    }
    let mut cache = LazyPayload::Unparsed(payload);
    evaluate_filter_inner(spec, &mut cache, source_kwargs, 8)
}

impl EngineState {
    pub fn new(max_label_values: usize) -> Self {
        let host_ram = crate::governor::detect_host_ram_bytes();
        let budget = (host_ram / 2).max(64 * 1024 * 1024);
        Self {
            brokers: BTreeMap::new(),
            tries: HashMap::new(),
            routes_count: 0,
            subscriptions: BTreeMap::new(),
            firewall: None,
            codec: CodecRegistry::new(),
            governor: MemoryGovernor::new(budget, 0.80).unwrap(),
            dlq_budget: BudgetedDlq::default_budget(),
            default_dlq_broker: None,
            default_dlq_topic: None,
            unroutable_policy: "nack".to_string(),
            unroutable_overrides: HashMap::new(),
            metrics: MetricsRegistry::new(max_label_values),
            applied_version: None,
            signer: None,
            signing_mode: "off".to_string(),
            allow_clear_state: false,
            max_event_bytes: 256 * 1024,
            event_bucket: TokenBucket::new(20.0, 100.0, 0.0).unwrap(),
            needs_resync: false,
            worker_engine: "native".to_string(),
            native_percentage: 100,
        }
    }

    pub fn route_matches_engine(
        &self,
        route_cfg: &JsonValue,
        source_broker: &str,
        source_topic: &str,
        payload: &[u8],
    ) -> bool {
        let route_engine = route_cfg
            .get("engine")
            .and_then(|v| v.as_str())
            .unwrap_or("all");
        if route_engine == "all" || self.worker_engine == "all" {
            return true;
        }
        if route_engine == "python" || route_engine == "native" {
            return route_engine == self.worker_engine;
        }
        if route_engine == "canary" {
            let bucket = canary_bucket(source_broker, source_topic, payload);
            if self.worker_engine == "native" {
                return bucket < self.native_percentage;
            }
            if self.worker_engine == "python" {
                return bucket >= self.native_percentage;
            }
        }
        true
    }

    pub fn route_matches_filter(
        &self,
        route_cfg: &JsonValue,
        payload: &[u8],
        source_kwargs: &JsonValue,
    ) -> bool {
        match route_cfg.get("filter") {
            None | Some(JsonValue::Null) => true,
            Some(spec) => evaluate_filter(spec, payload, source_kwargs),
        }
    }

    pub fn register_broker(
        &mut self,
        name: &str,
        protocol: &str,
        kwargs: Vec<(String, JsonValue)>,
    ) {
        let fail_publish = kwargs
            .iter()
            .find(|(k, _)| k == "fail_publish")
            .and_then(|(_, v)| v.as_bool())
            .unwrap_or(false);
        self.brokers.insert(
            name.to_string(),
            BrokerRegistration {
                name: name.to_string(),
                protocol: protocol.to_string(),
                kwargs,
                connected: true,
                fail_publish,
            },
        );
    }

    pub fn add_route(
        &mut self,
        source_broker: &str,
        source_topic: &str,
        target_broker: &str,
        target_topic: &str,
        target_kwargs: Vec<(String, JsonValue)>,
    ) -> Result<bool, String> {
        for (k, v) in &target_kwargs {
            if k == "filter" && !matches!(v, JsonValue::Null) {
                validate_filter_json(v, 8)?;
            }
        }
        let mut route_entries = vec![
            (
                "target_client".to_string(),
                JsonValue::Str(target_broker.to_string()),
            ),
            (
                "target_topic".to_string(),
                JsonValue::Str(target_topic.to_string()),
            ),
        ];
        for (k, v) in target_kwargs {
            route_entries.push((k, v));
        }
        let route_json = JsonValue::Object(route_entries).to_json_string();
        let trie = self
            .tries
            .entry(source_broker.to_string())
            .or_insert_with(TopicTrie::new);
        let added = trie.add_route(source_topic, &route_json, true)?;
        if added {
            self.routes_count += 1;
        }
        Ok(added)
    }

    pub fn set_unroutable_policy(
        &mut self,
        policy: &str,
        broker: Option<&str>,
    ) -> Result<(), String> {
        if !matches!(policy, "nack" | "dlq" | "drop") {
            return Err(format!("Invalid unroutable policy '{policy}'"));
        }
        if let Some(b) = broker {
            self.unroutable_overrides
                .insert(b.to_string(), policy.to_string());
        } else {
            self.unroutable_policy = policy.to_string();
        }
        Ok(())
    }

    pub fn set_dlq(&mut self, broker: &str, topic: &str) {
        self.default_dlq_broker = Some(broker.to_string());
        self.default_dlq_topic = Some(topic.to_string());
    }

    #[allow(clippy::too_many_arguments)]
    fn route_to_dlq(
        &mut self,
        source_broker: &str,
        source_topic: &str,
        payload: &[u8],
        error: &str,
        dlq_broker: Option<&str>,
        dlq_topic: Option<&str>,
        now_sec: f64,
        dlq_published: &mut Vec<JsonValue>,
    ) -> Result<bool, bool> {
        let target_broker = dlq_broker
            .map(|s| s.to_string())
            .or_else(|| self.default_dlq_broker.clone());
        let target_topic = dlq_topic
            .map(|s| s.to_string())
            .or_else(|| self.default_dlq_topic.clone());

        let (Some(tb), Some(tt)) = (target_broker, target_topic) else {
            return Ok(false);
        };

        // Anti-recursion protection
        if source_broker == tb && source_topic == tt {
            self.metrics.record_error(source_broker, "dlq_recursion");
            return Err(false); // nack(requeue=False)
        }

        let fp = if self.dlq_budget.dedupe_window.is_some() {
            Some(dlq_fingerprint(
                source_broker.as_bytes(),
                source_topic.as_bytes(),
                error.as_bytes(),
                payload,
            ))
        } else {
            None
        };

        let admission = self.dlq_budget.admit(
            source_broker,
            payload.len(),
            fp.as_deref(),
            false,
            now_sec,
        );
        if let Some(refusal) = admission.refusal {
            if refusal == "duplicate" {
                self.metrics.record_dlq_refused(source_broker, "duplicate");
                return Ok(true); // Acked
            }
            self.metrics.record_dlq_refused(source_broker, &refusal);
            if refusal == "oversize" {
                let meta = JsonValue::Object(vec![
                    ("payload_omitted".to_string(), JsonValue::Bool(true)),
                    (
                        "original_size".to_string(),
                        JsonValue::Number(payload.len() as f64, true),
                    ),
                    ("reason".to_string(), JsonValue::Str("oversize".to_string())),
                ]);
                let record =
                    format_dlq_payload(b"", source_broker, source_topic, error, Some(meta), None);
                if self
                    .dlq_budget
                    .admit(source_broker, record.len(), None, true, now_sec)
                    .refusal
                    .is_none()
                    && self.brokers.contains_key(&tb)
                {
                    dlq_published.push(JsonValue::Object(vec![
                        ("target_broker".to_string(), JsonValue::Str(tb.clone())),
                        ("topic".to_string(), JsonValue::Str(tt.clone())),
                        (
                            "payload_hex".to_string(),
                            JsonValue::Str(crate::signing::hex_encode(&record)),
                        ),
                    ]));
                    self.metrics.record_error(source_broker, "dlq_metadata_only");
                }
            }
            return Ok(false);
        }

        if !self.brokers.contains_key(&tb) {
            self.metrics
                .record_error(source_broker, "dlq_broker_unregistered");
            return Ok(false);
        }

        let meta_opt = if admission.repeat_count > 0 {
            Some(JsonValue::Object(vec![(
                "repeat_count".to_string(),
                JsonValue::Number(admission.repeat_count as f64, true),
            )]))
        } else {
            None
        };
        let dlq_bytes =
            format_dlq_payload(payload, source_broker, source_topic, error, meta_opt, None);

        dlq_published.push(JsonValue::Object(vec![
            ("target_broker".to_string(), JsonValue::Str(tb.clone())),
            ("topic".to_string(), JsonValue::Str(tt.clone())),
            (
                "payload_hex".to_string(),
                JsonValue::Str(crate::signing::hex_encode(&dlq_bytes)),
            ),
        ]));
        self.metrics.record_dlq(source_broker, &tb, &tt);
        Ok(true)
    }

    pub fn route(
        &mut self,
        source_broker: &str,
        source_topic: &str,
        payload: &[u8],
        source_kwargs: &JsonValue,
        now_sec: f64,
    ) -> JsonValue {
        let mut published: Vec<JsonValue> = Vec::new();
        let mut dlq_published: Vec<JsonValue> = Vec::new();

        // 1. Firewall check
        if let Some(fw) = &mut self.firewall {
            let sender_opt = source_kwargs.get("sender_id").and_then(|v| v.as_str());
            let ctx = MessageContext {
                broker: source_broker,
                sender: sender_opt,
                topic: source_topic,
                payload,
                now: now_sec,
            };
            let verdict = fw.evaluate(&ctx);
            if verdict.action != Action::Allow {
                let rule_name = verdict.rule.clone().unwrap_or_else(|| "unknown".to_string());
                let reason_str = verdict.reason.clone().unwrap_or_default();
                self.metrics
                    .record_ingress(source_broker, &rule_name, verdict.action.as_str());
                if let Some(cat) = &verdict.category {
                    if cat != "rule_error" {
                        self.metrics.record_sender_event(source_broker, cat);
                    }
                }
                let acked = verdict.action == Action::Drop;
                let nacked = verdict.action == Action::Reject;
                return Self::build_outcome(
                    acked,
                    nacked,
                    false,
                    verdict.action.as_str(),
                    &rule_name,
                    &reason_str,
                    published,
                    dlq_published,
                );
            }
        }

        // 2. TopicTrie match
        let matched_json_strs = self
            .tries
            .get(source_broker)
            .map(|t| t.match_topic(source_topic))
            .unwrap_or_default();

        if matched_json_strs.is_empty() {
            self.metrics.record_error(source_broker, "unroutable");
            if self.firewall.is_some() {
                self.metrics.record_sender_event(source_broker, "unroutable");
            }
            let policy = self
                .unroutable_overrides
                .get(source_broker)
                .cloned()
                .unwrap_or_else(|| self.unroutable_policy.clone());

            if policy == "dlq" {
                match self.route_to_dlq(
                    source_broker,
                    source_topic,
                    payload,
                    "Unroutable message: no matching routes",
                    None,
                    None,
                    now_sec,
                    &mut dlq_published,
                ) {
                    Ok(true) => {
                        return Self::build_outcome(
                            true,
                            false,
                            false,
                            "allow",
                            "",
                            "",
                            published,
                            dlq_published,
                        );
                    }
                    Err(requeue) => {
                        return Self::build_outcome(
                            false,
                            true,
                            requeue,
                            "allow",
                            "",
                            "",
                            published,
                            dlq_published,
                        );
                    }
                    Ok(false) => {}
                }
            }
            if policy == "drop" {
                self.metrics
                    .record_error(source_broker, "unroutable_dropped");
                return Self::build_outcome(
                    true,
                    false,
                    false,
                    "allow",
                    "",
                    "",
                    published,
                    dlq_published,
                );
            } else {
                self.metrics
                    .record_error(source_broker, "unroutable_nacked");
                return Self::build_outcome(
                    false,
                    true,
                    false,
                    "allow",
                    "",
                    "",
                    published,
                    dlq_published,
                );
            }
        }

        // 3. Fan-out across matched routes (filtered by worker_engine / canary split and content filter)
        let mut active_route_cfgs: Vec<JsonValue> = Vec::new();
        for route_str in &matched_json_strs {
            if let Ok(route_cfg) = parse_json(route_str) {
                if self.route_matches_engine(&route_cfg, source_broker, source_topic, payload)
                    && self.route_matches_filter(&route_cfg, payload, source_kwargs)
                {
                    active_route_cfgs.push(route_cfg);
                }
            }
        }
        if active_route_cfgs.is_empty() {
            return Self::build_outcome(
                true,
                false,
                false,
                "allow",
                "",
                "",
                published,
                dlq_published,
            );
        }

        let mut any_failed = false;
        let mut handled_by_dlq = false;

        for route_cfg in active_route_cfgs {
            let Some(entries) = route_cfg.as_object() else {
                continue;
            };
            let target_client = route_cfg
                .get("target_client")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let target_topic = route_cfg
                .get("target_topic")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let route_dlq_broker = route_cfg
                .get("dlq_broker")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let route_dlq_topic = route_cfg
                .get("dlq_topic")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());

            let excluded = [
                "target_client",
                "target_topic",
                "dlq_broker",
                "dlq_topic",
                "input_format",
                "input_schema",
                "output_format",
                "output_schema",
                "wire_format",
                "wire_format_in",
                "wire_format_out",
                "confluent_framing",
                "engine",
                "filter",
            ];
            let mut publish_kwargs: Vec<(String, JsonValue)> = Vec::new();
            for (k, v) in entries {
                if !excluded.contains(&k.as_str()) {
                    publish_kwargs.push((k.clone(), v.clone()));
                }
            }
            translate_semantics(source_kwargs, &mut publish_kwargs);

            // Schema transformation if configured
            let input_format = route_cfg.get("input_format").and_then(|v| v.as_str());
            let output_format = route_cfg.get("output_format").and_then(|v| v.as_str());
            let input_schema = route_cfg.get("input_schema").and_then(|v| v.as_str());
            let output_schema = route_cfg.get("output_schema").and_then(|v| v.as_str());
            let wire_format = route_cfg
                .get("wire_format")
                .or_else(|| route_cfg.get("confluent_framing"))
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            let wire_in = route_cfg
                .get("wire_format_in")
                .and_then(|v| v.as_bool())
                .unwrap_or_else(|| wire_format && input_format.is_some_and(|f| f != "json"));
            let wire_out = route_cfg
                .get("wire_format_out")
                .and_then(|v| v.as_bool())
                .unwrap_or_else(|| wire_format && output_format.is_some_and(|f| f != "json"));

            let needs_transform = output_format.is_some()
                || output_schema.is_some()
                || (input_format.is_some()
                    && output_format.is_some()
                    && input_format != output_format);

            let out_bytes = if needs_transform {
                let in_fmt = input_format.unwrap_or("json");
                let out_fmt = output_format.unwrap_or("json");
                let in_sch = input_schema.unwrap_or("");
                let out_sch = output_schema.unwrap_or("");
                let res = self
                    .codec
                    .deserialize(payload, in_fmt, in_sch, wire_in)
                    .and_then(|json_str| {
                        self.codec.serialize(&json_str, out_fmt, out_sch, wire_out)
                    });
                match res {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        self.metrics
                            .record_error(source_broker, "schema_validation_error");
                        self.metrics
                            .record_routed(source_broker, &target_client, "error");
                        if !any_failed {
                            any_failed = true;
                            let err_msg = format!("Schema transformation error: {e}");
                            if let Ok(true) = self.route_to_dlq(
                                source_broker,
                                source_topic,
                                payload,
                                &err_msg,
                                route_dlq_broker.as_deref(),
                                route_dlq_topic.as_deref(),
                                now_sec,
                                &mut dlq_published,
                            ) {
                                handled_by_dlq = true;
                            }
                        }
                        continue;
                    }
                }
            } else {
                payload.to_vec()
            };

            let Some(broker_reg) = self.brokers.get(&target_client) else {
                self.metrics
                    .record_error(source_broker, "unregistered_target");
                if !any_failed {
                    any_failed = true;
                    let err_msg = format!("Target client {target_client} not registered");
                    if let Ok(true) = self.route_to_dlq(
                        source_broker,
                        source_topic,
                        payload,
                        &err_msg,
                        route_dlq_broker.as_deref(),
                        route_dlq_topic.as_deref(),
                        now_sec,
                        &mut dlq_published,
                    ) {
                        handled_by_dlq = true;
                    }
                }
                continue;
            };

            if broker_reg.fail_publish {
                self.metrics.record_error(source_broker, "publish_failure");
                self.metrics
                    .record_routed(source_broker, &target_client, "error");
                if !any_failed {
                    any_failed = true;
                    let err_msg = format!("Simulated publish failure on {target_client}");
                    if let Ok(true) = self.route_to_dlq(
                        source_broker,
                        source_topic,
                        payload,
                        &err_msg,
                        route_dlq_broker.as_deref(),
                        route_dlq_topic.as_deref(),
                        now_sec,
                        &mut dlq_published,
                    ) {
                        handled_by_dlq = true;
                    }
                }
                continue;
            }

            published.push(JsonValue::Object(vec![
                (
                    "target_broker".to_string(),
                    JsonValue::Str(target_client.clone()),
                ),
                ("topic".to_string(), JsonValue::Str(target_topic)),
                (
                    "payload_hex".to_string(),
                    JsonValue::Str(crate::signing::hex_encode(&out_bytes)),
                ),
                ("kwargs".to_string(), JsonValue::Object(publish_kwargs)),
            ]));
            self.metrics
                .record_routed(source_broker, &target_client, "success");
        }

        let (acked, nacked, requeue) = if !any_failed {
            (true, false, false)
        } else if handled_by_dlq {
            (true, false, false)
        } else {
            (false, true, true)
        };

        Self::build_outcome(
            acked,
            nacked,
            requeue,
            "allow",
            "",
            "",
            published,
            dlq_published,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn build_outcome(
        acked: bool,
        nacked: bool,
        requeue: bool,
        verdict: &str,
        rule: &str,
        reason: &str,
        published: Vec<JsonValue>,
        dlq_published: Vec<JsonValue>,
    ) -> JsonValue {
        JsonValue::Object(vec![
            ("acked".to_string(), JsonValue::Bool(acked)),
            ("nacked".to_string(), JsonValue::Bool(nacked)),
            ("requeue".to_string(), JsonValue::Bool(requeue)),
            (
                "firewall_verdict".to_string(),
                JsonValue::Str(verdict.to_string()),
            ),
            ("rule".to_string(), JsonValue::Str(rule.to_string())),
            ("reason".to_string(), JsonValue::Str(reason.to_string())),
            ("published".to_string(), JsonValue::Array(published)),
            ("dlq_published".to_string(), JsonValue::Array(dlq_published)),
        ])
    }

    pub fn open_signed_raw(
        &mut self,
        raw: &str,
        kind: &str,
        hash_name: &str,
        field: &str,
    ) -> Result<Option<JsonValue>, String> {
        if raw.len() > self.max_event_bytes {
            self.metrics.record_control_event("oversize");
            return Ok(None);
        }
        if let Some(prob) = scan_json(raw.as_bytes(), 16, 10000, 10000, self.max_event_bytes) {
            return Err(prob);
        }
        let obj = parse_json(raw)?;
        let env_opt = extract_envelope(&obj)
            .map(|(k, s, p)| (k.to_string(), s.to_string(), p.to_string()));
        let Some((_kid, _sig, inner_payload)) = env_opt else {
            if self.signing_mode == "off" {
                return Ok(Some(obj));
            }
            self.metrics.record_control_event("unsigned");
            return if self.signing_mode == "observe" {
                Ok(Some(obj))
            } else {
                Ok(None)
            };
        };
        if self.signing_mode == "off" {
            return parse_json(&inner_payload).map(Some);
        }
        let valid = match &self.signer {
            Some(signer) => {
                if kind == "state" {
                    signer.verify_state_envelope(hash_name, field, raw)
                } else {
                    signer.verify_event_envelope(raw)
                }
            }
            None => false,
        };
        if valid {
            return parse_json(&inner_payload).map(Some);
        }
        self.metrics.record_control_event("bad_signature");
        if self.signing_mode == "observe" {
            parse_json(&inner_payload).map(Some)
        } else {
            Ok(None)
        }
    }

    pub fn apply_control_event(&mut self, raw_event: &str, now_sec: f64) -> String {
        if raw_event.len() > self.max_event_bytes {
            self.metrics.record_control_event("oversize");
            return "oversize".to_string();
        }
        if !self.event_bucket.try_consume(1.0, now_sec) {
            self.metrics.record_control_event("rate_limited");
            self.needs_resync = true;
            return "rate_limited".to_string();
        }
        let payload = match self.open_signed_raw(raw_event, "event", "", "") {
            Ok(Some(p)) => p,
            Ok(None) => return "rejected_auth".to_string(),
            Err(_) => {
                self.metrics.record_control_event("invalid");
                return "invalid".to_string();
            }
        };
        if payload.as_object().is_none() {
            self.metrics.record_control_event("invalid");
            return "invalid".to_string();
        }
        let schema_ver = payload
            .get("schema_version")
            .map(|v| match v {
                JsonValue::Number(n, true) if n.fract() == 0.0 => *n as i64,
                _ => -1,
            })
            .unwrap_or(1);
        if schema_ver != 1 {
            self.metrics.record_control_event("invalid");
            return "invalid".to_string();
        }

        if let Some(ver_val) = payload.get("version") {
            let Some(ver) = ver_val.as_i64() else {
                self.metrics.record_control_event("invalid");
                return "invalid".to_string();
            };
            if let Some(cur) = self.applied_version {
                if ver <= cur {
                    self.metrics.record_control_event("stale");
                    return "stale".to_string();
                }
                if ver > cur + 1 {
                    self.metrics.record_control_event("gap");
                    self.needs_resync = true;
                    return "gap".to_string();
                }
            }
            self.applied_version = Some(ver);
        }

        let action = payload.get("action").and_then(|v| v.as_str()).unwrap_or("");
        let data = payload
            .get("data")
            .cloned()
            .unwrap_or_else(|| JsonValue::Object(Vec::new()));

        if action == "clear_state" && !self.allow_clear_state {
            self.metrics.record_control_event("forbidden_action");
            return "forbidden_action".to_string();
        }
        if validate_action_json(action, &data).is_err() {
            self.metrics.record_control_event("invalid");
            return "invalid".to_string();
        }

        if action == "set_ingress" {
            let fw_json = data.to_json_string();
            match Firewall::from_json_str(&fw_json) {
                Ok(fw) => {
                    self.firewall = Some(fw);
                    self.metrics.record_control_event("applied");
                    return "applied".to_string();
                }
                Err(_) => {
                    self.metrics.record_control_event("invalid");
                    return "invalid".to_string();
                }
            }
        }

        self.metrics.record_control_event("applied");
        self.apply_validated_action(action, &data);
        "applied".to_string()
    }

    pub fn apply_validated_action(&mut self, action: &str, data: &JsonValue) {
        match action {
            "add_broker" => {
                let name = data.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let protocol = data.get("protocol").and_then(|v| v.as_str()).unwrap_or("");
                let kwargs = data
                    .get("kwargs")
                    .and_then(|v| v.as_object())
                    .map(|s| s.to_vec())
                    .unwrap_or_default();
                self.register_broker(name, protocol, kwargs);
            }
            "add_route" => {
                let sb = data
                    .get("source_broker")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let st = data
                    .get("source_topic")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let tb = data
                    .get("target_broker")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let tt = data
                    .get("target_topic")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let mut tkw = data
                    .get("target_kwargs")
                    .or_else(|| data.get("kwargs"))
                    .and_then(|v| v.as_object())
                    .map(|s| s.to_vec())
                    .unwrap_or_default();
                if let Some(eng) = data.get("engine") {
                    if !tkw.iter().any(|(k, _)| k == "engine") {
                        tkw.push(("engine".to_string(), eng.clone()));
                    }
                }
                let _ = self.add_route(sb, st, tb, tt, tkw);
            }
            "add_subscription" => {
                let bn = data
                    .get("broker_name")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let topic = data.get("topic").and_then(|v| v.as_str()).unwrap_or("");
                self.subscriptions
                    .insert(format!("{bn}:{topic}"), data.clone());
            }
            "update_broker_security" => {
                let name = data.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if let Some(sec) = data.get("security_config") {
                    if let Some(broker) = self.brokers.get_mut(name) {
                        broker.kwargs.retain(|(k, _)| k != "security_config");
                        broker
                            .kwargs
                            .push(("security_config".to_string(), sec.clone()));
                    } else {
                        self.metrics
                            .record_error("data_plane", "security_update_unknown_broker");
                        self.needs_resync = true;
                    }
                }
            }
            "clear_state" => {
                self.brokers.clear();
                self.tries.clear();
                self.routes_count = 0;
                self.subscriptions.clear();
                self.firewall = None;
                self.native_percentage = 0;
            }
            "set_rollout" => {
                if let Some(pct) = data.get("native_percentage").and_then(|v| v.as_i64()) {
                    self.native_percentage = pct.clamp(0, 100) as u8;
                }
            }
            "set_worker_engine" => {
                if let Some(eng) = data.get("engine").and_then(|v| v.as_str()) {
                    self.worker_engine = eng.to_string();
                }
            }
            "configure_signing" => {
                if let Some(kr) = data.get("keyring").and_then(|v| v.as_str()) {
                    self.signer = HmacSigner::from_keyring_str(kr).ok();
                }
                if let Some(mode) = data.get("mode").and_then(|v| v.as_str()) {
                    self.signing_mode = mode.to_string();
                }
            }
            "set_allow_clear_state" => {
                if let Some(allow) = data.get("allow").and_then(|v| v.as_bool()) {
                    self.allow_clear_state = allow;
                }
            }
            "apply_event" => {
                let raw = data.get("raw_event").and_then(|v| v.as_str()).unwrap_or("");
                let now = data.get("now_sec").and_then(|v| v.as_f64()).unwrap_or(0.0);
                let _ = self.apply_control_event(raw, now);
            }
            "register_schema" => {
                let name = data.get("name").and_then(|v| v.as_str()).unwrap_or("");
                let s_type_str = data
                    .get("schema_type")
                    .or_else(|| data.get("type"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("AVRO");
                let def = data
                    .get("schema_str")
                    .or_else(|| data.get("definition"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let sid = data
                    .get("schema_id")
                    .or_else(|| data.get("id"))
                    .and_then(|v| v.as_i64())
                    .map(|v| v.max(1) as u32)
                    .unwrap_or_else(|| (self.codec.len() as u32) + 1);
                let _ = self.codec.register_schema(sid, name, s_type_str, def);
            }
            _ => {}
        }
    }
}

fn translate_semantics(
    source_kwargs: &JsonValue,
    target_kwargs: &mut Vec<(String, JsonValue)>,
) {
    let has_dm = target_kwargs.iter().any(|(k, _)| k == "delivery_mode");
    if !has_dm {
        if let Some(qos) = source_kwargs.get("qos").and_then(|v| v.as_i64()) {
            if qos == 1 || qos == 2 {
                target_kwargs.push(("delivery_mode".to_string(), JsonValue::Number(2.0, true)));
            } else if qos == 0 {
                target_kwargs.push(("delivery_mode".to_string(), JsonValue::Number(1.0, true)));
            }
        }
    }
    let has_qos = target_kwargs.iter().any(|(k, _)| k == "qos");
    if !has_qos {
        if let Some(dm) = source_kwargs.get("delivery_mode").and_then(|v| v.as_i64()) {
            if dm == 2 {
                target_kwargs.push(("qos".to_string(), JsonValue::Number(1.0, true)));
            } else if dm == 1 {
                target_kwargs.push(("qos".to_string(), JsonValue::Number(0.0, true)));
            }
        }
    }
}

pub struct Engine {
    pub state: Arc<Mutex<EngineState>>,
    server_stop: Arc<AtomicBool>,
    server_thread: Option<JoinHandle<()>>,
}

impl Engine {
    pub fn new(max_label_values: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(EngineState::new(max_label_values))),
            server_stop: Arc::new(AtomicBool::new(false)),
            server_thread: None,
        }
    }

    pub fn start_metrics_server(&mut self, addr: &str, port: u16) -> Result<u16, String> {
        self.stop_metrics_server();
        let listener =
            TcpListener::bind((addr, port)).map_err(|e| format!("bind failed: {e}"))?;
        let bound_port = listener
            .local_addr()
            .map_err(|e| format!("local_addr failed: {e}"))?
            .port();
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("set_nonblocking failed: {e}"))?;

        let stop = Arc::new(AtomicBool::new(false));
        self.server_stop = Arc::clone(&stop);
        let state = Arc::clone(&self.state);

        let handle = thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        let _ = stream.set_read_timeout(Some(Duration::from_millis(500)));
                        let _ = stream.set_write_timeout(Some(Duration::from_millis(500)));
                        let mut req_buf = [0u8; 1024];
                        let n = stream.read(&mut req_buf).unwrap_or(0);
                        let req_str = std::str::from_utf8(&req_buf[..n]).unwrap_or("");
                        let path = req_str
                            .lines()
                            .next()
                            .and_then(|l| l.split_whitespace().nth(1))
                            .unwrap_or("/metrics");

                        let (status, content_type, body) = if path.starts_with("/healthz") {
                            (
                                "200 OK",
                                "application/json",
                                "{\"status\":\"ok\",\"version\":\"1.0.0\"}\n".to_string(),
                            )
                        } else if path.starts_with("/readyz") {
                            let guard = state.lock().unwrap();
                            let ver = guard.applied_version.unwrap_or(0);
                            (
                                "200 OK",
                                "application/json",
                                format!("{{\"ready\":true,\"applied_version\":{ver}}}\n"),
                            )
                        } else {
                            let guard = state.lock().unwrap();
                            (
                                "200 OK",
                                "text/plain; version=0.0.4; charset=utf-8",
                                guard.metrics.render_prometheus(),
                            )
                        };

                        let resp = format!(
                            "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                            body.len()
                        );
                        let _ = stream.write_all(resp.as_bytes());
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err(_) => break,
                }
            }
        });
        self.server_thread = Some(handle);
        Ok(bound_port)
    }

    pub fn stop_metrics_server(&mut self) {
        self.server_stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.server_thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        self.stop_metrics_server();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_validation_and_evaluation() {
        let spec = parse_json(
            r#"{"all":[{"path":"$.sensor.readings[1].temp","op":">=","value":25.5},{"path":"$headers.region","op":"in","value":["us-east","eu-west"]}]}"#,
        )
        .unwrap();
        assert!(validate_filter_json(&spec, 8).is_ok());

        let payload = br#"{"sensor":{"readings":[{"temp":20.0},{"temp":26.1}]}}"#;
        let kwargs = parse_json(r#"{"headers":{"region":"us-east"}}"#).unwrap();
        assert!(evaluate_filter(&spec, payload, &kwargs));

        let bad_kwargs = parse_json(r#"{"headers":{"region":"ap-south"}}"#).unwrap();
        assert!(!evaluate_filter(&spec, payload, &bad_kwargs));

        let strict_bool = parse_json(r#"{"path":"$.flag","op":"==","value":1}"#).unwrap();
        assert!(!evaluate_filter(
            &strict_bool,
            br#"{"flag":true}"#,
            &JsonValue::Object(vec![])
        ));
    }
}
