//! Ingress `Firewall` (`po_firewall_t`), `MaxPayloadBytes`, `TopicShape`, `RateLimit`
//! (`TokenBucket` + `KeyedLimiter`), `ContentGuard`, and `SenderLedger`.

use std::collections::BTreeMap;

use crate::json::{parse_json, scan_json, JsonValue};
use crate::trie::{extract_identity, topic_matches};

const CATEGORIES: &[&str] = &[
    "oversize",
    "malformed",
    "rate_exceeded",
    "unroutable",
    "schema_mismatch",
    "forbidden_topic",
    "cooled",
];
const OTHER: &str = "other";
const MIN_EVENTS_FOR_CAUSE: u64 = 5;
const DOMINANT_SHARE: f64 = 0.9;
const MIXED_CATEGORY_COUNT: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Action {
    Allow,
    Drop,
    Reject,
}

impl Action {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "allow" => Ok(Action::Allow),
            "drop" => Ok(Action::Drop),
            "reject" => Ok(Action::Reject),
            _ => Err(format!(
                "Invalid action '{}'. Use one of: allow, drop, reject.",
                s
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Action::Allow => "allow",
            Action::Drop => "drop",
            Action::Reject => "reject",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Verdict {
    pub action: Action,
    pub rule: Option<String>,
    pub category: Option<String>,
    pub reason: Option<String>,
}

impl Verdict {
    pub fn allow() -> Self {
        Self {
            action: Action::Allow,
            rule: None,
            category: None,
            reason: None,
        }
    }

    pub fn to_json_value(&self) -> JsonValue {
        JsonValue::Object(vec![
            (
                "action".to_string(),
                JsonValue::Str(self.action.as_str().to_string()),
            ),
            (
                "rule".to_string(),
                self.rule
                    .as_ref()
                    .map(|s| JsonValue::Str(s.clone()))
                    .unwrap_or(JsonValue::Null),
            ),
            (
                "category".to_string(),
                self.category
                    .as_ref()
                    .map(|s| JsonValue::Str(s.clone()))
                    .unwrap_or(JsonValue::Null),
            ),
            (
                "reason".to_string(),
                self.reason
                    .as_ref()
                    .map(|s| JsonValue::Str(s.clone()))
                    .unwrap_or(JsonValue::Null),
            ),
        ])
    }
}

pub struct MessageContext<'a> {
    pub broker: &'a str,
    pub sender: Option<&'a str>,
    pub topic: &'a str,
    pub payload: &'a [u8],
    pub now: f64,
}

#[derive(Clone, Debug)]
pub struct TokenBucket {
    pub rate: f64,
    pub burst: f64,
    tokens: f64,
    updated: f64,
}

impl TokenBucket {
    pub fn new(rate: f64, burst: f64, now: f64) -> Result<Self, String> {
        if rate <= 0.0 || !rate.is_finite() {
            return Err("rate must be > 0".to_string());
        }
        if burst <= 0.0 || !burst.is_finite() {
            return Err("burst must be > 0".to_string());
        }
        Ok(Self {
            rate,
            burst,
            tokens: burst,
            updated: now,
        })
    }

    fn refill(&mut self, now: f64) {
        let elapsed = (now - self.updated).max(0.0);
        self.tokens = self.burst.min(self.tokens + elapsed * self.rate);
        self.updated = now;
    }

    pub fn can_consume(&mut self, cost: f64, now: f64) -> bool {
        self.refill(now);
        cost <= self.tokens
    }

    pub fn try_consume(&mut self, cost: f64, now: f64) -> bool {
        self.refill(now);
        if cost <= self.tokens {
            self.tokens -= cost;
            true
        } else {
            false
        }
    }
}

#[derive(Clone, Debug)]
pub struct KeyedLimiter {
    rate: f64,
    burst: f64,
    max_keys: usize,
    ttl: f64,
    buckets: Vec<(String, TokenBucket, f64)>, // LRU order: oldest at 0, newest at back
}

impl KeyedLimiter {
    pub fn new(rate: f64, burst: f64, max_keys: usize, ttl: f64) -> Result<Self, String> {
        if max_keys == 0 {
            return Err("max_keys must be > 0".to_string());
        }
        if ttl <= 0.0 || !ttl.is_finite() {
            return Err("ttl must be > 0".to_string());
        }
        TokenBucket::new(rate, burst, 0.0)?;
        Ok(Self {
            rate,
            burst,
            max_keys,
            ttl,
            buckets: Vec::new(),
        })
    }

    pub fn try_consume(&mut self, key: &str, cost: f64, now: f64) -> bool {
        while let Some((_, _, last_seen)) = self.buckets.first() {
            if now - *last_seen <= self.ttl {
                break;
            }
            self.buckets.remove(0);
        }
        let mut bucket = if let Some(pos) = self.buckets.iter().position(|(k, _, _)| k == key) {
            self.buckets.remove(pos).1
        } else {
            TokenBucket::new(self.rate, self.burst, now).unwrap()
        };
        let allowed = bucket.try_consume(cost, now);
        self.buckets.push((key.to_string(), bucket, now));
        while self.buckets.len() > self.max_keys {
            self.buckets.remove(0);
        }
        allowed
    }
}

#[derive(Clone, Debug)]
pub enum Rule {
    MaxPayloadBytes {
        brokers: Option<Vec<String>>,
        max_bytes: usize,
        action: Action,
    },
    TopicShape {
        brokers: Option<Vec<String>>,
        max_length: usize,
        max_depth: usize,
        deny: Vec<String>,
        action: Action,
    },
    RateLimit {
        brokers: Option<Vec<String>>,
        key: String,
        template: Option<String>,
        cost: String,
        action: Action,
        limiter: KeyedLimiter,
    },
    ContentGuard {
        brokers: Option<Vec<String>>,
        topic: String,
        format: String,
        max_bytes: Option<usize>,
        max_depth: usize,
        max_keys: usize,
        max_array_length: usize,
        max_string_length: usize,
        required_fields: Vec<String>,
        field_ranges: Vec<(String, Option<f64>, Option<f64>)>,
        enums: Vec<(String, Vec<JsonValue>)>,
        action: Action,
    },
}

impl Rule {
    fn applies(&self, broker: &str) -> bool {
        let brokers = match self {
            Rule::MaxPayloadBytes { brokers, .. }
            | Rule::TopicShape { brokers, .. }
            | Rule::RateLimit { brokers, .. }
            | Rule::ContentGuard { brokers, .. } => brokers,
        };
        match brokers {
            None => true,
            Some(list) => list.iter().any(|b| b == broker),
        }
    }

    pub fn check(&mut self, ctx: &MessageContext) -> Verdict {
        if !self.applies(ctx.broker) {
            return Verdict::allow();
        }
        match self {
            Rule::MaxPayloadBytes {
                max_bytes, action, ..
            } => {
                if ctx.payload.len() > *max_bytes {
                    Verdict {
                        action: *action,
                        rule: Some("max_payload_bytes".to_string()),
                        category: Some("oversize".to_string()),
                        reason: Some(format!("{} > {} bytes", ctx.payload.len(), max_bytes)),
                    }
                } else {
                    Verdict::allow()
                }
            }
            Rule::TopicShape {
                max_length,
                max_depth,
                deny,
                action,
                ..
            } => {
                let bad = |cat: &str, reason: String| Verdict {
                    action: *action,
                    rule: Some("topic_shape".to_string()),
                    category: Some(cat.to_string()),
                    reason: Some(reason),
                };
                let t = ctx.topic;
                if t.is_empty() {
                    return bad("malformed", "empty topic".to_string());
                }
                if t.as_bytes().contains(&0) {
                    return bad("malformed", "null byte in topic".to_string());
                }
                if t.len() > *max_length {
                    return bad(
                        "malformed",
                        format!("topic length {} > {}", t.len(), max_length),
                    );
                }
                let depth = t.bytes().filter(|&b| b == b'/').count() + 1;
                if depth > *max_depth {
                    return bad("malformed", format!("topic depth > {}", max_depth));
                }
                for pat in deny {
                    if topic_matches(pat, t) {
                        return bad(
                            "forbidden_topic",
                            format!("matches denied pattern {}", pat),
                        );
                    }
                }
                Verdict::allow()
            }
            Rule::RateLimit {
                key,
                template,
                cost,
                action,
                limiter,
                ..
            } => {
                let limiter_key = if key == "broker" {
                    ctx.broker.to_string()
                } else if key == "identity" {
                    let ident = template
                        .as_deref()
                        .and_then(|tpl| extract_identity(tpl, ctx.topic));
                    format!("{}:{}", ctx.broker, ident.as_deref().unwrap_or(ctx.topic))
                } else {
                    format!("{}:{}", ctx.broker, ctx.topic)
                };
                let c = if cost == "bytes" {
                    ctx.payload.len() as f64
                } else {
                    1.0
                };
                if limiter.try_consume(&limiter_key, c, ctx.now) {
                    Verdict::allow()
                } else {
                    Verdict {
                        action: *action,
                        rule: Some("rate_limit".to_string()),
                        category: Some("rate_exceeded".to_string()),
                        reason: Some(format!("{} rate limit", key)),
                    }
                }
            }
            Rule::ContentGuard {
                topic,
                format,
                max_bytes,
                max_depth,
                max_keys,
                max_array_length,
                max_string_length,
                required_fields,
                field_ranges,
                enums,
                action,
                ..
            } => {
                if !topic_matches(topic, ctx.topic) {
                    return Verdict::allow();
                }
                let bad = |cat: &str, reason: String| Verdict {
                    action: *action,
                    rule: Some("content_guard".to_string()),
                    category: Some(cat.to_string()),
                    reason: Some(reason),
                };
                if let Some(mb) = max_bytes {
                    if ctx.payload.len() > *mb {
                        return bad("oversize", format!("{} > {} bytes", ctx.payload.len(), mb));
                    }
                }
                if format != "json" {
                    return bad("malformed", format!("unsupported native format {}", format));
                }
                if let Some(scan_err) = scan_json(
                    ctx.payload,
                    *max_depth,
                    *max_keys,
                    *max_array_length,
                    *max_string_length,
                ) {
                    return bad("malformed", scan_err);
                }
                let Ok(utf8_str) = std::str::from_utf8(ctx.payload) else {
                    return bad("malformed", "invalid utf-8".to_string());
                };
                let record = match parse_json(utf8_str) {
                    Ok(v) => v,
                    Err(e) => return bad("malformed", format!("invalid json: {}", e)),
                };
                for path in required_fields {
                    if record.lookup_path(path).is_none() {
                        return bad(
                            "schema_mismatch",
                            format!("required field '{}' missing", path),
                        );
                    }
                }
                for (path, min_b, max_b) in field_ranges {
                    let Some(val) = record.lookup_path(path) else {
                        continue;
                    };
                    let Some(num) = val.as_f64() else {
                        return bad("schema_mismatch", format!("field '{}' is not a number", path));
                    };
                    let below = min_b.map(|m| num < m).unwrap_or(false);
                    let above = max_b.map(|m| num > m).unwrap_or(false);
                    if below || above {
                        let mut bounds_parts = Vec::new();
                        if let Some(mn) = min_b {
                            bounds_parts.push(format!("'min': {}", format_py_float(*mn)));
                        }
                        if let Some(mx) = max_b {
                            bounds_parts.push(format!("'max': {}", format_py_float(*mx)));
                        }
                        return bad(
                            "schema_mismatch",
                            format!(
                                "field '{}'={} outside {{{}}}",
                                path,
                                val.format_python_repr(),
                                bounds_parts.join(", ")
                            ),
                        );
                    }
                }
                for (path, allowed) in enums {
                    let Some(val) = record.lookup_path(path) else {
                        continue;
                    };
                    if !allowed.iter().any(|a| json_values_equal(a, val)) {
                        let list_repr: Vec<String> =
                            allowed.iter().map(|a| a.format_python_repr()).collect();
                        return bad(
                            "schema_mismatch",
                            format!(
                                "field '{}'={} not in [{}]",
                                path,
                                val.format_python_repr(),
                                list_repr.join(", ")
                            ),
                        );
                    }
                }
                Verdict::allow()
            }
        }
    }
}

fn format_py_float(v: f64) -> String {
    if v.fract() == 0.0 {
        format!("{:.1}", v)
    } else {
        format!("{}", v)
    }
}

fn json_values_equal(a: &JsonValue, b: &JsonValue) -> bool {
    match (a, b) {
        (JsonValue::Number(x, _), JsonValue::Number(y, _)) => (x - y).abs() < 1e-12,
        _ => a == b,
    }
}

#[derive(Clone, Debug)]
struct BackoffState {
    base: f64,
    factor: f64,
    cap: f64,
    attempt: u32,
}

impl BackoffState {
    fn new(base: f64, factor: f64, cap: f64) -> Self {
        Self {
            base,
            factor,
            cap,
            attempt: 0,
        }
    }

    fn next_delay(&mut self) -> f64 {
        let raw = self.base * self.factor.powi(self.attempt as i32);
        let ceiling = if raw >= self.cap || !raw.is_finite() {
            self.cap
        } else {
            raw.min(self.cap)
        };
        if ceiling < self.cap {
            self.attempt = self.attempt.saturating_add(1);
        }
        ceiling
    }

    fn reset(&mut self) {
        self.attempt = 0;
    }
}

#[derive(Clone, Debug)]
struct LedgerEntry {
    broker: String,
    sender: String,
    first_seen: f64,
    last_seen: f64,
    win_start: f64,
    total: u64,
    cur: u64,
    prev: u64,
    strikes: u64,
    categories: BTreeMap<String, u64>,
    cooled_until: Option<f64>,
    backoff: Option<BackoffState>,
}

impl LedgerEntry {
    fn new(broker: &str, sender: &str, now: f64, backoff: Option<BackoffState>) -> Self {
        Self {
            broker: broker.to_string(),
            sender: sender.to_string(),
            first_seen: now,
            last_seen: now,
            win_start: now,
            total: 0,
            cur: 0,
            prev: 0,
            strikes: 0,
            categories: BTreeMap::new(),
            cooled_until: None,
            backoff,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SenderLedger {
    pub max_keys: usize,
    pub ttl: f64,
    pub window: f64,
    pub rise_factor: f64,
    pub rise_min_events: u64,
    pub widespread_senders: usize,
    pub identity_template: Option<String>,
    pub enforce: bool,
    pub strike_threshold: u64,
    pub cooldown_base: f64,
    pub cooldown_factor: f64,
    pub cooldown_cap: f64,
    pub decay_after: f64,
    entries: Vec<LedgerEntry>,
    other: Option<LedgerEntry>,
    pub overflow_events: u64,
}

impl SenderLedger {
    pub fn from_json(spec: &JsonValue) -> Result<Self, String> {
        let max_keys = spec
            .get("max_keys")
            .and_then(|v| v.as_u64())
            .unwrap_or(1000) as usize;
        let ttl = spec.get("ttl").and_then(|v| v.as_f64()).unwrap_or(3600.0);
        let window = spec.get("window").and_then(|v| v.as_f64()).unwrap_or(60.0);
        let rise_factor = spec
            .get("rise_factor")
            .and_then(|v| v.as_f64())
            .unwrap_or(2.0);
        let rise_min_events = spec
            .get("rise_min_events")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let widespread_senders = spec
            .get("widespread_senders")
            .and_then(|v| v.as_u64())
            .unwrap_or(10) as usize;
        let identity_template = spec
            .get("identity_template")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let enforce = spec
            .get("enforce")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        let strike_threshold = spec
            .get("strike_threshold")
            .and_then(|v| v.as_u64())
            .unwrap_or(10);
        let cooldown_base = spec
            .get("cooldown_base")
            .and_then(|v| v.as_f64())
            .unwrap_or(30.0);
        let cooldown_factor = spec
            .get("cooldown_factor")
            .and_then(|v| v.as_f64())
            .unwrap_or(2.0);
        let cooldown_cap = spec
            .get("cooldown_cap")
            .and_then(|v| v.as_f64())
            .unwrap_or(900.0);
        let decay_after = spec
            .get("decay_after")
            .and_then(|v| v.as_f64())
            .unwrap_or(600.0);

        if max_keys == 0 || ttl <= 0.0 || window <= 0.0 {
            return Err("max_keys, ttl and window must be > 0".to_string());
        }
        if strike_threshold == 0 {
            return Err("strike_threshold must be > 0".to_string());
        }
        if enforce && ttl < cooldown_cap {
            return Err("ttl must be >= cooldown_cap when enforce is set".to_string());
        }
        if enforce && (cooldown_base <= 0.0 || cooldown_factor < 1.0 || cooldown_cap < cooldown_base)
        {
            return Err("invalid cooldown backoff parameters".to_string());
        }

        Ok(Self {
            max_keys,
            ttl,
            window,
            rise_factor,
            rise_min_events,
            widespread_senders,
            identity_template,
            enforce,
            strike_threshold,
            cooldown_base,
            cooldown_factor,
            cooldown_cap,
            decay_after,
            entries: Vec::new(),
            other: None,
            overflow_events: 0,
        })
    }

    pub fn key_for(&self, ctx: &MessageContext) -> String {
        if let Some(s) = ctx.sender {
            if !s.is_empty() {
                return s.to_string();
            }
        }
        if let Some(tpl) = self.identity_template.as_deref() {
            if let Some(ident) = extract_identity(tpl, ctx.topic) {
                return ident;
            }
        }
        ctx.topic.to_string()
    }

    pub fn cooled_until(&self, broker: &str, sender: &str, now: f64) -> Option<f64> {
        let entry = self
            .entries
            .iter()
            .find(|e| e.broker == broker && e.sender == sender)?;
        match entry.cooled_until {
            Some(until) if now < until => Some(until),
            _ => None,
        }
    }

    pub fn record(&mut self, broker: &str, sender: &str, category: &str, now: f64) -> Result<(), String> {
        if !CATEGORIES.contains(&category) {
            return Err(format!("Unknown ledger category '{}'", category));
        }
        self.prune(now);

        if let Some(pos) = self
            .entries
            .iter()
            .position(|e| e.broker == broker && e.sender == sender)
        {
            let mut entry = self.entries.remove(pos);
            self.update_entry(&mut entry, true, category, now);
            self.entries.push(entry);
            return Ok(());
        }

        if self.entries.len() >= self.max_keys {
            self.overflow_events += 1;
            let mut other = self
                .other
                .take()
                .unwrap_or_else(|| LedgerEntry::new("*", OTHER, now, None));
            self.update_entry(&mut other, false, category, now);
            self.other = Some(other);
            return Ok(());
        }

        let backoff = if self.enforce {
            Some(BackoffState::new(
                self.cooldown_base,
                self.cooldown_factor,
                self.cooldown_cap,
            ))
        } else {
            None
        };
        let mut entry = LedgerEntry::new(broker, sender, now, backoff);
        self.update_entry(&mut entry, true, category, now);
        self.entries.push(entry);
        Ok(())
    }

    fn update_entry(&self, entry: &mut LedgerEntry, tracked: bool, category: &str, now: f64) {
        Self::rotate_entry(entry, self.window, now);
        if tracked && self.enforce && now - entry.last_seen > self.decay_after {
            if let Some(b) = entry.backoff.as_mut() {
                b.reset();
            }
            entry.strikes = 0;
        }
        entry.last_seen = now;
        entry.total += 1;
        entry.cur += 1;
        *entry.categories.entry(category.to_string()).or_insert(0) += 1;

        let is_cooled = entry.cooled_until.map(|u| now < u).unwrap_or(false);
        if tracked && self.enforce && category != "cooled" && !is_cooled {
            entry.strikes += 1;
            if entry.strikes >= self.strike_threshold {
                if let Some(b) = entry.backoff.as_mut() {
                    entry.cooled_until = Some(now + b.next_delay());
                }
                entry.strikes = 0;
            }
        }
    }

    fn prune(&mut self, now: f64) {
        while let Some(oldest) = self.entries.first() {
            if now - oldest.last_seen <= self.ttl {
                break;
            }
            self.entries.remove(0);
        }
    }

    fn rotate_entry(entry: &mut LedgerEntry, window: f64, now: f64) {
        let elapsed = now - entry.win_start;
        if elapsed < window {
            return;
        }
        let periods = (elapsed / window).floor() as u64;
        entry.prev = if periods == 1 { entry.cur } else { 0 };
        entry.cur = 0;
        entry.strikes = 0;
        entry.win_start += (periods as f64) * window;
    }

    fn is_rising(&self, entry: &LedgerEntry) -> bool {
        entry.cur >= self.rise_min_events
            && (entry.cur as f64) >= self.rise_factor * (entry.prev as f64)
    }

    fn cause(&self, entry: &LedgerEntry, active: usize) -> &'static str {
        if active >= self.widespread_senders {
            return "widespread";
        }
        if entry.total < MIN_EVENTS_FOR_CAUSE {
            return "insufficient_data";
        }
        if entry.categories.len() >= MIXED_CATEGORY_COUNT && self.is_rising(entry) {
            return "mixed_high_rate";
        }
        let max_cat = entry.categories.values().copied().max().unwrap_or(0);
        if (max_cat as f64) >= DOMINANT_SHARE * (entry.total as f64) {
            return "repeated_identical_error";
        }
        "varied_errors"
    }

    pub fn sender_report(&mut self, top: usize, now: f64) -> JsonValue {
        self.prune(now);
        for e in &mut self.entries {
            Self::rotate_entry(e, self.window, now);
        }
        if let Some(o) = self.other.as_mut() {
            Self::rotate_entry(o, self.window, now);
        }
        let active = self.entries.iter().filter(|e| e.cur > 0).count();
        let mut all: Vec<LedgerEntry> = self.entries.clone();
        if let Some(o) = self.other.clone() {
            all.push(o);
        }
        // Stable sort descending by (cur, total) matching Python's `entries.sort(key=..., reverse=True)`
        all.sort_by(|a, b| (b.cur, b.total).cmp(&(a.cur, a.total)));

        let rows: Vec<JsonValue> = all
            .into_iter()
            .take(top)
            .map(|e| {
                let cats: Vec<(String, JsonValue)> = e
                    .categories
                    .iter()
                    .map(|(k, &v)| (k.clone(), JsonValue::Number(v as f64, true)))
                    .collect();
                let rising = self.is_rising(&e);
                let likely = self.cause(&e, active);
                JsonValue::Object(vec![
                    ("broker".to_string(), JsonValue::Str(e.broker)),
                    ("sender".to_string(), JsonValue::Str(e.sender)),
                    ("total".to_string(), JsonValue::Number(e.total as f64, true)),
                    ("categories".to_string(), JsonValue::Object(cats)),
                    ("window".to_string(), JsonValue::Number(e.cur as f64, true)),
                    (
                        "previous_window".to_string(),
                        JsonValue::Number(e.prev as f64, true),
                    ),
                    ("rising".to_string(), JsonValue::Bool(rising)),
                    (
                        "first_seen".to_string(),
                        JsonValue::Number(e.first_seen, false),
                    ),
                    (
                        "last_seen".to_string(),
                        JsonValue::Number(e.last_seen, false),
                    ),
                    (
                        "cooled_until".to_string(),
                        e.cooled_until
                            .map(|u| JsonValue::Number(u, false))
                            .unwrap_or(JsonValue::Null),
                    ),
                    (
                        "likely_cause".to_string(),
                        JsonValue::Str(likely.to_string()),
                    ),
                ])
            })
            .collect();
        JsonValue::Array(rows)
    }
}

pub struct Firewall {
    rules: Vec<Rule>,
    on_rule_error: Action,
    pub ledger: Option<SenderLedger>,
}

impl Firewall {
    pub fn from_json_str(rules_json: &str) -> Result<Self, String> {
        let root = parse_json(rules_json)?;
        let on_rule_error = match root.get("on_rule_error").and_then(|v| v.as_str()) {
            Some(s) => Action::parse(s)?,
            None => Action::Reject,
        };
        let mut rules = Vec::new();
        if let Some(arr) = root.get("rules") {
            let list = arr.as_array().ok_or("rules must be an array")?;
            for spec in list {
                rules.push(parse_rule(spec)?);
            }
        }
        let ledger = match root.get("ledger") {
            Some(JsonValue::Null) | None => None,
            Some(l_spec) => Some(SenderLedger::from_json(l_spec)?),
        };
        Ok(Self {
            rules,
            on_rule_error,
            ledger,
        })
    }

    pub fn evaluate(&mut self, ctx: &MessageContext) -> Verdict {
        let _ = self.on_rule_error;
        let cooled = if let Some(ledger) = self.ledger.as_ref() {
            if ledger.enforce {
                let key = ledger.key_for(ctx);
                ledger.cooled_until(ctx.broker, &key, ctx.now).map(|until| Verdict {
                    action: Action::Drop,
                    rule: Some("cooldown".to_string()),
                    category: Some("cooled".to_string()),
                    reason: Some(format!("sender '{}' cooled until {:.0}", key, until)),
                })
            } else {
                None
            }
        } else {
            None
        };

        let verdict = match cooled {
            Some(v) => v,
            None => {
                let mut v = Verdict::allow();
                for rule in &mut self.rules {
                    let res = rule.check(ctx);
                    if res.action != Action::Allow {
                        v = res;
                        break;
                    }
                }
                v
            }
        };

        if let (Some(ledger), Some(cat)) = (self.ledger.as_mut(), verdict.category.as_deref()) {
            if cat != "rule_error" {
                let key = ledger.key_for(ctx);
                let _ = ledger.record(ctx.broker, &key, cat, ctx.now);
            }
        }

        verdict
    }
}

fn parse_brokers(spec: &JsonValue) -> Result<Option<Vec<String>>, String> {
    match spec.get("brokers") {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let s = item.as_str().ok_or("broker name must be a string")?;
                out.push(s.to_string());
            }
            Ok(Some(out))
        }
        _ => Err("brokers must be an array of strings".to_string()),
    }
}

fn parse_rule(spec: &JsonValue) -> Result<Rule, String> {
    let rule_type = spec
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or("Missing rule 'type'")?;
    let brokers = parse_brokers(spec)?;
    let action = match spec.get("action").and_then(|v| v.as_str()) {
        Some(s) => Action::parse(s)?,
        None => Action::Reject,
    };

    match rule_type {
        "max_payload_bytes" => {
            let max_bytes = spec
                .get("max_bytes")
                .and_then(|v| v.as_i64())
                .ok_or("max_payload_bytes requires integer max_bytes")?;
            if max_bytes <= 0 {
                return Err("max_bytes must be > 0".to_string());
            }
            Ok(Rule::MaxPayloadBytes {
                brokers,
                max_bytes: max_bytes as usize,
                action,
            })
        }
        "topic_shape" => {
            let max_length = spec
                .get("max_length")
                .and_then(|v| v.as_i64())
                .unwrap_or(256);
            let max_depth = spec.get("max_depth").and_then(|v| v.as_i64()).unwrap_or(16);
            if max_length <= 0 || max_depth <= 0 {
                return Err("max_length and max_depth must be > 0".to_string());
            }
            let mut deny = Vec::new();
            if let Some(JsonValue::Array(arr)) = spec.get("deny") {
                for item in arr {
                    deny.push(item.as_str().ok_or("deny pattern must be string")?.to_string());
                }
            }
            Ok(Rule::TopicShape {
                brokers,
                max_length: max_length as usize,
                max_depth: max_depth as usize,
                deny,
                action,
            })
        }
        "rate_limit" => {
            let rate = spec
                .get("rate")
                .and_then(|v| v.as_f64())
                .ok_or("rate_limit requires rate")?;
            let burst = spec.get("burst").and_then(|v| v.as_f64()).unwrap_or(rate);
            let key = spec
                .get("key")
                .and_then(|v| v.as_str())
                .unwrap_or("broker")
                .to_string();
            if !matches!(key.as_str(), "broker" | "topic" | "identity") {
                return Err("key must be 'broker', 'topic' or 'identity'".to_string());
            }
            let template = spec
                .get("template")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            if key == "identity" && template.as_deref().unwrap_or("").is_empty() {
                return Err("key='identity' requires a template".to_string());
            }
            let cost = spec
                .get("cost")
                .and_then(|v| v.as_str())
                .unwrap_or("messages")
                .to_string();
            if !matches!(cost.as_str(), "messages" | "bytes") {
                return Err("cost must be 'messages' or 'bytes'".to_string());
            }
            let max_keys = spec
                .get("max_keys")
                .and_then(|v| v.as_i64())
                .unwrap_or(10000);
            let ttl = spec.get("ttl").and_then(|v| v.as_f64()).unwrap_or(300.0);
            if max_keys <= 0 {
                return Err("max_keys must be > 0".to_string());
            }
            let limiter = KeyedLimiter::new(rate, burst, max_keys as usize, ttl)?;
            Ok(Rule::RateLimit {
                brokers,
                key,
                template,
                cost,
                action,
                limiter,
            })
        }
        "content_guard" => {
            let topic = spec
                .get("topic")
                .and_then(|v| v.as_str())
                .ok_or("content_guard requires topic")?
                .to_string();
            let format = spec
                .get("format")
                .and_then(|v| v.as_str())
                .unwrap_or("json")
                .to_string();
            if !matches!(format.as_str(), "json" | "avro" | "protobuf") {
                return Err("format must be json, avro, or protobuf".to_string());
            }
            let max_bytes = match spec.get("max_bytes") {
                None | Some(JsonValue::Null) => None,
                Some(v) => {
                    let b = v.as_i64().ok_or("max_bytes must be int")?;
                    if b <= 0 {
                        return Err("max_bytes must be > 0".to_string());
                    }
                    Some(b as usize)
                }
            };
            let max_depth = spec.get("max_depth").and_then(|v| v.as_i64()).unwrap_or(32);
            let max_keys = spec
                .get("max_keys")
                .and_then(|v| v.as_i64())
                .unwrap_or(1000);
            let max_array_length = spec
                .get("max_array_length")
                .and_then(|v| v.as_i64())
                .unwrap_or(10000);
            let max_string_length = spec
                .get("max_string_length")
                .and_then(|v| v.as_i64())
                .unwrap_or(65536);
            if max_depth <= 0 || max_keys <= 0 || max_array_length <= 0 || max_string_length <= 0 {
                return Err("limits must be > 0".to_string());
            }
            let mut required_fields = Vec::new();
            if let Some(JsonValue::Array(arr)) = spec.get("required_fields") {
                for f in arr {
                    required_fields.push(f.as_str().ok_or("required_field must be str")?.to_string());
                }
            }
            let mut field_ranges = Vec::new();
            if let Some(JsonValue::Object(entries)) = spec.get("field_ranges") {
                for (field, bounds) in entries {
                    let b_obj = bounds.as_object().ok_or("field_range bounds must be object")?;
                    if b_obj.is_empty()
                        || b_obj.iter().any(|(k, _)| k != "min" && k != "max")
                    {
                        return Err(format!("invalid field_ranges for {}", field));
                    }
                    let min_v = bounds.get("min").and_then(|v| v.as_f64());
                    let max_v = bounds.get("max").and_then(|v| v.as_f64());
                    field_ranges.push((field.clone(), min_v, max_v));
                }
            }
            let mut enums = Vec::new();
            if let Some(JsonValue::Object(entries)) = spec.get("enums") {
                for (field, allowed) in entries {
                    let arr = allowed.as_array().ok_or("enum values must be array")?;
                    enums.push((field.clone(), arr.to_vec()));
                }
            }
            Ok(Rule::ContentGuard {
                brokers,
                topic,
                format,
                max_bytes,
                max_depth: max_depth as usize,
                max_keys: max_keys as usize,
                max_array_length: max_array_length as usize,
                max_string_length: max_string_length as usize,
                required_fields,
                field_ranges,
                enums,
                action,
            })
        }
        other => Err(format!("Unknown ingress rule type '{}'", other)),
    }
}
