//! `TopicTrie` (`po_trie_t`) and MQTT v3.1.1/v5.0 §4.7.2 wildcard matching.
//!
//! Enforces:
//! - `+` matches exactly one segment.
//! - `#` matches zero or more trailing segments and must be the final segment.
//! - Root `+` or `#` (at segment index 0) never matches `$`-prefixed topics.
//! - Idempotent `add_route` and `remove_route`.

use std::collections::HashMap;

use crate::json::{parse_json, JsonValue};

#[derive(Default, Clone, Debug)]
struct TrieNode {
    children: HashMap<String, TrieNode>,
    wildcard_single: Option<Box<TrieNode>>,
    wildcard_multi: Vec<String>,
    routes: Vec<String>,
}

#[derive(Default, Clone, Debug)]
pub struct TopicTrie {
    root: TrieNode,
    count: usize,
}

pub fn validate_pattern(pattern: &str) -> Result<Vec<&str>, String> {
    let segments: Vec<&str> = pattern.split('/').collect();
    for (i, &seg) in segments.iter().enumerate() {
        if seg == "#" && i != segments.len() - 1 {
            return Err(format!(
                "Multi-level wildcard '#' must be the last topic segment: '{}'",
                pattern
            ));
        }
        if seg.contains('+') && seg != "+" {
            return Err(format!(
                "Single-level wildcard '+' must occupy the entire segment: '{}'",
                pattern
            ));
        }
        if seg.contains('#') && seg != "#" {
            return Err(format!(
                "Multi-level wildcard '#' must occupy the entire segment: '{}'",
                pattern
            ));
        }
    }
    Ok(segments)
}

pub fn topic_matches(pattern: &str, topic: &str) -> bool {
    let p: Vec<&str> = pattern.split('/').collect();
    let t: Vec<&str> = topic.split('/').collect();
    if topic.starts_with('$') && !p.is_empty() && (p[0] == "+" || p[0] == "#") {
        return false;
    }
    for (i, &seg) in p.iter().enumerate() {
        if seg == "#" {
            return true;
        }
        if i >= t.len() || (seg != "+" && seg != t[i]) {
            return false;
        }
    }
    p.len() == t.len()
}

pub fn extract_identity(template: &str, topic: &str) -> Option<String> {
    let t: Vec<&str> = topic.split('/').collect();
    let mut ident: Option<String> = None;
    for (i, seg) in template.split('/').enumerate() {
        if seg == "#" {
            return ident;
        }
        if i >= t.len() {
            return None;
        }
        if seg.starts_with('{') && seg.ends_with('}') {
            if ident.is_none() {
                ident = Some(t[i].to_string());
            }
        } else if seg != "+" && seg != t[i] {
            return None;
        }
    }
    ident
}

impl TopicTrie {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Inserts a route JSON payload under `pattern`.
    /// If `idempotent` is true, skips inserting an identical `route_json` at the same node
    /// and returns `Ok(false)`.
    pub fn add_route(
        &mut self,
        pattern: &str,
        route_json: &str,
        idempotent: bool,
    ) -> Result<bool, String> {
        let segments = validate_pattern(pattern)?;
        let mut node = &mut self.root;
        for seg in segments {
            if seg == "#" {
                if idempotent && node.wildcard_multi.iter().any(|r| r == route_json) {
                    return Ok(false);
                }
                node.wildcard_multi.push(route_json.to_string());
                self.count += 1;
                return Ok(true);
            } else if seg == "+" {
                node = node.wildcard_single.get_or_insert_with(|| Box::new(TrieNode::default()));
            } else {
                node = node.children.entry(seg.to_string()).or_default();
            }
        }
        if idempotent && node.routes.iter().any(|r| r == route_json) {
            return Ok(false);
        }
        node.routes.push(route_json.to_string());
        self.count += 1;
        Ok(true)
    }

    pub fn remove_route(
        &mut self,
        pattern: &str,
        target_broker: &str,
        target_topic: &str,
    ) -> Result<usize, String> {
        let segments = validate_pattern(pattern)?;
        let mut node = &mut self.root;
        for seg in segments {
            if seg == "#" {
                let removed = retain_non_matching(&mut node.wildcard_multi, target_broker, target_topic);
                self.count = self.count.saturating_sub(removed);
                return Ok(removed);
            } else if seg == "+" {
                match node.wildcard_single.as_mut() {
                    Some(next) => node = next,
                    None => return Ok(0),
                }
            } else {
                match node.children.get_mut(seg) {
                    Some(next) => node = next,
                    None => return Ok(0),
                }
            }
        }
        let removed = retain_non_matching(&mut node.routes, target_broker, target_topic);
        self.count = self.count.saturating_sub(removed);
        Ok(removed)
    }

    pub fn match_topic(&self, topic: &str) -> Vec<String> {
        let segments: Vec<&str> = topic.split('/').collect();
        let is_dollar = topic.starts_with('$');
        let mut results = Vec::new();
        Self::dfs(&self.root, &segments, 0, is_dollar, &mut results);
        results
    }

    pub fn match_topic_json_array(&self, topic: &str) -> String {
        let matched = self.match_topic(topic);
        let mut out = String::with_capacity(2 + matched.len() * 16);
        out.push('[');
        for (i, item) in matched.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(item);
        }
        out.push(']');
        out
    }

    fn dfs(
        node: &TrieNode,
        segments: &[&str],
        idx: usize,
        is_dollar: bool,
        results: &mut Vec<String>,
    ) {
        if !node.wildcard_multi.is_empty() && !(idx == 0 && is_dollar) {
            results.extend(node.wildcard_multi.iter().cloned());
        }
        if idx == segments.len() {
            if !node.routes.is_empty() {
                results.extend(node.routes.iter().cloned());
            }
            return;
        }
        let seg = segments[idx];
        if let Some(child) = node.children.get(seg) {
            Self::dfs(child, segments, idx + 1, is_dollar, results);
        }
        if let Some(plus) = node.wildcard_single.as_ref() {
            if !(idx == 0 && is_dollar) {
                Self::dfs(plus, segments, idx + 1, is_dollar, results);
            }
        }
    }
}

fn retain_non_matching(list: &mut Vec<String>, target_broker: &str, target_topic: &str) -> usize {
    let before = list.len();
    if target_broker.is_empty() && target_topic.is_empty() {
        list.clear();
        return before;
    }
    list.retain(|r_json| {
        if let Ok(JsonValue::Object(entries)) = parse_json(r_json) {
            let b = entries
                .iter()
                .find(|(k, _)| k == "target_client" || k == "target_broker" || k == "broker")
                .and_then(|(_, v)| v.as_str())
                .unwrap_or("");
            let t = entries
                .iter()
                .find(|(k, _)| k == "target_topic" || k == "topic")
                .and_then(|(_, v)| v.as_str())
                .unwrap_or("");
            !(b == target_broker && t == target_topic)
        } else {
            true
        }
    });
    before - list.len()
}
