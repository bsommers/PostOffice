//! D9 HMAC-SHA256 Control-Plane Authentication & Action Validation.

use std::collections::BTreeMap;

use crate::json::{parse_json, write_escaped_str, JsonValue};

const K256: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4,
    0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe,
    0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f,
    0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7,
    0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc,
    0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
    0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116,
    0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
    0xc67178f2,
];

/// Compute standard FIPS 180-4 SHA-256 over `data`.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (data.len() as u64).wrapping_mul(8);
    let mut padded = Vec::with_capacity(data.len() + 72);
    padded.extend_from_slice(data);
    padded.push(0x80);
    while (padded.len() % 64) != 56 {
        padded.push(0x00);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in padded.chunks_exact(64) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            let j = i * 4;
            w[i] = u32::from_be_bytes([chunk[j], chunk[j + 1], chunk[j + 2], chunk[j + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = h[0];
        let mut b = h[1];
        let mut c = h[2];
        let mut d = h[3];
        let mut e = h[4];
        let mut f = h[5];
        let mut g = h[6];
        let mut hh = h[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K256[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            hh = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
        h[5] = h[5].wrapping_add(f);
        h[6] = h[6].wrapping_add(g);
        h[7] = h[7].wrapping_add(hh);
    }

    let mut out = [0u8; 32];
    for (i, word) in h.iter().enumerate() {
        out[i * 4..(i + 1) * 4].copy_from_slice(&word.to_be_bytes());
    }
    out
}

/// Compute RFC 2104 HMAC-SHA256 over `message` with `key`.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    let mut k_block = [0u8; 64];
    if key.len() > 64 {
        let hashed = sha256(key);
        k_block[..32].copy_from_slice(&hashed);
    } else {
        k_block[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k_block[i];
        opad[i] ^= k_block[i];
    }

    let mut inner = Vec::with_capacity(64 + message.len());
    inner.extend_from_slice(&ipad);
    inner.extend_from_slice(message);
    let inner_hash = sha256(&inner);

    let mut outer = [0u8; 96];
    outer[..64].copy_from_slice(&opad);
    outer[64..].copy_from_slice(&inner_hash);
    sha256(&outer)
}

pub fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

pub fn escape_str_inner(s: &str) -> String {
    let mut tmp = String::with_capacity(s.len() + 2);
    write_escaped_str(&mut tmp, s);
    tmp[1..tmp.len() - 1].to_string()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (&x, &y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

pub const MIN_KEY_BYTES: usize = 16;

#[derive(Clone, Debug)]
pub struct HmacSigner {
    signing_kid: String,
    keys: BTreeMap<String, Vec<u8>>,
}

impl HmacSigner {
    pub fn from_keyring_str(raw: &str) -> Result<Self, String> {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Err("at least one signing key is required".to_string());
        }
        let mut keys = BTreeMap::new();
        let mut first_kid: Option<String> = None;
        for item in trimmed.split(',') {
            let item = item.trim();
            let Some((kid_raw, secret_raw)) = item.split_once(':') else {
                return Err("POSTOFFICE_EVENT_KEYS entries must look like kid:secret".to_string());
            };
            let kid = kid_raw.trim();
            let secret = secret_raw.trim().as_bytes().to_vec();
            if kid.is_empty() || kid.contains(':') || kid.contains(',') {
                return Err(format!("invalid key id '{kid}'"));
            }
            if secret.len() < MIN_KEY_BYTES {
                return Err(format!(
                    "key '{kid}' is shorter than {MIN_KEY_BYTES} bytes"
                ));
            }
            if first_kid.is_none() {
                first_kid = Some(kid.to_string());
            }
            keys.insert(kid.to_string(), secret);
        }
        let Some(signing_kid) = first_kid else {
            return Err("at least one signing key is required".to_string());
        };
        Ok(Self { signing_kid, keys })
    }

    pub fn sign(&self, message: &[u8]) -> (String, String) {
        let key = &self.keys[&self.signing_kid];
        let mac = hmac_sha256(key, message);
        (self.signing_kid.clone(), hex_encode(&mac))
    }

    pub fn verify(&self, kid: &str, sig: &str, message: &[u8]) -> bool {
        let Some(key) = self.keys.get(kid) else {
            return false;
        };
        let expected = hex_encode(&hmac_sha256(key, message));
        constant_time_eq(expected.as_bytes(), sig.as_bytes())
    }

    pub fn sign_event(&self, payload: &str) -> String {
        let mut msg = Vec::with_capacity(6 + payload.len());
        msg.extend_from_slice(b"event\x00");
        msg.extend_from_slice(payload.as_bytes());
        let (kid, sig) = self.sign(&msg);
        JsonValue::Object(vec![
            ("kid".to_string(), JsonValue::Str(kid)),
            ("sig".to_string(), JsonValue::Str(sig)),
            ("payload".to_string(), JsonValue::Str(payload.to_string())),
        ])
        .to_json_string()
    }

    pub fn sign_state(&self, hash_name: &str, field: &str, payload: &str) -> String {
        let mut msg = Vec::with_capacity(8 + hash_name.len() + field.len() + payload.len());
        msg.extend_from_slice(b"state\x00");
        msg.extend_from_slice(hash_name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(field.as_bytes());
        msg.push(0);
        msg.extend_from_slice(payload.as_bytes());
        let (kid, sig) = self.sign(&msg);
        JsonValue::Object(vec![
            ("kid".to_string(), JsonValue::Str(kid)),
            ("sig".to_string(), JsonValue::Str(sig)),
            ("payload".to_string(), JsonValue::Str(payload.to_string())),
        ])
        .to_json_string()
    }

    pub fn sign_version(&self, version: i64) -> String {
        let msg = format!("version\x00{version}");
        let (kid, sig) = self.sign(msg.as_bytes());
        JsonValue::Object(vec![
            ("kid".to_string(), JsonValue::Str(kid)),
            ("sig".to_string(), JsonValue::Str(sig)),
        ])
        .to_json_string()
    }

    pub fn verify_event_envelope(&self, envelope_json: &str) -> bool {
        let Ok(obj) = parse_json(envelope_json) else {
            return false;
        };
        let Some((kid, sig, payload)) = extract_envelope(&obj) else {
            return false;
        };
        let mut msg = Vec::with_capacity(6 + payload.len());
        msg.extend_from_slice(b"event\x00");
        msg.extend_from_slice(payload.as_bytes());
        self.verify(kid, sig, &msg)
    }

    pub fn verify_state_envelope(&self, hash_name: &str, field: &str, envelope_json: &str) -> bool {
        let Ok(obj) = parse_json(envelope_json) else {
            return false;
        };
        let Some((kid, sig, payload)) = extract_envelope(&obj) else {
            return false;
        };
        let mut msg = Vec::with_capacity(8 + hash_name.len() + field.len() + payload.len());
        msg.extend_from_slice(b"state\x00");
        msg.extend_from_slice(hash_name.as_bytes());
        msg.push(0);
        msg.extend_from_slice(field.as_bytes());
        msg.push(0);
        msg.extend_from_slice(payload.as_bytes());
        self.verify(kid, sig, &msg)
    }

    pub fn verify_version_envelope(&self, version: i64, entry_json: &str) -> bool {
        let Ok(obj) = parse_json(entry_json) else {
            return false;
        };
        let (Some(kid), Some(sig)) = (
            obj.get("kid").and_then(|v| v.as_str()),
            obj.get("sig").and_then(|v| v.as_str()),
        ) else {
            return false;
        };
        let msg = format!("version\x00{version}");
        self.verify(kid, sig, msg.as_bytes())
    }
}

pub fn extract_envelope(obj: &JsonValue) -> Option<(&str, &str, &str)> {
    let entries = obj.as_object()?;
    if entries.len() != 3 {
        return None;
    }
    let kid = obj.get("kid")?.as_str()?;
    let sig = obj.get("sig")?.as_str()?;
    let payload = obj.get("payload")?.as_str()?;
    Some((kid, sig, payload))
}

fn is_valid_name(s: &str) -> bool {
    if s.is_empty() || s.len() > 128 {
        return false;
    }
    let bytes = s.as_bytes();
    if !bytes[0].is_ascii_alphanumeric() {
        return false;
    }
    bytes
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b':' | b'-'))
}

fn is_valid_key(s: &str) -> bool {
    if s.is_empty() || s.len() > 64 {
        return false;
    }
    let bytes = s.as_bytes();
    if !(bytes[0].is_ascii_alphabetic() || bytes[0] == b'_') {
        return false;
    }
    bytes
        .iter()
        .all(|&b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

fn is_valid_topic(s: &str) -> bool {
    if s.is_empty() || s.len() > 256 || s.contains('\0') {
        return false;
    }
    s.bytes().filter(|&b| b == b'/').count() + 1 <= 16
}

fn check_broker_kwargs(protocol: &str, kwargs: &[(String, JsonValue)]) -> Result<(), String> {
    let base_kwargs: &[(&str, &str)] = &[
        ("enable_publish_buffer", "bool"),
        ("max_publish_buffer_size", "int"),
        ("buffer_overflow_policy", "str"),
        ("max_publish_buffer_bytes", "int"),
        ("max_publish_buffer_age", "float"),
        ("buffer_clock", "any"),
        ("security_config", "dict"),
        ("circuit_breaker_threshold", "int"),
        ("circuit_breaker_timeout", "float"),
    ];
    let proto_kwargs: &[(&str, &str)] = match protocol {
        "mqtt" | "nanomq" => &[
            ("host", "str"),
            ("port", "int"),
            ("security_config", "dict"),
        ],
        "amqp" => &[
            ("host", "str"),
            ("port", "int"),
            ("user", "str"),
            ("password", "str"),
            ("security_config", "dict"),
        ],
        "kafka" => &[
            ("bootstrap_servers", "str"),
            ("security_config", "dict"),
        ],
        _ => return Err("protocol is not a registered plugin".to_string()),
    };

    for (key, val) in kwargs {
        let expected = proto_kwargs
            .iter()
            .chain(base_kwargs.iter())
            .find(|(k, _)| *k == key.as_str())
            .map(|(_, t)| *t)
            .ok_or_else(|| format!("broker kwarg '{key}' is not accepted by protocol '{protocol}'"))?;
        if matches!(val, JsonValue::Null) {
            continue;
        }
        let ok = match expected {
            "str" => matches!(val, JsonValue::Str(_)),
            "bool" => matches!(val, JsonValue::Bool(_)),
            "int" => matches!(val, JsonValue::Number(n, true) if n.fract() == 0.0),
            "float" => matches!(val, JsonValue::Number(_, _)),
            "dict" => matches!(val, JsonValue::Object(_)),
            _ => true,
        };
        if !ok {
            return Err(format!("broker kwarg '{key}' has the wrong type"));
        }
    }
    Ok(())
}

/// Validate a Control-Plane action and its `data` JSON object, matching `src/postoffice/validation.py`.
pub fn validate_action_json(action: &str, data: &JsonValue) -> Result<(), String> {
    let Some(entries) = data.as_object() else {
        return Err("data must be an object".to_string());
    };
    let has_key = |k: &str| entries.iter().any(|(ek, _)| ek == k);

    match action {
        "add_broker" => {
            for req in ["name", "protocol", "kwargs"] {
                if !has_key(req) {
                    return Err(format!("add_broker is missing: {req}"));
                }
            }
            let Some(name) = data.get("name").and_then(|v| v.as_str()) else {
                return Err("broker name is not a valid name".to_string());
            };
            if !is_valid_name(name) {
                return Err("broker name is not a valid name".to_string());
            }
            let Some(protocol) = data.get("protocol").and_then(|v| v.as_str()) else {
                return Err("protocol must be a string".to_string());
            };
            let Some(kwargs) = data.get("kwargs").and_then(|v| v.as_object()) else {
                return Err("broker kwargs must be an object".to_string());
            };
            check_broker_kwargs(protocol, kwargs)
        }
        "add_route" => {
            for req in [
                "source_broker",
                "source_topic",
                "target_broker",
                "target_topic",
                "target_kwargs",
            ] {
                if !has_key(req) {
                    return Err(format!("add_route is missing: {req}"));
                }
            }
            let Some(sb) = data.get("source_broker").and_then(|v| v.as_str()) else {
                return Err("source_broker is not a valid name".to_string());
            };
            let Some(tb) = data.get("target_broker").and_then(|v| v.as_str()) else {
                return Err("target_broker is not a valid name".to_string());
            };
            if !is_valid_name(sb) || !is_valid_name(tb) {
                return Err("invalid broker name in add_route".to_string());
            }
            let Some(st) = data.get("source_topic").and_then(|v| v.as_str()) else {
                return Err("source_topic must be a non-empty string".to_string());
            };
            let Some(tt) = data.get("target_topic").and_then(|v| v.as_str()) else {
                return Err("target_topic must be a non-empty string".to_string());
            };
            if !is_valid_topic(st) || !is_valid_topic(tt) {
                return Err("invalid topic in add_route".to_string());
            }
            let Some(tkw) = data.get("target_kwargs").and_then(|v| v.as_object()) else {
                return Err("target_kwargs must be an object".to_string());
            };
            for (k, v) in tkw {
                if !is_valid_key(k) {
                    return Err("target_kwargs has an invalid key".to_string());
                }
                if k == "engine" {
                    let Some(eng) = v.as_str() else {
                        return Err("route engine must be one of all, python, native, canary".to_string());
                    };
                    if !matches!(eng, "all" | "python" | "native" | "canary") {
                        return Err("route engine must be one of all, python, native, canary".to_string());
                    }
                }
            }
            if let Some(eng_val) = data.get("engine") {
                let Some(eng) = eng_val.as_str() else {
                    return Err("route engine must be one of all, python, native, canary".to_string());
                };
                if !matches!(eng, "all" | "python" | "native" | "canary") {
                    return Err("route engine must be one of all, python, native, canary".to_string());
                }
            }
            Ok(())
        }
        "add_subscription" => {
            for req in ["broker_name", "topic", "kwargs"] {
                if !has_key(req) {
                    return Err(format!("add_subscription is missing: {req}"));
                }
            }
            let Some(bn) = data.get("broker_name").and_then(|v| v.as_str()) else {
                return Err("broker_name is not a valid name".to_string());
            };
            if !is_valid_name(bn) {
                return Err("broker_name is not a valid name".to_string());
            }
            let Some(t) = data.get("topic").and_then(|v| v.as_str()) else {
                return Err("topic must be a non-empty string".to_string());
            };
            if !is_valid_topic(t) {
                return Err("invalid topic in add_subscription".to_string());
            }
            let Some(kw) = data.get("kwargs").and_then(|v| v.as_object()) else {
                return Err("subscription kwargs must be an object".to_string());
            };
            for (k, _) in kw {
                if !is_valid_key(k) {
                    return Err("subscription kwargs has an invalid key".to_string());
                }
            }
            Ok(())
        }
        "update_broker_security" => {
            for req in ["name", "security_config"] {
                if !has_key(req) {
                    return Err(format!("update_broker_security is missing: {req}"));
                }
            }
            let Some(name) = data.get("name").and_then(|v| v.as_str()) else {
                return Err("broker name is not a valid name".to_string());
            };
            if !is_valid_name(name) {
                return Err("broker name is not a valid name".to_string());
            }
            if data.get("security_config").and_then(|v| v.as_object()).is_none() {
                return Err("security_config must be an object".to_string());
            }
            Ok(())
        }
        "set_ingress" => {
            for (k, _) in entries {
                if !matches!(
                    k.as_str(),
                    "rules" | "on_rule_error" | "ledger" | "inflight"
                ) {
                    return Err("set_ingress has an unknown key".to_string());
                }
            }
            if let Some(rules_val) = data.get("rules") {
                let Some(arr) = rules_val.as_array() else {
                    return Err("rules must be a list of at most 64".to_string());
                };
                if arr.len() > 64 || !arr.iter().all(|r| r.as_object().is_some()) {
                    return Err("each rule must be an object (max 64)".to_string());
                }
            }
            if let Some(ore) = data.get("on_rule_error") {
                if ore.as_str().is_none() {
                    return Err("on_rule_error must be a string".to_string());
                }
            }
            if let Some(ledger) = data.get("ledger") {
                if ledger.as_object().is_none() {
                    return Err("ledger must be an object".to_string());
                }
            }
            if let Some(inflight) = data.get("inflight") {
                let Some(inf_map) = inflight.as_object() else {
                    return Err("inflight must be an object".to_string());
                };
                for (broker, lim_val) in inf_map {
                    if !is_valid_name(broker) {
                        return Err("inflight broker name is not a valid name".to_string());
                    }
                    let Some(lim) = lim_val.as_object() else {
                        return Err("inflight limit must be an object".to_string());
                    };
                    for (lk, _) in lim {
                        if !matches!(lk.as_str(), "max_inflight" | "low_water" | "lease_timeout") {
                            return Err("inflight limit has an unknown key".to_string());
                        }
                    }
                    let Some(max_inf) = lim_val.get("max_inflight").and_then(|v| v.as_i64()) else {
                        return Err("max_inflight must be a positive integer".to_string());
                    };
                    if max_inf <= 0 {
                        return Err("max_inflight must be a positive integer".to_string());
                    }
                    if let Some(low_val) = lim_val.get("low_water") {
                        if !matches!(low_val, JsonValue::Null) {
                            let Some(low) = low_val.as_i64() else {
                                return Err("low_water must be in 0..max_inflight-1".to_string());
                            };
                            if low < 0 || low >= max_inf {
                                return Err("low_water must be in 0..max_inflight-1".to_string());
                            }
                        }
                    }
                    if let Some(lease_val) = lim_val.get("lease_timeout") {
                        if !matches!(lease_val, JsonValue::Null) {
                            let Some(lease) = lease_val.as_f64() else {
                                return Err("lease_timeout must be a positive number".to_string());
                            };
                            if lease <= 0.0 {
                                return Err("lease_timeout must be a positive number".to_string());
                            }
                        }
                    }
                }
            }
            Ok(())
        }
        "set_rollout" => {
            if !has_key("native_percentage") {
                return Err("set_rollout is missing: native_percentage".to_string());
            }
            for (k, _) in entries {
                if !matches!(
                    k.as_str(),
                    "native_percentage" | "environment" | "status" | "reason"
                ) {
                    return Err("set_rollout has an unknown key".to_string());
                }
            }
            let Some(pct) = data.get("native_percentage").and_then(|v| v.as_i64()) else {
                return Err("native_percentage must be an integer in 0..100".to_string());
            };
            if !(0..=100).contains(&pct) {
                return Err("native_percentage must be an integer in 0..100".to_string());
            }
            if let Some(env_val) = data.get("environment") {
                let Some(env) = env_val.as_str() else {
                    return Err("rollout environment is not a valid name".to_string());
                };
                if !is_valid_name(env) {
                    return Err("rollout environment is not a valid name".to_string());
                }
            }
            if let Some(st_val) = data.get("status") {
                let Some(st) = st_val.as_str() else {
                    return Err("rollout status is invalid".to_string());
                };
                if !matches!(st, "active" | "paused" | "rolled_back" | "completed") {
                    return Err("rollout status is invalid".to_string());
                }
            }
            if let Some(r_val) = data.get("reason") {
                let Some(r) = r_val.as_str() else {
                    return Err("rollout reason must be a string".to_string());
                };
                if r.len() > 256 {
                    return Err("rollout reason must be at most 256 characters".to_string());
                }
            }
            Ok(())
        }
        "clear_state" => Ok(()),
        _ => Err("unknown action".to_string()),
    }
}
