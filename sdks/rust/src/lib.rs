//! PostOffice Idiomatic Client SDK for Rust
//! Wraps libpostoffice native primitives with safe, high-level abstractions.

pub use postoffice::json::{parse_json, JsonValue};
use postoffice::codec as raw_codec;
use postoffice::engine as raw_engine;
use postoffice::firewall as raw_firewall;
use postoffice::queue as raw_queue;
use postoffice::signing as raw_signing;
use postoffice::trie as raw_trie;


// ============================================================================
// Wire Codec
// ============================================================================

pub struct WireCodec;

impl WireCodec {
    /// Encode Confluent 5-byte wire frame: magic byte 0x00 + 4-byte BE schema_id + payload.
    pub fn encode_confluent(schema_id: u32, payload: &[u8]) -> Vec<u8> {
        raw_codec::encode_confluent(schema_id, payload)
    }

    /// Decode Confluent 5-byte wire frame into (schema_id, payload_slice).
    pub fn decode_confluent(framed: &[u8]) -> Result<(u32, &[u8]), String> {
        raw_codec::decode_confluent(framed)
    }

    /// Compute 32-character lowercase hex BLAKE2b-128 DLQ fingerprint.
    pub fn dlq_fingerprint(
        source_broker: &str,
        source_topic: &str,
        error_msg: &str,
        payload: &[u8],
    ) -> String {
        raw_codec::dlq_fingerprint(
            source_broker.as_bytes(),
            source_topic.as_bytes(),
            error_msg.as_bytes(),
            payload,
        )
    }
}

// ============================================================================
// Topic Matcher
// ============================================================================

pub struct TopicMatcher;

impl TopicMatcher {
    /// Match an MQTT topic against a pattern, enforcing single-level '+', multi-level '#',
    /// and '$SYS' system topic isolation (§5.0).
    pub fn matches(pattern: &str, topic: &str) -> bool {
        raw_trie::topic_matches(pattern, topic)
    }

    /// Validate pattern syntax according to MQTT standards.
    pub fn validate_pattern(pattern: &str) -> Result<Vec<&str>, String> {
        raw_trie::validate_pattern(pattern)
    }
}

// ============================================================================
// Control Signer
// ============================================================================

pub struct ControlSigner {
    inner: raw_signing::HmacSigner,
}

impl ControlSigner {
    pub fn from_keyring(keyring: &str) -> Result<Self, String> {
        let signer = raw_signing::HmacSigner::from_keyring_str(keyring)?;
        Ok(Self { inner: signer })
    }

    pub fn sign_event(&self, payload: &str) -> String {
        self.inner.sign_event(payload)
    }

    pub fn sign_state(&self, hash_name: &str, field: &str, payload: &str) -> String {
        self.inner.sign_state(hash_name, field, payload)
    }

    pub fn sign_version(&self, version: i64) -> String {
        self.inner.sign_version(version)
    }

    pub fn verify_event(&self, envelope_json: &str) -> bool {
        self.inner.verify_event_envelope(envelope_json)
    }

    pub fn verify_state(&self, hash_name: &str, field: &str, envelope_json: &str) -> bool {
        self.inner.verify_state_envelope(hash_name, field, envelope_json)
    }

    pub fn verify_version(&self, version: i64, entry_json: &str) -> bool {
        self.inner.verify_version_envelope(version, entry_json)
    }

    pub fn sign(&self, message: &[u8]) -> (String, String) {
        self.inner.sign(message)
    }

    pub fn verify(&self, kid: &str, sig: &str, message: &[u8]) -> bool {
        self.inner.verify(kid, sig, message)
    }
}

// ============================================================================
// Publish Queue
// ============================================================================

pub struct PublishQueue {
    inner: raw_queue::TargetPublishQueue,
}

impl PublishQueue {
    pub fn new(
        max_messages: usize,
        max_bytes: Option<usize>,
        max_age_sec: Option<f64>,
        overflow_policy: &str,
    ) -> Result<Self, String> {
        let q = raw_queue::TargetPublishQueue::new(
            max_messages,
            max_bytes,
            max_age_sec,
            overflow_policy,
        )?;
        Ok(Self { inner: q })
    }

    pub fn enqueue(
        &mut self,
        topic: &str,
        payload: &[u8],
        kwargs_json: &str,
        now_sec: f64,
    ) -> JsonValue {
        self.inner.enqueue(topic, payload, kwargs_json, now_sec)
    }

    pub fn drain(&mut self, limit: usize, now_sec: f64) -> JsonValue {
        self.inner.drain(limit, now_sec)
    }
}

// ============================================================================
// PostOffice Engine
// ============================================================================

fn json_to_entries(val: JsonValue) -> Vec<(String, JsonValue)> {
    match val {
        JsonValue::Object(entries) => entries,
        _ => Vec::new(),
    }
}

pub struct PostOfficeEngine {
    inner: raw_engine::Engine,
}

impl PostOfficeEngine {
    pub fn new(max_label_values: usize) -> Self {
        Self {
            inner: raw_engine::Engine::new(max_label_values),
        }
    }

    pub fn apply_action(&self, action: &str, data_json: &str) -> Result<(), String> {
        let d_val = parse_json(data_json)?;
        let mut guard = self.inner.state.lock().map_err(|e| e.to_string())?;
        match action {
            "set_dlq" => {
                let b = d_val
                    .get("broker")
                    .or_else(|| d_val.get("target_broker"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let t = d_val.get("topic").and_then(|v| v.as_str()).unwrap_or("");
                guard.set_dlq(b, t);
                Ok(())
            }
            "set_unroutable_policy" => {
                let p = d_val
                    .get("policy")
                    .and_then(|v| v.as_str())
                    .unwrap_or("nack");
                let b = d_val.get("broker").and_then(|v| v.as_str());
                guard.set_unroutable_policy(p, b)
            }
            "set_ingress" => match raw_firewall::Firewall::from_json_str(data_json) {
                Ok(fw) => {
                    guard.firewall = Some(fw);
                    Ok(())
                }
                Err(e) => Err(e),
            },
            _ => {
                guard.apply_validated_action(action, &d_val);
                Ok(())
            }
        }
    }

    pub fn register_broker(&self, name: &str, protocol: &str, kwargs_json: &str) -> Result<(), String> {
        let kw_val = if kwargs_json.is_empty() {
            JsonValue::Object(Vec::new())
        } else {
            parse_json(kwargs_json)?
        };
        let mut guard = self.inner.state.lock().map_err(|e| e.to_string())?;
        guard.register_broker(name, protocol, json_to_entries(kw_val));
        Ok(())
    }

    pub fn add_route(
        &self,
        source_broker: &str,
        source_topic: &str,
        target_broker: &str,
        target_topic: &str,
        target_kwargs_json: &str,
    ) -> Result<(), String> {
        let kw_val = if target_kwargs_json.is_empty() {
            JsonValue::Object(Vec::new())
        } else {
            parse_json(target_kwargs_json)?
        };
        let mut guard = self.inner.state.lock().map_err(|e| e.to_string())?;
        guard.add_route(
            source_broker,
            source_topic,
            target_broker,
            target_topic,
            json_to_entries(kw_val),
        ).map(|_| ())
    }

    pub fn set_dlq(&self, broker: &str, topic: &str) -> Result<(), String> {
        let mut guard = self.inner.state.lock().map_err(|e| e.to_string())?;
        guard.set_dlq(broker, topic);
        Ok(())
    }

    pub fn set_unroutable_policy(&self, policy: &str, broker: Option<&str>) -> Result<(), String> {
        let mut guard = self.inner.state.lock().map_err(|e| e.to_string())?;
        guard.set_unroutable_policy(policy, broker)
    }

    pub fn route(
        &self,
        source_broker: &str,
        source_topic: &str,
        payload: &[u8],
        source_kwargs_json: &str,
        now_sec: f64,
    ) -> Result<JsonValue, String> {
        let kw_val = if source_kwargs_json.is_empty() {
            JsonValue::Object(Vec::new())
        } else {
            parse_json(source_kwargs_json)?
        };
        let mut guard = self.inner.state.lock().map_err(|e| e.to_string())?;
        Ok(guard.route(source_broker, source_topic, payload, &kw_val, now_sec))
    }
}

// ============================================================================
// Conformance Unit Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_to_bytes(hex: &str) -> Vec<u8> {
        (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
            .collect()
    }

    #[test]
    fn test_wire_framing_vector() {
        let data = std::fs::read_to_string("../../conformance/vectors/wire_framing.json")
            .expect("read wire_framing.json");
        let root = parse_json(&data).expect("parse json");
        let framed_hex = root.get("framed_json_hex").and_then(|v| v.as_str()).unwrap();

        let raw = hex_to_bytes(framed_hex);
        let (schema_id, payload) = WireCodec::decode_confluent(&raw).expect("decode confluent");
        assert_eq!(schema_id, 1);

        let re_encoded = WireCodec::encode_confluent(schema_id, payload);
        assert_eq!(re_encoded, raw);

        if let Some(error_cases) = root.get("error_cases").and_then(|v| v.as_array()) {
            for ec in error_cases {
                let raw_hex = ec.get("raw_hex").and_then(|v| v.as_str()).unwrap();
                let bytes = hex_to_bytes(raw_hex);
                let res = WireCodec::decode_confluent(&bytes);
                assert!(res.is_err(), "expected error for case {:?}", ec.get("name"));
            }
        }
    }

    #[test]
    fn test_topic_matching_vector() {
        let data = std::fs::read_to_string("../../conformance/vectors/topic_matching.json")
            .expect("read topic_matching.json");
        let root = parse_json(&data).expect("parse json");

        if let Some(invalid_patterns) = root.get("invalid_patterns").and_then(|v| v.as_array()) {
            for item in invalid_patterns {
                let pat = item.get("pattern").and_then(|v| v.as_str()).unwrap();
                let valid = item.get("valid").and_then(|v| v.as_bool()).unwrap();
                let res = TopicMatcher::validate_pattern(pat);
                assert_eq!(res.is_ok(), valid, "pattern: {pat}");
            }
        }

        if let Some(pair_cases) = root.get("pair_cases").and_then(|v| v.as_array()) {
            for pc in pair_cases {
                let pat = pc.get("pattern").and_then(|v| v.as_str()).unwrap();
                let topic = pc.get("topic").and_then(|v| v.as_str()).unwrap();
                let expected = pc.get("matches").and_then(|v| v.as_bool()).unwrap();
                assert_eq!(TopicMatcher::matches(pat, topic), expected, "pat: {pat}, topic: {topic}");
            }
        }
    }

    #[test]
    fn test_control_plane_signing_vector() {
        let data = std::fs::read_to_string("../../conformance/vectors/control_plane_signing.json")
            .expect("read control_plane_signing.json");
        let root = parse_json(&data).expect("parse json");

        let keyring = root.get("keyring").and_then(|v| v.as_str()).unwrap();
        let event_body = root.get("event_body").and_then(|v| v.as_str()).unwrap();
        let signer = ControlSigner::from_keyring(keyring).expect("init signer");

        // Event envelope verification
        let ev_env = root.get("signed_event_envelope").unwrap().to_json_string();
        assert!(signer.verify_event(&ev_env));

        let minted_event = signer.sign_event(event_body);
        let parsed_minted = parse_json(&minted_event).unwrap();
        let expected_ev = root.get("signed_event_envelope").unwrap();
        assert_eq!(parsed_minted.get("sig"), expected_ev.get("sig"));

        // State envelope verification
        let st_env = root.get("signed_state_envelope").unwrap().to_json_string();
        assert!(signer.verify_state("postoffice:routes", "route_1", &st_env));

        let minted_state = signer.sign_state("postoffice:routes", "route_1", event_body);
        let parsed_state = parse_json(&minted_state).unwrap();
        let expected_st = root.get("signed_state_envelope").unwrap();
        assert_eq!(parsed_state.get("sig"), expected_st.get("sig"));

        // Version envelope verification
        let ver_env = root.get("signed_version_envelope").unwrap().to_json_string();
        assert!(signer.verify_version(7, &ver_env));

        let minted_ver = signer.sign_version(7);
        let parsed_ver = parse_json(&minted_ver).unwrap();
        let expected_ver = root.get("signed_version_envelope").unwrap();
        assert_eq!(parsed_ver.get("sig"), expected_ver.get("sig"));

        // Cross-domain rejection
        let cross_rejected = !signer.verify_state("postoffice:routes", "route_1", &ev_env);
        let expected_cross = root.get("cross_domain_event_as_state_rejected").and_then(|v| v.as_bool()).unwrap();
        assert_eq!(cross_rejected, expected_cross);
    }

    #[test]
    fn test_router_delivery_vector() {
        let data = std::fs::read_to_string("../../conformance/vectors/router_delivery.json")
            .expect("read router_delivery.json");
        let root = parse_json(&data).expect("parse json");
        let cases = root.get("cases").and_then(|v| v.as_array()).unwrap();

        for case in cases {
            let case_name = case.get("case").and_then(|v| v.as_str()).unwrap();
            if case_name.starts_with("unroutable_policy_") {
                let policy = case.get("policy").and_then(|v| v.as_str()).unwrap();
                let engine = PostOfficeEngine::new(10);
                engine.register_broker("src", "mqtt", "{}").unwrap();
                engine.register_broker("dlq_b", "mqtt", "{}").unwrap();
                engine.set_dlq("dlq_b", "dlq.topic").unwrap();
                engine.set_unroutable_policy(policy, None).unwrap();

                let outcome = engine.route("src", "unmatched/topic", b"hello", "{}", 1000.0).unwrap();
                let acked = outcome.get("acked").and_then(|v| v.as_bool()).unwrap();
                let nacked = outcome.get("nacked").and_then(|v| v.as_bool()).unwrap();
                let dlq_count = outcome.get("dlq_published").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0);

                let exp_acked = case.get("acked").and_then(|v| v.as_bool()).unwrap();
                let exp_nacked = case.get("nacked").and_then(|v| v.as_bool()).unwrap();
                let exp_dlq_count = case.get("dlq_count").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

                assert_eq!(acked, exp_acked, "acked mismatch on {case_name}");
                assert_eq!(nacked, exp_nacked, "nacked mismatch on {case_name}");
                assert_eq!(dlq_count, exp_dlq_count, "dlq_count mismatch on {case_name}");
            } else if case_name == "fanout_with_semantic_translation" {
                let engine = PostOfficeEngine::new(10);
                engine.register_broker("mqtt_src", "mqtt", "{}").unwrap();
                engine.register_broker("amqp_t1", "amqp", "{}").unwrap();
                engine.register_broker("kafka_t2", "kafka", "{}").unwrap();

                let route_kw = r#"{"semantic_translation": true}"#;
                engine.add_route("mqtt_src", "telemetry/+", "amqp_t1", "amqp.telemetry", route_kw).unwrap();
                engine.add_route("mqtt_src", "telemetry/+", "kafka_t2", "kafka.telemetry", route_kw).unwrap();

                let outcome = engine.route("mqtt_src", "telemetry/dev1", b"{\"v\":1}", r#"{"qos": 1}"#, 1000.0).unwrap();
                assert_eq!(outcome.get("acked").and_then(|v| v.as_bool()).unwrap(), true);

                let published = outcome.get("published").and_then(|v| v.as_array()).unwrap();
                assert_eq!(published.len(), 2);
            }
        }
    }
}
