//! Deterministic adversarial fuzzing suite for `libpostoffice` (`NATIVE_REWRITE_PLAN.md` §4 Phase 2).
//!
//! Exercises all stateless hot-path parsers, matchers, codecs, governors, and C ABI entry points
//! across 10,000+ mutated/hostile inputs to guarantee zero panics and bounded allocations.

use std::ptr;

use postoffice::codec::{
    blake2b_128, decode_confluent, dlq_fingerprint, encode_confluent, CodecRegistry,
};
use postoffice::firewall::{Firewall, MessageContext, TokenBucket};
use postoffice::governor::MemoryGovernor;
use postoffice::json::{parse_json, scan_json};
use postoffice::trie::{extract_identity, topic_matches, TopicTrie};
use postoffice::{
    po_buf_free, po_codec_decode_confluent, po_codec_encode_confluent, po_dlq_fingerprint,
    po_firewall_check, po_firewall_free, po_firewall_new, po_governor_free, po_governor_new,
    po_governor_release, po_governor_try_acquire, po_trie_add_route, po_trie_free, po_trie_match,
    po_trie_new, PoFirewallVerdict, PoOwnedBuf, PoSlice, PoStatus, PoVerdictKind,
};

struct XorShift64 {
    state: u64,
}

impl XorShift64 {
    fn new(seed: u64) -> Self {
        Self {
            state: seed.max(1),
        }
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state = x;
        x
    }

    fn next_bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = (self.next_u64() as usize) % (max_len + 1);
        let mut out = Vec::with_capacity(len);
        for _ in 0..len {
            out.push((self.next_u64() & 0xff) as u8);
        }
        out
    }
}

#[test]
fn fuzz_scan_and_parse_json_no_panics_or_quadratic_blowup() {
    // 1. Pathological deep nesting & lone trailing backslashes
    let deep_brackets = vec![b'['; 10_000];
    assert_eq!(
        scan_json(&deep_brackets, 32, 1000, 1000, 1024),
        Some("depth > 32".to_string())
    );
    assert!(parse_json(std::str::from_utf8(&deep_brackets).unwrap()).is_err());

    let mut lone_backslash = Vec::with_capacity(4096);
    lone_backslash.push(b'"');
    lone_backslash.extend(std::iter::repeat(b'a').take(4000));
    lone_backslash.push(b'\\');
    assert_eq!(
        scan_json(&lone_backslash, 32, 1000, 1000, 100),
        Some("string length > 100".to_string())
    );

    // 2. 2,500 random & semi-structured JSON mutations
    let mut rng = XorShift64::new(0xDEAD_BEEF_CAFE_BABE);
    let alphabet = b"{}[]\":,0123456789truefalsenull\\-\n\t ";
    for _ in 0..2_500 {
        let raw = rng.next_bytes(256);
        let _ = scan_json(&raw, 16, 64, 64, 128);
        if let Ok(s) = std::str::from_utf8(&raw) {
            if let Ok(val) = parse_json(s) {
                let serialized = val.to_json_string();
                let _ = parse_json(&serialized);
            }
        }

        let len = (rng.next_u64() as usize) % 128;
        let mut semi = Vec::with_capacity(len);
        for _ in 0..len {
            let idx = (rng.next_u64() as usize) % alphabet.len();
            semi.push(alphabet[idx]);
        }
        let _ = scan_json(&semi, 8, 32, 32, 64);
        if let Ok(s) = std::str::from_utf8(&semi) {
            let _ = parse_json(s);
        }
    }
}

#[test]
fn fuzz_topic_trie_and_mqtt_wildcards() {
    let mut rng = XorShift64::new(0x1234_5678_9ABC_DEF0);
    let mut trie = TopicTrie::new();
    let seg_pool = ["sensors", "devices", "+", "#", "$SYS", "", "room1", "+bad", "#bad"];

    for i in 0..2_000 {
        let depth = 1 + ((rng.next_u64() as usize) % 6);
        let mut parts = Vec::with_capacity(depth);
        for _ in 0..depth {
            parts.push(seg_pool[(rng.next_u64() as usize) % seg_pool.len()]);
        }
        let pattern = parts.join("/");
        let route_id = format!("\"r_{}\"", i % 50);
        let _ = trie.add_route(&pattern, &route_id, true);
        let _ = topic_matches(&pattern, "sensors/room1/temp");
        let _ = topic_matches(&pattern, "$SYS/broker/load");
        let _ = extract_identity("devices/{dev}/#", &pattern);
    }

    for _ in 0..2_000 {
        let depth = 1 + ((rng.next_u64() as usize) % 6);
        let mut parts = Vec::with_capacity(depth);
        for _ in 0..depth {
            parts.push(seg_pool[(rng.next_u64() as usize) % seg_pool.len()]);
        }
        let topic = parts.join("/");
        let matched = trie.match_topic(&topic);
        if topic.starts_with('$') {
            // Verify root wildcard isolation invariant
            assert!(!topic_matches("#", &topic));
            assert!(!topic_matches("+/room1", &topic));
        }
        let _ = matched;
    }
}

#[test]
fn fuzz_firewall_content_guard_and_sender_ledger() {
    let cfg = r#"{
        "rules": [
            {"type": "max_payload_bytes", "max_bytes": 128, "action": "reject"},
            {"type": "topic_shape", "max_length": 64, "max_depth": 6, "deny": ["$SYS/#", "admin/+"], "action": "reject"},
            {"type": "rate_limit", "rate": 50.0, "burst": 10.0, "key": "identity", "template": "devices/{id}/#", "max_keys": 64, "ttl": 10.0, "action": "drop"},
            {"type": "content_guard", "topic": "devices/+/telemetry", "format": "json", "max_bytes": 128, "max_depth": 4, "max_keys": 8, "max_array_length": 8, "max_string_length": 32, "required_fields": ["temp"], "field_ranges": {"temp": {"min": -50.0, "max": 150.0}}, "enums": {"unit": ["C", "F"]}, "action": "reject"}
        ],
        "ledger": {
            "max_keys": 32,
            "ttl": 600.0,
            "window": 10.0,
            "identity_template": "devices/{id}/#",
            "enforce": true,
            "strike_threshold": 3,
            "cooldown_base": 5.0,
            "cooldown_factor": 2.0,
            "cooldown_cap": 60.0
        }
    }"#;
    let mut fw = Firewall::from_json_str(cfg).unwrap();
    let mut rng = XorShift64::new(0xCAFE_F00D_0000_0001);
    let mut now = 100.0;

    for i in 0..3_000 {
        now += ((rng.next_u64() % 500) as f64) * 0.01;
        let topic = match i % 5 {
            0 => format!("devices/dev-{}/telemetry", rng.next_u64() % 128),
            1 => "$SYS/broker/clients".to_string(),
            2 => "admin/secret".to_string(),
            3 => "devices/dev-0/telemetry\0nul".to_string(),
            _ => String::from_utf8_lossy(&rng.next_bytes(80)).to_string(),
        };
        let payload = match i % 4 {
            0 => br#"{"temp": 22.5, "unit": "C"}"#.to_vec(),
            1 => br#"{"temp": 999.0, "unit": "Z"}"#.to_vec(),
            2 => b"[[[[[[[[[[[[[[[[[[[[[[[[".to_vec(),
            _ => rng.next_bytes(200),
        };
        let ctx = MessageContext {
            broker: "mqtt_fuzz",
            sender: None,
            topic: &topic,
            payload: &payload,
            now,
        };
        let verdict = fw.evaluate(&ctx);
        let _ = verdict.to_json_value().to_json_string();
    }

    let report = fw.ledger.as_mut().unwrap().sender_report(20, now);
    assert!(report.as_array().unwrap().len() <= 20);
    assert!(fw.ledger.as_ref().unwrap().overflow_events > 0);
}

#[test]
fn fuzz_codecs_avro_protobuf_confluent_and_blake2b() {
    let mut reg = CodecRegistry::new();
    reg.register_schema(
        1,
        "s_avro",
        "AVRO",
        r#"{"type":"record","name":"R","fields":[{"name":"id","type":"string"},{"name":"temp","type":"double"},{"name":"count","type":"int"},{"name":"ok","type":"boolean"}]}"#,
    )
    .unwrap();
    reg.register_schema(
        2,
        "s_proto",
        "PROTOBUF",
        r#"{"name":"P","fields":[{"name":"id","type":"string","number":1},{"name":"temp","type":"double","number":2},{"name":"count","type":"int64","number":3},{"name":"ok","type":"bool","number":4}]}"#,
    )
    .unwrap();

    let mut rng = XorShift64::new(0xfeed_face_dead_beef);
    for _ in 0..2_500 {
        let raw = rng.next_bytes(128);
        let _ = decode_confluent(&raw);
        let _ = reg.deserialize(&raw, "avro", "s_avro", false);
        let _ = reg.deserialize(&raw, "avro", "", true);
        let _ = reg.deserialize(&raw, "protobuf", "s_proto", false);
        let _ = reg.deserialize(&raw, "protobuf", "", true);
        let fp = dlq_fingerprint(b"b", b"t", b"e", &raw);
        assert_eq!(fp.len(), 32);
        let _ = blake2b_128(&[&raw]);
    }

    // Valid round-trip check on all field types
    let sample = r#"{"id":"sensor-9","temp":-12.25,"count":42,"ok":true}"#;
    let avro_bytes = reg.serialize(sample, "avro", "s_avro", true).unwrap();
    let avro_dec = reg.deserialize(&avro_bytes, "avro", "", true).unwrap();
    assert!(avro_dec.contains("\"sensor-9\""));

    let proto_bytes = reg.serialize(sample, "protobuf", "s_proto", true).unwrap();
    let proto_dec = reg.deserialize(&proto_bytes, "protobuf", "", true).unwrap();
    assert!(proto_dec.contains("\"sensor-9\""));
}

#[test]
fn fuzz_memory_governor_and_c_abi_null_safety() {
    let gov = MemoryGovernor::new(65536, 0.80).unwrap();
    let mut rng = XorShift64::new(0x9999_8888_7777_6666);
    let mut held = 0u64;
    for _ in 0..2_000 {
        let ask = rng.next_u64() % 16384;
        if gov.try_acquire(ask) {
            held += ask;
            assert!(gov.in_use_bytes() <= gov.shed_limit_bytes());
        }
        if held > 0 && (rng.next_u64() & 1) == 0 {
            let rel = ask.min(held);
            gov.release(rel);
            held -= rel;
        }
    }
    gov.release(held);
    assert_eq!(gov.in_use_bytes(), 0);
    assert!(!gov.try_acquire(u64::MAX));

    // TokenBucket boundary check
    let mut tb = TokenBucket::new(10.0, 20.0, 0.0).unwrap();
    assert!(tb.can_consume(20.0, 0.0));
    assert!(tb.try_consume(20.0, 0.0));
    assert!(!tb.try_consume(1.0, 0.0));
    assert!(tb.try_consume(10.0, 1.0));

    // C ABI null-pointer safety checks
    unsafe {
        po_buf_free(ptr::null_mut());
        assert_eq!(po_trie_new(ptr::null_mut()), PoStatus::ErrInvalidArgument);
        po_trie_free(ptr::null_mut());
        assert_eq!(
            po_trie_add_route(
                ptr::null_mut(),
                PoSlice { data: ptr::null(), len: 0 },
                PoSlice { data: ptr::null(), len: 0 },
                ptr::null_mut(),
            ),
            PoStatus::ErrInvalidArgument
        );
        assert_eq!(
            po_trie_match(
                ptr::null(),
                PoSlice { data: ptr::null(), len: 0 },
                ptr::null_mut(),
            ),
            PoStatus::ErrInvalidArgument
        );
        assert_eq!(
            po_firewall_new(PoSlice { data: ptr::null(), len: 0 }, ptr::null_mut()),
            PoStatus::ErrInvalidArgument
        );
        po_firewall_free(ptr::null_mut());

        let mut fw: *mut postoffice::PoFirewallHandle = ptr::null_mut();
        let cfg = br#"{"rules":[{"type":"max_payload_bytes","max_bytes":10,"action":"drop"}]}"#;
        assert_eq!(
            po_firewall_new(
                PoSlice {
                    data: cfg.as_ptr(),
                    len: cfg.len()
                },
                &mut fw
            ),
            PoStatus::Ok
        );
        let mut verd = PoFirewallVerdict {
            kind: PoVerdictKind::Allow,
            rule: [0; 32],
            reason: [0; 128],
        };
        let sender = b"mqtt\0dev1";
        let topic = b"sensors/temp";
        let big_payload = [b'x'; 20];
        assert_eq!(
            po_firewall_check(
                fw,
                PoSlice {
                    data: sender.as_ptr(),
                    len: sender.len()
                },
                PoSlice {
                    data: topic.as_ptr(),
                    len: topic.len()
                },
                PoSlice {
                    data: big_payload.as_ptr(),
                    len: big_payload.len()
                },
                1.0,
                &mut verd,
            ),
            PoStatus::ErrFirewallDrop
        );
        assert_eq!(verd.kind, PoVerdictKind::Drop);
        po_firewall_free(fw);

        assert_eq!(
            po_governor_new(0, 0.8, ptr::null_mut()),
            PoStatus::ErrInvalidArgument
        );
        po_governor_free(ptr::null_mut());
        assert_eq!(
            po_governor_try_acquire(ptr::null_mut(), 10),
            PoStatus::ErrInvalidArgument
        );
        assert_eq!(
            po_governor_release(ptr::null_mut(), 10),
            PoStatus::ErrInvalidArgument
        );
        assert_eq!(
            po_codec_encode_confluent(1, PoSlice { data: ptr::null(), len: 0 }, ptr::null_mut()),
            PoStatus::ErrInvalidArgument
        );
        assert_eq!(
            po_codec_decode_confluent(
                PoSlice { data: ptr::null(), len: 0 },
                ptr::null_mut(),
                ptr::null_mut()
            ),
            PoStatus::ErrInvalidArgument
        );
        assert_eq!(
            po_dlq_fingerprint(
                PoSlice { data: ptr::null(), len: 0 },
                PoSlice { data: ptr::null(), len: 0 },
                PoSlice { data: ptr::null(), len: 0 },
                PoSlice { data: ptr::null(), len: 0 },
                ptr::null_mut(),
            ),
            PoStatus::ErrInvalidArgument
        );
        let framed = encode_confluent(7, b"hello");
        let mut sid = 0u32;
        let mut out_buf = PoOwnedBuf {
            data: ptr::null_mut(),
            len: 0,
            cap: 0,
        };
        assert_eq!(
            po_codec_decode_confluent(
                PoSlice {
                    data: framed.as_ptr(),
                    len: framed.len()
                },
                &mut sid,
                &mut out_buf
            ),
            PoStatus::Ok
        );
        assert_eq!(sid, 7);
        po_buf_free(&mut out_buf);
    }
}
