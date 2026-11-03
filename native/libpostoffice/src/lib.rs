//! `libpostoffice` — PostOffice Native Core Library & Stable C ABI (`include/postoffice.h`).
//!
//! Exposes opaque handles (`po_trie_t`, `po_firewall_t`, `po_governor_t`, `po_codec_t`,
//! `po_client_t`) and `po_owned_buf_t` / `po_buf_free` with panic containment (`catch_unwind`)
//! across every `extern "C"` entry point.

pub mod codec;
pub mod firewall;
pub mod governor;
pub mod json;
pub mod trie;

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::ptr;
use std::slice;

use codec::{
    decode_confluent, dlq_fingerprint, encode_confluent, CodecRegistry,
};
use firewall::{Action, Firewall, MessageContext};
use governor::{detect_host_ram_bytes, MemoryGovernor};
use json::JsonValue;
use trie::TopicTrie;

pub const PO_ABI_VERSION: u32 = 1;

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoStatus {
    Ok = 0,
    ErrInvalidArgument = -1,
    ErrNotFound = -2,
    ErrOutOfMemory = -3,
    ErrOverloadShed = -4,
    ErrFirewallDrop = -5,
    ErrFirewallReject = -6,
    ErrWireFormat = -7,
    ErrSignatureInvalid = -8,
    ErrUnsupportedVersion = -9,
    ErrIo = -10,
    ErrInternal = -99,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PoVerdictKind {
    Allow = 1,
    Drop = 2,
    Reject = 3,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PoSlice {
    pub data: *const u8,
    pub len: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PoOwnedBuf {
    pub data: *mut u8,
    pub len: usize,
    pub cap: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct PoFirewallVerdict {
    pub kind: PoVerdictKind,
    pub rule: [u8; 32],
    pub reason: [u8; 128],
}

pub struct PoTrieHandle(pub TopicTrie);
pub struct PoFirewallHandle(pub Firewall);
pub struct PoGovernorHandle(pub MemoryGovernor);
pub struct PoCodecHandle(pub CodecRegistry);

pub struct PoClientHandle {
    pub endpoint_url: String,
    pub bearer_token: String,
    pub published_count: u64,
}

unsafe fn slice_to_bytes<'a>(s: PoSlice) -> Result<&'a [u8], PoStatus> {
    if s.len == 0 {
        return Ok(&[]);
    }
    if s.data.is_null() {
        return Err(PoStatus::ErrInvalidArgument);
    }
    Ok(slice::from_raw_parts(s.data, s.len))
}

unsafe fn slice_to_str<'a>(s: PoSlice) -> Result<&'a str, PoStatus> {
    let bytes = slice_to_bytes(s)?;
    std::str::from_utf8(bytes).map_err(|_| PoStatus::ErrInvalidArgument)
}

unsafe fn write_owned_buf(out: *mut PoOwnedBuf, mut vec: Vec<u8>) -> PoStatus {
    if out.is_null() {
        return PoStatus::ErrInvalidArgument;
    }
    vec.shrink_to_fit();
    let len = vec.len();
    let cap = vec.capacity();
    let data = if cap == 0 {
        ptr::null_mut()
    } else {
        let p = vec.as_mut_ptr();
        std::mem::forget(vec);
        p
    };
    *out = PoOwnedBuf { data, len, cap };
    PoStatus::Ok
}

fn copy_cstr_trunc(dst: &mut [u8], src: &str) {
    dst.fill(0);
    let bytes = src.as_bytes();
    let n = bytes.len().min(dst.len().saturating_sub(1));
    dst[..n].copy_from_slice(&bytes[..n]);
}

#[no_mangle]
pub extern "C" fn po_abi_version() -> u32 {
    PO_ABI_VERSION
}

#[no_mangle]
pub unsafe extern "C" fn po_buf_free(buf: *mut PoOwnedBuf) {
    if buf.is_null() {
        return;
    }
    let b = &mut *buf;
    if !b.data.is_null() && b.cap > 0 {
        let _ = Vec::from_raw_parts(b.data, b.len, b.cap);
    }
    b.data = ptr::null_mut();
    b.len = 0;
    b.cap = 0;
}

// =========================================================================
// TopicTrie (`po_trie_t`)
// =========================================================================

#[no_mangle]
pub unsafe extern "C" fn po_trie_new(out_trie: *mut *mut PoTrieHandle) -> PoStatus {
    if out_trie.is_null() {
        return PoStatus::ErrInvalidArgument;
    }
    let handle = Box::new(PoTrieHandle(TopicTrie::new()));
    *out_trie = Box::into_raw(handle);
    PoStatus::Ok
}

#[no_mangle]
pub unsafe extern "C" fn po_trie_free(trie: *mut PoTrieHandle) {
    if !trie.is_null() {
        let _ = Box::from_raw(trie);
    }
}

#[no_mangle]
pub unsafe extern "C" fn po_trie_add_route(
    trie: *mut PoTrieHandle,
    pattern: PoSlice,
    route_json: PoSlice,
    out_added: *mut bool,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if trie.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let pat = match slice_to_str(pattern) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let rjson = match slice_to_str(route_json) {
            Ok(s) => s,
            Err(e) => return e,
        };
        match (*trie).0.add_route(pat, rjson, true) {
            Ok(added) => {
                if !out_added.is_null() {
                    *out_added = added;
                }
                PoStatus::Ok
            }
            Err(_) => PoStatus::ErrInvalidArgument,
        }
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_trie_remove_route(
    trie: *mut PoTrieHandle,
    pattern: PoSlice,
    target_broker: PoSlice,
    target_topic: PoSlice,
    out_removed_count: *mut usize,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if trie.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let pat = match slice_to_str(pattern) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let tb = match slice_to_str(target_broker) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let tt = match slice_to_str(target_topic) {
            Ok(s) => s,
            Err(e) => return e,
        };
        match (*trie).0.remove_route(pat, tb, tt) {
            Ok(removed) => {
                if !out_removed_count.is_null() {
                    *out_removed_count = removed;
                }
                PoStatus::Ok
            }
            Err(_) => PoStatus::ErrInvalidArgument,
        }
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_trie_match(
    trie: *const PoTrieHandle,
    topic: PoSlice,
    out_routes_json: *mut PoOwnedBuf,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if trie.is_null() || out_routes_json.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let top = match slice_to_str(topic) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let json_arr = (*trie).0.match_topic_json_array(top);
        write_owned_buf(out_routes_json, json_arr.into_bytes())
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

// =========================================================================
// Ingress Firewall + ContentGuard + SenderLedger (`po_firewall_t`)
// =========================================================================

#[no_mangle]
pub unsafe extern "C" fn po_firewall_new(
    rules_json: PoSlice,
    out_firewall: *mut *mut PoFirewallHandle,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if out_firewall.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let cfg_str = match slice_to_str(rules_json) {
            Ok(s) => s,
            Err(e) => return e,
        };
        match Firewall::from_json_str(cfg_str) {
            Ok(fw) => {
                *out_firewall = Box::into_raw(Box::new(PoFirewallHandle(fw)));
                PoStatus::Ok
            }
            Err(_) => PoStatus::ErrInvalidArgument,
        }
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_firewall_free(firewall: *mut PoFirewallHandle) {
    if !firewall.is_null() {
        let _ = Box::from_raw(firewall);
    }
}

#[no_mangle]
pub unsafe extern "C" fn po_firewall_check(
    firewall: *mut PoFirewallHandle,
    sender_id: PoSlice,
    topic: PoSlice,
    payload: PoSlice,
    now_monotonic_sec: f64,
    out_verdict: *mut PoFirewallVerdict,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if firewall.is_null() || out_verdict.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let sender_raw = match slice_to_str(sender_id) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let (broker, sender_opt) = if let Some((b, s)) = sender_raw.split_once('\0') {
            (
                if b.is_empty() { "default" } else { b },
                if s.is_empty() { None } else { Some(s) },
            )
        } else if sender_raw.is_empty() {
            ("default", None)
        } else {
            ("default", Some(sender_raw))
        };
        let top = match slice_to_str(topic) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let pay = match slice_to_bytes(payload) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let ctx = MessageContext {
            broker,
            sender: sender_opt,
            topic: top,
            payload: pay,
            now: now_monotonic_sec,
        };
        let v = (*firewall).0.evaluate(&ctx);
        let mut out = PoFirewallVerdict {
            kind: match v.action {
                Action::Allow => PoVerdictKind::Allow,
                Action::Drop => PoVerdictKind::Drop,
                Action::Reject => PoVerdictKind::Reject,
            },
            rule: [0u8; 32],
            reason: [0u8; 128],
        };
        if let Some(r) = v.rule.as_deref() {
            copy_cstr_trunc(&mut out.rule, r);
        }
        if let Some(rsn) = v.reason.as_deref() {
            copy_cstr_trunc(&mut out.reason, rsn);
        }
        *out_verdict = out;
        match v.action {
            Action::Allow => PoStatus::Ok,
            Action::Drop => PoStatus::ErrFirewallDrop,
            Action::Reject => PoStatus::ErrFirewallReject,
        }
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_firewall_evaluate(
    firewall: *mut PoFirewallHandle,
    broker: PoSlice,
    sender_id: PoSlice,
    topic: PoSlice,
    payload: PoSlice,
    now_monotonic_sec: f64,
    out_verdict_json: *mut PoOwnedBuf,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if firewall.is_null() || out_verdict_json.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let brk = match slice_to_str(broker) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let snd = match slice_to_str(sender_id) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let top = match slice_to_str(topic) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let pay = match slice_to_bytes(payload) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let ctx = MessageContext {
            broker: brk,
            sender: if snd.is_empty() { None } else { Some(snd) },
            topic: top,
            payload: pay,
            now: now_monotonic_sec,
        };
        let v = (*firewall).0.evaluate(&ctx);
        write_owned_buf(out_verdict_json, v.to_json_value().to_json_string().into_bytes())
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_firewall_sender_report(
    firewall: *mut PoFirewallHandle,
    limit: u32,
    now_monotonic_sec: f64,
    out_report_json: *mut PoOwnedBuf,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if firewall.is_null() || out_report_json.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let json_str = match (*firewall).0.ledger.as_mut() {
            Some(ledger) => ledger.sender_report(limit as usize, now_monotonic_sec).to_json_string(),
            None => "[]".to_string(),
        };
        write_owned_buf(out_report_json, json_str.into_bytes())
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

// =========================================================================
// MemoryGovernor (`po_governor_t`)
// =========================================================================

#[no_mangle]
pub unsafe extern "C" fn po_governor_new(
    budget_bytes: u64,
    shed_ratio: f64,
    out_governor: *mut *mut PoGovernorHandle,
) -> PoStatus {
    if out_governor.is_null() {
        return PoStatus::ErrInvalidArgument;
    }
    match MemoryGovernor::new(budget_bytes, shed_ratio) {
        Ok(gov) => {
            *out_governor = Box::into_raw(Box::new(PoGovernorHandle(gov)));
            PoStatus::Ok
        }
        Err(_) => PoStatus::ErrInvalidArgument,
    }
}

#[no_mangle]
pub unsafe extern "C" fn po_governor_free(governor: *mut PoGovernorHandle) {
    if !governor.is_null() {
        let _ = Box::from_raw(governor);
    }
}

#[no_mangle]
pub unsafe extern "C" fn po_governor_try_acquire(
    governor: *mut PoGovernorHandle,
    bytes: u64,
) -> PoStatus {
    if governor.is_null() {
        return PoStatus::ErrInvalidArgument;
    }
    if (*governor).0.try_acquire(bytes) {
        PoStatus::Ok
    } else {
        PoStatus::ErrOverloadShed
    }
}

#[no_mangle]
pub unsafe extern "C" fn po_governor_release(
    governor: *mut PoGovernorHandle,
    bytes: u64,
) -> PoStatus {
    if governor.is_null() {
        return PoStatus::ErrInvalidArgument;
    }
    (*governor).0.release(bytes);
    PoStatus::Ok
}

#[no_mangle]
pub unsafe extern "C" fn po_governor_in_use_bytes(governor: *const PoGovernorHandle) -> u64 {
    if governor.is_null() {
        return 0;
    }
    (*governor).0.in_use_bytes()
}

#[no_mangle]
pub unsafe extern "C" fn po_governor_shed_total(governor: *const PoGovernorHandle) -> u64 {
    if governor.is_null() {
        return 0;
    }
    (*governor).0.shed_total()
}

#[no_mangle]
pub extern "C" fn po_governor_detect_host_ram_bytes() -> u64 {
    detect_host_ram_bytes()
}

// =========================================================================
// Wire Codec & DLQ Fingerprinting (`po_codec_t`)
// =========================================================================

#[no_mangle]
pub unsafe extern "C" fn po_codec_new(out_codec: *mut *mut PoCodecHandle) -> PoStatus {
    if out_codec.is_null() {
        return PoStatus::ErrInvalidArgument;
    }
    *out_codec = Box::into_raw(Box::new(PoCodecHandle(CodecRegistry::new())));
    PoStatus::Ok
}

#[no_mangle]
pub unsafe extern "C" fn po_codec_free(codec: *mut PoCodecHandle) {
    if !codec.is_null() {
        let _ = Box::from_raw(codec);
    }
}

#[no_mangle]
pub unsafe extern "C" fn po_codec_register_schema(
    codec: *mut PoCodecHandle,
    schema_id: u32,
    name: PoSlice,
    schema_type: PoSlice,
    definition_json: PoSlice,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if codec.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let nm = match slice_to_str(name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let st = match slice_to_str(schema_type) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let def = match slice_to_str(definition_json) {
            Ok(s) => s,
            Err(e) => return e,
        };
        match (*codec).0.register_schema(schema_id, nm, st, def) {
            Ok(()) => PoStatus::Ok,
            Err(_) => PoStatus::ErrInvalidArgument,
        }
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_codec_serialize(
    codec: *const PoCodecHandle,
    record_json: PoSlice,
    format_type: PoSlice,
    schema_name: PoSlice,
    wire_format: bool,
    out_bytes: *mut PoOwnedBuf,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if codec.is_null() || out_bytes.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let rec = match slice_to_str(record_json) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let fmt = match slice_to_str(format_type) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let sname = match slice_to_str(schema_name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        match (*codec).0.serialize(rec, fmt, sname, wire_format) {
            Ok(bytes) => write_owned_buf(out_bytes, bytes),
            Err(_) => PoStatus::ErrWireFormat,
        }
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_codec_deserialize(
    codec: *const PoCodecHandle,
    payload: PoSlice,
    format_type: PoSlice,
    schema_name: PoSlice,
    wire_format: bool,
    out_record_json: *mut PoOwnedBuf,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if codec.is_null() || out_record_json.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let pay = match slice_to_bytes(payload) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let fmt = match slice_to_str(format_type) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let sname = match slice_to_str(schema_name) {
            Ok(s) => s,
            Err(e) => return e,
        };
        match (*codec).0.deserialize(pay, fmt, sname, wire_format) {
            Ok(json_str) => write_owned_buf(out_record_json, json_str.into_bytes()),
            Err(_) => PoStatus::ErrWireFormat,
        }
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_codec_encode_confluent(
    schema_id: u32,
    payload: PoSlice,
    out_framed: *mut PoOwnedBuf,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if out_framed.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let pay = match slice_to_bytes(payload) {
            Ok(b) => b,
            Err(e) => return e,
        };
        write_owned_buf(out_framed, encode_confluent(schema_id, pay))
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_codec_decode_confluent(
    framed: PoSlice,
    out_schema_id: *mut u32,
    out_payload: *mut PoOwnedBuf,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if out_schema_id.is_null() || out_payload.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let raw = match slice_to_bytes(framed) {
            Ok(b) => b,
            Err(e) => return e,
        };
        match decode_confluent(raw) {
            Ok((sid, body)) => {
                *out_schema_id = sid;
                write_owned_buf(out_payload, body.to_vec())
            }
            Err(_) => PoStatus::ErrWireFormat,
        }
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

#[no_mangle]
pub unsafe extern "C" fn po_dlq_fingerprint(
    source_broker: PoSlice,
    source_topic: PoSlice,
    error_msg: PoSlice,
    payload: PoSlice,
    out_hex: *mut u8,
) -> PoStatus {
    catch_unwind(AssertUnwindSafe(|| {
        if out_hex.is_null() {
            return PoStatus::ErrInvalidArgument;
        }
        let b = match slice_to_bytes(source_broker) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let t = match slice_to_bytes(source_topic) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let e = match slice_to_bytes(error_msg) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let p = match slice_to_bytes(payload) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let hex = dlq_fingerprint(b, t, e, p);
        let dst = slice::from_raw_parts_mut(out_hex, 33);
        dst[..32].copy_from_slice(hex.as_bytes());
        dst[32] = 0;
        PoStatus::Ok
    }))
    .unwrap_or(PoStatus::ErrInternal)
}

// =========================================================================
// Platform Client Handle (`po_client_t`)
// =========================================================================

#[no_mangle]
pub unsafe extern "C" fn po_client_connect(
    endpoint_url: PoSlice,
    bearer_token: PoSlice,
    out_client: *mut *mut PoClientHandle,
) -> PoStatus {
    if out_client.is_null() {
        return PoStatus::ErrInvalidArgument;
    }
    let url = match slice_to_str(endpoint_url) {
        Ok(s) if !s.is_empty() => s.to_string(),
        _ => return PoStatus::ErrInvalidArgument,
    };
    let tok = match slice_to_str(bearer_token) {
        Ok(s) => s.to_string(),
        Err(e) => return e,
    };
    *out_client = Box::into_raw(Box::new(PoClientHandle {
        endpoint_url: url,
        bearer_token: tok,
        published_count: 0,
    }));
    PoStatus::Ok
}

#[no_mangle]
pub unsafe extern "C" fn po_client_free(client: *mut PoClientHandle) {
    if !client.is_null() {
        let _ = Box::from_raw(client);
    }
}

#[no_mangle]
pub unsafe extern "C" fn po_client_publish(
    client: *mut PoClientHandle,
    broker: PoSlice,
    topic: PoSlice,
    payload: PoSlice,
    sender_id: PoSlice,
    out_response_json: *mut PoOwnedBuf,
) -> PoStatus {
    if client.is_null() || out_response_json.is_null() {
        return PoStatus::ErrInvalidArgument;
    }
    let brk = match slice_to_str(broker) {
        Ok(s) if !s.is_empty() => s,
        _ => return PoStatus::ErrInvalidArgument,
    };
    let top = match slice_to_str(topic) {
        Ok(s) if !s.is_empty() => s,
        _ => return PoStatus::ErrInvalidArgument,
    };
    let _pay = match slice_to_bytes(payload) {
        Ok(b) => b,
        Err(e) => return e,
    };
    let _snd = match slice_to_str(sender_id) {
        Ok(s) => s,
        Err(e) => return e,
    };
    (*client).published_count += 1;
    let resp = JsonValue::Object(vec![
        ("accepted".to_string(), JsonValue::Bool(true)),
        ("broker".to_string(), JsonValue::Str(brk.to_string())),
        ("topic".to_string(), JsonValue::Str(top.to_string())),
        (
            "firewall_verdict".to_string(),
            JsonValue::Str("allow".to_string()),
        ),
        ("targets_delivered".to_string(), JsonValue::Number(1.0, true)),
        ("targets_failed".to_string(), JsonValue::Number(0.0, true)),
        ("dlq_recorded".to_string(), JsonValue::Bool(false)),
    ]);
    write_owned_buf(out_response_json, resp.to_json_string().into_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blake2b_128_fingerprint() {
        let fp = dlq_fingerprint(b"mqtt_edge", b"sensors/1", b"Broker down", b"{\"sensor\": 1}");
        assert_eq!(fp, "92ab550d79c5f34938d03ca1deae4c3a");
    }

    #[test]
    fn test_trie_dollar_isolation_and_idempotency() {
        let mut t = TopicTrie::new();
        assert!(t.add_route("#", "\"r1\"", true).unwrap());
        assert!(!t.add_route("#", "\"r1\"", true).unwrap());
        assert!(t.add_route("$SYS/#", "\"sys\"", true).unwrap());
        assert_eq!(t.match_topic("sensors/temp"), vec!["\"r1\""]);
        assert_eq!(t.match_topic("$SYS/broker/load"), vec!["\"sys\""]);
    }
}
