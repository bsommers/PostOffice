//! Redis RESP2 Wire Client & T7 Subscribe-Before-Snapshot Convergence.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::time::Duration;

use crate::engine::EngineState;
use crate::firewall::Firewall;
use crate::json::JsonValue;
use crate::signing::validate_action_json;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<RespValue>),
}

pub struct RespConn {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl RespConn {
    pub fn connect(host: &str, port: u16, timeout_ms: u64) -> Result<Self, String> {
        let stream =
            TcpStream::connect((host, port)).map_err(|e| format!("Redis connect failed: {e}"))?;
        let _ = stream.set_read_timeout(Some(Duration::from_millis(timeout_ms)));
        let _ = stream.set_write_timeout(Some(Duration::from_millis(timeout_ms)));
        let writer = stream
            .try_clone()
            .map_err(|e| format!("Socket clone failed: {e}"))?;
        Ok(Self {
            reader: BufReader::new(stream),
            writer,
        })
    }

    pub fn set_read_timeout(&mut self, timeout_ms: Option<u64>) {
        let dur = timeout_ms.map(Duration::from_millis);
        let _ = self.reader.get_ref().set_read_timeout(dur);
    }

    pub fn send_command(&mut self, args: &[&str]) -> Result<(), String> {
        let mut cmd = format!("*{}\r\n", args.len()).into_bytes();
        for arg in args {
            let b = arg.as_bytes();
            cmd.extend_from_slice(format!("${}\r\n", b.len()).as_bytes());
            cmd.extend_from_slice(b);
            cmd.extend_from_slice(b"\r\n");
        }
        self.writer
            .write_all(&cmd)
            .map_err(|e| format!("Redis write error: {e}"))
    }

    pub fn read_value(&mut self) -> Result<RespValue, String> {
        let mut line = String::new();
        let n = self
            .reader
            .read_line(&mut line)
            .map_err(|e| format!("Redis read error: {e}"))?;
        if n == 0 {
            return Err("Redis EOF".to_string());
        }
        let trimmed = line.trim_end_matches(&['\r', '\n'][..]);
        if trimmed.is_empty() {
            return Err("Empty RESP line".to_string());
        }
        let prefix = trimmed.as_bytes()[0];
        let rest = &trimmed[1..];
        match prefix {
            b'+' => Ok(RespValue::SimpleString(rest.to_string())),
            b'-' => Ok(RespValue::Error(rest.to_string())),
            b':' => {
                let v = rest
                    .parse::<i64>()
                    .map_err(|_| "Invalid RESP integer".to_string())?;
                Ok(RespValue::Integer(v))
            }
            b'$' => {
                let len = rest
                    .parse::<isize>()
                    .map_err(|_| "Invalid RESP bulk length".to_string())?;
                if len < 0 {
                    return Ok(RespValue::BulkString(None));
                }
                let mut buf = vec![0u8; (len as usize) + 2];
                self.reader
                    .read_exact(&mut buf)
                    .map_err(|e| format!("Redis bulk read error: {e}"))?;
                buf.truncate(len as usize);
                let s = String::from_utf8_lossy(&buf).into_owned();
                Ok(RespValue::BulkString(Some(s)))
            }
            b'*' => {
                let count = rest
                    .parse::<isize>()
                    .map_err(|_| "Invalid RESP array length".to_string())?;
                if count <= 0 {
                    return Ok(RespValue::Array(Vec::new()));
                }
                let mut items = Vec::with_capacity(count as usize);
                for _ in 0..count {
                    items.push(self.read_value()?);
                }
                Ok(RespValue::Array(items))
            }
            _ => Err(format!("Unknown RESP prefix: {}", prefix as char)),
        }
    }

    pub fn get_str(&mut self, key: &str) -> Result<Option<String>, String> {
        self.send_command(&["GET", key])?;
        match self.read_value()? {
            RespValue::BulkString(opt) => Ok(opt),
            RespValue::Error(e) => Err(e),
            other => Err(format!("Unexpected GET response: {other:?}")),
        }
    }

    pub fn hgetall(&mut self, key: &str) -> Result<BTreeMap<String, String>, String> {
        self.send_command(&["HGETALL", key])?;
        match self.read_value()? {
            RespValue::Array(items) => {
                let mut map = BTreeMap::new();
                for pair in items.chunks_exact(2) {
                    if let (
                        RespValue::BulkString(Some(k)),
                        RespValue::BulkString(Some(v)),
                    ) = (&pair[0], &pair[1])
                    {
                        map.insert(k.clone(), v.clone());
                    }
                }
                Ok(map)
            }
            RespValue::Error(e) => Err(e),
            other => Err(format!("Unexpected HGETALL response: {other:?}")),
        }
    }

    pub fn xrange_payloads(&mut self, stream_key: &str) -> Result<Vec<String>, String> {
        self.send_command(&["XRANGE", stream_key, "-", "+"])?;
        match self.read_value()? {
            RespValue::Array(entries) => {
                let mut payloads = Vec::new();
                for entry in entries {
                    if let RespValue::Array(parts) = entry {
                        if parts.len() >= 2 {
                            if let RespValue::Array(kv) = &parts[1] {
                                for pair in kv.chunks_exact(2) {
                                    if let (
                                        RespValue::BulkString(Some(k)),
                                        RespValue::BulkString(Some(v)),
                                    ) = (&pair[0], &pair[1])
                                    {
                                        if k == "payload" {
                                            payloads.push(v.clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(payloads)
            }
            _ => Ok(Vec::new()),
        }
    }
}

/// Execute T7 subscribe-before-snapshot state convergence against a Redis server.
pub fn converge_from_redis(
    state: &mut EngineState,
    host: &str,
    port: u16,
    now_sec: f64,
) -> Result<JsonValue, String> {
    // 1. Open Pub/Sub connection and confirm SUBSCRIBE before touching snapshot
    let mut pubsub = RespConn::connect(host, port, 2000)?;
    pubsub.send_command(&["SUBSCRIBE", "postoffice:config_updates"])?;
    match pubsub.read_value()? {
        RespValue::Array(items) if items.len() >= 2 => {
            if let RespValue::BulkString(Some(kind)) = &items[0] {
                if kind != "subscribe" {
                    return Err(format!("Expected subscribe ack, got {kind}"));
                }
            }
        }
        other => return Err(format!("Invalid subscribe ack: {other:?}")),
    }

    // 2. Read snapshot on a dedicated connection
    let mut snap = RespConn::connect(host, port, 2000)?;
    let ver_str = snap.get_str("postoffice:version")?;
    let snapshot_version = ver_str
        .and_then(|s| s.parse::<i64>().ok())
        .unwrap_or(0);

    let brokers_raw = snap.hgetall("postoffice:brokers")?;
    for (name, raw) in brokers_raw {
        if let Ok(Some(JsonValue::Object(mut entries))) =
            state.open_signed_raw(&raw, "state", "postoffice:brokers", &name)
        {
            entries.retain(|(k, _)| k != "name");
            entries.push(("name".to_string(), JsonValue::Str(name)));
            let val = JsonValue::Object(entries);
            if validate_action_json("add_broker", &val).is_ok() {
                state.apply_validated_action("add_broker", &val);
            } else {
                state.metrics.record_control_event("invalid");
            }
        }
    }

    let routes_raw = snap.hgetall("postoffice:routes")?;
    for (field, raw) in routes_raw {
        if let Ok(Some(val)) = state.open_signed_raw(&raw, "state", "postoffice:routes", &field) {
            if validate_action_json("add_route", &val).is_ok() {
                state.apply_validated_action("add_route", &val);
            } else {
                state.metrics.record_control_event("invalid");
            }
        }
    }

    let subs_raw = snap.hgetall("postoffice:subscriptions")?;
    for (field, raw) in subs_raw {
        if let Ok(Some(val)) =
            state.open_signed_raw(&raw, "state", "postoffice:subscriptions", &field)
        {
            if validate_action_json("add_subscription", &val).is_ok() {
                state.apply_validated_action("add_subscription", &val);
            } else {
                state.metrics.record_control_event("invalid");
            }
        }
    }

    let ingress_raw = snap.hgetall("postoffice:ingress")?;
    if let Some(raw) = ingress_raw.get("rules") {
        if let Ok(Some(val)) = state.open_signed_raw(raw, "state", "postoffice:ingress", "rules") {
            if validate_action_json("set_ingress", &val).is_ok() {
                if let Ok(fw) = Firewall::from_json_str(&val.to_json_string()) {
                    state.firewall = Some(fw);
                }
            } else {
                state.metrics.record_control_event("invalid");
            }
        }
    }

    let rollout_raw = snap.hgetall("postoffice:rollout")?;
    for (field, raw) in rollout_raw {
        if let Ok(Some(val)) = state.open_signed_raw(&raw, "state", "postoffice:rollout", &field) {
            if validate_action_json("set_rollout", &val).is_ok() {
                state.apply_validated_action("set_rollout", &val);
            } else {
                state.metrics.record_control_event("invalid");
            }
        }
    }

    state.applied_version = Some(snapshot_version);

    // 2b. Replay any Redis Stream (`postoffice:config_stream`) entries newer than snapshot_version
    if let Ok(stream_payloads) = snap.xrange_payloads("postoffice:config_stream") {
        for ev in stream_payloads {
            state.apply_control_event(&ev, now_sec);
        }
    }

    // 3. Drain any events published to the pubsub socket while the snapshot was read
    pubsub.set_read_timeout(Some(30));
    let mut buffered = Vec::new();
    for _ in 0..10_000 {
        match pubsub.read_value() {
            Ok(RespValue::Array(items)) if items.len() == 3 => {
                if let (
                    RespValue::BulkString(Some(kind)),
                    RespValue::BulkString(Some(data)),
                ) = (&items[0], &items[2])
                {
                    if kind == "message" {
                        buffered.push(data.clone());
                    }
                }
            }
            _ => break,
        }
    }

    for ev in buffered {
        state.apply_control_event(&ev, now_sec);
    }

    Ok(JsonValue::Object(vec![
        (
            "applied_version".to_string(),
            JsonValue::Number(state.applied_version.unwrap_or(0) as f64, true),
        ),
        (
            "active_brokers".to_string(),
            JsonValue::Number(state.brokers.len() as f64, true),
        ),
        (
            "active_routes".to_string(),
            JsonValue::Number(state.routes_count as f64, true),
        ),
        (
            "active_subscriptions".to_string(),
            JsonValue::Number(state.subscriptions.len() as f64, true),
        ),
    ]))
}
