//! Phase 5 Native Per-Target Bounded Publish Queues & Edge Disk-Backed Store-and-Forward.
//!
//! Implements:
//! - `TargetPublishQueue`: per-target bounded FIFO queue (`max_messages`, `max_bytes`,
//!   `max_age_sec`, `overflow_policy`: `"reject"` or `"drop_oldest"`).
//! - `DiskStoreAndForward`: opt-in edge durable store-and-forward log (`§7 Decision 5`)
//!   with `size_cap_bytes`, wear-aware write batching (`batch_max_messages`,
//!   `batch_max_bytes`, `flush_interval_sec`), CRC32 frame integrity, and automatic
//!   crash recovery of torn trailing writes.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use crate::json::JsonValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OverflowPolicy {
    Reject,
    DropOldest,
}

impl OverflowPolicy {
    pub fn from_str(raw: &str) -> Result<Self, String> {
        match raw.trim() {
            "reject" => Ok(Self::Reject),
            "drop_oldest" => Ok(Self::DropOldest),
            other => Err(format!(
                "Invalid overflow_policy '{other}'. Must be 'reject' or 'drop_oldest'."
            )),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Reject => "reject",
            Self::DropOldest => "drop_oldest",
        }
    }
}

#[derive(Clone, Debug)]
pub struct QueuedMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    pub kwargs_json: String,
    pub enqueued_at: f64,
}

impl QueuedMessage {
    pub fn to_json_value(&self) -> JsonValue {
        let mut hex = String::with_capacity(self.payload.len() * 2);
        for b in &self.payload {
            use std::fmt::Write as _;
            let _ = write!(&mut hex, "{b:02x}");
        }
        JsonValue::Object(vec![
            ("topic".to_string(), JsonValue::Str(self.topic.clone())),
            ("payload_hex".to_string(), JsonValue::Str(hex)),
            (
                "kwargs_json".to_string(),
                JsonValue::Str(self.kwargs_json.clone()),
            ),
            (
                "size".to_string(),
                JsonValue::Number(self.payload.len() as f64, true),
            ),
            (
                "enqueued_at".to_string(),
                JsonValue::Number(self.enqueued_at, false),
            ),
        ])
    }
}

pub struct TargetPublishQueue {
    pub max_messages: usize,
    pub max_bytes: Option<usize>,
    pub max_age_sec: Option<f64>,
    pub overflow_policy: OverflowPolicy,
    items: VecDeque<QueuedMessage>,
    queued_bytes: usize,
    total_enqueued: u64,
    total_rejected: u64,
    total_evicted: u64,
    total_expired: u64,
}

impl TargetPublishQueue {
    pub fn new(
        max_messages: usize,
        max_bytes: Option<usize>,
        max_age_sec: Option<f64>,
        overflow_policy: &str,
    ) -> Result<Self, String> {
        if max_messages == 0 {
            return Err("max_messages must be > 0".to_string());
        }
        if let Some(mb) = max_bytes {
            if mb == 0 {
                return Err("max_bytes must be > 0".to_string());
            }
        }
        if let Some(age) = max_age_sec {
            if age <= 0.0 || !age.is_finite() {
                return Err("max_age_sec must be > 0".to_string());
            }
        }
        let policy = OverflowPolicy::from_str(overflow_policy)?;
        Ok(Self {
            max_messages,
            max_bytes,
            max_age_sec,
            overflow_policy: policy,
            items: VecDeque::new(),
            queued_bytes: 0,
            total_enqueued: 0,
            total_rejected: 0,
            total_evicted: 0,
            total_expired: 0,
        })
    }

    fn is_stale(&self, msg: &QueuedMessage, now_sec: f64) -> bool {
        match self.max_age_sec {
            Some(age) => (now_sec - msg.enqueued_at) > age,
            None => false,
        }
    }

    fn pop_front(&mut self) -> Option<QueuedMessage> {
        let item = self.items.pop_front()?;
        self.queued_bytes = self.queued_bytes.saturating_sub(item.payload.len());
        Some(item)
    }

    fn expire_stale(&mut self, now_sec: f64) -> Vec<QueuedMessage> {
        let mut expired = Vec::new();
        while let Some(front) = self.items.front() {
            if self.is_stale(front, now_sec) {
                if let Some(item) = self.pop_front() {
                    self.total_expired += 1;
                    expired.push(item);
                }
            } else {
                break;
            }
        }
        expired
    }

    pub fn enqueue(
        &mut self,
        topic: &str,
        payload: &[u8],
        kwargs_json: &str,
        now_sec: f64,
    ) -> JsonValue {
        let expired = self.expire_stale(now_sec);
        let size = payload.len();
        let mut evicted = Vec::new();

        if let Some(max_b) = self.max_bytes {
            if size > max_b {
                self.total_rejected += 1;
                return JsonValue::Object(vec![
                    ("accepted".to_string(), JsonValue::Bool(false)),
                    (
                        "reason".to_string(),
                        JsonValue::Str("oversize".to_string()),
                    ),
                    (
                        "expired".to_string(),
                        JsonValue::Array(expired.iter().map(|m| m.to_json_value()).collect()),
                    ),
                    ("evicted".to_string(), JsonValue::Array(Vec::new())),
                ]);
            }
        }

        loop {
            let count_full = self.items.len() >= self.max_messages;
            let bytes_full = self
                .max_bytes
                .map(|mb| self.queued_bytes + size > mb)
                .unwrap_or(false);
            if !count_full && !bytes_full {
                break;
            }
            if self.overflow_policy == OverflowPolicy::DropOldest && !self.items.is_empty() {
                if let Some(old) = self.pop_front() {
                    self.total_evicted += 1;
                    evicted.push(old);
                }
            } else {
                self.total_rejected += 1;
                let reason = if count_full {
                    "count_full"
                } else {
                    "bytes_full"
                };
                return JsonValue::Object(vec![
                    ("accepted".to_string(), JsonValue::Bool(false)),
                    ("reason".to_string(), JsonValue::Str(reason.to_string())),
                    (
                        "expired".to_string(),
                        JsonValue::Array(expired.iter().map(|m| m.to_json_value()).collect()),
                    ),
                    (
                        "evicted".to_string(),
                        JsonValue::Array(evicted.iter().map(|m| m.to_json_value()).collect()),
                    ),
                ]);
            }
        }

        self.queued_bytes += size;
        self.total_enqueued += 1;
        self.items.push_back(QueuedMessage {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            kwargs_json: if kwargs_json.is_empty() {
                "{}".to_string()
            } else {
                kwargs_json.to_string()
            },
            enqueued_at: now_sec,
        });

        JsonValue::Object(vec![
            ("accepted".to_string(), JsonValue::Bool(true)),
            ("reason".to_string(), JsonValue::Str("ok".to_string())),
            (
                "expired".to_string(),
                JsonValue::Array(expired.iter().map(|m| m.to_json_value()).collect()),
            ),
            (
                "evicted".to_string(),
                JsonValue::Array(evicted.iter().map(|m| m.to_json_value()).collect()),
            ),
        ])
    }

    pub fn drain(&mut self, limit: usize, now_sec: f64) -> JsonValue {
        let mut expired = self.expire_stale(now_sec);
        let mut messages = Vec::new();
        while messages.len() < limit {
            let Some(item) = self.pop_front() else {
                break;
            };
            if self.is_stale(&item, now_sec) {
                self.total_expired += 1;
                expired.push(item);
            } else {
                messages.push(item);
            }
        }
        JsonValue::Object(vec![
            (
                "messages".to_string(),
                JsonValue::Array(messages.iter().map(|m| m.to_json_value()).collect()),
            ),
            (
                "expired".to_string(),
                JsonValue::Array(expired.iter().map(|m| m.to_json_value()).collect()),
            ),
        ])
    }

    pub fn stats(&self) -> JsonValue {
        JsonValue::Object(vec![
            (
                "queued_messages".to_string(),
                JsonValue::Number(self.items.len() as f64, true),
            ),
            (
                "queued_bytes".to_string(),
                JsonValue::Number(self.queued_bytes as f64, true),
            ),
            (
                "total_enqueued".to_string(),
                JsonValue::Number(self.total_enqueued as f64, true),
            ),
            (
                "total_rejected".to_string(),
                JsonValue::Number(self.total_rejected as f64, true),
            ),
            (
                "total_evicted".to_string(),
                JsonValue::Number(self.total_evicted as f64, true),
            ),
            (
                "total_expired".to_string(),
                JsonValue::Number(self.total_expired as f64, true),
            ),
            (
                "overflow_policy".to_string(),
                JsonValue::Str(self.overflow_policy.as_str().to_string()),
            ),
        ])
    }
}

// =========================================================================
// Edge Disk-Backed Store-and-Forward (`DiskStoreAndForward`)
// =========================================================================
//
// Binary Record Wire Format (little-endian):
//   [0..4]   magic: u32 = 0x4653_4f50 ("POSF")
//   [4..8]   crc32: u32 over header[8..28] + topic + payload + kwargs
//   [8..12]  topic_len: u32
//   [12..16] payload_len: u32
//   [16..20] kwargs_len: u32
//   [20..28] enqueued_at_bits: u64 (f64::to_bits)
//   [28..]   topic_bytes || payload_bytes || kwargs_bytes

const SAF_MAGIC: u32 = 0x4653_4f50;
const SAF_HEADER_BYTES: usize = 28;

fn crc32_ieee(chunks: &[&[u8]]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for chunk in chunks {
        for &b in *chunk {
            crc ^= b as u32;
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
            }
        }
    }
    !crc
}

fn encode_saf_record(msg: &QueuedMessage) -> Vec<u8> {
    let tb = msg.topic.as_bytes();
    let pb = msg.payload.as_slice();
    let kb = msg.kwargs_json.as_bytes();
    let mut meta = [0u8; 20];
    meta[0..4].copy_from_slice(&(tb.len() as u32).to_le_bytes());
    meta[4..8].copy_from_slice(&(pb.len() as u32).to_le_bytes());
    meta[8..12].copy_from_slice(&(kb.len() as u32).to_le_bytes());
    meta[12..20].copy_from_slice(&msg.enqueued_at.to_bits().to_le_bytes());

    let crc = crc32_ieee(&[&meta, tb, pb, kb]);
    let mut buf = Vec::with_capacity(SAF_HEADER_BYTES + tb.len() + pb.len() + kb.len());
    buf.extend_from_slice(&SAF_MAGIC.to_le_bytes());
    buf.extend_from_slice(&crc.to_le_bytes());
    buf.extend_from_slice(&meta);
    buf.extend_from_slice(tb);
    buf.extend_from_slice(pb);
    buf.extend_from_slice(kb);
    buf
}

fn record_disk_size(msg: &QueuedMessage) -> usize {
    SAF_HEADER_BYTES + msg.topic.len() + msg.payload.len() + msg.kwargs_json.len()
}

pub struct DiskStoreAndForward {
    path: PathBuf,
    size_cap_bytes: usize,
    batch_max_messages: usize,
    batch_max_bytes: usize,
    flush_interval_sec: f64,
    overflow_policy: OverflowPolicy,
    persisted: VecDeque<QueuedMessage>,
    persisted_bytes: usize,
    staged: VecDeque<QueuedMessage>,
    staged_bytes: usize,
    last_flush_sec: Option<f64>,
    disk_flushes: u64,
    recovered_records: u64,
    truncated_torn_bytes: u64,
}

impl DiskStoreAndForward {
    pub fn open(
        path: &str,
        size_cap_bytes: usize,
        batch_max_messages: usize,
        batch_max_bytes: usize,
        flush_interval_sec: f64,
        overflow_policy: &str,
    ) -> Result<Self, String> {
        if path.trim().is_empty() {
            return Err("Disk store path must not be empty".to_string());
        }
        if size_cap_bytes < SAF_HEADER_BYTES + 1 {
            return Err("size_cap_bytes is too small".to_string());
        }
        let policy = OverflowPolicy::from_str(overflow_policy)?;
        let path_buf = PathBuf::from(path);
        if let Some(parent) = path_buf.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)
                    .map_err(|e| format!("Failed to create parent dir: {e}"))?;
            }
        }

        let mut store = Self {
            path: path_buf,
            size_cap_bytes,
            batch_max_messages: batch_max_messages.max(1),
            batch_max_bytes: batch_max_bytes.max(1),
            flush_interval_sec: if flush_interval_sec > 0.0 {
                flush_interval_sec
            } else {
                1.0
            },
            overflow_policy: policy,
            persisted: VecDeque::new(),
            persisted_bytes: 0,
            staged: VecDeque::new(),
            staged_bytes: 0,
            last_flush_sec: None,
            disk_flushes: 0,
            recovered_records: 0,
            truncated_torn_bytes: 0,
        };
        store.recover_from_disk()?;
        Ok(store)
    }

    fn recover_from_disk(&mut self) -> Result<(), String> {
        if !self.path.exists() {
            return Ok(());
        }
        let mut raw = Vec::new();
        {
            let mut f = File::open(&self.path)
                .map_err(|e| format!("Failed to open store file: {e}"))?;
            f.read_to_end(&mut raw)
                .map_err(|e| format!("Failed to read store file: {e}"))?;
        }
        let mut offset = 0usize;
        while offset + SAF_HEADER_BYTES <= raw.len() {
            let magic = u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap());
            if magic != SAF_MAGIC {
                break;
            }
            let expected_crc = u32::from_le_bytes(raw[offset + 4..offset + 8].try_into().unwrap());
            let meta = &raw[offset + 8..offset + 28];
            let t_len = u32::from_le_bytes(meta[0..4].try_into().unwrap()) as usize;
            let p_len = u32::from_le_bytes(meta[4..8].try_into().unwrap()) as usize;
            let k_len = u32::from_le_bytes(meta[8..12].try_into().unwrap()) as usize;
            let ts_bits = u64::from_le_bytes(meta[12..20].try_into().unwrap());
            let total_frame = SAF_HEADER_BYTES
                .checked_add(t_len)
                .and_then(|v| v.checked_add(p_len))
                .and_then(|v| v.checked_add(k_len));
            let Some(frame_len) = total_frame else {
                break;
            };
            if offset + frame_len > raw.len() {
                break;
            }
            let body = &raw[offset + SAF_HEADER_BYTES..offset + frame_len];
            let tb = &body[0..t_len];
            let pb = &body[t_len..t_len + p_len];
            let kb = &body[t_len + p_len..t_len + p_len + k_len];
            let actual_crc = crc32_ieee(&[meta, tb, pb, kb]);
            if actual_crc != expected_crc {
                break;
            }
            let Ok(topic) = std::str::from_utf8(tb) else {
                break;
            };
            let Ok(kwargs_json) = std::str::from_utf8(kb) else {
                break;
            };
            let msg = QueuedMessage {
                topic: topic.to_string(),
                payload: pb.to_vec(),
                kwargs_json: kwargs_json.to_string(),
                enqueued_at: f64::from_bits(ts_bits),
            };
            self.persisted_bytes += frame_len;
            self.persisted.push_back(msg);
            self.recovered_records += 1;
            offset += frame_len;
        }

        if offset < raw.len() {
            let torn = (raw.len() - offset) as u64;
            self.truncated_torn_bytes += torn;
            let f = OpenOptions::new()
                .write(true)
                .open(&self.path)
                .map_err(|e| format!("Failed to open store for truncation: {e}"))?;
            f.set_len(offset as u64)
                .map_err(|e| format!("Failed to truncate torn tail: {e}"))?;
            f.sync_all()
                .map_err(|e| format!("Failed to sync truncated store: {e}"))?;
        }
        Ok(())
    }

    fn rewrite_persisted_file(&mut self) -> Result<(), String> {
        let mut buf = Vec::with_capacity(self.persisted_bytes);
        for msg in &self.persisted {
            buf.extend_from_slice(&encode_saf_record(msg));
        }
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to rewrite store file: {e}"))?;
        f.write_all(&buf)
            .map_err(|e| format!("Failed to write compacted store: {e}"))?;
        f.sync_data()
            .map_err(|e| format!("Failed to sync compacted store: {e}"))?;
        self.disk_flushes += 1;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<usize, String> {
        if self.staged.is_empty() {
            return Ok(0);
        }
        let mut batch_buf = Vec::with_capacity(self.staged_bytes);
        let mut count = 0usize;
        while let Some(msg) = self.staged.pop_front() {
            let sz = record_disk_size(&msg);
            self.staged_bytes = self.staged_bytes.saturating_sub(sz);
            batch_buf.extend_from_slice(&encode_saf_record(&msg));
            self.persisted_bytes += sz;
            self.persisted.push_back(msg);
            count += 1;
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| format!("Failed to open store for batch append: {e}"))?;
        f.write_all(&batch_buf)
            .map_err(|e| format!("Failed to append batch: {e}"))?;
        f.sync_data()
            .map_err(|e| format!("Failed to sync batch: {e}"))?;
        self.disk_flushes += 1;
        Ok(count)
    }

    pub fn append(
        &mut self,
        topic: &str,
        payload: &[u8],
        kwargs_json: &str,
        now_sec: f64,
    ) -> Result<JsonValue, String> {
        let candidate = QueuedMessage {
            topic: topic.to_string(),
            payload: payload.to_vec(),
            kwargs_json: if kwargs_json.is_empty() {
                "{}".to_string()
            } else {
                kwargs_json.to_string()
            },
            enqueued_at: now_sec,
        };
        let frame_bytes = record_disk_size(&candidate);
        if frame_bytes > self.size_cap_bytes {
            return Ok(JsonValue::Object(vec![
                ("accepted".to_string(), JsonValue::Bool(false)),
                (
                    "reason".to_string(),
                    JsonValue::Str("oversize".to_string()),
                ),
                ("evicted".to_string(), JsonValue::Number(0.0, true)),
            ]));
        }

        let mut evicted = 0usize;
        let mut compacted_disk = false;
        while self.persisted_bytes + self.staged_bytes + frame_bytes > self.size_cap_bytes {
            if self.overflow_policy == OverflowPolicy::Reject {
                return Ok(JsonValue::Object(vec![
                    ("accepted".to_string(), JsonValue::Bool(false)),
                    (
                        "reason".to_string(),
                        JsonValue::Str("size_cap_exceeded".to_string()),
                    ),
                    ("evicted".to_string(), JsonValue::Number(evicted as f64, true)),
                ]));
            }
            if let Some(old) = self.persisted.pop_front() {
                self.persisted_bytes = self
                    .persisted_bytes
                    .saturating_sub(record_disk_size(&old));
                evicted += 1;
                compacted_disk = true;
            } else if let Some(old) = self.staged.pop_front() {
                self.staged_bytes = self.staged_bytes.saturating_sub(record_disk_size(&old));
                evicted += 1;
            } else {
                break;
            }
        }
        if compacted_disk {
            self.rewrite_persisted_file()?;
        }

        let first_staged_time = self.last_flush_sec.unwrap_or(now_sec);
        if self.last_flush_sec.is_none() {
            self.last_flush_sec = Some(now_sec);
        }
        self.staged_bytes += frame_bytes;
        self.staged.push_back(candidate);

        let should_flush = self.staged.len() >= self.batch_max_messages
            || self.staged_bytes >= self.batch_max_bytes
            || (now_sec - first_staged_time) >= self.flush_interval_sec;
        if should_flush {
            self.flush()?;
            self.last_flush_sec = Some(now_sec);
        }

        Ok(JsonValue::Object(vec![
            ("accepted".to_string(), JsonValue::Bool(true)),
            ("reason".to_string(), JsonValue::Str("ok".to_string())),
            ("evicted".to_string(), JsonValue::Number(evicted as f64, true)),
            ("flushed".to_string(), JsonValue::Bool(should_flush)),
        ]))
    }

    pub fn drain(&mut self, limit: usize) -> Result<JsonValue, String> {
        let mut out = Vec::new();
        let mut modified_disk = false;
        while out.len() < limit {
            if let Some(msg) = self.persisted.pop_front() {
                self.persisted_bytes = self
                    .persisted_bytes
                    .saturating_sub(record_disk_size(&msg));
                out.push(msg);
                modified_disk = true;
            } else if let Some(msg) = self.staged.pop_front() {
                self.staged_bytes = self.staged_bytes.saturating_sub(record_disk_size(&msg));
                out.push(msg);
            } else {
                break;
            }
        }
        if modified_disk {
            if self.persisted.is_empty() && self.path.exists() {
                let f = OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .open(&self.path)
                    .map_err(|e| format!("Failed to truncate drained store: {e}"))?;
                let _ = f.sync_data();
                let _ = Seek::seek(&mut &f, SeekFrom::Start(0));
            } else {
                self.rewrite_persisted_file()?;
            }
        }
        Ok(JsonValue::Array(
            out.iter().map(|m| m.to_json_value()).collect(),
        ))
    }

    pub fn stats(&self) -> JsonValue {
        let total_msgs = self.persisted.len() + self.staged.len();
        let total_bytes = self.persisted_bytes + self.staged_bytes;
        JsonValue::Object(vec![
            (
                "total_messages".to_string(),
                JsonValue::Number(total_msgs as f64, true),
            ),
            (
                "total_bytes".to_string(),
                JsonValue::Number(total_bytes as f64, true),
            ),
            (
                "persisted_messages".to_string(),
                JsonValue::Number(self.persisted.len() as f64, true),
            ),
            (
                "persisted_bytes".to_string(),
                JsonValue::Number(self.persisted_bytes as f64, true),
            ),
            (
                "staged_messages".to_string(),
                JsonValue::Number(self.staged.len() as f64, true),
            ),
            (
                "staged_bytes".to_string(),
                JsonValue::Number(self.staged_bytes as f64, true),
            ),
            (
                "disk_flushes".to_string(),
                JsonValue::Number(self.disk_flushes as f64, true),
            ),
            (
                "recovered_records".to_string(),
                JsonValue::Number(self.recovered_records as f64, true),
            ),
            (
                "truncated_torn_bytes".to_string(),
                JsonValue::Number(self.truncated_torn_bytes as f64, true),
            ),
            (
                "size_cap_bytes".to_string(),
                JsonValue::Number(self.size_cap_bytes as f64, true),
            ),
        ])
    }
}

impl Drop for DiskStoreAndForward {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
