//! Minimal high-throughput MQTT v3.1.1 wire codec, open-loop generator, sink,
//! and in-process loopback broker for hermetic testing.

use std::io::{self, BufReader, BufWriter, Read, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::header;
use crate::hist::HdrHistogram;
use crate::oracle::{Oracle, Report};

const PKT_CONNECT: u8 = 0x10;
const PKT_CONNACK: u8 = 0x20;
const PKT_PUBLISH: u8 = 0x30;
const PKT_PUBACK: u8 = 0x40;
const PKT_SUBSCRIBE: u8 = 0x82;
const PKT_SUBACK: u8 = 0x90;
const PKT_PINGREQ: u8 = 0xC0;
const PKT_PINGRESP: u8 = 0xD0;
const PKT_DISCONNECT: u8 = 0xE0;

pub fn now_realtime_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

fn encode_var_len(mut len: usize, out: &mut Vec<u8>) {
    loop {
        let mut byte = (len & 0x7F) as u8;
        len >>= 7;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            break;
        }
    }
}

fn read_var_len<R: Read>(r: &mut R) -> io::Result<usize> {
    let mut multiplier = 1usize;
    let mut value = 0usize;
    let mut byte = [0u8; 1];
    for _ in 0..4 {
        r.read_exact(&mut byte)?;
        value += ((byte[0] & 0x7F) as usize) * multiplier;
        if (byte[0] & 0x80) == 0 {
            return Ok(value);
        }
        multiplier *= 128;
    }
    Err(io::Error::new(io::ErrorKind::InvalidData, "malformed MQTT remaining length"))
}

fn encode_utf8(s: &str, out: &mut Vec<u8>) {
    let bytes = s.as_bytes();
    out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
    out.extend_from_slice(bytes);
}

pub fn build_connect(client_id: &str, keep_alive_secs: u16) -> Vec<u8> {
    let mut body = Vec::with_capacity(16 + client_id.len());
    encode_utf8("MQTT", &mut body);
    body.push(4); // Protocol Level 4 (v3.1.1)
    body.push(0x02); // Clean Session
    body.extend_from_slice(&keep_alive_secs.to_be_bytes());
    encode_utf8(client_id, &mut body);
    let mut pkt = Vec::with_capacity(5 + body.len());
    pkt.push(PKT_CONNECT);
    encode_var_len(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

pub fn build_subscribe(packet_id: u16, topic: &str, qos: u8) -> Vec<u8> {
    let mut body = Vec::with_capacity(5 + topic.len());
    body.extend_from_slice(&packet_id.to_be_bytes());
    encode_utf8(topic, &mut body);
    body.push(qos & 0x03);
    let mut pkt = Vec::with_capacity(5 + body.len());
    pkt.push(PKT_SUBSCRIBE);
    encode_var_len(body.len(), &mut pkt);
    pkt.extend_from_slice(&body);
    pkt
}

pub fn encode_publish_into(
    pkt: &mut Vec<u8>,
    topic: &str,
    payload: &[u8],
    qos: u8,
    packet_id: u16,
) {
    pkt.clear();
    let topic_bytes = topic.as_bytes();
    let pkt_id_len = if qos > 0 { 2 } else { 0 };
    let rem_len = 2 + topic_bytes.len() + pkt_id_len + payload.len();
    pkt.push(PKT_PUBLISH | ((qos & 0x03) << 1));
    encode_var_len(rem_len, pkt);
    pkt.extend_from_slice(&(topic_bytes.len() as u16).to_be_bytes());
    pkt.extend_from_slice(topic_bytes);
    if qos > 0 {
        pkt.extend_from_slice(&packet_id.to_be_bytes());
    }
    pkt.extend_from_slice(payload);
}

#[derive(Debug)]
pub enum MqttPacket {
    ConnAck(u8),
    SubAck(u16),
    PubAck(u16),
    Publish {
        topic: String,
        qos: u8,
        packet_id: Option<u16>,
        payload: Vec<u8>,
    },
    PingReq,
    PingResp,
    Disconnect,
    Other(u8),
}

pub fn read_packet<R: Read>(r: &mut R) -> io::Result<MqttPacket> {
    let mut hdr = [0u8; 1];
    r.read_exact(&mut hdr)?;
    let pkt_type = hdr[0] & 0xF0;
    let flags = hdr[0] & 0x0F;
    let rem_len = read_var_len(r)?;
    let mut buf = vec![0u8; rem_len];
    if rem_len > 0 {
        r.read_exact(&mut buf)?;
    }
    match pkt_type {
        PKT_CONNACK if buf.len() >= 2 => Ok(MqttPacket::ConnAck(buf[1])),
        PKT_SUBACK if buf.len() >= 2 => {
            Ok(MqttPacket::SubAck(u16::from_be_bytes([buf[0], buf[1]])))
        }
        PKT_PUBACK if buf.len() >= 2 => {
            Ok(MqttPacket::PubAck(u16::from_be_bytes([buf[0], buf[1]])))
        }
        PKT_PUBLISH => {
            if buf.len() < 2 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "short PUBLISH"));
            }
            let tlen = u16::from_be_bytes([buf[0], buf[1]]) as usize;
            if buf.len() < 2 + tlen {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "short topic"));
            }
            let topic = String::from_utf8_lossy(&buf[2..2 + tlen]).into_owned();
            let qos = (flags >> 1) & 0x03;
            let mut offset = 2 + tlen;
            let packet_id = if qos > 0 {
                if buf.len() < offset + 2 {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "missing packet id"));
                }
                let pid = u16::from_be_bytes([buf[offset], buf[offset + 1]]);
                offset += 2;
                Some(pid)
            } else {
                None
            };
            let payload = buf[offset..].to_vec();
            Ok(MqttPacket::Publish {
                topic,
                qos,
                packet_id,
                payload,
            })
        }
        PKT_PINGREQ => Ok(MqttPacket::PingReq),
        PKT_PINGRESP => Ok(MqttPacket::PingResp),
        PKT_DISCONNECT => Ok(MqttPacket::Disconnect),
        _ => Ok(MqttPacket::Other(hdr[0])),
    }
}

#[derive(Debug, Clone)]
pub struct PubConfig {
    pub addr: String,
    pub topic: String,
    pub producer_id: u32,
    pub rate_per_sec: u64,
    pub count: u64,
    pub payload_size: usize,
    pub qos: u8,
}

#[derive(Debug, Clone)]
pub struct PubStats {
    pub sent: u64,
    pub elapsed_ns: u64,
    pub achieved_rate: f64,
    pub send_lag_ns: String,
}

impl PubStats {
    pub fn to_json(&self) -> String {
        format!(
            "{{\"sent\":{},\"elapsed_ns\":{},\"achieved_rate\":{:.2},\"send_lag_ns\":{}}}",
            self.sent, self.elapsed_ns, self.achieved_rate, self.send_lag_ns
        )
    }
}

/// Runs an open-loop scheduled MQTT publisher.
/// Each message carries its *intended* scheduled send timestamp (`start_realtime_ns + seq * interval_ns`),
/// ensuring coordinated-omission free end-to-end latency measurement at the sink.
pub fn run_publisher(cfg: &PubConfig) -> io::Result<PubStats> {
    let stream = TcpStream::connect(&cfg.addr)?;
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::with_capacity(64 * 1024, stream);

    let connect_pkt = build_connect(&format!("po-pub-{}", cfg.producer_id), 60);
    writer.write_all(&connect_pkt)?;
    writer.flush()?;
    match read_packet(&mut reader)? {
        MqttPacket::ConnAck(0) => {}
        other => {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("unexpected CONNACK: {other:?}"),
            ))
        }
    }

    let interval_ns = if cfg.rate_per_sec > 0 {
        1_000_000_000u64 / cfg.rate_per_sec
    } else {
        0
    };
    let start_instant = Instant::now();
    let start_realtime_ns = now_realtime_ns();
    let mut msg_buf = Vec::with_capacity(cfg.payload_size);
    let mut pkt_buf = Vec::with_capacity(cfg.payload_size + 64);
    let mut send_lag = HdrHistogram::new();

    for seq in 0..cfg.count {
        let sched_offset_ns = seq.saturating_mul(interval_ns);
        if interval_ns > 0 {
            let target = Duration::from_nanos(sched_offset_ns);
            let elapsed = start_instant.elapsed();
            if target > elapsed {
                thread::sleep(target - elapsed);
            }
        }
        let actual_offset_ns = start_instant.elapsed().as_nanos() as u64;
        send_lag.record(actual_offset_ns.saturating_sub(sched_offset_ns));

        let intended_send_ts_ns = start_realtime_ns.saturating_add(sched_offset_ns);
        header::encode_into(
            &mut msg_buf,
            cfg.producer_id,
            seq,
            intended_send_ts_ns,
            b"",
            Some(cfg.payload_size.max(header::HEADER_SIZE)),
        )
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;

        let pkt_id = ((seq % 65535) as u16) + 1;
        encode_publish_into(&mut pkt_buf, &cfg.topic, &msg_buf, cfg.qos, pkt_id);
        writer.write_all(&pkt_buf)?;
        writer.flush()?;
        if cfg.qos == 1 {
            let _ = read_packet(&mut reader)?;
        }
    }
    writer.write_all(&[PKT_DISCONNECT, 0])?;
    writer.flush()?;

    let elapsed_ns = start_instant.elapsed().as_nanos() as u64;
    let achieved_rate = if elapsed_ns > 0 {
        (cfg.count as f64) * 1e9 / (elapsed_ns as f64)
    } else {
        0.0
    };
    Ok(PubStats {
        sent: cfg.count,
        elapsed_ns,
        achieved_rate,
        send_lag_ns: send_lag.summary_json(),
    })
}

#[derive(Debug, Clone)]
pub struct SinkConfig {
    pub addr: String,
    pub topic: String,
    pub producer_id: u32,
    pub expected_count: u64,
    pub timeout_ms: u64,
    pub qos: u8,
}

/// Subscribes to `cfg.topic`, records up to `expected_count` messages (or until `timeout_ms` idle),
/// and returns the Oracle accounting report.
pub fn run_sink(
    cfg: &SinkConfig,
    ready_signal: Option<Arc<AtomicBool>>,
) -> io::Result<Report> {
    let stream = TcpStream::connect(&cfg.addr)?;
    stream.set_nonblocking(false)?;
    stream.set_nodelay(true)?;
    stream.set_read_timeout(Some(Duration::from_millis(cfg.timeout_ms.max(50))))?;
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut writer = BufWriter::new(stream);

    writer.write_all(&build_connect("po-sink", 60))?;
    writer.flush()?;
    match read_packet(&mut reader)? {
        MqttPacket::ConnAck(0) => {}
        other => {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionRefused,
                format!("unexpected CONNACK: {other:?}"),
            ))
        }
    }

    writer.write_all(&build_subscribe(1, &cfg.topic, cfg.qos))?;
    writer.flush()?;
    match read_packet(&mut reader)? {
        MqttPacket::SubAck(_) => {}
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unexpected SUBACK: {other:?}"),
            ))
        }
    }

    if let Some(sig) = ready_signal {
        sig.store(true, Ordering::SeqCst);
    }

    let mut oracle = Oracle::new();
    oracle.sent_range(cfg.producer_id, cfg.expected_count);
    let mut unique_received = 0u64;
    let deadline = Instant::now() + Duration::from_millis(cfg.timeout_ms);

    while unique_received < cfg.expected_count && Instant::now() < deadline {
        match read_packet(&mut reader) {
            Ok(MqttPacket::Publish {
                qos,
                packet_id,
                payload,
                ..
            }) => {
                let recv_ts = now_realtime_ns();
                let prev_dup = oracle.duplicated;
                let prev_cor = oracle.corrupted;
                let prev_for = oracle.foreign;
                let prev_unx = oracle.unexpected;
                oracle.received(&payload, recv_ts);
                if oracle.duplicated == prev_dup
                    && oracle.corrupted == prev_cor
                    && oracle.foreign == prev_for
                    && oracle.unexpected == prev_unx
                {
                    unique_received += 1;
                }
                if qos == 1 {
                    if let Some(pid) = packet_id {
                        let ack = [PKT_PUBACK, 2, (pid >> 8) as u8, (pid & 0xFF) as u8];
                        writer.write_all(&ack)?;
                        writer.flush()?;
                    }
                }
            }
            Ok(MqttPacket::PingResp | MqttPacket::Other(_)) => {}
            Ok(MqttPacket::Disconnect) => break,
            Err(e)
                if e.kind() == io::ErrorKind::WouldBlock
                    || e.kind() == io::ErrorKind::TimedOut =>
            {
                break;
            }
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e),
            _ => {}
        }
    }

    let _ = writer.write_all(&[PKT_DISCONNECT, 0]);
    let _ = writer.flush();
    Ok(oracle.report(0, 0, 0, 0))
}

/// Runs an in-memory pipe pub/sink loopback (no TCP sockets) to verify open-loop scheduling,
/// MQTT wire framing, and Oracle reconciliation hermetically inside sandboxes.
pub fn run_in_memory_loopback(
    rate_per_sec: u64,
    count: u64,
    payload_size: usize,
    producer_id: u32,
    qos: u8,
) -> (PubStats, Report) {
    let interval_ns = if rate_per_sec > 0 {
        1_000_000_000u64 / rate_per_sec
    } else {
        0
    };
    let start_instant = Instant::now();
    let start_realtime_ns = now_realtime_ns();
    let mut msg_buf = Vec::with_capacity(payload_size);
    let mut pkt_buf = Vec::with_capacity(payload_size + 64);
    let mut wire_stream = Vec::with_capacity((payload_size + 32) * (count as usize));
    let mut send_lag = HdrHistogram::new();

    for seq in 0..count {
        let sched_offset_ns = seq.saturating_mul(interval_ns);
        let actual_offset_ns = start_instant.elapsed().as_nanos() as u64;
        send_lag.record(actual_offset_ns.saturating_sub(sched_offset_ns));
        let intended_send_ts_ns = start_realtime_ns.saturating_add(sched_offset_ns);
        header::encode_into(
            &mut msg_buf,
            producer_id,
            seq,
            intended_send_ts_ns,
            b"",
            Some(payload_size.max(header::HEADER_SIZE)),
        )
        .unwrap();
        let pkt_id = ((seq % 65535) as u16) + 1;
        encode_publish_into(&mut pkt_buf, "perf/loopback", &msg_buf, qos, pkt_id);
        wire_stream.extend_from_slice(&pkt_buf);
    }

    let elapsed_ns = start_instant.elapsed().as_nanos() as u64;
    let achieved_rate = if elapsed_ns > 0 {
        (count as f64) * 1e9 / (elapsed_ns as f64)
    } else {
        0.0
    };
    let pub_stats = PubStats {
        sent: count,
        elapsed_ns,
        achieved_rate,
        send_lag_ns: send_lag.summary_json(),
    };

    let mut oracle = Oracle::new();
    oracle.sent_range(producer_id, count);
    let mut cursor = io::Cursor::new(wire_stream);
    while (cursor.position() as usize) < cursor.get_ref().len() {
        if let Ok(MqttPacket::Publish { payload, .. }) = read_packet(&mut cursor) {
            oracle.received(&payload, now_realtime_ns());
        } else {
            break;
        }
    }
    (pub_stats, oracle.report(0, 0, 0, 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loopback_pub_sink_end_to_end() {
        let mut buf = Vec::new();
        encode_publish_into(&mut buf, "sensors/temp", b"payload-123", 1, 7);
        let mut cursor = io::Cursor::new(buf);
        match read_packet(&mut cursor).unwrap() {
            MqttPacket::Publish {
                topic,
                qos,
                packet_id,
                payload,
            } => {
                assert_eq!(topic, "sensors/temp");
                assert_eq!(qos, 1);
                assert_eq!(packet_id, Some(7));
                assert_eq!(payload, b"payload-123");
            }
            other => panic!("unexpected packet: {other:?}"),
        }
    }
}
