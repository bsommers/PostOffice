//! 32-byte big-endian `POB1` perf header, byte-for-byte identical to `perf/header.py`.
//!
//! Layout:
//!   0..4   magic       b"POB1"
//!   4..8   producer    u32 BE
//!   8..16  seq         u64 BE
//!  16..24  send_ts_ns  u64 BE (intended scheduled send timestamp in ns)
//!  24..28  crc32       u32 BE (IEEE CRC32 of body)
//!  28..32  reserved    u32 BE (0)

pub const MAGIC: &[u8; 4] = b"POB1";
pub const HEADER_SIZE: usize = 32;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Header {
    pub producer: u32,
    pub seq: u64,
    pub send_ts_ns: u64,
    pub intact: bool,
    pub body_len: usize,
}

/// Standard IEEE 802.3 CRC32 (matches Python `zlib.crc32`).
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        crc ^= b as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Encodes a message into `buf`, resizing it to `total_size` (must be >= HEADER_SIZE + body.len()).
pub fn encode_into(
    buf: &mut Vec<u8>,
    producer: u32,
    seq: u64,
    send_ts_ns: u64,
    body: &[u8],
    total_size: Option<usize>,
) -> Result<(), &'static str> {
    let min_len = HEADER_SIZE + body.len();
    let target_len = total_size.unwrap_or(min_len);
    if target_len < min_len {
        return Err("size must be >= HEADER_SIZE + body.len()");
    }
    buf.clear();
    buf.resize(target_len, 0u8);
    buf[HEADER_SIZE..HEADER_SIZE + body.len()].copy_from_slice(body);
    let body_crc = crc32(&buf[HEADER_SIZE..]);
    buf[0..4].copy_from_slice(MAGIC);
    buf[4..8].copy_from_slice(&producer.to_be_bytes());
    buf[8..16].copy_from_slice(&seq.to_be_bytes());
    buf[16..24].copy_from_slice(&send_ts_ns.to_be_bytes());
    buf[24..28].copy_from_slice(&body_crc.to_be_bytes());
    buf[28..32].copy_from_slice(&0u32.to_be_bytes());
    Ok(())
}

pub fn encode(
    producer: u32,
    seq: u64,
    send_ts_ns: u64,
    body: &[u8],
    total_size: Option<usize>,
) -> Result<Vec<u8>, &'static str> {
    let mut buf = Vec::new();
    encode_into(&mut buf, producer, seq, send_ts_ns, body, total_size)?;
    Ok(buf)
}

pub fn decode(message: &[u8]) -> Option<Header> {
    if message.len() < HEADER_SIZE || &message[0..4] != MAGIC {
        return None;
    }
    let producer = u32::from_be_bytes(message[4..8].try_into().ok()?);
    let seq = u64::from_be_bytes(message[8..16].try_into().ok()?);
    let send_ts_ns = u64::from_be_bytes(message[16..24].try_into().ok()?);
    let expected_crc = u32::from_be_bytes(message[24..28].try_into().ok()?);
    let body = &message[HEADER_SIZE..];
    Some(Header {
        producer,
        seq,
        send_ts_ns,
        intact: crc32(body) == expected_crc,
        body_len: body.len(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc32_known_vector() {
        // Standard IEEE CRC32 check value for "123456789" is 0xCBF43926
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b"hello"), 0x3610_A686);
    }

    #[test]
    fn test_round_trip_and_corruption() {
        let msg = encode(7, 42, 1_000_000, b"hello", None).unwrap();
        assert_eq!(msg.len(), HEADER_SIZE + 5);
        let h = decode(&msg).unwrap();
        assert_eq!(h.producer, 7);
        assert_eq!(h.seq, 42);
        assert_eq!(h.send_ts_ns, 1_000_000);
        assert!(h.intact);
        assert_eq!(h.body_len, 5);

        let mut corrupted = msg.clone();
        *corrupted.last_mut().unwrap() ^= 0xFF;
        assert!(!decode(&corrupted).unwrap().intact);
    }

    #[test]
    fn test_padded_and_invalid_size() {
        let msg = encode(1, 0, 0, b"", Some(1024)).unwrap();
        assert_eq!(msg.len(), 1024);
        assert!(decode(&msg).unwrap().intact);
        assert!(encode(1, 0, 0, b"abc", Some(HEADER_SIZE - 1)).is_err());
        assert!(decode(b"short").is_none());
    }
}
