//! Wire Codecs (`po_codec_t`) & DLQ Fingerprinting:
//! - Confluent 5-byte wire header (`0x00` + big-endian `u32` schema ID).
//! - RFC 7693 `BLAKE2b-128` (`digest_size = 16`) for `po_dlq_fingerprint`.
//! - Native Avro binary schemaless reader/writer & Protobuf binary reader/writer.

use std::collections::HashMap;

use crate::json::{parse_json, JsonValue};

// =========================================================================
// 1. Confluent 5-Byte Wire Format
// =========================================================================

pub fn encode_confluent(schema_id: u32, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.push(0x00);
    out.extend_from_slice(&schema_id.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

pub fn decode_confluent(framed: &[u8]) -> Result<(u32, &[u8]), String> {
    if framed.len() < 5 {
        return Err(format!(
            "Payload too short for wire format framing (expected >= 5 bytes, got {})",
            framed.len()
        ));
    }
    if framed[0] != 0x00 {
        return Err(format!(
            "Invalid wire format magic byte: expected 0x00, got 0x{:x}",
            framed[0]
        ));
    }
    let schema_id = u32::from_be_bytes([framed[1], framed[2], framed[3], framed[4]]);
    Ok((schema_id, &framed[5..]))
}

// =========================================================================
// 2. RFC 7693 BLAKE2b (unkeyed, arbitrary digest_size 1..=64)
// =========================================================================

const BLAKE2B_IV: [u64; 8] = [
    0x6a09e667f3bcc908,
    0xbb67ae8584caa73b,
    0x3c6ef372fe94f82b,
    0xa54ff53a5f1d36f1,
    0x510e527fade682d1,
    0x9b05688c2b3e6c1f,
    0x1f83d9abfb41bd6b,
    0x5be0cd19137e2179,
];

const SIGMA: [[usize; 16]; 12] = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
    [11, 8, 12, 0, 5, 2, 15, 13, 10, 14, 3, 6, 7, 1, 9, 4],
    [7, 9, 3, 1, 13, 12, 11, 14, 2, 6, 5, 10, 4, 0, 15, 8],
    [9, 0, 5, 7, 2, 4, 10, 15, 14, 1, 11, 12, 6, 8, 3, 13],
    [2, 12, 6, 10, 0, 11, 8, 3, 4, 13, 7, 5, 15, 14, 1, 9],
    [12, 5, 1, 15, 14, 13, 4, 10, 0, 7, 6, 3, 9, 2, 8, 11],
    [13, 11, 7, 14, 12, 1, 3, 9, 5, 0, 15, 4, 8, 6, 2, 10],
    [6, 15, 14, 9, 11, 3, 0, 8, 12, 2, 13, 7, 1, 4, 10, 5],
    [10, 2, 8, 4, 7, 6, 1, 5, 15, 11, 9, 14, 3, 12, 13, 0],
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
    [14, 10, 4, 8, 9, 15, 13, 6, 1, 12, 0, 2, 11, 7, 5, 3],
];

#[inline]
fn blake2b_g(v: &mut [u64; 16], a: usize, b: usize, c: usize, d: usize, x: u64, y: u64) {
    v[a] = v[a].wrapping_add(v[b]).wrapping_add(x);
    v[d] = (v[d] ^ v[a]).rotate_right(32);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(24);
    v[a] = v[a].wrapping_add(v[b]).wrapping_add(y);
    v[d] = (v[d] ^ v[a]).rotate_right(16);
    v[c] = v[c].wrapping_add(v[d]);
    v[b] = (v[b] ^ v[c]).rotate_right(63);
}

fn blake2b_compress(h: &mut [u64; 8], block: &[u8; 128], t0: u64, t1: u64, last: bool) {
    let mut v = [0u64; 16];
    v[..8].copy_from_slice(h);
    v[8..16].copy_from_slice(&BLAKE2B_IV);
    v[12] ^= t0;
    v[13] ^= t1;
    if last {
        v[14] = !v[14];
    }

    let mut m = [0u64; 16];
    for (i, chunk) in block.chunks_exact(8).enumerate() {
        m[i] = u64::from_le_bytes(chunk.try_into().unwrap());
    }

    for s in &SIGMA {
        blake2b_g(&mut v, 0, 4, 8, 12, m[s[0]], m[s[1]]);
        blake2b_g(&mut v, 1, 5, 9, 13, m[s[2]], m[s[3]]);
        blake2b_g(&mut v, 2, 6, 10, 14, m[s[4]], m[s[5]]);
        blake2b_g(&mut v, 3, 7, 11, 15, m[s[6]], m[s[7]]);
        blake2b_g(&mut v, 0, 5, 10, 15, m[s[8]], m[s[9]]);
        blake2b_g(&mut v, 1, 6, 11, 12, m[s[10]], m[s[11]]);
        blake2b_g(&mut v, 2, 7, 8, 13, m[s[12]], m[s[13]]);
        blake2b_g(&mut v, 3, 4, 9, 14, m[s[14]], m[s[15]]);
    }

    for i in 0..8 {
        h[i] ^= v[i] ^ v[i + 8];
    }
}

pub fn blake2b_128(parts: &[&[u8]]) -> [u8; 16] {
    let mut h = BLAKE2B_IV;
    h[0] ^= 0x0101_0000 ^ 16; // fanout=1, depth=1, digest_length=16

    let mut buf = [0u8; 128];
    let mut buf_len = 0usize;
    let mut t0 = 0u64;
    let mut t1 = 0u64;

    for &part in parts {
        let mut slice = part;
        while !slice.is_empty() {
            if buf_len == 128 {
                let (new_t0, overflow) = t0.overflowing_add(128);
                t0 = new_t0;
                if overflow {
                    t1 = t1.wrapping_add(1);
                }
                blake2b_compress(&mut h, &buf, t0, t1, false);
                buf_len = 0;
            }
            let take = (128 - buf_len).min(slice.len());
            buf[buf_len..buf_len + take].copy_from_slice(&slice[..take]);
            buf_len += take;
            slice = &slice[take..];
        }
    }

    let (new_t0, overflow) = t0.overflowing_add(buf_len as u64);
    t0 = new_t0;
    if overflow {
        t1 = t1.wrapping_add(1);
    }
    for b in &mut buf[buf_len..] {
        *b = 0;
    }
    blake2b_compress(&mut h, &buf, t0, t1, true);

    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&h[0].to_le_bytes());
    out[8..16].copy_from_slice(&h[1].to_le_bytes());
    out
}

pub fn dlq_fingerprint(
    source_broker: &[u8],
    source_topic: &[u8],
    error_msg: &[u8],
    payload: &[u8],
) -> String {
    let digest = blake2b_128(&[
        source_broker,
        b"\x00",
        source_topic,
        b"\x00",
        error_msg,
        b"\x00",
        payload,
    ]);
    let mut hex = String::with_capacity(32);
    for b in digest {
        use std::fmt::Write;
        let _ = write!(&mut hex, "{:02x}", b);
    }
    hex
}

// =========================================================================
// 3. Schema Registry & Avro / Protobuf Binary Codecs (`po_codec_t`)
// =========================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SchemaKind {
    Avro,
    Protobuf,
}

#[derive(Clone, Debug)]
pub struct FieldDef {
    pub name: String,
    pub field_type: String,
    pub number: u32,
}

#[derive(Clone, Debug)]
pub struct SchemaDef {
    pub schema_id: u32,
    pub name: String,
    pub kind: SchemaKind,
    pub fields: Vec<FieldDef>,
}

#[derive(Default)]
pub struct CodecRegistry {
    by_id: HashMap<u32, SchemaDef>,
    by_name: HashMap<String, u32>,
}

impl CodecRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn register_schema(
        &mut self,
        schema_id: u32,
        name: &str,
        schema_type: &str,
        definition_json: &str,
    ) -> Result<(), String> {
        if name.is_empty() || name.len() > 128 || name.contains('\0') {
            return Err("invalid schema name".to_string());
        }
        let kind = match schema_type.to_ascii_uppercase().as_str() {
            "AVRO" => SchemaKind::Avro,
            "PROTOBUF" => SchemaKind::Protobuf,
            _ => return Err(format!("Unsupported schema type: {}", schema_type)),
        };
        let def_val = parse_json(definition_json)?;
        let fields_arr = def_val
            .get("fields")
            .and_then(|v| v.as_array())
            .ok_or("schema definition requires 'fields' array")?;
        if fields_arr.len() > 256 {
            return Err("more than 256 fields".to_string());
        }
        let mut fields = Vec::with_capacity(fields_arr.len());
        for (idx, f) in fields_arr.iter().enumerate() {
            let fname = f
                .get("name")
                .and_then(|v| v.as_str())
                .ok_or("field missing name")?
                .to_string();
            let ftype = f
                .get("type")
                .and_then(|v| v.as_str())
                .ok_or("field missing type")?
                .to_ascii_lowercase();
            let number = f
                .get("number")
                .and_then(|v| v.as_u64())
                .unwrap_or((idx + 1) as u64) as u32;
            if number == 0 || (19000..=19999).contains(&number) {
                return Err("invalid protobuf field number".to_string());
            }
            fields.push(FieldDef {
                name: fname,
                field_type: ftype,
                number,
            });
        }
        let def = SchemaDef {
            schema_id,
            name: name.to_string(),
            kind,
            fields,
        };
        self.by_name.insert(name.to_string(), schema_id);
        self.by_id.insert(schema_id, def);
        Ok(())
    }

    fn resolve(&self, name: &str, id: Option<u32>, expected: SchemaKind) -> Result<&SchemaDef, String> {
        let def = if !name.is_empty() {
            let sid = self
                .by_name
                .get(name)
                .ok_or_else(|| format!("Schema '{}' not found", name))?;
            self.by_id.get(sid).ok_or("Schema ID not found")?
        } else if let Some(sid) = id {
            self.by_id
                .get(&sid)
                .ok_or_else(|| format!("Schema ID {} not found", sid))?
        } else {
            return Err("Schema name or ID required".to_string());
        };
        if def.kind != expected {
            return Err("Schema type mismatch".to_string());
        }
        Ok(def)
    }

    pub fn serialize(
        &self,
        record_json: &str,
        format_type: &str,
        schema_name: &str,
        wire_format: bool,
    ) -> Result<Vec<u8>, String> {
        let record = parse_json(record_json)?;
        let norm = format_type.to_ascii_lowercase();
        let (schema_id, body) = match norm.as_str() {
            "json" => {
                let sid = self.by_name.get(schema_name).copied().unwrap_or(0);
                (sid, record.to_json_string().into_bytes())
            }
            "avro" => {
                let def = self.resolve(schema_name, None, SchemaKind::Avro)?;
                (def.schema_id, encode_avro_record(&record, def)?)
            }
            "protobuf" => {
                let def = self.resolve(schema_name, None, SchemaKind::Protobuf)?;
                (def.schema_id, encode_protobuf_record(&record, def)?)
            }
            _ => return Err(format!("Unsupported format: {}", format_type)),
        };
        if wire_format {
            Ok(encode_confluent(schema_id, &body))
        } else {
            Ok(body)
        }
    }

    pub fn deserialize(
        &self,
        payload: &[u8],
        format_type: &str,
        schema_name: &str,
        wire_format: bool,
    ) -> Result<String, String> {
        let (extracted_id, body) = if wire_format {
            let (sid, b) = decode_confluent(payload)?;
            (Some(sid), b)
        } else {
            (None, payload)
        };
        let norm = format_type.to_ascii_lowercase();
        let val = match norm.as_str() {
            "json" => {
                let s = std::str::from_utf8(body).map_err(|_| "Invalid UTF-8")?;
                parse_json(s)?
            }
            "avro" => {
                let def = self.resolve(schema_name, extracted_id, SchemaKind::Avro)?;
                decode_avro_record(body, def)?
            }
            "protobuf" => {
                let def = self.resolve(schema_name, extracted_id, SchemaKind::Protobuf)?;
                decode_protobuf_record(body, def)?
            }
            _ => return Err(format!("Unsupported format: {}", format_type)),
        };
        Ok(val.to_json_string())
    }
}

fn write_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(((v as u8) & 0x7f) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn read_uvarint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
    let mut result = 0u64;
    let mut shift = 0u32;
    while *pos < data.len() {
        let b = data[*pos];
        *pos += 1;
        result |= ((b & 0x7f) as u64) << shift;
        if (b & 0x80) == 0 {
            return Ok(result);
        }
        shift += 7;
        if shift >= 64 {
            return Err("varint overflow".to_string());
        }
    }
    Err("unexpected EOF reading varint".to_string())
}

fn write_zigzag(out: &mut Vec<u8>, n: i64) {
    let zz = ((n << 1) ^ (n >> 63)) as u64;
    write_uvarint(out, zz);
}

fn read_zigzag(data: &[u8], pos: &mut usize) -> Result<i64, String> {
    let zz = read_uvarint(data, pos)?;
    Ok(((zz >> 1) as i64) ^ -((zz & 1) as i64))
}

fn encode_avro_record(record: &JsonValue, def: &SchemaDef) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for f in &def.fields {
        let val = record
            .get(&f.name)
            .ok_or_else(|| format!("Missing Avro field '{}'", f.name))?;
        match f.field_type.as_str() {
            "string" => {
                let s = val.as_str().ok_or("Expected string")?;
                write_zigzag(&mut out, s.len() as i64);
                out.extend_from_slice(s.as_bytes());
            }
            "bytes" => {
                let s = val.as_str().ok_or("Expected bytes string")?;
                write_zigzag(&mut out, s.len() as i64);
                out.extend_from_slice(s.as_bytes());
            }
            "int" | "long" => {
                let n = val.as_i64().ok_or("Expected integer")?;
                write_zigzag(&mut out, n);
            }
            "float" => {
                let n = val.as_f64().ok_or("Expected float")? as f32;
                out.extend_from_slice(&n.to_le_bytes());
            }
            "double" => {
                let n = val.as_f64().ok_or("Expected double")?;
                out.extend_from_slice(&n.to_le_bytes());
            }
            "boolean" => {
                let b = val.as_bool().ok_or("Expected boolean")?;
                out.push(if b { 1 } else { 0 });
            }
            other => return Err(format!("Unsupported Avro field type: {}", other)),
        }
    }
    Ok(out)
}

fn decode_avro_record(data: &[u8], def: &SchemaDef) -> Result<JsonValue, String> {
    let mut pos = 0usize;
    let mut entries = Vec::with_capacity(def.fields.len());
    for f in &def.fields {
        let val = match f.field_type.as_str() {
            "string" => {
                let len = read_zigzag(data, &mut pos)?;
                if len < 0 || pos + (len as usize) > data.len() {
                    return Err("Invalid Avro string length".to_string());
                }
                let l = len as usize;
                let s = std::str::from_utf8(&data[pos..pos + l])
                    .map_err(|_| "Invalid UTF-8 in Avro string")?
                    .to_string();
                pos += l;
                JsonValue::Str(s)
            }
            "int" | "long" => {
                let n = read_zigzag(data, &mut pos)?;
                JsonValue::Number(n as f64, true)
            }
            "float" => {
                if pos + 4 > data.len() {
                    return Err("EOF reading Avro float".to_string());
                }
                let v = f32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as f64;
                pos += 4;
                JsonValue::Number(v, false)
            }
            "double" => {
                if pos + 8 > data.len() {
                    return Err("EOF reading Avro double".to_string());
                }
                let v = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                JsonValue::Number(v, false)
            }
            "boolean" => {
                if pos >= data.len() {
                    return Err("EOF reading Avro boolean".to_string());
                }
                let b = data[pos] != 0;
                pos += 1;
                JsonValue::Bool(b)
            }
            other => return Err(format!("Unsupported Avro field type: {}", other)),
        };
        entries.push((f.name.clone(), val));
    }
    Ok(JsonValue::Object(entries))
}

fn encode_protobuf_record(record: &JsonValue, def: &SchemaDef) -> Result<Vec<u8>, String> {
    let mut out = Vec::new();
    for f in &def.fields {
        let Some(val) = record.get(&f.name) else {
            continue;
        };
        match f.field_type.as_str() {
            "string" | "bytes" => {
                let s = val.as_str().ok_or("Expected string")?;
                if !s.is_empty() {
                    write_uvarint(&mut out, ((f.number as u64) << 3) | 2);
                    write_uvarint(&mut out, s.len() as u64);
                    out.extend_from_slice(s.as_bytes());
                }
            }
            "double" => {
                let n = val.as_f64().ok_or("Expected double")?;
                if n != 0.0 {
                    write_uvarint(&mut out, ((f.number as u64) << 3) | 1);
                    out.extend_from_slice(&n.to_le_bytes());
                }
            }
            "float" => {
                let n = val.as_f64().ok_or("Expected float")? as f32;
                if n != 0.0 {
                    write_uvarint(&mut out, ((f.number as u64) << 3) | 5);
                    out.extend_from_slice(&n.to_le_bytes());
                }
            }
            "int32" | "int64" | "uint32" | "uint64" => {
                let n = val.as_i64().ok_or("Expected int")?;
                if n != 0 {
                    write_uvarint(&mut out, (f.number as u64) << 3);
                    write_uvarint(&mut out, n as u64);
                }
            }
            "bool" | "boolean" => {
                let b = val.as_bool().ok_or("Expected bool")?;
                if b {
                    write_uvarint(&mut out, (f.number as u64) << 3);
                    write_uvarint(&mut out, 1);
                }
            }
            other => return Err(format!("Unsupported Protobuf field type: {}", other)),
        }
    }
    Ok(out)
}

fn decode_protobuf_record(data: &[u8], def: &SchemaDef) -> Result<JsonValue, String> {
    let mut pos = 0usize;
    let mut by_num: HashMap<u32, JsonValue> = HashMap::new();
    while pos < data.len() {
        let tag = read_uvarint(data, &mut pos)?;
        let field_num = (tag >> 3) as u32;
        let wire_type = (tag & 0x07) as u8;
        let fdef = def.fields.iter().find(|f| f.number == field_num);
        match wire_type {
            0 => {
                let v = read_uvarint(data, &mut pos)?;
                if let Some(f) = fdef {
                    let jv = if f.field_type == "bool" || f.field_type == "boolean" {
                        JsonValue::Bool(v != 0)
                    } else {
                        JsonValue::Number((v as i64) as f64, true)
                    };
                    by_num.insert(field_num, jv);
                }
            }
            1 => {
                if pos + 8 > data.len() {
                    return Err("EOF reading fixed64".to_string());
                }
                let v = f64::from_le_bytes(data[pos..pos + 8].try_into().unwrap());
                pos += 8;
                by_num.insert(field_num, JsonValue::Number(v, false));
            }
            2 => {
                let len = read_uvarint(data, &mut pos)? as usize;
                if pos + len > data.len() {
                    return Err("EOF reading length-delimited field".to_string());
                }
                let s = std::str::from_utf8(&data[pos..pos + len])
                    .map_err(|_| "Invalid UTF-8 in protobuf string")?
                    .to_string();
                pos += len;
                by_num.insert(field_num, JsonValue::Str(s));
            }
            5 => {
                if pos + 4 > data.len() {
                    return Err("EOF reading fixed32".to_string());
                }
                let v = f32::from_le_bytes(data[pos..pos + 4].try_into().unwrap()) as f64;
                pos += 4;
                by_num.insert(field_num, JsonValue::Number(v, false));
            }
            _ => return Err(format!("Unsupported protobuf wire type {}", wire_type)),
        }
    }
    let mut entries = Vec::new();
    for f in &def.fields {
        if let Some(val) = by_num.remove(&f.number) {
            entries.push((f.name.clone(), val));
        }
    }
    Ok(JsonValue::Object(entries))
}
