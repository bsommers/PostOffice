//! Minimal zero-dependency RFC 8259 JSON value, parser, serializer, and single-pass
//! linear `scan_json` pre-scanner matching `src/postoffice/jsonscan.py`.

use std::fmt::Write;

#[derive(Clone, Debug, PartialEq)]
pub enum JsonValue {
    Null,
    Bool(bool),
    Number(f64, bool), // (value, is_integer_repr)
    Str(String),
    Array(Vec<JsonValue>),
    Object(Vec<(String, JsonValue)>),
}

impl JsonValue {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            JsonValue::Str(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            JsonValue::Number(n, _) => Some(*n),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            JsonValue::Number(n, _) => Some(*n as i64),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            JsonValue::Number(n, _) if *n >= 0.0 => Some(*n as u64),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            JsonValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[JsonValue]> {
        match self {
            JsonValue::Array(a) => Some(a.as_slice()),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&[(String, JsonValue)]> {
        match self {
            JsonValue::Object(o) => Some(o.as_slice()),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&JsonValue> {
        match self {
            JsonValue::Object(entries) => entries.iter().rev().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    pub fn lookup_path(&self, path: &str) -> Option<&JsonValue> {
        let mut cur = self;
        for part in path.split('.') {
            cur = cur.get(part)?;
        }
        Some(cur)
    }

    pub fn to_json_string(&self) -> String {
        let mut out = String::new();
        self.write_json(&mut out);
        out
    }

    pub fn write_json(&self, out: &mut String) {
        match self {
            JsonValue::Null => out.push_str("null"),
            JsonValue::Bool(true) => out.push_str("true"),
            JsonValue::Bool(false) => out.push_str("false"),
            JsonValue::Number(n, is_int) => {
                if *is_int && n.fract() == 0.0 && n.abs() < (1i64 << 53) as f64 {
                    let _ = write!(out, "{}", *n as i64);
                } else if n.fract() == 0.0 {
                    let _ = write!(out, "{:.1}", n);
                } else {
                    let _ = write!(out, "{}", n);
                }
            }
            JsonValue::Str(s) => write_escaped_str(out, s),
            JsonValue::Array(items) => {
                out.push('[');
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    item.write_json(out);
                }
                out.push(']');
            }
            JsonValue::Object(entries) => {
                out.push('{');
                for (i, (k, v)) in entries.iter().enumerate() {
                    if i > 0 {
                        out.push(',');
                    }
                    write_escaped_str(out, k);
                    out.push(':');
                    v.write_json(out);
                }
                out.push('}');
            }
        }
    }

    pub fn format_python_repr(&self) -> String {
        match self {
            JsonValue::Null => "None".to_string(),
            JsonValue::Bool(true) => "True".to_string(),
            JsonValue::Bool(false) => "False".to_string(),
            JsonValue::Number(n, is_int) => {
                if *is_int && n.fract() == 0.0 {
                    format!("{}", *n as i64)
                } else if n.fract() == 0.0 {
                    format!("{:.1}", n)
                } else {
                    format!("{}", n)
                }
            }
            JsonValue::Str(s) => format!("'{}'", s.replace('\\', "\\\\").replace('\'', "\\'")),
            JsonValue::Array(items) => {
                let parts: Vec<String> = items.iter().map(|v| v.format_python_repr()).collect();
                format!("[{}]", parts.join(", "))
            }
            JsonValue::Object(entries) => {
                let parts: Vec<String> = entries
                    .iter()
                    .map(|(k, v)| format!("'{}': {}", k, v.format_python_repr()))
                    .collect();
                format!("{{{}}}", parts.join(", "))
            }
        }
    }
}

pub fn write_escaped_str(out: &mut String, s: &str) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\x08' => out.push_str("\\b"),
            '\x0c' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Linear single-pass JSON bounds scanner matching `src/postoffice/jsonscan.py`.
pub fn scan_json(
    data: &[u8],
    max_depth: usize,
    max_keys: usize,
    max_array_length: usize,
    max_string_length: usize,
) -> Option<String> {
    let mut depth: usize = 0;
    let mut keys: usize = 0;
    let mut stack: Vec<isize> = Vec::new(); // -1 for object, >= 0 for array comma count
    let mut i = 0;
    let len = data.len();

    while i < len {
        let b = data[i];
        match b {
            b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c => {
                i += 1;
            }
            b'"' => {
                let start = i;
                i += 1;
                while i < len {
                    if data[i] == b'\\' {
                        if i + 1 < len {
                            i += 2;
                        } else {
                            i += 1;
                        }
                    } else if data[i] == b'"' {
                        i += 1;
                        break;
                    } else {
                        i += 1;
                    }
                }
                let end = i;
                let str_len = end.saturating_sub(start).saturating_sub(2);
                if str_len > max_string_length {
                    return Some(format!("string length > {}", max_string_length));
                }
            }
            b'{' | b'[' => {
                depth += 1;
                if depth > max_depth {
                    return Some(format!("depth > {}", max_depth));
                }
                stack.push(if b == b'[' { 0 } else { -1 });
                i += 1;
            }
            b'}' | b']' => {
                depth = depth.saturating_sub(1);
                let _ = stack.pop();
                i += 1;
            }
            b':' => {
                keys += 1;
                if keys > max_keys {
                    return Some(format!("keys > {}", max_keys));
                }
                i += 1;
            }
            b',' => {
                if let Some(last) = stack.last_mut() {
                    if *last >= 0 {
                        *last += 1;
                        if (*last as usize) >= max_array_length {
                            return Some(format!("array length > {}", max_array_length));
                        }
                    }
                }
                i += 1;
            }
            _ => {
                i += 1;
                while i < len {
                    let c = data[i];
                    if matches!(
                        c,
                        b' ' | b'\t' | b'\n' | b'\r' | 0x0b | 0x0c | b'[' | b']' | b'{' | b'}' | b':' | b',' | b'"'
                    ) {
                        break;
                    }
                    i += 1;
                }
            }
        }
    }
    None
}

pub fn parse_json(input: &str) -> Result<JsonValue, String> {
    let mut parser = Parser {
        bytes: input.as_bytes(),
        src: input,
        pos: 0,
        depth: 0,
    };
    let val = parser.parse_value()?;
    parser.skip_ws();
    if parser.pos != parser.bytes.len() {
        return Err(format!("Extra data at char {}", parser.pos));
    }
    Ok(val)
}

struct Parser<'a> {
    bytes: &'a [u8],
    src: &'a str,
    pos: usize,
    depth: usize,
}

impl<'a> Parser<'a> {
    fn skip_ws(&mut self) {
        while self.pos < self.bytes.len() {
            match self.bytes[self.pos] {
                b' ' | b'\t' | b'\n' | b'\r' => self.pos += 1,
                _ => break,
            }
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, String> {
        self.skip_ws();
        if self.pos >= self.bytes.len() {
            return Err("Expecting value: line 1 column 1 (char 0)".to_string());
        }
        match self.bytes[self.pos] {
            b'n' => self.parse_null(),
            b't' => self.parse_true(),
            b'f' => self.parse_false(),
            b'"' => self.parse_string().map(JsonValue::Str),
            b'[' => self.parse_array(),
            b'{' => self.parse_object(),
            b'-' | b'0'..=b'9' => self.parse_number(),
            _ => Err(format!("Expecting value: line 1 column {} (char {})", self.pos + 1, self.pos)),
        }
    }

    fn parse_null(&mut self) -> Result<JsonValue, String> {
        if self.bytes[self.pos..].starts_with(b"null") {
            self.pos += 4;
            Ok(JsonValue::Null)
        } else {
            Err("Expecting value: line 1 column 1 (char 0)".to_string())
        }
    }

    fn parse_true(&mut self) -> Result<JsonValue, String> {
        if self.bytes[self.pos..].starts_with(b"true") {
            self.pos += 4;
            Ok(JsonValue::Bool(true))
        } else {
            Err("Expecting value: line 1 column 1 (char 0)".to_string())
        }
    }

    fn parse_false(&mut self) -> Result<JsonValue, String> {
        if self.bytes[self.pos..].starts_with(b"false") {
            self.pos += 5;
            Ok(JsonValue::Bool(false))
        } else {
            Err("Expecting value: line 1 column 1 (char 0)".to_string())
        }
    }

    fn parse_string(&mut self) -> Result<String, String> {
        if self.bytes.get(self.pos) != Some(&b'"') {
            return Err("Expected string".to_string());
        }
        self.pos += 1;
        let mut out = String::new();
        while self.pos < self.bytes.len() {
            let b = self.bytes[self.pos];
            if b == b'"' {
                self.pos += 1;
                return Ok(out);
            }
            if b == b'\\' {
                self.pos += 1;
                if self.pos >= self.bytes.len() {
                    return Err("Unterminated escape".to_string());
                }
                match self.bytes[self.pos] {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\x08'),
                    b'f' => out.push('\x0c'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        if self.pos + 4 >= self.bytes.len() {
                            return Err("Invalid unicode escape".to_string());
                        }
                        let hex = &self.src[self.pos + 1..self.pos + 5];
                        let code = u32::from_str_radix(hex, 16)
                            .map_err(|_| "Invalid unicode escape".to_string())?;
                        if let Some(ch) = char::from_u32(code) {
                            out.push(ch);
                        }
                        self.pos += 4;
                    }
                    _ => return Err("Invalid escape".to_string()),
                }
                self.pos += 1;
            } else if b < 0x20 {
                return Err("Control character in string".to_string());
            } else {
                let ch = self.src[self.pos..].chars().next().ok_or("Invalid UTF-8")?;
                out.push(ch);
                self.pos += ch.len_utf8();
            }
        }
        Err("Unterminated string".to_string())
    }

    fn parse_number(&mut self) -> Result<JsonValue, String> {
        let start = self.pos;
        if self.bytes.get(self.pos) == Some(&b'-') {
            self.pos += 1;
        }
        let mut has_digits = false;
        while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
            self.pos += 1;
            has_digits = true;
        }
        if !has_digits {
            return Err("Invalid number".to_string());
        }
        let mut is_int = true;
        if self.bytes.get(self.pos) == Some(&b'.') {
            is_int = false;
            self.pos += 1;
            let frac_start = self.pos;
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            if self.pos == frac_start {
                return Err("Invalid decimal".to_string());
            }
        }
        if matches!(self.bytes.get(self.pos), Some(b'e' | b'E')) {
            is_int = false;
            self.pos += 1;
            if matches!(self.bytes.get(self.pos), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            let exp_start = self.pos;
            while self.pos < self.bytes.len() && self.bytes[self.pos].is_ascii_digit() {
                self.pos += 1;
            }
            if self.pos == exp_start {
                return Err("Invalid exponent".to_string());
            }
        }
        let num_str = &self.src[start..self.pos];
        let val: f64 = num_str.parse().map_err(|_| "Invalid number".to_string())?;
        if !val.is_finite() {
            return Err("Non-finite number".to_string());
        }
        Ok(JsonValue::Number(val, is_int))
    }

    fn parse_array(&mut self) -> Result<JsonValue, String> {
        self.depth += 1;
        if self.depth > 256 {
            return Err("Recursion limit exceeded".to_string());
        }
        self.pos += 1; // '['
        self.skip_ws();
        let mut items = Vec::new();
        if self.bytes.get(self.pos) == Some(&b']') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(JsonValue::Array(items));
        }
        loop {
            items.push(self.parse_value()?);
            self.skip_ws();
            match self.bytes.get(self.pos) {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b']') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(JsonValue::Array(items));
                }
                _ => return Err("Expected ',' or ']'".to_string()),
            }
        }
    }

    fn parse_object(&mut self) -> Result<JsonValue, String> {
        self.depth += 1;
        if self.depth > 256 {
            return Err("Recursion limit exceeded".to_string());
        }
        self.pos += 1; // '{'
        self.skip_ws();
        let mut entries = Vec::new();
        if self.bytes.get(self.pos) == Some(&b'}') {
            self.pos += 1;
            self.depth -= 1;
            return Ok(JsonValue::Object(entries));
        }
        loop {
            self.skip_ws();
            let key = self.parse_string()?;
            self.skip_ws();
            if self.bytes.get(self.pos) != Some(&b':') {
                return Err("Expected ':'".to_string());
            }
            self.pos += 1;
            let val = self.parse_value()?;
            entries.push((key, val));
            self.skip_ws();
            match self.bytes.get(self.pos) {
                Some(b',') => {
                    self.pos += 1;
                }
                Some(b'}') => {
                    self.pos += 1;
                    self.depth -= 1;
                    return Ok(JsonValue::Object(entries));
                }
                _ => return Err("Expected ',' or '}'".to_string()),
            }
        }
    }
}
