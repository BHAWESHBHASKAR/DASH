use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JwtValidationConfig {
    pub hs256_secret: String,
    pub hs256_fallback_secrets: Vec<String>,
    pub hs256_secrets_by_kid: HashMap<String, String>,
    pub issuer: Option<String>,
    pub audience: Option<String>,
    pub leeway_secs: u64,
    pub require_exp: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum JwtValidationError {
    InvalidTokenFormat,
    UnsupportedAlgorithm,
    InvalidBase64,
    InvalidUtf8,
    InvalidJson,
    InvalidSignature,
    MissingClaim(&'static str),
    InvalidClaimType(&'static str),
    UnknownKeyId,
    Expired,
    NotYetValid,
    IssuerMismatch,
    AudienceMismatch,
    TenantNotAllowed,
}

pub fn verify_hs256_token_for_tenant(
    token: &str,
    tenant_id: &str,
    config: &JwtValidationConfig,
    now_unix_secs: u64,
) -> Result<(), JwtValidationError> {
    let (header_b64, payload_b64, signature_b64) =
        split_jwt(token).ok_or(JwtValidationError::InvalidTokenFormat)?;
    let signing_input = format!("{header_b64}.{payload_b64}");

    let header_raw = decode_base64url(header_b64)?;
    let signature = decode_base64url(signature_b64)?;
    let header_text =
        std::str::from_utf8(&header_raw).map_err(|_| JwtValidationError::InvalidUtf8)?;
    let header = parse_json_object(header_text)?;

    let alg = object_get_string(&header, "alg").ok_or(JwtValidationError::MissingClaim("alg"))?;
    if alg != "HS256" {
        return Err(JwtValidationError::UnsupportedAlgorithm);
    }

    let candidate_secrets = select_hs256_secrets_for_header(&header, config)?;
    let mut signature_matches = false;
    for secret in candidate_secrets {
        let expected_signature = hmac_sha256(secret.as_bytes(), signing_input.as_bytes());
        if constant_time_eq(signature.as_slice(), &expected_signature) {
            signature_matches = true;
            break;
        }
    }
    if !signature_matches {
        return Err(JwtValidationError::InvalidSignature);
    }

    let payload_raw = decode_base64url(payload_b64)?;
    let payload_text =
        std::str::from_utf8(&payload_raw).map_err(|_| JwtValidationError::InvalidUtf8)?;
    let claims = parse_json_object(payload_text)?;

    if let Some(issuer) = config.issuer.as_deref() {
        let claim_issuer =
            object_get_string(&claims, "iss").ok_or(JwtValidationError::MissingClaim("iss"))?;
        if claim_issuer != issuer {
            return Err(JwtValidationError::IssuerMismatch);
        }
    }

    if let Some(audience) = config.audience.as_deref() {
        let claim_audiences = object_get_string_or_string_array(&claims, "aud")?;
        if !claim_audiences.iter().any(|value| value == audience) {
            return Err(JwtValidationError::AudienceMismatch);
        }
    }

    let exp = object_get_u64(&claims, "exp")?;
    if config.require_exp && exp.is_none() {
        return Err(JwtValidationError::MissingClaim("exp"));
    }
    if let Some(exp) = exp {
        let expiry = exp.saturating_add(config.leeway_secs);
        if now_unix_secs > expiry {
            return Err(JwtValidationError::Expired);
        }
    }

    if let Some(nbf) = object_get_u64(&claims, "nbf")? {
        let now_with_leeway = now_unix_secs.saturating_add(config.leeway_secs);
        if now_with_leeway < nbf {
            return Err(JwtValidationError::NotYetValid);
        }
    }

    let tenants = extract_tenants(&claims)?;
    if !tenants.contains("*") && !tenants.contains(tenant_id) {
        return Err(JwtValidationError::TenantNotAllowed);
    }

    Ok(())
}

fn select_hs256_secrets_for_header<'a>(
    header: &HashMap<String, JsonValue>,
    config: &'a JwtValidationConfig,
) -> Result<Vec<&'a str>, JwtValidationError> {
    if let Some(kid) = object_get_optional_string(header, "kid")? {
        let kid = kid.trim();
        if kid.is_empty() {
            return Err(JwtValidationError::InvalidClaimType("kid"));
        }
        let secret = config
            .hs256_secrets_by_kid
            .get(kid)
            .ok_or(JwtValidationError::UnknownKeyId)?;
        return Ok(vec![secret.as_str()]);
    }

    let mut out = Vec::with_capacity(1 + config.hs256_fallback_secrets.len());
    out.push(config.hs256_secret.as_str());
    for secret in &config.hs256_fallback_secrets {
        if !secret.is_empty() && !out.contains(&secret.as_str()) {
            out.push(secret.as_str());
        }
    }
    Ok(out)
}

pub fn encode_hs256_token(claims_json: &str, secret: &str) -> Result<String, JwtValidationError> {
    encode_hs256_token_with_kid(claims_json, secret, None)
}

pub fn encode_hs256_token_with_kid(
    claims_json: &str,
    secret: &str,
    kid: Option<&str>,
) -> Result<String, JwtValidationError> {
    parse_json_object(claims_json)?;
    let mut header_json = String::from(r#"{"alg":"HS256","typ":"JWT""#);
    if let Some(kid) = kid {
        let kid = kid.trim();
        if kid.is_empty() {
            return Err(JwtValidationError::InvalidClaimType("kid"));
        }
        header_json.push_str(",\"kid\":\"");
        header_json.push_str(&json_escape(kid));
        header_json.push('"');
    }
    header_json.push('}');
    let header = encode_base64url(header_json.as_bytes());
    let payload = encode_base64url(claims_json.as_bytes());
    let signing_input = format!("{header}.{payload}");
    let signature = hmac_sha256(secret.as_bytes(), signing_input.as_bytes());
    let signature_b64 = encode_base64url(&signature);
    Ok(format!("{header}.{payload}.{signature_b64}"))
}

fn split_jwt(token: &str) -> Option<(&str, &str, &str)> {
    let mut parts = token.split('.');
    let header = parts.next()?;
    let payload = parts.next()?;
    let signature = parts.next()?;
    if parts.next().is_some() || header.is_empty() || payload.is_empty() || signature.is_empty() {
        return None;
    }
    Some((header, payload, signature))
}

fn decode_base64url(input: &str) -> Result<Vec<u8>, JwtValidationError> {
    let mut out = Vec::with_capacity((input.len() * 3) / 4);
    let mut acc: u32 = 0;
    let mut bits: u8 = 0;

    for byte in input.bytes() {
        if byte == b'=' {
            break;
        }
        let value: u8 = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return Err(JwtValidationError::InvalidBase64),
        };
        acc = (acc << 6) | u32::from(value);
        bits += 6;
        while bits >= 8 {
            bits -= 8;
            out.push(((acc >> bits) & 0xFF) as u8);
        }
    }

    if bits > 0 {
        let mask = (1u32 << bits) - 1;
        if (acc & mask) != 0 {
            return Err(JwtValidationError::InvalidBase64);
        }
    }
    Ok(out)
}

fn encode_base64url(input: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity((input.len() * 4).div_ceil(3));
    let mut i = 0usize;
    while i + 3 <= input.len() {
        let chunk = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8) | input[i + 2] as u32;
        out.push(ALPHABET[((chunk >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((chunk >> 12) & 0x3F) as usize] as char);
        out.push(ALPHABET[((chunk >> 6) & 0x3F) as usize] as char);
        out.push(ALPHABET[(chunk & 0x3F) as usize] as char);
        i += 3;
    }

    let remaining = input.len() - i;
    if remaining == 1 {
        let chunk = (input[i] as u32) << 16;
        out.push(ALPHABET[((chunk >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((chunk >> 12) & 0x3F) as usize] as char);
    } else if remaining == 2 {
        let chunk = ((input[i] as u32) << 16) | ((input[i + 1] as u32) << 8);
        out.push(ALPHABET[((chunk >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((chunk >> 12) & 0x3F) as usize] as char);
        out.push(ALPHABET[((chunk >> 6) & 0x3F) as usize] as char);
    }
    out
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

fn extract_tenants(
    claims: &HashMap<String, JsonValue>,
) -> Result<HashSet<String>, JwtValidationError> {
    let mut tenants = HashSet::new();

    if let Some(value) = claims.get("tenant_id") {
        match value {
            JsonValue::String(raw) => {
                if !raw.trim().is_empty() {
                    tenants.insert(raw.trim().to_string());
                }
            }
            _ => return Err(JwtValidationError::InvalidClaimType("tenant_id")),
        }
    }

    for key in ["tenants", "tenant_ids"] {
        if let Some(value) = claims.get(key) {
            match value {
                JsonValue::String(raw) => {
                    for tenant in raw.split(',') {
                        let tenant = tenant.trim();
                        if !tenant.is_empty() {
                            tenants.insert(tenant.to_string());
                        }
                    }
                }
                JsonValue::Array(items) => {
                    for item in items {
                        let JsonValue::String(raw) = item else {
                            return Err(JwtValidationError::InvalidClaimType(key));
                        };
                        let tenant = raw.trim();
                        if !tenant.is_empty() {
                            tenants.insert(tenant.to_string());
                        }
                    }
                }
                _ => return Err(JwtValidationError::InvalidClaimType(key)),
            }
        }
    }

    if tenants.is_empty() {
        return Err(JwtValidationError::MissingClaim("tenant_id"));
    }
    Ok(tenants)
}

fn parse_json_object(input: &str) -> Result<HashMap<String, JsonValue>, JwtValidationError> {
    let mut parser = JsonParser::new(input);
    let value = parser.parse_value()?;
    parser.skip_whitespace();
    if !parser.is_eof() {
        return Err(JwtValidationError::InvalidJson);
    }
    match value {
        JsonValue::Object(map) => Ok(map),
        _ => Err(JwtValidationError::InvalidJson),
    }
}

fn object_get_string<'a>(map: &'a HashMap<String, JsonValue>, key: &str) -> Option<&'a str> {
    match map.get(key) {
        Some(JsonValue::String(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn object_get_optional_string<'a>(
    map: &'a HashMap<String, JsonValue>,
    key: &'static str,
) -> Result<Option<&'a str>, JwtValidationError> {
    let Some(value) = map.get(key) else {
        return Ok(None);
    };
    match value {
        JsonValue::String(value) => Ok(Some(value.as_str())),
        _ => Err(JwtValidationError::InvalidClaimType(key)),
    }
}

fn object_get_u64(
    map: &HashMap<String, JsonValue>,
    key: &'static str,
) -> Result<Option<u64>, JwtValidationError> {
    let Some(value) = map.get(key) else {
        return Ok(None);
    };
    match value {
        JsonValue::Number(raw) => raw
            .parse::<u64>()
            .map(Some)
            .map_err(|_| JwtValidationError::InvalidClaimType(key)),
        _ => Err(JwtValidationError::InvalidClaimType(key)),
    }
}

fn object_get_string_or_string_array(
    map: &HashMap<String, JsonValue>,
    key: &'static str,
) -> Result<Vec<String>, JwtValidationError> {
    let Some(value) = map.get(key) else {
        return Err(JwtValidationError::MissingClaim(key));
    };
    match value {
        JsonValue::String(raw) => Ok(vec![raw.to_string()]),
        JsonValue::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let JsonValue::String(value) = item else {
                    return Err(JwtValidationError::InvalidClaimType(key));
                };
                out.push(value.clone());
            }
            Ok(out)
        }
        _ => Err(JwtValidationError::InvalidClaimType(key)),
    }
}

fn json_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    const BLOCK_SIZE: usize = 64;
    let mut key_block = [0u8; BLOCK_SIZE];
    if key.len() > BLOCK_SIZE {
        let hashed = sha256(key);
        key_block[..hashed.len()].copy_from_slice(&hashed);
    } else {
        key_block[..key.len()].copy_from_slice(key);
    }

    let mut inner_pad = [0u8; BLOCK_SIZE];
    let mut outer_pad = [0u8; BLOCK_SIZE];
    for i in 0..BLOCK_SIZE {
        inner_pad[i] = key_block[i] ^ 0x36;
        outer_pad[i] = key_block[i] ^ 0x5c;
    }

    let mut inner_input = Vec::with_capacity(BLOCK_SIZE + message.len());
    inner_input.extend_from_slice(&inner_pad);
    inner_input.extend_from_slice(message);
    let inner_hash = sha256(&inner_input);

    let mut outer_input = Vec::with_capacity(BLOCK_SIZE + inner_hash.len());
    outer_input.extend_from_slice(&outer_pad);
    outer_input.extend_from_slice(&inner_hash);
    sha256(&outer_input)
}

fn sha256(input: &[u8]) -> [u8; 32] {
    const K: [u32; 64] = [
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
    let mut h: [u32; 8] = [
        0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab,
        0x5be0cd19,
    ];

    let bit_len = (input.len() as u64) * 8;
    let mut msg = input.to_vec();
    msg.push(0x80);
    while !(msg.len() + 8).is_multiple_of(64) {
        msg.push(0);
    }
    msg.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in msg.chunks(64) {
        let mut w = [0u32; 64];
        for (idx, word) in w.iter_mut().take(16).enumerate() {
            let base = idx * 4;
            *word = u32::from_be_bytes([
                chunk[base],
                chunk[base + 1],
                chunk[base + 2],
                chunk[base + 3],
            ]);
        }
        for idx in 16..64 {
            let s0 =
                w[idx - 15].rotate_right(7) ^ w[idx - 15].rotate_right(18) ^ (w[idx - 15] >> 3);
            let s1 = w[idx - 2].rotate_right(17) ^ w[idx - 2].rotate_right(19) ^ (w[idx - 2] >> 10);
            w[idx] = w[idx - 16]
                .wrapping_add(s0)
                .wrapping_add(w[idx - 7])
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

        for idx in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = hh
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[idx])
                .wrapping_add(w[idx]);
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
    for (idx, value) in h.into_iter().enumerate() {
        let bytes = value.to_be_bytes();
        out[idx * 4..idx * 4 + 4].copy_from_slice(&bytes);
    }
    out
}

#[derive(Debug, Clone, PartialEq)]
enum JsonValue {
    Object(HashMap<String, JsonValue>),
    Array(Vec<JsonValue>),
    String(String),
    Number(String),
    Bool(bool),
    Null,
}

struct JsonParser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> JsonParser<'a> {
    fn new(input: &'a str) -> Self {
        Self {
            bytes: input.as_bytes(),
            pos: 0,
        }
    }

    fn parse_value(&mut self) -> Result<JsonValue, JwtValidationError> {
        self.skip_whitespace();
        match self.peek_byte() {
            Some(b'{') => self.parse_object(),
            Some(b'[') => self.parse_array(),
            Some(b'"') => self.parse_string().map(JsonValue::String),
            Some(b't') => {
                self.expect_literal("true")?;
                Ok(JsonValue::Bool(true))
            }
            Some(b'f') => {
                self.expect_literal("false")?;
                Ok(JsonValue::Bool(false))
            }
            Some(b'n') => {
                self.expect_literal("null")?;
                Ok(JsonValue::Null)
            }
            Some(b'-' | b'0'..=b'9') => self.parse_number().map(JsonValue::Number),
            _ => Err(JwtValidationError::InvalidJson),
        }
    }

    fn parse_object(&mut self) -> Result<JsonValue, JwtValidationError> {
        self.expect_byte(b'{')?;
        self.skip_whitespace();
        let mut map = HashMap::new();
        if self.peek_byte() == Some(b'}') {
            self.pos += 1;
            return Ok(JsonValue::Object(map));
        }
        loop {
            self.skip_whitespace();
            let key = self.parse_string()?;
            self.skip_whitespace();
            self.expect_byte(b':')?;
            let value = self.parse_value()?;
            map.insert(key, value);
            self.skip_whitespace();
            match self.peek_byte() {
                Some(b',') => self.pos += 1,
                Some(b'}') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(JwtValidationError::InvalidJson),
            }
        }
        Ok(JsonValue::Object(map))
    }

    fn parse_array(&mut self) -> Result<JsonValue, JwtValidationError> {
        self.expect_byte(b'[')?;
        self.skip_whitespace();
        let mut items = Vec::new();
        if self.peek_byte() == Some(b']') {
            self.pos += 1;
            return Ok(JsonValue::Array(items));
        }
        loop {
            let value = self.parse_value()?;
            items.push(value);
            self.skip_whitespace();
            match self.peek_byte() {
                Some(b',') => self.pos += 1,
                Some(b']') => {
                    self.pos += 1;
                    break;
                }
                _ => return Err(JwtValidationError::InvalidJson),
            }
        }
        Ok(JsonValue::Array(items))
    }

    fn parse_string(&mut self) -> Result<String, JwtValidationError> {
        self.expect_byte(b'"')?;
        let mut out = String::new();
        while let Some(byte) = self.next_byte() {
            match byte {
                b'"' => return Ok(out),
                b'\\' => {
                    let escaped = self.next_byte().ok_or(JwtValidationError::InvalidJson)?;
                    match escaped {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{0008}'),
                        b'f' => out.push('\u{000C}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let code = self.parse_hex4()?;
                            let ch = char::from_u32(code).ok_or(JwtValidationError::InvalidJson)?;
                            out.push(ch);
                        }
                        _ => return Err(JwtValidationError::InvalidJson),
                    }
                }
                b if b.is_ascii_control() => return Err(JwtValidationError::InvalidJson),
                b => out.push(b as char),
            }
        }
        Err(JwtValidationError::InvalidJson)
    }

    fn parse_number(&mut self) -> Result<String, JwtValidationError> {
        let start = self.pos;
        if self.peek_byte() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek_byte() {
            Some(b'0') => self.pos += 1,
            Some(b'1'..=b'9') => {
                self.pos += 1;
                while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                    self.pos += 1;
                }
            }
            _ => return Err(JwtValidationError::InvalidJson),
        }
        if self.peek_byte() == Some(b'.') {
            self.pos += 1;
            if !matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                return Err(JwtValidationError::InvalidJson);
            }
            while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        if matches!(self.peek_byte(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek_byte(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                return Err(JwtValidationError::InvalidJson);
            }
            while matches!(self.peek_byte(), Some(b'0'..=b'9')) {
                self.pos += 1;
            }
        }
        let raw = std::str::from_utf8(&self.bytes[start..self.pos])
            .map_err(|_| JwtValidationError::InvalidJson)?;
        Ok(raw.to_string())
    }

    fn parse_hex4(&mut self) -> Result<u32, JwtValidationError> {
        let mut value: u32 = 0;
        for _ in 0..4 {
            let byte = self.next_byte().ok_or(JwtValidationError::InvalidJson)?;
            value = (value << 4)
                + match byte {
                    b'0'..=b'9' => (byte - b'0') as u32,
                    b'a'..=b'f' => (byte - b'a' + 10) as u32,
                    b'A'..=b'F' => (byte - b'A' + 10) as u32,
                    _ => return Err(JwtValidationError::InvalidJson),
                };
        }
        Ok(value)
    }

    fn expect_literal(&mut self, literal: &str) -> Result<(), JwtValidationError> {
        let bytes = literal.as_bytes();
        if self.bytes.get(self.pos..self.pos + bytes.len()) == Some(bytes) {
            self.pos += bytes.len();
            Ok(())
        } else {
            Err(JwtValidationError::InvalidJson)
        }
    }

    fn expect_byte(&mut self, expected: u8) -> Result<(), JwtValidationError> {
        match self.next_byte() {
            Some(value) if value == expected => Ok(()),
            _ => Err(JwtValidationError::InvalidJson),
        }
    }

    fn skip_whitespace(&mut self) {
        while matches!(self.peek_byte(), Some(b' ' | b'\n' | b'\r' | b'\t')) {
            self.pos += 1;
        }
    }

    fn is_eof(&self) -> bool {
        self.pos >= self.bytes.len()
    }

    fn peek_byte(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn next_byte(&mut self) -> Option<u8> {
        let out = self.peek_byte()?;
        self.pos += 1;
        Some(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> JwtValidationConfig {
        JwtValidationConfig {
            hs256_secret: "secret".to_string(),
            hs256_fallback_secrets: Vec::new(),
            hs256_secrets_by_kid: HashMap::new(),
            issuer: Some("dash".to_string()),
            audience: Some("ingestion".to_string()),
            leeway_secs: 0,
            require_exp: true,
        }
    }

    #[test]
    fn verify_hs256_token_for_tenant_accepts_valid_token() {
        let token = encode_hs256_token(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":"ingestion","exp":4102444800}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &sample_config(), 1_000);
        assert!(result.is_ok());
    }

    #[test]
    fn verify_hs256_token_for_tenant_rejects_expired_token() {
        let token = encode_hs256_token(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":"ingestion","exp":1}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &sample_config(), 10);
        assert_eq!(result, Err(JwtValidationError::Expired));
    }

    #[test]
    fn verify_hs256_token_for_tenant_rejects_wrong_tenant() {
        let token = encode_hs256_token(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":"ingestion","exp":4102444800}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-b", &sample_config(), 1_000);
        assert_eq!(result, Err(JwtValidationError::TenantNotAllowed));
    }

    #[test]
    fn verify_hs256_token_for_tenant_rejects_invalid_signature() {
        let token = encode_hs256_token(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":"ingestion","exp":4102444800}"#,
            "secret",
        )
        .unwrap();
        let result =
            verify_hs256_token_for_tenant(&token, "tenant-a", &sample_config(), 1_000_000_000);
        assert!(result.is_ok());
        let mut parts = token
            .split('.')
            .map(ToString::to_string)
            .collect::<Vec<String>>();
        let signature = parts.last_mut().unwrap();
        let replacement = if signature.ends_with('A') { 'B' } else { 'A' };
        let _ = signature.pop();
        signature.push(replacement);
        let bad = parts.join(".");
        let bad_result =
            verify_hs256_token_for_tenant(&bad, "tenant-a", &sample_config(), 1_000_000_000);
        assert_eq!(bad_result, Err(JwtValidationError::InvalidSignature));
    }

    #[test]
    fn verify_hs256_token_for_tenant_accepts_fallback_rotation_secret() {
        let mut config = sample_config();
        config.hs256_secret = "active-secret".to_string();
        config.hs256_fallback_secrets = vec!["previous-secret".to_string()];
        let token = encode_hs256_token(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":"ingestion","exp":4102444800}"#,
            "previous-secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &config, 1_000);
        assert!(result.is_ok());
    }

    #[test]
    fn verify_hs256_token_for_tenant_uses_kid_secret_map() {
        let mut config = sample_config();
        config.hs256_secret = "fallback-only".to_string();
        config.hs256_fallback_secrets = vec!["unused".to_string()];
        config
            .hs256_secrets_by_kid
            .insert("next".to_string(), "next-secret".to_string());
        let token = encode_hs256_token_with_kid(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":"ingestion","exp":4102444800}"#,
            "next-secret",
            Some("next"),
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &config, 1_000);
        assert!(result.is_ok());
    }

    #[test]
    fn verify_hs256_token_for_tenant_rejects_unknown_kid() {
        let mut config = sample_config();
        config
            .hs256_secrets_by_kid
            .insert("current".to_string(), "current-secret".to_string());
        let token = encode_hs256_token_with_kid(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":"ingestion","exp":4102444800}"#,
            "current-secret",
            Some("next"),
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &config, 1_000);
        assert_eq!(result, Err(JwtValidationError::UnknownKeyId));
    }
}
