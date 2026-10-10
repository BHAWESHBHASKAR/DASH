//! Minimal HS256 JWT minting (HMAC-SHA256 built on `sha2`).

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD as B64;
use serde_json::Value;
use sha2::{Digest, Sha256};

fn hmac_sha256(key: &[u8], msg: &[u8]) -> Vec<u8> {
    let mut k = if key.len() > 64 {
        Sha256::digest(key).to_vec()
    } else {
        key.to_vec()
    };
    k.resize(64, 0);
    let mut inner = Sha256::new();
    inner.update(k.iter().map(|b| b ^ 0x36).collect::<Vec<u8>>());
    inner.update(msg);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(k.iter().map(|b| b ^ 0x5c).collect::<Vec<u8>>());
    outer.update(inner);
    outer.finalize().to_vec()
}

/// Mint an HS256 token over `claims` signed with `secret`.
pub fn mint_hs256(secret: &str, claims: &Value) -> String {
    let header = B64.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
    let payload = B64.encode(serde_json::to_vec(claims).unwrap());
    let signing_input = format!("{header}.{payload}");
    let sig = B64.encode(hmac_sha256(secret.as_bytes(), signing_input.as_bytes()));
    format!("{signing_input}.{sig}")
}

pub fn now_unix() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
