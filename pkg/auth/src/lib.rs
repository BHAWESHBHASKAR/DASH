//! HS256 JWT verification with tenant allowlist enforcement.
//!
//! Crypto and JSON parsing are delegated to the `jsonwebtoken`, `serde_json`,
//! and `sha2` crates. The previous hand-rolled SHA-256, HMAC-SHA256, base64url
//! decoder, and recursive-descent JSON parser have been removed.
//!
//! The public API (`JwtValidationConfig`, `JwtValidationError`,
//! `verify_hs256_token_for_tenant`, `encode_hs256_token`,
//! `encode_hs256_token_with_kid`, `sha256_hex`) is preserved so service-layer
//! consumers in `services/*/transport/authz.rs` continue to work unchanged.

use std::collections::{HashMap, HashSet};

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde_json::Value;
use sha2::{Digest, Sha256};

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

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JwtValidationError {
    #[error("invalid token format")]
    InvalidTokenFormat,
    #[error("unsupported algorithm")]
    UnsupportedAlgorithm,
    #[error("invalid base64url encoding")]
    InvalidBase64,
    #[error("invalid utf-8 in token segment")]
    InvalidUtf8,
    #[error("invalid json in token segment")]
    InvalidJson,
    #[error("invalid signature")]
    InvalidSignature,
    #[error("missing claim: {0}")]
    MissingClaim(&'static str),
    #[error("missing claim: {0}")]
    MissingClaimName(String),
    #[error("invalid claim type: {0}")]
    InvalidClaimType(&'static str),
    #[error("unknown key id")]
    UnknownKeyId,
    #[error("token expired")]
    Expired,
    #[error("token not yet valid")]
    NotYetValid,
    #[error("issuer mismatch")]
    IssuerMismatch,
    #[error("audience mismatch")]
    AudienceMismatch,
    #[error("tenant not allowed")]
    TenantNotAllowed,
}

pub fn verify_hs256_token_for_tenant(
    token: &str,
    tenant_id: &str,
    config: &JwtValidationConfig,
    now_unix_secs: u64,
) -> Result<(), JwtValidationError> {
    let header = decode_header(token).map_err(map_jwt_error)?;
    if header.alg != Algorithm::HS256 {
        return Err(JwtValidationError::UnsupportedAlgorithm);
    }

    let candidates = select_hs256_secrets_for_header(&header, config)?;
    let mut last_sig_err: Option<JwtValidationError> = None;

    for secret in candidates {
        let mut validation = Validation::new(Algorithm::HS256);
        // jsonwebtoken's clock is `Utc::now()`, which would break caller-injected
        // time for tests. We do the exp/nbf checks ourselves below with `now_unix_secs`.
        validation.leeway = 0;
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.required_spec_claims = if config.require_exp {
            HashSet::from(["exp".to_string()])
        } else {
            HashSet::new()
        };
        if let Some(iss) = config.issuer.as_deref() {
            validation.set_issuer(&[iss]);
        }
        if let Some(aud) = config.audience.as_deref() {
            validation.set_audience(&[aud]);
        } else {
            validation.validate_aud = false;
        }

        let key = DecodingKey::from_secret(secret.as_bytes());
        match decode::<Value>(token, &key, &validation) {
            Ok(data) => {
                check_time_bounds(&data.claims, config, now_unix_secs)?;
                return check_tenant_allowlist(&data.claims, tenant_id);
            }
            Err(e) => {
                let mapped = map_jwt_error(e);
                if matches!(mapped, JwtValidationError::InvalidSignature) {
                    last_sig_err = Some(mapped);
                    continue;
                }
                return Err(mapped);
            }
        }
    }

    Err(last_sig_err.unwrap_or(JwtValidationError::InvalidSignature))
}

fn select_hs256_secrets_for_header<'a>(
    header: &Header,
    config: &'a JwtValidationConfig,
) -> Result<Vec<&'a str>, JwtValidationError> {
    if let Some(kid) = header.kid.as_deref() {
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

fn check_time_bounds(
    claims: &Value,
    config: &JwtValidationConfig,
    now_unix_secs: u64,
) -> Result<(), JwtValidationError> {
    let obj = claims.as_object().ok_or(JwtValidationError::InvalidJson)?;

    if let Some(exp_value) = obj.get("exp") {
        let exp = exp_value
            .as_u64()
            .ok_or(JwtValidationError::InvalidClaimType("exp"))?;
        let expiry = exp.saturating_add(config.leeway_secs);
        if now_unix_secs > expiry {
            return Err(JwtValidationError::Expired);
        }
    } else if config.require_exp {
        return Err(JwtValidationError::MissingClaim("exp"));
    }

    if let Some(nbf_value) = obj.get("nbf") {
        let nbf = nbf_value
            .as_u64()
            .ok_or(JwtValidationError::InvalidClaimType("nbf"))?;
        let now_with_leeway = now_unix_secs.saturating_add(config.leeway_secs);
        if now_with_leeway < nbf {
            return Err(JwtValidationError::NotYetValid);
        }
    }

    Ok(())
}

fn check_tenant_allowlist(claims: &Value, tenant_id: &str) -> Result<(), JwtValidationError> {
    let tenants = extract_tenants(claims)?;
    if !tenants.contains("*") && !tenants.contains(tenant_id) {
        return Err(JwtValidationError::TenantNotAllowed);
    }
    Ok(())
}

fn extract_tenants(claims: &Value) -> Result<HashSet<String>, JwtValidationError> {
    let obj = claims.as_object().ok_or(JwtValidationError::InvalidJson)?;
    let mut tenants = HashSet::new();

    if let Some(value) = obj.get("tenant_id") {
        match value {
            Value::String(raw) => {
                let trimmed = raw.trim();
                if !trimmed.is_empty() {
                    tenants.insert(trimmed.to_string());
                }
            }
            _ => return Err(JwtValidationError::InvalidClaimType("tenant_id")),
        }
    }

    for key in ["tenants", "tenant_ids"] {
        if let Some(value) = obj.get(key) {
            match value {
                Value::String(raw) => {
                    for tenant in raw.split(',') {
                        let trimmed = tenant.trim();
                        if !trimmed.is_empty() {
                            tenants.insert(trimmed.to_string());
                        }
                    }
                }
                Value::Array(items) => {
                    for item in items {
                        let Value::String(raw) = item else {
                            return Err(JwtValidationError::InvalidClaimType(key));
                        };
                        let trimmed = raw.trim();
                        if !trimmed.is_empty() {
                            tenants.insert(trimmed.to_string());
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

fn map_jwt_error(err: jsonwebtoken::errors::Error) -> JwtValidationError {
    use jsonwebtoken::errors::ErrorKind;
    match err.kind() {
        ErrorKind::InvalidToken => JwtValidationError::InvalidTokenFormat,
        ErrorKind::InvalidSignature => JwtValidationError::InvalidSignature,
        ErrorKind::ExpiredSignature => JwtValidationError::Expired,
        ErrorKind::ImmatureSignature => JwtValidationError::NotYetValid,
        ErrorKind::InvalidIssuer => JwtValidationError::IssuerMismatch,
        ErrorKind::InvalidAudience => JwtValidationError::AudienceMismatch,
        ErrorKind::MissingRequiredClaim(name) => match name.as_str() {
            "exp" => JwtValidationError::MissingClaim("exp"),
            "iss" => JwtValidationError::MissingClaim("iss"),
            "aud" => JwtValidationError::MissingClaim("aud"),
            "sub" => JwtValidationError::MissingClaim("sub"),
            "nbf" => JwtValidationError::MissingClaim("nbf"),
            other => JwtValidationError::MissingClaimName(other.to_string()),
        },
        ErrorKind::Base64(_) => JwtValidationError::InvalidBase64,
        ErrorKind::Json(_) => JwtValidationError::InvalidJson,
        ErrorKind::Utf8(_) => JwtValidationError::InvalidUtf8,
        ErrorKind::InvalidAlgorithm
        | ErrorKind::InvalidAlgorithmName
        | ErrorKind::InvalidKeyFormat => JwtValidationError::UnsupportedAlgorithm,
        ErrorKind::Crypto(_) => JwtValidationError::InvalidSignature,
        _ => JwtValidationError::InvalidTokenFormat,
    }
}

pub fn encode_hs256_token(claims_json: &str, secret: &str) -> Result<String, JwtValidationError> {
    encode_hs256_token_with_kid(claims_json, secret, None)
}

pub fn encode_hs256_token_with_kid(
    claims_json: &str,
    secret: &str,
    kid: Option<&str>,
) -> Result<String, JwtValidationError> {
    let value: Value =
        serde_json::from_str(claims_json).map_err(|_| JwtValidationError::InvalidJson)?;
    if !value.is_object() {
        return Err(JwtValidationError::InvalidJson);
    }

    let mut header = Header::new(Algorithm::HS256);
    if let Some(kid_value) = kid {
        let trimmed = kid_value.trim();
        if trimmed.is_empty() {
            return Err(JwtValidationError::InvalidClaimType("kid"));
        }
        header.kid = Some(trimmed.to_string());
    }

    let key = EncodingKey::from_secret(secret.as_bytes());
    jsonwebtoken::encode(&header, &value, &key).map_err(map_jwt_error)
}

pub fn sha256_hex(input: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

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
        assert!(result.is_ok(), "expected Ok, got {result:?}");
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
        assert!(result.is_ok(), "expected Ok on the good token, got {result:?}");
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
        assert!(result.is_ok(), "expected Ok, got {result:?}");
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
        assert!(result.is_ok(), "expected Ok, got {result:?}");
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

    #[test]
    fn verify_hs256_token_rejects_aud_as_array_when_config_audience_set() {
        let token = encode_hs256_token(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":["ingestion","other"],"exp":4102444800}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &sample_config(), 1_000);
        assert!(result.is_ok(), "expected Ok for array-aud, got {result:?}");
    }

    #[test]
    fn verify_hs256_token_rejects_aud_mismatch() {
        let token = encode_hs256_token(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":"other-aud","exp":4102444800}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &sample_config(), 1_000);
        assert_eq!(result, Err(JwtValidationError::AudienceMismatch));
    }

    #[test]
    fn verify_hs256_token_rejects_nbf_in_future() {
        let token = encode_hs256_token(
            r#"{"tenant_id":"tenant-a","iss":"dash","aud":"ingestion","exp":4102444800,"nbf":1000000}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &sample_config(), 1_000);
        assert_eq!(result, Err(JwtValidationError::NotYetValid));
    }

    #[test]
    fn verify_hs256_token_supports_tenants_array_claim() {
        let token = encode_hs256_token(
            r#"{"tenants":["tenant-a","tenant-b"],"iss":"dash","aud":"ingestion","exp":4102444800}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-b", &sample_config(), 1_000);
        assert!(result.is_ok(), "expected Ok via tenants array, got {result:?}");
    }

    #[test]
    fn verify_hs256_token_supports_wildcard_tenant() {
        let token = encode_hs256_token(
            r#"{"tenant_id":"*","iss":"dash","aud":"ingestion","exp":4102444800}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "any-tenant", &sample_config(), 1_000);
        assert!(result.is_ok(), "expected Ok via wildcard, got {result:?}");
    }

    #[test]
    fn verify_hs256_token_rejects_missing_tenant_claim() {
        let token = encode_hs256_token(
            r#"{"iss":"dash","aud":"ingestion","exp":4102444800}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &sample_config(), 1_000);
        assert_eq!(result, Err(JwtValidationError::MissingClaim("tenant_id")));
    }

    #[test]
    fn verify_hs256_token_rejects_issuer_mismatch() {
        let token = encode_hs256_token(
            r#"{"tenant_id":"tenant-a","iss":"other-iss","aud":"ingestion","exp":4102444800}"#,
            "secret",
        )
        .unwrap();
        let result = verify_hs256_token_for_tenant(&token, "tenant-a", &sample_config(), 1_000);
        assert_eq!(result, Err(JwtValidationError::IssuerMismatch));
    }

    #[test]
    fn verify_hs256_token_rejects_malformed_token() {
        let result =
            verify_hs256_token_for_tenant("not.a.token", "tenant-a", &sample_config(), 1_000);
        assert!(matches!(
            result,
            Err(JwtValidationError::InvalidTokenFormat)
                | Err(JwtValidationError::InvalidBase64)
                | Err(JwtValidationError::InvalidJson)
                | Err(JwtValidationError::InvalidUtf8)
        ));
    }

    #[test]
    fn sha256_hex_matches_known_vector_for_abc() {
        // Known SHA-256("abc") = ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad
        let hex_digest = sha256_hex(b"abc");
        assert_eq!(
            hex_digest,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
