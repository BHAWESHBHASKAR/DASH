use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use jsonwebtoken::{DecodingKey, Validation, decode, decode_header, jwk::JwkSet};
use serde_json::Value;

use crate::{JwtValidationError, check_tenant_allowlist, check_time_bounds_value, map_jwt_error};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcValidationConfig {
    pub issuer: String,
    pub audience: Option<String>,
    pub jwks_url: String,
    pub jwks_refresh_minutes: u64,
    pub leeway_secs: u64,
    pub require_exp: bool,
    pub tenant_claims: Vec<String>,
}

impl Default for OidcValidationConfig {
    fn default() -> Self {
        Self {
            issuer: String::new(),
            audience: None,
            jwks_url: String::new(),
            jwks_refresh_minutes: 15,
            leeway_secs: 0,
            require_exp: true,
            tenant_claims: vec!["tenant_id".to_string()],
        }
    }
}

#[derive(Debug, Clone)]
struct CachedJwks {
    keys: JwkSet,
    fetched_at: Instant,
}

static JWKS_CACHE: OnceLock<Mutex<HashMap<String, CachedJwks>>> = OnceLock::new();

fn jwks_cache() -> &'static Mutex<HashMap<String, CachedJwks>> {
    JWKS_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn cache_lock_err() -> JwtValidationError {
    JwtValidationError::OidcProviderError("JWKS cache lock poisoned".to_string())
}

fn fetch_jwks(url: &str) -> Result<JwkSet, String> {
    let response = ureq::get(url)
        .timeout(Duration::from_secs(15))
        .call()
        .map_err(|e| format!("JWKS fetch failed for {url}: {e}"))?;
    response
        .into_json::<JwkSet>()
        .map_err(|e| format!("JWKS JSON parse failed for {url}: {e}"))
}

fn load_jwks(url: &str, refresh_minutes: u64) -> Result<JwkSet, JwtValidationError> {
    let cache = jwks_cache();
    let ttl = Duration::from_secs(refresh_minutes.max(1).saturating_mul(60));
    let now = Instant::now();

    {
        let guard = cache.lock().map_err(|_| cache_lock_err())?;
        if let Some(cached) = guard.get(url)
            && now.duration_since(cached.fetched_at) < ttl
        {
            return Ok(cached.keys.clone());
        }
    }

    let keys = fetch_jwks(url).map_err(JwtValidationError::OidcProviderError)?;

    let mut guard = cache.lock().map_err(|_| cache_lock_err())?;
    guard.insert(
        url.to_string(),
        CachedJwks {
            keys: keys.clone(),
            fetched_at: now,
        },
    );
    Ok(keys)
}

pub fn clear_jwks_cache() {
    if let Ok(mut guard) = jwks_cache().lock() {
        guard.clear();
    }
}

fn extract_tenants_from_claims(
    claims: &Value,
    claim_names: &[String],
) -> Result<HashSet<String>, JwtValidationError> {
    let obj = claims.as_object().ok_or(JwtValidationError::InvalidJson)?;
    let mut tenants = HashSet::new();

    for name in claim_names {
        if let Some(value) = obj.get(name) {
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
                            return Err(JwtValidationError::InvalidClaimType("tenant"));
                        };
                        let trimmed = raw.trim();
                        if !trimmed.is_empty() {
                            tenants.insert(trimmed.to_string());
                        }
                    }
                }
                _ => return Err(JwtValidationError::InvalidClaimType("tenant")),
            }
        }
    }

    if tenants.is_empty() {
        return Err(JwtValidationError::MissingClaim("tenant_id"));
    }
    Ok(tenants)
}

fn check_tenant_claim(
    claims: &Value,
    tenant_id: &str,
    claim_names: &[String],
) -> Result<(), JwtValidationError> {
    let tenants = extract_tenants_from_claims(claims, claim_names)?;
    if !tenants.contains("*") && !tenants.contains(tenant_id) {
        return Err(JwtValidationError::TenantNotAllowed);
    }
    Ok(())
}

pub fn verify_oidc_token_with_jwks(
    token: &str,
    tenant_id: &str,
    config: &OidcValidationConfig,
    now_unix_secs: u64,
    jwks: &JwkSet,
) -> Result<Value, JwtValidationError> {
    let header = decode_header(token).map_err(map_jwt_error)?;
    let kid = header
        .kid
        .as_deref()
        .ok_or(JwtValidationError::MissingKeyId)?;
    let jwk = jwks.find(kid).ok_or(JwtValidationError::UnknownKeyId)?;

    let key = DecodingKey::from_jwk(jwk).map_err(|_| JwtValidationError::InvalidSignature)?;

    let mut validation = Validation::new(header.alg);
    validation.leeway = 0;
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.required_spec_claims = if config.require_exp {
        HashSet::from(["exp".to_string()])
    } else {
        HashSet::new()
    };
    validation.set_issuer(&[&config.issuer]);
    if let Some(aud) = config.audience.as_deref() {
        validation.set_audience(&[aud]);
    } else {
        validation.validate_aud = false;
    }

    let token_data = decode::<Value>(token, &key, &validation).map_err(map_jwt_error)?;
    check_time_bounds_value(
        &token_data.claims,
        config.leeway_secs,
        config.require_exp,
        now_unix_secs,
    )?;

    if config.tenant_claims.is_empty() {
        check_tenant_allowlist(&token_data.claims, tenant_id)?;
    } else {
        check_tenant_claim(&token_data.claims, tenant_id, &config.tenant_claims)?;
    }
    Ok(token_data.claims)
}

pub fn verify_oidc_token_for_tenant(
    token: &str,
    tenant_id: &str,
    config: &OidcValidationConfig,
    now_unix_secs: u64,
) -> Result<Value, JwtValidationError> {
    if config.jwks_url.is_empty() {
        return Err(JwtValidationError::OidcProviderError(
            "JWKS URL is empty".to_string(),
        ));
    }
    let jwks = load_jwks(&config.jwks_url, config.jwks_refresh_minutes)?;
    verify_oidc_token_with_jwks(token, tenant_id, config, now_unix_secs, &jwks)
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use serde_json::json;

    use super::*;
    use crate::encode_hs256_token_with_kid;

    fn sample_oidc_config() -> OidcValidationConfig {
        OidcValidationConfig {
            issuer: "https://dash.example.com".to_string(),
            audience: Some("dash-api".to_string()),
            jwks_url: "https://dash.example.com/.well-known/jwks.json".to_string(),
            jwks_refresh_minutes: 15,
            leeway_secs: 0,
            require_exp: true,
            tenant_claims: vec!["tenant_id".to_string()],
        }
    }

    fn make_hs256_jwks(secret: &str, kid: &str) -> JwkSet {
        let key = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(secret);
        JwkSet {
            keys: vec![
                serde_json::from_value(json!({
                    "kty": "oct",
                    "alg": "HS256",
                    "kid": kid,
                    "k": key,
                }))
                .unwrap(),
            ],
        }
    }

    fn make_token_with_kid(claims_json: &str, secret: &str, kid: &str) -> String {
        encode_hs256_token_with_kid(claims_json, secret, Some(kid)).unwrap()
    }

    #[test]
    fn oidc_accepts_valid_hs256_jwk() {
        clear_jwks_cache();
        let jwks = make_hs256_jwks("shared-secret", "key-1");
        let token = make_token_with_kid(
            r#"{"tenant_id":"tenant-a","iss":"https://dash.example.com","aud":"dash-api","exp":4102444800}"#,
            "shared-secret",
            "key-1",
        );
        let result =
            verify_oidc_token_with_jwks(&token, "tenant-a", &sample_oidc_config(), 1_000, &jwks);
        assert!(result.is_ok(), "expected Ok, got {result:?}");
    }

    #[test]
    fn oidc_rejects_unknown_kid() {
        clear_jwks_cache();
        let jwks = make_hs256_jwks("shared-secret", "key-1");
        let token = make_token_with_kid(
            r#"{"tenant_id":"tenant-a","iss":"https://dash.example.com","aud":"dash-api","exp":4102444800}"#,
            "shared-secret",
            "key-2",
        );
        let result =
            verify_oidc_token_with_jwks(&token, "tenant-a", &sample_oidc_config(), 1_000, &jwks);
        assert_eq!(result, Err(JwtValidationError::UnknownKeyId));
    }

    #[test]
    fn oidc_rejects_wrong_tenant() {
        clear_jwks_cache();
        let jwks = make_hs256_jwks("shared-secret", "key-1");
        let token = make_token_with_kid(
            r#"{"tenant_id":"tenant-a","iss":"https://dash.example.com","aud":"dash-api","exp":4102444800}"#,
            "shared-secret",
            "key-1",
        );
        let result =
            verify_oidc_token_with_jwks(&token, "tenant-b", &sample_oidc_config(), 1_000, &jwks);
        assert_eq!(result, Err(JwtValidationError::TenantNotAllowed));
    }

    #[test]
    fn oidc_rejects_expired_token() {
        clear_jwks_cache();
        let jwks = make_hs256_jwks("shared-secret", "key-1");
        let token = make_token_with_kid(
            r#"{"tenant_id":"tenant-a","iss":"https://dash.example.com","aud":"dash-api","exp":1}"#,
            "shared-secret",
            "key-1",
        );
        let result =
            verify_oidc_token_with_jwks(&token, "tenant-a", &sample_oidc_config(), 10, &jwks);
        assert_eq!(result, Err(JwtValidationError::Expired));
    }

    #[test]
    fn oidc_rejects_issuer_mismatch() {
        clear_jwks_cache();
        let jwks = make_hs256_jwks("shared-secret", "key-1");
        let token = make_token_with_kid(
            r#"{"tenant_id":"tenant-a","iss":"other-issuer","aud":"dash-api","exp":4102444800}"#,
            "shared-secret",
            "key-1",
        );
        let result =
            verify_oidc_token_with_jwks(&token, "tenant-a", &sample_oidc_config(), 1_000, &jwks);
        assert_eq!(result, Err(JwtValidationError::IssuerMismatch));
    }

    #[test]
    fn oidc_rejects_audience_mismatch() {
        clear_jwks_cache();
        let jwks = make_hs256_jwks("shared-secret", "key-1");
        let token = make_token_with_kid(
            r#"{"tenant_id":"tenant-a","iss":"https://dash.example.com","aud":"other","exp":4102444800}"#,
            "shared-secret",
            "key-1",
        );
        let result =
            verify_oidc_token_with_jwks(&token, "tenant-a", &sample_oidc_config(), 1_000, &jwks);
        assert_eq!(result, Err(JwtValidationError::AudienceMismatch));
    }

    #[test]
    fn oidc_rejects_tampered_signature() {
        clear_jwks_cache();
        let jwks = make_hs256_jwks("shared-secret", "key-1");
        let mut token = make_token_with_kid(
            r#"{"tenant_id":"tenant-a","iss":"https://dash.example.com","aud":"dash-api","exp":4102444800}"#,
            "shared-secret",
            "key-1",
        );
        let mut parts = token
            .split('.')
            .map(ToString::to_string)
            .collect::<Vec<_>>();
        let signature = parts.last_mut().unwrap();
        let replacement = if signature.ends_with('A') { 'B' } else { 'A' };
        signature.pop();
        signature.push(replacement);
        token = parts.join(".");
        let result =
            verify_oidc_token_with_jwks(&token, "tenant-a", &sample_oidc_config(), 1_000, &jwks);
        assert_eq!(result, Err(JwtValidationError::InvalidSignature));
    }
}
