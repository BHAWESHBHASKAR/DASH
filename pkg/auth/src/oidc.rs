//! OIDC (JWKS) token verification.
//!
//! Hardening summary (all of it is unit-tested):
//!
//! * The JWT header is parsed and its `alg` checked against an allow-list
//!   *before* any network access, so garbage tokens never cause a JWKS fetch.
//! * `iss`, `aud` and `exp` are always required.
//! * One fetch per JWKS URL at a time (single-flight). Other callers use the
//!   cached (stale) keys, or wait for the in-flight fetch when there are none.
//! * Cached keys keep being served for up to 24 hours when a refresh fails
//!   (stale-while-revalidate); a failed fetch is remembered for 30 seconds.
//! * A token whose `kid` is not in the cached set triggers at most one forced
//!   refresh per 60 seconds.
//! * A JWK's own `alg` and `use` members are enforced when present.
//! * Fetches use `https` (or loopback), do not follow redirects, time out
//!   after 3 seconds and are capped at 256 KiB.

use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

use jsonwebtoken::{
    Algorithm, DecodingKey, Validation, decode, decode_header,
    jwk::{Jwk, JwkSet, PublicKeyUse},
};
use serde_json::Value;

use crate::{
    DEFAULT_MAX_LIFETIME_SECS, JwtValidationError, check_time_bounds_value, extract_tenants,
    map_jwt_error, tenant_permitted,
};

/// Keys older than this are never served, even when refreshing fails.
pub const JWKS_STALE_MAX: Duration = Duration::from_secs(24 * 60 * 60);
/// A failed fetch suppresses further fetches for this long.
pub const JWKS_NEGATIVE_TTL: Duration = Duration::from_secs(30);
/// Minimum spacing of forced refreshes triggered by an unknown `kid`.
pub const JWKS_FORCED_REFRESH_INTERVAL: Duration = Duration::from_secs(60);
/// Total time budget for one JWKS fetch.
pub const JWKS_FETCH_TIMEOUT: Duration = Duration::from_secs(3);
/// Maximum accepted JWKS response size.
pub const JWKS_MAX_BYTES: u64 = 256 * 1024;
/// A fetch that has not finished after this long is assumed dead.
const REFRESH_STUCK_AFTER: Duration = Duration::from_secs(10);

/// Signature algorithms accepted from an IdP. Symmetric algorithms and
/// `none` are never accepted for OIDC tokens.
const ALLOWED_ALGORITHMS: &[Algorithm] = &[
    Algorithm::RS256,
    Algorithm::RS384,
    Algorithm::RS512,
    Algorithm::ES256,
    Algorithm::ES384,
    Algorithm::EdDSA,
    Algorithm::PS256,
    Algorithm::PS384,
    Algorithm::PS512,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OidcValidationConfig {
    pub issuer: String,
    /// Required. An empty audience rejects every token.
    pub audience: String,
    pub jwks_url: String,
    pub jwks_refresh_minutes: u64,
    /// Clock-skew leeway; clamped to 60 seconds.
    pub leeway_secs: u64,
    pub max_lifetime_secs: u64,
    pub tenant_claims: Vec<String>,
    /// Honor a `"*"` tenant in the token. Off by default.
    pub allow_wildcard_tenant: bool,
    /// Permit a non-https, non-loopback JWKS URL.
    pub allow_insecure_jwks: bool,
}

impl Default for OidcValidationConfig {
    fn default() -> Self {
        Self {
            issuer: String::new(),
            audience: String::new(),
            jwks_url: String::new(),
            jwks_refresh_minutes: 15,
            leeway_secs: 0,
            max_lifetime_secs: DEFAULT_MAX_LIFETIME_SECS,
            tenant_claims: vec!["tenant_id".to_string()],
            allow_wildcard_tenant: false,
            allow_insecure_jwks: false,
        }
    }
}

/// Reject JWKS URLs that are not `https` unless the host is loopback or
/// `allow_insecure` is set.
pub fn validate_jwks_url(url: &str, allow_insecure: bool) -> Result<(), String> {
    let lower = url.trim().to_ascii_lowercase();
    if lower.starts_with("https://") {
        return Ok(());
    }
    let Some(rest) = lower.strip_prefix("http://") else {
        return Err("JWKS URL must be an https:// URL".to_string());
    };
    if allow_insecure {
        return Ok(());
    }
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    let authority = authority.rsplit('@').next().unwrap_or_default();
    let host = if let Some(stripped) = authority.strip_prefix('[') {
        stripped.split(']').next().unwrap_or_default()
    } else {
        authority.split(':').next().unwrap_or_default()
    };
    let loopback = host == "localhost"
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    if loopback {
        Ok(())
    } else {
        Err(
            "JWKS URL must use https:// (plain http is only allowed for loopback hosts \
             or with DASH_OIDC_ALLOW_INSECURE_JWKS=1)"
                .to_string(),
        )
    }
}

// ---------------------------------------------------------------------------
// JWKS cache
// ---------------------------------------------------------------------------

type Fetcher<'a> = &'a dyn Fn(&str) -> Result<JwkSet, String>;

#[derive(Default)]
struct SlotState {
    keys: Option<Arc<JwkSet>>,
    fetched_at: Option<Instant>,
    failed_at: Option<Instant>,
    forced_at: Option<Instant>,
    refreshing_since: Option<Instant>,
}

#[derive(Default)]
struct Slot {
    state: Mutex<SlotState>,
    changed: Condvar,
}

#[derive(Default)]
pub(crate) struct JwksCache {
    slots: Mutex<HashMap<String, Arc<Slot>>>,
}

fn provider_error(message: &str) -> JwtValidationError {
    JwtValidationError::OidcProviderError(message.to_string())
}

impl JwksCache {
    fn slot(&self, url: &str) -> Arc<Slot> {
        let mut slots = self.slots.lock().unwrap_or_else(|p| p.into_inner());
        Arc::clone(slots.entry(url.to_string()).or_default())
    }

    pub(crate) fn clear(&self) {
        self.slots.lock().unwrap_or_else(|p| p.into_inner()).clear();
    }

    /// Return the key set for `url`.
    ///
    /// * Fresh keys (younger than `ttl`) are returned without fetching.
    /// * `force` asks for a refresh even when the keys are fresh, but at most
    ///   once per [`JWKS_FORCED_REFRESH_INTERVAL`].
    /// * Only one caller fetches at a time. Others get the stale keys, or
    ///   wait for the fetch when no keys have ever been loaded.
    /// * After a failed fetch nothing is fetched for [`JWKS_NEGATIVE_TTL`];
    ///   cached keys younger than [`JWKS_STALE_MAX`] are served meanwhile.
    pub(crate) fn get(
        &self,
        url: &str,
        ttl: Duration,
        force: bool,
        now: Instant,
        fetch: Fetcher<'_>,
    ) -> Result<Arc<JwkSet>, JwtValidationError> {
        let slot = self.slot(url);
        let wait_deadline = Instant::now() + JWKS_FETCH_TIMEOUT + Duration::from_secs(1);
        let mut state = slot.state.lock().unwrap_or_else(|p| p.into_inner());
        let mut force = force;
        loop {
            let age = |t: Option<Instant>| t.map(|t| now.saturating_duration_since(t));
            if force && age(state.forced_at).is_some_and(|a| a < JWKS_FORCED_REFRESH_INTERVAL) {
                force = false;
            }
            let fresh = age(state.fetched_at).is_some_and(|a| a < ttl);
            let stale_ok = age(state.fetched_at).is_some_and(|a| a < JWKS_STALE_MAX);
            if fresh
                && !force
                && let Some(keys) = state.keys.clone()
            {
                return Ok(keys);
            }
            let refreshing = state
                .refreshing_since
                .is_some_and(|t| now.saturating_duration_since(t) < REFRESH_STUCK_AFTER);
            if refreshing {
                if stale_ok && let Some(keys) = state.keys.clone() {
                    return Ok(keys);
                }
                let remaining = wait_deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(provider_error("JWKS refresh timed out"));
                }
                state = slot
                    .changed
                    .wait_timeout(state, remaining)
                    .unwrap_or_else(|p| p.into_inner())
                    .0;
                continue;
            }
            if age(state.failed_at).is_some_and(|a| a < JWKS_NEGATIVE_TTL) {
                if stale_ok && let Some(keys) = state.keys.clone() {
                    return Ok(keys);
                }
                return Err(provider_error("JWKS unavailable"));
            }

            // This caller becomes the fetcher.
            state.refreshing_since = Some(now);
            if force {
                state.forced_at = Some(now);
            }
            drop(state);
            let result = fetch(url);
            let mut state = slot.state.lock().unwrap_or_else(|p| p.into_inner());
            state.refreshing_since = None;
            let outcome = match result {
                Ok(set) => {
                    let keys = Arc::new(set);
                    state.keys = Some(Arc::clone(&keys));
                    state.fetched_at = Some(now);
                    state.failed_at = None;
                    Ok(keys)
                }
                Err(_) => {
                    state.failed_at = Some(now);
                    let stale_ok = state
                        .fetched_at
                        .is_some_and(|t| now.saturating_duration_since(t) < JWKS_STALE_MAX);
                    match state.keys.clone() {
                        Some(keys) if stale_ok => Ok(keys),
                        _ => Err(provider_error("JWKS unavailable")),
                    }
                }
            };
            slot.changed.notify_all();
            return outcome;
        }
    }
}

fn global_cache() -> &'static JwksCache {
    static CACHE: OnceLock<JwksCache> = OnceLock::new();
    CACHE.get_or_init(JwksCache::default)
}

pub fn clear_jwks_cache() {
    global_cache().clear();
}

fn fetch_jwks(url: &str) -> Result<JwkSet, String> {
    let agent = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout(JWKS_FETCH_TIMEOUT)
        .build();
    let response = agent
        .get(url)
        .call()
        .map_err(|e| format!("JWKS fetch failed: {e}"))?;
    let mut body = Vec::new();
    response
        .into_reader()
        .take(JWKS_MAX_BYTES + 1)
        .read_to_end(&mut body)
        .map_err(|e| format!("JWKS read failed: {e}"))?;
    if body.len() as u64 > JWKS_MAX_BYTES {
        return Err("JWKS response exceeds the size cap".to_string());
    }
    serde_json::from_slice::<JwkSet>(&body).map_err(|e| format!("JWKS JSON parse failed: {e}"))
}

// ---------------------------------------------------------------------------
// Verification
// ---------------------------------------------------------------------------

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

fn alg_name<T: serde::Serialize>(alg: T) -> Option<String> {
    serde_json::to_value(alg)
        .ok()
        .and_then(|v| v.as_str().map(ToOwned::to_owned))
}

/// Enforce the JWK's own `alg` and `use` members when present.
fn check_jwk_constraints(jwk: &Jwk, alg: Algorithm) -> Result<(), JwtValidationError> {
    if let Some(key_alg) = jwk.common.key_algorithm
        && alg_name(key_alg) != alg_name(alg)
    {
        return Err(JwtValidationError::UnsupportedAlgorithm);
    }
    if let Some(usage) = jwk.common.public_key_use.as_ref()
        && *usage != PublicKeyUse::Signature
    {
        return Err(JwtValidationError::InvalidSignature);
    }
    Ok(())
}

/// Parse the header and enforce the algorithm allow-list. Performs no I/O.
fn parse_header(token: &str) -> Result<(Algorithm, String), JwtValidationError> {
    let header = decode_header(token).map_err(map_jwt_error)?;
    if !ALLOWED_ALGORITHMS.contains(&header.alg) {
        return Err(JwtValidationError::UnsupportedAlgorithm);
    }
    let kid = header.kid.ok_or(JwtValidationError::MissingKeyId)?;
    Ok((header.alg, kid))
}

fn verify_with_key(
    token: &str,
    tenant_id: Option<&str>,
    config: &OidcValidationConfig,
    now_unix_secs: u64,
    alg: Algorithm,
    jwk: &Jwk,
) -> Result<Value, JwtValidationError> {
    check_jwk_constraints(jwk, alg)?;
    let key = DecodingKey::from_jwk(jwk).map_err(|_| JwtValidationError::InvalidSignature)?;

    let mut validation = Validation::new(alg);
    validation.algorithms = vec![alg];
    validation.leeway = 0;
    validation.validate_exp = false;
    validation.validate_nbf = false;
    validation.required_spec_claims = ["exp", "iss", "aud"].map(String::from).into();
    validation.set_issuer(&[&config.issuer]);
    validation.set_audience(&[&config.audience]);

    let token_data = decode::<Value>(token, &key, &validation).map_err(map_jwt_error)?;
    check_time_bounds_value(
        &token_data.claims,
        config.leeway_secs,
        config.max_lifetime_secs,
        now_unix_secs,
    )?;

    if let Some(tenant_id) = tenant_id {
        let tenants = if config.tenant_claims.is_empty() {
            extract_tenants(&token_data.claims)?
        } else {
            extract_tenants_from_claims(&token_data.claims, &config.tenant_claims)?
        };
        tenant_permitted(&tenants, tenant_id, config.allow_wildcard_tenant)?;
    }
    Ok(token_data.claims)
}

fn require_complete_config(config: &OidcValidationConfig) -> Result<(), JwtValidationError> {
    if config.jwks_url.is_empty() {
        return Err(provider_error("JWKS URL is empty"));
    }
    if config.audience.trim().is_empty() || config.issuer.trim().is_empty() {
        return Err(provider_error("issuer and audience are required"));
    }
    validate_jwks_url(&config.jwks_url, config.allow_insecure_jwks)
        .map_err(|_| provider_error("JWKS URL is not allowed"))
}

/// Verify against an explicit key set (no network, no cache).
pub fn verify_oidc_token_with_jwks(
    token: &str,
    tenant_id: &str,
    config: &OidcValidationConfig,
    now_unix_secs: u64,
    jwks: &JwkSet,
) -> Result<Value, JwtValidationError> {
    let (alg, kid) = parse_header(token)?;
    let jwk = jwks.find(&kid).ok_or(JwtValidationError::UnknownKeyId)?;
    verify_with_key(token, Some(tenant_id), config, now_unix_secs, alg, jwk)
}

fn verify_via_cache(
    cache: &JwksCache,
    fetch: Fetcher<'_>,
    token: &str,
    tenant_id: Option<&str>,
    config: &OidcValidationConfig,
    now_unix_secs: u64,
    now: Instant,
) -> Result<Value, JwtValidationError> {
    // Header and algorithm first: garbage never reaches the network.
    let (alg, kid) = parse_header(token)?;
    require_complete_config(config)?;
    let ttl = Duration::from_secs(config.jwks_refresh_minutes.max(1).saturating_mul(60));
    let mut keys = cache.get(&config.jwks_url, ttl, false, now, fetch)?;
    if keys.find(&kid).is_none() {
        // Possible key rotation: one forced refresh per interval.
        keys = cache.get(&config.jwks_url, ttl, true, now, fetch)?;
    }
    let jwk = keys.find(&kid).ok_or(JwtValidationError::UnknownKeyId)?;
    verify_with_key(token, tenant_id, config, now_unix_secs, alg, jwk)
}

pub fn verify_oidc_token_for_tenant(
    token: &str,
    tenant_id: &str,
    config: &OidcValidationConfig,
    now_unix_secs: u64,
) -> Result<Value, JwtValidationError> {
    verify_via_cache(
        global_cache(),
        &fetch_jwks,
        token,
        Some(tenant_id),
        config,
        now_unix_secs,
        Instant::now(),
    )
}

/// Verify an OIDC token (signature, issuer, audience, time bounds) without a
/// tenant check, for endpoints that are not tenant-scoped.
pub fn verify_oidc_token(
    token: &str,
    config: &OidcValidationConfig,
    now_unix_secs: u64,
) -> Result<Value, JwtValidationError> {
    verify_via_cache(
        global_cache(),
        &fetch_jwks,
        token,
        None,
        config,
        now_unix_secs,
        Instant::now(),
    )
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;

    pub(crate) fn new_cache() -> JwksCache {
        JwksCache::default()
    }

    pub(crate) fn verify(
        cache: &JwksCache,
        fetch: Fetcher<'_>,
        token: &str,
        tenant: &str,
        config: &OidcValidationConfig,
        now_unix: u64,
        now: Instant,
    ) -> Result<Value, JwtValidationError> {
        verify_via_cache(cache, fetch, token, Some(tenant), config, now_unix, now)
    }

    pub(crate) fn get(
        cache: &JwksCache,
        url: &str,
        ttl: Duration,
        force: bool,
        now: Instant,
        fetch: Fetcher<'_>,
    ) -> Result<Arc<JwkSet>, JwtValidationError> {
        cache.get(url, ttl, force, now, fetch)
    }

    pub(crate) fn real_fetch(url: &str) -> Result<JwkSet, String> {
        fetch_jwks(url)
    }
}
