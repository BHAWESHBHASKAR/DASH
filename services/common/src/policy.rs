//! Deny-by-default request authorization shared by the retrieval and
//! ingestion services.
//!
//! An [`AuthPolicy`] is built once at process start (see [`PolicyCell::pin`])
//! and shared behind an `Arc`. Decision rules:
//!
//! * If any authentication method is configured (API keys, scoped keys,
//!   HS256 JWT secrets or OIDC) a request without valid credentials is
//!   rejected with 401. A JWT-only configuration never falls through to an
//!   "open" API-key branch.
//! * If nothing is configured, every request is rejected unless
//!   `DASH_INSECURE_DEV_MODE=1` is set. Startup refuses to proceed in that
//!   situation (see [`AuthPolicy::build`]).
//! * A per-tenant token bucket applies to every authenticated request
//!   (API key, JWT and OIDC alike) and yields 429 with `Retry-After`.

use std::{
    collections::{HashMap, HashSet},
    hash::{Hash, Hasher},
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use auth::{
    JwtValidationConfig, JwtValidationError, OidcValidationConfig, Role, RoleSet, parse_role_claim,
    verify_hs256_token, verify_hs256_token_for_tenant, verify_oidc_token,
    verify_oidc_token_for_tenant,
};

use crate::{
    JWT_SECRET_MIN_LENGTH, SECRET_MIN_LENGTH, constant_time_eq, env_with_fallback,
    insecure_dev_mode_enabled, strict_secrets_enabled, validate_secret_csv_min_len,
    validate_secret_min_len,
};

/// Outcome of an authorization check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthDecision {
    Allowed,
    Unauthorized(&'static str),
    Forbidden(&'static str),
    /// The tenant exceeded its rate limit; retry after the given seconds.
    RateLimited {
        retry_after_secs: u64,
    },
}

/// How a service names its environment variables and defaults.
#[derive(Debug, Clone, Copy)]
pub struct ServiceAuthEnv {
    /// Human readable service name used in messages ("retrieval").
    pub service: &'static str,
    /// Environment infix: `RETRIEVAL` gives `DASH_RETRIEVAL_*` / `EME_RETRIEVAL_*`.
    pub prefix: &'static str,
    /// Role granted to scoped keys that do not list roles explicitly.
    pub default_scoped_role: Role,
    pub default_rate_limit_rps: u64,
    pub default_rate_limit_burst: u64,
}

impl ServiceAuthEnv {
    fn var(&self, suffix: &str) -> Option<String> {
        env_with_fallback(
            &format!("DASH_{}_{suffix}", self.prefix),
            &format!("EME_{}_{suffix}", self.prefix),
        )
    }

    fn dash(&self, suffix: &str) -> String {
        format!("DASH_{}_{suffix}", self.prefix)
    }
}

/// Raw (string) authentication configuration, as read from the environment.
#[derive(Debug, Clone, Default)]
pub struct RawAuthConfig {
    pub api_key: Option<String>,
    pub api_keys: Option<String>,
    pub revoked_api_keys: Option<String>,
    pub allowed_tenants: Option<String>,
    pub api_key_scopes: Option<String>,
    pub jwt_hs256_secret: Option<String>,
    pub jwt_hs256_secrets: Option<String>,
    pub jwt_hs256_secrets_by_kid: Option<String>,
    pub jwt_issuer: Option<String>,
    pub jwt_audience: Option<String>,
    pub jwt_leeway_secs: Option<String>,
    pub jwt_require_exp: Option<String>,
    pub jwt_role_claim: Option<String>,
    pub jwt_provider: Option<String>,
    pub jwt_jwks_url: Option<String>,
    pub jwt_jwks_refresh_minutes: Option<String>,
    pub jwt_tenant_claims: Option<String>,
    pub rate_limit_rps: Option<String>,
    pub rate_limit_burst: Option<String>,
    pub revoked_keys_path: Option<String>,
    /// `DASH_INSECURE_DEV_MODE`.
    pub insecure_dev: bool,
    /// Validate secret strength and reject placeholders.
    pub strict_secrets: bool,
    /// `DASH_METRICS_PUBLIC=1`: expose `/metrics` without authentication.
    pub metrics_public: bool,
}

impl RawAuthConfig {
    pub fn from_env(svc: &ServiceAuthEnv) -> Self {
        Self {
            api_key: svc.var("API_KEY"),
            api_keys: svc.var("API_KEYS"),
            revoked_api_keys: svc.var("REVOKED_API_KEYS"),
            allowed_tenants: svc.var("ALLOWED_TENANTS"),
            api_key_scopes: svc.var("API_KEY_SCOPES"),
            jwt_hs256_secret: svc.var("JWT_HS256_SECRET"),
            jwt_hs256_secrets: svc.var("JWT_HS256_SECRETS"),
            jwt_hs256_secrets_by_kid: svc.var("JWT_HS256_SECRETS_BY_KID"),
            jwt_issuer: svc.var("JWT_ISSUER"),
            jwt_audience: svc.var("JWT_AUDIENCE"),
            jwt_leeway_secs: svc.var("JWT_LEEWAY_SECS"),
            jwt_require_exp: svc.var("JWT_REQUIRE_EXP"),
            jwt_role_claim: svc.var("JWT_ROLE_CLAIM"),
            jwt_provider: svc.var("JWT_PROVIDER"),
            jwt_jwks_url: svc.var("JWT_JWKS_URL"),
            jwt_jwks_refresh_minutes: svc.var("JWT_JWKS_REFRESH_MINUTES"),
            jwt_tenant_claims: svc.var("JWT_TENANT_CLAIMS"),
            rate_limit_rps: svc.var("RATE_LIMIT_PER_TENANT_RPS"),
            rate_limit_burst: svc.var("RATE_LIMIT_BURST"),
            revoked_keys_path: svc.var("REVOKED_KEYS_PATH"),
            insecure_dev: insecure_dev_mode_enabled(),
            strict_secrets: strict_secrets_enabled(),
            metrics_public: matches!(
                std::env::var("DASH_METRICS_PUBLIC")
                    .unwrap_or_default()
                    .trim()
                    .to_ascii_lowercase()
                    .as_str(),
                "1" | "true" | "yes" | "on"
            ),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JwtMode {
    Hs256,
    Oidc,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TenantScope {
    Any,
    Set(HashSet<String>),
}

impl TenantScope {
    fn allows(&self, tenant_id: &str) -> bool {
        match self {
            Self::Any => true,
            Self::Set(tenants) => tenants.contains(tenant_id),
        }
    }
}

struct ScopedKey {
    key: String,
    tenant_scope: TenantScope,
    roles: RoleSet,
}

/// Compiled authorization policy. Build once, share via `Arc`.
pub struct AuthPolicy {
    required_api_keys: Vec<String>,
    revoked_api_keys: HashSet<String>,
    allowed_tenants: TenantScope,
    scoped_api_keys: Vec<ScopedKey>,
    jwt_validation: Option<JwtValidationConfig>,
    oidc_validation: Option<OidcValidationConfig>,
    jwt_role_claim: String,
    rate_limiter: Option<TenantRateLimiter>,
    revocation_list: RevocationList,
    insecure_dev: bool,
    metrics_public: bool,
    /// Set when the configuration could not be built. The policy then denies
    /// every request.
    config_error: Option<String>,
}

impl std::fmt::Debug for AuthPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately omit key material.
        f.debug_struct("AuthPolicy")
            .field("api_keys", &self.required_api_keys.len())
            .field("scoped_api_keys", &self.scoped_api_keys.len())
            .field("jwt_hs256", &self.jwt_validation.is_some())
            .field("oidc", &self.oidc_validation.is_some())
            .field("insecure_dev", &self.insecure_dev)
            .field("config_error", &self.config_error)
            .finish()
    }
}

impl AuthPolicy {
    /// Build a policy from the process environment, validating it. Intended
    /// to be called once at startup.
    pub fn from_env(svc: &ServiceAuthEnv) -> Result<Self, String> {
        Self::build(RawAuthConfig::from_env(svc), svc)
    }

    /// A policy that rejects every request, used when configuration is
    /// invalid and no startup check pinned a policy.
    pub fn deny_all(reason: String) -> Self {
        Self {
            required_api_keys: Vec::new(),
            revoked_api_keys: HashSet::new(),
            allowed_tenants: TenantScope::Any,
            scoped_api_keys: Vec::new(),
            jwt_validation: None,
            oidc_validation: None,
            jwt_role_claim: "dash_roles".to_string(),
            rate_limiter: None,
            revocation_list: RevocationList::new(None),
            insecure_dev: false,
            metrics_public: false,
            config_error: Some(reason),
        }
    }

    /// Parse and validate `raw`. Fails when:
    /// * OIDC is selected but `JWKS_URL` or `ISSUER` is missing,
    /// * the JWT provider name is unknown,
    /// * a scoped key entry is malformed,
    /// * strict secrets are on and a key/secret is a placeholder or too short,
    /// * nothing is configured and dev mode is off.
    pub fn build(raw: RawAuthConfig, svc: &ServiceAuthEnv) -> Result<Self, String> {
        let provider = parse_jwt_mode(svc, raw.jwt_provider.as_deref())?;

        if raw.strict_secrets {
            validate_raw_secrets(&raw, svc)?;
        }

        let required_api_keys = parse_key_list(raw.api_key.as_deref(), raw.api_keys.as_deref());
        let revoked_api_keys: HashSet<String> =
            parse_key_list(None, raw.revoked_api_keys.as_deref())
                .into_iter()
                .collect();
        let scoped_api_keys =
            parse_scoped_api_keys(svc, raw.api_key_scopes.as_deref(), svc.default_scoped_role)?;

        let (jwt_validation, oidc_validation) = match provider {
            JwtMode::Hs256 => (parse_hs256_config(&raw), None),
            JwtMode::Oidc => (None, Some(parse_oidc_config(svc, &raw)?)),
        };

        let has_method = !required_api_keys.is_empty()
            || !scoped_api_keys.is_empty()
            || jwt_validation.is_some()
            || oidc_validation.is_some();
        if !has_method {
            if raw.insecure_dev {
                tracing::warn!(
                    "{} is running with NO AUTHENTICATION because DASH_INSECURE_DEV_MODE=1. \
                     Every request is accepted. Never use this in production.",
                    svc.service
                );
            } else {
                return Err(format!(
                    "{service}: no authentication is configured. Set {api_key} / {api_keys}, \
                     {scopes}, {jwt} or an OIDC provider; for local development only, \
                     set DASH_INSECURE_DEV_MODE=1",
                    service = svc.service,
                    api_key = svc.dash("API_KEY"),
                    api_keys = svc.dash("API_KEYS"),
                    scopes = svc.dash("API_KEY_SCOPES"),
                    jwt = svc.dash("JWT_HS256_SECRET"),
                ));
            }
        }

        let jwt_role_claim = raw
            .jwt_role_claim
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .unwrap_or("dash_roles")
            .to_string();

        Ok(Self {
            required_api_keys,
            revoked_api_keys,
            allowed_tenants: parse_tenant_scope(raw.allowed_tenants.as_deref(), true),
            scoped_api_keys,
            jwt_validation,
            oidc_validation,
            jwt_role_claim,
            rate_limiter: TenantRateLimiter::from_raw(
                raw.rate_limit_rps.as_deref(),
                raw.rate_limit_burst.as_deref(),
                svc.default_rate_limit_rps,
                svc.default_rate_limit_burst,
            ),
            revocation_list: RevocationList::new(
                raw.revoked_keys_path
                    .as_deref()
                    .map(str::trim)
                    .filter(|v| !v.is_empty())
                    .map(PathBuf::from),
            ),
            insecure_dev: raw.insecure_dev,
            metrics_public: raw.metrics_public,
            config_error: None,
        })
    }

    /// True when `/metrics` was explicitly exempted from authentication.
    pub fn metrics_public(&self) -> bool {
        self.metrics_public
    }

    fn has_any_method(&self) -> bool {
        !self.required_api_keys.is_empty()
            || !self.scoped_api_keys.is_empty()
            || self.jwt_validation.is_some()
            || self.oidc_validation.is_some()
    }

    /// True when authentication is disabled (dev mode, nothing configured).
    pub fn is_open_dev_mode(&self) -> bool {
        self.insecure_dev && !self.has_any_method()
    }

    /// Authorize a request for `tenant_id` and `required_role`.
    pub fn authorize_for_tenant(
        &self,
        headers: &HashMap<String, String>,
        tenant_id: &str,
        required_role: Role,
    ) -> AuthDecision {
        self.authorize(headers, Some(tenant_id), required_role)
    }

    /// Authorize a request that is not tenant-scoped (`/metrics`, debug,
    /// `/v1/embeddings`). The caller must still present valid credentials
    /// and hold `required_role`.
    pub fn authorize_any_tenant(
        &self,
        headers: &HashMap<String, String>,
        required_role: Role,
    ) -> AuthDecision {
        self.authorize(headers, None, required_role)
    }

    fn authorize(
        &self,
        headers: &HashMap<String, String>,
        tenant_id: Option<&str>,
        required_role: Role,
    ) -> AuthDecision {
        if self.config_error.is_some() {
            return AuthDecision::Unauthorized("authentication is misconfigured");
        }
        if !self.has_any_method() {
            if !self.insecure_dev {
                return AuthDecision::Unauthorized("authentication is not configured");
            }
            return self.finish(tenant_id);
        }

        let bearer = presented_bearer_token(headers);
        let jwt_candidate = bearer.filter(|token| bearer_looks_like_jwt(token));

        // 1. JWT / OIDC. A JWT-shaped bearer is judged by the JWT verifier
        //    when one is configured; it never falls through to API keys.
        if let Some(token) = jwt_candidate {
            if let Some(oidc) = self.oidc_validation.as_ref() {
                return self.finish_jwt(
                    verify_oidc(token, tenant_id, oidc),
                    tenant_id,
                    required_role,
                    "invalid OIDC token",
                );
            }
            if let Some(jwt) = self.jwt_validation.as_ref() {
                return self.finish_jwt(
                    verify_hs256(token, tenant_id, jwt),
                    tenant_id,
                    required_role,
                    "invalid JWT",
                );
            }
        }

        // 2. API keys (x-api-key header or non-JWT bearer token).
        let presented = headers
            .get("x-api-key")
            .map(String::as_str)
            .or(bearer)
            .map(str::trim)
            .filter(|key| !key.is_empty());
        let Some(key) = presented else {
            return AuthDecision::Unauthorized("missing or invalid API key");
        };
        if self.revoked_api_keys.contains(key) || self.revocation_list.is_revoked(key) {
            return AuthDecision::Unauthorized("API key revoked");
        }

        let roles = if let Some(scoped) = self
            .scoped_api_keys
            .iter()
            .find(|scoped| constant_time_eq(scoped.key.as_bytes(), key.as_bytes()))
        {
            if let Some(tenant_id) = tenant_id
                && !scoped.tenant_scope.allows(tenant_id)
            {
                return AuthDecision::Forbidden("tenant is not allowed for this API key");
            }
            Some(&scoped.roles)
        } else if self
            .required_api_keys
            .iter()
            .any(|candidate| constant_time_eq(candidate.as_bytes(), key.as_bytes()))
        {
            None
        } else {
            return AuthDecision::Unauthorized("missing or invalid API key");
        };

        if let Some(roles) = roles
            && !roles.allows(required_role)
        {
            return AuthDecision::Forbidden("role is not allowed for this API key");
        }
        self.finish(tenant_id)
    }

    fn finish_jwt(
        &self,
        verified: Result<serde_json::Value, JwtValidationError>,
        tenant_id: Option<&str>,
        required_role: Role,
        invalid_message: &'static str,
    ) -> AuthDecision {
        match verified {
            Ok(claims) => {
                if !parse_role_claim(&claims, &self.jwt_role_claim).allows(required_role) {
                    return AuthDecision::Forbidden("role is not allowed for this JWT");
                }
                self.finish(tenant_id)
            }
            Err(JwtValidationError::TenantNotAllowed) => {
                AuthDecision::Forbidden("tenant is not allowed for this JWT")
            }
            Err(JwtValidationError::Expired) => AuthDecision::Unauthorized("JWT expired"),
            Err(JwtValidationError::OidcProviderError(_)) => {
                AuthDecision::Unauthorized("OIDC provider unreachable")
            }
            Err(_) => AuthDecision::Unauthorized(invalid_message),
        }
    }

    /// Final stage shared by every credential type: service tenant policy,
    /// then the per-tenant rate limit.
    fn finish(&self, tenant_id: Option<&str>) -> AuthDecision {
        if let Some(tenant_id) = tenant_id
            && !self.allowed_tenants.allows(tenant_id)
        {
            return AuthDecision::Forbidden("tenant is not allowed by service policy");
        }
        if let Some(limiter) = self.rate_limiter.as_ref()
            && let Err(retry_after_secs) = limiter.check(tenant_id.unwrap_or("*"))
        {
            return AuthDecision::RateLimited { retry_after_secs };
        }
        AuthDecision::Allowed
    }
}

fn verify_hs256(
    token: &str,
    tenant_id: Option<&str>,
    config: &JwtValidationConfig,
) -> Result<serde_json::Value, JwtValidationError> {
    match tenant_id {
        Some(tenant) => verify_hs256_token_for_tenant(token, tenant, config, unix_now_secs()),
        None => verify_hs256_token(token, config, unix_now_secs()),
    }
}

fn verify_oidc(
    token: &str,
    tenant_id: Option<&str>,
    config: &OidcValidationConfig,
) -> Result<serde_json::Value, JwtValidationError> {
    match tenant_id {
        Some(tenant) => verify_oidc_token_for_tenant(token, tenant, config, unix_now_secs()),
        None => verify_oidc_token(token, config, unix_now_secs()),
    }
}

fn presented_bearer_token(headers: &HashMap<String, String>) -> Option<&str> {
    let value = headers.get("authorization")?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    Some(token.trim())
}

fn bearer_looks_like_jwt(token: &str) -> bool {
    let mut parts = token.split('.');
    let first = parts.next().unwrap_or_default();
    let second = parts.next().unwrap_or_default();
    let third = parts.next().unwrap_or_default();
    parts.next().is_none() && !first.is_empty() && !second.is_empty() && !third.is_empty()
}

fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Parsing and validation
// ---------------------------------------------------------------------------

fn parse_jwt_mode(svc: &ServiceAuthEnv, raw: Option<&str>) -> Result<JwtMode, String> {
    match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("hs256") => Ok(JwtMode::Hs256),
        Some("oidc") => Ok(JwtMode::Oidc),
        Some(_) => Err(format!(
            "{} must be 'hs256' or 'oidc'",
            svc.dash("JWT_PROVIDER")
        )),
    }
}

fn parse_tenant_scope(raw: Option<&str>, empty_means_any: bool) -> TenantScope {
    let Some(raw) = raw else {
        return TenantScope::Any;
    };
    let mut tenants = HashSet::new();
    for value in raw.split(',') {
        let tenant = value.trim();
        if tenant.is_empty() {
            continue;
        }
        if tenant == "*" {
            return TenantScope::Any;
        }
        tenants.insert(tenant.to_string());
    }
    if tenants.is_empty() && empty_means_any {
        TenantScope::Any
    } else {
        TenantScope::Set(tenants)
    }
}

fn parse_key_list(single: Option<&str>, csv: Option<&str>) -> Vec<String> {
    let mut keys: Vec<String> = Vec::new();
    let mut push = |value: &str| {
        let value = value.trim();
        if !value.is_empty() && !keys.iter().any(|existing| existing == value) {
            keys.push(value.to_string());
        }
    };
    if let Some(single) = single {
        push(single);
    }
    if let Some(csv) = csv {
        for part in csv.split(',') {
            push(part);
        }
    }
    keys
}

fn scoped_entries(raw: Option<&str>) -> impl Iterator<Item = &str> {
    raw.into_iter()
        .flat_map(|raw| raw.split(';'))
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
}

fn parse_scoped_api_keys(
    svc: &ServiceAuthEnv,
    raw: Option<&str>,
    default_role: Role,
) -> Result<Vec<ScopedKey>, String> {
    let mut scoped = Vec::new();
    for entry in scoped_entries(raw) {
        let parts: Vec<&str> = entry.splitn(3, ':').collect();
        let key = parts[0].trim();
        if parts.len() < 2 || key.is_empty() {
            return Err(format!(
                "{} entries must look like 'key:tenant1,tenant2[:role1,role2]'",
                svc.dash("API_KEY_SCOPES")
            ));
        }
        let tenant_scope = parse_tenant_scope(Some(parts[1].trim()), false);
        let roles = if parts.len() == 3 {
            let mut parsed = Vec::new();
            for role in parts[2]
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
            {
                parsed.push(Role::parse(role).ok_or_else(|| {
                    format!(
                        "{} contains an unknown role '{role}'",
                        svc.dash("API_KEY_SCOPES")
                    )
                })?);
            }
            RoleSet::from_roles(parsed.into_iter())
        } else {
            RoleSet::from_roles(std::iter::once(default_role))
        };
        scoped.push(ScopedKey {
            key: key.to_string(),
            tenant_scope,
            roles,
        });
    }
    Ok(scoped)
}

fn parse_secret_list(raw: Option<&str>) -> Vec<String> {
    raw.map(|raw| {
        raw.split(',')
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .collect()
    })
    .unwrap_or_default()
}

fn parse_secrets_by_kid(raw: Option<&str>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for entry in scoped_entries(raw) {
        let Some((kid, secret)) = entry.split_once(':') else {
            continue;
        };
        let (kid, secret) = (kid.trim(), secret.trim());
        if !kid.is_empty() && !secret.is_empty() {
            out.insert(kid.to_string(), secret.to_string());
        }
    }
    out
}

fn parse_bool(raw: Option<&str>, default: bool) -> bool {
    match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        Some("1" | "true" | "yes" | "on") => true,
        Some("0" | "false" | "no" | "off") => false,
        _ => default,
    }
}

fn non_empty(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToOwned::to_owned)
}

fn parse_hs256_config(raw: &RawAuthConfig) -> Option<JwtValidationConfig> {
    let primary = non_empty(raw.jwt_hs256_secret.as_deref());
    let mut fallback = parse_secret_list(raw.jwt_hs256_secrets.as_deref());
    let by_kid = parse_secrets_by_kid(raw.jwt_hs256_secrets_by_kid.as_deref());

    let primary = match primary {
        Some(primary) => primary,
        // A rotation list or kid map alone is a valid configuration. The
        // first list entry doubles as the primary secret; never an empty key.
        None if !fallback.is_empty() => fallback.remove(0),
        None if !by_kid.is_empty() => String::new(),
        None => return None,
    };
    fallback.retain(|value| value != &primary);
    let mut seen = HashSet::new();
    fallback.retain(|value| seen.insert(value.clone()));

    Some(JwtValidationConfig {
        hs256_secret: primary,
        hs256_fallback_secrets: fallback,
        hs256_secrets_by_kid: by_kid,
        issuer: non_empty(raw.jwt_issuer.as_deref()),
        audience: non_empty(raw.jwt_audience.as_deref()),
        leeway_secs: raw
            .jwt_leeway_secs
            .as_deref()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0),
        require_exp: parse_bool(raw.jwt_require_exp.as_deref(), true),
    })
}

fn parse_oidc_config(
    svc: &ServiceAuthEnv,
    raw: &RawAuthConfig,
) -> Result<OidcValidationConfig, String> {
    let jwks_url = non_empty(raw.jwt_jwks_url.as_deref()).ok_or_else(|| {
        format!(
            "{} is 'oidc' but {} is not set",
            svc.dash("JWT_PROVIDER"),
            svc.dash("JWT_JWKS_URL")
        )
    })?;
    let issuer = non_empty(raw.jwt_issuer.as_deref()).ok_or_else(|| {
        format!(
            "{} is 'oidc' but {} is not set",
            svc.dash("JWT_PROVIDER"),
            svc.dash("JWT_ISSUER")
        )
    })?;
    let tenant_claims = raw
        .jwt_tenant_claims
        .as_deref()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned)
                .collect::<Vec<String>>()
        })
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| {
            vec![
                "tenant_id".to_string(),
                "tenants".to_string(),
                "tenant_ids".to_string(),
            ]
        });
    Ok(OidcValidationConfig {
        issuer,
        audience: non_empty(raw.jwt_audience.as_deref()),
        jwks_url,
        jwks_refresh_minutes: raw
            .jwt_jwks_refresh_minutes
            .as_deref()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(15),
        leeway_secs: raw
            .jwt_leeway_secs
            .as_deref()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(0),
        require_exp: parse_bool(raw.jwt_require_exp.as_deref(), true),
        tenant_claims,
    })
}

/// Strength checks for every secret-bearing setting. Errors name the
/// setting, never the value.
fn validate_raw_secrets(raw: &RawAuthConfig, svc: &ServiceAuthEnv) -> Result<(), String> {
    if let Some(value) = raw.api_key.as_deref().filter(|v| !v.trim().is_empty()) {
        validate_secret_min_len(value, &svc.dash("API_KEY"), SECRET_MIN_LENGTH)?;
    }
    validate_secret_csv_min_len(
        raw.api_keys.as_deref(),
        &svc.dash("API_KEYS"),
        SECRET_MIN_LENGTH,
    )?;
    for entry in scoped_entries(raw.api_key_scopes.as_deref()) {
        let key = entry.split(':').next().unwrap_or_default();
        validate_secret_min_len(key, &svc.dash("API_KEY_SCOPES"), SECRET_MIN_LENGTH)?;
    }
    if let Some(value) = raw
        .jwt_hs256_secret
        .as_deref()
        .filter(|v| !v.trim().is_empty())
    {
        validate_secret_min_len(value, &svc.dash("JWT_HS256_SECRET"), JWT_SECRET_MIN_LENGTH)?;
    }
    validate_secret_csv_min_len(
        raw.jwt_hs256_secrets.as_deref(),
        &svc.dash("JWT_HS256_SECRETS"),
        JWT_SECRET_MIN_LENGTH,
    )?;
    for entry in scoped_entries(raw.jwt_hs256_secrets_by_kid.as_deref()) {
        let secret = entry.split_once(':').map(|(_, s)| s).unwrap_or_default();
        validate_secret_min_len(
            secret,
            &svc.dash("JWT_HS256_SECRETS_BY_KID"),
            JWT_SECRET_MIN_LENGTH,
        )?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Revocation list (reloaded when the file changes)
// ---------------------------------------------------------------------------

const REVOCATION_RECHECK: std::time::Duration = std::time::Duration::from_secs(1);

struct RevocationState {
    keys: HashSet<String>,
    modified: Option<SystemTime>,
    len: u64,
    checked_at: Option<Instant>,
}

struct RevocationList {
    path: Option<PathBuf>,
    state: Mutex<RevocationState>,
}

impl RevocationList {
    fn new(path: Option<PathBuf>) -> Self {
        Self {
            path,
            state: Mutex::new(RevocationState {
                keys: HashSet::new(),
                modified: None,
                len: 0,
                checked_at: None,
            }),
        }
    }

    /// Cheap on the hot path: the file is stat'ed at most once per second and
    /// re-read only when its mtime or size changes.
    fn is_revoked(&self, key: &str) -> bool {
        let Some(path) = self.path.as_ref() else {
            return false;
        };
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        let due = state
            .checked_at
            .is_none_or(|at| at.elapsed() >= REVOCATION_RECHECK);
        if due {
            state.checked_at = Some(Instant::now());
            match std::fs::metadata(path) {
                Ok(meta) => {
                    let modified = meta.modified().ok();
                    if (state.modified != modified || state.len != meta.len())
                        && let Ok(content) = std::fs::read_to_string(path)
                    {
                        state.keys = content
                            .lines()
                            .map(str::trim)
                            .filter(|line| !line.is_empty())
                            .map(ToOwned::to_owned)
                            .collect();
                        state.modified = modified;
                        state.len = meta.len();
                    }
                }
                Err(_) => {
                    state.keys.clear();
                    state.modified = None;
                    state.len = 0;
                }
            }
        }
        state.keys.contains(key)
    }
}

// ---------------------------------------------------------------------------
// Rate limiter
// ---------------------------------------------------------------------------

struct Bucket {
    tokens: f64,
    updated: Instant,
}

/// Per-tenant token bucket: refills at `rps` tokens per second up to `burst`.
pub struct TenantRateLimiter {
    rps: f64,
    burst: f64,
    state: Mutex<HashMap<String, Bucket>>,
}

const BUCKET_IDLE_EVICT_SECS: u64 = 300;
const BUCKET_SOFT_CAP: usize = 4096;

impl TenantRateLimiter {
    /// `rps == 0` is not meaningful here; callers that want "disabled" pass
    /// `None` instead of building a limiter.
    pub fn new(rps: u64, burst: u64) -> Self {
        Self {
            rps: rps.max(1) as f64,
            burst: burst.max(1) as f64,
            state: Mutex::new(HashMap::new()),
        }
    }

    fn from_raw(
        rps: Option<&str>,
        burst: Option<&str>,
        default_rps: u64,
        default_burst: u64,
    ) -> Option<Self> {
        let rps = rps
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(default_rps);
        if rps == 0 {
            return None;
        }
        let burst = burst
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|v| *v > 0)
            .unwrap_or(default_burst.max(rps));
        Some(Self::new(rps, burst))
    }

    /// Take one token for `tenant_id`. On exhaustion returns the number of
    /// seconds (at least 1) after which a token will be available.
    pub fn check(&self, tenant_id: &str) -> Result<(), u64> {
        self.check_at(tenant_id, Instant::now())
    }

    fn check_at(&self, tenant_id: &str, now: Instant) -> Result<(), u64> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.len() > BUCKET_SOFT_CAP {
            state.retain(|_, bucket| {
                now.saturating_duration_since(bucket.updated).as_secs() < BUCKET_IDLE_EVICT_SECS
            });
        }
        let bucket = state.entry(tenant_id.to_string()).or_insert(Bucket {
            tokens: self.burst,
            updated: now,
        });
        let elapsed = now.saturating_duration_since(bucket.updated).as_secs_f64();
        bucket.tokens = (bucket.tokens + elapsed * self.rps).min(self.burst);
        bucket.updated = now;
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            Ok(())
        } else {
            let wait = (1.0 - bucket.tokens) / self.rps;
            Err(wait.ceil().max(1.0) as u64)
        }
    }
}

// ---------------------------------------------------------------------------
// Process-wide policy holder
// ---------------------------------------------------------------------------

/// Holds the policy for a service. [`PolicyCell::pin`] builds it once at
/// startup; the request path then only clones an `Arc`.
///
/// When nothing was pinned (library use, unit and integration tests driving
/// the handler directly) [`PolicyCell::current`] builds a policy from the
/// environment and caches it until the relevant environment changes, so the
/// rate limiter keeps its state across calls.
pub struct PolicyCell {
    pinned: OnceLock<Arc<AuthPolicy>>,
    cached: Mutex<Option<(u64, Arc<AuthPolicy>)>>,
}

impl Default for PolicyCell {
    fn default() -> Self {
        Self::new()
    }
}

impl PolicyCell {
    pub const fn new() -> Self {
        Self {
            pinned: OnceLock::new(),
            cached: Mutex::new(None),
        }
    }

    /// Build the policy from the environment and pin it for the lifetime of
    /// the process. Returns the validation error unchanged so the caller can
    /// refuse to start.
    pub fn pin(&self, svc: &ServiceAuthEnv) -> Result<Arc<AuthPolicy>, String> {
        if let Some(existing) = self.pinned.get() {
            return Ok(Arc::clone(existing));
        }
        let policy = Arc::new(AuthPolicy::from_env(svc)?);
        Ok(Arc::clone(self.pinned.get_or_init(|| policy)))
    }

    /// The policy for the current request.
    pub fn current(&self, svc: &ServiceAuthEnv) -> Arc<AuthPolicy> {
        if let Some(pinned) = self.pinned.get() {
            return Arc::clone(pinned);
        }
        let fingerprint = env_fingerprint(svc);
        let mut cached = self.cached.lock().unwrap_or_else(|p| p.into_inner());
        if let Some((existing, policy)) = cached.as_ref()
            && *existing == fingerprint
        {
            return Arc::clone(policy);
        }
        let policy = Arc::new(AuthPolicy::from_env(svc).unwrap_or_else(|reason| {
            tracing::error!("{} authentication config rejected: {reason}", svc.service);
            AuthPolicy::deny_all(reason)
        }));
        *cached = Some((fingerprint, Arc::clone(&policy)));
        policy
    }
}

fn env_fingerprint(svc: &ServiceAuthEnv) -> u64 {
    let dash = format!("DASH_{}_", svc.prefix);
    let eme = format!("EME_{}_", svc.prefix);
    let mut entries: Vec<(String, String)> = std::env::vars()
        .filter(|(key, _)| {
            key.starts_with(&dash)
                || key.starts_with(&eme)
                || key == "DASH_INSECURE_DEV_MODE"
                || key == "DASH_STRICT_SECRETS"
                || key == "DASH_METRICS_PUBLIC"
        })
        .collect();
    entries.sort();
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    entries.hash(&mut hasher);
    hasher.finish()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    const SVC: ServiceAuthEnv = ServiceAuthEnv {
        service: "test",
        prefix: "TEST",
        default_scoped_role: Role::Retrieve,
        default_rate_limit_rps: 1000,
        default_rate_limit_burst: 1000,
    };

    fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn raw() -> RawAuthConfig {
        RawAuthConfig::default()
    }

    #[test]
    fn jwt_only_policy_rejects_requests_without_credentials() {
        let policy = AuthPolicy::build(
            RawAuthConfig {
                jwt_hs256_secret: Some("0123456789abcdef0123456789abcdef".into()),
                ..raw()
            },
            &SVC,
        )
        .unwrap();
        for hdrs in [
            headers(&[]),
            headers(&[("authorization", "Bearer not-a-jwt")]),
            headers(&[("x-api-key", "anything")]),
        ] {
            assert!(
                matches!(
                    policy.authorize_for_tenant(&hdrs, "tenant-a", Role::Retrieve),
                    AuthDecision::Unauthorized(_)
                ),
                "headers {hdrs:?} must be rejected"
            );
        }
    }

    #[test]
    fn no_auth_configured_fails_closed_unless_dev_mode() {
        let err = AuthPolicy::build(raw(), &SVC).unwrap_err();
        assert!(err.contains("DASH_INSECURE_DEV_MODE"), "{err}");

        let dev = AuthPolicy::build(
            RawAuthConfig {
                insecure_dev: true,
                ..raw()
            },
            &SVC,
        )
        .unwrap();
        assert_eq!(
            dev.authorize_for_tenant(&headers(&[]), "t", Role::Retrieve),
            AuthDecision::Allowed
        );
        assert!(
            matches!(
                AuthPolicy::deny_all("x".into()).authorize_for_tenant(
                    &headers(&[]),
                    "t",
                    Role::Retrieve
                ),
                AuthDecision::Unauthorized(_)
            ),
            "deny_all must reject"
        );
    }

    #[test]
    fn incomplete_oidc_config_is_a_startup_error() {
        let err = AuthPolicy::build(
            RawAuthConfig {
                api_key: Some("k".repeat(20)),
                jwt_provider: Some("oidc".into()),
                jwt_issuer: Some("https://issuer.test".into()),
                ..raw()
            },
            &SVC,
        )
        .unwrap_err();
        assert!(err.contains("JWKS_URL"), "{err}");
        let err = AuthPolicy::build(
            RawAuthConfig {
                api_key: Some("k".repeat(20)),
                jwt_provider: Some("oidc".into()),
                jwt_jwks_url: Some("https://issuer.test/jwks".into()),
                ..raw()
            },
            &SVC,
        )
        .unwrap_err();
        assert!(err.contains("ISSUER"), "{err}");
    }

    #[test]
    fn strict_secrets_reject_placeholders_and_short_values_without_echoing_them() {
        for bad in [
            "<generate-a-32-char-random-string>",
            "change-me-retrieval-key",
            "changeme",
            "example-key-value-1234",
            "secret",
            "password12345678901234",
            "tiny-key",
        ] {
            let err = AuthPolicy::build(
                RawAuthConfig {
                    api_key: Some(bad.into()),
                    strict_secrets: true,
                    ..raw()
                },
                &SVC,
            )
            .unwrap_err();
            assert!(!err.contains(bad), "error leaked the secret: {err}");
        }
        // Each secret-bearing setting is covered.
        for cfg in [
            RawAuthConfig {
                api_key_scopes: Some("changeme:tenant-a".into()),
                ..raw()
            },
            RawAuthConfig {
                jwt_hs256_secret: Some("0123456789abcdef".into()),
                ..raw()
            },
            RawAuthConfig {
                jwt_hs256_secrets_by_kid: Some("k1:<replace>".into()),
                ..raw()
            },
        ] {
            assert!(
                AuthPolicy::build(
                    RawAuthConfig {
                        strict_secrets: true,
                        ..cfg
                    },
                    &SVC
                )
                .is_err()
            );
        }
        AuthPolicy::build(
            RawAuthConfig {
                api_key: Some("a8f3b1c9d2e47f60".into()),
                strict_secrets: true,
                ..raw()
            },
            &SVC,
        )
        .unwrap();
    }

    #[test]
    fn rate_limiter_honors_burst_then_refills_and_reports_retry_after() {
        let limiter = TenantRateLimiter::new(2, 3);
        let t0 = Instant::now();
        assert!(limiter.check_at("a", t0).is_ok());
        assert!(limiter.check_at("a", t0).is_ok());
        assert!(limiter.check_at("a", t0).is_ok());
        let retry = limiter.check_at("a", t0).unwrap_err();
        assert!(retry >= 1);
        // other tenants are independent
        assert!(limiter.check_at("b", t0).is_ok());
        // half a second at 2 rps refills one token
        assert!(
            limiter
                .check_at("a", t0 + std::time::Duration::from_millis(600))
                .is_ok()
        );
        assert!(
            limiter
                .check_at("a", t0 + std::time::Duration::from_millis(600))
                .is_err()
        );
    }

    #[test]
    fn revocation_file_is_reloaded_when_it_changes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("revoked.txt");
        std::fs::write(&path, "key-one\n").unwrap();
        let list = RevocationList::new(Some(path.clone()));
        assert!(list.is_revoked("key-one"));
        assert!(!list.is_revoked("key-two"));
        std::fs::write(&path, "key-one\nkey-two-longer\n").unwrap();
        // force the recheck window to elapse
        list.state.lock().unwrap().checked_at = None;
        assert!(list.is_revoked("key-two-longer"));
    }

    #[test]
    fn constant_time_eq_matches_normal_equality() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
        assert!(!constant_time_eq(b"", b"a"));
        assert!(constant_time_eq(b"", b""));
    }

    #[test]
    fn policy_cell_reuses_one_policy_until_the_environment_changes() {
        // Unique prefix: no other test reads these variables.
        const CELL_SVC: ServiceAuthEnv = ServiceAuthEnv {
            service: "cell-test",
            prefix: "CELLTEST",
            default_scoped_role: Role::Retrieve,
            default_rate_limit_rps: 1000,
            default_rate_limit_burst: 1000,
        };
        unsafe {
            std::env::set_var("DASH_CELLTEST_API_KEY", "cell-test-api-key-0123456789ab");
        }
        let cell = PolicyCell::new();
        let first = cell.current(&CELL_SVC);
        let second = cell.current(&CELL_SVC);
        assert!(
            Arc::ptr_eq(&first, &second),
            "policy must not be rebuilt per call"
        );

        // A pinned policy wins over the environment and is never rebuilt.
        let pinned = cell.pin(&CELL_SVC).expect("pin should succeed");
        unsafe {
            std::env::remove_var("DASH_CELLTEST_API_KEY");
        }
        assert!(Arc::ptr_eq(&pinned, &cell.current(&CELL_SVC)));
        assert!(
            matches!(
                pinned.authorize_for_tenant(
                    &headers(&[("x-api-key", "cell-test-api-key-0123456789ab")]),
                    "t",
                    Role::Retrieve
                ),
                AuthDecision::Allowed
            ),
            "pinned policy keeps the startup configuration"
        );

        // With nothing configured the pin is refused (fail closed).
        let fresh = PolicyCell::new();
        assert!(fresh.pin(&CELL_SVC).is_err());
        assert!(matches!(
            fresh
                .current(&CELL_SVC)
                .authorize_for_tenant(&headers(&[]), "t", Role::Retrieve),
            AuthDecision::Unauthorized(_)
        ));
    }
}
