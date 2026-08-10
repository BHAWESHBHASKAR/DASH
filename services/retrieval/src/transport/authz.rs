use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use auth::{
    JwtValidationConfig, JwtValidationError, OidcValidationConfig, verify_hs256_token_for_tenant,
    verify_oidc_token_for_tenant,
};
pub use auth::{Role, RoleSet, parse_role_claim};

use super::{HttpRequest, env_with_fallback};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AuthDecision {
    Allowed,
    Unauthorized(&'static str),
    Forbidden(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JwtMode {
    Hs256,
    Oidc,
}

struct ScopedKey {
    tenant_scope: TenantScope,
    roles: RoleSet,
}

pub(crate) struct AuthPolicy {
    required_api_keys: HashSet<String>,
    revoked_api_keys: HashSet<String>,
    allowed_tenants: TenantScope,
    scoped_api_keys: HashMap<String, ScopedKey>,
    jwt_validation: Option<JwtValidationConfig>,
    oidc_validation: Option<OidcValidationConfig>,
    jwt_mode: JwtMode,
    jwt_role_claim: String,
    rate_limiter: Option<TenantRateLimiter>,
    revocation_list: Option<RevocationList>,
}

impl AuthPolicy {
    pub(super) fn from_env(
        required_api_key: Option<String>,
        required_api_keys_raw: Option<String>,
        revoked_api_keys_raw: Option<String>,
        allowed_tenants_raw: Option<String>,
        scoped_api_keys_raw: Option<String>,
    ) -> Self {
        Self {
            required_api_keys: parse_api_key_set(
                required_api_key.as_deref(),
                required_api_keys_raw.as_deref(),
            ),
            revoked_api_keys: parse_api_key_set(None, revoked_api_keys_raw.as_deref()),
            allowed_tenants: parse_tenant_scope(allowed_tenants_raw.as_deref(), true),
            scoped_api_keys: parse_scoped_api_keys(scoped_api_keys_raw.as_deref(), Role::Retrieve),
            jwt_role_claim: env_with_fallback(
                "DASH_RETRIEVAL_JWT_ROLE_CLAIM",
                "EME_RETRIEVAL_JWT_ROLE_CLAIM",
            )
            .as_deref()
            .map(|value| {
                let trimmed = value.trim();
                if trimmed.is_empty() {
                    "dash_roles".to_string()
                } else {
                    trimmed.to_string()
                }
            })
            .unwrap_or_else(|| "dash_roles".to_string()),
            jwt_validation: parse_jwt_validation_config(
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_HS256_SECRET",
                    "EME_RETRIEVAL_JWT_HS256_SECRET",
                ),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_HS256_SECRETS",
                    "EME_RETRIEVAL_JWT_HS256_SECRETS",
                ),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_HS256_SECRETS_BY_KID",
                    "EME_RETRIEVAL_JWT_HS256_SECRETS_BY_KID",
                ),
                env_with_fallback("DASH_RETRIEVAL_JWT_ISSUER", "EME_RETRIEVAL_JWT_ISSUER"),
                env_with_fallback("DASH_RETRIEVAL_JWT_AUDIENCE", "EME_RETRIEVAL_JWT_AUDIENCE"),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_LEEWAY_SECS",
                    "EME_RETRIEVAL_JWT_LEEWAY_SECS",
                ),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_REQUIRE_EXP",
                    "EME_RETRIEVAL_JWT_REQUIRE_EXP",
                ),
            ),
            rate_limiter: TenantRateLimiter::from_env(
                "DASH_RETRIEVAL_RATE_LIMIT_PER_TENANT_RPS",
                "DASH_RETRIEVAL_RATE_LIMIT_BURST",
            ),
            revocation_list: Some(RevocationList::from_env(
                "DASH_RETRIEVAL_REVOKED_KEYS_PATH",
                "EME_RETRIEVAL_REVOKED_KEYS_PATH",
            )),
            oidc_validation: parse_oidc_validation_config(
                parse_jwt_mode(
                    env_with_fallback("DASH_RETRIEVAL_JWT_PROVIDER", "EME_RETRIEVAL_JWT_PROVIDER")
                        .as_deref(),
                ),
                env_with_fallback("DASH_RETRIEVAL_JWT_JWKS_URL", "EME_RETRIEVAL_JWT_JWKS_URL"),
                env_with_fallback("DASH_RETRIEVAL_JWT_ISSUER", "EME_RETRIEVAL_JWT_ISSUER"),
                env_with_fallback("DASH_RETRIEVAL_JWT_AUDIENCE", "EME_RETRIEVAL_JWT_AUDIENCE"),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_LEEWAY_SECS",
                    "EME_RETRIEVAL_JWT_LEEWAY_SECS",
                ),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_REQUIRE_EXP",
                    "EME_RETRIEVAL_JWT_REQUIRE_EXP",
                ),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_JWKS_REFRESH_MINUTES",
                    "EME_RETRIEVAL_JWT_JWKS_REFRESH_MINUTES",
                ),
                env_with_fallback(
                    "DASH_RETRIEVAL_JWT_TENANT_CLAIMS",
                    "EME_RETRIEVAL_JWT_TENANT_CLAIMS",
                ),
            ),
            jwt_mode: parse_jwt_mode(
                env_with_fallback("DASH_RETRIEVAL_JWT_PROVIDER", "EME_RETRIEVAL_JWT_PROVIDER")
                    .as_deref(),
            ),
        }
    }
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

pub(crate) fn authorize_request_for_tenant(
    request: &HttpRequest,
    tenant_id: &str,
    policy: &AuthPolicy,
    required_role: Role,
) -> AuthDecision {
    if let Some(oidc_config) = policy.oidc_validation.as_ref()
        && let Some(token) = presented_bearer_token(request)
        && bearer_looks_like_jwt(token)
        && policy.jwt_mode == JwtMode::Oidc
    {
        return match verify_oidc_token_for_tenant(token, tenant_id, oidc_config, unix_now_secs()) {
            Ok(claims) => {
                if !policy.allowed_tenants.allows(tenant_id) {
                    AuthDecision::Forbidden("tenant is not allowed by service policy")
                } else if !parse_role_claim(&claims, &policy.jwt_role_claim).allows(required_role) {
                    AuthDecision::Forbidden("role is not allowed for this JWT")
                } else {
                    AuthDecision::Allowed
                }
            }
            Err(JwtValidationError::TenantNotAllowed) => {
                AuthDecision::Forbidden("tenant is not allowed for this JWT")
            }
            Err(JwtValidationError::Expired) => AuthDecision::Unauthorized("JWT expired"),
            Err(JwtValidationError::OidcProviderError(_)) => {
                AuthDecision::Unauthorized("OIDC provider unreachable")
            }
            Err(_) => AuthDecision::Unauthorized("invalid OIDC token"),
        };
    }

    if let Some(jwt_config) = policy.jwt_validation.as_ref()
        && let Some(token) = presented_bearer_token(request)
        && bearer_looks_like_jwt(token)
    {
        return match verify_hs256_token_for_tenant(token, tenant_id, jwt_config, unix_now_secs()) {
            Ok(claims) => {
                if !policy.allowed_tenants.allows(tenant_id) {
                    AuthDecision::Forbidden("tenant is not allowed by service policy")
                } else if !parse_role_claim(&claims, &policy.jwt_role_claim).allows(required_role) {
                    AuthDecision::Forbidden("role is not allowed for this JWT")
                } else {
                    AuthDecision::Allowed
                }
            }
            Err(JwtValidationError::TenantNotAllowed) => {
                AuthDecision::Forbidden("tenant is not allowed for this JWT")
            }
            Err(JwtValidationError::Expired) => AuthDecision::Unauthorized("JWT expired"),
            Err(_) => AuthDecision::Unauthorized("invalid JWT"),
        };
    }

    let maybe_api_key = presented_api_key(request);
    if policy
        .revoked_api_keys
        .contains(maybe_api_key.unwrap_or(""))
    {
        return AuthDecision::Unauthorized("API key revoked");
    }
    if let Some(ref revocation_list) = policy.revocation_list
        && let Some(key) = maybe_api_key
        && revocation_list.is_revoked(key)
    {
        return AuthDecision::Unauthorized("API key revoked");
    }

    let api_key_roles = if !policy.scoped_api_keys.is_empty() {
        let Some(api_key) = maybe_api_key else {
            return AuthDecision::Unauthorized("missing or invalid API key");
        };
        if let Some(scoped) = policy.scoped_api_keys.get(api_key) {
            if !scoped.tenant_scope.allows(tenant_id) {
                return AuthDecision::Forbidden("tenant is not allowed for this API key");
            }
            Some(scoped.roles.clone())
        } else if policy.required_api_keys.is_empty() || !policy.required_api_keys.contains(api_key)
        {
            return AuthDecision::Unauthorized("missing or invalid API key");
        } else {
            None
        }
    } else if !policy.required_api_keys.is_empty()
        && !matches!(maybe_api_key, Some(key) if policy.required_api_keys.contains(key))
    {
        return AuthDecision::Unauthorized("missing or invalid API key");
    } else {
        None
    };

    if let Some(roles) = api_key_roles
        && !roles.allows(required_role)
    {
        return AuthDecision::Forbidden("role is not allowed for this API key");
    }

    if !policy.allowed_tenants.allows(tenant_id) {
        return AuthDecision::Forbidden("tenant is not allowed by service policy");
    }
    if let Some(ref limiter) = policy.rate_limiter
        && let Err((current, limit)) = limiter.check(tenant_id)
    {
        eprintln!(
            "rate limit exceeded for tenant {}: {}/{} rps",
            tenant_id, current, limit
        );
        return AuthDecision::Unauthorized("rate limit exceeded");
    }
    AuthDecision::Allowed
}

fn presented_api_key(request: &HttpRequest) -> Option<&str> {
    if let Some(value) = request.headers.get("x-api-key") {
        return Some(value.as_str());
    }
    presented_bearer_token(request)
}

fn presented_bearer_token(request: &HttpRequest) -> Option<&str> {
    let value = request.headers.get("authorization")?;
    value.strip_prefix("Bearer ").map(str::trim)
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

fn parse_scoped_api_keys(raw: Option<&str>, default_role: Role) -> HashMap<String, ScopedKey> {
    let mut scoped = HashMap::new();
    let Some(raw) = raw else {
        return scoped;
    };

    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let parts: Vec<&str> = entry.splitn(3, ':').collect();
        if parts.len() < 2 {
            continue;
        }
        let key = parts[0].trim();
        if key.is_empty() {
            continue;
        }
        let tenant_scope = parse_tenant_scope(Some(parts[1].trim()), false);
        let roles = if parts.len() == 3 {
            RoleSet::from_roles(
                parts[2]
                    .split(',')
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .filter_map(Role::parse),
            )
        } else {
            RoleSet::from_roles(std::iter::once(default_role))
        };
        scoped.insert(
            key.to_string(),
            ScopedKey {
                tenant_scope,
                roles,
            },
        );
    }
    scoped
}

fn parse_api_key_set(single_key: Option<&str>, raw: Option<&str>) -> HashSet<String> {
    let mut keys = HashSet::new();
    if let Some(single_key) = single_key {
        let key = single_key.trim();
        if !key.is_empty() {
            keys.insert(key.to_string());
        }
    }
    if let Some(raw) = raw {
        for key in raw.split(',') {
            let key = key.trim();
            if key.is_empty() {
                continue;
            }
            keys.insert(key.to_string());
        }
    }
    keys
}

fn parse_jwt_validation_config(
    secret_raw: Option<String>,
    secret_set_raw: Option<String>,
    secrets_by_kid_raw: Option<String>,
    issuer_raw: Option<String>,
    audience_raw: Option<String>,
    leeway_secs_raw: Option<String>,
    require_exp_raw: Option<String>,
) -> Option<JwtValidationConfig> {
    let secret = secret_raw?.trim().to_string();
    if secret.is_empty() {
        return None;
    }
    let leeway_secs = leeway_secs_raw
        .as_deref()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let require_exp = parse_bool_env_default(require_exp_raw.as_deref(), true);
    let mut fallback_secrets = parse_secret_list(secret_set_raw.as_deref());
    fallback_secrets.retain(|value| value != &secret);
    let mut seen = HashSet::new();
    fallback_secrets.retain(|value| seen.insert(value.clone()));
    Some(JwtValidationConfig {
        hs256_secret: secret,
        hs256_fallback_secrets: fallback_secrets,
        hs256_secrets_by_kid: parse_jwt_secrets_by_kid(secrets_by_kid_raw.as_deref()),
        issuer: issuer_raw.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }),
        audience: audience_raw.and_then(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        }),
        leeway_secs,
        require_exp,
    })
}

fn parse_jwt_mode(raw: Option<&str>) -> JwtMode {
    match raw.map(str::to_ascii_lowercase).as_deref() {
        Some("oidc") => JwtMode::Oidc,
        _ => JwtMode::Hs256,
    }
}

#[allow(clippy::too_many_arguments)]
fn parse_oidc_validation_config(
    mode: JwtMode,
    jwks_url_raw: Option<String>,
    issuer_raw: Option<String>,
    audience_raw: Option<String>,
    leeway_secs_raw: Option<String>,
    require_exp_raw: Option<String>,
    jwks_refresh_minutes_raw: Option<String>,
    tenant_claims_raw: Option<String>,
) -> Option<OidcValidationConfig> {
    if mode != JwtMode::Oidc {
        return None;
    }
    let jwks_url = jwks_url_raw?;
    let jwks_url = jwks_url.trim();
    if jwks_url.is_empty() {
        return None;
    }
    let issuer = issuer_raw?;
    let issuer = issuer.trim();
    if issuer.is_empty() {
        return None;
    }
    let leeway_secs = leeway_secs_raw
        .as_deref()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(0);
    let require_exp = parse_bool_env_default(require_exp_raw.as_deref(), true);
    let jwks_refresh_minutes = jwks_refresh_minutes_raw
        .as_deref()
        .and_then(|value| value.trim().parse::<u64>().ok())
        .unwrap_or(15);
    let audience = audience_raw.and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    });
    let tenant_claims = tenant_claims_raw
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
    Some(OidcValidationConfig {
        issuer: issuer.to_string(),
        audience,
        jwks_url: jwks_url.to_string(),
        jwks_refresh_minutes,
        leeway_secs,
        require_exp,
        tenant_claims,
    })
}

fn parse_bool_env_default(raw: Option<&str>, default: bool) -> bool {
    let Some(raw) = raw else {
        return default;
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => true,
        "0" | "false" | "no" | "off" => false,
        _ => default,
    }
}

fn parse_secret_list(raw: Option<&str>) -> Vec<String> {
    let Some(raw) = raw else {
        return Vec::new();
    };
    raw.split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

use std::sync::Mutex;

#[derive(Debug)]
pub(super) struct RevocationList {
    #[allow(dead_code)]
    inner: Mutex<HashSet<String>>,
    #[allow(dead_code)]
    path: Mutex<Option<std::path::PathBuf>>,
}

#[allow(dead_code)]
impl RevocationList {
    pub(super) fn from_env(primary_var: &str, fallback_var: &str) -> Self {
        let primary = std::env::var(primary_var).ok();
        let fallback = std::env::var(fallback_var).ok();
        let path = primary.or(fallback);
        let path_mutex = Mutex::new(path.clone().map(std::path::PathBuf::from));
        let inner = Mutex::new(Self::load_from_path(path.as_deref()).unwrap_or_default());
        Self {
            inner,
            path: path_mutex,
        }
    }

    fn load_from_path(path: Option<&str>) -> Option<HashSet<String>> {
        let path = path?;
        let content = std::fs::read_to_string(path).ok()?;
        let mut set = HashSet::new();
        for line in content.lines() {
            let key = line.trim();
            if !key.is_empty() {
                set.insert(key.to_string());
            }
        }
        Some(set)
    }

    fn persist(&self) {
        let path = self.path.lock().ok().and_then(|g| g.clone());
        let Some(path) = path else { return };
        let guard = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let keys: Vec<String> = guard.iter().cloned().collect();
        drop(guard);
        let content = keys.join("\n");
        let _ = std::fs::write(&path, content);
    }

    pub(super) fn is_revoked(&self, key: &str) -> bool {
        self.inner
            .lock()
            .ok()
            .map(|g| g.contains(key))
            .unwrap_or(false)
    }

    pub(super) fn revoke(&self, key: String) {
        if self
            .inner
            .lock()
            .ok()
            .map(|mut g| g.insert(key))
            .unwrap_or(false)
        {
            self.persist();
        }
    }

    pub(super) fn revoke_many(&self, keys: &[String]) {
        let mut guard = match self.inner.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let mut changed = false;
        for key in keys {
            changed = guard.insert(key.clone()) || changed;
        }
        drop(guard);
        if changed {
            self.persist();
        }
    }
}

pub(super) struct TenantRateLimiter {
    rps: usize,
    burst: usize,
    state: Mutex<HashMap<String, (usize, std::time::Instant)>>,
}

#[allow(dead_code)]
impl TenantRateLimiter {
    pub(super) fn new(rps: usize, burst: usize) -> Self {
        Self {
            rps: rps.max(1),
            burst: burst.max(1),
            state: Mutex::new(HashMap::new()),
        }
    }

    pub(super) fn from_env(rps_var: &str, burst_var: &str) -> Option<Self> {
        let rps = std::env::var(rps_var)
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(500);
        let burst = std::env::var(burst_var)
            .ok()
            .and_then(|v| v.trim().parse().ok())
            .unwrap_or(1000);
        Some(Self::new(rps, burst))
    }

    pub(super) fn check(&self, tenant_id: &str) -> Result<usize, (usize, usize)> {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(_) => return Err((0, self.rps)),
        };
        let now = std::time::Instant::now();
        let entry = guard.entry(tenant_id.to_string()).or_insert((0, now));
        if now.duration_since(entry.1).as_secs() >= 1 {
            entry.0 = 0;
            entry.1 = now;
        }
        if entry.0 >= self.burst {
            return Err((entry.0, self.rps));
        }
        entry.0 += 1;
        Ok(entry.0)
    }

    pub(super) fn cleanup_stale(&self) {
        let mut guard = match self.state.lock() {
            Ok(g) => g,
            Err(_) => return,
        };
        let now = std::time::Instant::now();
        guard.retain(|_, (_, instant)| now.duration_since(*instant).as_secs() < 300);
    }
}

fn parse_jwt_secrets_by_kid(raw: Option<&str>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(raw) = raw else {
        return out;
    };
    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((kid_raw, secret_raw)) = entry.split_once(':') else {
            continue;
        };
        let kid = kid_raw.trim();
        let secret = secret_raw.trim();
        if kid.is_empty() || secret.is_empty() {
            continue;
        }
        out.insert(kid.to_string(), secret.to_string());
    }
    out
}
