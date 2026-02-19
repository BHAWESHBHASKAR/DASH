use std::{
    collections::{HashMap, HashSet},
    time::{SystemTime, UNIX_EPOCH},
};

use auth::{JwtValidationConfig, JwtValidationError, verify_hs256_token_for_tenant};

use super::{HttpRequest, env_with_fallback};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum AuthDecision {
    Allowed,
    Unauthorized(&'static str),
    Forbidden(&'static str),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct AuthPolicy {
    required_api_keys: HashSet<String>,
    revoked_api_keys: HashSet<String>,
    allowed_tenants: TenantScope,
    scoped_api_keys: HashMap<String, TenantScope>,
    jwt_validation: Option<JwtValidationConfig>,
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
            scoped_api_keys: parse_scoped_api_keys(scoped_api_keys_raw.as_deref()),
            jwt_validation: parse_jwt_validation_config(
                env_with_fallback(
                    "DASH_INGEST_JWT_HS256_SECRET",
                    "EME_INGEST_JWT_HS256_SECRET",
                ),
                env_with_fallback(
                    "DASH_INGEST_JWT_HS256_SECRETS",
                    "EME_INGEST_JWT_HS256_SECRETS",
                ),
                env_with_fallback(
                    "DASH_INGEST_JWT_HS256_SECRETS_BY_KID",
                    "EME_INGEST_JWT_HS256_SECRETS_BY_KID",
                ),
                env_with_fallback("DASH_INGEST_JWT_ISSUER", "EME_INGEST_JWT_ISSUER"),
                env_with_fallback("DASH_INGEST_JWT_AUDIENCE", "EME_INGEST_JWT_AUDIENCE"),
                env_with_fallback("DASH_INGEST_JWT_LEEWAY_SECS", "EME_INGEST_JWT_LEEWAY_SECS"),
                env_with_fallback("DASH_INGEST_JWT_REQUIRE_EXP", "EME_INGEST_JWT_REQUIRE_EXP"),
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

pub(super) fn authorize_request_for_tenant(
    request: &HttpRequest,
    tenant_id: &str,
    policy: &AuthPolicy,
) -> AuthDecision {
    if let Some(jwt_config) = policy.jwt_validation.as_ref()
        && let Some(token) = presented_bearer_token(request)
        && bearer_looks_like_jwt(token)
    {
        return match verify_hs256_token_for_tenant(token, tenant_id, jwt_config, unix_now_secs()) {
            Ok(()) => {
                if !policy.allowed_tenants.allows(tenant_id) {
                    AuthDecision::Forbidden("tenant is not allowed by service policy")
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
    if let Some(api_key) = maybe_api_key
        && policy.revoked_api_keys.contains(api_key)
    {
        return AuthDecision::Unauthorized("API key revoked");
    }

    if !policy.scoped_api_keys.is_empty() {
        let Some(api_key) = maybe_api_key else {
            return AuthDecision::Unauthorized("missing or invalid API key");
        };
        if let Some(scope) = policy.scoped_api_keys.get(api_key) {
            if !scope.allows(tenant_id) {
                return AuthDecision::Forbidden("tenant is not allowed for this API key");
            }
        } else if !policy.required_api_keys.is_empty()
            && !policy.required_api_keys.contains(api_key)
        {
            return AuthDecision::Unauthorized("missing or invalid API key");
        }
    } else if !policy.required_api_keys.is_empty()
        && !matches!(maybe_api_key, Some(key) if policy.required_api_keys.contains(key))
    {
        return AuthDecision::Unauthorized("missing or invalid API key");
    }

    if !policy.allowed_tenants.allows(tenant_id) {
        return AuthDecision::Forbidden("tenant is not allowed by service policy");
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

fn parse_scoped_api_keys(raw: Option<&str>) -> HashMap<String, TenantScope> {
    let mut scoped = HashMap::new();
    let Some(raw) = raw else {
        return scoped;
    };

    for entry in raw.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let Some((raw_key, raw_scope)) = entry.split_once(':') else {
            continue;
        };
        let key = raw_key.trim();
        if key.is_empty() {
            continue;
        }
        scoped.insert(
            key.to_string(),
            parse_tenant_scope(Some(raw_scope.trim()), false),
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
