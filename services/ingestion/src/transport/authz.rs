//! Ingestion-service adapter over the shared deny-by-default policy in
//! `dash-common`. The policy is built once at startup
//! ([`initialize_auth_policy`]) and shared behind an `Arc`.

use std::sync::Arc;

pub use auth::Role;
pub(crate) use dash_common::AuthDecision;
use dash_common::{AuthPolicy, PolicyCell, ServiceAuthEnv};

use super::HttpRequest;

const SERVICE_AUTH: ServiceAuthEnv = ServiceAuthEnv {
    service: "ingestion",
    prefix: "INGEST",
    default_scoped_role: Role::Ingest,
    default_rate_limit_rps: 100,
    default_rate_limit_burst: 200,
};

static POLICY: PolicyCell = PolicyCell::new();

/// Build, validate and pin the authentication policy from the environment
/// (`DASH_INGEST_*`, falling back to `EME_INGEST_*`). Call once at process
/// start; an error means the service must not start.
pub fn initialize_auth_policy() -> Result<(), String> {
    POLICY.pin(&SERVICE_AUTH)?;
    validate_replication_config()?;
    // On unix the policy is rebuilt on SIGHUP (see `PolicyCell::reload`).
    dash_common::spawn_sighup_reload(&POLICY, SERVICE_AUTH);
    Ok(())
}

/// Replication needs a strong shared token. A follower (source URL set)
/// without one cannot start outside dev mode; a leader without one keeps its
/// replication endpoints closed (they answer 403) and says so at startup.
fn validate_replication_config() -> Result<(), String> {
    let token = dash_common::env_with_fallback(
        "DASH_INGEST_REPLICATION_TOKEN",
        "EME_INGEST_REPLICATION_TOKEN",
    )
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty());
    let follower = dash_common::env_with_fallback(
        "DASH_INGEST_REPLICATION_SOURCE_URL",
        "EME_INGEST_REPLICATION_SOURCE_URL",
    )
    .is_some_and(|value| !value.trim().is_empty());
    let dev = dash_common::insecure_dev_mode_enabled();
    match token {
        Some(token) => {
            if dash_common::strict_secrets_enabled() {
                dash_common::validate_secret(&token, "DASH_INGEST_REPLICATION_TOKEN")?;
            }
        }
        None if follower && !dev => {
            return Err(
                "replication is enabled (DASH_INGEST_REPLICATION_SOURCE_URL) but \
                 DASH_INGEST_REPLICATION_TOKEN is not set; set a token or, for local \
                 development only, DASH_INSECURE_DEV_MODE=1"
                    .to_string(),
            );
        }
        None if dev => tracing::warn!(
            "DASH_INGEST_REPLICATION_TOKEN is not set: replication endpoints are open \
             because DASH_INSECURE_DEV_MODE=1"
        ),
        None => tracing::warn!(
            "DASH_INGEST_REPLICATION_TOKEN is not set: /internal/replication/* endpoints \
             will reject every request"
        ),
    }
    Ok(())
}

/// The policy used by request handling: the pinned startup policy, or (when
/// the handler is driven directly without a startup step) one built from the
/// environment and cached until that environment changes.
pub(crate) fn shared_auth_policy() -> Arc<AuthPolicy> {
    POLICY.current(&SERVICE_AUTH)
}

pub(crate) fn authorize_request_for_tenant(
    request: &HttpRequest,
    tenant_id: &str,
    policy: &AuthPolicy,
    required_role: Role,
) -> AuthDecision {
    policy.authorize_for_tenant(&request.headers, tenant_id, required_role)
}

/// Authorize a request that is not scoped to a tenant (`/metrics`, debug).
pub(crate) fn authorize_request_any_tenant(
    request: &HttpRequest,
    policy: &AuthPolicy,
    required_role: Role,
) -> AuthDecision {
    policy.authorize_any_tenant(&request.headers, required_role)
}

/// Build a policy from explicit values (no environment, no strict secret
/// checks) for unit tests.
#[cfg(test)]
pub(crate) fn policy_from_parts(
    api_key: Option<String>,
    api_keys: Option<String>,
    revoked_api_keys: Option<String>,
    allowed_tenants: Option<String>,
    api_key_scopes: Option<String>,
) -> AuthPolicy {
    policy_from_raw(dash_common::RawAuthConfig {
        api_key,
        api_keys,
        revoked_api_keys,
        allowed_tenants,
        api_key_scopes,
        ..Default::default()
    })
    .expect("test auth policy should build")
}

#[cfg(test)]
pub(crate) fn policy_from_raw(raw: dash_common::RawAuthConfig) -> Result<AuthPolicy, String> {
    AuthPolicy::build(raw, &SERVICE_AUTH)
}
