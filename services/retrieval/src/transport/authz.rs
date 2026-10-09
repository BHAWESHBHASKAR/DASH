//! Retrieval-service adapter over the shared deny-by-default policy in
//! `dash-common`. The policy is built once at startup
//! ([`initialize_auth_policy`]) and shared behind an `Arc`.

use std::sync::Arc;

pub use auth::Role;
pub(crate) use dash_common::AuthDecision;
use dash_common::{AuthPolicy, PolicyCell, ServiceAuthEnv};

use super::HttpRequest;

const SERVICE_AUTH: ServiceAuthEnv = ServiceAuthEnv {
    service: "retrieval",
    prefix: "RETRIEVAL",
    default_scoped_role: Role::Retrieve,
    default_rate_limit_rps: 500,
    default_rate_limit_burst: 1000,
};

static POLICY: PolicyCell = PolicyCell::new();

/// Build, validate and pin the authentication policy from the environment.
/// Call once at process start; an error means the service must not start
/// (no auth configured without `DASH_INSECURE_DEV_MODE=1`, placeholder
/// secrets, incomplete OIDC configuration, ...).
pub fn initialize_auth_policy() -> Result<(), String> {
    POLICY.pin(&SERVICE_AUTH).map(|_| ())
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

/// Authorize a request that is not scoped to a tenant (`/metrics`,
/// `/debug/placement`, `/v1/embeddings`).
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
