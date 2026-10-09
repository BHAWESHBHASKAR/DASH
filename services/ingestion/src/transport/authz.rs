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
    dash_common::tls::check_listener_tls(&dash_common::tls::INGEST_TLS_ENV)?;
    validate_replication_config()?;
    dash_common::audit::warn_if_fail_open("INGEST");
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
    if follower {
        let source = dash_common::env_with_fallback(
            "DASH_INGEST_REPLICATION_SOURCE_URL",
            "EME_INGEST_REPLICATION_SOURCE_URL",
        )
        .unwrap_or_default();
        check_follower_token_transport(
            &source,
            token.is_some(),
            dash_common::replication_client::insecure_http_allowed_from_env(),
        )?;
        dash_common::replication_client::ClientOptions::from_env().validate()?;
    }
    validate_replication_client_cert_policy(
        &super::replication::ReplicationClientCertPolicy::from_env(),
        dash_common::tls::RawListenerTls::from_env(&dash_common::tls::INGEST_TLS_ENV)
            .client_ca_file
            .is_some(),
    )?;
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

/// Requiring client certificates on the replication routes only works when
/// the listener verifies them.
fn validate_replication_client_cert_policy(
    policy: &super::replication::ReplicationClientCertPolicy,
    listener_verifies_client_certs: bool,
) -> Result<(), String> {
    if let Some(err) = &policy.invalid {
        return Err(err.clone());
    }
    if policy.required() && !listener_verifies_client_certs {
        return Err(
            "DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT / DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS \
             need TLS with client certificate verification on the ingestion listener \
             (DASH_INGEST_TLS_CERT_FILE, DASH_INGEST_TLS_KEY_FILE, DASH_INGEST_TLS_CLIENT_CA_FILE)"
                .to_string(),
        );
    }
    Ok(())
}

/// A follower must not send the replication token over plain http to a
/// non-loopback host unless the operator acknowledged it.
fn check_follower_token_transport(
    source_url: &str,
    token_present: bool,
    allow_insecure_http: bool,
) -> Result<(), String> {
    let url = dash_common::replication_client::parse_source_url(source_url)?;
    dash_common::replication_client::check_token_transport(&url, token_present, allow_insecure_http)
}

/// Startup warnings about how replication traffic crosses the network.
/// `source_url` is the follower source (when this node follows another one),
/// `bind_addr` the listener address.
pub(crate) fn replication_transport_warnings(
    token_set: bool,
    source_url: Option<&str>,
    bind_addr: &str,
    listener_tls: bool,
) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(source) = source_url
        && let Some(message) = dash_common::replication_client::plaintext_source_warning(source)
    {
        out.push(message);
    }
    if let Some(message) =
        dash_common::replication_client::leader_exposure_warning(bind_addr, token_set, listener_tls)
    {
        out.push(message);
    }
    out
}

/// Log [`replication_transport_warnings`] for this process. Call once at
/// startup with the resolved listener address.
pub fn warn_replication_transport(bind_addr: &str) {
    let token_set = dash_common::env_with_fallback(
        "DASH_INGEST_REPLICATION_TOKEN",
        "EME_INGEST_REPLICATION_TOKEN",
    )
    .is_some_and(|value| !value.trim().is_empty());
    let source = dash_common::env_with_fallback(
        "DASH_INGEST_REPLICATION_SOURCE_URL",
        "EME_INGEST_REPLICATION_SOURCE_URL",
    )
    .filter(|value| !value.trim().is_empty());
    let listener_tls = dash_common::tls::listener_tls_enabled(&dash_common::tls::INGEST_TLS_ENV);
    for message in
        replication_transport_warnings(token_set, source.as_deref(), bind_addr, listener_tls)
    {
        tracing::warn!("{message}");
    }
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

/// Authorize a tenant-less operations request (`/metrics`, debug). Requires
/// the admin role or an unscoped credential, see `AuthPolicy::authorize_ops`.
pub(crate) fn authorize_request_ops(
    request: &HttpRequest,
    policy: &AuthPolicy,
    required_role: Role,
) -> AuthDecision {
    policy.authorize_ops(&request.headers, required_role)
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

#[cfg(test)]
mod replication_transport_tests {
    use super::*;

    #[test]
    fn follower_with_a_token_over_remote_plain_http_is_refused_unless_acknowledged() {
        let err =
            check_follower_token_transport("http://leader.internal:8081", true, false).unwrap_err();
        assert!(
            err.contains("DASH_REPLICATION_ALLOW_INSECURE_HTTP"),
            "{err}"
        );
        assert!(check_follower_token_transport("http://leader.internal:8081", true, true).is_ok());
        assert!(
            check_follower_token_transport("https://leader.internal:8443", true, false).is_ok()
        );
        assert!(check_follower_token_transport("http://127.0.0.1:8081", true, false).is_ok());
    }

    #[test]
    fn startup_warns_for_remote_plaintext_source_and_exposed_leader() {
        // Follower side.
        let warnings = replication_transport_warnings(
            true,
            Some("http://leader.internal:8081"),
            "127.0.0.1:8081",
            false,
        );
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("plain http://"), "{warnings:?}");
        assert!(warnings[0].contains("replication-security.md"));
        // Leader side.
        let warnings = replication_transport_warnings(true, None, "0.0.0.0:8081", false);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(warnings[0].contains("not loopback"), "{warnings:?}");
        assert!(
            warnings[0].contains("DASH_INGEST_TLS_CERT_FILE"),
            "{warnings:?}"
        );
        // Quiet cases.
        assert!(replication_transport_warnings(true, None, "127.0.0.1:8081", false).is_empty());
        assert!(replication_transport_warnings(false, None, "0.0.0.0:8081", false).is_empty());
        // A leader serving TLS itself is not exposed.
        assert!(replication_transport_warnings(true, None, "0.0.0.0:8081", true).is_empty());
        assert!(
            replication_transport_warnings(
                true,
                Some("https://leader.internal:8443"),
                "127.0.0.1:8081",
                false,
            )
            .is_empty()
        );
    }

    #[test]
    fn replication_client_cert_policy_needs_a_verifying_listener() {
        use super::super::replication::ReplicationClientCertPolicy;
        let off = ReplicationClientCertPolicy::from_values(None, None);
        assert!(validate_replication_client_cert_policy(&off, false).is_ok());
        let required = ReplicationClientCertPolicy::from_values(Some("1"), None);
        let err = validate_replication_client_cert_policy(&required, false).unwrap_err();
        assert!(err.contains("DASH_INGEST_TLS_CLIENT_CA_FILE"), "{err}");
        assert!(validate_replication_client_cert_policy(&required, true).is_ok());
        let bad = ReplicationClientCertPolicy::from_values(None, Some("not-a-fingerprint"));
        let err = validate_replication_client_cert_policy(&bad, true).unwrap_err();
        assert!(
            err.contains("DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS"),
            "{err}"
        );
    }
}
