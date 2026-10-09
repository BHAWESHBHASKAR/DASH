//! Authorization decision matrix for the retrieval HTTP handler: every route
//! crossed with every credential type, asserting the exact status.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use auth::encode_hs256_token;
use dash_common::{AuthPolicy, RawAuthConfig};
use store::InMemoryStore;

use super::{
    HttpRequest, TransportMetrics, authz::policy_from_raw, handle_request_with_policy,
    render_response_text, tests::env_lock,
};

const JWT_SECRET: &str = "matrix-hs256-signing-key-9f2b7c41d8e0a356";
const KEY_TENANT_A: &str = "key-for-tenant-a-8f3a91c2d7e4b605";
const KEY_TENANT_B: &str = "key-for-tenant-b-51d0c9a7e2f8b436";

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_secs()
}

fn jwt(tenant: &str, exp: u64) -> String {
    encode_hs256_token(
        &format!("{{\"tenant_id\":\"{tenant}\",\"exp\":{exp}}}"),
        JWT_SECRET,
    )
    .expect("token should encode")
}

fn matrix_policy() -> AuthPolicy {
    policy_from_raw(RawAuthConfig {
        api_key_scopes: Some(format!(
            "{KEY_TENANT_A}:tenant-a:retrieve,read_only;{KEY_TENANT_B}:tenant-b:retrieve,read_only"
        )),
        jwt_hs256_secret: Some(JWT_SECRET.to_string()),
        strict_secrets: true,
        ..Default::default()
    })
    .expect("matrix policy should build")
}

fn request(method: &str, target: &str, body: Option<&str>, auth: &[(&str, String)]) -> HttpRequest {
    let mut headers: HashMap<String, String> = auth
        .iter()
        .map(|(name, value)| (name.to_string(), value.clone()))
        .collect();
    if body.is_some() {
        headers.insert("content-type".to_string(), "application/json".to_string());
    }
    HttpRequest {
        method: method.to_string(),
        target: target.to_string(),
        headers,
        body: body.unwrap_or_default().as_bytes().to_vec(),
    }
}

fn status(policy: &AuthPolicy, req: &HttpRequest) -> u16 {
    handle(policy, req).status
}

fn handle(policy: &AuthPolicy, req: &HttpRequest) -> super::HttpResponse {
    let store = InMemoryStore::new();
    let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
    handle_request_with_policy(&store, req, &metrics, None, None, policy)
}

struct Route {
    name: &'static str,
    method: &'static str,
    target: &'static str,
    body: Option<&'static str>,
    /// Statuses for: none, bad key, key for another tenant, key for this
    /// tenant, expired JWT, valid JWT in the wrong header, valid JWT.
    expected: [u16; 7],
}

const RETRIEVE_BODY: &str =
    "{\"tenant_id\":\"tenant-a\",\"query\":\"company x\",\"top_k\":1,\"return_graph\":false}";

#[test]
fn authz_decision_matrix_covers_every_route_and_credential_type() {
    let _env = env_lock().lock().expect("env lock");
    let policy = matrix_policy();

    let valid_jwt = jwt("tenant-a", now_secs() + 300);
    let expired_jwt = jwt("tenant-a", now_secs().saturating_sub(60));
    let credentials: [(&str, Vec<(&str, String)>); 7] = [
        ("none", vec![]),
        (
            "bad key",
            vec![("x-api-key", "not-a-real-key-0123456789".into())],
        ),
        (
            "key for another tenant",
            vec![("x-api-key", KEY_TENANT_B.into())],
        ),
        (
            "key for this tenant",
            vec![("x-api-key", KEY_TENANT_A.into())],
        ),
        (
            "expired JWT",
            vec![("authorization", format!("Bearer {expired_jwt}"))],
        ),
        (
            "valid JWT in the wrong header",
            vec![("x-api-key", valid_jwt.clone())],
        ),
        (
            "valid JWT",
            vec![("authorization", format!("Bearer {valid_jwt}"))],
        ),
    ];

    let tenant_scoped = [401, 401, 403, 200, 401, 401, 200];
    // Not tenant-scoped: any authenticated principal with the role may call.
    let any_tenant = [401, 401, 200, 200, 401, 401, 200];
    let open = [200; 7];

    let routes = [
        Route {
            name: "GET /v1/retrieve",
            method: "GET",
            target: "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1",
            body: None,
            expected: tenant_scoped,
        },
        Route {
            name: "POST /v1/retrieve",
            method: "POST",
            target: "/v1/retrieve",
            body: Some(RETRIEVE_BODY),
            expected: tenant_scoped,
        },
        Route {
            name: "GET /debug/planner",
            method: "GET",
            target: "/debug/planner?tenant_id=tenant-a&query=company+x&top_k=1",
            body: None,
            expected: tenant_scoped,
        },
        Route {
            name: "POST /v1/embeddings",
            method: "POST",
            target: "/v1/embeddings",
            body: Some("{\"input\":\"hello\",\"model\":\"text-embedding-3-small\"}"),
            expected: any_tenant,
        },
        Route {
            name: "GET /metrics",
            method: "GET",
            target: "/metrics",
            body: None,
            expected: any_tenant,
        },
        Route {
            name: "GET /debug/placement",
            method: "GET",
            target: "/debug/placement",
            body: None,
            expected: any_tenant,
        },
        Route {
            name: "GET /health",
            method: "GET",
            target: "/health",
            body: None,
            expected: open,
        },
        Route {
            name: "GET /live",
            method: "GET",
            target: "/live",
            body: None,
            expected: open,
        },
        Route {
            name: "GET /ready",
            method: "GET",
            target: "/ready",
            body: None,
            expected: open,
        },
    ];

    let mut failures = Vec::new();
    for route in &routes {
        for (index, (cred_name, headers)) in credentials.iter().enumerate() {
            let req = request(route.method, route.target, route.body, headers);
            let got = status(&policy, &req);
            if got != route.expected[index] {
                failures.push(format!(
                    "{} with {cred_name}: expected {}, got {got}",
                    route.name, route.expected[index]
                ));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "authz matrix mismatches:\n{}",
        failures.join("\n")
    );
}

#[test]
fn jwt_only_policy_does_not_fall_through_to_an_open_api_key_branch() {
    let _env = env_lock().lock().expect("env lock");
    let policy = policy_from_raw(RawAuthConfig {
        jwt_hs256_secret: Some(JWT_SECRET.to_string()),
        strict_secrets: true,
        ..Default::default()
    })
    .expect("jwt-only policy should build");
    let target = "/v1/retrieve?tenant_id=tenant-other&query=company+x&top_k=1";
    for (label, auth) in [
        ("no Authorization header", vec![]),
        (
            "non-JWT bearer",
            vec![("authorization", "Bearer plainly-not-a-jwt".to_string())],
        ),
        (
            "x-api-key only",
            vec![("x-api-key", "anything-at-all".to_string())],
        ),
    ] {
        let response = handle(&policy, &request("GET", target, None, &auth));
        assert_eq!(response.status, 401, "{label} must be rejected");
    }
}

#[test]
fn unconfigured_service_denies_everything_except_probes() {
    let _env = env_lock().lock().expect("env lock");
    // `build` refuses to produce an open policy outside dev mode.
    assert!(policy_from_raw(RawAuthConfig::default()).is_err());
    // A handler that nevertheless ends up with no usable policy fails closed.
    let policy = AuthPolicy::deny_all("test".to_string());
    for target in [
        "/v1/retrieve?tenant_id=tenant-a&query=company+x",
        "/metrics",
        "/debug/placement",
    ] {
        assert_eq!(
            status(&policy, &request("GET", target, None, &[])),
            401,
            "{target}"
        );
    }
    assert_eq!(status(&policy, &request("GET", "/live", None, &[])), 200);
}

#[test]
fn explicit_dev_mode_allows_unauthenticated_requests() {
    let _env = env_lock().lock().expect("env lock");
    let policy = policy_from_raw(RawAuthConfig {
        insecure_dev: true,
        ..Default::default()
    })
    .expect("dev mode policy should build");
    assert_eq!(
        status(
            &policy,
            &request("GET", "/v1/retrieve?tenant_id=tenant-a&query=x", None, &[])
        ),
        200
    );
}

#[test]
fn metrics_can_only_be_made_public_with_the_explicit_flag() {
    let _env = env_lock().lock().expect("env lock");
    let policy = policy_from_raw(RawAuthConfig {
        api_key: Some(KEY_TENANT_A.to_string()),
        metrics_public: true,
        ..Default::default()
    })
    .expect("policy should build");
    assert_eq!(status(&policy, &request("GET", "/metrics", None, &[])), 200);
    // The exemption is for /metrics only.
    assert_eq!(
        status(&policy, &request("GET", "/debug/placement", None, &[])),
        401
    );
}

#[test]
fn rate_limit_returns_429_with_retry_after_for_api_keys_and_jwts() {
    let _env = env_lock().lock().expect("env lock");
    let policy = policy_from_raw(RawAuthConfig {
        api_key: Some(KEY_TENANT_A.to_string()),
        jwt_hs256_secret: Some(JWT_SECRET.to_string()),
        rate_limit_rps: Some("1".to_string()),
        rate_limit_burst: Some("2".to_string()),
        ..Default::default()
    })
    .expect("policy should build");
    let target = "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1";

    let key_headers = [("x-api-key", KEY_TENANT_A.to_string())];
    assert_eq!(
        status(&policy, &request("GET", target, None, &key_headers)),
        200
    );
    assert_eq!(
        status(&policy, &request("GET", target, None, &key_headers)),
        200
    );
    let limited = handle(&policy, &request("GET", target, None, &key_headers));
    assert_eq!(limited.status, 429);
    let wire = render_response_text(&limited);
    assert!(wire.starts_with("HTTP/1.1 429 Too Many Requests"), "{wire}");
    assert!(wire.contains("Retry-After: "), "{wire}");

    // The limiter is per tenant and also covers JWT-authenticated requests.
    let token = jwt("tenant-j", now_secs() + 300);
    let jwt_headers = [("authorization", format!("Bearer {token}"))];
    let jwt_target = "/v1/retrieve?tenant_id=tenant-j&query=company+x&top_k=1";
    assert_eq!(
        status(&policy, &request("GET", jwt_target, None, &jwt_headers)),
        200
    );
    assert_eq!(
        status(&policy, &request("GET", jwt_target, None, &jwt_headers)),
        200
    );
    assert_eq!(
        status(&policy, &request("GET", jwt_target, None, &jwt_headers)),
        429
    );
}
