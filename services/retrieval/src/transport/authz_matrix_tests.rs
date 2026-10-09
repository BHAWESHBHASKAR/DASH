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
    jwt_with_claims(tenant, exp, ",\"dash_roles\":[\"retrieve\",\"read_only\"]")
}

/// `extra` is spliced into the claims object (it must start with a comma or
/// be empty).
fn jwt_with_claims(tenant: &str, exp: u64, extra: &str) -> String {
    encode_hs256_token(
        &format!("{{\"tenant_id\":\"{tenant}\",\"exp\":{exp}{extra}}}"),
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
    // Tenant-less operations routes expose all-tenant topology: tenant-scoped
    // keys and non-admin JWTs are refused (see the dedicated ops test below).
    let ops = [401, 401, 403, 403, 401, 401, 403];
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
            expected: ops,
        },
        Route {
            name: "GET /debug/placement",
            method: "GET",
            target: "/debug/placement",
            body: None,
            expected: ops,
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

// ---------------------------------------------------------------------------
// Role dimension: route x credential x role
// ---------------------------------------------------------------------------

fn retrieve_request(auth: &[(&str, String)]) -> HttpRequest {
    request(
        "GET",
        "/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1",
        None,
        auth,
    )
}

fn planner_request(auth: &[(&str, String)]) -> HttpRequest {
    request(
        "GET",
        "/debug/planner?tenant_id=tenant-a&query=company+x&top_k=1",
        None,
        auth,
    )
}

/// Status of the primary route and the debug-read route for one credential.
fn role_statuses(policy: &AuthPolicy, auth: &[(&str, String)]) -> (u16, u16) {
    (
        status(policy, &retrieve_request(auth)),
        status(policy, &planner_request(auth)),
    )
}

/// roles granted -> (primary route, debug route).
/// Hierarchy: admin implies everything; read_only implies retrieve but never
/// ingest; ingest and retrieve are independent and neither implies read_only.
const ROLE_TABLE: [(&str, u16, u16); 8] = [
    ("", 403, 403),
    ("retrieve", 200, 403),
    ("ingest", 403, 403),
    ("read_only", 200, 200),
    ("admin", 200, 200),
    ("retrieve,read_only", 200, 200),
    ("ingest,retrieve", 200, 403),
    ("ingest,read_only", 200, 200),
];

fn roles_claim_json(roles: &str) -> String {
    let items: Vec<String> = roles
        .split(',')
        .filter(|r| !r.is_empty())
        .map(|r| format!("\"{r}\""))
        .collect();
    format!(",\"dash_roles\":[{}]", items.join(","))
}

#[test]
fn role_matrix_for_jwts_and_scoped_keys() {
    let _env = env_lock().lock().expect("env lock");
    let mut failures = Vec::new();
    for (roles, want_main, want_debug) in ROLE_TABLE {
        let want = (want_main, want_debug);
        // Scoped API key carrying exactly these roles.
        let key = format!(
            "role-matrix-key-{}-0123456789abcdef",
            roles.replace(',', "-")
        );
        let policy = policy_from_raw(RawAuthConfig {
            api_key_scopes: Some(format!("{key}:tenant-a:{roles}")),
            jwt_hs256_secret: Some(JWT_SECRET.to_string()),
            strict_secrets: true,
            ..Default::default()
        })
        .expect("policy");
        let got = role_statuses(&policy, &[("x-api-key", key.clone())]);
        if got != want {
            failures.push(format!(
                "scoped key [{roles}]: expected {want:?}, got {got:?}"
            ));
        }
        // JWT carrying exactly these roles.
        let token = jwt_with_claims("tenant-a", now_secs() + 300, &roles_claim_json(roles));
        let got = role_statuses(&policy, &[("authorization", format!("Bearer {token}"))]);
        if got != want {
            failures.push(format!("JWT [{roles}]: expected {want:?}, got {got:?}"));
        }
    }
    assert!(
        failures.is_empty(),
        "role matrix mismatches:\n{}",
        failures.join("\n")
    );
}

#[test]
fn jwt_without_role_claim_gets_no_roles_unless_a_default_is_configured() {
    let _env = env_lock().lock().expect("env lock");
    let token = jwt_with_claims("tenant-a", now_secs() + 300, "");
    let auth = [("authorization", format!("Bearer {token}"))];

    let strict = policy_from_raw(RawAuthConfig {
        jwt_hs256_secret: Some(JWT_SECRET.to_string()),
        strict_secrets: true,
        ..Default::default()
    })
    .expect("policy");
    assert_eq!(
        role_statuses(&strict, &auth),
        (403, 403),
        "no claim => no roles"
    );

    let with_default = policy_from_raw(RawAuthConfig {
        jwt_hs256_secret: Some(JWT_SECRET.to_string()),
        jwt_default_roles: Some("retrieve".to_string()),
        strict_secrets: true,
        ..Default::default()
    })
    .expect("policy");
    assert_eq!(role_statuses(&with_default, &auth), (200, 403));

    // A present-but-unusable claim never falls back to the default.
    let bad = jwt_with_claims("tenant-a", now_secs() + 300, ",\"dash_roles\":7");
    assert_eq!(
        role_statuses(&with_default, &[("authorization", format!("Bearer {bad}"))]),
        (403, 403)
    );
    let unknown = jwt_with_claims(
        "tenant-a",
        now_secs() + 300,
        ",\"dash_roles\":[\"superuser\"]",
    );
    assert_eq!(
        role_statuses(
            &with_default,
            &[("authorization", format!("Bearer {unknown}"))]
        ),
        (403, 403)
    );
}

#[test]
fn jwt_role_claim_accepts_arrays_strings_and_a_custom_claim_name() {
    let _env = env_lock().lock().expect("env lock");
    let policy = policy_from_raw(RawAuthConfig {
        jwt_hs256_secret: Some(JWT_SECRET.to_string()),
        jwt_role_claim: Some("roles".to_string()),
        strict_secrets: true,
        ..Default::default()
    })
    .expect("policy");
    for (label, extra, want) in [
        (
            "space delimited",
            ",\"roles\":\"retrieve read_only\"",
            (200, 200),
        ),
        (
            "comma delimited",
            ",\"roles\":\"retrieve,read_only\"",
            (200, 200),
        ),
        ("array", ",\"roles\":[\"retrieve\"]", (200, 403)),
        (
            "default claim name is ignored",
            ",\"dash_roles\":[\"admin\"]",
            (403, 403),
        ),
    ] {
        let token = jwt_with_claims("tenant-a", now_secs() + 300, extra);
        let got = role_statuses(&policy, &[("authorization", format!("Bearer {token}"))]);
        assert_eq!(got, want, "{label}");
    }
}

#[test]
fn legacy_unscoped_keys_get_an_explicit_default_role_set() {
    let _env = env_lock().lock().expect("env lock");
    let legacy = |defaults: Option<&str>| {
        policy_from_raw(RawAuthConfig {
            api_key: Some(KEY_TENANT_A.to_string()),
            api_key_default_roles: defaults.map(str::to_string),
            strict_secrets: true,
            ..Default::default()
        })
    };
    let auth = [("x-api-key", KEY_TENANT_A.to_string())];
    // Default: the service's primary role only.
    assert_eq!(role_statuses(&legacy(None).unwrap(), &auth), (200, 403));
    assert_eq!(
        role_statuses(&legacy(Some("retrieve,read_only")).unwrap(), &auth),
        (200, 200)
    );
    assert_eq!(
        role_statuses(&legacy(Some("admin")).unwrap(), &auth),
        (200, 200)
    );
    assert_eq!(
        role_statuses(&legacy(Some("ingest")).unwrap(), &auth),
        (403, 403)
    );
    assert!(
        legacy(Some("not-a-role")).is_err(),
        "unknown role is a startup error"
    );
}

#[test]
fn ops_routes_need_admin_or_an_unscoped_credential() {
    let _env = env_lock().lock().expect("env lock");
    const WILD: &str = "key-for-all-tenants-2c7e91d0a4b83f65";
    const ADMIN: &str = "admin-key-for-tenant-a-7d41e8a0c9b2f365";
    let policy = policy_from_raw(RawAuthConfig {
        api_key_scopes: Some(format!(
            "{KEY_TENANT_A}:tenant-a:retrieve,read_only;{WILD}:*:read_only;{ADMIN}:tenant-a:admin"
        )),
        strict_secrets: true,
        ..Default::default()
    })
    .expect("policy");
    for target in ["/metrics", "/debug/placement"] {
        let get = |auth: &[(&str, String)]| status(&policy, &request("GET", target, None, auth));
        assert_eq!(get(&[]), 401, "{target} anonymous");
        assert_eq!(
            get(&[("x-api-key", KEY_TENANT_A.into())]),
            403,
            "{target} tenant-scoped read_only key"
        );
        assert_eq!(
            get(&[("x-api-key", WILD.into())]),
            200,
            "{target} wildcard key"
        );
        assert_eq!(
            get(&[("x-api-key", ADMIN.into())]),
            200,
            "{target} admin key"
        );
    }
}

/// Sets an environment variable for the duration of a test and restores the
/// previous value on drop (also when the test panics).
struct ScopedEnv {
    key: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl ScopedEnv {
    #[allow(unused_unsafe)]
    fn set(key: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(key);
        unsafe { std::env::set_var(key, value) };
        Self { key, previous }
    }
}

impl Drop for ScopedEnv {
    #[allow(unused_unsafe)]
    fn drop(&mut self) {
        match self.previous.take() {
            Some(value) => unsafe { std::env::set_var(self.key, value) },
            None => unsafe { std::env::remove_var(self.key) },
        }
    }
}

/// Review finding 1: oversized identifiers must be refused with 400 before
/// authentication so that denial audit records stay small.
#[test]
fn unauthenticated_oversized_tenant_id_is_rejected_before_any_audit_record() {
    let _env = env_lock().lock().unwrap_or_else(|p| p.into_inner());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let path = std::env::temp_dir()
        .join(format!(
            "dash-retrieve-review-{}-{nanos}.jsonl",
            std::process::id()
        ))
        .to_string_lossy()
        .to_string();
    let _audit = ScopedEnv::set("DASH_RETRIEVAL_AUDIT_LOG_PATH", &path);
    let policy = matrix_policy();
    let huge = "T".repeat(1024 * 1024);
    let post_body = format!("{{\"tenant_id\":\"{huge}\",\"query\":\"company x\",\"top_k\":1}}");
    let get_target = format!("/v1/retrieve?tenant_id={huge}&query=company+x&top_k=1");
    let planner_target = format!("/debug/planner?tenant_id={huge}&query=company+x&top_k=1");
    for (method, target, body) in [
        ("POST", "/v1/retrieve", Some(post_body.as_str())),
        ("GET", get_target.as_str(), None),
        ("GET", planner_target.as_str(), None),
    ] {
        let got = status(&policy, &request(method, target, body, &[]));
        assert_eq!(got, 400, "{method} {}", &target[..target.len().min(40)]);
    }
    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    assert_eq!(size, 0, "oversized tenant_id reached the audit log");

    // At the limit the normal 401 + (bounded) audit record still happens.
    let at_limit = "t".repeat(256);
    let body = format!("{{\"tenant_id\":\"{at_limit}\",\"query\":\"company x\",\"top_k\":1}}");
    assert_eq!(
        status(&policy, &request("POST", "/v1/retrieve", Some(&body), &[])),
        401
    );
    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    assert!(size > 0 && size < 2048, "audit record is {size} bytes");
    let _ = std::fs::remove_file(&path);
}
