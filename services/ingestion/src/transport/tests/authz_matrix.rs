//! Authorization decision matrix for the ingestion HTTP handler: every route
//! crossed with every credential type, asserting the exact status.

use std::time::{SystemTime, UNIX_EPOCH};

use auth::encode_hs256_token;
use dash_common::{AuthPolicy, RawAuthConfig};

use super::super::authz::policy_from_raw;
use super::super::routes::handle_request_with_policy;
use super::super::*;
use super::{env_lock, sample_runtime};

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
    jwt_with_claims(tenant, exp, ",\"dash_roles\":[\"ingest\",\"read_only\"]")
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
            "{KEY_TENANT_A}:tenant-a:ingest,read_only;{KEY_TENANT_B}:tenant-b:ingest,read_only"
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

fn handle(policy: &AuthPolicy, req: &HttpRequest) -> HttpResponse {
    handle_request_with_policy(&sample_runtime(), req, policy)
}

fn status(policy: &AuthPolicy, req: &HttpRequest) -> u16 {
    handle(policy, req).status
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

const INGEST_BODY: &str = "{\"claim\":{\"claim_id\":\"c-matrix\",\"tenant_id\":\"tenant-a\",\"canonical_text\":\"Company X acquired Company Y\",\"confidence\":0.9}}";
const RAW_BODY: &str = "{\"tenant_id\":\"tenant-a\",\"document_id\":\"doc-matrix\",\"source_id\":\"source://doc-matrix\",\"text\":\"Company X acquired Company Y in 2024. Revenue rose in Q4.\",\"min_sentence_chars\":10,\"max_claims\":4}";
const BATCH_BODY: &str = "{\"items\":[{\"claim\":{\"claim_id\":\"c-matrix-b\",\"tenant_id\":\"tenant-a\",\"canonical_text\":\"Batch item\",\"confidence\":0.9}}]}";

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
    // Tenant-less operations routes expose all-tenant topology: tenant-scoped
    // keys and non-admin JWTs are refused (see the dedicated ops test below).
    let ops = [401, 401, 403, 403, 401, 401, 403];
    let open = [200; 7];

    let routes = [
        Route {
            name: "POST /v1/ingest",
            method: "POST",
            target: "/v1/ingest",
            body: Some(INGEST_BODY),
            expected: tenant_scoped,
        },
        Route {
            name: "POST /v1/ingest/raw",
            method: "POST",
            target: "/v1/ingest/raw",
            body: Some(RAW_BODY),
            expected: tenant_scoped,
        },
        Route {
            name: "POST /v1/ingest/batch",
            method: "POST",
            target: "/v1/ingest/batch",
            body: Some(BATCH_BODY),
            expected: tenant_scoped,
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
            name: "GET /debug/document-parser",
            method: "GET",
            target: "/debug/document-parser",
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
        let response = handle(
            &policy,
            &request("POST", "/v1/ingest", Some(INGEST_BODY), &auth),
        );
        assert_eq!(response.status, 401, "{label} must be rejected");
    }
}

#[test]
fn unconfigured_service_denies_everything_except_probes() {
    let _env = env_lock().lock().expect("env lock");
    assert!(policy_from_raw(RawAuthConfig::default()).is_err());
    let policy = AuthPolicy::deny_all("test".to_string());
    assert_eq!(
        status(
            &policy,
            &request("POST", "/v1/ingest", Some(INGEST_BODY), &[])
        ),
        401
    );
    assert_eq!(status(&policy, &request("GET", "/metrics", None, &[])), 401);
    assert_eq!(status(&policy, &request("GET", "/live", None, &[])), 200);
}

#[test]
fn explicit_dev_mode_allows_unauthenticated_ingest() {
    let _env = env_lock().lock().expect("env lock");
    let policy = policy_from_raw(RawAuthConfig {
        insecure_dev: true,
        ..Default::default()
    })
    .expect("dev mode policy should build");
    assert_eq!(
        status(
            &policy,
            &request("POST", "/v1/ingest", Some(INGEST_BODY), &[])
        ),
        200
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
    let runtime = sample_runtime();
    let counter = std::cell::Cell::new(0u32);
    // Distinct claim ids so only the rate limiter decides the outcome.
    let send = |tenant: &str, auth: &[(&str, String)]| {
        counter.set(counter.get() + 1);
        let body = INGEST_BODY
            .replace("tenant-a", tenant)
            .replace("c-matrix", &format!("c-rate-{}", counter.get()));
        handle_request_with_policy(
            &runtime,
            &request("POST", "/v1/ingest", Some(&body), auth),
            &policy,
        )
    };

    let key_headers = [("x-api-key", KEY_TENANT_A.to_string())];
    assert_eq!(send("tenant-a", &key_headers).status, 200);
    assert_eq!(send("tenant-a", &key_headers).status, 200);
    let limited = send("tenant-a", &key_headers);
    assert_eq!(limited.status, 429);
    let wire = render_response_text(&limited);
    assert!(wire.starts_with("HTTP/1.1 429 Too Many Requests"), "{wire}");
    assert!(wire.contains("Retry-After: "), "{wire}");

    // JWT-authenticated requests are limited too (separate tenant bucket).
    let token = jwt("tenant-j", now_secs() + 300);
    let jwt_headers = [("authorization", format!("Bearer {token}"))];
    assert_eq!(send("tenant-j", &jwt_headers).status, 200);
    assert_eq!(send("tenant-j", &jwt_headers).status, 200);
    assert_eq!(send("tenant-j", &jwt_headers).status, 429);
}

// ---------------------------------------------------------------------------
// Role dimension: route x credential x role
// ---------------------------------------------------------------------------

fn ingest_request(auth: &[(&str, String)]) -> HttpRequest {
    // A fresh claim id per call keeps repeated writes from colliding.
    static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    let n = NEXT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    let body = INGEST_BODY.replace("c-matrix", &format!("c-role-{n}"));
    request("POST", "/v1/ingest", Some(&body), auth)
}

fn placement_request(auth: &[(&str, String)]) -> HttpRequest {
    request("GET", "/debug/placement", None, auth)
}

/// Status of the primary route and the debug-read route for one credential.
fn role_statuses(policy: &AuthPolicy, auth: &[(&str, String)]) -> (u16, u16) {
    (
        status(policy, &ingest_request(auth)),
        status(policy, &placement_request(auth)),
    )
}

/// roles granted -> (primary route, debug route).
/// Hierarchy: admin implies everything; read_only implies retrieve but never
/// ingest; ingest and retrieve are independent and neither implies read_only.
/// The debug route is a tenant-less operations route: a (tenant-bound) JWT
/// needs the admin role for it, read_only alone is not enough.
const ROLE_TABLE: [(&str, u16, u16); 8] = [
    ("", 403, 403),
    ("ingest", 200, 403),
    ("retrieve", 403, 403),
    ("read_only", 403, 403),
    ("admin", 200, 200),
    ("ingest,read_only", 200, 403),
    ("ingest,retrieve", 200, 403),
    ("retrieve,read_only", 403, 403),
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
        jwt_default_roles: Some("ingest".to_string()),
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
            ",\"roles\":\"ingest read_only admin\"",
            (200, 200),
        ),
        (
            "comma delimited",
            ",\"roles\":\"ingest,read_only,admin\"",
            (200, 200),
        ),
        ("array", ",\"roles\":[\"ingest\"]", (200, 403)),
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
        role_statuses(&legacy(Some("ingest,read_only")).unwrap(), &auth),
        (200, 200)
    );
    assert_eq!(
        role_statuses(&legacy(Some("admin")).unwrap(), &auth),
        (200, 200)
    );
    assert_eq!(
        role_statuses(&legacy(Some("retrieve")).unwrap(), &auth),
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
            "{KEY_TENANT_A}:tenant-a:ingest,read_only;{WILD}:*:read_only;{ADMIN}:tenant-a:admin"
        )),
        strict_secrets: true,
        ..Default::default()
    })
    .expect("policy");
    for target in ["/metrics", "/debug/placement", "/debug/document-parser"] {
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
