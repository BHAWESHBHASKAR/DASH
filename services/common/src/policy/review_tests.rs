//! Regression tests for the independent security review findings: rate
//! limiter keying and bounds, ops-route scoping, audit actor attribution and
//! malformed tenant allowlists.

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

fn limited(extra: RawAuthConfig) -> AuthPolicy {
    AuthPolicy::build(
        RawAuthConfig {
            rate_limit_rps: Some("1".into()),
            rate_limit_burst: Some("3".into()),
            ..extra
        },
        &SVC,
    )
    .unwrap()
}

#[test]
fn rotating_tenant_ids_with_one_wildcard_key_cannot_exceed_its_rate() {
    for scopes in [Some("wild-key-0123456789abcdef:*:retrieve"), None] {
        let policy = limited(RawAuthConfig {
            api_key_scopes: scopes.map(str::to_string),
            api_key: scopes
                .is_none()
                .then(|| "wild-key-0123456789abcdef".to_string()),
            ..Default::default()
        });
        let hdrs = headers(&[("x-api-key", "wild-key-0123456789abcdef")]);
        let allowed = (0..50)
            .filter(|i| {
                policy.authorize_for_tenant(&hdrs, &format!("tenant-{i}"), Role::Retrieve)
                    == AuthDecision::Allowed
            })
            .count();
        assert!(allowed <= 3, "{allowed} requests passed a burst of 3");
    }
}

#[test]
fn tenant_bound_keys_keep_one_bucket_per_allowed_tenant() {
    let policy = limited(RawAuthConfig {
        api_key_scopes: Some("bound-key-0123456789abcdef:t1,t2:retrieve".into()),
        ..Default::default()
    });
    let hdrs = headers(&[("x-api-key", "bound-key-0123456789abcdef")]);
    for tenant in ["t1", "t2"] {
        for _ in 0..3 {
            assert_eq!(
                policy.authorize_for_tenant(&hdrs, tenant, Role::Retrieve),
                AuthDecision::Allowed
            );
        }
        assert!(matches!(
            policy.authorize_for_tenant(&hdrs, tenant, Role::Retrieve),
            AuthDecision::RateLimited { .. }
        ));
    }
}

#[test]
fn route_classes_do_not_share_a_bucket() {
    let policy = limited(RawAuthConfig {
        api_key_scopes: Some("admin-key-0123456789abcdef:*:admin".into()),
        ..Default::default()
    });
    let hdrs = headers(&[("x-api-key", "admin-key-0123456789abcdef")]);
    // Exhaust the ops bucket (a metrics scraper).
    for _ in 0..3 {
        assert_eq!(
            policy.authorize_ops(&hdrs, Role::ReadOnly),
            AuthDecision::Allowed
        );
    }
    assert!(matches!(
        policy.authorize_ops(&hdrs, Role::ReadOnly),
        AuthDecision::RateLimited { .. }
    ));
    // Data and embeddings traffic is unaffected.
    assert_eq!(
        policy.authorize_for_tenant(&hdrs, "t", Role::Retrieve),
        AuthDecision::Allowed
    );
    assert_eq!(
        policy.authorize_any_tenant(&hdrs, Role::Retrieve),
        AuthDecision::Allowed
    );
}

#[test]
fn rate_limit_keys_never_contain_the_raw_credential() {
    let key = "very-secret-key-0123456789abcdef";
    let policy = limited(RawAuthConfig {
        api_key_scopes: Some(format!("{key}:*:retrieve")),
        ..Default::default()
    });
    let hdrs = headers(&[("x-api-key", key)]);
    assert_eq!(
        policy.authorize_for_tenant(&hdrs, "t", Role::Retrieve),
        AuthDecision::Allowed
    );
    let limiter = policy.rate_limiter.as_ref().unwrap();
    let state = limiter.state.lock().unwrap();
    assert_eq!(state.buckets.len(), 1);
    for bucket_key in state.buckets.keys() {
        assert!(!bucket_key.contains(key), "{bucket_key}");
        assert!(!bucket_key.contains("very-secret"), "{bucket_key}");
    }
}

#[test]
fn bucket_count_is_capped_and_sweeps_are_not_per_call() {
    let limiter = TenantRateLimiter::with_capacity(1, 1, 1_000);
    let now = Instant::now();
    for i in 0..100_000u32 {
        assert!(limiter.check_at(&format!("k{i}"), now).is_ok());
        assert!(limiter.bucket_count() <= 1_000);
    }
    assert_eq!(limiter.bucket_count(), 1_000);
    // No O(n) scan ran while time stood still: eviction at the cap is O(1).
    assert_eq!(limiter.sweep_scanned(), 0);

    // The default cap is 50k and holds for 100k distinct keys too.
    let limiter = TenantRateLimiter::new(1, 1);
    let now = Instant::now();
    for i in 0..100_000u32 {
        let _ = limiter.check_at(&format!("key-{i}"), now);
    }
    assert!(limiter.bucket_count() <= BUCKET_CAPACITY);
    assert_eq!(limiter.sweep_scanned(), 0);

    // Idle buckets are swept on a timer: one scan per interval, not per call.
    let later = now + std::time::Duration::from_secs(BUCKET_IDLE_EVICT_SECS + 60);
    for i in 0..1_000u32 {
        let _ = limiter.check_at(&format!("fresh-{i}"), later);
    }
    assert!(limiter.bucket_count() <= 1_000 + 1);
    assert!(limiter.sweep_scanned() <= 2 * BUCKET_CAPACITY as u64 + 10);
}

#[test]
fn evicting_at_the_cap_removes_the_oldest_bucket_not_the_new_one() {
    let limiter = TenantRateLimiter::with_capacity(1, 1, 2);
    let now = Instant::now();
    assert!(limiter.check_at("a", now).is_ok());
    assert!(limiter.check_at("b", now).is_ok());
    assert!(limiter.check_at("c", now).is_ok()); // evicts "a"
    assert_eq!(limiter.bucket_count(), 2);
    assert!(limiter.check_at("b", now).is_err()); // still tracked, still empty
    assert!(limiter.check_at("c", now).is_err());
    assert!(limiter.check_at("a", now).is_ok()); // fresh bucket again
}

fn scoped_policy() -> AuthPolicy {
    AuthPolicy::build(
        RawAuthConfig {
            api_key_scopes: Some(
                "tenant-ro-key-0123456789abc:tenant-a:read_only;\
                 wild-ro-key-0123456789abcde:*:read_only;\
                 tenant-admin-key-0123456789:tenant-a:admin"
                    .into(),
            ),
            ..Default::default()
        },
        &SVC,
    )
    .unwrap()
}

#[test]
fn ops_routes_refuse_tenant_scoped_keys() {
    let policy = scoped_policy();
    let tenant_ro = headers(&[("x-api-key", "tenant-ro-key-0123456789abc")]);
    // The same key may still read its own tenant's data.
    assert_eq!(
        policy.authorize_for_tenant(&tenant_ro, "tenant-a", Role::ReadOnly),
        AuthDecision::Allowed
    );
    assert!(matches!(
        policy.authorize_ops(&tenant_ro, Role::ReadOnly),
        AuthDecision::Forbidden(_)
    ));
    let wild_ro = headers(&[("x-api-key", "wild-ro-key-0123456789abcde")]);
    assert_eq!(
        policy.authorize_ops(&wild_ro, Role::ReadOnly),
        AuthDecision::Allowed
    );
    let tenant_admin = headers(&[("x-api-key", "tenant-admin-key-0123456789")]);
    assert_eq!(
        policy.authorize_ops(&tenant_admin, Role::ReadOnly),
        AuthDecision::Allowed
    );
    // Unknown and missing credentials are still 401.
    assert!(matches!(
        policy.authorize_ops(&headers(&[("x-api-key", "nope")]), Role::ReadOnly),
        AuthDecision::Unauthorized(_)
    ));
    assert!(matches!(
        policy.authorize_ops(&headers(&[]), Role::ReadOnly),
        AuthDecision::Unauthorized(_)
    ));
}

#[test]
fn ops_routes_need_admin_for_jwts() {
    const HS: &str = "0123456789abcdef0123456789abcdef";
    let policy = AuthPolicy::build(
        RawAuthConfig {
            jwt_hs256_secret: Some(HS.into()),
            strict_secrets: true,
            ..Default::default()
        },
        &SVC,
    )
    .unwrap();
    let exp = unix_now_secs() + 300;
    let mk = |roles: &str| {
        let t = auth::encode_hs256_token(
            &format!(r#"{{"tenant_id":"t","dash_roles":"{roles}","exp":{exp}}}"#),
            HS,
        )
        .unwrap();
        headers(&[("authorization", &format!("Bearer {t}"))])
    };
    assert!(matches!(
        policy.authorize_ops(&mk("read_only"), Role::ReadOnly),
        AuthDecision::Forbidden(_)
    ));
    assert_eq!(
        policy.authorize_ops(&mk("admin"), Role::ReadOnly),
        AuthDecision::Allowed
    );
}

#[test]
fn audit_actor_is_the_credential_the_policy_evaluated() {
    let policy = AuthPolicy::build(
        RawAuthConfig {
            api_key: Some("real-api-key-0123456789abcdef".into()),
            api_key_default_roles: Some("retrieve".into()),
            ..Default::default()
        },
        &SVC,
    )
    .unwrap();
    // x-api-key wins over a (different) bearer token, as in the policy.
    let hdrs = headers(&[
        ("x-api-key", "real-api-key-0123456789abcdef"),
        ("authorization", "Bearer some-other-bearer-token"),
    ]);
    let _guard = audit::enter_context(audit::context_from_headers(&hdrs));
    assert_eq!(
        policy.authorize_for_tenant(&hdrs, "t", Role::Retrieve),
        AuthDecision::Allowed
    );
    let actor = audit::current_context().actor.expect("actor");
    assert_eq!(actor.kind, "api_key");
    assert_eq!(
        actor.id,
        Some(audit::key_fingerprint("real-api-key-0123456789abcdef"))
    );
}

#[test]
fn audit_actor_kind_reflects_the_verifier_that_judged_the_token() {
    const HS: &str = "0123456789abcdef0123456789abcdef";
    let policy = AuthPolicy::build(
        RawAuthConfig {
            jwt_hs256_secret: Some(HS.into()),
            jwt_default_roles: Some("retrieve".into()),
            strict_secrets: true,
            ..Default::default()
        },
        &SVC,
    )
    .unwrap();
    let exp = unix_now_secs() + 300;
    let t = auth::encode_hs256_token(&format!(r#"{{"tenant_id":"t","exp":{exp}}}"#), HS).unwrap();
    let hdrs = headers(&[("authorization", &format!("Bearer {t}"))]);
    let _guard = audit::enter_context(audit::AuditContext::default());
    assert_eq!(
        policy.authorize_for_tenant(&hdrs, "t", Role::Retrieve),
        AuthDecision::Allowed
    );
    let actor = audit::current_context().actor.expect("actor");
    assert_eq!(actor.kind, "jwt");
    assert_eq!(actor.id, Some(audit::key_fingerprint(&t)));
}

#[test]
fn blank_allowed_tenants_is_a_startup_error_not_any_tenant() {
    for bad in ["", " ", ",", " , ,", ",,"] {
        let err = AuthPolicy::build(
            RawAuthConfig {
                api_key: Some("key-0123456789abcdef".into()),
                allowed_tenants: Some(bad.to_string()),
                ..Default::default()
            },
            &SVC,
        )
        .unwrap_err();
        assert!(err.contains("DASH_TEST_ALLOWED_TENANTS"), "{bad:?}: {err}");
    }
    // Unset still means any tenant; an explicit list and "*" keep working.
    for (ok, tenant, allowed) in [
        (None, "anything", true),
        (Some("*"), "anything", true),
        (Some("a, b"), "b", true),
        (Some("a, b"), "c", false),
    ] {
        let policy = AuthPolicy::build(
            RawAuthConfig {
                api_key: Some("key-0123456789abcdef".into()),
                api_key_default_roles: Some("retrieve".into()),
                allowed_tenants: ok.map(str::to_string),
                ..Default::default()
            },
            &SVC,
        )
        .unwrap();
        let hdrs = headers(&[("x-api-key", "key-0123456789abcdef")]);
        assert_eq!(
            policy.authorize_for_tenant(&hdrs, tenant, Role::Retrieve) == AuthDecision::Allowed,
            allowed,
            "{ok:?} {tenant}"
        );
    }
}
