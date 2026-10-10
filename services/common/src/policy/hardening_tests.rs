//! Tests for the auth hardening follow-ups: default roles, OIDC config
//! requirements, JWT limits, `jti` denylist and policy reload.

use super::*;

const SVC: ServiceAuthEnv = ServiceAuthEnv {
    service: "test",
    prefix: "TEST",
    default_scoped_role: Role::Retrieve,
    default_rate_limit_rps: 1000,
    default_rate_limit_burst: 1000,
};

const HS_SECRET: &str = "0123456789abcdef0123456789abcdef";

fn headers(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn raw() -> RawAuthConfig {
    RawAuthConfig::default()
}

fn hs256_raw() -> RawAuthConfig {
    RawAuthConfig {
        jwt_hs256_secret: Some(HS_SECRET.into()),
        strict_secrets: true,
        ..raw()
    }
}

fn token(claims: &str) -> String {
    auth::encode_hs256_token(claims, HS_SECRET).unwrap()
}

fn bearer(token: &str) -> HashMap<String, String> {
    headers(&[("authorization", &format!("Bearer {token}"))])
}

fn exp_in(secs: u64) -> u64 {
    unix_now_secs() + secs
}

#[test]
fn oidc_requires_an_audience_and_a_safe_jwks_url() {
    let base = RawAuthConfig {
        jwt_provider: Some("oidc".into()),
        jwt_issuer: Some("https://issuer.test".into()),
        jwt_jwks_url: Some("https://issuer.test/jwks".into()),
        ..raw()
    };
    let err = AuthPolicy::build(base.clone(), &SVC).unwrap_err();
    assert!(err.contains("AUDIENCE"), "{err}");

    let ok = RawAuthConfig {
        jwt_audience: Some("dash".into()),
        ..base.clone()
    };
    assert!(AuthPolicy::build(ok.clone(), &SVC).is_ok());

    let http = RawAuthConfig {
        jwt_jwks_url: Some("http://issuer.test/jwks".into()),
        ..ok.clone()
    };
    let err = AuthPolicy::build(http.clone(), &SVC).unwrap_err();
    assert!(err.contains("JWKS_URL") && err.contains("https"), "{err}");
    // loopback http and the explicit opt-in are accepted
    assert!(
        AuthPolicy::build(
            RawAuthConfig {
                jwt_jwks_url: Some("http://127.0.0.1:9/jwks".into()),
                ..ok
            },
            &SVC
        )
        .is_ok()
    );
    assert!(
        AuthPolicy::build(
            RawAuthConfig {
                allow_insecure_jwks: true,
                ..http
            },
            &SVC
        )
        .is_ok()
    );
}

#[test]
fn jwt_without_roles_is_forbidden_until_default_roles_are_configured() {
    let t = token(&format!(r#"{{"tenant_id":"t","exp":{}}}"#, exp_in(300)));
    let strict = AuthPolicy::build(hs256_raw(), &SVC).unwrap();
    assert!(matches!(
        strict.authorize_for_tenant(&bearer(&t), "t", Role::Retrieve),
        AuthDecision::Forbidden(_)
    ));
    let with_default = AuthPolicy::build(
        RawAuthConfig {
            jwt_default_roles: Some("retrieve".into()),
            ..hs256_raw()
        },
        &SVC,
    )
    .unwrap();
    assert_eq!(
        with_default.authorize_for_tenant(&bearer(&t), "t", Role::Retrieve),
        AuthDecision::Allowed
    );
    assert!(matches!(
        with_default.authorize_for_tenant(&bearer(&t), "t", Role::Ingest),
        AuthDecision::Forbidden(_)
    ));
    assert!(
        AuthPolicy::build(
            RawAuthConfig {
                jwt_default_roles: Some("bogus".into()),
                ..hs256_raw()
            },
            &SVC
        )
        .is_err()
    );
}

#[test]
fn wildcard_tenant_token_needs_the_explicit_flag() {
    let t = token(&format!(
        r#"{{"tenant_id":"*","dash_roles":["retrieve"],"exp":{}}}"#,
        exp_in(300)
    ));
    let off = AuthPolicy::build(hs256_raw(), &SVC).unwrap();
    assert!(matches!(
        off.authorize_for_tenant(&bearer(&t), "any", Role::Retrieve),
        AuthDecision::Forbidden(_)
    ));
    let on = AuthPolicy::build(
        RawAuthConfig {
            jwt_allow_wildcard_tenant: Some("1".into()),
            ..hs256_raw()
        },
        &SVC,
    )
    .unwrap();
    assert_eq!(
        on.authorize_for_tenant(&bearer(&t), "any", Role::Retrieve),
        AuthDecision::Allowed
    );
}

#[test]
fn jwt_lifetime_exp_and_leeway_limits_apply() {
    let long = token(&format!(
        r#"{{"tenant_id":"t","dash_roles":["retrieve"],"exp":{}}}"#,
        exp_in(90_000)
    ));
    let default = AuthPolicy::build(hs256_raw(), &SVC).unwrap();
    assert!(matches!(
        default.authorize_for_tenant(&bearer(&long), "t", Role::Retrieve),
        AuthDecision::Unauthorized(_)
    ));
    let relaxed = AuthPolicy::build(
        RawAuthConfig {
            jwt_max_lifetime_secs: Some("100000".into()),
            ..hs256_raw()
        },
        &SVC,
    )
    .unwrap();
    assert_eq!(
        relaxed.authorize_for_tenant(&bearer(&long), "t", Role::Retrieve),
        AuthDecision::Allowed
    );
    // No exp at all is always rejected, even if JWT_REQUIRE_EXP=0.
    let no_exp = token(r#"{"tenant_id":"t","dash_roles":["retrieve"]}"#);
    let lax = AuthPolicy::build(
        RawAuthConfig {
            jwt_require_exp: Some("0".into()),
            ..hs256_raw()
        },
        &SVC,
    )
    .unwrap();
    assert!(matches!(
        lax.authorize_for_tenant(&bearer(&no_exp), "t", Role::Retrieve),
        AuthDecision::Unauthorized(_)
    ));
    // Leeway is capped at 60 seconds.
    let stale = token(&format!(
        r#"{{"tenant_id":"t","dash_roles":["retrieve"],"exp":{}}}"#,
        unix_now_secs() - 120
    ));
    let huge_leeway = AuthPolicy::build(
        RawAuthConfig {
            jwt_leeway_secs: Some("100000".into()),
            ..hs256_raw()
        },
        &SVC,
    )
    .unwrap();
    assert!(matches!(
        huge_leeway.authorize_for_tenant(&bearer(&stale), "t", Role::Retrieve),
        AuthDecision::Unauthorized(_)
    ));
}

#[test]
fn short_hs256_secrets_are_rejected_by_the_strict_validator() {
    for secret in ["0123456789abcdef", "a8f3b1c9d2e47f60a8f3b1c9d2e47f"] {
        let err = AuthPolicy::build(
            RawAuthConfig {
                jwt_hs256_secret: Some(secret.into()),
                strict_secrets: true,
                ..raw()
            },
            &SVC,
        )
        .unwrap_err();
        assert!(err.contains("too short") && !err.contains(secret), "{err}");
    }
    assert!(
        AuthPolicy::build(
            RawAuthConfig {
                jwt_hs256_secret: Some("a8f3b1c9d2e47f60a8f3b1c9d2e47f60".into()),
                strict_secrets: true,
                ..raw()
            },
            &SVC
        )
        .is_ok()
    );
}

#[test]
fn jwt_error_decisions_never_echo_claim_values() {
    let t = token(&format!(
        r#"{{"tenant_id":"LEAKY-TENANT","dash_roles":["LEAKY-ROLE"],"jti":"LEAKY-JTI","exp":{}}}"#,
        exp_in(300)
    ));
    let policy = AuthPolicy::build(hs256_raw(), &SVC).unwrap();
    for decision in [
        policy.authorize_for_tenant(&bearer(&t), "other", Role::Retrieve),
        policy.authorize_for_tenant(&bearer(&t), "LEAKY-TENANT", Role::Retrieve),
        policy.authorize_for_tenant(&bearer("a.b.c"), "t", Role::Retrieve),
    ] {
        let text = format!("{decision:?}");
        assert!(!text.contains("LEAKY"), "{text}");
    }
}

#[test]
fn revoked_jti_is_rejected_from_env_list_and_reloaded_file() {
    let t = token(&format!(
        r#"{{"tenant_id":"t","dash_roles":["retrieve"],"jti":"jti-1","exp":{}}}"#,
        exp_in(300)
    ));
    let other = token(&format!(
        r#"{{"tenant_id":"t","dash_roles":["retrieve"],"jti":"jti-2","exp":{}}}"#,
        exp_in(300)
    ));
    let by_env = AuthPolicy::build(
        RawAuthConfig {
            jwt_revoked_jtis: Some("jti-1, jti-x".into()),
            ..hs256_raw()
        },
        &SVC,
    )
    .unwrap();
    assert_eq!(
        by_env.authorize_for_tenant(&bearer(&t), "t", Role::Retrieve),
        AuthDecision::Unauthorized("JWT revoked")
    );
    assert_eq!(
        by_env.authorize_for_tenant(&bearer(&other), "t", Role::Retrieve),
        AuthDecision::Allowed
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jtis.txt");
    std::fs::write(&path, "").unwrap();
    let by_file = AuthPolicy::build(
        RawAuthConfig {
            jwt_revoked_jtis_path: Some(path.to_string_lossy().into()),
            ..hs256_raw()
        },
        &SVC,
    )
    .unwrap();
    assert_eq!(
        by_file.authorize_for_tenant(&bearer(&t), "t", Role::Retrieve),
        AuthDecision::Allowed
    );
    std::fs::write(&path, "jti-1\n").unwrap();
    by_file.jti_revocation_list.state.lock().unwrap().checked_at = None;
    assert_eq!(
        by_file.authorize_for_tenant(&bearer(&t), "t", Role::Retrieve),
        AuthDecision::Unauthorized("JWT revoked")
    );
}

#[test]
fn legacy_key_defaults_to_the_primary_role_and_is_configurable() {
    let key = "legacy-key-0123456789abcdef";
    let hdrs = headers(&[("x-api-key", key)]);
    let default = AuthPolicy::build(
        RawAuthConfig {
            api_key: Some(key.into()),
            ..raw()
        },
        &SVC,
    )
    .unwrap();
    assert_eq!(
        default.authorize_for_tenant(&hdrs, "t", Role::Retrieve),
        AuthDecision::Allowed
    );
    assert!(matches!(
        default.authorize_for_tenant(&hdrs, "t", Role::Ingest),
        AuthDecision::Forbidden(_)
    ));
    assert!(matches!(
        default.authorize_for_tenant(&hdrs, "t", Role::ReadOnly),
        AuthDecision::Forbidden(_)
    ));
    let widened = AuthPolicy::build(
        RawAuthConfig {
            api_key: Some(key.into()),
            api_key_default_roles: Some("retrieve,ingest".into()),
            ..raw()
        },
        &SVC,
    )
    .unwrap();
    assert_eq!(
        widened.authorize_for_tenant(&hdrs, "t", Role::Ingest),
        AuthDecision::Allowed
    );
}

#[test]
fn reload_overlay_parses_key_value_lines() {
    let overlay = parse_overlay(
        "# comment\n\nDASH_X_API_KEY = \"quoted-value\"\nEME_Y=plain\nNOT_DASH=ignored\nbroken line\nDASH_EMPTY=\n",
    );
    assert_eq!(overlay.get("DASH_X_API_KEY").unwrap(), "quoted-value");
    assert_eq!(overlay.get("EME_Y").unwrap(), "plain");
    assert_eq!(overlay.get("DASH_EMPTY").unwrap(), "");
    assert!(!overlay.contains_key("NOT_DASH"));
    assert_eq!(overlay.len(), 3);
}

#[test]
fn reload_swaps_the_policy_and_keeps_the_old_one_on_error() {
    static RELOAD_SVC: ServiceAuthEnv = ServiceAuthEnv {
        service: "reload-test",
        prefix: "RELOADTEST",
        default_scoped_role: Role::Retrieve,
        default_rate_limit_rps: 1000,
        default_rate_limit_burst: 1000,
    };
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("reload.env");
    std::fs::write(&file, "DASH_RELOADTEST_API_KEY=old-key-0123456789abcdef\n").unwrap();
    unsafe {
        std::env::set_var(CONFIG_RELOAD_FILE_ENV, &file);
    }
    let cell = PolicyCell::new();
    let old = headers(&[("x-api-key", "old-key-0123456789abcdef")]);
    let new = headers(&[("x-api-key", "new-key-0123456789abcdef")]);
    assert!(
        cell.reload(&RELOAD_SVC).is_err(),
        "reload needs a pinned policy"
    );
    cell.pin(&RELOAD_SVC).unwrap();
    let allowed = |cell: &PolicyCell, h: &HashMap<String, String>| {
        cell.current(&RELOAD_SVC)
            .authorize_for_tenant(h, "t", Role::Retrieve)
            == AuthDecision::Allowed
    };
    assert!(allowed(&cell, &old) && !allowed(&cell, &new));

    std::fs::write(&file, "DASH_RELOADTEST_API_KEY=new-key-0123456789abcdef\n").unwrap();
    cell.reload(&RELOAD_SVC).unwrap();
    assert!(!allowed(&cell, &old) && allowed(&cell, &new));

    // An invalid configuration is rejected and the running policy stays.
    std::fs::write(
        &file,
        "DASH_RELOADTEST_API_KEY=new-key-0123456789abcdef\nDASH_RELOADTEST_API_KEY_DEFAULT_ROLES=bogus\n",
    )
    .unwrap();
    assert!(cell.reload(&RELOAD_SVC).is_err());
    assert!(allowed(&cell, &new));
    unsafe {
        std::env::remove_var(CONFIG_RELOAD_FILE_ENV);
    }
}

#[cfg(unix)]
#[test]
fn sighup_reloads_the_pinned_policy() {
    static SIG_SVC: ServiceAuthEnv = ServiceAuthEnv {
        service: "sighup-test",
        prefix: "SIGHUPTEST",
        default_scoped_role: Role::Retrieve,
        default_rate_limit_rps: 1000,
        default_rate_limit_burst: 1000,
    };
    static CELL: PolicyCell = PolicyCell::new();
    unsafe {
        std::env::set_var("DASH_SIGHUPTEST_API_KEY", "sighup-old-key-0123456789");
    }
    CELL.pin(&SIG_SVC).unwrap();
    spawn_sighup_reload(&CELL, SIG_SVC);
    let old = headers(&[("x-api-key", "sighup-old-key-0123456789")]);
    let new = headers(&[("x-api-key", "sighup-new-key-0123456789")]);
    let allowed = |h: &HashMap<String, String>| {
        CELL.current(&SIG_SVC)
            .authorize_for_tenant(h, "t", Role::Retrieve)
            == AuthDecision::Allowed
    };
    assert!(allowed(&old) && !allowed(&new));
    unsafe {
        std::env::set_var("DASH_SIGHUPTEST_API_KEY", "sighup-new-key-0123456789");
    }
    signal_hook::low_level::raise(signal_hook::consts::SIGHUP).unwrap();
    let deadline = Instant::now() + std::time::Duration::from_secs(5);
    while !allowed(&new) && Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert!(
        allowed(&new) && !allowed(&old),
        "SIGHUP must rebuild the policy"
    );
}
