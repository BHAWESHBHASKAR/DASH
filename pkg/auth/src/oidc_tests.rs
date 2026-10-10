//! OIDC verification and JWKS cache tests: RS256 with generated keys, a
//! fake fetcher for cache logic and a local stub HTTP server for end to end
//! behavior (outage, slow IdP, oversize, redirect, rotation, garbage flood).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use jsonwebtoken::{Algorithm, EncodingKey, Header, jwk::JwkSet};
use serde_json::json;

use crate::oidc::testing::{self, new_cache};
use crate::oidc::{
    JWKS_FORCED_REFRESH_INTERVAL, JWKS_NEGATIVE_TTL, JWKS_STALE_MAX, OidcValidationConfig,
    validate_jwks_url, verify_oidc_token_for_tenant, verify_oidc_token_with_jwks,
};
use crate::test_keys::{KEY_A_N, KEY_A_PEM, KEY_B_N, KEY_B_PEM};
use crate::{JwtValidationError, encode_hs256_token_with_kid};

const NOW: u64 = 1_000_000_000;
const ISS: &str = "https://idp.example.test";
const AUD: &str = "dash-api";

fn config(url: &str) -> OidcValidationConfig {
    OidcValidationConfig {
        issuer: ISS.to_string(),
        audience: AUD.to_string(),
        jwks_url: url.to_string(),
        jwks_refresh_minutes: 15,
        leeway_secs: 0,
        max_lifetime_secs: 3_600,
        tenant_claims: vec!["tenant_id".to_string()],
        allow_wildcard_tenant: false,
        allow_insecure_jwks: false,
    }
}

fn jwk_json(kid: &str, n: &str, extra: serde_json::Value) -> serde_json::Value {
    let mut value = json!({"kty": "RSA", "kid": kid, "n": n, "e": "AQAB"});
    if let (Some(base), Some(more)) = (value.as_object_mut(), extra.as_object()) {
        for (k, v) in more {
            base.insert(k.clone(), v.clone());
        }
    }
    value
}

fn jwks(keys: Vec<serde_json::Value>) -> JwkSet {
    serde_json::from_value(json!({ "keys": keys })).expect("jwks")
}

fn sign(alg: Algorithm, pem: &str, kid: &str, claims: serde_json::Value) -> String {
    let mut header = Header::new(alg);
    header.kid = Some(kid.to_string());
    jsonwebtoken::encode(
        &header,
        &claims,
        &EncodingKey::from_rsa_pem(pem.as_bytes()).expect("pem"),
    )
    .expect("sign")
}

fn claims() -> serde_json::Value {
    json!({"tenant_id": "tenant-a", "iss": ISS, "aud": AUD, "iat": NOW, "exp": NOW + 300})
}

fn token_a(kid: &str) -> String {
    sign(Algorithm::RS256, KEY_A_PEM, kid, claims())
}

fn set_a() -> JwkSet {
    jwks(vec![jwk_json(
        "a",
        KEY_A_N,
        json!({"alg": "RS256", "use": "sig"}),
    )])
}

// --- RS256 verification -----------------------------------------------------

#[test]
fn rs256_token_is_accepted_with_matching_key() {
    let result =
        verify_oidc_token_with_jwks(&token_a("a"), "tenant-a", &config("x"), NOW, &set_a());
    assert!(result.is_ok(), "{result:?}");
}

#[test]
fn rs256_token_signed_by_another_key_is_rejected() {
    // Token signed with key B but claiming kid "a".
    let forged = sign(Algorithm::RS256, KEY_B_PEM, "a", claims());
    let result = verify_oidc_token_with_jwks(&forged, "tenant-a", &config("x"), NOW, &set_a());
    assert_eq!(result, Err(JwtValidationError::InvalidSignature));
}

#[test]
fn unknown_kid_is_rejected() {
    let result =
        verify_oidc_token_with_jwks(&token_a("zzz"), "tenant-a", &config("x"), NOW, &set_a());
    assert_eq!(result, Err(JwtValidationError::UnknownKeyId));
}

#[test]
fn symmetric_algorithms_are_not_in_the_allow_list() {
    let key = base64url(b"shared-secret-shared-secret-shared-secret");
    let set = jwks(vec![json!({"kty":"oct","kid":"k","alg":"HS256","k":key})]);
    let token = encode_hs256_token_with_kid(
        &claims().to_string(),
        "shared-secret-shared-secret-shared-secret",
        Some("k"),
    )
    .unwrap();
    let result = verify_oidc_token_with_jwks(&token, "tenant-a", &config("x"), NOW, &set);
    assert_eq!(result, Err(JwtValidationError::UnsupportedAlgorithm));
}

fn base64url(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

#[test]
fn aud_iss_and_exp_are_required_and_checked() {
    let cfg = config("x");
    let set = set_a();
    let mut no_aud = claims();
    no_aud.as_object_mut().unwrap().remove("aud");
    let t = sign(Algorithm::RS256, KEY_A_PEM, "a", no_aud);
    assert_eq!(
        verify_oidc_token_with_jwks(&t, "tenant-a", &cfg, NOW, &set),
        Err(JwtValidationError::MissingClaim("aud"))
    );
    let mut no_iss = claims();
    no_iss.as_object_mut().unwrap().remove("iss");
    let t = sign(Algorithm::RS256, KEY_A_PEM, "a", no_iss);
    assert_eq!(
        verify_oidc_token_with_jwks(&t, "tenant-a", &cfg, NOW, &set),
        Err(JwtValidationError::MissingClaim("iss"))
    );
    let mut no_exp = claims();
    no_exp.as_object_mut().unwrap().remove("exp");
    let t = sign(Algorithm::RS256, KEY_A_PEM, "a", no_exp);
    assert_eq!(
        verify_oidc_token_with_jwks(&t, "tenant-a", &cfg, NOW, &set),
        Err(JwtValidationError::MissingClaim("exp"))
    );
    let mut wrong_aud = claims();
    wrong_aud["aud"] = json!("someone-else");
    let t = sign(Algorithm::RS256, KEY_A_PEM, "a", wrong_aud);
    assert_eq!(
        verify_oidc_token_with_jwks(&t, "tenant-a", &cfg, NOW, &set),
        Err(JwtValidationError::AudienceMismatch)
    );
    let mut wrong_iss = claims();
    wrong_iss["iss"] = json!("https://evil.test");
    let t = sign(Algorithm::RS256, KEY_A_PEM, "a", wrong_iss);
    assert_eq!(
        verify_oidc_token_with_jwks(&t, "tenant-a", &cfg, NOW, &set),
        Err(JwtValidationError::IssuerMismatch)
    );
}

#[test]
fn empty_audience_config_rejects_everything() {
    let mut cfg = config("http://127.0.0.1:9/jwks");
    cfg.audience = String::new();
    let cache = new_cache();
    let fetch = |_: &str| -> Result<JwkSet, String> { panic!("must not fetch") };
    let result = testing::verify(
        &cache,
        &fetch,
        &token_a("a"),
        "tenant-a",
        &cfg,
        NOW,
        Instant::now(),
    );
    assert!(matches!(
        result,
        Err(JwtValidationError::OidcProviderError(_))
    ));
}

#[test]
fn max_lifetime_is_enforced_for_oidc() {
    let mut long = claims();
    long["exp"] = json!(NOW + 7_200);
    let t = sign(Algorithm::RS256, KEY_A_PEM, "a", long);
    assert_eq!(
        verify_oidc_token_with_jwks(&t, "tenant-a", &config("x"), NOW, &set_a()),
        Err(JwtValidationError::TokenLifetimeTooLong)
    );
}

#[test]
fn wildcard_tenant_requires_explicit_opt_in() {
    let mut wild = claims();
    wild["tenant_id"] = json!("*");
    let t = sign(Algorithm::RS256, KEY_A_PEM, "a", wild);
    let mut cfg = config("x");
    assert_eq!(
        verify_oidc_token_with_jwks(&t, "other", &cfg, NOW, &set_a()),
        Err(JwtValidationError::TenantNotAllowed)
    );
    cfg.allow_wildcard_tenant = true;
    assert!(verify_oidc_token_with_jwks(&t, "other", &cfg, NOW, &set_a()).is_ok());
}

#[test]
fn jwk_own_alg_and_use_are_enforced() {
    // Key restricted to RS384 must not verify an RS256 token.
    let set = jwks(vec![jwk_json("a", KEY_A_N, json!({"alg": "RS384"}))]);
    assert_eq!(
        verify_oidc_token_with_jwks(&token_a("a"), "tenant-a", &config("x"), NOW, &set),
        Err(JwtValidationError::UnsupportedAlgorithm)
    );
    // Encryption keys must not verify signatures.
    let set = jwks(vec![jwk_json("a", KEY_A_N, json!({"use": "enc"}))]);
    assert_eq!(
        verify_oidc_token_with_jwks(&token_a("a"), "tenant-a", &config("x"), NOW, &set),
        Err(JwtValidationError::InvalidSignature)
    );
    // No alg/use members at all is fine.
    let set = jwks(vec![jwk_json("a", KEY_A_N, json!({}))]);
    assert!(
        verify_oidc_token_with_jwks(&token_a("a"), "tenant-a", &config("x"), NOW, &set).is_ok()
    );
}

#[test]
fn jwks_url_must_be_https_unless_loopback_or_allowed() {
    assert!(validate_jwks_url("https://idp.example.test/jwks", false).is_ok());
    assert!(validate_jwks_url("http://idp.example.test/jwks", false).is_err());
    assert!(validate_jwks_url("http://127.0.0.1:8080/jwks", false).is_ok());
    assert!(validate_jwks_url("http://localhost/jwks", false).is_ok());
    assert!(validate_jwks_url("http://[::1]:9/jwks", false).is_ok());
    assert!(validate_jwks_url("http://127.0.0.1.evil.test/jwks", false).is_err());
    assert!(validate_jwks_url("http://127.0.0.1@evil.test/jwks", false).is_err());
    assert!(validate_jwks_url("ftp://idp.example.test/jwks", true).is_err());
    assert!(validate_jwks_url("http://idp.example.test/jwks", true).is_ok());
}

#[test]
fn non_https_remote_jwks_url_is_never_fetched() {
    let cfg = config("http://idp.example.test/jwks");
    let cache = new_cache();
    let fetches = AtomicUsize::new(0);
    let fetch = |_: &str| -> Result<JwkSet, String> {
        fetches.fetch_add(1, Ordering::SeqCst);
        Ok(set_a())
    };
    let result = testing::verify(
        &cache,
        &fetch,
        &token_a("a"),
        "tenant-a",
        &cfg,
        NOW,
        Instant::now(),
    );
    assert!(result.is_err());
    assert_eq!(fetches.load(Ordering::SeqCst), 0);
}

// --- cache behavior with a fake fetcher --------------------------------------

const URL: &str = "https://idp.example.test/jwks";

#[test]
fn garbage_tokens_never_cause_a_fetch() {
    let cache = new_cache();
    let fetches = AtomicUsize::new(0);
    let fetch = |_: &str| -> Result<JwkSet, String> {
        fetches.fetch_add(1, Ordering::SeqCst);
        Ok(set_a())
    };
    let cfg = config(URL);
    let hs256 = encode_hs256_token_with_kid(
        &claims().to_string(),
        "shared-secret-shared-secret-shared-secret",
        Some("a"),
    )
    .unwrap();
    let no_kid = {
        let header = Header::new(Algorithm::RS256);
        jsonwebtoken::encode(
            &header,
            &claims(),
            &EncodingKey::from_rsa_pem(KEY_A_PEM.as_bytes()).unwrap(),
        )
        .unwrap()
    };
    let alg_none = format!(
        "{}.{}.",
        base64url(br#"{"alg":"none","kid":"a"}"#),
        base64url(claims().to_string().as_bytes())
    );
    let mut tokens = vec![hs256, no_kid, alg_none];
    for i in 0..2_000 {
        tokens.push(format!("garbage-{i}"));
        tokens.push(format!("a{i}.b{i}.c{i}"));
        tokens.push(String::new());
    }
    for token in &tokens {
        let result = testing::verify(&cache, &fetch, token, "tenant-a", &cfg, NOW, Instant::now());
        assert!(result.is_err());
    }
    assert_eq!(
        fetches.load(Ordering::SeqCst),
        0,
        "no JWKS fetch for garbage"
    );
}

#[test]
fn concurrent_cold_start_fetches_once() {
    let cache = Arc::new(new_cache());
    let fetches = Arc::new(AtomicUsize::new(0));
    let now = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..8 {
        let cache = Arc::clone(&cache);
        let fetches = Arc::clone(&fetches);
        handles.push(std::thread::spawn(move || {
            let fetch = |_: &str| -> Result<JwkSet, String> {
                fetches.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(300));
                Ok(set_a())
            };
            testing::get(&cache, URL, Duration::from_secs(900), false, now, &fetch).is_ok()
        }));
    }
    for handle in handles {
        assert!(handle.join().unwrap(), "every caller gets keys");
    }
    assert_eq!(fetches.load(Ordering::SeqCst), 1, "single flight");
}

#[test]
fn stale_keys_are_served_when_refresh_fails_for_up_to_24_hours() {
    let cache = new_cache();
    let ttl = Duration::from_secs(900);
    let t0 = Instant::now();
    let ok = |_: &str| -> Result<JwkSet, String> { Ok(set_a()) };
    testing::get(&cache, URL, ttl, false, t0, &ok).unwrap();

    let fetches = AtomicUsize::new(0);
    let down = |_: &str| -> Result<JwkSet, String> {
        fetches.fetch_add(1, Ordering::SeqCst);
        Err("idp down".to_string())
    };
    // TTL expired, IdP down: stale keys keep working.
    let t1 = t0 + ttl + Duration::from_secs(1);
    assert!(testing::get(&cache, URL, ttl, false, t1, &down).is_ok());
    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    // Negative caching: no further fetches for 30 seconds.
    for i in 0..50 {
        let t = t1 + Duration::from_millis(i * 100);
        assert!(testing::get(&cache, URL, ttl, false, t, &down).is_ok());
    }
    assert_eq!(fetches.load(Ordering::SeqCst), 1, "failure is cached");
    // After the negative TTL one new attempt is made.
    let t2 = t1 + JWKS_NEGATIVE_TTL + Duration::from_secs(1);
    assert!(testing::get(&cache, URL, ttl, false, t2, &down).is_ok());
    assert_eq!(fetches.load(Ordering::SeqCst), 2);
    // Beyond 24 hours the stale keys are no longer served.
    let t3 = t0 + JWKS_STALE_MAX + Duration::from_secs(60);
    assert!(matches!(
        testing::get(&cache, URL, ttl, false, t3, &down),
        Err(JwtValidationError::OidcProviderError(_))
    ));
}

#[test]
fn unknown_kid_forces_at_most_one_refresh_per_minute_and_picks_up_rotation() {
    let cache = new_cache();
    let cfg = config(URL);
    let fetches = AtomicUsize::new(0);
    let rotated = std::sync::atomic::AtomicBool::new(false);
    let fetch = |_: &str| -> Result<JwkSet, String> {
        fetches.fetch_add(1, Ordering::SeqCst);
        if rotated.load(Ordering::SeqCst) {
            Ok(jwks(vec![
                jwk_json("a", KEY_A_N, json!({})),
                jwk_json("b", KEY_B_N, json!({})),
            ]))
        } else {
            Ok(set_a())
        }
    };
    let t0 = Instant::now();
    let tok_a = token_a("a");
    assert!(testing::verify(&cache, &fetch, &tok_a, "tenant-a", &cfg, NOW, t0).is_ok());
    assert_eq!(fetches.load(Ordering::SeqCst), 1);

    // A flood of tokens with unknown kids: one forced refresh, then none.
    let bogus = token_a("nope");
    for _ in 0..100 {
        let r = testing::verify(&cache, &fetch, &bogus, "tenant-a", &cfg, NOW, t0);
        assert_eq!(r, Err(JwtValidationError::UnknownKeyId));
    }
    assert_eq!(fetches.load(Ordering::SeqCst), 2, "one forced refresh");

    // The IdP rotates; within the interval the new kid is still refused...
    rotated.store(true, Ordering::SeqCst);
    let tok_b = sign(Algorithm::RS256, KEY_B_PEM, "b", claims());
    let t1 = t0 + Duration::from_secs(10);
    assert_eq!(
        testing::verify(&cache, &fetch, &tok_b, "tenant-a", &cfg, NOW, t1),
        Err(JwtValidationError::UnknownKeyId)
    );
    assert_eq!(fetches.load(Ordering::SeqCst), 2);
    // ...and accepted once the forced-refresh interval has passed.
    let t2 = t0 + JWKS_FORCED_REFRESH_INTERVAL + Duration::from_secs(1);
    assert!(testing::verify(&cache, &fetch, &tok_b, "tenant-a", &cfg, NOW, t2).is_ok());
    assert_eq!(fetches.load(Ordering::SeqCst), 3);
}

// --- end to end against a local stub IdP -------------------------------------

#[derive(Clone)]
enum Behavior {
    Serve(String),
    Hang,
    Redirect,
}

struct Stub {
    url: String,
    hits: Arc<AtomicUsize>,
    behavior: Arc<Mutex<Behavior>>,
}

fn spawn_stub(behavior: Behavior) -> Stub {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().unwrap().port();
    let hits = Arc::new(AtomicUsize::new(0));
    let behavior = Arc::new(Mutex::new(behavior));
    let (h, b) = (Arc::clone(&hits), Arc::clone(&behavior));
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            h.fetch_add(1, Ordering::SeqCst);
            let behavior = b.lock().unwrap().clone();
            std::thread::spawn(move || {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf);
                match behavior {
                    Behavior::Serve(body) => {
                        let head = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\
                             Content-Length: {}\r\nConnection: close\r\n\r\n",
                            body.len()
                        );
                        let _ = stream.write_all(head.as_bytes());
                        let _ = stream.write_all(body.as_bytes());
                    }
                    Behavior::Hang => std::thread::sleep(Duration::from_secs(8)),
                    Behavior::Redirect => {
                        let _ = stream.write_all(
                            b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:1/x\r\n\
                              Content-Length: 0\r\nConnection: close\r\n\r\n",
                        );
                    }
                }
            });
        }
    });
    Stub {
        url: format!("http://127.0.0.1:{port}/jwks"),
        hits,
        behavior,
    }
}

fn serve_set(keys: Vec<serde_json::Value>) -> Behavior {
    Behavior::Serve(json!({ "keys": keys }).to_string())
}

fn key_a_entry() -> serde_json::Value {
    jwk_json("a", KEY_A_N, json!({"alg": "RS256", "use": "sig"}))
}

#[test]
fn stub_idp_serves_keys_and_results_are_cached() {
    let stub = spawn_stub(serve_set(vec![key_a_entry()]));
    let cfg = config(&stub.url);
    for _ in 0..5 {
        assert!(verify_oidc_token_for_tenant(&token_a("a"), "tenant-a", &cfg, NOW).is_ok());
    }
    assert_eq!(stub.hits.load(Ordering::SeqCst), 1);
    // A token signed by the wrong key is rejected end to end.
    let forged = sign(Algorithm::RS256, KEY_B_PEM, "a", claims());
    assert_eq!(
        verify_oidc_token_for_tenant(&forged, "tenant-a", &cfg, NOW),
        Err(JwtValidationError::InvalidSignature)
    );
}

#[test]
fn stub_idp_garbage_flood_causes_zero_fetches() {
    let stub = spawn_stub(serve_set(vec![key_a_entry()]));
    let cfg = config(&stub.url);
    for i in 0..500 {
        let _ = verify_oidc_token_for_tenant(&format!("junk.{i}.token"), "tenant-a", &cfg, NOW);
        let _ = verify_oidc_token_for_tenant(&format!("junk-{i}"), "tenant-a", &cfg, NOW);
    }
    assert_eq!(stub.hits.load(Ordering::SeqCst), 0);
}

#[test]
fn stub_idp_outage_fails_fast_and_is_negatively_cached() {
    // Bind and drop: connections are refused.
    let port = {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let cfg = config(&format!("http://127.0.0.1:{port}/jwks"));
    let started = Instant::now();
    let first = verify_oidc_token_for_tenant(&token_a("a"), "tenant-a", &cfg, NOW);
    assert!(matches!(
        first,
        Err(JwtValidationError::OidcProviderError(_))
    ));
    // Repeated calls are answered from the negative cache without I/O.
    for _ in 0..100 {
        let r = verify_oidc_token_for_tenant(&token_a("a"), "tenant-a", &cfg, NOW);
        assert!(matches!(r, Err(JwtValidationError::OidcProviderError(_))));
    }
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[test]
fn stub_idp_outage_after_success_keeps_serving_stale_keys() {
    let stub = spawn_stub(serve_set(vec![key_a_entry()]));
    let mut cfg = config(&stub.url);
    cfg.jwks_refresh_minutes = 1;
    assert!(verify_oidc_token_for_tenant(&token_a("a"), "tenant-a", &cfg, NOW).is_ok());
    // Break the IdP; a kid miss forces a refresh that fails, yet the known
    // kid keeps verifying from the cache.
    *stub.behavior.lock().unwrap() = Behavior::Redirect;
    assert_eq!(
        verify_oidc_token_for_tenant(&token_a("unknown"), "tenant-a", &cfg, NOW),
        Err(JwtValidationError::UnknownKeyId)
    );
    assert!(verify_oidc_token_for_tenant(&token_a("a"), "tenant-a", &cfg, NOW).is_ok());
}

#[test]
fn slow_idp_is_cut_off_by_the_fetch_timeout() {
    let stub = spawn_stub(Behavior::Hang);
    let cfg = config(&stub.url);
    let token = token_a("a");
    let started = Instant::now();
    let result = verify_oidc_token_for_tenant(&token, "tenant-a", &cfg, NOW);
    let elapsed = started.elapsed();
    assert!(matches!(
        result,
        Err(JwtValidationError::OidcProviderError(_))
    ));
    assert!(elapsed < Duration::from_millis(4_500), "took {elapsed:?}");
    // Subsequent requests do not wait on the slow IdP again.
    let again = Instant::now();
    assert!(verify_oidc_token_for_tenant(&token, "tenant-a", &cfg, NOW).is_err());
    assert!(
        again.elapsed() < Duration::from_millis(1_000),
        "second call took {:?}",
        again.elapsed()
    );
    assert_eq!(stub.hits.load(Ordering::SeqCst), 1);
}

#[test]
fn oversized_jwks_response_is_rejected() {
    let padding = "x".repeat(300 * 1024);
    let body = json!({"keys": [key_a_entry()], "padding": padding}).to_string();
    let stub = spawn_stub(Behavior::Serve(body));
    let cfg = config(&stub.url);
    let result = verify_oidc_token_for_tenant(&token_a("a"), "tenant-a", &cfg, NOW);
    assert!(matches!(
        result,
        Err(JwtValidationError::OidcProviderError(_))
    ));
}

#[test]
fn jwks_redirects_are_not_followed() {
    let stub = spawn_stub(Behavior::Redirect);
    let cfg = config(&stub.url);
    let result = verify_oidc_token_for_tenant(&token_a("a"), "tenant-a", &cfg, NOW);
    assert!(matches!(
        result,
        Err(JwtValidationError::OidcProviderError(_))
    ));
    assert_eq!(stub.hits.load(Ordering::SeqCst), 1);
}

#[test]
fn stub_idp_key_rotation_is_picked_up_via_forced_refresh() {
    let stub = spawn_stub(serve_set(vec![key_a_entry()]));
    let cfg = config(&stub.url);
    assert!(verify_oidc_token_for_tenant(&token_a("a"), "tenant-a", &cfg, NOW).is_ok());
    *stub.behavior.lock().unwrap() = serve_set(vec![
        key_a_entry(),
        jwk_json("b", KEY_B_N, json!({"alg": "RS256"})),
    ]);
    let tok_b = sign(Algorithm::RS256, KEY_B_PEM, "b", claims());
    assert!(verify_oidc_token_for_tenant(&tok_b, "tenant-a", &cfg, NOW).is_ok());
    assert_eq!(stub.hits.load(Ordering::SeqCst), 2);
    // Fetching directly returns the parsed set.
    assert!(testing::real_fetch(&stub.url).is_ok());
}
