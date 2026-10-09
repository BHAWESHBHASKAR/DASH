//! Scenario 1: authentication, authorization, rate limiting and replication
//! tokens, over real sockets against the real binaries.

use std::net::SocketAddr;
use std::time::Duration;

use dash_e2e::jwt::{mint_hs256, now_unix};
use dash_e2e::*;
use serde_json::json;

fn env(pairs: &[(&str, String)]) -> Vec<(String, String)> {
    pairs.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

/// Start `bin` with `envs` and wait until /live answers.
fn start(dir: &std::path::Path, bin: &str, port: u16, live: &str, envs: Vec<(String, String)>) -> (Proc, SocketAddr) {
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut p = Proc::spawn(bin, bin, &[], &envs, &dir.join(format!("{bin}-{port}.log")));
    p.wait_live(addr, live, Duration::from_secs(30));
    (p, addr)
}

fn assert_refuses_to_start(bin: &str, envs: Vec<(String, String)>, expect_in_log: &[&str]) {
    let dir = tempfile::tempdir().unwrap();
    let mut p = Proc::spawn(bin, bin, &[], &envs, &dir.path().join("out.log"));
    let status = p
        .wait_exit(Duration::from_secs(20))
        .unwrap_or_else(|| panic!("{bin} kept running with no credentials and no dev mode\n{}", p.log()));
    assert!(!status.success(), "{bin} exited 0 without credentials");
    let log = p.log();
    assert!(
        expect_in_log.iter().any(|needle| log.contains(needle)),
        "{bin} exit message should mention one of {expect_in_log:?}; got:\n{log}"
    );
}

#[test]
fn no_credentials_and_no_dev_mode_refuses_to_start() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    assert_refuses_to_start(
        "ingestion",
        env(&[
            ("DASH_INGEST_BIND", format!("127.0.0.1:{}", free_port())),
            ("DASH_INGEST_WAL_PATH", d.join("i.wal").display().to_string()),
            ("DASH_INGEST_PERSISTENCE_PATH", d.join("i.redb").display().to_string()),
        ]),
        &["DASH_INSECURE_DEV_MODE", "startup refused"],
    );
    assert_refuses_to_start(
        "retrieval",
        env(&[
            ("DASH_RETRIEVAL_BIND", format!("127.0.0.1:{}", free_port())),
            ("DASH_RETRIEVAL_PERSISTENCE_DISABLE", "1".into()),
        ]),
        &["DASH_INSECURE_DEV_MODE", "startup refused"],
    );
    assert_refuses_to_start(
        "control-plane",
        env(&[("DASH_CONTROL_PLANE_BIND", format!("127.0.0.1:{}", free_port()))]),
        &["DASH_CONTROL_PLANE_TOKEN", "refusing to start"],
    );
}

#[test]
fn weak_or_placeholder_secrets_are_rejected_at_startup() {
    let dir = tempfile::tempdir().unwrap();
    assert_refuses_to_start(
        "retrieval",
        env(&[
            ("DASH_RETRIEVAL_BIND", format!("127.0.0.1:{}", free_port())),
            ("DASH_RETRIEVAL_PERSISTENCE_DISABLE", "1".into()),
            ("DASH_RETRIEVAL_API_KEY", "change-me".into()),
        ]),
        &["placeholder", "too short", "startup refused", "secret"],
    );
    let _ = dir;
}

#[test]
fn api_keys_are_enforced_on_every_route_of_both_services() {
    let mut s = Stack::new(StackOpts::default());
    s.start_all();
    let a_ing = s.ik("tenant-a").1;
    let a_ret = s.rk("tenant-a").1;
    let ic = s.ic();
    let rc = s.rc();
    let body_a = bundle("tenant-a", "c1", "alpha beta gamma", 1);
    let body_b = bundle("tenant-b", "c2", "alpha beta gamma", 1);
    let q = |t: &str| json!({"tenant_id": t, "query": "alpha", "top_k": 5});

    // Anonymous: 401 everywhere that is not a probe.
    assert_eq!(ic.post_json("/v1/ingest", &[], &body_a).status, 401, "anonymous ingest");
    assert_eq!(
        ic.post_json("/v1/ingest/batch", &[], &json!({"items":[body_a.clone()]})).status,
        401,
        "anonymous batch ingest"
    );
    assert_eq!(ic.get("/metrics", &[]).status, 401, "ingestion /metrics anonymous");
    assert_eq!(ic.get("/debug/placement", &[]).status, 401, "ingestion /debug/placement anonymous");
    assert_eq!(rc.post_json("/v1/retrieve", &[], &q("tenant-a")).status, 401, "anonymous retrieve");
    assert_eq!(
        rc.get("/v1/retrieve?tenant_id=tenant-a&query=alpha", &[]).status,
        401,
        "anonymous GET retrieve"
    );
    assert_eq!(
        rc.post_json("/v1/embeddings", &[], &json!({"input": "hello", "model": "x"})).status,
        401,
        "anonymous embeddings"
    );
    assert_eq!(rc.get("/metrics", &[]).status, 401, "retrieval /metrics anonymous");
    assert_eq!(rc.get("/debug/placement", &[]).status, 401, "retrieval /debug/placement anonymous");
    assert_eq!(
        rc.get("/debug/planner?tenant_id=tenant-a&query=alpha", &[]).status,
        401,
        "retrieval /debug/planner anonymous"
    );
    assert_eq!(
        rc.get("/debug/storage-visibility?tenant_id=tenant-a&query=alpha", &[]).status,
        401,
        "retrieval /debug/storage-visibility anonymous"
    );
    // Garbage credentials are also 401, including a JWT-shaped bearer token.
    assert_eq!(
        ic.post_json("/v1/ingest", &[("x-api-key", "not-a-real-key")], &body_a).status,
        401
    );
    assert_eq!(
        ic.post_json("/v1/ingest", &[("Authorization", "Bearer aaa.bbb.ccc")], &body_a).status,
        401
    );

    // Wrong tenant: the key is valid but scoped elsewhere.
    assert_eq!(
        ic.post_json("/v1/ingest", &[("x-api-key", &a_ing)], &body_b).status,
        403,
        "tenant-a ingest key writing tenant-b"
    );
    assert_eq!(
        rc.post_json("/v1/retrieve", &[("x-api-key", &a_ret)], &q("tenant-b")).status,
        403,
        "tenant-a retrieve key reading tenant-b"
    );
    // Wrong role: a retrieve key cannot ingest and an ingest key cannot retrieve.
    assert_eq!(
        ic.post_json("/v1/ingest", &[("x-api-key", &a_ret)], &body_a).status,
        401,
        "a retrieval key is not a credential of the ingestion service"
    );

    // Right keys.
    let r = ic.post_json("/v1/ingest", &[("x-api-key", &a_ing)], &body_a);
    assert_eq!(r.status, 200, "ingest with right key: {}", r.body);
    s.wait_claim_visible("tenant-a", "alpha", "c1", Duration::from_secs(15));
    assert_eq!(rc.post_json("/v1/retrieve", &[("x-api-key", &a_ret)], &q("tenant-a")).status, 200);
    assert_eq!(
        rc.post_json(
            "/v1/embeddings",
            &[("x-api-key", &a_ret)],
            &json!({"input": "hello", "model": "x"})
        )
        .status,
        200,
        "embeddings with a retrieve key"
    );
    assert_eq!(rc.get("/metrics", &[("x-api-key", &a_ret)]).status, 200);
    assert_eq!(ic.get("/metrics", &[("x-api-key", &a_ing)]).status, 200);
    assert_eq!(rc.get("/debug/placement", &[("x-api-key", &a_ret)]).status, 200);
    // The same key also works as a bearer token.
    assert_eq!(
        rc.post_json("/v1/retrieve", &[("Authorization", &format!("Bearer {a_ret}"))], &q("tenant-a")).status,
        200
    );

    // Probes stay open.
    for c in [&ic, &rc] {
        for p in ["/live", "/health", "/ready", "/v1/live", "/v1/health", "/v1/ready"] {
            assert_eq!(c.get(p, &[]).status, 200, "{p} should be open");
        }
    }
}

#[test]
fn jwt_only_configuration_rejects_requests_without_authorization() {
    let dir = tempfile::tempdir().unwrap();
    let secret = random_secret();
    let (_pi, ia) = {
        let port = free_port();
        start(
            dir.path(),
            "ingestion",
            port,
            "/live",
            env(&[
                ("DASH_INGEST_BIND", format!("127.0.0.1:{port}")),
                ("DASH_INGEST_WAL_PATH", dir.path().join("i.wal").display().to_string()),
                ("DASH_INGEST_PERSISTENCE_PATH", dir.path().join("i.redb").display().to_string()),
                ("DASH_INGEST_JWT_HS256_SECRET", secret.clone()),
                ("DASH_INGEST_JWT_DEFAULT_ROLES", "ingest,read_only".into()),
                ("DASH_INGEST_REPLICATION_TOKEN", random_secret()),
            ]),
        )
    };
    let (_pr, ra) = {
        let port = free_port();
        start(
            dir.path(),
            "retrieval",
            port,
            "/live",
            env(&[
                ("DASH_RETRIEVAL_BIND", format!("127.0.0.1:{port}")),
                ("DASH_RETRIEVAL_PERSISTENCE_DISABLE", "1".into()),
                ("DASH_RETRIEVAL_JWT_HS256_SECRET", secret.clone()),
                ("DASH_RETRIEVAL_JWT_DEFAULT_ROLES", "retrieve,read_only".into()),
            ]),
        )
    };
    let (ic, rc) = (Client::new(ia), Client::new(ra));
    let q = json!({"tenant_id": "tenant-a", "query": "alpha", "top_k": 5});
    let body = bundle("tenant-a", "j1", "alpha beta", 1);

    // The old bypass: JWT-only config and no Authorization header at all.
    assert_eq!(rc.post_json("/v1/retrieve", &[], &q).status, 401, "retrieve without Authorization");
    assert_eq!(ic.post_json("/v1/ingest", &[], &body).status, 401, "ingest without Authorization");
    assert_eq!(rc.post_json("/v1/embeddings", &[], &json!({"input":"x"})).status, 401);
    assert_eq!(rc.get("/metrics", &[]).status, 401);
    assert_eq!(rc.get("/debug/placement", &[]).status, 401);
    // Malformed / forged tokens.
    assert_eq!(rc.post_json("/v1/retrieve", &[("Authorization", "Bearer not.a.jwt")], &q).status, 401);
    let forged = mint_hs256(&random_secret(), &json!({"tenant_id":"tenant-a","exp": now_unix()+300}));
    assert_eq!(
        rc.post_json("/v1/retrieve", &[("Authorization", &format!("Bearer {forged}"))], &q).status,
        401,
        "token signed with the wrong secret"
    );
    let expired = mint_hs256(&secret, &json!({"tenant_id":"tenant-a","exp": now_unix()-3600,"iat": now_unix()-7200}));
    assert_eq!(
        rc.post_json("/v1/retrieve", &[("Authorization", &format!("Bearer {expired}"))], &q).status,
        401,
        "expired token"
    );
    // Valid token for the right tenant works; for another tenant it is 403.
    let good = mint_hs256(&secret, &json!({"tenant_id":"tenant-a","exp": now_unix()+300,"iat": now_unix()}));
    let bearer = format!("Bearer {good}");
    assert_eq!(rc.post_json("/v1/retrieve", &[("Authorization", &bearer)], &q).status, 200);
    assert_eq!(
        rc.post_json(
            "/v1/retrieve",
            &[("Authorization", &bearer)],
            &json!({"tenant_id": "tenant-b", "query": "alpha", "top_k": 5})
        )
        .status,
        403,
        "token for tenant-a used on tenant-b"
    );
    assert_eq!(ic.post_json("/v1/ingest", &[("Authorization", &bearer)], &body).status, 200);
    // An API key header is not accepted when only JWT is configured.
    assert_eq!(rc.post_json("/v1/retrieve", &[("x-api-key", "whatever")], &q).status, 401);
}

#[test]
fn rate_limit_returns_429_with_retry_after() {
    let dir = tempfile::tempdir().unwrap();
    let key = random_secret();
    let port = free_port();
    let (_p, ra) = start(
        dir.path(),
        "retrieval",
        port,
        "/live",
        env(&[
            ("DASH_RETRIEVAL_BIND", format!("127.0.0.1:{port}")),
            ("DASH_RETRIEVAL_PERSISTENCE_DISABLE", "1".into()),
            ("DASH_RETRIEVAL_API_KEY_SCOPES", format!("{key}:tenant-a:retrieve")),
            ("DASH_RETRIEVAL_RATE_LIMIT_PER_TENANT_RPS", "1".into()),
            ("DASH_RETRIEVAL_RATE_LIMIT_BURST", "3".into()),
        ]),
    );
    let rc = Client::new(ra);
    let q = json!({"tenant_id": "tenant-a", "query": "alpha", "top_k": 1});
    let mut statuses = vec![];
    let mut retry_after = None;
    for _ in 0..25 {
        let r = rc.post_json("/v1/retrieve", &[("x-api-key", &key)], &q);
        if r.status == 429 && retry_after.is_none() {
            retry_after = r.header("retry-after").map(str::to_string);
        }
        statuses.push(r.status);
    }
    assert!(statuses[0] == 200, "first request inside the burst must pass: {statuses:?}");
    assert!(statuses.contains(&429), "burst of 25 at 1 rps/burst 3 must hit 429: {statuses:?}");
    let ra = retry_after.expect("429 response must carry Retry-After");
    assert!(ra.parse::<u64>().is_ok_and(|n| n >= 1), "Retry-After should be whole seconds >= 1, got {ra:?}");
    // Rate limiting is per tenant and credential check still precedes it.
    assert_eq!(rc.post_json("/v1/retrieve", &[], &q).status, 401);

    // Same on ingestion.
    let ikey = random_secret();
    let iport = free_port();
    let (_pi, ia) = start(
        dir.path(),
        "ingestion",
        iport,
        "/live",
        env(&[
            ("DASH_INGEST_BIND", format!("127.0.0.1:{iport}")),
            ("DASH_INGEST_API_KEY_SCOPES", format!("{ikey}:tenant-a:ingest")),
            ("DASH_INGEST_RATE_LIMIT_PER_TENANT_RPS", "1".into()),
            ("DASH_INGEST_RATE_LIMIT_BURST", "3".into()),
        ]),
    );
    let ic = Client::new(ia);
    let mut got429 = None;
    let mut ok = 0;
    for i in 0..25 {
        let r = ic.post_json("/v1/ingest", &[("x-api-key", &ikey)], &bundle("tenant-a", &format!("r{i}"), "rate limited", 1));
        match r.status {
            200 => ok += 1,
            429 => {
                got429.get_or_insert_with(|| r.header("retry-after").map(str::to_string));
            }
            other => panic!("unexpected status {other}: {}", r.body),
        }
    }
    assert!(ok >= 1, "no ingest passed the burst");
    let retry = got429.expect("ingestion never rate limited");
    assert!(retry.is_some_and(|v| v.parse::<u64>().is_ok()), "ingestion 429 lacks Retry-After");
}

#[test]
fn replication_endpoints_require_the_token() {
    let mut s = Stack::new(StackOpts::default());
    s.start_ingest();
    let ic = s.ic();
    let tok = s.replication_token.clone();
    s.ingest_as("tenant-a", &bundle("tenant-a", "r1", "replicate me", 1));
    let ik = s.ik("tenant-a").1;

    for path in [
        "/internal/replication/wal?from_offset=0&max_records=10",
        "/internal/replication/export",
        "/internal/replication/commit-status?commit_id=x",
    ] {
        assert_eq!(ic.get(path, &[]).status, 403, "{path} without token");
        assert_eq!(ic.get(path, &[("x-replication-token", "wrong")]).status, 403, "{path} wrong token");
        // An ingest/admin API key is not a replication credential.
        assert_eq!(ic.get(path, &[("x-api-key", &ik)]).status, 403, "{path} with API key only");
    }
    assert_eq!(
        ic.request("POST", "/internal/replication/ack?commit_id=x&replica_id=y", &[], Some(b"")).unwrap().status,
        403,
        "ack without token"
    );
    let r = ic.get("/internal/replication/wal?from_offset=0&max_records=10", &[("x-replication-token", &tok)]);
    assert_eq!(r.status, 200, "wal with token: {}", r.body);
    assert!(!r.body.is_empty());
    let r = ic.get("/internal/replication/export", &[("x-replication-token", &tok)]);
    assert_eq!(r.status, 200, "export with token: {}", r.body);
    // The token is never echoed back.
    assert!(!r.body.contains(&tok));

    // No token configured at all (API keys only): endpoints stay closed.
    let mut s2 = Stack::new(StackOpts {
        extra_ingest_env: vec![("DASH_INGEST_REPLICATION_TOKEN".into(), String::new())],
        ..Default::default()
    });
    s2.start_ingest();
    let r = s2.ic().get("/internal/replication/wal?from_offset=0", &[]);
    assert_eq!(r.status, 403, "replication must stay closed when no token is configured: {}", r.body);
    let r = s2.ic().get("/internal/replication/export", &[("x-replication-token", "")]);
    assert_eq!(r.status, 403, "empty token must not match an unset token");
}
