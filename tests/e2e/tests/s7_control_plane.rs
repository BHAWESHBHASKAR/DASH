//! Scenario 7: the control plane binary: bearer-token auth, clients that
//! never half-close, and lease-based leader election between two processes.

use std::io::Write;
use std::net::SocketAddr;
use std::path::Path;
use std::time::{Duration, Instant};

use dash_e2e::http;
use dash_e2e::*;
use serde_json::Value;

const CSV: &str = "tenant-a,1,8,node-a,leader,healthy\ntenant-a,1,8,node-b,follower,healthy\n";

struct Cp {
    proc: Proc,
    addr: SocketAddr,
}

impl Cp {
    fn client(&self) -> Client {
        Client::new(self.addr)
    }
    fn get(&self, path: &str, token: &str) -> Resp {
        self.client()
            .get(path, &[("Authorization", &format!("Bearer {token}"))])
    }
    fn leader(&self, token: &str) -> Option<Value> {
        let r = self.get("/v1/control-plane/leader", token);
        (r.status == 200).then(|| r.json())
    }
    fn is_leader(&self, token: &str) -> bool {
        self.leader(token).is_some_and(|v| v["is_leader"] == true)
    }
}

fn start(dir: &Path, node: &str, token: &str, extra: &[(&str, String)]) -> Cp {
    let port = free_port();
    let addr: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let mut env: Vec<(String, String)> = vec![
        ("DASH_CONTROL_PLANE_BIND".into(), addr.to_string()),
        ("DASH_CONTROL_PLANE_TOKEN".into(), token.to_string()),
        ("DASH_CONTROL_PLANE_NODE_ID".into(), node.to_string()),
    ];
    env.extend(extra.iter().map(|(k, v)| (k.to_string(), v.clone())));
    let mut proc = Proc::spawn(
        node,
        "control-plane",
        &[],
        &env,
        &dir.join(format!("{node}.log")),
    );
    proc.wait_live(addr, "/v1/control-plane/health", Duration::from_secs(20));
    Cp { proc, addr }
}

fn lease_env(dir: &Path) -> Vec<(&'static str, String)> {
    vec![
        (
            "DASH_CONTROL_PLANE_LEASE_PATH",
            dir.join("leader.lease").display().to_string(),
        ),
        ("DASH_CONTROL_PLANE_LEASE_DURATION_MS", "3000".into()),
        ("DASH_CONTROL_PLANE_LEASE_RENEWAL_MS", "500".into()),
        ("DASH_CONTROL_PLANE_LEASE_SAFETY_MARGIN_MS", "500".into()),
        (
            "DASH_CONTROL_PLANE_STATE_PATH",
            dir.join("placement.csv").display().to_string(),
        ),
    ]
}

#[test]
fn repeated_bad_tokens_are_throttled() {
    let dir = tempfile::tempdir().unwrap();
    let token = random_secret();
    let cp = start(
        dir.path(),
        "throttle",
        &token,
        &[("DASH_CONTROL_PLANE_REQUEST_DEADLINE_MS", "3000".into())],
    );
    let c = cp.client();
    let bad = [("Authorization", "Bearer guess-guess-guess")];

    // Ten wrong tokens are answered 401, the eleventh is throttled.
    for n in 1..=10 {
        let r = c.get("/v1/control-plane/leader", &bad);
        assert_eq!(r.status, 401, "wrong token #{n}: {}", r.body);
    }
    let r = c.get("/v1/control-plane/leader", &bad);
    assert_eq!(r.status, 429, "eleventh wrong token: {}", r.body);
    let retry: u64 = r
        .header("retry-after")
        .and_then(|v| v.parse().ok())
        .expect("429 carries a numeric Retry-After");
    assert!(
        (1..=60).contains(&retry),
        "Retry-After within the window: {retry}"
    );

    // Open probes are not affected by the throttle.
    assert_eq!(c.get("/v1/control-plane/health", &[]).status, 200);
    assert_eq!(c.get("/health", &[]).status, 200);
}

#[test]
fn requests_need_the_bearer_token() {
    let dir = tempfile::tempdir().unwrap();
    let token = random_secret();
    let cp = start(
        dir.path(),
        "solo",
        &token,
        &[("DASH_CONTROL_PLANE_REQUEST_DEADLINE_MS", "3000".into())],
    );
    let c = cp.client();

    // The service throttles repeated bad credentials per peer (10 per minute),
    // so this loop stays under that budget: a request with no credential is
    // not counted, a wrong token is (6), and the wrong-scheme probe runs on
    // the first three routes only (3), for 9 counted failures. The throttle
    // itself is covered by `repeated_bad_tokens_are_throttled`.
    for (i, (m, p)) in [
        ("GET", "/v1/control-plane/leader"),
        ("GET", "/v1/control-plane/placement"),
        ("PUT", "/v1/control-plane/placement"),
        ("POST", "/v1/control-plane/leader/acquire"),
        ("POST", "/v1/control-plane/failover/promote"),
        ("POST", "/v1/control-plane/replica-lag"),
    ]
    .into_iter()
    .enumerate()
    {
        let r = c.request(m, p, &[], Some(CSV.as_bytes())).unwrap();
        assert_eq!(r.status, 401, "{m} {p} without a token: {}", r.body);
        let r = c
            .request(
                m,
                p,
                &[("Authorization", "Bearer wrong-token")],
                Some(CSV.as_bytes()),
            )
            .unwrap();
        assert_eq!(r.status, 401, "{m} {p} with a wrong token");
        if i < 3 {
            let r = c
                .request(
                    m,
                    p,
                    &[("Authorization", &format!("Basic {token}"))],
                    Some(CSV.as_bytes()),
                )
                .unwrap();
            assert_eq!(
                r.status, 401,
                "{m} {p} with the right secret under the wrong scheme"
            );
        }
    }
    // Probes are open.
    assert_eq!(c.get("/v1/control-plane/health", &[]).status, 200);
    assert_eq!(c.get("/health", &[]).status, 200);
    assert_eq!(c.get("/v1/control-plane/ready", &[]).status, 200);

    // With the token the API works end to end.
    let auth = format!("Bearer {token}");
    let h = [("Authorization", auth.as_str())];
    let r = c
        .request(
            "PUT",
            "/v1/control-plane/placement",
            &h,
            Some(CSV.as_bytes()),
        )
        .unwrap();
    assert_eq!(r.status, 200, "PUT placement: {}", r.body);
    let r = c.get("/v1/control-plane/placement", &h);
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(
        r.body.contains("tenant-a") && r.body.contains("node-b"),
        "placement body: {}",
        r.body
    );
    let r = c.get("/v1/control-plane/placement?format=csv", &h);
    assert_eq!(r.status, 200);
    assert!(
        r.body.contains("tenant-a,1,8,node-a,leader,healthy"),
        "csv: {}",
        r.body
    );
    // Epoch regressions are refused.
    let stale = "tenant-a,1,3,node-a,leader,healthy\n";
    let r = c
        .request(
            "PUT",
            "/v1/control-plane/placement",
            &h,
            Some(stale.as_bytes()),
        )
        .unwrap();
    assert_eq!(r.status, 409, "epoch regression must conflict: {}", r.body);
    assert!(!r.body.contains(&token));

    // Malformed bodies are client errors, not crashes.
    let r = c
        .request(
            "PUT",
            "/v1/control-plane/placement",
            &h,
            Some(b"not,a,placement"),
        )
        .unwrap();
    assert!((400..500).contains(&r.status), "bad CSV -> {}", r.status);
    let r = c
        .request(
            "PUT",
            "/v1/control-plane/placement",
            &h,
            Some(&[0xff, 0xfe, 0xfd]),
        )
        .unwrap();
    assert!(
        (400..500).contains(&r.status),
        "non UTF-8 body -> {}",
        r.status
    );
}

#[test]
fn client_that_never_half_closes_gets_an_answer() {
    let dir = tempfile::tempdir().unwrap();
    let token = random_secret();
    let mut cp = start(
        dir.path(),
        "solo",
        &token,
        &[("DASH_CONTROL_PLANE_REQUEST_DEADLINE_MS", "3000".into())],
    );
    let c = cp.client();

    // A curl-like exchange: write the request, keep the socket open, read.
    // (The old reader waited for EOF and never answered.) Keep-alive, no
    // Connection: close, no shutdown(Write).
    let mut s = c.connect().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(
        format!("GET /v1/control-plane/leader HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\nAccept: */*\r\n\r\n").as_bytes(),
    )
    .unwrap();
    let started = Instant::now();
    let r = http::read_response(&mut s).expect("no response while the client kept the socket open");
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "answer took {:?}",
        started.elapsed()
    );
    assert!(r.status == 200 || r.status == 503, "status {}", r.status);
    drop(s);

    // Same with a body, split across writes (headers, pause, body).
    let mut s = c.connect().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(
        format!(
            "PUT /v1/control-plane/placement HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\nContent-Length: {}\r\n\r\n",
            CSV.len()
        )
        .as_bytes(),
    )
    .unwrap();
    std::thread::sleep(Duration::from_millis(300));
    s.write_all(CSV.as_bytes()).unwrap();
    let r = http::read_response(&mut s).expect("no response to a body split across writes");
    assert_eq!(r.status, 200, "{}", r.body);

    // A client that sends a partial request and goes silent is dropped at the
    // deadline instead of pinning a worker forever.
    let mut s = c.connect().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(12))).unwrap();
    s.write_all(b"GET /v1/control-plane/leader HTTP/1.1\r\nHost: x\r\n")
        .unwrap();
    let started = Instant::now();
    let _ = http::read_optional(&mut s);
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "silent client held a worker for {:?}",
        started.elapsed()
    );

    // Oversized declared body and garbage are rejected, process survives.
    let r = c
        .raw(format!("PUT /v1/control-plane/placement HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {token}\r\nContent-Length: 999999999\r\n\r\n").as_bytes())
        .unwrap();
    if let Some(r) = r {
        assert!(
            (400..500).contains(&r.status),
            "oversized body -> {}",
            r.status
        );
    }
    let _ = c.raw(b"\x00\x01\x02 garbage \r\n\r\n");
    assert!(
        cp.proc.is_alive(),
        "control plane died on hostile input\n{}",
        cp.proc.log()
    );
    assert_eq!(cp.get("/v1/control-plane/leader", &token).status, 200);
}

#[test]
fn two_processes_one_lease_exactly_one_leader_and_failover_bumps_the_epoch() {
    let dir = tempfile::tempdir().unwrap();
    let token = random_secret();
    let env = lease_env(dir.path());
    let mut a = start(dir.path(), "cp-a", &token, &env);
    let b = start(dir.path(), "cp-b", &token, &env);

    // Wait for a leader to appear.
    let end = Instant::now() + Duration::from_secs(15);
    while !(a.is_leader(&token) || b.is_leader(&token)) {
        assert!(
            Instant::now() < end,
            "no leader elected\nA:{}\nB:{}",
            a.proc.log(),
            b.proc.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        a.is_leader(&token),
        "the first process should hold the lease\nA:{}\nB:{}",
        a.proc.log(),
        b.proc.log()
    );
    let epoch_a = a.leader(&token).unwrap()["epoch"].as_u64().unwrap();
    assert!(epoch_a >= 1);

    // Observe through several lease renewal periods: never two leaders, the
    // leader never flaps, and the follower agrees on who leads.
    for _ in 0..50 {
        let (la, lb) = (a.is_leader(&token), b.is_leader(&token));
        assert!(!(la && lb), "split brain: both processes report leadership");
        assert!(la, "leadership moved away from a healthy, renewing leader");
        let seen_by_b = b.leader(&token).expect("follower should know the leader");
        assert_eq!(seen_by_b["leader_node_id"], "cp-a");
        assert_eq!(
            seen_by_b["epoch"].as_u64().unwrap(),
            epoch_a,
            "epoch changed without a failover"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    // Readiness reflects the role.
    assert_eq!(a.get("/v1/control-plane/ready", &token).status, 200);
    assert_eq!(b.get("/v1/control-plane/ready", &token).status, 503);

    // Only the leader accepts placement writes; followers refuse.
    let auth = format!("Bearer {token}");
    let h = [("Authorization", auth.as_str())];
    let r = b
        .client()
        .request(
            "PUT",
            "/v1/control-plane/placement",
            &h,
            Some(CSV.as_bytes()),
        )
        .unwrap();
    assert!(
        r.status != 200,
        "follower accepted a placement write: {}",
        r.body
    );
    let r = a
        .client()
        .request(
            "PUT",
            "/v1/control-plane/placement",
            &h,
            Some(CSV.as_bytes()),
        )
        .unwrap();
    assert_eq!(
        r.status, 200,
        "leader refused a placement write: {}",
        r.body
    );

    // Kill the leader abruptly; the other node must take over with a higher epoch.
    a.proc.kill9();
    let started = Instant::now();
    let takeover = loop {
        if let Some(l) = b.leader(&token)
            && l["is_leader"] == true
        {
            break l;
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "follower never took over after the leader was killed\nB:{}",
            b.proc.log()
        );
        std::thread::sleep(Duration::from_millis(100));
    };
    let epoch_b = takeover["epoch"].as_u64().unwrap();
    assert!(
        epoch_b > epoch_a,
        "fencing epoch must increase on takeover: {epoch_a} -> {epoch_b}"
    );
    assert_eq!(takeover["leader_node_id"], "cp-b");
    println!(
        "failover took {:?}, epoch {epoch_a} -> {epoch_b}",
        started.elapsed()
    );
    assert_eq!(b.get("/v1/control-plane/ready", &token).status, 200);
    // The new leader serves the placement the old leader persisted.
    let r = b.get("/v1/control-plane/placement", &token);
    assert_eq!(r.status, 200, "{}", r.body);
    assert!(
        r.body.contains("tenant-a") && r.body.contains("node-b"),
        "placement lost in failover: {}",
        r.body
    );

    // The old leader comes back as a follower and must not steal the lease.
    let a2 = start(dir.path(), "cp-a", &token, &env);
    for _ in 0..40 {
        assert!(
            !a2.is_leader(&token),
            "returning node preempted a healthy leader"
        );
        assert!(
            b.is_leader(&token),
            "healthy leader lost the lease to a returning node"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(
        b.leader(&token).unwrap()["epoch"].as_u64().unwrap(),
        epoch_b
    );
}
