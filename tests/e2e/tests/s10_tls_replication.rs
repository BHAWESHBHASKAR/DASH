//! Scenario 10: native TLS. Ingestion serves HTTPS with client-certificate
//! verification and requires a verified certificate on the replication
//! routes; the retrieval follower pulls over mutual TLS (`https://` source,
//! private CA, client certificate) and serves what it replicated. Plaintext
//! to the TLS port and replication without a client certificate are refused,
//! and a half-configured listener refuses to start.

use std::path::Path;
use std::time::Duration;

use dash_e2e::*;
use dash_tls_fixtures::TestPki;

fn s(value: &Path) -> String {
    value.display().to_string()
}

fn tls_stack(pki: &TestPki) -> Stack {
    let mut stack = Stack::new(StackOpts::default());
    let source = format!("https://{}", stack.ingest_addr());
    stack.opts.extra_ingest_env = vec![
        ("DASH_INGEST_TLS_CERT_FILE".into(), s(&pki.server_cert)),
        ("DASH_INGEST_TLS_KEY_FILE".into(), s(&pki.server_key)),
        ("DASH_INGEST_TLS_CLIENT_CA_FILE".into(), s(&pki.client_ca)),
        (
            "DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT".into(),
            "1".into(),
        ),
    ];
    stack.opts.extra_retrieval_env = vec![
        ("DASH_RETRIEVAL_REPLICATION_SOURCE_URL".into(), source),
        ("DASH_REPLICATION_CA_FILE".into(), s(&pki.server_ca)),
        (
            "DASH_REPLICATION_CLIENT_CERT_FILE".into(),
            s(&pki.client_cert),
        ),
        (
            "DASH_REPLICATION_CLIENT_KEY_FILE".into(),
            s(&pki.client_key),
        ),
    ];
    stack
}

fn start_tls_ingest(stack: &mut Stack, client: &TlsClient) {
    let env = stack.ingest_env();
    let mut p = Proc::spawn(
        "ingestion",
        "ingestion",
        &[],
        &env,
        &stack.path("ingestion.log"),
    );
    wait_live_tls(&mut p, client, "/live", Duration::from_secs(30));
    stack.ingest = Some(p);
}

#[test]
fn replication_runs_over_mutual_tls_end_to_end() {
    let pki = TestPki::new();
    let mut stack = tls_stack(&pki);
    // Public API clients need no certificate; the follower identity does.
    let anonymous = TlsClient::new(stack.ingest_addr(), &pki.server_ca, None);
    let follower = TlsClient::new(
        stack.ingest_addr(),
        &pki.server_ca,
        Some((pki.client_cert.as_path(), pki.client_key.as_path())),
    );
    start_tls_ingest(&mut stack, &anonymous);
    stack.start_retrieval();

    // Ingest over HTTPS, read it back from the follower.
    let (k, v) = stack.ik("tenant-a");
    let r = anonymous.post_json(
        "/v1/ingest",
        &[(k, v.as_str())],
        &bundle("tenant-a", "tls-claim", "encrypted replication works", 2),
    );
    assert_eq!(r.status, 200, "ingest over TLS: {}", r.body);
    let hit = stack.wait_claim_visible(
        "tenant-a",
        "encrypted replication",
        "tls-claim",
        Duration::from_secs(30),
    );
    assert_eq!(hit["citations"].as_array().map(Vec::len), Some(2), "{hit}");
    let ready = stack.rc().get("/ready", &[]);
    assert_eq!(ready.status, 200, "{}", ready.body);

    // Replication needs the token *and* a verified client certificate.
    let token = stack.replication_token.clone();
    let wal = "/internal/replication/wal?from_offset=0&max_records=1";
    let with_cert = follower.get(wal, &[("x-replication-token", token.as_str())]);
    assert_eq!(with_cert.status, 200, "{}", with_cert.body);
    let without_cert = anonymous.get(wal, &[("x-replication-token", token.as_str())]);
    assert_eq!(without_cert.status, 403, "{}", without_cert.body);
    // A forged fingerprint header does not help.
    let forged = anonymous.get(
        wal,
        &[
            ("x-replication-token", token.as_str()),
            ("x-dash-verified-client-cert-sha256", &"a".repeat(64)),
        ],
    );
    assert_eq!(forged.status, 403, "{}", forged.body);
    let no_token = follower.get(wal, &[]);
    assert_eq!(no_token.status, 403, "{}", no_token.body);

    // A plaintext client on the TLS port gets a clean 400.
    let plain = stack
        .ic()
        .raw(b"GET /live HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .expect("plaintext exchange")
        .expect("plaintext client gets an answer");
    assert_eq!(plain.status, 400, "{}", plain.body);
    assert!(plain.body.contains("requires TLS"), "{}", plain.body);

    // A client certificate from an untrusted CA fails the handshake.
    let rogue = TlsClient::new(
        stack.ingest_addr(),
        &pki.server_ca,
        Some((pki.rogue_cert.as_path(), pki.rogue_key.as_path())),
    );
    assert!(rogue.request("GET", "/live", &[], None).is_err());
}

#[test]
fn half_configured_tls_refuses_to_start() {
    let pki = TestPki::new();
    let stack = Stack::new(StackOpts::default());
    let mut env = stack.ingest_env();
    env.push(("DASH_INGEST_TLS_CERT_FILE".into(), s(&pki.server_cert)));
    let mut p = Proc::spawn(
        "ingestion",
        "ingestion",
        &[],
        &env,
        &stack.path("ingestion.log"),
    );
    let code = p
        .wait_exit(Duration::from_secs(20))
        .and_then(|st| st.code());
    assert_eq!(code, Some(2), "{}", p.log());
    assert!(p.log().contains("DASH_INGEST_TLS_KEY_FILE"), "{}", p.log());

    // A follower whose client key is missing refuses to start too.
    let mut env = stack.ingest_env();
    env.push((
        "DASH_INGEST_REPLICATION_SOURCE_URL".into(),
        "https://127.0.0.1:1".into(),
    ));
    env.push((
        "DASH_REPLICATION_CLIENT_CERT_FILE".into(),
        s(&pki.client_cert),
    ));
    let mut p = Proc::spawn(
        "ingestion-follower",
        "ingestion",
        &[],
        &env,
        &stack.path("follower.log"),
    );
    let code = p
        .wait_exit(Duration::from_secs(20))
        .and_then(|st| st.code());
    assert_eq!(code, Some(2), "{}", p.log());
    assert!(
        p.log().contains("DASH_REPLICATION_CLIENT_KEY_FILE"),
        "{}",
        p.log()
    );
}
