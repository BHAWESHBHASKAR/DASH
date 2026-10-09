//! The replication follower client against real sockets: plain http, a local
//! TLS server (certificate from rcgen) and the token-transport rule.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc;
use std::thread::JoinHandle;
use std::time::Duration;

use dash_common::replication_client::{ALLOW_INSECURE_HTTP_ENV, ClientOptions, request};

fn read_head<S: Read>(stream: &mut S) -> String {
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while !buf.ends_with(b"\r\n\r\n") {
        match stream.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            _ => break,
        }
    }
    String::from_utf8_lossy(&buf).to_string()
}

/// Serve one connection with a canned raw response; returns the request head.
fn serve_once_plain(raw_response: Vec<u8>) -> (String, JoinHandle<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let head = read_head(&mut stream);
        let _ = stream.write_all(&raw_response);
        let _ = stream.flush();
        head
    });
    (format!("http://{addr}"), handle)
}

fn http_ok(body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

#[test]
fn plain_http_sends_the_token_and_returns_status_and_body() {
    let (base, server) = serve_once_plain(http_ok("status=ok\n"));
    let response = request(
        "GET",
        &format!("{base}/internal/replication/wal?from_offset=0"),
        Some("tok-123"),
        1024,
        &ClientOptions::default(),
    )
    .unwrap();
    assert_eq!(response.status, 200);
    assert_eq!(response.body, "status=ok\n");
    let head = server.join().unwrap().to_ascii_lowercase();
    assert!(
        head.starts_with("get /internal/replication/wal?from_offset=0 http/1.1"),
        "{head}"
    );
    assert!(head.contains("x-replication-token: tok-123"), "{head}");
}

#[test]
fn post_ack_reaches_the_source_and_error_statuses_keep_their_body() {
    let body = "unknown commit_id";
    let raw = format!(
        "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let (base, server) = serve_once_plain(raw.into_bytes());
    let response = request(
        "POST",
        &format!("{base}/internal/replication/ack?commit_id=c1&replica_id=r1"),
        None,
        1024,
        &ClientOptions::default(),
    )
    .unwrap();
    assert_eq!((response.status, response.body.as_str()), (404, body));
    assert!(
        server
            .join()
            .unwrap()
            .starts_with("POST /internal/replication/ack?")
    );
}

#[test]
fn oversized_and_truncated_responses_are_rejected() {
    let (base, _server) = serve_once_plain(http_ok(&"x".repeat(100)));
    let err = request(
        "GET",
        &format!("{base}/x"),
        None,
        10,
        &ClientOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("exceeds 10 byte limit"), "{err}");

    // No Content-Length: the cap still applies while streaming.
    let raw = format!(
        "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n{}",
        "y".repeat(100)
    );
    let (base, _server) = serve_once_plain(raw.into_bytes());
    let err = request(
        "GET",
        &format!("{base}/x"),
        None,
        10,
        &ClientOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("exceeds 10 byte limit"), "{err}");

    let raw = b"HTTP/1.1 200 OK\r\nContent-Length: 50\r\nConnection: close\r\n\r\nshort".to_vec();
    let (base, _server) = serve_once_plain(raw);
    let err = request(
        "GET",
        &format!("{base}/x"),
        None,
        1024,
        &ClientOptions::default(),
    )
    .unwrap_err();
    assert!(err.contains("truncated"), "{err}");
}

#[test]
fn redirects_are_not_followed_so_the_token_cannot_leave_the_host() {
    let raw = b"HTTP/1.1 302 Found\r\nLocation: http://127.0.0.1:9/steal\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec();
    let (base, _server) = serve_once_plain(raw);
    let response = request(
        "GET",
        &format!("{base}/x"),
        Some("tok"),
        1024,
        &ClientOptions::default(),
    )
    .unwrap();
    assert_eq!(response.status, 302);
}

#[test]
fn token_is_not_sent_over_plain_http_to_a_remote_host_without_the_override() {
    let options = ClientOptions {
        connect_timeout: Duration::from_millis(200),
        request_deadline: Duration::from_millis(500),
        ..ClientOptions::default()
    };
    // TEST-NET-1 (RFC 5737): never routable, so nothing can receive the token.
    let err = request(
        "GET",
        "http://192.0.2.1:8081/x",
        Some("tok"),
        1024,
        &options,
    )
    .unwrap_err();
    assert!(
        err.contains("refusing to send the replication token"),
        "{err}"
    );
    assert!(err.contains(ALLOW_INSECURE_HTTP_ENV), "{err}");

    // With the explicit acknowledgement the request is attempted (and fails
    // on connect rather than on the policy check).
    let relaxed = ClientOptions {
        allow_insecure_http: true,
        ..options.clone()
    };
    let err = request(
        "GET",
        "http://192.0.2.1:8081/x",
        Some("tok"),
        1024,
        &relaxed,
    )
    .unwrap_err();
    assert!(
        err.contains("failed requesting replication source"),
        "{err}"
    );

    // Without a token there is nothing to protect.
    let err = request("GET", "http://192.0.2.1:8081/x", None, 1024, &options).unwrap_err();
    assert!(
        err.contains("failed requesting replication source"),
        "{err}"
    );
}

// ---------------------------------------------------------------------
// TLS
// ---------------------------------------------------------------------

struct TlsSource {
    base: String,
    ca_file: PathBuf,
    _dir: tempfile::TempDir,
    requests: mpsc::Receiver<String>,
    handle: JoinHandle<()>,
}

/// An https server on localhost serving one request, with a freshly
/// generated self-signed certificate written out as the CA bundle.
fn tls_source(body: &'static str) -> TlsSource {
    let certified = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let ca_file = dir.path().join("ca.pem");
    std::fs::write(&ca_file, certified.cert.pem()).unwrap();

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let key =
        rustls::pki_types::PrivateKeyDer::try_from(certified.signing_key.serialize_der()).unwrap();
    let config = rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![certified.cert.der().clone()], key)
        .unwrap();
    let config = Arc::new(config);

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (tx, requests) = mpsc::channel();
    let handle = std::thread::spawn(move || {
        let (tcp, _) = listener.accept().unwrap();
        serve_tls(tcp, config, body, tx);
    });
    TlsSource {
        base: format!("https://localhost:{port}"),
        ca_file,
        _dir: dir,
        requests,
        handle,
    }
}

fn serve_tls(
    tcp: TcpStream,
    config: Arc<rustls::ServerConfig>,
    body: &str,
    tx: mpsc::Sender<String>,
) {
    let conn = rustls::ServerConnection::new(config).unwrap();
    let mut tls = rustls::StreamOwned::new(conn, tcp);
    // A client that rejects the certificate aborts the handshake; the read
    // below then fails and the server just ends.
    let head = read_head(&mut tls);
    let _ = tx.send(head);
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = tls.write_all(response.as_bytes());
    let _ = tls.flush();
    tls.conn.send_close_notify();
    let _ = tls.flush();
}

#[test]
fn https_source_is_fetched_with_the_token_over_tls() {
    let source = tls_source("status=ok\ntls=1\n");
    let options = ClientOptions {
        ca_file: Some(source.ca_file.clone()),
        ..ClientOptions::default()
    };
    let response = request(
        "GET",
        &format!("{}/internal/replication/export", source.base),
        Some("tok-tls"),
        1024,
        &options,
    )
    .expect("https request should succeed against a trusted CA");
    assert_eq!(response.status, 200);
    assert_eq!(response.body, "status=ok\ntls=1\n");
    let head = source
        .requests
        .recv_timeout(Duration::from_secs(5))
        .unwrap()
        .to_ascii_lowercase();
    assert!(head.contains("x-replication-token: tok-tls"), "{head}");
    source.handle.join().unwrap();
}

#[test]
fn https_source_with_an_untrusted_certificate_is_refused() {
    let source = tls_source("never delivered");
    // No CA bundle: the self-signed certificate is not trusted.
    let err = request(
        "GET",
        &format!("{}/x", source.base),
        Some("tok-tls"),
        1024,
        &ClientOptions::default(),
    )
    .unwrap_err();
    assert!(
        err.contains("failed requesting replication source"),
        "{err}"
    );
    // The token must not have reached the server: the handshake failed first.
    source.handle.join().unwrap();
    let seen = source.requests.try_recv().unwrap_or_default();
    assert!(
        !seen.to_ascii_lowercase().contains("x-replication-token"),
        "{seen}"
    );
}
