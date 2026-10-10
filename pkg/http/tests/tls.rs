//! TLS and mutual TLS on the shared server, against real sockets with
//! certificates generated per test (rcgen).

mod common;

use std::{
    io::{Read, Write},
    net::TcpStream,
    path::Path,
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};

use common::*;
use dash_http::{
    Handler, Request, Response, ServerConfig, TlsAcceptor, TlsSettings, default_health_classifier,
    rustls, sha256_hex,
};
use dash_tls_fixtures::{TestPki as Pki, leaf, new_ca};

fn settings(pki: &Pki, client_ca: bool, require: bool) -> TlsSettings {
    TlsSettings {
        cert_file: pki.server_cert.clone(),
        key_file: pki.server_key.clone(),
        client_ca_file: client_ca.then(|| pki.client_ca.clone()),
        require_client_cert: require,
    }
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

fn client_config(ca: &Path, identity: Option<(&Path, &Path)>) -> Arc<rustls::ClientConfig> {
    let mut config = (*dash_http::client_config(Some(ca), identity).unwrap()).clone();
    config.alpn_protocols = vec![b"http/1.1".to_vec()];
    Arc::new(config)
}

struct TlsResponse {
    text: String,
    alpn: Option<Vec<u8>>,
    version: Option<rustls::ProtocolVersion>,
}

/// Send `request` over TLS and read until the server closes.
fn https(
    addr: &str,
    config: Arc<rustls::ClientConfig>,
    request: &[u8],
) -> std::io::Result<TlsResponse> {
    let tcp = TcpStream::connect(addr)?;
    tcp.set_read_timeout(Some(Duration::from_secs(10)))?;
    let name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let conn = rustls::ClientConnection::new(config, name).map_err(std::io::Error::other)?;
    let mut tls = rustls::StreamOwned::new(conn, tcp);
    tls.write_all(request)?;
    tls.flush()?;
    let mut out = Vec::new();
    tls.read_to_end(&mut out)?;
    Ok(TlsResponse {
        text: String::from_utf8_lossy(&out).into_owned(),
        alpn: tls.conn.alpn_protocol().map(<[u8]>::to_vec),
        version: tls.conn.protocol_version(),
    })
}

const GET_WHO: &[u8] = b"GET /who HTTP/1.1\r\nHost: localhost\r\n\r\n";

/// Reports what the handler sees of the TLS session.
fn who(request: Request) -> Response {
    if request.path() == "/health" {
        return Response::json(200, "{\"status\":\"ok\"}".to_string());
    }
    let tls = request.tls.is_some();
    let client = request
        .tls
        .and_then(|info| info.client_cert_sha256)
        .unwrap_or_default();
    Response::json(
        200,
        format!(
            "{{\"tls\":{tls},\"client\":\"{client}\",\"body_len\":{}}}",
            request.body.len()
        ),
    )
}

fn tls_config(settings: TlsSettings) -> ServerConfig {
    let mut config = config();
    config.tls = Some(TlsAcceptor::new(settings).expect("tls settings load"));
    config
}

fn start_tls(config: ServerConfig) -> Harness {
    start_with(
        config,
        Arc::new(who) as Handler,
        default_health_classifier,
        |l| l,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn https_request_round_trip_negotiates_http11_and_tls12_or_newer() {
    let pki = Pki::new();
    let server = start_tls(tls_config(settings(&pki, false, false)));
    let client = client_config(&pki.server_ca, None);

    let response = https(&server.addr, Arc::clone(&client), GET_WHO).unwrap();
    assert!(
        status_line(&response.text).contains("200"),
        "{}",
        response.text
    );
    assert!(response.text.contains("\"tls\":true"), "{}", response.text);
    assert!(
        response.text.contains("\"client\":\"\""),
        "{}",
        response.text
    );
    assert_eq!(response.alpn.as_deref(), Some(&b"http/1.1"[..]));
    assert!(matches!(
        response.version,
        Some(rustls::ProtocolVersion::TLSv1_3 | rustls::ProtocolVersion::TLSv1_2)
    ));

    // A body and the health lane both work over TLS.
    let post = b"POST /who HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\n\r\nhello";
    let response = https(&server.addr, Arc::clone(&client), post).unwrap();
    assert!(
        response.text.contains("\"body_len\":5"),
        "{}",
        response.text
    );
    let health = https(
        &server.addr,
        client,
        b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .unwrap();
    assert!(health.text.contains("\"status\":\"ok\""), "{}", health.text);
}

#[test]
fn tls12_only_client_is_served() {
    let pki = Pki::new();
    let server = start_tls(tls_config(settings(&pki, false, false)));
    let mut roots = rustls::RootCertStore::empty();
    let pem = std::fs::read(&pki.server_ca).unwrap();
    use rustls::pki_types::pem::PemObject;
    for cert in rustls::pki_types::CertificateDer::pem_slice_iter(&pem) {
        roots.add(cert.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS12])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let response = https(&server.addr, Arc::new(config), GET_WHO).unwrap();
    assert!(
        status_line(&response.text).contains("200"),
        "{}",
        response.text
    );
    assert_eq!(response.version, Some(rustls::ProtocolVersion::TLSv1_2));
}

#[test]
fn untrusted_server_certificate_is_refused_by_the_client() {
    let pki = Pki::new();
    let server = start_tls(tls_config(settings(&pki, false, false)));
    // Trusting only the client CA: the server chain does not verify.
    let client = client_config(&pki.client_ca, None);
    let err = https(&server.addr, client, GET_WHO)
        .err()
        .expect("must fail");
    assert!(
        err.to_string().to_ascii_lowercase().contains("certificate"),
        "{err}"
    );
    assert_alive_tls(&server.addr, &pki);
}

fn assert_alive_tls(addr: &str, pki: &Pki) {
    let response = https(addr, client_config(&pki.server_ca, None), GET_WHO).unwrap();
    assert!(
        status_line(&response.text).contains("200"),
        "{}",
        response.text
    );
}

#[test]
fn plaintext_client_on_a_tls_port_gets_a_clean_400() {
    let pki = Pki::new();
    let server = start_tls(tls_config(settings(&pki, false, false)));
    let response = send_raw(
        &server.addr,
        b"GET /who HTTP/1.1\r\nHost: localhost\r\n\r\n",
    );
    assert!(status_line(&response).contains("400"), "{response:?}");
    assert!(response.contains("requires TLS"), "{response:?}");
    assert_eq!(server.hooks.enqueued.load(Ordering::SeqCst), 0);
    assert_alive_tls(&server.addr, &pki);
}

#[test]
fn mtls_required_accepts_a_trusted_client_and_exposes_its_fingerprint() {
    let pki = Pki::new();
    let server = start_tls(tls_config(settings(&pki, true, true)));
    let client = client_config(
        &pki.server_ca,
        Some((pki.client_cert.as_path(), pki.client_key.as_path())),
    );
    let response = https(&server.addr, client, GET_WHO).unwrap();
    assert!(
        status_line(&response.text).contains("200"),
        "{}",
        response.text
    );
    let fingerprint = sha256_hex(&pki.client_der);
    assert!(
        response
            .text
            .contains(&format!("\"client\":\"{fingerprint}\"")),
        "{}",
        response.text
    );
}

#[test]
fn mtls_required_rejects_missing_and_untrusted_client_certificates() {
    let pki = Pki::new();
    let server = start_tls(tls_config(settings(&pki, true, true)));

    let anonymous = https(&server.addr, client_config(&pki.server_ca, None), GET_WHO);
    assert!(
        anonymous.is_err(),
        "client without a certificate must be refused"
    );

    let rogue = https(
        &server.addr,
        client_config(
            &pki.server_ca,
            Some((pki.rogue_cert.as_path(), pki.rogue_key.as_path())),
        ),
        GET_WHO,
    );
    assert!(
        rogue.is_err(),
        "client with an untrusted certificate must be refused"
    );
    // Nothing reached a worker.
    assert_eq!(server.hooks.enqueued.load(Ordering::SeqCst), 0);

    let trusted = https(
        &server.addr,
        client_config(
            &pki.server_ca,
            Some((pki.client_cert.as_path(), pki.client_key.as_path())),
        ),
        GET_WHO,
    )
    .unwrap();
    assert!(
        status_line(&trusted.text).contains("200"),
        "{}",
        trusted.text
    );
}

#[test]
fn optional_client_certs_allow_anonymous_clients_but_still_verify_presented_ones() {
    let pki = Pki::new();
    let server = start_tls(tls_config(settings(&pki, true, false)));

    let anonymous = https(&server.addr, client_config(&pki.server_ca, None), GET_WHO).unwrap();
    assert!(
        anonymous.text.contains("\"client\":\"\""),
        "{}",
        anonymous.text
    );

    let trusted = https(
        &server.addr,
        client_config(
            &pki.server_ca,
            Some((pki.client_cert.as_path(), pki.client_key.as_path())),
        ),
        GET_WHO,
    )
    .unwrap();
    assert!(
        trusted.text.contains(&sha256_hex(&pki.client_der)),
        "{}",
        trusted.text
    );

    let rogue = https(
        &server.addr,
        client_config(
            &pki.server_ca,
            Some((pki.rogue_cert.as_path(), pki.rogue_key.as_path())),
        ),
        GET_WHO,
    );
    assert!(rogue.is_err(), "a presented certificate must still verify");
}

#[test]
fn stalled_handshakes_never_reach_a_worker_and_are_closed_at_the_first_byte_timeout() {
    let pki = Pki::new();
    let mut config = tls_config(settings(&pki, false, false));
    config.workers = 1;
    config.health_workers = 0;
    config.first_byte_timeout = Duration::from_millis(300);
    let server = start_tls(config);

    // Silent sockets and a truncated ClientHello (record header only).
    let mut stalled: Vec<TcpStream> = (0..3).map(|_| connect(&server.addr)).collect();
    let mut partial = connect(&server.addr);
    partial.write_all(&[0x16, 0x03, 0x01, 0x02, 0x00]).unwrap();
    stalled.push(partial);

    // The single worker is free: a real request is served right away.
    let started = Instant::now();
    assert_alive_tls(&server.addr, &pki);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(server.hooks.enqueued.load(Ordering::SeqCst), 1);

    // Every stalled connection is closed by the server, unanswered.
    for mut stream in stalled {
        let mut buf = Vec::new();
        let _ = stream.read_to_end(&mut buf);
        assert!(buf.is_empty(), "stalled connection got bytes: {buf:?}");
    }
    assert_eq!(server.hooks.enqueued.load(Ordering::SeqCst), 1);
}

#[test]
fn per_ip_cap_applies_before_the_handshake() {
    let pki = Pki::new();
    let mut config = tls_config(settings(&pki, false, false));
    config.max_conns_per_ip = 1;
    config.first_byte_timeout = Duration::from_secs(5);
    let server = start_tls(config);

    // Accepted in connect order: the holder takes the only slot (its
    // handshake is pending), the second connection is over the cap and is
    // closed without a TLS answer, long before the first-byte timeout.
    let _holder = connect(&server.addr);
    let mut extra = connect(&server.addr);
    let started = Instant::now();
    let mut buf = Vec::new();
    let _ = extra.read_to_end(&mut buf);
    assert!(buf.is_empty(), "over-cap connection got bytes: {buf:?}");
    assert!(started.elapsed() < Duration::from_secs(4));
    wait_for("per-IP reject counted", || {
        server.hooks.per_ip_rejects.load(Ordering::SeqCst) == 1
    });
}

#[test]
fn certificate_rotation_is_picked_up_without_a_restart() {
    let pki = Pki::new();
    let acceptor = TlsAcceptor::new(settings(&pki, false, false)).unwrap();
    let mut config = config();
    config.tls = Some(acceptor.clone());
    let server = start_tls(config);
    assert_alive_tls(&server.addr, &pki);

    // Rotate to a certificate from a new CA (key written first, then the
    // certificate, as a renewal job would).
    let new_ca = new_ca("rotated CA");
    let rotated_leaf = leaf(&new_ca, &dash_tls_fixtures::SERVER_NAMES, false);
    let new_ca_file = pki.write("rotated-ca.pem", &new_ca.pem());
    std::fs::write(&pki.server_key, rotated_leaf.key_pem).unwrap();
    std::fs::write(&pki.server_cert, rotated_leaf.cert_pem).unwrap();

    let rotated = client_config(&new_ca_file, None);
    wait_for("rotated certificate served", || {
        https(&server.addr, Arc::clone(&rotated), GET_WHO)
            .is_ok_and(|r| status_line(&r.text).contains("200"))
    });
    let old = https(&server.addr, client_config(&pki.server_ca, None), GET_WHO);
    assert!(old.is_err(), "the old chain must no longer be served");
    // Nothing changed since: an explicit reload is a no-op.
    assert_eq!(acceptor.reload(), Ok(false));
}

#[test]
fn broken_rotation_keeps_serving_the_previous_certificate() {
    let pki = Pki::new();
    let acceptor = TlsAcceptor::new(settings(&pki, false, false)).unwrap();
    let mut config = config();
    config.tls = Some(acceptor.clone());
    let server = start_tls(config);

    std::fs::write(
        &pki.server_cert,
        "-----BEGIN CERTIFICATE-----\nnot base64\n",
    )
    .unwrap();
    let err = acceptor.reload().unwrap_err();
    assert!(err.contains("server.pem"), "{err}");
    assert_alive_tls(&server.addr, &pki);
}

#[test]
fn mismatched_key_and_bad_ca_are_refused_at_load() {
    let pki = Pki::new();
    let mut mismatched = settings(&pki, false, false);
    mismatched.key_file = pki.client_key.clone();
    let err = TlsAcceptor::new(mismatched).unwrap_err();
    assert!(err.contains("unusable"), "{err}");

    let mut empty_ca = settings(&pki, true, true);
    empty_ca.client_ca_file = Some(pki.write("empty.pem", ""));
    let err = TlsAcceptor::new(empty_ca).unwrap_err();
    assert!(err.contains("no certificates"), "{err}");
}

#[test]
fn serve_once_speaks_tls_too() {
    let pki = Pki::new();
    let config = tls_config(settings(&pki, true, true));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let handle = std::thread::spawn(move || {
        let handler: Handler = Arc::new(who);
        dash_http::serve_once(&listener, &config, &handler, &dash_http::NoHooks)
    });
    let client = client_config(
        &pki.server_ca,
        Some((pki.client_cert.as_path(), pki.client_key.as_path())),
    );
    let response = https(&addr, client, GET_WHO).unwrap();
    assert!(
        response.text.contains(&sha256_hex(&pki.client_der)),
        "{}",
        response.text
    );
    handle.join().unwrap().unwrap();
}
