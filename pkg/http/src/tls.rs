//! Optional TLS for the listener, and the client TLS configuration shared by
//! the internal HTTPS clients (replication follower, placement router).
//!
//! Server side:
//!
//! * certificate chain and private key come from PEM files; TLS 1.2 and 1.3
//!   with the rustls safe defaults (ring provider), ALPN `http/1.1`;
//! * optional client-certificate verification against a CA bundle. Without
//!   `require_client_cert` a client may connect without a certificate; one it
//!   presents must still chain to the CA. The SHA-256 fingerprint of a
//!   verified client certificate reaches handlers as [`TlsInfo`];
//! * the files are re-read at most once per [`RELOAD_CHECK_INTERVAL`] and the
//!   configuration is rebuilt when their content changed, so certificate
//!   rotation needs no restart. A reload that fails (half-written file,
//!   mismatched key) keeps the previous configuration and is logged;
//! * the handshake runs non-blocking on the accept thread
//!   (see `ConnFrontend`), bounded by the first-byte timeout, the per-IP cap
//!   and the pending-connection bound, so a slow or silent handshake never
//!   occupies a worker;
//! * a plaintext HTTP request sent to a TLS port gets a plaintext `400` and
//!   the connection is closed.

use std::io::{self, ErrorKind, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use rustls::{RootCertStore, ServerConnection};
use sha2::{Digest, Sha256};

use crate::conn::{Lane, PEEK_BYTES, classify};
use crate::response::{Response, render_response};
use crate::server::HealthClassifier;

/// How often the certificate, key and client CA files are checked for
/// changes (at most; the check runs when a connection is accepted).
pub const RELOAD_CHECK_INTERVAL: Duration = Duration::from_secs(1);
/// ALPN protocol the server offers.
pub const ALPN_HTTP_1_1: &[u8] = b"http/1.1";
/// First byte of a TLS handshake record.
const TLS_HANDSHAKE_RECORD: u8 = 0x16;
/// Read/process rounds per connection per poll, so one busy handshake cannot
/// monopolise the accept thread.
const MAX_ROUNDS_PER_POLL: usize = 8;
/// Bytes discarded (non-blocking) before answering a plaintext client.
const PLAINTEXT_DRAIN_BYTES: usize = 64 * 1024;

/// Where the listener's TLS material lives.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TlsSettings {
    /// PEM certificate chain, leaf first.
    pub cert_file: PathBuf,
    /// PEM private key (PKCS#8, PKCS#1 or SEC1).
    pub key_file: PathBuf,
    /// PEM bundle of CAs that client certificates must chain to. `None`
    /// disables client-certificate verification.
    pub client_ca_file: Option<PathBuf>,
    /// Refuse clients that present no certificate (requires
    /// `client_ca_file`).
    pub require_client_cert: bool,
}

/// TLS details of the connection a request arrived on.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TlsInfo {
    /// Lowercase hex SHA-256 of the client's leaf certificate (DER), present
    /// only when the client sent a certificate that verified against the
    /// configured client CA.
    pub client_cert_sha256: Option<String>,
}

impl TlsInfo {
    fn from_connection(conn: &ServerConnection) -> Self {
        Self {
            client_cert_sha256: conn
                .peer_certificates()
                .and_then(|chain| chain.first())
                .map(|leaf| sha256_hex(leaf.as_ref())),
        }
    }
}

/// Lowercase hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

struct FileSet {
    cert: Vec<u8>,
    key: Vec<u8>,
    client_ca: Option<Vec<u8>>,
}

impl FileSet {
    fn read(settings: &TlsSettings) -> Result<Self, String> {
        let read = |path: &Path, what: &str| {
            std::fs::read(path)
                .map_err(|err| format!("cannot read {what} '{}': {err}", path.display()))
        };
        Ok(Self {
            cert: read(&settings.cert_file, "TLS certificate file")?,
            key: read(&settings.key_file, "TLS private key file")?,
            client_ca: match settings.client_ca_file.as_deref() {
                Some(path) => Some(read(path, "TLS client CA file")?),
                None => None,
            },
        })
    }

    fn digest(&self) -> [u8; 32] {
        let mut hasher = Sha256::new();
        for part in [Some(&self.cert), Some(&self.key), self.client_ca.as_ref()] {
            match part {
                Some(bytes) => {
                    hasher.update((bytes.len() as u64).to_le_bytes());
                    hasher.update(bytes);
                }
                None => hasher.update(u64::MAX.to_le_bytes()),
            }
        }
        hasher.finalize().into()
    }
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

fn parse_certs(
    pem: &[u8],
    what: &str,
    path: &Path,
) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| format!("invalid PEM in {what} '{}': {err}", path.display()))?;
    if certs.is_empty() {
        return Err(format!(
            "{what} '{}' contains no certificates",
            path.display()
        ));
    }
    Ok(certs)
}

fn roots_from_pem(pem: &[u8], what: &str, path: &Path) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    for cert in parse_certs(pem, what, path)? {
        roots
            .add(cert)
            .map_err(|err| format!("invalid certificate in {what} '{}': {err}", path.display()))?;
    }
    Ok(roots)
}

fn build_server_config(
    settings: &TlsSettings,
    files: &FileSet,
) -> Result<Arc<rustls::ServerConfig>, String> {
    let chain = parse_certs(&files.cert, "TLS certificate file", &settings.cert_file)?;
    let key = PrivateKeyDer::from_pem_slice(&files.key).map_err(|err| {
        format!(
            "no usable private key in TLS private key file '{}': {err}",
            settings.key_file.display()
        )
    })?;
    let provider = provider();
    let builder = rustls::ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|err| format!("TLS configuration failed: {err}"))?;
    let builder = match (
        files.client_ca.as_deref(),
        settings.client_ca_file.as_deref(),
    ) {
        (Some(pem), Some(path)) => {
            let roots = roots_from_pem(pem, "TLS client CA file", path)?;
            let mut verifier =
                WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider));
            if !settings.require_client_cert {
                verifier = verifier.allow_unauthenticated();
            }
            let verifier = verifier
                .build()
                .map_err(|err| format!("TLS client verifier failed: {err}"))?;
            builder.with_client_cert_verifier(verifier)
        }
        _ => builder.with_no_client_auth(),
    };
    let mut config = builder.with_single_cert(chain, key).map_err(|err| {
        format!(
            "TLS certificate '{}' and private key '{}' are unusable together: {err}",
            settings.cert_file.display(),
            settings.key_file.display()
        )
    })?;
    config.alpn_protocols = vec![ALPN_HTTP_1_1.to_vec()];
    Ok(Arc::new(config))
}

struct ReloadState {
    config: Arc<rustls::ServerConfig>,
    digest: [u8; 32],
    last_check: Instant,
}

struct Inner {
    settings: TlsSettings,
    state: Mutex<ReloadState>,
}

/// Loaded server TLS configuration with change-driven reload. Cheap to
/// clone; clones share the reload state.
#[derive(Clone)]
pub struct TlsAcceptor {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for TlsAcceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsAcceptor")
            .field("settings", &self.inner.settings)
            .finish_non_exhaustive()
    }
}

impl TlsAcceptor {
    /// Load and validate the files. Errors name the file, never its content.
    pub fn new(settings: TlsSettings) -> Result<Self, String> {
        if settings.require_client_cert && settings.client_ca_file.is_none() {
            return Err("requiring client certificates needs a client CA file".to_string());
        }
        let files = FileSet::read(&settings)?;
        let config = build_server_config(&settings, &files)?;
        Ok(Self {
            inner: Arc::new(Inner {
                state: Mutex::new(ReloadState {
                    config,
                    digest: files.digest(),
                    last_check: Instant::now(),
                }),
                settings,
            }),
        })
    }

    pub fn settings(&self) -> &TlsSettings {
        &self.inner.settings
    }

    /// True when client certificates are verified (optional or required).
    pub fn verifies_client_certs(&self) -> bool {
        self.inner.settings.client_ca_file.is_some()
    }

    /// The configuration for a new connection. At most once per
    /// [`RELOAD_CHECK_INTERVAL`] the files are re-read and, when their
    /// content changed, the configuration is rebuilt. A failed rebuild keeps
    /// the previous configuration.
    pub fn config(&self) -> Arc<rustls::ServerConfig> {
        let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        if state.last_check.elapsed() >= RELOAD_CHECK_INTERVAL {
            state.last_check = Instant::now();
            if let Err(err) = self.reload_locked(&mut state) {
                eprintln!(
                    "tls: reload of '{}' failed, keeping the previous certificate: {err}",
                    self.inner.settings.cert_file.display()
                );
            }
        }
        Arc::clone(&state.config)
    }

    /// Re-read the files now. `Ok(true)` when the configuration changed.
    pub fn reload(&self) -> Result<bool, String> {
        let mut state = self.inner.state.lock().unwrap_or_else(|p| p.into_inner());
        state.last_check = Instant::now();
        self.reload_locked(&mut state)
    }

    fn reload_locked(&self, state: &mut ReloadState) -> Result<bool, String> {
        let files = FileSet::read(&self.inner.settings)?;
        let digest = files.digest();
        if digest == state.digest {
            return Ok(false);
        }
        let config = build_server_config(&self.inner.settings, &files)?;
        state.config = config;
        state.digest = digest;
        eprintln!(
            "tls: reloaded certificate '{}'",
            self.inner.settings.cert_file.display()
        );
        Ok(true)
    }
}

/// Client TLS configuration for internal HTTPS clients: the public web roots
/// plus an optional PEM bundle of extra CAs, and an optional client
/// certificate (chain PEM, key PEM) for mutual TLS. Verification is never
/// disabled.
pub fn client_config(
    extra_ca_file: Option<&Path>,
    identity: Option<(&Path, &Path)>,
) -> Result<Arc<rustls::ClientConfig>, String> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    if let Some(path) = extra_ca_file {
        let pem = std::fs::read(path)
            .map_err(|err| format!("cannot read CA file '{}': {err}", path.display()))?;
        for cert in parse_certs(&pem, "CA file", path)? {
            roots.add(cert).map_err(|err| {
                format!("invalid certificate in CA file '{}': {err}", path.display())
            })?;
        }
    }
    let builder = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(|err| format!("TLS configuration failed: {err}"))?
        .with_root_certificates(roots);
    let config = match identity {
        None => builder.with_no_client_auth(),
        Some((cert_path, key_path)) => {
            let cert_pem = std::fs::read(cert_path).map_err(|err| {
                format!(
                    "cannot read client certificate file '{}': {err}",
                    cert_path.display()
                )
            })?;
            let chain = parse_certs(&cert_pem, "client certificate file", cert_path)?;
            let key_pem = std::fs::read(key_path).map_err(|err| {
                format!(
                    "cannot read client key file '{}': {err}",
                    key_path.display()
                )
            })?;
            let key = PrivateKeyDer::from_pem_slice(&key_pem).map_err(|err| {
                format!(
                    "no usable private key in client key file '{}': {err}",
                    key_path.display()
                )
            })?;
            builder.with_client_auth_cert(chain, key).map_err(|err| {
                format!(
                    "client certificate '{}' and key '{}' are unusable together: {err}",
                    cert_path.display(),
                    key_path.display()
                )
            })?
        }
    };
    Ok(Arc::new(config))
}

// ---------------------------------------------------------------------------
// Per-connection state
// ---------------------------------------------------------------------------

/// TLS state of one accepted connection.
#[derive(Debug)]
pub(crate) struct TlsState {
    pub(crate) conn: ServerConnection,
    /// The first byte was checked to be a TLS handshake record.
    saw_first_byte: bool,
    /// Plaintext already decrypted to classify the request line; served to
    /// the parser before anything else.
    pub(crate) prefix: Vec<u8>,
    pub(crate) info: Option<TlsInfo>,
}

impl TlsState {
    pub(crate) fn new(acceptor: &TlsAcceptor) -> Option<Self> {
        let conn = ServerConnection::new(acceptor.config()).ok()?;
        Some(Self {
            conn,
            saw_first_byte: false,
            prefix: Vec::new(),
            info: None,
        })
    }

    fn handshake_done(&self) -> bool {
        self.info.is_some()
    }
}

/// Outcome of one non-blocking advance of a pending TLS connection.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TlsStep {
    /// Needs more bytes from the peer.
    Pending,
    /// Handshake done (and, with a classifier, the request line routed).
    Ready(Lane),
    /// Close the connection.
    Drop,
}

fn would_block(err: &io::Error) -> bool {
    matches!(err.kind(), ErrorKind::WouldBlock | ErrorKind::Interrupted)
}

/// Answer a plaintext HTTP client on a TLS port with a plaintext 400 and
/// close. The socket must be non-blocking; everything is best effort.
pub(crate) fn reject_plaintext(sock: &mut TcpStream) {
    let _ = sock.set_nonblocking(true);
    let mut buf = [0u8; 4096];
    let mut drained = 0usize;
    // Discard what already arrived so closing does not reset the
    // connection before the client reads the answer.
    while drained < PLAINTEXT_DRAIN_BYTES {
        match sock.read(&mut buf) {
            Ok(0) | Err(_) => break,
            Ok(n) => drained += n,
        }
    }
    let response = Response::error(400, "this port requires TLS; use https://")
        .with_header("Connection", "close");
    let _ = sock.write_all(render_response(&response).as_bytes());
    let _ = sock.shutdown(std::net::Shutdown::Write);
}

/// Check that the first byte is a TLS handshake record. `None` while nothing
/// arrived yet.
fn check_first_byte(state: &mut TlsState, sock: &mut TcpStream) -> Option<TlsStep> {
    if state.saw_first_byte {
        return None;
    }
    let mut first = [0u8; 1];
    match sock.peek(&mut first) {
        Ok(0) => Some(TlsStep::Drop),
        Ok(_) if first[0] != TLS_HANDSHAKE_RECORD => {
            reject_plaintext(sock);
            Some(TlsStep::Drop)
        }
        Ok(_) => {
            state.saw_first_byte = true;
            None
        }
        Err(err) if would_block(&err) => Some(TlsStep::Pending),
        Err(_) => Some(TlsStep::Drop),
    }
}

/// Advance a pending connection on a non-blocking socket: handshake, then
/// (with `classifier`) decrypt just enough plaintext to route the request.
pub(crate) fn advance(
    state: &mut TlsState,
    sock: &mut TcpStream,
    classifier: Option<HealthClassifier>,
) -> TlsStep {
    if let Some(step) = check_first_byte(state, sock) {
        return step;
    }
    for _ in 0..MAX_ROUNDS_PER_POLL {
        while state.conn.wants_write() {
            match state.conn.write_tls(sock) {
                Ok(_) => {}
                Err(err) if would_block(&err) => break,
                Err(_) => return TlsStep::Drop,
            }
        }
        if !state.conn.is_handshaking() {
            if state.info.is_none() {
                state.info = Some(TlsInfo::from_connection(&state.conn));
            }
            let Some(classifier) = classifier else {
                return TlsStep::Ready(Lane::General);
            };
            if state.prefix.len() < PEEK_BYTES {
                let mut buf = [0u8; PEEK_BYTES];
                let room = PEEK_BYTES - state.prefix.len();
                match state.conn.reader().read(&mut buf[..room]) {
                    Ok(0) => return partial_or_drop(state),
                    Ok(n) => state.prefix.extend_from_slice(&buf[..n]),
                    Err(err) if err.kind() == ErrorKind::WouldBlock => {}
                    Err(_) => return partial_or_drop(state),
                }
            }
            if let Some(lane) = classify(&state.prefix, classifier) {
                return TlsStep::Ready(lane);
            }
        }
        match state.conn.read_tls(sock) {
            Ok(0) => return partial_or_drop(state),
            Ok(_) => {
                if state.conn.process_new_packets().is_err() {
                    // Send the alert rustls queued, then close.
                    let _ = state.conn.write_tls(sock);
                    return TlsStep::Drop;
                }
            }
            Err(err) if would_block(&err) => return TlsStep::Pending,
            Err(_) => return TlsStep::Drop,
        }
    }
    TlsStep::Pending
}

/// After a deadline or EOF: a connection that finished its handshake and sent
/// part of a request goes to a worker (which answers 400/408); anything else
/// is closed.
pub(crate) fn partial_or_drop(state: &TlsState) -> TlsStep {
    if state.handshake_done() && !state.prefix.is_empty() {
        TlsStep::Ready(Lane::General)
    } else {
        TlsStep::Drop
    }
}

/// Blocking handshake for the one-shot server: per-read timeout `timeout`.
pub(crate) fn handshake_blocking(
    acceptor: &TlsAcceptor,
    sock: &mut TcpStream,
    timeout: Duration,
) -> Option<TlsState> {
    sock.set_read_timeout(Some(timeout)).ok()?;
    sock.set_write_timeout(Some(timeout)).ok()?;
    let mut first = [0u8; 1];
    match sock.peek(&mut first) {
        Ok(1) if first[0] == TLS_HANDSHAKE_RECORD => {}
        Ok(1) => {
            reject_plaintext(sock);
            return None;
        }
        _ => return None,
    }
    let mut state = TlsState::new(acceptor)?;
    state.saw_first_byte = true;
    while state.conn.is_handshaking() {
        if state.conn.complete_io(sock).is_err() {
            let _ = state.conn.write_tls(sock);
            return None;
        }
    }
    state.info = Some(TlsInfo::from_connection(&state.conn));
    Some(state)
}

/// Encrypt `bytes`, then send them and a `close_notify` on a socket in any
/// mode; best effort (used for the overload response).
pub(crate) fn write_and_close(state: &mut TlsState, sock: &mut TcpStream, bytes: &[u8]) {
    let _ = state.conn.writer().write_all(bytes);
    state.conn.send_close_notify();
    while state.conn.wants_write() {
        if state.conn.write_tls(sock).is_err() {
            break;
        }
    }
    let _ = sock.shutdown(std::net::Shutdown::Write);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sha256_hex_is_lowercase_and_64_chars() {
        let hex = sha256_hex(b"abc");
        assert_eq!(
            hex,
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn required_client_certs_need_a_ca() {
        let err = TlsAcceptor::new(TlsSettings {
            cert_file: PathBuf::from("/nonexistent/cert.pem"),
            key_file: PathBuf::from("/nonexistent/key.pem"),
            client_ca_file: None,
            require_client_cert: true,
        })
        .unwrap_err();
        assert!(err.contains("client CA"), "{err}");
    }

    #[test]
    fn missing_files_are_named_in_the_error() {
        let err = TlsAcceptor::new(TlsSettings {
            cert_file: PathBuf::from("/nonexistent/cert.pem"),
            key_file: PathBuf::from("/nonexistent/key.pem"),
            client_ca_file: None,
            require_client_cert: false,
        })
        .unwrap_err();
        assert!(err.contains("/nonexistent/cert.pem"), "{err}");
    }
}
