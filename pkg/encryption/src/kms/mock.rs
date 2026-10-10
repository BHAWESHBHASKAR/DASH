//! In-process mock key managers for tests: a tiny HTTP/1.1 server, a Vault
//! Transit engine (`encrypt` / `decrypt`, key versions, token check) and an
//! AWS KMS endpoint (`Encrypt` / `Decrypt`, SigV4 verification, encryption
//! context). They encrypt for real (AES-256-GCM) so ciphertexts behave like
//! the real services' ones: damaged or mismatched input is refused.
//!
//! Compiled for this crate's tests and, with the `mock-kms` feature, for
//! other crates' tests. Not for production use.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use aes_gcm::aead::{Aead, KeyInit, Payload};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use rand::RngCore;

use super::sigv4::{self, SigningRequest, SigningScope};

/// Access key the mock KMS accepts.
pub const MOCK_ACCESS_KEY: &str = "AKIDMOCKEXAMPLE";
/// Secret key the mock KMS verifies signatures with.
pub const MOCK_SECRET_KEY: &str = "mock/secret+key/for/tests/only/0000000000";

/// One request as the mock server received it.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    /// Header names lower-cased, in arrival order.
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RecordedRequest {
    /// The first value of header `name` (case-insensitive).
    pub fn header(&self, name: &str) -> Option<String> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.clone())
    }
}

/// A response the handler returns.
#[derive(Debug, Clone)]
pub struct MockResponse {
    pub status: u16,
    pub body: Vec<u8>,
}

impl MockResponse {
    pub fn new(status: u16, body: &str) -> Self {
        Self {
            status,
            body: body.as_bytes().to_vec(),
        }
    }

    pub fn json(status: u16, value: serde_json::Value) -> Self {
        Self {
            status,
            body: value.to_string().into_bytes(),
        }
    }
}

type Handler = dyn Fn(&RecordedRequest) -> MockResponse + Send + Sync;

#[derive(Default)]
struct Injection {
    fail_remaining: usize,
    fail_status: u16,
    delay_remaining: usize,
    delay: Duration,
}

struct ServerState {
    requests: Mutex<Vec<RecordedRequest>>,
    injection: Mutex<Injection>,
    stop: AtomicBool,
}

/// A minimal HTTP/1.1 server on 127.0.0.1 (one thread per connection, one
/// request per connection). Stops when dropped.
pub struct MockServer {
    addr: String,
    state: Arc<ServerState>,
}

impl MockServer {
    pub fn start(
        handler: impl Fn(&RecordedRequest) -> MockResponse + Send + Sync + 'static,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock server");
        let addr = format!("http://{}", listener.local_addr().expect("local addr"));
        let state = Arc::new(ServerState {
            requests: Mutex::new(Vec::new()),
            injection: Mutex::new(Injection::default()),
            stop: AtomicBool::new(false),
        });
        let handler: Arc<Handler> = Arc::new(handler);
        let thread_state = state.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if thread_state.stop.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                let state = thread_state.clone();
                let handler = handler.clone();
                std::thread::spawn(move || serve(stream, &state, &*handler));
            }
        });
        Self { addr, state }
    }

    /// `http://127.0.0.1:<port>`.
    pub fn addr(&self) -> String {
        self.addr.clone()
    }

    /// Answer the next `n` requests with `status` instead of the handler.
    pub fn fail_next(&self, n: usize, status: u16) {
        let mut inj = self.state.injection.lock().unwrap();
        inj.fail_remaining = n;
        inj.fail_status = status;
    }

    /// Hold the answer to the next `n` requests for `delay`.
    pub fn delay_next(&self, n: usize, delay: Duration) {
        let mut inj = self.state.injection.lock().unwrap();
        inj.delay_remaining = n;
        inj.delay = delay;
    }

    /// Requests received so far.
    pub fn count(&self) -> usize {
        self.state.requests.lock().unwrap().len()
    }

    /// Requests whose path contains `fragment`.
    pub fn count_matching(&self, fragment: &str) -> usize {
        self.state
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.path.contains(fragment))
            .count()
    }

    pub fn last_request(&self) -> Option<RecordedRequest> {
        self.state.requests.lock().unwrap().last().cloned()
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::SeqCst);
        // Wake the accept loop.
        let _ = TcpStream::connect(self.addr.trim_start_matches("http://"));
    }
}

fn read_request(stream: &TcpStream) -> Option<RecordedRequest> {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    let mut parts = line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();
    let mut headers = Vec::new();
    loop {
        let mut h = String::new();
        if reader.read_line(&mut h).ok()? == 0 {
            return None;
        }
        let h = h.trim_end_matches(['\r', '\n']);
        if h.is_empty() {
            break;
        }
        let (name, value) = h.split_once(':')?;
        headers.push((name.trim().to_ascii_lowercase(), value.trim().to_string()));
    }
    let len = headers
        .iter()
        .find(|(n, _)| n == "content-length")
        .and_then(|(_, v)| v.parse::<usize>().ok())
        .unwrap_or(0);
    let mut body = vec![0u8; len];
    reader.read_exact(&mut body).ok()?;
    Some(RecordedRequest {
        method,
        path,
        headers,
        body,
    })
}

fn serve(mut stream: TcpStream, state: &ServerState, handler: &Handler) {
    let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
    let Some(request) = read_request(&stream) else {
        return;
    };
    state.requests.lock().unwrap().push(request.clone());
    let (fail, delay) = {
        let mut inj = state.injection.lock().unwrap();
        let fail = if inj.fail_remaining > 0 {
            inj.fail_remaining -= 1;
            Some(inj.fail_status)
        } else {
            None
        };
        let delay = if inj.delay_remaining > 0 {
            inj.delay_remaining -= 1;
            Some(inj.delay)
        } else {
            None
        };
        (fail, delay)
    };
    if let Some(d) = delay {
        std::thread::sleep(d);
    }
    let response = match fail {
        Some(status) => MockResponse::new(status, "{\"errors\":[\"injected failure\"]}"),
        None => handler(&request),
    };
    let head = format!(
        "HTTP/1.1 {} Mock\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        response.status,
        response.body.len()
    );
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&response.body);
    let _ = stream.flush();
}

/// Writes a token file with mode 0600 and returns its path.
pub fn write_token(dir: &Path, name: &str, token: &str) -> PathBuf {
    let path = dir.join(name);
    std::fs::write(&path, format!("{token}\n")).expect("write token");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("chmod");
    }
    path
}

fn seal(key: &[u8; 32], aad: &[u8], plain: &[u8]) -> Vec<u8> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    let nonce: [u8; 12] = rand::Rng::r#gen(&mut rand::rngs::OsRng);
    let mut out = nonce.to_vec();
    out.extend(
        cipher
            .encrypt(Nonce::from_slice(&nonce), Payload { msg: plain, aad })
            .expect("seal"),
    );
    out
}

fn open(key: &[u8; 32], aad: &[u8], sealed: &[u8]) -> Option<Vec<u8>> {
    if sealed.len() < 12 {
        return None;
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key));
    cipher
        .decrypt(
            Nonce::from_slice(&sealed[..12]),
            Payload {
                msg: &sealed[12..],
                aad,
            },
        )
        .ok()
}

fn random_key() -> [u8; 32] {
    let mut k = [0u8; 32];
    rand::rngs::OsRng.fill_bytes(&mut k);
    k
}

// ---------------------------------------------------------------------------
// Vault Transit
// ---------------------------------------------------------------------------

struct VaultState {
    token: String,
    /// Key name -> versions (index 0 = v1).
    keys: HashMap<String, Vec<[u8; 32]>>,
}

/// A Vault Transit engine at any mount: `POST /v1/<mount>/encrypt/<key>`
/// and `/decrypt/<key>`, `X-Vault-Token` checked, keys created on first
/// encrypt (as Transit's upsert does).
pub struct MockVault {
    pub server: MockServer,
    state: Arc<Mutex<VaultState>>,
}

impl MockVault {
    pub fn start(token: &str) -> Self {
        let state = Arc::new(Mutex::new(VaultState {
            token: token.to_string(),
            keys: HashMap::new(),
        }));
        let handler_state = state.clone();
        let server = MockServer::start(move |req| vault_handle(&handler_state, req));
        Self { server, state }
    }

    pub fn addr(&self) -> String {
        self.server.addr()
    }

    /// Accept only `token` from now on.
    pub fn set_token(&self, token: &str) {
        self.state.lock().unwrap().token = token.to_string();
    }

    /// Adds a key version (`vault write -f transit/keys/<key>/rotate`).
    pub fn rotate(&self, key: &str) {
        self.state
            .lock()
            .unwrap()
            .keys
            .entry(key.to_string())
            .or_default()
            .push(random_key());
    }

    /// Requests to `/encrypt/` or `/decrypt/` (`op` = `encrypt` / `decrypt`).
    pub fn requests(&self, op: &str) -> usize {
        self.server.count_matching(&format!("/{op}/"))
    }

    pub fn fail_next(&self, n: usize, status: u16) {
        self.server.fail_next(n, status)
    }

    pub fn delay_next(&self, n: usize, delay: Duration) {
        self.server.delay_next(n, delay)
    }

    pub fn last_request(&self) -> Option<RecordedRequest> {
        self.server.last_request()
    }
}

fn vault_error(status: u16, msg: &str) -> MockResponse {
    MockResponse::json(status, serde_json::json!({ "errors": [msg] }))
}

fn vault_handle(state: &Mutex<VaultState>, req: &RecordedRequest) -> MockResponse {
    let mut st = state.lock().unwrap();
    if req.header("x-vault-token").as_deref() != Some(st.token.as_str()) {
        return vault_error(403, "permission denied");
    }
    if req.method != "POST" {
        return vault_error(405, "unsupported operation");
    }
    let Some(rest) = req.path.strip_prefix("/v1/") else {
        return vault_error(404, "no handler for route");
    };
    let segments: Vec<&str> = rest.split('/').collect();
    if segments.len() < 3 {
        return vault_error(404, "no handler for route");
    }
    let key_name = segments[segments.len() - 1].to_string();
    let op = segments[segments.len() - 2];
    let body: serde_json::Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) => return vault_error(400, "failed to parse JSON input"),
    };
    let aad = format!("{}/{}", segments[..segments.len() - 2].join("/"), key_name);
    match op {
        "encrypt" => {
            let Some(plain) = body["plaintext"].as_str().and_then(|p| B64.decode(p).ok()) else {
                return vault_error(400, "missing plaintext to encrypt");
            };
            let versions = st.keys.entry(key_name).or_default();
            if versions.is_empty() {
                versions.push(random_key());
            }
            let version = versions.len();
            let sealed = seal(&versions[version - 1], aad.as_bytes(), &plain);
            MockResponse::json(
                200,
                serde_json::json!({
                    "data": {
                        "ciphertext": format!("vault:v{version}:{}", B64.encode(sealed)),
                        "key_version": version,
                    }
                }),
            )
        }
        "decrypt" => {
            let Some(ct) = body["ciphertext"].as_str() else {
                return vault_error(400, "missing ciphertext to decrypt");
            };
            let Some(versions) = st.keys.get(&key_name) else {
                return vault_error(400, "encryption key not found");
            };
            let parsed = ct
                .strip_prefix("vault:v")
                .and_then(|r| r.split_once(':'))
                .and_then(|(v, b)| Some((v.parse::<usize>().ok()?, B64.decode(b).ok()?)));
            let Some((version, sealed)) = parsed else {
                return vault_error(400, "invalid ciphertext: no prefix");
            };
            let Some(key) = version.checked_sub(1).and_then(|i| versions.get(i)) else {
                return vault_error(400, "invalid key version");
            };
            match open(key, aad.as_bytes(), &sealed) {
                Some(plain) => MockResponse::json(
                    200,
                    serde_json::json!({ "data": { "plaintext": B64.encode(plain) } }),
                ),
                None => vault_error(400, "cipher: message authentication failed"),
            }
        }
        _ => vault_error(404, "no handler for route"),
    }
}

// ---------------------------------------------------------------------------
// AWS KMS
// ---------------------------------------------------------------------------

struct KmsState {
    region: String,
    /// Key id -> key material.
    keys: Vec<(String, [u8; 32])>,
    aliases: HashMap<String, String>,
    session_token: Option<String>,
    throttle_remaining: usize,
    bad_signatures: usize,
}

/// An AWS KMS endpoint (`TrentService.Encrypt` / `Decrypt`) that verifies
/// every request's SigV4 signature with [`MOCK_SECRET_KEY`].
pub struct MockKms {
    pub server: MockServer,
    state: Arc<Mutex<KmsState>>,
}

impl MockKms {
    /// A KMS in `us-east-1` with the given key ids.
    pub fn start(key_ids: &[&str]) -> Self {
        let state = Arc::new(Mutex::new(KmsState {
            region: "us-east-1".to_string(),
            keys: key_ids
                .iter()
                .map(|k| (k.to_string(), random_key()))
                .collect(),
            aliases: HashMap::new(),
            session_token: None,
            throttle_remaining: 0,
            bad_signatures: 0,
        }));
        let handler_state = state.clone();
        let server = MockServer::start(move |req| kms_handle(&handler_state, req));
        Self { server, state }
    }

    pub fn addr(&self) -> String {
        self.server.addr()
    }

    pub fn alias(&self, alias: &str, key: &str) {
        self.state
            .lock()
            .unwrap()
            .aliases
            .insert(alias.to_string(), key.to_string());
    }

    /// Require this session token (`X-Amz-Security-Token`); others get
    /// `ExpiredTokenException`.
    pub fn expect_session_token(&self, token: &str) {
        self.state.lock().unwrap().session_token = Some(token.to_string());
    }

    /// Answer the next `n` requests with `ThrottlingException`.
    pub fn throttle_next(&self, n: usize) {
        self.state.lock().unwrap().throttle_remaining = n;
    }

    pub fn requests(&self) -> usize {
        self.server.count()
    }

    /// Requests whose signature did not verify.
    pub fn bad_signatures(&self) -> usize {
        self.state.lock().unwrap().bad_signatures
    }

    pub fn last_request(&self) -> Option<RecordedRequest> {
        self.server.last_request()
    }
}

fn kms_error(status: u16, kind: &str, msg: &str) -> MockResponse {
    MockResponse::json(
        status,
        serde_json::json!({ "__type": kind, "message": msg }),
    )
}

fn arn(region: &str, key: &str) -> String {
    format!("arn:aws:kms:{region}:111122223333:key/{key}")
}

/// Verifies the SigV4 `Authorization` header of `req`.
fn verify_signature(req: &RecordedRequest, region: &str) -> Result<(), MockResponse> {
    let auth = req.header("authorization").ok_or_else(|| {
        kms_error(
            400,
            "MissingAuthenticationTokenException",
            "no Authorization",
        )
    })?;
    let rest = auth
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .ok_or_else(|| kms_error(400, "IncompleteSignatureException", "bad algorithm"))?;
    let mut credential = "";
    let mut signed = "";
    for part in rest.split(", ") {
        if let Some(v) = part.strip_prefix("Credential=") {
            credential = v;
        } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
            signed = v;
        }
    }
    let cred: Vec<&str> = credential.split('/').collect();
    if cred.len() != 5 || cred[2] != region || cred[3] != "kms" || cred[4] != "aws4_request" {
        return Err(kms_error(
            400,
            "IncompleteSignatureException",
            "bad credential scope",
        ));
    }
    if cred[0] != MOCK_ACCESS_KEY {
        return Err(kms_error(
            400,
            "UnrecognizedClientException",
            "The security token included in the request is invalid.",
        ));
    }
    let amz_date = req
        .header("x-amz-date")
        .ok_or_else(|| kms_error(400, "IncompleteSignatureException", "no X-Amz-Date"))?;
    if !amz_date.starts_with(cred[1]) {
        return Err(kms_error(
            400,
            "IncompleteSignatureException",
            "date mismatch",
        ));
    }
    let names: Vec<&str> = signed.split(';').collect();
    for required in ["host", "x-amz-date", "x-amz-target", "content-type"] {
        if !names.contains(&required) {
            return Err(kms_error(
                400,
                "IncompleteSignatureException",
                "required header not signed",
            ));
        }
    }
    let values: Vec<(String, String)> = names
        .iter()
        .map(|n| (n.to_string(), req.header(n).unwrap_or_default()))
        .collect();
    let headers: Vec<(&str, &str)> = values
        .iter()
        .map(|(n, v)| (n.as_str(), v.as_str()))
        .collect();
    let expected = sigv4::authorization(
        MOCK_ACCESS_KEY,
        MOCK_SECRET_KEY,
        &SigningScope {
            amz_date: &amz_date,
            region,
            service: "kms",
        },
        &SigningRequest {
            method: &req.method,
            path: &req.path,
            query: &[],
            headers: &headers,
            payload: &req.body,
        },
    );
    if expected != auth {
        return Err(kms_error(
            400,
            "InvalidSignatureException",
            "The request signature we calculated does not match the signature you provided.",
        ));
    }
    Ok(())
}

fn context_aad(body: &serde_json::Value) -> Vec<u8> {
    let mut pairs: Vec<(String, String)> = body["EncryptionContext"]
        .as_object()
        .map(|m| {
            m.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("").to_string()))
                .collect()
        })
        .unwrap_or_default();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v};"))
        .collect::<String>()
        .into_bytes()
}

fn kms_handle(state: &Mutex<KmsState>, req: &RecordedRequest) -> MockResponse {
    let mut st = state.lock().unwrap();
    if let Err(resp) = verify_signature(req, &st.region.clone()) {
        if resp.body.windows(9).any(|w| w == b"Signature") {
            st.bad_signatures += 1;
        }
        return resp;
    }
    if let Some(expected) = &st.session_token
        && req.header("x-amz-security-token").as_deref() != Some(expected.as_str())
    {
        return kms_error(
            400,
            "ExpiredTokenException",
            "The security token included in the request is expired",
        );
    }
    if st.throttle_remaining > 0 {
        st.throttle_remaining -= 1;
        return kms_error(400, "ThrottlingException", "Rate exceeded");
    }
    if req.header("content-type").as_deref() != Some("application/x-amz-json-1.1") {
        return kms_error(400, "SerializationException", "bad content type");
    }
    let body: serde_json::Value = match serde_json::from_slice(&req.body) {
        Ok(v) => v,
        Err(_) => return kms_error(400, "SerializationException", "bad JSON"),
    };
    let region = st.region.clone();
    let resolve = |st: &KmsState, id: &str| -> Option<usize> {
        let id = st.aliases.get(id).map(String::as_str).unwrap_or(id);
        let id = id.rsplit("key/").next().unwrap_or(id);
        st.keys.iter().position(|(k, _)| k == id)
    };
    let aad = context_aad(&body);
    match req.header("x-amz-target").as_deref() {
        Some("TrentService.Encrypt") => {
            let Some(idx) = body["KeyId"].as_str().and_then(|k| resolve(&st, k)) else {
                return kms_error(400, "NotFoundException", "key not found");
            };
            let Some(plain) = body["Plaintext"].as_str().and_then(|p| B64.decode(p).ok()) else {
                return kms_error(400, "ValidationException", "bad Plaintext");
            };
            let mut blob = vec![idx as u8];
            blob.extend(seal(&st.keys[idx].1, &aad, &plain));
            MockResponse::json(
                200,
                serde_json::json!({
                    "CiphertextBlob": B64.encode(blob),
                    "KeyId": arn(&region, &st.keys[idx].0),
                    "EncryptionAlgorithm": "SYMMETRIC_DEFAULT",
                }),
            )
        }
        Some("TrentService.Decrypt") => {
            let Some(blob) = body["CiphertextBlob"]
                .as_str()
                .and_then(|b| B64.decode(b).ok())
            else {
                return kms_error(400, "ValidationException", "bad CiphertextBlob");
            };
            let Some((&idx, sealed)) = blob.split_first() else {
                return kms_error(400, "InvalidCiphertextException", "");
            };
            let idx = idx as usize;
            if idx >= st.keys.len() {
                return kms_error(400, "InvalidCiphertextException", "");
            }
            if let Some(requested) = body["KeyId"].as_str()
                && resolve(&st, requested) != Some(idx)
            {
                return kms_error(
                    400,
                    "IncorrectKeyException",
                    "The key ID in the request does not identify a CMK that can perform this operation.",
                );
            }
            match open(&st.keys[idx].1, &aad, sealed) {
                Some(plain) => MockResponse::json(
                    200,
                    serde_json::json!({
                        "Plaintext": B64.encode(plain),
                        "KeyId": arn(&region, &st.keys[idx].0),
                    }),
                ),
                None => kms_error(400, "InvalidCiphertextException", ""),
            }
        }
        _ => kms_error(400, "UnknownOperationException", ""),
    }
}
