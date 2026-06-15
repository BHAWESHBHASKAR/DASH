//! Embedding adapter crate.
//!
//! Provides a unified [`EmbeddingProvider`] trait with three concrete
//! implementations:
//!
//! * [`HashEmbeddingProvider`] — deterministic, dependency-free, useful as a
//!   fallback or for unit tests.
//! * [`OllamaEmbeddingProvider`] — talks to a local Ollama daemon over plain
//!   HTTP at `http://localhost:11434/api/embeddings`.
//! * [`OpenAIEmbeddingProvider`] — talks to OpenAI's `/v1/embeddings` endpoint
//!   using a Bearer token.
//!
//! HTTP I/O is done with [`std::net::TcpStream`] so this crate pulls in
//! nothing beyond `serde`, `serde_json`, and `thiserror`. The optional
//! `tokio` dependency is gated behind the `async-runtime` feature and is
//! reserved for future async wrappers.

use std::io::{Read, Write};
use std::net::{Shutdown, TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum EmbeddingError {
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("http error: {status} {body}")]
    Http { status: u16, body: String },
    #[error("response parse error: {0}")]
    Parse(String),
    #[error("dimension mismatch: expected {expected}, got {actual}")]
    DimensionMismatch { expected: usize, actual: usize },
    #[error("timeout after {0} seconds")]
    Timeout(u64),
    #[error("circuit breaker is open: {reason}")]
    CircuitOpen { reason: String },
}

pub trait EmbeddingProvider: Send + Sync {
    fn name(&self) -> &str;
    fn dimensions(&self) -> usize;
    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError>;
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a_64(bytes: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[derive(Debug, Clone)]
pub struct HashEmbeddingProvider {
    dimensions: usize,
}

impl HashEmbeddingProvider {
    pub fn new(dimensions: usize) -> Self {
        let dimensions = if dimensions == 0 { 1 } else { dimensions };
        Self { dimensions }
    }
}

impl Default for HashEmbeddingProvider {
    fn default() -> Self {
        Self::new(384)
    }
}

impl EmbeddingProvider for HashEmbeddingProvider {
    fn name(&self) -> &str {
        "hash"
    }

    fn dimensions(&self) -> usize {
        self.dimensions
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        Ok(texts
            .iter()
            .map(|text| hash_embed(text, self.dimensions))
            .collect())
    }
}

fn hash_embed(text: &str, dimensions: usize) -> Vec<f32> {
    let mut vector = vec![0.0f32; dimensions];
    if text.trim().is_empty() {
        vector[0] = 1.0;
        return vector;
    }

    for token in text.split_whitespace() {
        let normalized = token.to_ascii_lowercase();
        let hash = fnv1a_64(normalized.as_bytes());
        let index = (hash % dimensions as u64) as usize;
        let sign = if (hash >> 63) & 1 == 0 { 1.0 } else { -1.0 };
        vector[index] += sign;
    }

    let norm_sq: f32 = vector.iter().map(|v| v * v).sum();
    if norm_sq > 0.0 {
        let norm = norm_sq.sqrt();
        for v in &mut vector {
            *v /= norm;
        }
    } else {
        vector[0] = 1.0;
    }
    vector
}

#[derive(Debug, Clone)]
pub struct OllamaEmbeddingProvider {
    model: String,
    endpoint: String,
    timeout: Duration,
}

impl OllamaEmbeddingProvider {
    pub const DEFAULT_ENDPOINT: &'static str = "http://localhost:11434/api/embeddings";
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    pub fn new(model: String, endpoint: Option<String>) -> Self {
        Self {
            model,
            endpoint: endpoint.unwrap_or_else(|| Self::DEFAULT_ENDPOINT.to_string()),
            timeout: Self::DEFAULT_TIMEOUT,
        }
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl EmbeddingProvider for OllamaEmbeddingProvider {
    fn name(&self) -> &str {
        "ollama"
    }

    fn dimensions(&self) -> usize {
        0
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let mut out = Vec::with_capacity(texts.len());
        for text in texts {
            let body = serde_json::json!({
                "model": self.model,
                "prompt": text,
            })
            .to_string();
            let (status, response_body) =
                http_post(&self.endpoint, &body, &[], self.timeout)?;
            if !(200..300).contains(&status) {
                return Err(EmbeddingError::Http {
                    status,
                    body: response_body,
                });
            }
            let response: OllamaResponse = serde_json::from_str(&response_body)
                .map_err(|e| EmbeddingError::Parse(format!("ollama: {e}")))?;
            out.push(response.embedding);
        }
        Ok(out)
    }
}

#[derive(Debug, Clone)]
pub struct OpenAIEmbeddingProvider {
    model: String,
    api_key: String,
    endpoint: String,
    timeout: Duration,
}

impl OpenAIEmbeddingProvider {
    pub const DEFAULT_MODEL: &'static str = "text-embedding-3-small";
    pub const DEFAULT_ENDPOINT: &'static str = "https://api.openai.com/v1/embeddings";
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

    pub fn new(model: String, api_key: String) -> Result<Self, EmbeddingError> {
        if api_key.trim().is_empty() {
            return Err(EmbeddingError::InvalidConfig(
                "api_key must not be empty".to_string(),
            ));
        }
        let model = if model.trim().is_empty() {
            Self::DEFAULT_MODEL.to_string()
        } else {
            model
        };
        Ok(Self {
            model,
            api_key,
            endpoint: Self::DEFAULT_ENDPOINT.to_string(),
            timeout: Self::DEFAULT_TIMEOUT,
        })
    }

    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
        self
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

impl EmbeddingProvider for OpenAIEmbeddingProvider {
    fn name(&self) -> &str {
        "openai"
    }

    fn dimensions(&self) -> usize {
        0
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let body = serde_json::json!({
            "input": texts,
            "model": self.model,
        })
        .to_string();
        let auth_header = format!("Bearer {}", self.api_key);
        let (status, response_body) = http_post(
            &self.endpoint,
            &body,
            &[("Authorization", &auth_header)],
            self.timeout,
        )?;
        if !(200..300).contains(&status) {
            return Err(EmbeddingError::Http {
                status,
                body: response_body,
            });
        }
        let response: OpenAIResponse = serde_json::from_str(&response_body)
            .map_err(|e| EmbeddingError::Parse(format!("openai: {e}")))?;
        if response.data.len() != texts.len() {
            return Err(EmbeddingError::Parse(format!(
                "openai: expected {} embeddings, got {}",
                texts.len(),
                response.data.len()
            )));
        }
        Ok(response.data.into_iter().map(|item| item.embedding).collect())
    }
}

#[derive(Debug, Deserialize)]
struct OllamaResponse {
    embedding: Vec<f32>,
}

#[derive(Debug, Deserialize)]
struct OpenAIResponse {
    data: Vec<OpenAIEmbeddingItem>,
}

#[derive(Debug, Deserialize)]
struct OpenAIEmbeddingItem {
    embedding: Vec<f32>,
}

#[derive(Debug)]
struct ParsedUrl {
    host: String,
    port: u16,
    path: String,
}

fn parse_url(url: &str) -> Result<ParsedUrl, EmbeddingError> {
    let (scheme, rest) = url.split_once("://").ok_or_else(|| {
        EmbeddingError::InvalidConfig(format!("url missing scheme: {url}"))
    })?;

    let (authority, path) = match rest.find('/') {
        Some(idx) => (&rest[..idx], &rest[idx..]),
        None => (rest, "/"),
    };

    let (host, port) = match authority.rfind(':') {
        Some(idx) => {
            let port_str = &authority[idx + 1..];
            let port = port_str.parse::<u16>().map_err(|_| {
                EmbeddingError::InvalidConfig(format!("invalid port: {port_str}"))
            })?;
            (&authority[..idx], port)
        }
        None => {
            let default_port = match scheme {
                "https" => 443,
                _ => 80,
            };
            (authority, default_port)
        }
    };

    Ok(ParsedUrl {
        host: host.to_string(),
        port,
        path: path.to_string(),
    })
}

fn http_post(
    endpoint: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
    timeout: Duration,
) -> Result<(u16, String), EmbeddingError> {
    let parsed = parse_url(endpoint)?;

    let addr = (parsed.host.as_str(), parsed.port)
        .to_socket_addrs()
        .map_err(|e| {
            EmbeddingError::Io(format!(
                "resolve {}:{}: {}",
                parsed.host, parsed.port, e
            ))
        })?
        .next()
        .ok_or_else(|| {
            EmbeddingError::Io(format!("no address for {}:{}", parsed.host, parsed.port))
        })?;

    let stream = TcpStream::connect_timeout(&addr, timeout)
        .map_err(|e| map_io_error(e, timeout))?;
    stream.set_read_timeout(Some(timeout)).ok();
    stream.set_write_timeout(Some(timeout)).ok();

    let mut request = format!(
        "POST {path} HTTP/1.1\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {len}\r\nConnection: close\r\n",
        path = parsed.path,
        host = parsed.host,
        len = body.len(),
    );
    for (key, value) in extra_headers {
        request.push_str(&format!("{key}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);

    let mut stream = stream;
    stream
        .write_all(request.as_bytes())
        .map_err(|e| map_io_error(e, timeout))?;
    stream.shutdown(Shutdown::Write).ok();

    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .map_err(|e| map_io_error(e, timeout))?;

    let response_str = String::from_utf8(response)
        .map_err(|e| EmbeddingError::Parse(format!("response is not utf-8: {e}")))?;

    let (headers, body) = response_str
        .split_once("\r\n\r\n")
        .ok_or_else(|| EmbeddingError::Parse("response missing header/body separator".to_string()))?;

    let status = parse_status(headers)?;
    Ok((status, body.to_string()))
}

fn map_io_error(err: std::io::Error, timeout: Duration) -> EmbeddingError {
    if matches!(
        err.kind(),
        std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
    ) {
        EmbeddingError::Timeout(timeout.as_secs())
    } else {
        EmbeddingError::Io(err.to_string())
    }
}

fn parse_status(headers: &str) -> Result<u16, EmbeddingError> {
    let first_line = headers
        .lines()
        .next()
        .ok_or_else(|| EmbeddingError::Parse("empty response".to_string()))?;
    let mut parts = first_line.split_whitespace();
    let _version = parts.next();
    let code = parts
        .next()
        .ok_or_else(|| EmbeddingError::Parse(format!("missing status code: {first_line}")))?;
    code.parse::<u16>()
        .map_err(|_| EmbeddingError::Parse(format!("invalid status code: {code}")))
}

// ---------------------------------------------------------------------------
// Circuit breaker
// ---------------------------------------------------------------------------
//
// `CircuitBreaker` and `CircuitBreakerProvider` wrap any `EmbeddingProvider`
// and short-circuit calls when the provider has been failing repeatedly.
//
// State machine:
//   Closed   -> on `failure_threshold` consecutive failures:  Open
//   Open     -> after `reset_timeout` since the breaker opened: HalfOpen
//   HalfOpen -> on successful probe:  Closed
//   HalfOpen -> on failed probe:      Open (timer restarts)
//
// `Closed` is the steady state and lets every request through. `Open` rejects
// every request immediately with `EmbeddingError::CircuitOpen`. `HalfOpen`
// lets a single request through as a probe; if it succeeds, the breaker
// closes; if it fails, the breaker reopens for another `reset_timeout`.
//
// This is opt-in: callers compose `CircuitBreakerProvider` around the inner
// provider explicitly, so the default behavior of `OpenAIEmbeddingProvider`
// and `OllamaEmbeddingProvider` is unchanged.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitState {
    pub fn as_label(self) -> &'static str {
        match self {
            CircuitState::Closed => "closed",
            CircuitState::Open => "open",
            CircuitState::HalfOpen => "half_open",
        }
    }
}

#[derive(Debug)]
pub struct CircuitBreaker {
    failure_threshold: u32,
    reset_timeout: Duration,
    state: Mutex<CircuitStateInner>,
}

#[derive(Debug)]
struct CircuitStateInner {
    kind: CircuitState,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, reset_timeout: Duration) -> Self {
        let failure_threshold = failure_threshold.max(1);
        Self {
            failure_threshold,
            reset_timeout,
            state: Mutex::new(CircuitStateInner {
                kind: CircuitState::Closed,
                consecutive_failures: 0,
                opened_at: None,
            }),
        }
    }

    pub fn state(&self) -> CircuitState {
        let mut guard = self.state.lock().expect("circuit breaker mutex poisoned");
        self.maybe_transition_to_half_open(&mut guard);
        guard.kind
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.state
            .lock()
            .expect("circuit breaker mutex poisoned")
            .consecutive_failures
    }

    /// Acquire a permit for one call. Returns `Ok(())` if the call is
    /// allowed, or `Err(EmbeddingError::CircuitOpen { .. })` if the
    /// breaker is open and the reset window has not elapsed.
    pub fn try_acquire(&self) -> Result<(), EmbeddingError> {
        let mut guard = self.state.lock().expect("circuit breaker mutex poisoned");
        self.maybe_transition_to_half_open(&mut guard);
        match guard.kind {
            CircuitState::Closed => Ok(()),
            CircuitState::HalfOpen => Ok(()),
            CircuitState::Open => Err(EmbeddingError::CircuitOpen {
                reason: format!(
                    "breaker open after {} consecutive failures; reset in {:?}",
                    guard.consecutive_failures, self.reset_timeout
                ),
            }),
        }
    }

    pub fn record_success(&self) {
        let mut guard = self.state.lock().expect("circuit breaker mutex poisoned");
        guard.kind = CircuitState::Closed;
        guard.consecutive_failures = 0;
        guard.opened_at = None;
    }

    pub fn record_failure(&self) {
        let mut guard = self.state.lock().expect("circuit breaker mutex poisoned");
        guard.consecutive_failures = guard.consecutive_failures.saturating_add(1);
        let should_open = guard.kind == CircuitState::HalfOpen
            || guard.consecutive_failures >= self.failure_threshold;
        if should_open {
            guard.kind = CircuitState::Open;
            guard.opened_at = Some(Instant::now());
        }
    }

    fn maybe_transition_to_half_open(&self, guard: &mut CircuitStateInner) {
        if guard.kind != CircuitState::Open {
            return;
        }
        if let Some(opened_at) = guard.opened_at
            && opened_at.elapsed() >= self.reset_timeout
        {
            guard.kind = CircuitState::HalfOpen;
        }
    }
}

pub struct CircuitBreakerProvider<P: EmbeddingProvider> {
    inner: P,
    breaker: Arc<CircuitBreaker>,
}

impl<P: EmbeddingProvider> CircuitBreakerProvider<P> {
    pub fn new(inner: P, breaker: Arc<CircuitBreaker>) -> Self {
        Self { inner, breaker }
    }

    pub fn breaker(&self) -> Arc<CircuitBreaker> {
        Arc::clone(&self.breaker)
    }

    pub fn inner(&self) -> &P {
        &self.inner
    }
}

impl<P: EmbeddingProvider> EmbeddingProvider for CircuitBreakerProvider<P> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn dimensions(&self) -> usize {
        self.inner.dimensions()
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        self.breaker.try_acquire()?;
        match self.inner.embed(texts) {
            Ok(v) => {
                self.breaker.record_success();
                Ok(v)
            }
            Err(e) => {
                self.breaker.record_failure();
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener};
    use std::time::Duration;

    fn spawn_mock_server(response: Vec<u8>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock listener");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 8192];
                let _ = stream.read(&mut buf);
                let _ = stream.write_all(&response);
                let _ = stream.shutdown(Shutdown::Write);
            }
        });
        port
    }

    fn spawn_slow_server(sleep: Duration) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind slow listener");
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((_stream, _)) = listener.accept() {
                std::thread::sleep(sleep);
            }
        });
        port
    }

    #[test]
    fn hash_provider_is_deterministic() {
        let provider = HashEmbeddingProvider::new(128);
        let a = provider
            .embed(&["the same input string".to_string()])
            .expect("embed call should succeed");
        let b = provider
            .embed(&["the same input string".to_string()])
            .expect("embed call should succeed");
        assert_eq!(a, b);
    }

    #[test]
    fn hash_provider_returns_correct_dimensions() {
        let provider = HashEmbeddingProvider::new(256);
        let out = provider
            .embed(&["hello world".to_string()])
            .expect("embed call should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].len(), 256);
        assert_eq!(provider.dimensions(), 256);

        let default_provider = HashEmbeddingProvider::default();
        assert_eq!(default_provider.dimensions(), 384);
    }

    #[test]
    fn hash_provider_different_inputs_different_outputs() {
        let provider = HashEmbeddingProvider::default();
        let a = provider
            .embed(&["the quick brown fox jumps".to_string()])
            .expect("embed call should succeed");
        let b = provider
            .embed(&["completely different words here".to_string()])
            .expect("embed call should succeed");
        assert_ne!(a, b);
    }

    #[test]
    fn ollama_provider_parses_valid_response() {
        let body_json = r#"{"embedding":[0.1,0.2,0.3,0.4,0.5]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body_json.len(),
            body_json,
        );
        let port = spawn_mock_server(response.into_bytes());
        let endpoint = format!("http://127.0.0.1:{port}/api/embeddings");
        let provider = OllamaEmbeddingProvider::new("test-model".to_string(), Some(endpoint))
            .with_timeout(Duration::from_secs(2));
        let out = provider
            .embed(&["hello world".to_string()])
            .expect("embed call should succeed");
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].len(), 5);
        assert!((out[0][0] - 0.1).abs() < 1e-6);
    }

    #[test]
    fn openai_provider_rejects_empty_api_key() {
        let err = OpenAIEmbeddingProvider::new(
            "text-embedding-3-small".to_string(),
            "".to_string(),
        )
        .unwrap_err();
        match err {
            EmbeddingError::InvalidConfig(_) => {}
            other => panic!("expected InvalidConfig, got {other:?}"),
        }

        let err_whitespace = OpenAIEmbeddingProvider::new(
            "text-embedding-3-small".to_string(),
            "   ".to_string(),
        )
        .unwrap_err();
        match err_whitespace {
            EmbeddingError::InvalidConfig(_) => {}
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn openai_provider_parses_valid_response() {
        let body_json =
            r#"{"data":[{"embedding":[0.1,0.2,0.3]},{"embedding":[0.4,0.5,0.6]}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body_json.len(),
            body_json,
        );
        let port = spawn_mock_server(response.into_bytes());
        let endpoint = format!("http://127.0.0.1:{port}/v1/embeddings");
        let provider = OpenAIEmbeddingProvider::new(
            "text-embedding-3-small".to_string(),
            "sk-test".to_string(),
        )
        .expect("non-empty api key")
        .with_endpoint(endpoint)
        .with_timeout(Duration::from_secs(2));
        let out = provider
            .embed(&["first".to_string(), "second".to_string()])
            .expect("embed call should succeed");
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], vec![0.1, 0.2, 0.3]);
        assert_eq!(out[1], vec![0.4, 0.5, 0.6]);
    }

    #[test]
    fn http_provider_times_out_on_slow_response() {
        let port = spawn_slow_server(Duration::from_secs(5));
        let endpoint = format!("http://127.0.0.1:{port}/api/embeddings");
        let provider = OllamaEmbeddingProvider::new("test-model".to_string(), Some(endpoint))
            .with_timeout(Duration::from_millis(200));
        let result = provider.embed(&["hello".to_string()]);
        match result {
            Err(EmbeddingError::Timeout(_)) => {}
            other => panic!("expected Timeout error, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // Circuit breaker tests
    // -----------------------------------------------------------------

    /// Counting provider used to drive the breaker through a known
    /// sequence of successes and failures.
    struct CountingProvider {
        name: &'static str,
        dims: usize,
        outcomes: std::sync::Mutex<Vec<Result<Vec<Vec<f32>>, EmbeddingError>>>,
        calls: std::sync::atomic::AtomicUsize,
    }

    impl CountingProvider {
        fn new(name: &'static str, dims: usize, outcomes: Vec<Result<Vec<Vec<f32>>, EmbeddingError>>) -> Self {
            Self {
                name,
                dims,
                outcomes: std::sync::Mutex::new(outcomes),
                calls: std::sync::atomic::AtomicUsize::new(0),
            }
        }
        fn calls(&self) -> usize {
            self.calls.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl EmbeddingProvider for CountingProvider {
        fn name(&self) -> &str {
            self.name
        }
        fn dimensions(&self) -> usize {
            self.dims
        }
        fn embed(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let mut outcomes = self.outcomes.lock().expect("outcomes mutex poisoned");
            if outcomes.is_empty() {
                panic!("CountingProvider ran out of scripted outcomes");
            }
            outcomes.remove(0)
        }
    }

    #[test]
    fn circuit_breaker_starts_closed() {
        let breaker = CircuitBreaker::new(3, Duration::from_secs(30));
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert_eq!(breaker.consecutive_failures(), 0);
        breaker.try_acquire().expect("closed breaker allows calls");
    }

    #[test]
    fn circuit_breaker_opens_after_threshold_failures() {
        let breaker = CircuitBreaker::new(3, Duration::from_secs(30));
        for _ in 0..3 {
            breaker.record_failure();
        }
        assert_eq!(breaker.state(), CircuitState::Open);
        assert_eq!(breaker.consecutive_failures(), 3);
        let err = breaker.try_acquire().expect_err("open breaker rejects");
        match err {
            EmbeddingError::CircuitOpen { .. } => {}
            other => panic!("expected CircuitOpen, got {other:?}"),
        }
    }

    #[test]
    fn circuit_breaker_success_resets_failure_count() {
        let breaker = CircuitBreaker::new(3, Duration::from_secs(30));
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.consecutive_failures(), 2);
        breaker.record_success();
        assert_eq!(breaker.consecutive_failures(), 0);
        assert_eq!(breaker.state(), CircuitState::Closed);
    }

    #[test]
    fn circuit_breaker_transitions_to_half_open_after_timeout() {
        let breaker = CircuitBreaker::new(2, Duration::from_millis(50));
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        std::thread::sleep(Duration::from_millis(70));
        assert_eq!(breaker.state(), CircuitState::HalfOpen);
        // Half-open allows a probe
        breaker.try_acquire().expect("half-open allows probe");
    }

    #[test]
    fn circuit_breaker_successful_probe_closes_breaker() {
        let breaker = CircuitBreaker::new(2, Duration::from_millis(30));
        breaker.record_failure();
        breaker.record_failure();
        std::thread::sleep(Duration::from_millis(40));
        // State should be half-open now
        assert_eq!(breaker.state(), CircuitState::HalfOpen);
        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::Closed);
        assert_eq!(breaker.consecutive_failures(), 0);
    }

    #[test]
    fn circuit_breaker_failed_probe_reopens_breaker() {
        let breaker = CircuitBreaker::new(2, Duration::from_millis(30));
        breaker.record_failure();
        breaker.record_failure();
        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(breaker.state(), CircuitState::HalfOpen);
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        let err = breaker.try_acquire().expect_err("reopened breaker rejects");
        assert!(matches!(err, EmbeddingError::CircuitOpen { .. }));
    }

    #[test]
    fn circuit_breaker_provider_short_circuits_inner_provider() {
        // Inner provider fails 3 times then would succeed; the breaker
        // should trip after 2 failures and the inner provider should
        // not be called for the third request.
        let inner = CountingProvider::new(
            "counting",
            4,
            vec![
                Err(EmbeddingError::Io("first".to_string())),
                Err(EmbeddingError::Io("second".to_string())),
                Ok(vec![vec![1.0, 0.0, 0.0, 0.0]]),
            ],
        );
        let breaker = Arc::new(CircuitBreaker::new(2, Duration::from_secs(30)));
        let wrapped = CircuitBreakerProvider::new(inner, Arc::clone(&breaker));

        let err1 = wrapped.embed(&["a".to_string()]).expect_err("first fails");
        assert!(matches!(err1, EmbeddingError::Io(_)));
        let err2 = wrapped.embed(&["b".to_string()]).expect_err("second fails");
        assert!(matches!(err2, EmbeddingError::Io(_)));

        // Now the breaker is open, the third call should be short-circuited
        // and return CircuitOpen without consulting the inner provider.
        let err3 = wrapped.embed(&["c".to_string()]).expect_err("third short-circuits");
        match err3 {
            EmbeddingError::CircuitOpen { .. } => {}
            other => panic!("expected CircuitOpen, got {other:?}"),
        }

        // Inner provider should have been called exactly twice (the third
        // call was short-circuited).
        assert_eq!(wrapped.inner().calls(), 2);
    }

    #[test]
    fn circuit_breaker_provider_recovers_via_probe() {
        // Script: 2 failures, then 1 success, then 1 success.
        let inner = CountingProvider::new(
            "counting",
            4,
            vec![
                Err(EmbeddingError::Io("a".to_string())),
                Err(EmbeddingError::Io("b".to_string())),
                Ok(vec![vec![1.0, 0.0, 0.0, 0.0]]),
                Ok(vec![vec![0.0, 1.0, 0.0, 0.0]]),
            ],
        );
        let breaker = Arc::new(CircuitBreaker::new(2, Duration::from_millis(30)));
        let wrapped = CircuitBreakerProvider::new(inner, Arc::clone(&breaker));

        let _ = wrapped.embed(&["a".to_string()]).expect_err("a fails");
        let _ = wrapped.embed(&["b".to_string()]).expect_err("b fails");
        assert_eq!(breaker.state(), CircuitState::Open);

        // Wait for the reset window
        std::thread::sleep(Duration::from_millis(40));
        // Probe call: breaker is half-open, allows the call, inner succeeds,
        // breaker closes.
        let r = wrapped
            .embed(&["c".to_string()])
            .expect("probe succeeds");
        assert_eq!(r, vec![vec![1.0, 0.0, 0.0, 0.0]]);
        assert_eq!(breaker.state(), CircuitState::Closed);

        // Next call should pass through and the inner provider should be
        // called again (4 total invocations of the inner: 2 failed, 1 probe,
        // 1 post-recovery).
        let r2 = wrapped
            .embed(&["d".to_string()])
            .expect("post-recovery call succeeds");
        assert_eq!(r2, vec![vec![0.0, 1.0, 0.0, 0.0]]);
        assert_eq!(wrapped.inner().calls(), 4);
    }

    #[test]
    fn circuit_breaker_dimensions_passthrough() {
        let inner = HashEmbeddingProvider::new(96);
        let breaker = Arc::new(CircuitBreaker::new(3, Duration::from_secs(30)));
        let wrapped = CircuitBreakerProvider::new(inner, Arc::clone(&breaker));
        assert_eq!(wrapped.dimensions(), 96);
        assert_eq!(wrapped.name(), "hash");
    }
}
