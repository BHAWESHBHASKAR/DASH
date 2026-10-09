//! Embedding adapter crate.
//!
//! Provides a unified [`EmbeddingProvider`] trait with three concrete
//! implementations:
//!
//! * [`HashEmbeddingProvider`] - deterministic, dependency-free, useful as a
//!   fallback or for unit tests.
//! * [`OllamaEmbeddingProvider`] - talks to an Ollama daemon (`/api/embed`,
//!   or the legacy `/api/embeddings` when given that URL).
//! * [`OpenAIEmbeddingProvider`] - talks to OpenAI's `/v1/embeddings`
//!   endpoint using a Bearer token over https.
//!
//! HTTP I/O goes through [`http`], a bounded client (rustls TLS, no
//! redirects, size-capped bodies, jittered retries within a total deadline).
//! The optional `tokio` dependency is gated behind the `async-runtime`
//! feature and is reserved for future async wrappers.

pub mod http;

use std::sync::atomic::{AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Once};
use std::time::{Duration, Instant};

use serde::Deserialize;
use thiserror::Error;

pub use http::HttpOptions;

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
    #[error("response body exceeds the {limit} byte limit")]
    ResponseTooLarge { limit: usize },
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

/// Validate that every vector has exactly `expected` components.
///
/// `expected == 0` means "unknown" and only checks that all vectors in the
/// batch have the same length.
pub fn validate_dimensions(expected: usize, vectors: &[Vec<f32>]) -> Result<(), EmbeddingError> {
    let expected = if expected == 0 {
        match vectors.first() {
            Some(v) => v.len(),
            None => return Ok(()),
        }
    } else {
        expected
    };
    for v in vectors {
        if v.len() != expected {
            return Err(EmbeddingError::DimensionMismatch {
                expected,
                actual: v.len(),
            });
        }
    }
    Ok(())
}

/// Validate `vectors` against `provider.dimensions()`.
pub fn validate_provider_output(
    provider: &dyn EmbeddingProvider,
    vectors: &[Vec<f32>],
) -> Result<(), EmbeddingError> {
    validate_dimensions(provider.dimensions(), vectors)
}

/// Remembers the dimension learned from the first successful response and
/// rejects later responses that disagree.
#[derive(Debug, Default)]
struct DimensionCache(AtomicUsize);

impl DimensionCache {
    fn get(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }

    fn set(&self, dims: usize) {
        self.0.store(dims, Ordering::Release);
    }

    /// Check the batch against the known dimension, learning it if unknown.
    fn check_and_learn(&self, vectors: &[Vec<f32>]) -> Result<(), EmbeddingError> {
        validate_dimensions(self.get(), vectors)?;
        if let Some(first) = vectors.first() {
            // First writer wins; a concurrent different value is caught on
            // the next call.
            let _ = self
                .0
                .compare_exchange(0, first.len(), Ordering::AcqRel, Ordering::Acquire);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Ollama
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct OllamaEmbeddingProvider {
    model: String,
    endpoint: String,
    http: HttpOptions,
    dims: Arc<DimensionCache>,
}

impl OllamaEmbeddingProvider {
    /// Default endpoint: the batch `/api/embed` API of a local daemon.
    pub const DEFAULT_ENDPOINT: &'static str = "http://localhost:11434/api/embed";
    pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

    /// `endpoint` may be a bare base URL (`http://host:11434`, with or without
    /// a trailing slash) or a full `/api/embed` / `/api/embeddings` URL.
    pub fn new(model: String, endpoint: Option<String>) -> Self {
        let raw = endpoint.unwrap_or_else(|| Self::DEFAULT_ENDPOINT.to_string());
        let endpoint = resolve_ollama_endpoint(&raw).unwrap_or(raw);
        Self {
            model,
            endpoint,
            http: HttpOptions::with_total_timeout(Self::DEFAULT_TIMEOUT),
            dims: Arc::new(DimensionCache::default()),
        }
    }

    /// Total deadline for one `embed` call, including retries.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.http.total_timeout = timeout;
        self
    }

    pub fn with_http_options(mut self, http: HttpOptions) -> Self {
        self.http = http;
        self
    }

    /// Declare the model's dimensionality up front instead of learning it.
    pub fn with_dimensions(self, dimensions: usize) -> Self {
        self.dims.set(dimensions);
        self
    }

    pub fn model(&self) -> &str {
        &self.model
    }

    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}

/// Normalize an Ollama base or full URL to the embedding endpoint URL.
///
/// * `http://h:11434` and `http://h:11434/` -> `http://h:11434/api/embed`
/// * `http://h:11434/api` -> `http://h:11434/api/embed`
/// * `.../api/embed` and `.../api/embeddings` are kept as given.
/// * Any other path prefix (reverse proxy) gets `/api/embed` appended.
pub fn resolve_ollama_endpoint(raw: &str) -> Result<String, EmbeddingError> {
    let mut url = http::parse_endpoint(raw)?;
    url.set_query(None);
    url.set_fragment(None);
    let path = url.path().trim_end_matches('/').to_string();
    let new_path = if path.ends_with("/api/embed") || path.ends_with("/api/embeddings") {
        path
    } else if path.ends_with("/api") {
        format!("{path}/embed")
    } else {
        format!("{path}/api/embed")
    };
    url.set_path(&new_path);
    Ok(url.to_string())
}

/// Pick the Ollama endpoint from the two env spellings. `DASH_OLLAMA_ENDPOINT`
/// wins; `DASH_OLLAMA_BASE_URL` is a deprecated alias. The bool is true when
/// the deprecated alias was used.
pub fn ollama_endpoint_from_values(
    endpoint: Option<String>,
    base_url: Option<String>,
) -> Option<(String, bool)> {
    let non_empty = |v: Option<String>| v.filter(|s| !s.trim().is_empty());
    match (non_empty(endpoint), non_empty(base_url)) {
        (Some(e), _) => Some((e, false)),
        (None, Some(b)) => Some((b, true)),
        (None, None) => None,
    }
}

#[derive(Debug, Deserialize)]
struct OllamaResponse {
    /// Legacy `/api/embeddings` shape.
    #[serde(default)]
    embedding: Option<Vec<f32>>,
    /// `/api/embed` shape.
    #[serde(default)]
    embeddings: Option<Vec<Vec<f32>>>,
}

impl OllamaEmbeddingProvider {
    fn call(&self, url: &url::Url, body: String) -> Result<OllamaResponse, EmbeddingError> {
        let text = http::post_json(url, &body, &[], &self.http, &[])?;
        serde_json::from_str(&text).map_err(|e| EmbeddingError::Parse(format!("ollama: {e}")))
    }
}

impl EmbeddingProvider for OllamaEmbeddingProvider {
    fn name(&self) -> &str {
        "ollama"
    }

    /// Declared or learned dimension; `0` until the first successful call.
    fn dimensions(&self) -> usize {
        self.dims.get()
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let url = http::parse_endpoint(&self.endpoint)?;
        let legacy = url
            .path()
            .trim_end_matches('/')
            .ends_with("/api/embeddings");
        let out: Vec<Vec<f32>> = if legacy {
            let mut out = Vec::with_capacity(texts.len());
            for text in texts {
                let body = serde_json::json!({ "model": self.model, "prompt": text }).to_string();
                let resp = self.call(&url, body)?;
                out.push(resp.embedding.ok_or_else(|| {
                    EmbeddingError::Parse("ollama: response missing 'embedding'".to_string())
                })?);
            }
            out
        } else {
            let body = serde_json::json!({ "model": self.model, "input": texts }).to_string();
            let resp = self.call(&url, body)?;
            let vectors = resp.embeddings.ok_or_else(|| {
                EmbeddingError::Parse("ollama: response missing 'embeddings'".to_string())
            })?;
            if vectors.len() != texts.len() {
                return Err(EmbeddingError::Parse(format!(
                    "ollama: expected {} embeddings, got {}",
                    texts.len(),
                    vectors.len()
                )));
            }
            vectors
        };
        self.dims.check_and_learn(&out)?;
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// OpenAI
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct OpenAIEmbeddingProvider {
    model: String,
    api_key: String,
    endpoint: String,
    http: HttpOptions,
    /// Value sent as the `dimensions` request parameter, if any.
    requested_dimensions: Option<usize>,
    dims: Arc<DimensionCache>,
    allow_insecure_http: bool,
}

// Manual Debug so the API key can never be printed.
impl std::fmt::Debug for OpenAIEmbeddingProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenAIEmbeddingProvider")
            .field("model", &self.model)
            .field("endpoint", &self.endpoint)
            .field("api_key", &"[redacted]")
            .finish()
    }
}

/// Native output dimensions of known OpenAI embedding models.
pub fn openai_model_dimensions(model: &str) -> Option<usize> {
    match model {
        "text-embedding-3-small" => Some(1536),
        "text-embedding-3-large" => Some(3072),
        "text-embedding-ada-002" => Some(1536),
        _ => None,
    }
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
        let dims = DimensionCache::default();
        if let Some(native) = openai_model_dimensions(&model) {
            dims.set(native);
        }
        Ok(Self {
            model,
            api_key,
            endpoint: Self::DEFAULT_ENDPOINT.to_string(),
            http: HttpOptions::with_total_timeout(Self::DEFAULT_TIMEOUT),
            requested_dimensions: None,
            dims: Arc::new(dims),
            allow_insecure_http: http::insecure_http_allowed_from_env(),
        })
    }

    /// Total deadline for one `embed` call, including retries.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.http.total_timeout = timeout;
        self
    }

    pub fn with_http_options(mut self, http: HttpOptions) -> Self {
        self.http = http;
        self
    }

    pub fn with_endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = endpoint;
        self
    }

    /// Explicitly allow (or forbid) sending the API key over plaintext http
    /// to non-loopback hosts. Defaults to the
    /// `DASH_EMBEDDING_ALLOW_INSECURE_HTTP` environment variable.
    pub fn with_allow_insecure_http(mut self, allow: bool) -> Self {
        self.allow_insecure_http = allow;
        self
    }

    /// Request shortened embeddings via the API `dimensions` parameter.
    /// Only the `text-embedding-3-*` models support it.
    pub fn with_dimensions(mut self, dimensions: usize) -> Result<Self, EmbeddingError> {
        if dimensions == 0 {
            return Err(EmbeddingError::InvalidConfig(
                "dimensions must be greater than zero".to_string(),
            ));
        }
        if !self.model.starts_with("text-embedding-3-") {
            return Err(EmbeddingError::InvalidConfig(format!(
                "model '{}' does not support the dimensions parameter",
                self.model
            )));
        }
        if let Some(native) = openai_model_dimensions(&self.model)
            && dimensions > native
        {
            return Err(EmbeddingError::InvalidConfig(format!(
                "dimensions {dimensions} exceeds native size {native} of '{}'",
                self.model
            )));
        }
        self.requested_dimensions = Some(dimensions);
        self.dims = Arc::new({
            let d = DimensionCache::default();
            d.set(dimensions);
            d
        });
        Ok(self)
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

    /// Native size of known models, the requested `dimensions`, or the size
    /// learned from the first response for unknown models (`0` before that).
    fn dimensions(&self) -> usize {
        self.dims.get()
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let url = http::parse_endpoint(&self.endpoint)?;
        http::ensure_secure_transport(&url, true, self.allow_insecure_http)?;

        let mut payload = serde_json::json!({
            "input": texts,
            "model": self.model,
        });
        if let Some(d) = self.requested_dimensions {
            payload["dimensions"] = serde_json::json!(d);
        }
        let auth_header = format!("Bearer {}", self.api_key);
        let text = http::post_json(
            &url,
            &payload.to_string(),
            &[("Authorization", &auth_header)],
            &self.http,
            &[&self.api_key],
        )?;
        let response: OpenAIResponse = serde_json::from_str(&text)
            .map_err(|e| EmbeddingError::Parse(format!("openai: {e}")))?;
        if response.data.len() != texts.len() {
            return Err(EmbeddingError::Parse(format!(
                "openai: expected {} embeddings, got {}",
                texts.len(),
                response.data.len()
            )));
        }
        let mut items = response.data;
        if items.iter().all(|i| i.index.is_some()) {
            items.sort_by_key(|i| i.index);
        }
        let out: Vec<Vec<f32>> = items.into_iter().map(|i| i.embedding).collect();
        self.dims.check_and_learn(&out)?;
        Ok(out)
    }
}

#[derive(Debug, Deserialize)]
struct OpenAIResponse {
    data: Vec<OpenAIEmbeddingItem>,
}

#[derive(Debug, Deserialize)]
struct OpenAIEmbeddingItem {
    embedding: Vec<f32>,
    #[serde(default)]
    index: Option<usize>,
}

// ---------------------------------------------------------------------------
// Circuit breaker
// ---------------------------------------------------------------------------
//
// `CircuitBreaker` and `CircuitBreakerProvider` wrap any `EmbeddingProvider`
// and short-circuit calls when the provider has been failing repeatedly.
//
// State machine (lock-free, atomic):
//   Closed   -> on `failure_threshold` consecutive failures:  Open
//   Open     -> after `reset_timeout`, exactly ONE caller wins a
//               compare-and-swap Open -> HalfOpen and becomes the probe
//   HalfOpen -> probe success: Closed; probe failure: Open (timer restarts)
//
// While a probe is in flight (HalfOpen) every other caller fails fast with
// `EmbeddingError::CircuitOpen`. If a probe never reports back (for example
// its thread died), another probe is admitted after another `reset_timeout`.
//
// This is opt-in: callers compose `CircuitBreakerProvider` around the inner
// provider explicitly.

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

const STATE_CLOSED: u8 = 0;
const STATE_OPEN: u8 = 1;
/// A probe has been admitted and has not yet reported.
const STATE_HALF_OPEN: u8 = 2;

#[derive(Debug)]
pub struct CircuitBreaker {
    failure_threshold: u32,
    reset_timeout: Duration,
    epoch: Instant,
    state: AtomicU8,
    consecutive_failures: AtomicU32,
    /// Nanoseconds since `epoch` (+1) when the breaker opened or the current
    /// probe was admitted. 0 means unset.
    stamp_nanos: AtomicU64,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, reset_timeout: Duration) -> Self {
        Self {
            failure_threshold: failure_threshold.max(1),
            reset_timeout,
            epoch: Instant::now(),
            state: AtomicU8::new(STATE_CLOSED),
            consecutive_failures: AtomicU32::new(0),
            stamp_nanos: AtomicU64::new(0),
        }
    }

    fn now_stamp(&self) -> u64 {
        (self.epoch.elapsed().as_nanos() as u64).saturating_add(1)
    }

    fn window_elapsed(&self, stamp: u64) -> bool {
        if stamp == 0 {
            return true;
        }
        self.now_stamp().saturating_sub(stamp) >= self.reset_timeout.as_nanos() as u64
    }

    /// Observed state. An `Open` breaker whose reset window has elapsed is
    /// reported as `HalfOpen` (a probe may now be admitted); this does not
    /// consume the probe.
    pub fn state(&self) -> CircuitState {
        match self.state.load(Ordering::Acquire) {
            STATE_CLOSED => CircuitState::Closed,
            STATE_OPEN => {
                if self.window_elapsed(self.stamp_nanos.load(Ordering::Acquire)) {
                    CircuitState::HalfOpen
                } else {
                    CircuitState::Open
                }
            }
            _ => CircuitState::HalfOpen,
        }
    }

    pub fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures.load(Ordering::Acquire)
    }

    fn open_error(&self) -> EmbeddingError {
        EmbeddingError::CircuitOpen {
            reason: format!(
                "breaker open after {} consecutive failures; reset in {:?}",
                self.consecutive_failures(),
                self.reset_timeout
            ),
        }
    }

    /// Acquire a permit for one call. Returns `Ok(())` if the call is
    /// allowed (closed, or this caller is the single half-open probe), or
    /// `Err(EmbeddingError::CircuitOpen { .. })` otherwise.
    pub fn try_acquire(&self) -> Result<(), EmbeddingError> {
        loop {
            match self.state.load(Ordering::Acquire) {
                STATE_CLOSED => return Ok(()),
                observed @ (STATE_OPEN | STATE_HALF_OPEN) => {
                    let stamp = self.stamp_nanos.load(Ordering::Acquire);
                    if !self.window_elapsed(stamp) {
                        return Err(self.open_error());
                    }
                    // Window elapsed: race to become the (only) probe. For a
                    // stale HalfOpen the stamp CAS below arbitrates.
                    let new_stamp = self.now_stamp();
                    if self
                        .stamp_nanos
                        .compare_exchange(stamp, new_stamp, Ordering::AcqRel, Ordering::Acquire)
                        .is_err()
                    {
                        return Err(self.open_error());
                    }
                    if self
                        .state
                        .compare_exchange(
                            observed,
                            STATE_HALF_OPEN,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(());
                    }
                    // State changed under us (success/failure recorded);
                    // re-evaluate.
                }
                _ => unreachable!("invalid circuit breaker state"),
            }
        }
    }

    pub fn record_success(&self) {
        self.consecutive_failures.store(0, Ordering::Release);
        self.stamp_nanos.store(0, Ordering::Release);
        self.state.store(STATE_CLOSED, Ordering::Release);
    }

    pub fn record_failure(&self) {
        let failures = self
            .consecutive_failures
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| {
                Some(v.saturating_add(1))
            })
            .unwrap_or(u32::MAX)
            .saturating_add(1);
        let state = self.state.load(Ordering::Acquire);
        if state != STATE_CLOSED || failures >= self.failure_threshold {
            self.stamp_nanos.store(self.now_stamp(), Ordering::Release);
            self.state.store(STATE_OPEN, Ordering::Release);
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

/// Records a failure if the guarded call unwinds, so a panicking probe cannot
/// wedge the breaker half-open.
struct ProbeGuard<'a> {
    breaker: &'a CircuitBreaker,
    done: bool,
}

impl Drop for ProbeGuard<'_> {
    fn drop(&mut self) {
        if !self.done {
            self.breaker.record_failure();
        }
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
        let mut guard = ProbeGuard {
            breaker: &self.breaker,
            done: false,
        };
        let result = self.inner.embed(texts);
        guard.done = true;
        match &result {
            Ok(_) => self.breaker.record_success(),
            Err(_) => self.breaker.record_failure(),
        }
        result
    }
}

/// Build an [`EmbeddingProvider`] from the process environment. This is the
/// single place service binaries read `DASH_EMBEDDING_PROVIDER` so the
/// selection logic stays consistent between ingestion and retrieval.
///
/// Reads:
/// - `DASH_EMBEDDING_PROVIDER` — `"hash"` (default, deterministic, no
///   network), `"ollama"`, or `"openai"`. Unknown values fall back to `hash`
///   with a warning.
/// - For `ollama`: `DASH_OLLAMA_ENDPOINT` (default `http://localhost:11434`;
///   a bare base URL is expanded to `/api/embed`), with
///   `DASH_OLLAMA_BASE_URL` accepted as a deprecated alias, and
///   `DASH_OLLAMA_MODEL` (default `nomic-embed-text`).
/// - For `openai`: `DASH_OPENAI_API_KEY` (required; error if missing),
///   `DASH_OPENAI_MODEL` (default `text-embedding-3-small`).
/// - `DASH_EMBEDDING_ALLOW_INSECURE_HTTP=1` allows sending the OpenAI key
///   over plaintext http to non-loopback hosts (off by default).
pub fn select_embedding_provider_from_env() -> Box<dyn EmbeddingProvider + Send + Sync + 'static> {
    let provider = std::env::var("DASH_EMBEDDING_PROVIDER")
        .unwrap_or_else(|_| "hash".to_string())
        .to_ascii_lowercase();

    match provider.as_str() {
        "ollama" => {
            let selected = ollama_endpoint_from_values(
                std::env::var("DASH_OLLAMA_ENDPOINT").ok(),
                std::env::var("DASH_OLLAMA_BASE_URL").ok(),
            );
            let endpoint = match selected {
                Some((value, deprecated)) => {
                    if deprecated {
                        warn_deprecated_ollama_base_url();
                    }
                    value
                }
                None => "http://localhost:11434".to_string(),
            };
            let model = std::env::var("DASH_OLLAMA_MODEL")
                .unwrap_or_else(|_| "nomic-embed-text".to_string());
            Box::new(OllamaEmbeddingProvider::new(model, Some(endpoint)))
        }
        "openai" => {
            let key = std::env::var("DASH_OPENAI_API_KEY").unwrap_or_default();
            let model = std::env::var("DASH_OPENAI_MODEL")
                .unwrap_or_else(|_| "text-embedding-3-small".to_string());
            match OpenAIEmbeddingProvider::new(model, key) {
                Ok(p) => Box::new(p),
                Err(e) => {
                    eprintln!(
                        "dash: failed to build OpenAI embedding provider ({e}); falling back to hash"
                    );
                    Box::new(HashEmbeddingProvider::default())
                }
            }
        }
        "hash" => Box::new(HashEmbeddingProvider::default()),
        other => {
            eprintln!("dash: unknown DASH_EMBEDDING_PROVIDER='{other}'; falling back to hash");
            Box::new(HashEmbeddingProvider::default())
        }
    }
}

fn warn_deprecated_ollama_base_url() {
    static WARNED: Once = Once::new();
    WARNED.call_once(|| {
        eprintln!("dash: DASH_OLLAMA_BASE_URL is deprecated; use DASH_OLLAMA_ENDPOINT instead");
    });
}

/// Returns the configured provider name (or `"hash"` when unset) without
/// building the provider. Useful for startup banners and strict-mode checks.
pub fn embedding_provider_name_from_env() -> String {
    std::env::var("DASH_EMBEDDING_PROVIDER")
        .unwrap_or_else(|_| "hash".to_string())
        .to_ascii_lowercase()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::{Shutdown, TcpListener};
    use std::time::Duration;

    struct MockServer {
        port: u16,
        requests: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl MockServer {
        /// Serve one canned response per accepted connection, in order.
        fn start(responses: Vec<Vec<u8>>) -> MockServer {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock listener");
            let port = listener.local_addr().unwrap().port();
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let captured = Arc::clone(&requests);
            std::thread::spawn(move || {
                for response in responses {
                    let Ok((mut stream, _)) = listener.accept() else {
                        return;
                    };
                    let request = read_request(&mut stream);
                    captured.lock().unwrap().push(request);
                    let _ = stream.write_all(&response);
                    let _ = stream.flush();
                    let _ = stream.shutdown(Shutdown::Both);
                }
            });
            MockServer { port, requests }
        }

        fn url(&self, path: &str) -> String {
            format!("http://127.0.0.1:{}{path}", self.port)
        }

        fn requests(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    fn read_request(stream: &mut std::net::TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut data = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            let n = match stream.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            data.extend_from_slice(&buf[..n]);
            if let Some(pos) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                let head = String::from_utf8_lossy(&data[..pos]).to_ascii_lowercase();
                let len = head
                    .lines()
                    .find_map(|l| l.strip_prefix("content-length:"))
                    .and_then(|v| v.trim().parse::<usize>().ok())
                    .unwrap_or(0);
                if data.len() >= pos + 4 + len {
                    break;
                }
            }
        }
        String::from_utf8_lossy(&data).into_owned()
    }

    fn json_response(status_line: &str, extra: &str, body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 {status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\n{extra}Connection: close\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn spawn_mock_server(response: Vec<u8>) -> u16 {
        MockServer::start(vec![response]).port
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
        let err =
            OpenAIEmbeddingProvider::new("text-embedding-3-small".to_string(), "".to_string())
                .unwrap_err();
        match err {
            EmbeddingError::InvalidConfig(_) => {}
            other => panic!("expected InvalidConfig, got {other:?}"),
        }

        let err_whitespace =
            OpenAIEmbeddingProvider::new("text-embedding-3-small".to_string(), "   ".to_string())
                .unwrap_err();
        match err_whitespace {
            EmbeddingError::InvalidConfig(_) => {}
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
    }

    #[test]
    fn openai_provider_parses_valid_response() {
        let body_json = r#"{"data":[{"embedding":[0.1,0.2,0.3]},{"embedding":[0.4,0.5,0.6]}]}"#;
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body_json.len(),
            body_json,
        );
        let port = spawn_mock_server(response.into_bytes());
        let endpoint = format!("http://127.0.0.1:{port}/v1/embeddings");
        let provider =
            OpenAIEmbeddingProvider::new("test-model".to_string(), "sk-test".to_string())
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
        fn new(
            name: &'static str,
            dims: usize,
            outcomes: Vec<Result<Vec<Vec<f32>>, EmbeddingError>>,
        ) -> Self {
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
            self.calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
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
        let err3 = wrapped
            .embed(&["c".to_string()])
            .expect_err("third short-circuits");
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
        let r = wrapped.embed(&["c".to_string()]).expect("probe succeeds");
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

    // -----------------------------------------------------------------
    // HTTP client (EMB-01, EMB-05, SEC-23)
    // -----------------------------------------------------------------

    fn fast_opts() -> HttpOptions {
        let mut o = HttpOptions::with_total_timeout(Duration::from_secs(5));
        o.backoff_base = Duration::from_millis(10);
        o.backoff_max = Duration::from_millis(50);
        o
    }

    fn openai_for(server: &MockServer) -> OpenAIEmbeddingProvider {
        OpenAIEmbeddingProvider::new("test-model".into(), "sk-secret-key".into())
            .unwrap()
            .with_endpoint(server.url("/v1/embeddings"))
            .with_http_options(fast_opts())
    }

    #[test]
    fn chunked_response_is_decoded() {
        let part1 = r#"{"embeddings":[[0.5,"#;
        let part2 = r#"0.25]]}"#;
        let body = format!(
            "{:x}\r\n{}\r\n{:x}\r\n{}\r\n0\r\n\r\n",
            part1.len(),
            part1,
            part2.len(),
            part2
        );
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{body}"
        );
        let server = MockServer::start(vec![response.into_bytes()]);
        let provider = OllamaEmbeddingProvider::new("m".into(), Some(server.url("")))
            .with_http_options(fast_opts());
        let out = provider.embed(&["hi".to_string()]).expect("chunked body");
        assert_eq!(out, vec![vec![0.5, 0.25]]);
        assert_eq!(provider.dimensions(), 2);
    }

    #[test]
    fn oversized_content_length_is_rejected() {
        let body = format!(r#"{{"data":[{{"embedding":[{}]}}]}}"#, "0.1,".repeat(2000));
        let server = MockServer::start(vec![json_response("200 OK", "", &body)]);
        let mut opts = fast_opts();
        opts.max_response_bytes = 256;
        let provider = openai_for(&server).with_http_options(opts);
        match provider.embed(&["x".to_string()]) {
            Err(EmbeddingError::ResponseTooLarge { limit: 256 }) => {}
            other => panic!("expected ResponseTooLarge, got {other:?}"),
        }
    }

    #[test]
    fn oversized_chunked_body_without_length_is_capped() {
        let chunk = "a".repeat(1024);
        let mut body = String::new();
        for _ in 0..64 {
            body.push_str(&format!("{:x}\r\n{chunk}\r\n", chunk.len()));
        }
        body.push_str("0\r\n\r\n");
        let response = format!(
            "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{body}"
        );
        let server = MockServer::start(vec![response.into_bytes()]);
        let mut opts = fast_opts();
        opts.max_response_bytes = 4096;
        let provider = openai_for(&server).with_http_options(opts);
        assert!(matches!(
            provider.embed(&["x".to_string()]),
            Err(EmbeddingError::ResponseTooLarge { limit: 4096 })
        ));
    }

    #[test]
    fn http_error_carries_status_and_truncated_sanitized_body() {
        let noisy = format!(
            "bad request for Bearer sk-secret-key and sk-secret-key\n{}",
            "x".repeat(5000)
        );
        let server = MockServer::start(vec![json_response("400 Bad Request", "", &noisy)]);
        let provider = openai_for(&server);
        match provider.embed(&["x".to_string()]) {
            Err(EmbeddingError::Http { status: 400, body }) => {
                assert!(!body.contains("sk-secret-key"), "key leaked: {body}");
                assert!(!body.contains('\n'));
                assert!(body.len() < 400, "not truncated: {}", body.len());
                assert!(body.ends_with("(truncated)"));
            }
            other => panic!("expected Http 400, got {other:?}"),
        }
        assert_eq!(server.requests().len(), 1, "4xx must not be retried");
    }

    #[test]
    fn sanitize_snippet_redacts_and_truncates() {
        let s = http::sanitize_snippet(b"auth failed: Bearer abc.def-123, retry", &[]);
        assert_eq!(s, "auth failed: Bearer [redacted], retry");
        let s = http::sanitize_snippet(b"key=sk-1 key2=sk-1", &["sk-1"]);
        assert!(!s.contains("sk-1"));
        let s = http::sanitize_snippet("é".repeat(1000).as_bytes(), &[]);
        assert!(s.chars().count() < 330);
    }

    #[test]
    fn redirects_are_not_followed() {
        let target = MockServer::start(vec![json_response("200 OK", "", "{}")]);
        let redirect = format!(
            "HTTP/1.1 302 Found\r\nLocation: {}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            target.url("/stolen")
        );
        let server = MockServer::start(vec![redirect.into_bytes()]);
        let provider = openai_for(&server);
        match provider.embed(&["x".to_string()]) {
            Err(EmbeddingError::Http { status: 302, .. }) => {}
            other => panic!("expected Http 302, got {other:?}"),
        }
        assert!(
            target.requests().is_empty(),
            "redirect target must receive no request"
        );
    }

    #[test]
    fn retries_5xx_then_succeeds() {
        let ok = r#"{"data":[{"embedding":[1.0,2.0],"index":0}]}"#;
        let server = MockServer::start(vec![
            json_response("503 Service Unavailable", "", "overloaded"),
            json_response("502 Bad Gateway", "", "bad gw"),
            json_response("200 OK", "", ok),
        ]);
        let provider = openai_for(&server);
        let out = provider.embed(&["x".to_string()]).expect("retried");
        assert_eq!(out, vec![vec![1.0, 2.0]]);
        assert_eq!(server.requests().len(), 3);
    }

    #[test]
    fn retries_are_bounded() {
        let responses = (0..10)
            .map(|_| json_response("500 Internal Server Error", "", "boom"))
            .collect();
        let server = MockServer::start(responses);
        let mut opts = fast_opts();
        opts.max_retries = 2;
        let provider = openai_for(&server).with_http_options(opts);
        match provider.embed(&["x".to_string()]) {
            Err(EmbeddingError::Http { status: 500, .. }) => {}
            other => panic!("expected Http 500, got {other:?}"),
        }
        assert_eq!(server.requests().len(), 3, "1 attempt + 2 retries");
    }

    #[test]
    fn retry_after_is_honored() {
        let ok = r#"{"data":[{"embedding":[1.0],"index":0}]}"#;
        let server = MockServer::start(vec![
            json_response("429 Too Many Requests", "Retry-After: 1\r\n", "slow down"),
            json_response("200 OK", "", ok),
        ]);
        let provider = openai_for(&server);
        let started = Instant::now();
        provider.embed(&["x".to_string()]).expect("retried");
        assert!(
            started.elapsed() >= Duration::from_millis(990),
            "Retry-After ignored: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn retry_after_beyond_deadline_gives_up_immediately() {
        let server = MockServer::start(vec![
            json_response("429 Too Many Requests", "Retry-After: 20\r\n", "slow"),
            json_response("200 OK", "", "{}"),
        ]);
        let mut opts = fast_opts();
        opts.total_timeout = Duration::from_secs(2);
        let provider = openai_for(&server).with_http_options(opts);
        let started = Instant::now();
        assert!(matches!(
            provider.embed(&["x".to_string()]),
            Err(EmbeddingError::Http { status: 429, .. })
        ));
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(server.requests().len(), 1);
    }

    #[test]
    fn backoff_is_exponential_jittered_and_capped() {
        let mut opts = HttpOptions::with_total_timeout(Duration::from_secs(10));
        opts.backoff_base = Duration::from_millis(100);
        opts.backoff_max = Duration::from_millis(800);
        for _ in 0..50 {
            let d0 = http::backoff_delay(&opts, 0);
            assert!(d0 >= Duration::from_millis(50) && d0 <= Duration::from_millis(100));
            let d2 = http::backoff_delay(&opts, 2);
            assert!(d2 >= Duration::from_millis(200) && d2 <= Duration::from_millis(400));
            let d9 = http::backoff_delay(&opts, 9);
            assert!(d9 <= Duration::from_millis(800));
        }
        assert_eq!(http::parse_retry_after(" 7 "), Some(Duration::from_secs(7)));
        assert_eq!(
            http::parse_retry_after("9999"),
            Some(Duration::from_secs(30))
        );
        assert_eq!(http::parse_retry_after("soon"), None);
    }

    #[test]
    fn connect_errors_are_retried_within_deadline() {
        // Bind then drop to obtain a port nobody listens on.
        let port = {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let provider =
            OllamaEmbeddingProvider::new("m".into(), Some(format!("http://127.0.0.1:{port}")))
                .with_http_options(fast_opts());
        let started = Instant::now();
        assert!(matches!(
            provider.embed(&["x".to_string()]),
            Err(EmbeddingError::Io(_))
        ));
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[test]
    fn https_endpoint_speaks_tls_and_never_leaks_key_in_cleartext() {
        // A plain TCP server that records raw bytes. If the client sent
        // plaintext HTTP, the Authorization header (and key) would show up.
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let seen = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let seen2 = Arc::clone(&seen);
        let handle = std::thread::spawn(move || {
            if let Ok((mut s, _)) = listener.accept() {
                s.set_read_timeout(Some(Duration::from_millis(800))).ok();
                let mut buf = [0u8; 4096];
                if let Ok(n) = s.read(&mut buf) {
                    seen2.lock().unwrap().extend_from_slice(&buf[..n]);
                }
                // Answer with garbage: not a TLS server.
                let _ = s.write_all(b"HTTP/1.1 200 OK\r\n\r\n");
            }
        });
        let provider =
            OpenAIEmbeddingProvider::new("text-embedding-3-small".into(), "sk-secret-key".into())
                .unwrap()
                .with_endpoint(format!("https://127.0.0.1:{port}/v1/embeddings"))
                .with_http_options({
                    let mut o = fast_opts();
                    o.max_retries = 0;
                    o
                });
        let result = provider.embed(&["x".to_string()]);
        handle.join().unwrap();
        assert!(result.is_err(), "handshake with a non-TLS peer must fail");
        let raw = seen.lock().unwrap().clone();
        assert!(!raw.is_empty(), "client should have started a handshake");
        assert_eq!(raw[0], 0x16, "first byte must be a TLS handshake record");
        let text = String::from_utf8_lossy(&raw);
        assert!(!text.contains("sk-secret-key") && !text.contains("POST "));
    }

    // -----------------------------------------------------------------
    // Plaintext-key protection (SEC-23)
    // -----------------------------------------------------------------

    #[test]
    fn credentials_over_plain_http_only_allowed_for_loopback() {
        let ok =
            |u: &str| http::ensure_secure_transport(&http::parse_endpoint(u).unwrap(), true, false);
        assert!(ok("http://127.0.0.1:8080/v1/embeddings").is_ok());
        assert!(ok("http://127.5.6.7/x").is_ok());
        assert!(ok("http://[::1]:9000/x").is_ok());
        assert!(ok("http://localhost:11434/x").is_ok());
        assert!(ok("https://api.openai.com/v1/embeddings").is_ok());
        assert!(matches!(
            ok("http://api.openai.com/v1/embeddings"),
            Err(EmbeddingError::InvalidConfig(_))
        ));
        assert!(matches!(
            ok("http://10.0.0.5/v1"),
            Err(EmbeddingError::InvalidConfig(_))
        ));
        assert!(matches!(
            ok("http://localhost.evil.com/v1"),
            Err(EmbeddingError::InvalidConfig(_))
        ));
        // Explicit opt-in, and no credentials, both pass.
        let url = http::parse_endpoint("http://10.0.0.5/v1").unwrap();
        assert!(http::ensure_secure_transport(&url, true, true).is_ok());
        assert!(http::ensure_secure_transport(&url, false, false).is_ok());
    }

    #[test]
    fn openai_provider_refuses_plain_http_to_remote_host_without_connecting() {
        let provider = OpenAIEmbeddingProvider::new("m".into(), "sk-secret-key".into())
            .unwrap()
            .with_endpoint("http://192.0.2.1/v1/embeddings".into())
            .with_allow_insecure_http(false);
        let started = Instant::now();
        match provider.embed(&["x".to_string()]) {
            Err(EmbeddingError::InvalidConfig(msg)) => {
                assert!(msg.contains("plaintext"));
                assert!(!msg.contains("sk-secret-key"));
            }
            other => panic!("expected InvalidConfig, got {other:?}"),
        }
        assert!(started.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn endpoint_scheme_and_credentials_are_validated() {
        for bad in [
            "ftp://h/x",
            "file:///etc/passwd",
            "not a url",
            "http://user:pw@h/x",
        ] {
            assert!(
                matches!(
                    http::parse_endpoint(bad),
                    Err(EmbeddingError::InvalidConfig(_))
                ),
                "{bad}"
            );
        }
    }

    #[test]
    fn openai_debug_does_not_print_api_key() {
        let p = OpenAIEmbeddingProvider::new("m".into(), "sk-secret-key".into()).unwrap();
        assert!(!format!("{p:?}").contains("sk-secret-key"));
    }

    #[test]
    fn openai_sends_bearer_header_to_loopback_http() {
        let ok = r#"{"data":[{"embedding":[1.0],"index":0}]}"#;
        let server = MockServer::start(vec![json_response("200 OK", "", ok)]);
        openai_for(&server).embed(&["x".to_string()]).unwrap();
        let req = server.requests().remove(0).to_ascii_lowercase();
        assert!(req.contains("authorization: bearer sk-secret-key"));
    }

    // -----------------------------------------------------------------
    // Ollama endpoint handling (EMB-02)
    // -----------------------------------------------------------------

    #[test]
    fn ollama_endpoint_resolution_handles_base_urls_and_slashes() {
        let r = |u: &str| resolve_ollama_endpoint(u).unwrap();
        assert_eq!(
            r("http://localhost:11434"),
            "http://localhost:11434/api/embed"
        );
        assert_eq!(
            r("http://localhost:11434/"),
            "http://localhost:11434/api/embed"
        );
        assert_eq!(r("http://h:1///"), "http://h:1/api/embed");
        assert_eq!(r("http://h:1/api"), "http://h:1/api/embed");
        assert_eq!(r("http://h:1/api/"), "http://h:1/api/embed");
        assert_eq!(r("http://h:1/api/embed"), "http://h:1/api/embed");
        assert_eq!(r("http://h:1/api/embed/"), "http://h:1/api/embed");
        assert_eq!(r("http://h:1/api/embeddings"), "http://h:1/api/embeddings");
        assert_eq!(r("http://h:1/ollama/"), "http://h:1/ollama/api/embed");
        assert_eq!(r("https://h/x?y=1#z"), "https://h/x/api/embed");
        assert!(resolve_ollama_endpoint("localhost:11434").is_err());
        assert_eq!(
            OllamaEmbeddingProvider::new("m".into(), None).endpoint(),
            OllamaEmbeddingProvider::DEFAULT_ENDPOINT
        );
        assert_eq!(
            OllamaEmbeddingProvider::new("m".into(), Some("http://h:1".into())).endpoint(),
            "http://h:1/api/embed"
        );
    }

    #[test]
    fn ollama_bare_base_url_posts_to_api_embed() {
        let ok = r#"{"embeddings":[[0.1,0.2],[0.3,0.4]]}"#;
        let server = MockServer::start(vec![json_response("200 OK", "", ok)]);
        let provider = OllamaEmbeddingProvider::new("nomic".into(), Some(server.url("/")))
            .with_http_options(fast_opts());
        let out = provider.embed(&["a".to_string(), "b".to_string()]).unwrap();
        assert_eq!(out.len(), 2);
        let req = server.requests().remove(0);
        assert!(req.starts_with("POST /api/embed HTTP/1.1"), "{req}");
        assert!(req.contains(r#""input":["a","b"]"#), "{req}");
        assert!(req.contains(r#""model":"nomic""#));
    }

    #[test]
    fn ollama_legacy_endpoint_uses_prompt_shape() {
        let ok = r#"{"embedding":[0.1,0.2,0.3]}"#;
        let server = MockServer::start(vec![
            json_response("200 OK", "", ok),
            json_response("200 OK", "", ok),
        ]);
        let provider =
            OllamaEmbeddingProvider::new("m".into(), Some(server.url("/api/embeddings")))
                .with_http_options(fast_opts());
        let out = provider.embed(&["a".to_string(), "b".to_string()]).unwrap();
        assert_eq!(out.len(), 2);
        let req = server.requests().remove(0);
        assert!(req.starts_with("POST /api/embeddings HTTP/1.1"));
        assert!(req.contains(r#""prompt":"a""#));
    }

    #[test]
    fn ollama_env_alias_resolution() {
        let s = |v: &str| Some(v.to_string());
        assert_eq!(ollama_endpoint_from_values(None, None), None);
        assert_eq!(
            ollama_endpoint_from_values(s("http://a"), s("http://b")),
            Some(("http://a".to_string(), false))
        );
        assert_eq!(
            ollama_endpoint_from_values(None, s("http://b")),
            Some(("http://b".to_string(), true))
        );
        assert_eq!(
            ollama_endpoint_from_values(s("  "), s("http://b")),
            Some(("http://b".to_string(), true))
        );
    }

    // -----------------------------------------------------------------
    // Dimensions (EMB-04)
    // -----------------------------------------------------------------

    #[test]
    fn openai_dimensions_come_from_model_table() {
        let mk = |m: &str| OpenAIEmbeddingProvider::new(m.into(), "k".into()).unwrap();
        assert_eq!(mk("text-embedding-3-small").dimensions(), 1536);
        assert_eq!(mk("text-embedding-3-large").dimensions(), 3072);
        assert_eq!(mk("text-embedding-ada-002").dimensions(), 1536);
        assert_eq!(mk("").dimensions(), 1536);
        assert_eq!(mk("some-custom-model").dimensions(), 0);
    }

    #[test]
    fn openai_dimensions_parameter_is_sent_and_validated() {
        let ok = r#"{"data":[{"embedding":[0.1,0.2,0.3],"index":0}]}"#;
        let server = MockServer::start(vec![json_response("200 OK", "", ok)]);
        let provider = OpenAIEmbeddingProvider::new("text-embedding-3-small".into(), "k".into())
            .unwrap()
            .with_endpoint(server.url("/v1/embeddings"))
            .with_http_options(fast_opts())
            .with_dimensions(3)
            .unwrap();
        assert_eq!(provider.dimensions(), 3);
        provider.embed(&["x".to_string()]).unwrap();
        assert!(server.requests()[0].contains(r#""dimensions":3"#));

        let ada =
            OpenAIEmbeddingProvider::new("text-embedding-ada-002".into(), "k".into()).unwrap();
        assert!(ada.with_dimensions(256).is_err());
        let small =
            OpenAIEmbeddingProvider::new("text-embedding-3-small".into(), "k".into()).unwrap();
        assert!(small.clone().with_dimensions(0).is_err());
        assert!(small.with_dimensions(4096).is_err());
    }

    #[test]
    fn openai_response_with_wrong_dimension_is_an_error() {
        let ok = r#"{"data":[{"embedding":[0.1,0.2,0.3],"index":0}]}"#;
        let server = MockServer::start(vec![json_response("200 OK", "", ok)]);
        // Model table says 1536; the server returned 3.
        let provider = OpenAIEmbeddingProvider::new("text-embedding-3-small".into(), "k".into())
            .unwrap()
            .with_endpoint(server.url("/v1/embeddings"))
            .with_http_options(fast_opts());
        match provider.embed(&["x".to_string()]) {
            Err(EmbeddingError::DimensionMismatch {
                expected: 1536,
                actual: 3,
            }) => {}
            other => panic!("expected DimensionMismatch, got {other:?}"),
        }
    }

    #[test]
    fn openai_unknown_model_learns_dimension_and_reorders_by_index() {
        let ok =
            r#"{"data":[{"embedding":[2.0,2.0],"index":1},{"embedding":[1.0,1.0],"index":0}]}"#;
        let bad = r#"{"data":[{"embedding":[1.0,1.0,1.0],"index":0}]}"#;
        let server = MockServer::start(vec![
            json_response("200 OK", "", ok),
            json_response("200 OK", "", bad),
        ]);
        let provider = OpenAIEmbeddingProvider::new("custom".into(), "sk-secret-key".into())
            .unwrap()
            .with_endpoint(server.url("/v1/embeddings"))
            .with_http_options(fast_opts());
        assert_eq!(provider.dimensions(), 0);
        let out = provider.embed(&["a".to_string(), "b".to_string()]).unwrap();
        assert_eq!(out, vec![vec![1.0, 1.0], vec![2.0, 2.0]]);
        assert_eq!(provider.dimensions(), 2);
        assert!(matches!(
            provider.embed(&["a".to_string()]),
            Err(EmbeddingError::DimensionMismatch {
                expected: 2,
                actual: 3
            })
        ));
    }

    #[test]
    fn ollama_learns_dimensions_from_first_response() {
        let ok = r#"{"embeddings":[[0.1,0.2,0.3,0.4]]}"#;
        let server = MockServer::start(vec![json_response("200 OK", "", ok)]);
        let provider = OllamaEmbeddingProvider::new("m".into(), Some(server.url("")))
            .with_http_options(fast_opts());
        assert_eq!(provider.dimensions(), 0);
        provider.embed(&["x".to_string()]).unwrap();
        assert_eq!(provider.dimensions(), 4);
        assert_eq!(
            OllamaEmbeddingProvider::new("m".into(), None)
                .with_dimensions(768)
                .dimensions(),
            768
        );
    }

    #[test]
    fn dimension_validation_helpers() {
        let v = vec![vec![0.0; 4], vec![0.0; 4]];
        assert!(validate_dimensions(4, &v).is_ok());
        assert!(validate_dimensions(0, &v).is_ok());
        assert!(validate_dimensions(4, &[]).is_ok());
        assert!(matches!(
            validate_dimensions(8, &v),
            Err(EmbeddingError::DimensionMismatch {
                expected: 8,
                actual: 4
            })
        ));
        let ragged = vec![vec![0.0; 4], vec![0.0; 3]];
        assert!(validate_dimensions(0, &ragged).is_err());
        let hash = HashEmbeddingProvider::new(16);
        let out = hash.embed(&["a".to_string()]).unwrap();
        assert!(validate_provider_output(&hash, &out).is_ok());
        assert!(validate_provider_output(&HashEmbeddingProvider::new(8), &out).is_err());
    }

    // -----------------------------------------------------------------
    // Circuit breaker single probe (EMB-03)
    // -----------------------------------------------------------------

    #[test]
    fn half_open_admits_exactly_one_concurrent_probe() {
        for _ in 0..20 {
            let breaker = Arc::new(CircuitBreaker::new(1, Duration::from_millis(300)));
            breaker.record_failure();
            std::thread::sleep(Duration::from_millis(320));
            let barrier = Arc::new(std::sync::Barrier::new(16));
            let handles: Vec<_> = (0..16)
                .map(|_| {
                    let b = Arc::clone(&breaker);
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        barrier.wait();
                        b.try_acquire().is_ok()
                    })
                })
                .collect();
            let admitted = handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .filter(|ok| *ok)
                .count();
            assert_eq!(admitted, 1, "exactly one probe must be admitted");
            // While the probe is outstanding everyone else fails fast.
            assert!(breaker.try_acquire().is_err());
        }
    }

    #[test]
    fn probe_success_reopens_traffic_and_failure_restarts_timer() {
        let breaker = CircuitBreaker::new(1, Duration::from_millis(200));
        breaker.record_failure();
        std::thread::sleep(Duration::from_millis(220));
        breaker.try_acquire().expect("probe");
        assert!(breaker.try_acquire().is_err());
        breaker.record_failure();
        assert_eq!(breaker.state(), CircuitState::Open);
        assert!(breaker.try_acquire().is_err());
        std::thread::sleep(Duration::from_millis(220));
        breaker.try_acquire().expect("second probe");
        breaker.record_success();
        assert_eq!(breaker.state(), CircuitState::Closed);
        breaker.try_acquire().unwrap();
        breaker.try_acquire().unwrap();
    }

    #[test]
    fn lost_probe_is_replaced_after_another_reset_window() {
        let breaker = CircuitBreaker::new(1, Duration::from_millis(20));
        breaker.record_failure();
        std::thread::sleep(Duration::from_millis(30));
        breaker.try_acquire().expect("probe that never reports");
        assert!(breaker.try_acquire().is_err());
        std::thread::sleep(Duration::from_millis(30));
        breaker.try_acquire().expect("replacement probe");
    }

    #[test]
    fn provider_only_calls_inner_once_while_probe_in_flight() {
        struct Slow {
            calls: std::sync::atomic::AtomicUsize,
            fail: bool,
        }
        impl EmbeddingProvider for Slow {
            fn name(&self) -> &str {
                "slow"
            }
            fn dimensions(&self) -> usize {
                1
            }
            fn embed(&self, _t: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(150));
                if self.fail {
                    Err(EmbeddingError::Io("down".into()))
                } else {
                    Ok(vec![vec![1.0]])
                }
            }
        }
        let breaker = Arc::new(CircuitBreaker::new(1, Duration::from_millis(400)));
        breaker.record_failure();
        std::thread::sleep(Duration::from_millis(420));
        let wrapped = Arc::new(CircuitBreakerProvider::new(
            Slow {
                calls: Default::default(),
                fail: false,
            },
            breaker,
        ));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let w = Arc::clone(&wrapped);
                std::thread::spawn(move || w.embed(&["x".to_string()]).is_ok())
            })
            .collect();
        let ok = handles
            .into_iter()
            .map(|h| h.join().unwrap())
            .filter(|b| *b)
            .count();
        assert_eq!(ok, 1);
        assert_eq!(wrapped.inner().calls.load(Ordering::SeqCst), 1);
        assert_eq!(wrapped.breaker().state(), CircuitState::Closed);
    }
}
