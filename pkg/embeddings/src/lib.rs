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
use std::time::Duration;

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
}
