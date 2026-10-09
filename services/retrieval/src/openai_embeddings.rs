//! OpenAI-compatible `/v1/embeddings` endpoint.
//!
//! This module is the single biggest adoption lever for DASH: any client
//! that speaks the OpenAI embeddings protocol (langchain, llama-index,
//! semantic-kernel, the OpenAI Python SDK, the `openai` CLI, etc.) can
//! point at a DASH retrieval service by changing one environment
//! variable (`OPENAI_API_BASE=http://localhost:8080/v1`).
//!
//! Wire format is byte-for-byte compatible with the OpenAI v1 embeddings
//! API. The only deliberate deviations:
//!
//! - `model` is accepted as a string but is treated as a hint for the
//!   response payload; the actual embedding is produced by the
//!   configured `EmbeddingProvider` (default: `HashEmbeddingProvider`).
//! - `encoding_format` accepts `"float"` (default) and `"base64"`.
//! - `input` accepts a string, an array of strings, an array of token ids
//!   or an array of token-id arrays. Token ids cannot be decoded without
//!   the client's tokenizer, so they are rejected with a 400
//!   (`unsupported_input_type`) unless `DASH_EMBEDDING_ALLOW_TOKEN_IDS=1`;
//!   in that mode each id array is embedded as the decimal ids joined by a
//!   single space (`[1, 2, 3]` becomes `"1 2 3"`). That is deterministic
//!   and keeps clients that default to token arrays working, but it is NOT
//!   semantically equivalent to embedding the decoded text.
//! - `dimensions` is accepted and must equal the provider's dimensions;
//!   `user` is accepted and ignored.
//! - `usage.prompt_tokens` is an estimate (`ceil(chars / 4)` per text, or
//!   the id count for token inputs), not a real tokenizer count.
//! - Errors use OpenAI's `{"error":{"message","type","param","code"}}`
//!   shape. Provider failures return short codes; details are logged.
//! - At most 2048 inputs per request and `DASH_EMBEDDING_MAX_TOTAL_CHARS`
//!   (default 524288) characters in total.
//!
//! The endpoint is intentionally provider-agnostic: swap the
//!   `HashEmbeddingProvider` for an `OllamaEmbeddingProvider` or
//!   `OpenAIEmbeddingProvider` and the wire format stays the same.

use embeddings::{EmbeddingError, EmbeddingProvider};
use serde::{Deserialize, Serialize};

/// Maximum number of inputs per request (same as OpenAI).
pub const MAX_EMBEDDING_INPUTS: usize = 2048;
const DEFAULT_MAX_TOTAL_CHARS: usize = 512 * 1024;

/// Wire-compatible request body for `POST /v1/embeddings`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OpenAIEmbeddingsRequest {
    pub input: OpenAIInput,
    pub model: String,
    /// Accepted for compatibility and ignored.
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub encoding_format: Option<String>,
    #[serde(default)]
    pub dimensions: Option<i64>,
}

/// Accepts a string, an array of strings, an array of token ids, or an
/// array of token-id arrays.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OpenAIInput {
    Single(String),
    Multiple(Vec<String>),
    Tokens(Vec<i64>),
    TokenBatches(Vec<Vec<i64>>),
}

impl OpenAIInput {
    /// Text inputs only; empty for token inputs (see [`Self::is_token_input`]).
    pub fn texts(&self) -> Vec<&str> {
        match self {
            OpenAIInput::Single(s) => vec![s.as_str()],
            OpenAIInput::Multiple(v) => v.iter().map(|s| s.as_str()).collect(),
            OpenAIInput::Tokens(_) | OpenAIInput::TokenBatches(_) => Vec::new(),
        }
    }

    pub fn is_token_input(&self) -> bool {
        matches!(self, OpenAIInput::Tokens(_) | OpenAIInput::TokenBatches(_))
    }

    pub fn len(&self) -> usize {
        match self {
            OpenAIInput::Single(_) => 1,
            OpenAIInput::Multiple(v) => v.len(),
            OpenAIInput::Tokens(v) => usize::from(!v.is_empty()),
            OpenAIInput::TokenBatches(v) => v.len(),
        }
    }

    /// Returns `true` only when `input` is an empty array.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Wire-compatible success response.
#[derive(Debug, Clone, Serialize)]
pub struct OpenAIEmbeddingsResponse {
    pub object: &'static str,
    pub data: Vec<OpenAIEmbeddingData>,
    pub model: String,
    pub usage: OpenAIUsage,
}

/// Encoding format requested by the client. Mirrors the OpenAI
/// `/v1/embeddings` spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EncodingFormat {
    #[default]
    Float,
    Base64,
}

impl EncodingFormat {
    pub fn from_request_str(s: Option<&str>) -> Result<Self, String> {
        match s.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
            None | Some("") | Some("float") => Ok(Self::Float),
            Some("base64") => Ok(Self::Base64),
            Some(other) => Err(format!(
                "encoding_format '{other}' is not supported; only 'float' and 'base64' are accepted"
            )),
        }
    }
}

/// The wire format for a single embedding. Held in an untagged form so
/// that `Serialize` produces either a `Vec<f32>` (Float) or a `String`
/// (Base64) under the same `"embedding"` key, matching the OpenAI
/// /v1/embeddings response shape byte-for-byte.
#[derive(Debug, Clone, PartialEq)]
pub enum EmbeddingValue {
    Float(Vec<f32>),
    Base64(String),
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAIEmbeddingData {
    pub object: &'static str,
    pub index: usize,
    #[serde(serialize_with = "serialize_embedding_value")]
    pub embedding: EmbeddingValue,
}

fn serialize_embedding_value<S: serde::Serializer>(
    value: &EmbeddingValue,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    match value {
        EmbeddingValue::Float(v) => v.serialize(serializer),
        EmbeddingValue::Base64(s) => s.serialize(serializer),
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAIUsage {
    pub prompt_tokens: usize,
    pub total_tokens: usize,
}

/// Errors returned to the client, with OpenAI-compatible shape.
#[derive(Debug, Clone, Serialize)]
pub struct OpenAIErrorResponse {
    pub error: OpenAIErrorBody,
}

#[derive(Debug, Clone, Serialize)]
pub struct OpenAIErrorBody {
    pub message: String,
    #[serde(rename = "type")]
    pub kind: &'static str,
    pub param: Option<String>,
    pub code: Option<String>,
}

impl OpenAIErrorResponse {
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self {
            error: OpenAIErrorBody {
                message: message.into(),
                kind: "invalid_request_error",
                param: None,
                code: None,
            },
        }
    }

    pub fn invalid_param(message: impl Into<String>, param: &str, code: &str) -> Self {
        Self {
            error: OpenAIErrorBody {
                message: message.into(),
                kind: "invalid_request_error",
                param: Some(param.to_string()),
                code: Some(code.to_string()),
            },
        }
    }

    /// Server-side failure with a short machine-readable code. The message
    /// is the code itself, never a raw internal error.
    pub fn server_error(code: &str) -> Self {
        Self {
            error: OpenAIErrorBody {
                message: code.to_string(),
                kind: "server_error",
                param: None,
                code: Some(code.to_string()),
            },
        }
    }
}

/// Handler for `POST /v1/embeddings`. Uses the provider selected by
/// `DASH_EMBEDDING_PROVIDER` (default: `hash`) so the retrieval service can
/// serve real semantic embeddings when Ollama or OpenAI is configured.
pub fn handle_openai_embeddings(
    body: &str,
) -> Result<OpenAIEmbeddingsResponse, OpenAIErrorResponse> {
    let provider = embeddings::select_embedding_provider_from_env();
    handle_openai_embeddings_with_provider(body, provider.as_ref())
}

fn max_total_chars() -> usize {
    std::env::var("DASH_EMBEDDING_MAX_TOTAL_CHARS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_MAX_TOTAL_CHARS)
}

fn token_ids_allowed() -> bool {
    std::env::var("DASH_EMBEDDING_ALLOW_TOKEN_IDS")
        .is_ok_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

fn join_token_ids(ids: &[i64]) -> String {
    ids.iter().map(i64::to_string).collect::<Vec<_>>().join(" ")
}

/// Deterministic token estimate: `ceil(chars / 4)`, at least 1 per non-empty
/// text. This is an estimate, not a tokenizer count.
fn estimate_tokens(text: &str) -> usize {
    text.chars().count().div_ceil(4).max(1)
}

/// Resolve the request input into the texts to embed plus the estimated
/// prompt-token count, enforcing every input bound.
fn resolve_input_texts(input: &OpenAIInput) -> Result<(Vec<String>, usize), OpenAIErrorResponse> {
    if input.is_empty() {
        return Err(OpenAIErrorResponse::invalid_param(
            "input must contain at least one text",
            "input",
            "empty_input",
        ));
    }
    if input.len() > MAX_EMBEDDING_INPUTS {
        return Err(OpenAIErrorResponse::invalid_param(
            format!("input must contain at most {MAX_EMBEDDING_INPUTS} items"),
            "input",
            "too_many_inputs",
        ));
    }
    let (texts, tokens): (Vec<String>, usize) = if input.is_token_input() {
        if !token_ids_allowed() {
            return Err(OpenAIErrorResponse::invalid_param(
                "token id arrays are not supported because tokens cannot be decoded \
                 without a tokenizer; send text instead",
                "input",
                "unsupported_input_type",
            ));
        }
        let batches: Vec<&[i64]> = match input {
            OpenAIInput::Tokens(ids) => vec![ids.as_slice()],
            OpenAIInput::TokenBatches(batches) => batches.iter().map(Vec::as_slice).collect(),
            _ => Vec::new(),
        };
        if batches.iter().any(|ids| ids.is_empty()) {
            return Err(OpenAIErrorResponse::invalid_param(
                "input token arrays must be non-empty",
                "input",
                "empty_input",
            ));
        }
        let tokens = batches.iter().map(|ids| ids.len()).sum();
        (
            batches.iter().map(|ids| join_token_ids(ids)).collect(),
            tokens,
        )
    } else {
        let texts: Vec<String> = input.texts().iter().map(|s| s.to_string()).collect();
        if texts.iter().any(|text| text.trim().is_empty()) {
            return Err(OpenAIErrorResponse::invalid_param(
                "input text must be non-empty",
                "input",
                "empty_input",
            ));
        }
        let tokens = texts.iter().map(|t| estimate_tokens(t)).sum();
        (texts, tokens)
    };
    let total_chars: usize = texts.iter().map(|t| t.chars().count()).sum();
    let max_chars = max_total_chars();
    if total_chars > max_chars {
        return Err(OpenAIErrorResponse::invalid_param(
            format!("input must contain at most {max_chars} characters in total"),
            "input",
            "input_too_long",
        ));
    }
    Ok((texts, tokens))
}

/// Embed `texts` and verify the provider returned one vector per text.
/// Provider failures are logged and mapped to short codes. Shared by the
/// `/v1/embeddings` route and the retrieve-query embedding path.
pub fn embed_texts_checked(
    provider: &dyn EmbeddingProvider,
    texts: &[String],
) -> Result<Vec<Vec<f32>>, OpenAIErrorResponse> {
    let vectors = provider.embed(texts).map_err(|err: EmbeddingError| {
        eprintln!("retrieval embedding provider failure: {err}");
        match err {
            EmbeddingError::CircuitOpen { .. }
            | EmbeddingError::Timeout(_)
            | EmbeddingError::InvalidConfig(_) => {
                OpenAIErrorResponse::server_error("embedding_unavailable")
            }
            _ => OpenAIErrorResponse::server_error("embedding_provider_error"),
        }
    })?;
    if vectors.len() != texts.len() {
        eprintln!(
            "retrieval embedding count mismatch: expected {}, got {}",
            texts.len(),
            vectors.len()
        );
        return Err(OpenAIErrorResponse::server_error(
            "embedding_provider_error",
        ));
    }
    Ok(vectors)
}

/// Run an OpenAI-compatible embeddings request with a custom provider.
/// This is the single validated implementation behind the HTTP route.
pub fn handle_openai_embeddings_with_provider(
    body: &str,
    provider: &dyn EmbeddingProvider,
) -> Result<OpenAIEmbeddingsResponse, OpenAIErrorResponse> {
    let req: OpenAIEmbeddingsRequest = serde_json::from_str(body).map_err(|err| {
        let detail = err.to_string();
        if detail.contains("OpenAIInput") {
            OpenAIErrorResponse::invalid_param(
                "input must be a string, an array of strings, an array of token ids, or an array of token id arrays",
                "input",
                "invalid_input_type",
            )
        } else {
            OpenAIErrorResponse::invalid_param(
                format!("invalid request body: {detail}"),
                "body",
                "invalid_request_body",
            )
        }
    })?;

    let encoding =
        EncodingFormat::from_request_str(req.encoding_format.as_deref()).map_err(|message| {
            OpenAIErrorResponse::invalid_param(
                message,
                "encoding_format",
                "invalid_encoding_format",
            )
        })?;
    if let Some(dimensions) = req.dimensions {
        let provider_dims = provider.dimensions();
        if usize::try_from(dimensions).ok() != Some(provider_dims) {
            return Err(OpenAIErrorResponse::invalid_param(
                format!("dimensions must be {provider_dims} for this model"),
                "dimensions",
                "unsupported_dimensions",
            ));
        }
    }
    let (texts, prompt_tokens) = resolve_input_texts(&req.input)?;
    let embeddings = embed_texts_checked(provider, &texts)?;

    let data: Vec<OpenAIEmbeddingData> = embeddings
        .into_iter()
        .enumerate()
        .map(|(index, embedding)| OpenAIEmbeddingData {
            object: "embedding",
            embedding: encode_embedding_value(embedding, encoding),
            index,
        })
        .collect();

    Ok(OpenAIEmbeddingsResponse {
        object: "list",
        data,
        model: req.model,
        usage: OpenAIUsage {
            prompt_tokens,
            total_tokens: prompt_tokens,
        },
    })
}

/// Convert a provider-returned float vector to the wire format
/// requested by the client. `Float` passes through unchanged; `Base64`
/// packs the float32 values as little-endian bytes and base64-encodes
/// the result, matching the OpenAI Python SDK's `embeddings_base64` flow.
pub fn encode_embedding_value(embedding: Vec<f32>, encoding: EncodingFormat) -> EmbeddingValue {
    match encoding {
        EncodingFormat::Float => EmbeddingValue::Float(embedding),
        EncodingFormat::Base64 => {
            let bytes = pack_f32_le(&embedding);
            EmbeddingValue::Base64(base64_encode(&bytes))
        }
    }
}

/// Pack a slice of `f32` values into a little-endian byte buffer.
/// This is the canonical wire format for OpenAI base64 embeddings.
pub fn pack_f32_le(values: &[f32]) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    bytes
}

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard base64 encoder (RFC 4648 §4). Pads with `=` to a multiple
/// of 4 characters. No URL-safe variant; matches what the OpenAI API
/// emits in `encoding_format: "base64"` responses.
pub fn base64_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    let mut i = 0;
    while i + 3 <= bytes.len() {
        let b0 = bytes[i];
        let b1 = bytes[i + 1];
        let b2 = bytes[i + 2];
        out.push(BASE64_ALPHABET[(b0 >> 2) as usize] as char);
        out.push(BASE64_ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(BASE64_ALPHABET[(((b1 & 0x0f) << 2) | (b2 >> 6)) as usize] as char);
        out.push(BASE64_ALPHABET[(b2 & 0x3f) as usize] as char);
        i += 3;
    }
    let rem = bytes.len() - i;
    if rem == 1 {
        let b0 = bytes[i];
        out.push(BASE64_ALPHABET[(b0 >> 2) as usize] as char);
        out.push(BASE64_ALPHABET[((b0 & 0x03) << 4) as usize] as char);
        out.push('=');
        out.push('=');
    } else if rem == 2 {
        let b0 = bytes[i];
        let b1 = bytes[i + 1];
        out.push(BASE64_ALPHABET[(b0 >> 2) as usize] as char);
        out.push(BASE64_ALPHABET[(((b0 & 0x03) << 4) | (b1 >> 4)) as usize] as char);
        out.push(BASE64_ALPHABET[((b1 & 0x0f) << 2) as usize] as char);
        out.push('=');
    }
    out
}

/// Inverse of [`base64_encode`]. Accepts padded and unpadded input.
/// Used in tests to verify the wire format round-trips.
pub fn base64_decode(s: &str) -> Result<Vec<u8>, String> {
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    if !bytes.len().is_multiple_of(4) {
        return Err(format!(
            "invalid base64 length {} (must be a multiple of 4)",
            bytes.len()
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    for chunk in bytes.chunks(4) {
        let mut buf = [0u8; 4];
        for (i, &b) in chunk.iter().enumerate() {
            buf[i] = match b {
                b'A'..=b'Z' => b - b'A',
                b'a'..=b'z' => b - b'a' + 26,
                b'0'..=b'9' => b - b'0' + 52,
                b'+' => 62,
                b'/' => 63,
                b'=' => 0,
                _ => return Err(format!("invalid base64 char: {}", b as char)),
            };
        }
        out.push((buf[0] << 2) | (buf[1] >> 4));
        if chunk[2] != b'=' {
            out.push((buf[1] << 4) | (buf[2] >> 2));
        }
        if chunk[3] != b'=' {
            out.push((buf[2] << 6) | buf[3]);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Env-driven provider selection
/// Returns the provider name for startup logging/strict-mode checks.
pub fn provider_name_from_env() -> String {
    embeddings::embedding_provider_name_from_env()
}

/// Serializes tests that read or write embedding-related env vars.
#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    &LOCK
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env-provider handler under the shared env lock.
    fn handle_env(body: &str) -> Result<OpenAIEmbeddingsResponse, OpenAIErrorResponse> {
        let _guard = test_env_lock().lock().unwrap_or_else(|p| p.into_inner());
        handle_openai_embeddings(body)
    }

    #[test]
    fn parses_single_string_input() {
        let body = r#"{"input": "hello world", "model": "text-embedding-3-small"}"#;
        let req: OpenAIEmbeddingsRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.input.len(), 1);
        assert_eq!(req.input.texts(), vec!["hello world"]);
        assert_eq!(req.model, "text-embedding-3-small");
    }

    #[test]
    fn parses_array_input() {
        let body = r#"{"input": ["hello", "world", "foo"], "model": "text-embedding-3-small"}"#;
        let req: OpenAIEmbeddingsRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.input.len(), 3);
        assert_eq!(req.input.texts(), vec!["hello", "world", "foo"]);
    }

    #[test]
    fn rejects_empty_array() {
        let body = r#"{"input": [], "model": "text-embedding-3-small"}"#;
        let err = handle_env(body).unwrap_err();
        assert_eq!(err.error.kind, "invalid_request_error");
        assert!(err.error.message.contains("at least one text"));
    }

    #[test]
    fn rejects_empty_string_in_array() {
        let body = r#"{"input": ["hello", ""], "model": "x"}"#;
        let err = handle_env(body).unwrap_err();
        assert_eq!(err.error.kind, "invalid_request_error");
        assert!(err.error.message.contains("non-empty"));
    }

    #[test]
    fn rejects_unsupported_encoding_format() {
        // base64 is supported in this version; use a format that
        // is still rejected (e.g. binary, int8) to exercise the
        // unsupported-format branch.
        let body = r#"{"input": "hi", "model": "x", "encoding_format": "binary"}"#;
        let err = handle_env(body).unwrap_err();
        assert_eq!(err.error.kind, "invalid_request_error");
        assert!(err.error.message.contains("encoding_format"));
    }

    #[test]
    fn rejects_malformed_json() {
        let body = r#"{"input": "hi""#; // truncated
        let err = handle_env(body).unwrap_err();
        assert_eq!(err.error.kind, "invalid_request_error");
        assert!(err.error.message.contains("invalid request body"));
    }

    #[test]
    fn response_shape_is_openai_compatible_for_single_input() {
        let body = r#"{"input": "hello world", "model": "text-embedding-3-small"}"#;
        let resp = handle_env(body).unwrap();
        let json = serde_json::to_string(&resp).unwrap();
        // Required top-level fields per the OpenAI spec
        assert!(json.contains("\"object\":\"list\""));
        assert!(json.contains("\"data\":["));
        assert!(json.contains("\"model\":\"text-embedding-3-small\""));
        assert!(json.contains("\"usage\":"));
        // Per-data-item shape
        assert!(json.contains("\"object\":\"embedding\""));
        assert!(json.contains("\"embedding\":["));
        assert!(json.contains("\"index\":0"));
        // Usage subshape
        assert!(json.contains("\"prompt_tokens\":"));
        assert!(json.contains("\"total_tokens\":"));
    }

    #[test]
    fn response_shape_is_openai_compatible_for_array_input() {
        let body = r#"{"input": ["a", "b", "c"], "model": "text-embedding-3-small"}"#;
        let resp = handle_env(body).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert_eq!(resp.data[0].index, 0);
        assert_eq!(resp.data[1].index, 1);
        assert_eq!(resp.data[2].index, 2);
        // Determinism: the SAME input must produce the SAME vector.
        // Compare the first item against a second request for the same input.
        let resp_again = handle_env(body).unwrap();
        assert_eq!(resp.data[0].embedding, resp_again.data[0].embedding);
        assert_eq!(resp.data[1].embedding, resp_again.data[1].embedding);
        // Collision resistance: DIFFERENT inputs produce DIFFERENT vectors.
        assert_ne!(resp.data[0].embedding, resp.data[1].embedding);
        assert_ne!(resp.data[0].embedding, resp.data[2].embedding);
        assert_ne!(resp.data[1].embedding, resp.data[2].embedding);
    }

    #[test]
    fn embedding_dimensions_match_provider() {
        let body = r#"{"input": "x", "model": "x"}"#;
        let resp = handle_env(body).unwrap();
        let provider = embeddings::HashEmbeddingProvider::default();
        let actual = match &resp.data[0].embedding {
            EmbeddingValue::Float(v) => v.len(),
            EmbeddingValue::Base64(b) => base64_decode(b).unwrap().len() / 4,
        };
        assert_eq!(actual, provider.dimensions());
    }

    #[test]
    fn prompt_tokens_estimated_from_char_count() {
        // 19 chars -> ceil(19 / 4) = 5 (an estimate, not a tokenizer count).
        let body = r#"{"input": "the quick brown fox", "model": "x"}"#;
        let resp = handle_env(body).unwrap();
        assert_eq!(resp.usage.prompt_tokens, 5);
        assert_eq!(resp.usage.total_tokens, 5);
    }

    #[test]
    fn custom_provider_is_used() {
        let body = r#"{"input": "hello", "model": "test-model"}"#;
        let provider = embeddings::HashEmbeddingProvider::new(8);
        let resp = handle_openai_embeddings_with_provider(body, &provider).unwrap();
        assert_eq!(resp.data.len(), 1);
        let actual = match &resp.data[0].embedding {
            EmbeddingValue::Float(v) => v.len(),
            EmbeddingValue::Base64(b) => base64_decode(b).unwrap().len() / 4,
        };
        assert_eq!(actual, 8);
        assert_eq!(resp.model, "test-model");
    }

    #[test]
    fn error_response_shape_is_openai_compatible() {
        let body = r#"{"input": [], "model": "x"}"#;
        let err = handle_env(body).unwrap_err();
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"error\":{"));
        assert!(json.contains("\"message\":"));
        assert!(json.contains("\"type\":\"invalid_request_error\""));
    }

    // -----------------------------------------------------------------
    // base64 encoding format tests
    // -----------------------------------------------------------------

    #[test]
    fn base64_encode_matches_known_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg==");
        assert_eq!(base64_encode(b"fo"), "Zm8=");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
        assert_eq!(base64_encode(b"foob"), "Zm9vYg==");
        assert_eq!(base64_encode(b"fooba"), "Zm9vYmE=");
        assert_eq!(base64_encode(b"foobar"), "Zm9vYmFy");
        // RFC 4648 §10 test vector
        assert_eq!(
            base64_encode(b"foobarfoobarfoobarfoobarfoobarfoobar"),
            "Zm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFyZm9vYmFy"
        );
    }

    #[test]
    fn base64_decode_round_trips_with_encode() {
        let cases: &[&[u8]] = &[
            b"",
            b"f",
            b"fo",
            b"foo",
            b"foob",
            b"fooba",
            b"foobar",
            b"\x00\xff\x7f\x80",
            &[0u8; 32],
            &[0xab; 100],
        ];
        for original in cases {
            let encoded = base64_encode(original);
            let decoded = base64_decode(&encoded).expect("decode should succeed");
            assert_eq!(&decoded, original, "round-trip for {original:?}");
        }
    }

    #[test]
    fn base64_decode_rejects_invalid_length() {
        let err = base64_decode("abc").expect_err("len=3 is invalid");
        assert!(err.contains("invalid base64 length"));
    }

    #[test]
    fn base64_decode_rejects_invalid_char() {
        let err = base64_decode("!!!!").expect_err("invalid chars");
        assert!(err.contains("invalid base64 char"));
    }

    #[test]
    fn pack_f32_le_emits_little_endian_bytes() {
        // 1.0f32 = 0x3F800000, 2.0f32 = 0x40000000
        let bytes = pack_f32_le(&[1.0, 2.0]);
        assert_eq!(bytes, vec![0x00, 0x00, 0x80, 0x3F, 0x00, 0x00, 0x00, 0x40]);
    }

    #[test]
    fn pack_f32_le_handles_negative_zero_and_subnormal() {
        // -0.0f32 = 0x80000000
        // The smallest positive subnormal = f32::from_bits(0x00000001)
        let smallest_subnormal = f32::from_bits(0x0000_0001);
        let bytes = pack_f32_le(&[-0.0, smallest_subnormal]);
        assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x80, 0x01, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn encoding_format_from_request_str_handles_all_cases() {
        assert_eq!(
            EncodingFormat::from_request_str(None).unwrap(),
            EncodingFormat::Float
        );
        assert_eq!(
            EncodingFormat::from_request_str(Some("")).unwrap(),
            EncodingFormat::Float
        );
        assert_eq!(
            EncodingFormat::from_request_str(Some("float")).unwrap(),
            EncodingFormat::Float
        );
        assert_eq!(
            EncodingFormat::from_request_str(Some("FLOAT")).unwrap(),
            EncodingFormat::Float
        );
        assert_eq!(
            EncodingFormat::from_request_str(Some("base64")).unwrap(),
            EncodingFormat::Base64
        );
        assert_eq!(
            EncodingFormat::from_request_str(Some("Base64")).unwrap(),
            EncodingFormat::Base64
        );
        assert_eq!(
            EncodingFormat::from_request_str(Some("  base64  ")).unwrap(),
            EncodingFormat::Base64
        );
        let err = EncodingFormat::from_request_str(Some("binary")).unwrap_err();
        assert!(err.contains("'binary'"));
    }

    #[test]
    fn handle_openai_embeddings_with_base64_format_returns_base64_strings() {
        let body = r#"{"input":"hello world","model":"text-embedding-3-small","encoding_format":"base64"}"#;
        let resp = handle_env(body).expect("handler should succeed");
        assert_eq!(resp.data.len(), 1);
        let json = serde_json::to_string(&resp).expect("serialize");
        // The embedding field should be a base64 string, not a JSON array.
        assert!(
            json.contains("\"embedding\":\""),
            "expected base64 string, got: {json}"
        );
        assert!(
            !json.contains("\"embedding\":["),
            "should not be a JSON array, got: {json}"
        );
        // Round-trip the value through base64_decode + f32::from_le_bytes
        match &resp.data[0].embedding {
            EmbeddingValue::Base64(b) => {
                let bytes = base64_decode(b).expect("decode base64");
                assert_eq!(bytes.len() % 4, 0, "byte length should be a multiple of 4");
                let floats: Vec<f32> = bytes
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                assert_eq!(floats.len(), 384);
                // Hash provider's first dim for "hello world" should match
                let expected = embeddings::HashEmbeddingProvider::default()
                    .embed(&["hello world".to_string()])
                    .unwrap()
                    .remove(0);
                for (a, b) in floats.iter().zip(expected.iter()) {
                    assert!((a - b).abs() < 1e-6, "{a} vs {b}");
                }
            }
            other => panic!("expected Base64, got {other:?}"),
        }
    }

    #[test]
    fn handle_openai_embeddings_with_base64_format_for_array_input() {
        let body = r#"{"input":["alpha","beta"],"model":"text-embedding-3-small","encoding_format":"base64"}"#;
        let resp = handle_env(body).expect("handler should succeed");
        assert_eq!(resp.data.len(), 2);
        for (idx, item) in resp.data.iter().enumerate() {
            match &item.embedding {
                EmbeddingValue::Base64(b) => {
                    let bytes = base64_decode(b).expect("decode base64");
                    assert_eq!(bytes.len() % 4, 0);
                    let floats: Vec<f32> = bytes
                        .chunks_exact(4)
                        .map(|chunk| f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                        .collect();
                    assert_eq!(floats.len(), 384);
                }
                other => panic!("item {idx}: expected Base64, got {other:?}"),
            }
        }
    }

    #[test]
    fn handle_openai_embeddings_with_unknown_format_returns_400() {
        let body = r#"{"input":"hello","model":"x","encoding_format":"binary"}"#;
        let err = handle_env(body).expect_err("unknown format rejected");
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("'binary'"));
        assert!(json.contains("'float'"));
        assert!(json.contains("'base64'"));
    }

    #[test]
    fn handle_openai_embeddings_with_provider_base64_round_trip() {
        // A stub provider returns a known float vector; the handler must
        // serialize it as base64 and we can decode back to the same floats.
        struct FixedProvider;
        impl embeddings::EmbeddingProvider for FixedProvider {
            fn name(&self) -> &str {
                "fixed"
            }
            fn dimensions(&self) -> usize {
                4
            }
            fn embed(
                &self,
                _texts: &[String],
            ) -> Result<Vec<Vec<f32>>, embeddings::EmbeddingError> {
                Ok(vec![vec![1.0, -2.0, 3.5, f32::NAN]])
            }
        }
        let body = r#"{"input":"x","model":"m","encoding_format":"base64"}"#;
        let resp = handle_openai_embeddings_with_provider(body, &FixedProvider).unwrap();
        let b = match &resp.data[0].embedding {
            EmbeddingValue::Base64(b) => b.clone(),
            other => panic!("expected Base64, got {other:?}"),
        };
        let bytes = base64_decode(&b).unwrap();
        assert_eq!(bytes.len(), 16);
        let floats: Vec<f32> = bytes
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        assert_eq!(floats[0], 1.0);
        assert_eq!(floats[1], -2.0);
        assert_eq!(floats[2], 3.5);
        assert!(floats[3].is_nan());
    }

    #[test]
    fn handle_openai_embeddings_default_format_still_uses_float() {
        // When encoding_format is absent, the response is a float array
        // (backward-compatible with v0.2 clients).
        let body = r#"{"input":"hi","model":"x"}"#;
        let resp = handle_env(body).unwrap();
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"embedding\":["));
        match &resp.data[0].embedding {
            EmbeddingValue::Float(v) => assert_eq!(v.len(), 384),
            other => panic!("expected Float, got {other:?}"),
        }
    }

    // -----------------------------------------------------------------
    // EMB-06: input shapes, bounds and error shape
    // -----------------------------------------------------------------

    struct CountingProvider {
        dims: usize,
        short: bool,
    }
    impl embeddings::EmbeddingProvider for CountingProvider {
        fn name(&self) -> &str {
            "counting"
        }
        fn dimensions(&self) -> usize {
            self.dims
        }
        fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, embeddings::EmbeddingError> {
            let n = if self.short {
                texts.len().saturating_sub(1)
            } else {
                texts.len()
            };
            Ok(vec![vec![1.0; self.dims]; n])
        }
    }

    fn run(body: &str) -> Result<OpenAIEmbeddingsResponse, OpenAIErrorResponse> {
        handle_openai_embeddings_with_provider(
            body,
            &CountingProvider {
                dims: 4,
                short: false,
            },
        )
    }

    #[test]
    fn token_id_inputs_parse_but_are_rejected_by_default() {
        let _guard = test_env_lock().lock().unwrap_or_else(|p| p.into_inner());
        unsafe { std::env::remove_var("DASH_EMBEDDING_ALLOW_TOKEN_IDS") };
        for body in [
            r#"{"input":[1,2,3],"model":"m"}"#,
            r#"{"input":[[1,2],[3]],"model":"m"}"#,
        ] {
            let err = run(body).unwrap_err();
            assert_eq!(err.error.kind, "invalid_request_error");
            assert_eq!(err.error.code.as_deref(), Some("unsupported_input_type"));
            assert_eq!(err.error.param.as_deref(), Some("input"));
            assert!(err.error.message.contains("tokenizer"));
        }
    }

    #[test]
    fn token_id_inputs_are_embedded_as_decimal_join_when_enabled() {
        let _guard = test_env_lock().lock().unwrap_or_else(|p| p.into_inner());
        unsafe { std::env::set_var("DASH_EMBEDDING_ALLOW_TOKEN_IDS", "1") };
        let hash = embeddings::HashEmbeddingProvider::new(8);
        let resp = handle_openai_embeddings_with_provider(
            r#"{"input":[[12,7,99],[5]],"model":"m"}"#,
            &hash,
        );
        let flat =
            handle_openai_embeddings_with_provider(r#"{"input":[12,7,99],"model":"m"}"#, &hash);
        unsafe { std::env::remove_var("DASH_EMBEDDING_ALLOW_TOKEN_IDS") };
        let resp = resp.unwrap();
        let expected = hash.embed(&["12 7 99".to_string()]).unwrap().remove(0);
        assert_eq!(resp.data[0].embedding, EmbeddingValue::Float(expected));
        assert_eq!(resp.data.len(), 2);
        assert_eq!(resp.usage.prompt_tokens, 4);
        assert_eq!(flat.unwrap().data.len(), 1);
    }

    #[test]
    fn rejects_too_many_inputs_and_oversized_total() {
        let many = format!(
            r#"{{"input":[{}],"model":"m"}}"#,
            vec!["\"a\""; MAX_EMBEDDING_INPUTS + 1].join(",")
        );
        let err = run(&many).unwrap_err();
        assert_eq!(err.error.code.as_deref(), Some("too_many_inputs"));
        let ok = format!(
            r#"{{"input":[{}],"model":"m"}}"#,
            vec!["\"a\""; MAX_EMBEDDING_INPUTS].join(",")
        );
        assert_eq!(run(&ok).unwrap().data.len(), MAX_EMBEDDING_INPUTS);

        let huge = format!(
            r#"{{"input":["{}"],"model":"m"}}"#,
            "x".repeat(DEFAULT_MAX_TOTAL_CHARS + 1)
        );
        let err = run(&huge).unwrap_err();
        assert_eq!(err.error.code.as_deref(), Some("input_too_long"));
    }

    #[test]
    fn rejects_empty_and_whitespace_only_strings_with_provider_variant() {
        // The provider-injected variant used by the HTTP route must apply the
        // same checks as the env-provider variant.
        for body in [
            r#"{"input":"","model":"m"}"#,
            r#"{"input":["a","  "],"model":"m"}"#,
            r#"{"input":[],"model":"m"}"#,
            r#"{"input":[[]],"model":"m"}"#,
        ] {
            assert!(run(body).is_err(), "{body}");
        }
    }

    #[test]
    fn rejects_provider_count_mismatch_with_short_code() {
        let err = handle_openai_embeddings_with_provider(
            r#"{"input":["a","b"],"model":"m"}"#,
            &CountingProvider {
                dims: 4,
                short: true,
            },
        )
        .unwrap_err();
        assert_eq!(err.error.kind, "server_error");
        assert_eq!(err.error.code.as_deref(), Some("embedding_provider_error"));
        assert_eq!(err.error.message, "embedding_provider_error");
    }

    #[test]
    fn dimensions_param_is_validated_against_provider() {
        assert!(run(r#"{"input":"a","model":"m","dimensions":4}"#).is_ok());
        for bad in ["8", "0", "-4"] {
            let err = run(&format!(
                r#"{{"input":"a","model":"m","dimensions":{bad}}}"#
            ))
            .unwrap_err();
            assert_eq!(err.error.param.as_deref(), Some("dimensions"));
            assert_eq!(err.error.code.as_deref(), Some("unsupported_dimensions"));
        }
    }

    #[test]
    fn user_field_is_ignored_and_bad_input_type_names_the_param() {
        assert!(run(r#"{"input":"a","model":"m","user":"someone"}"#).is_ok());
        let err = run(r#"{"input":{"a":1},"model":"m"}"#).unwrap_err();
        assert_eq!(err.error.param.as_deref(), Some("input"));
        assert_eq!(err.error.code.as_deref(), Some("invalid_input_type"));
        let err = run(r#"{"input":["a",1],"model":"m"}"#).unwrap_err();
        assert_eq!(err.error.code.as_deref(), Some("invalid_input_type"));
    }

    #[test]
    fn error_body_has_all_four_openai_fields() {
        let err = run(r#"{"input":"a","model":"m","encoding_format":"binary"}"#).unwrap_err();
        let value: serde_json::Value = serde_json::to_value(&err).unwrap();
        let error = &value["error"];
        for key in ["message", "type", "param", "code"] {
            assert!(error.get(key).is_some(), "missing {key}: {value}");
        }
        assert_eq!(error["param"], "encoding_format");
    }
}
