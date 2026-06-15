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
//!   configured [`EmbeddingProvider`] (default: [`HashEmbeddingProvider`]).
//! - `encoding_format` is accepted but currently only `"float"` is
//!   supported (base64 JSON encoding may be added later).
//!
//! The endpoint is intentionally provider-agnostic: swap the
//!   `HashEmbeddingProvider` for an `OllamaEmbeddingProvider` or
//!   `OpenAIEmbeddingProvider` and the wire format stays the same.

use embeddings::{EmbeddingError, EmbeddingProvider, HashEmbeddingProvider, OllamaEmbeddingProvider, OpenAIEmbeddingProvider};
use serde::{Deserialize, Serialize};

/// Wire-compatible request body for `POST /v1/embeddings`.
///
/// OpenAI's `input` field accepts either a single string or a list of
/// strings; we use `#[serde(untagged)]` to accept both shapes with a
/// single enum.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OpenAIEmbeddingsRequest {
    pub input: OpenAIInput,
    pub model: String,
    #[serde(default)]
    pub user: Option<String>,
    #[serde(default)]
    pub encoding_format: Option<String>,
}

/// Accepts either `"input": "single text"` or `"input": ["text1", "text2"]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OpenAIInput {
    Single(String),
    Multiple(Vec<String>),
}

impl OpenAIInput {
    pub fn texts(&self) -> Vec<&str> {
        match self {
            OpenAIInput::Single(s) => vec![s.as_str()],
            OpenAIInput::Multiple(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            OpenAIInput::Single(_) => 1,
            OpenAIInput::Multiple(v) => v.len(),
        }
    }

    /// Returns `true` only when `input` is an empty array. A single
    /// string input is never empty (it has one element), and a
    /// non-empty array obviously isn't empty either.
    pub fn is_empty(&self) -> bool {
        match self {
            OpenAIInput::Single(_) => false,
            OpenAIInput::Multiple(v) => v.is_empty(),
        }
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

#[derive(Debug, Clone, Serialize)]
pub struct OpenAIEmbeddingData {
    pub object: &'static str,
    pub embedding: Vec<f32>,
    pub index: usize,
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

    pub fn server_error(message: impl Into<String>) -> Self {
        Self {
            error: OpenAIErrorBody {
                message: message.into(),
                kind: "server_error",
                param: None,
                code: None,
            },
        }
    }
}

/// Handler for `POST /v1/embeddings`. Constructs the default
/// `HashEmbeddingProvider` (deterministic, no network) and serializes
/// the response. Swap the provider to integrate Ollama, OpenAI, or
/// custom models.
pub fn handle_openai_embeddings(body: &str) -> Result<OpenAIEmbeddingsResponse, OpenAIErrorResponse> {
    let req: OpenAIEmbeddingsRequest =
        serde_json::from_str(body).map_err(|err| {
            OpenAIErrorResponse::invalid_request(format!("invalid request body: {err}"))
        })?;

    if req.input.is_empty() {
        return Err(OpenAIErrorResponse::invalid_request(
            "input must contain at least one text",
        ));
    }
    for text in req.input.texts() {
        if text.is_empty() {
            return Err(OpenAIErrorResponse::invalid_request(
                "input text must be non-empty",
            ));
        }
    }
    if let Some(fmt) = req.encoding_format.as_deref()
        && fmt != "float"
    {
        return Err(OpenAIErrorResponse::invalid_request(format!(
            "encoding_format '{fmt}' is not supported; only 'float' is currently implemented"
        )));
    }

    let provider = HashEmbeddingProvider::default();
    let texts: Vec<String> = req.input.texts().iter().map(|s| s.to_string()).collect();
    let embeddings = provider
        .embed(&texts)
        .map_err(|err| OpenAIErrorResponse::server_error(format!("embedding failed: {err}")))?;

    if embeddings.len() != texts.len() {
        return Err(OpenAIErrorResponse::server_error(format!(
            "embedding count mismatch: expected {}, got {}",
            texts.len(),
            embeddings.len()
        )));
    }

    let data: Vec<OpenAIEmbeddingData> = embeddings
        .into_iter()
        .enumerate()
        .map(|(index, embedding)| OpenAIEmbeddingData {
            object: "embedding",
            embedding,
            index,
        })
        .collect();

    // Approximate token count: whitespace-separated word count. This is
    // intentionally a rough estimate; full BPE tokenization is out of
    // scope for the v1 endpoint.
    let total_tokens: usize = texts.iter().map(|t| t.split_whitespace().count()).sum();

    Ok(OpenAIEmbeddingsResponse {
        object: "list",
        data,
        model: req.model,
        usage: OpenAIUsage {
            prompt_tokens: total_tokens,
            total_tokens,
        },
    })
}

/// Run an OpenAI-compatible embeddings request with a custom provider.
/// Useful for tests and for callers that want to inject a different
/// embedding backend (Ollama, OpenAI passthrough, custom).
pub fn handle_openai_embeddings_with_provider(
    body: &str,
    provider: &dyn EmbeddingProvider,
) -> Result<OpenAIEmbeddingsResponse, OpenAIErrorResponse> {
    let req: OpenAIEmbeddingsRequest =
        serde_json::from_str(body).map_err(|err| {
            OpenAIErrorResponse::invalid_request(format!("invalid request body: {err}"))
        })?;

    if req.input.is_empty() {
        return Err(OpenAIErrorResponse::invalid_request(
            "input must contain at least one text",
        ));
    }

    let texts: Vec<String> = req.input.texts().iter().map(|s| s.to_string()).collect();
    let embeddings = provider
        .embed(&texts)
        .map_err(|err: EmbeddingError| {
            OpenAIErrorResponse::server_error(format!("embedding failed: {err}"))
        })?;

    let data: Vec<OpenAIEmbeddingData> = embeddings
        .into_iter()
        .enumerate()
        .map(|(index, embedding)| OpenAIEmbeddingData {
            object: "embedding",
            embedding,
            index,
        })
        .collect();

    let total_tokens: usize = texts.iter().map(|t| t.split_whitespace().count()).sum();

    Ok(OpenAIEmbeddingsResponse {
        object: "list",
        data,
        model: req.model,
        usage: OpenAIUsage {
            prompt_tokens: total_tokens,
            total_tokens,
        },
    })
}


// ---------------------------------------------------------------------------
// Env-driven provider selection
// ---------------------------------------------------------------------------

/// Build an [`EmbeddingProvider`] from the process environment. Reads:
///
/// - `DASH_EMBEDDING_PROVIDER` — `"hash"` (default, deterministic, no
///   network), `"ollama"`, or `"openai"`. Unknown values fall back to
///   `hash` with a tracing warning.
/// - For `ollama`: `DASH_OLLAMA_ENDPOINT` (default `http://localhost:11434`),
///   `DASH_OLLAMA_MODEL` (default `nomic-embed-text`).
/// - For `openai`: `DASH_OPENAI_API_KEY` (required; error if missing),
///   `DASH_OPENAI_MODEL` (default `text-embedding-3-small`).
///
/// The returned trait object is `Send + Sync + 'static` so it can be
/// shared across the service's worker threads.
pub fn select_provider_from_env() -> Box<dyn EmbeddingProvider + Send + Sync + 'static> {
    let provider = std::env::var("DASH_EMBEDDING_PROVIDER")
        .unwrap_or_else(|_| "hash".to_string())
        .to_ascii_lowercase();

    match provider.as_str() {
        "ollama" => {
            let endpoint = std::env::var("DASH_OLLAMA_ENDPOINT")
                .unwrap_or_else(|_| "http://localhost:11434".to_string());
            let model = std::env::var("DASH_OLLAMA_MODEL")
                .unwrap_or_else(|_| "nomic-embed-text".to_string());
            Box::new(OllamaEmbeddingProvider::new(model, Some(endpoint)))
        }
        "openai" => {
            let key = std::env::var("DASH_OPENAI_API_KEY")
                .unwrap_or_default();
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
            eprintln!(
                "dash: unknown DASH_EMBEDDING_PROVIDER='{other}'; falling back to hash"
            );
            Box::new(HashEmbeddingProvider::default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let body =
            r#"{"input": ["hello", "world", "foo"], "model": "text-embedding-3-small"}"#;
        let req: OpenAIEmbeddingsRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.input.len(), 3);
        assert_eq!(req.input.texts(), vec!["hello", "world", "foo"]);
    }

    #[test]
    fn rejects_empty_array() {
        let body = r#"{"input": [], "model": "text-embedding-3-small"}"#;
        let err = handle_openai_embeddings(body).unwrap_err();
        assert_eq!(err.error.kind, "invalid_request_error");
        assert!(err.error.message.contains("at least one text"));
    }

    #[test]
    fn rejects_empty_string_in_array() {
        let body = r#"{"input": ["hello", ""], "model": "x"}"#;
        let err = handle_openai_embeddings(body).unwrap_err();
        assert_eq!(err.error.kind, "invalid_request_error");
        assert!(err.error.message.contains("non-empty"));
    }

    #[test]
    fn rejects_unsupported_encoding_format() {
        let body = r#"{"input": "hi", "model": "x", "encoding_format": "base64"}"#;
        let err = handle_openai_embeddings(body).unwrap_err();
        assert_eq!(err.error.kind, "invalid_request_error");
        assert!(err.error.message.contains("encoding_format"));
    }

    #[test]
    fn rejects_malformed_json() {
        let body = r#"{"input": "hi""#; // truncated
        let err = handle_openai_embeddings(body).unwrap_err();
        assert_eq!(err.error.kind, "invalid_request_error");
        assert!(err.error.message.contains("invalid request body"));
    }

    #[test]
    fn response_shape_is_openai_compatible_for_single_input() {
        let body = r#"{"input": "hello world", "model": "text-embedding-3-small"}"#;
        let resp = handle_openai_embeddings(body).unwrap();
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
        let resp = handle_openai_embeddings(body).unwrap();
        assert_eq!(resp.data.len(), 3);
        assert_eq!(resp.data[0].index, 0);
        assert_eq!(resp.data[1].index, 1);
        assert_eq!(resp.data[2].index, 2);
        // Determinism: the SAME input must produce the SAME vector.
        // Compare the first item against a second request for the same input.
        let resp_again = handle_openai_embeddings(body).unwrap();
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
        let resp = handle_openai_embeddings(body).unwrap();
        let provider = HashEmbeddingProvider::default();
        assert_eq!(resp.data[0].embedding.len(), provider.dimensions());
    }

    #[test]
    fn prompt_tokens_approximated_by_word_count() {
        let body = r#"{"input": "the quick brown fox", "model": "x"}"#;
        let resp = handle_openai_embeddings(body).unwrap();
        assert_eq!(resp.usage.prompt_tokens, 4);
        assert_eq!(resp.usage.total_tokens, 4);
    }

    #[test]
    fn custom_provider_is_used() {
        let body = r#"{"input": "hello", "model": "test-model"}"#;
        let provider = HashEmbeddingProvider::new(8);
        let resp = handle_openai_embeddings_with_provider(body, &provider).unwrap();
        assert_eq!(resp.data.len(), 1);
        assert_eq!(resp.data[0].embedding.len(), 8);
        assert_eq!(resp.model, "test-model");
    }

    #[test]
    fn error_response_shape_is_openai_compatible() {
        let body = r#"{"input": [], "model": "x"}"#;
        let err = handle_openai_embeddings(body).unwrap_err();
        let json = serde_json::to_string(&err).unwrap();
        assert!(json.contains("\"error\":{"));
        assert!(json.contains("\"message\":"));
        assert!(json.contains("\"type\":\"invalid_request_error\""));
    }
}
