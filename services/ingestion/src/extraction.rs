use crate::api::{
    IngestApiRequest, IngestBatchApiRequest, IngestDocumentApiRequest, IngestRawApiRequest,
};
use schema::{Claim, Evidence, Stance};
use std::io::Write;
use std::process::{Command, Stdio};

const DEFAULT_CLAIM_CONFIDENCE: f32 = 0.8;
const DEFAULT_SOURCE_QUALITY: f32 = 0.8;
const DEFAULT_MIN_SENTENCE_CHARS: usize = 24;
const DEFAULT_MAX_CLAIMS: usize = 32;
const MAX_CLAIMS_LIMIT: usize = 512;
const MAX_SENTENCE_CHARS_LIMIT: usize = 4096;
const DEFAULT_EXTRACTION_PROVIDER: &str = "rule_sentence";
const DEFAULT_DOCUMENT_PARSER_PROVIDER: &str = "builtin_utf8";
const DEFAULT_EMBEDDING_PROVIDER: &str = "hash_vector";
const DEFAULT_EMBEDDING_DIMENSIONS: usize = 64;
const MIN_EMBEDDING_DIMENSIONS: usize = 8;
const MAX_EMBEDDING_DIMENSIONS: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
struct ExtractedSentence {
    canonical_text: String,
    span_start: Option<u32>,
    span_end: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ExtractionProvider {
    RuleSentence,
    AdapterCommand(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DocumentParserProvider {
    BuiltinUtf8,
    AdapterCommand(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum EmbeddingProvider {
    Disabled,
    HashVector { dimensions: usize },
    AdapterCommand(String),
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestRawBuildOutput {
    pub batch: IngestBatchApiRequest,
    pub embedding_provider: String,
    pub embeddings_generated: usize,
    pub embedding_dimensions: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IngestDocumentBuildOutput {
    pub batch: IngestBatchApiRequest,
    pub parser_provider: String,
    pub mime_type: String,
    pub embedding_provider: String,
    pub embeddings_generated: usize,
    pub embedding_dimensions: Option<usize>,
}

pub fn build_ingest_batch_from_raw_request(
    request: &IngestRawApiRequest,
) -> Result<IngestBatchApiRequest, String> {
    Ok(build_ingest_raw_output_from_request(request)?.batch)
}

pub fn build_ingest_raw_output_from_request(
    request: &IngestRawApiRequest,
) -> Result<IngestRawBuildOutput, String> {
    let tenant_id = request.tenant_id.trim();
    if tenant_id.is_empty() {
        return Err("tenant_id is required".to_string());
    }
    let document_id = request.document_id.trim();
    if document_id.is_empty() {
        return Err("document_id is required".to_string());
    }
    let source_id = request.source_id.trim();
    if source_id.is_empty() {
        return Err("source_id is required".to_string());
    }
    if request.text.trim().is_empty() {
        return Err("text is required".to_string());
    }

    let claim_confidence = request.claim_confidence.unwrap_or(DEFAULT_CLAIM_CONFIDENCE);
    if !claim_confidence.is_finite() || !(0.0..=1.0).contains(&claim_confidence) {
        return Err("claim_confidence must be a finite number in [0, 1]".to_string());
    }

    let source_quality = request.source_quality.unwrap_or(DEFAULT_SOURCE_QUALITY);
    if !source_quality.is_finite() || !(0.0..=1.0).contains(&source_quality) {
        return Err("source_quality must be a finite number in [0, 1]".to_string());
    }

    let min_sentence_chars = request
        .min_sentence_chars
        .unwrap_or(DEFAULT_MIN_SENTENCE_CHARS)
        .clamp(1, MAX_SENTENCE_CHARS_LIMIT);
    let max_claims = request
        .max_claims
        .unwrap_or(DEFAULT_MAX_CLAIMS)
        .clamp(1, MAX_CLAIMS_LIMIT);

    let provider = resolve_extraction_provider()?;
    let extracted =
        extract_claim_sentences(&provider, &request.text, min_sentence_chars, max_claims)?;
    if extracted.is_empty() {
        return Err(format!(
            "text did not yield any claim-sized sentences (min_sentence_chars={min_sentence_chars})"
        ));
    }

    let tenant_component = sanitize_id_component(tenant_id, 48);
    let document_component = sanitize_id_component(document_id, 80);
    let extraction_model = request.extraction_model.clone();
    let embedding_model = request.embedding_model.clone();
    let embedding_provider = resolve_embedding_provider(request.generate_embeddings)?;
    let embedding_provider_component = embedding_provider.cache_key_component();
    let mut items = Vec::with_capacity(extracted.len());
    let mut embeddings_generated = 0usize;
    let mut embedding_dimensions = None;

    for (index, sentence) in extracted.into_iter().enumerate() {
        let claim_embedding =
            generate_claim_embedding(&embedding_provider, &sentence.canonical_text)?;
        if let Some(vector) = claim_embedding.as_ref() {
            embeddings_generated += 1;
            if let Some(existing_dimension) = embedding_dimensions {
                if existing_dimension != vector.len() {
                    return Err(format!(
                        "embedding provider produced inconsistent dimensions (expected {existing_dimension}, observed {})",
                        vector.len()
                    ));
                }
            } else {
                embedding_dimensions = Some(vector.len());
            }
        }
        let claim_id = format!(
            "raw:{tenant_component}:{document_component}:c{:04}",
            index + 1
        );
        let evidence_id = format!(
            "raw:{tenant_component}:{document_component}:e{:04}",
            index + 1
        );
        let claim = Claim {
            claim_id: claim_id.clone(),
            tenant_id: tenant_id.to_string(),
            canonical_text: sentence.canonical_text,
            confidence: claim_confidence,
            event_time_unix: None,
            entities: Vec::new(),
            embedding_ids: embedding_model.clone().into_iter().collect(),
            claim_type: None,
            valid_from: None,
            valid_to: None,
            created_at: None,
            updated_at: None,
        };
        let evidence = Evidence {
            evidence_id,
            claim_id,
            source_id: source_id.to_string(),
            stance: Stance::Supports,
            source_quality,
            chunk_id: Some(format!("sentence-{}", index + 1)),
            span_start: sentence.span_start,
            span_end: sentence.span_end,
            doc_id: Some(document_id.to_string()),
            extraction_model: extraction_model.clone(),
            ingested_at: None,
        };
        items.push(IngestApiRequest {
            claim,
            claim_embedding,
            evidence: vec![evidence],
            edges: Vec::new(),
        });
    }

    Ok(IngestRawBuildOutput {
        batch: IngestBatchApiRequest {
            commit_id: Some(raw_commit_id(
                tenant_id,
                document_id,
                min_sentence_chars,
                max_claims,
                &provider.cache_key_component(),
                &embedding_provider_component,
            )),
            items,
        },
        embedding_provider: embedding_provider_component,
        embeddings_generated,
        embedding_dimensions,
    })
}

pub fn build_ingest_batch_from_document_request(
    request: &IngestDocumentApiRequest,
) -> Result<IngestDocumentBuildOutput, String> {
    let mime_type = request.mime_type.trim().to_ascii_lowercase();
    if mime_type.is_empty() {
        return Err("mime_type is required".to_string());
    }

    let (text, parser_provider) = if let Some(text) = request
        .text
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
    {
        (text.to_string(), "inline_text".to_string())
    } else {
        let content_base64 = request
            .content_base64
            .as_ref()
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| "either text or content_base64 is required".to_string())?;
        let bytes = decode_base64(content_base64)?;
        let provider = resolve_document_parser_provider()?;
        let parsed = parse_document_bytes(&provider, &mime_type, &bytes)?;
        let parsed = parsed.trim().to_string();
        if parsed.is_empty() {
            return Err("document parser yielded empty text".to_string());
        }
        (parsed, provider.cache_key_component())
    };

    let raw_request = IngestRawApiRequest {
        tenant_id: request.tenant_id.clone(),
        document_id: request.document_id.clone(),
        source_id: request.source_id.clone(),
        text,
        extraction_model: request.extraction_model.clone(),
        claim_confidence: request.claim_confidence,
        source_quality: request.source_quality,
        min_sentence_chars: request.min_sentence_chars,
        max_claims: request.max_claims,
        generate_embeddings: request.generate_embeddings,
        embedding_model: request.embedding_model.clone(),
    };
    let parsed = build_ingest_raw_output_from_request(&raw_request)?;
    Ok(IngestDocumentBuildOutput {
        batch: parsed.batch,
        parser_provider,
        mime_type,
        embedding_provider: parsed.embedding_provider,
        embeddings_generated: parsed.embeddings_generated,
        embedding_dimensions: parsed.embedding_dimensions,
    })
}

fn raw_commit_id(
    tenant_id: &str,
    document_id: &str,
    min_sentence_chars: usize,
    max_claims: usize,
    provider_component: &str,
    embedding_provider_component: &str,
) -> String {
    let tenant_component = sanitize_id_component(tenant_id, 48);
    let document_component = sanitize_id_component(document_id, 80);
    format!(
        "raw:{tenant_component}:{document_component}:p{provider_component}:e{embedding_provider_component}:m{min_sentence_chars}:k{max_claims}"
    )
}

fn resolve_extraction_provider() -> Result<ExtractionProvider, String> {
    let raw = env_with_fallback(
        "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
        "EME_INGEST_RAW_EXTRACTION_PROVIDER",
    )
    .unwrap_or_else(|| DEFAULT_EXTRACTION_PROVIDER.to_string());
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == "rule" || normalized == "rule_sentence" {
        return Ok(ExtractionProvider::RuleSentence);
    }
    if normalized == "adapter_command" || normalized == "model_adapter" {
        let command =
            env_with_fallback("DASH_INGEST_RAW_ADAPTER_CMD", "EME_INGEST_RAW_ADAPTER_CMD")
                .unwrap_or_default();
        let command = command.trim().to_string();
        if command.is_empty() {
            return Err(
                "adapter_command provider requires DASH_INGEST_RAW_ADAPTER_CMD (or EME_*)"
                    .to_string(),
            );
        }
        return Ok(ExtractionProvider::AdapterCommand(command));
    }
    Err(format!(
        "unsupported extraction provider '{raw}'; expected rule_sentence or adapter_command"
    ))
}

fn resolve_document_parser_provider() -> Result<DocumentParserProvider, String> {
    let raw = env_with_fallback(
        "DASH_INGEST_DOCUMENT_PARSER_PROVIDER",
        "EME_INGEST_DOCUMENT_PARSER_PROVIDER",
    )
    .unwrap_or_else(|| DEFAULT_DOCUMENT_PARSER_PROVIDER.to_string());
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty() || normalized == "builtin_utf8" || normalized == "utf8" {
        return Ok(DocumentParserProvider::BuiltinUtf8);
    }
    if normalized == "adapter_command" || normalized == "document_adapter" {
        let command = env_with_fallback(
            "DASH_INGEST_DOCUMENT_ADAPTER_CMD",
            "EME_INGEST_DOCUMENT_ADAPTER_CMD",
        )
        .unwrap_or_default();
        let command = command.trim().to_string();
        if command.is_empty() {
            return Err(
                "adapter_command parser requires DASH_INGEST_DOCUMENT_ADAPTER_CMD (or EME_*)"
                    .to_string(),
            );
        }
        return Ok(DocumentParserProvider::AdapterCommand(command));
    }
    Err(format!(
        "unsupported document parser provider '{raw}'; expected builtin_utf8 or adapter_command"
    ))
}

fn resolve_embedding_provider(
    generate_embeddings: Option<bool>,
) -> Result<EmbeddingProvider, String> {
    if !generate_embeddings.unwrap_or(false) {
        return Ok(EmbeddingProvider::Disabled);
    }
    let raw = env_with_fallback(
        "DASH_INGEST_EMBEDDING_PROVIDER",
        "EME_INGEST_EMBEDDING_PROVIDER",
    )
    .unwrap_or_else(|| DEFAULT_EMBEDDING_PROVIDER.to_string());
    let normalized = raw.trim().to_ascii_lowercase();
    if normalized.is_empty()
        || matches!(normalized.as_str(), "hash" | "hash_vector" | "builtin_hash")
    {
        return Ok(EmbeddingProvider::HashVector {
            dimensions: resolve_embedding_dimensions(),
        });
    }
    if matches!(normalized.as_str(), "off" | "none" | "disabled") {
        return Ok(EmbeddingProvider::Disabled);
    }
    if normalized == "adapter_command" || normalized == "model_adapter" {
        let command = env_with_fallback(
            "DASH_INGEST_EMBEDDING_ADAPTER_CMD",
            "EME_INGEST_EMBEDDING_ADAPTER_CMD",
        )
        .unwrap_or_default();
        let command = command.trim().to_string();
        if command.is_empty() {
            return Err(
                "adapter_command embedding provider requires DASH_INGEST_EMBEDDING_ADAPTER_CMD (or EME_*)"
                    .to_string(),
            );
        }
        return Ok(EmbeddingProvider::AdapterCommand(command));
    }
    Err(format!(
        "unsupported embedding provider '{raw}'; expected hash_vector or adapter_command"
    ))
}

fn resolve_embedding_dimensions() -> usize {
    let raw = env_with_fallback(
        "DASH_INGEST_EMBEDDING_DIMENSIONS",
        "EME_INGEST_EMBEDDING_DIMENSIONS",
    )
    .unwrap_or_else(|| DEFAULT_EMBEDDING_DIMENSIONS.to_string());
    raw.parse::<usize>()
        .ok()
        .unwrap_or(DEFAULT_EMBEDDING_DIMENSIONS)
        .clamp(MIN_EMBEDDING_DIMENSIONS, MAX_EMBEDDING_DIMENSIONS)
}

fn parse_document_bytes(
    provider: &DocumentParserProvider,
    mime_type: &str,
    bytes: &[u8],
) -> Result<String, String> {
    match provider {
        DocumentParserProvider::BuiltinUtf8 => parse_document_bytes_builtin_utf8(mime_type, bytes),
        DocumentParserProvider::AdapterCommand(command) => {
            parse_document_bytes_with_adapter_command(command, mime_type, bytes)
        }
    }
}

fn parse_document_bytes_builtin_utf8(mime_type: &str, bytes: &[u8]) -> Result<String, String> {
    let utf8_allowed = mime_type.starts_with("text/")
        || matches!(
            mime_type,
            "application/json"
                | "application/xml"
                | "text/xml"
                | "application/yaml"
                | "application/x-yaml"
                | "application/markdown"
                | "application/csv"
        );
    if !utf8_allowed {
        return Err(format!(
            "mime_type '{mime_type}' requires document parser adapter; set DASH_INGEST_DOCUMENT_PARSER_PROVIDER=adapter_command"
        ));
    }
    String::from_utf8(bytes.to_vec())
        .map_err(|_| format!("mime_type '{mime_type}' payload is not valid UTF-8"))
}

fn parse_document_bytes_with_adapter_command(
    command: &str,
    mime_type: &str,
    bytes: &[u8],
) -> Result<String, String> {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(command)
        .env("DASH_DOCUMENT_MIME_TYPE", mime_type)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to start document adapter command: {err}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(bytes)
            .map_err(|err| format!("failed to write document bytes to adapter command: {err}"))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|err| format!("failed to await document adapter command: {err}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "document adapter command exited with status {}{}",
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        ));
    }

    String::from_utf8(output.stdout)
        .map_err(|_| "document adapter stdout is not valid UTF-8".to_string())
}

fn decode_base64(input: &str) -> Result<Vec<u8>, String> {
    let compact = input
        .bytes()
        .filter(|byte| !byte.is_ascii_whitespace())
        .collect::<Vec<_>>();
    if compact.is_empty() {
        return Err("content_base64 must not be empty".to_string());
    }

    let mut out = Vec::with_capacity(compact.len() * 3 / 4);
    let mut idx = 0usize;
    while idx < compact.len() {
        let remaining = compact.len() - idx;
        if remaining < 2 {
            return Err("content_base64 has invalid length".to_string());
        }
        let c0 = compact[idx];
        let c1 = compact[idx + 1];
        let v0 = base64_value(c0)
            .ok_or_else(|| format!("content_base64 contains invalid character '{}'", c0 as char))?;
        let v1 = base64_value(c1)
            .ok_or_else(|| format!("content_base64 contains invalid character '{}'", c1 as char))?;

        if remaining == 2 {
            out.push((v0 << 2) | (v1 >> 4));
            break;
        }

        let c2 = compact[idx + 2];
        if remaining == 3 {
            let v2 = base64_value(c2).ok_or_else(|| {
                format!("content_base64 contains invalid character '{}'", c2 as char)
            })?;
            out.push((v0 << 2) | (v1 >> 4));
            out.push(((v1 & 0x0f) << 4) | (v2 >> 2));
            break;
        }

        let c3 = compact[idx + 3];
        if c2 == b'=' {
            if c3 != b'=' {
                return Err("content_base64 has invalid padding".to_string());
            }
            out.push((v0 << 2) | (v1 >> 4));
        } else if c3 == b'=' {
            let v2 = base64_value(c2).ok_or_else(|| {
                format!("content_base64 contains invalid character '{}'", c2 as char)
            })?;
            out.push((v0 << 2) | (v1 >> 4));
            out.push(((v1 & 0x0f) << 4) | (v2 >> 2));
        } else {
            let v2 = base64_value(c2).ok_or_else(|| {
                format!("content_base64 contains invalid character '{}'", c2 as char)
            })?;
            let v3 = base64_value(c3).ok_or_else(|| {
                format!("content_base64 contains invalid character '{}'", c3 as char)
            })?;
            out.push((v0 << 2) | (v1 >> 4));
            out.push(((v1 & 0x0f) << 4) | (v2 >> 2));
            out.push(((v2 & 0x03) << 6) | v3);
        }
        idx += 4;
    }
    Ok(out)
}

fn base64_value(byte: u8) -> Option<u8> {
    match byte {
        b'A'..=b'Z' => Some(byte - b'A'),
        b'a'..=b'z' => Some(byte - b'a' + 26),
        b'0'..=b'9' => Some(byte - b'0' + 52),
        b'+' | b'-' => Some(62),
        b'/' | b'_' => Some(63),
        _ => None,
    }
}

fn generate_claim_embedding(
    provider: &EmbeddingProvider,
    claim_text: &str,
) -> Result<Option<Vec<f32>>, String> {
    match provider {
        EmbeddingProvider::Disabled => Ok(None),
        EmbeddingProvider::HashVector { dimensions } => {
            Ok(Some(generate_hash_embedding(claim_text, *dimensions)))
        }
        EmbeddingProvider::AdapterCommand(command) => {
            generate_embedding_with_adapter_command(command, claim_text).map(Some)
        }
    }
}

fn generate_hash_embedding(text: &str, dimensions: usize) -> Vec<f32> {
    let mut vector = vec![0.0f32; dimensions];
    for token in text.split_whitespace() {
        let hash = fnv1a_64(token.as_bytes());
        let index = (hash % dimensions as u64) as usize;
        let sign = if ((hash >> 63) & 1) == 0 { 1.0 } else { -1.0 };
        vector[index] += sign;
    }

    let norm_sq = vector
        .iter()
        .map(|value| (*value as f64) * (*value as f64))
        .sum::<f64>();
    if norm_sq > 0.0 {
        let norm = norm_sq.sqrt() as f32;
        for value in &mut vector {
            *value /= norm;
        }
    } else if let Some(first) = vector.first_mut() {
        *first = 1.0;
    }
    vector
}

fn fnv1a_64(bytes: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash = FNV_OFFSET;
    for byte in bytes {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn generate_embedding_with_adapter_command(command: &str, text: &str) -> Result<Vec<f32>, String> {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to start embedding adapter command: {err}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(text.as_bytes()).map_err(|err| {
            format!("failed to write claim text to embedding adapter command: {err}")
        })?;
    }

    let output = child
        .wait_with_output()
        .map_err(|err| format!("failed to await embedding adapter command: {err}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "embedding adapter command exited with status {}{}",
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        ));
    }

    parse_embedding_vector_output(&output.stdout)
}

fn parse_embedding_vector_output(stdout: &[u8]) -> Result<Vec<f32>, String> {
    let raw = String::from_utf8(stdout.to_vec())
        .map_err(|_| "embedding adapter stdout is not valid UTF-8".to_string())?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("embedding adapter produced empty output".to_string());
    }
    let trimmed = trimmed.trim_start_matches('[').trim_end_matches(']');
    let mut out = Vec::new();
    for token in trimmed.split(|ch: char| ch == ',' || ch.is_ascii_whitespace()) {
        let token = token.trim();
        if token.is_empty() {
            continue;
        }
        let value = token
            .parse::<f32>()
            .map_err(|_| format!("embedding adapter emitted non-numeric value '{token}'"))?;
        if !value.is_finite() {
            return Err("embedding adapter emitted non-finite value".to_string());
        }
        out.push(value);
    }
    if out.is_empty() {
        return Err("embedding adapter did not emit any numeric values".to_string());
    }
    Ok(out)
}

fn extract_claim_sentences(
    provider: &ExtractionProvider,
    text: &str,
    min_sentence_chars: usize,
    max_claims: usize,
) -> Result<Vec<ExtractedSentence>, String> {
    match provider {
        ExtractionProvider::RuleSentence => {
            Ok(extract_sentences(text, min_sentence_chars, max_claims))
        }
        ExtractionProvider::AdapterCommand(command) => {
            extract_with_adapter_command(command, text, min_sentence_chars, max_claims)
        }
    }
}

#[cfg(feature = "model-extraction-adapter")]
fn extract_with_adapter_command(
    command: &str,
    text: &str,
    min_sentence_chars: usize,
    max_claims: usize,
) -> Result<Vec<ExtractedSentence>, String> {
    let mut child = Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|err| format!("failed to start extraction adapter command: {err}"))?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(text.as_bytes())
            .map_err(|err| format!("failed to write request text to adapter command: {err}"))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|err| format!("failed to await extraction adapter command: {err}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        return Err(format!(
            "extraction adapter command exited with status {}{}",
            output.status,
            if stderr.is_empty() {
                String::new()
            } else {
                format!(": {stderr}")
            }
        ));
    }

    let stdout = String::from_utf8(output.stdout)
        .map_err(|_| "adapter command stdout is not valid UTF-8".to_string())?;
    let mut out = Vec::new();
    for line in stdout.lines() {
        let canonical_text = normalize_whitespace(line.trim());
        if canonical_text.is_empty() || canonical_text.chars().count() < min_sentence_chars {
            continue;
        }
        out.push(ExtractedSentence {
            canonical_text,
            span_start: None,
            span_end: None,
        });
        if out.len() >= max_claims {
            break;
        }
    }
    Ok(out)
}

#[cfg(not(feature = "model-extraction-adapter"))]
fn extract_with_adapter_command(
    _command: &str,
    _text: &str,
    _min_sentence_chars: usize,
    _max_claims: usize,
) -> Result<Vec<ExtractedSentence>, String> {
    Err(
        "adapter_command provider requires ingestion feature 'model-extraction-adapter'"
            .to_string(),
    )
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

fn extract_sentences(
    text: &str,
    min_sentence_chars: usize,
    max_claims: usize,
) -> Vec<ExtractedSentence> {
    let mut out = Vec::new();
    let mut segment_start = 0usize;

    for (idx, ch) in text.char_indices() {
        if matches!(ch, '.' | '!' | '?' | '\n' | '\r') {
            push_extracted_sentence(
                &mut out,
                &text[segment_start..idx],
                segment_start,
                min_sentence_chars,
            );
            segment_start = idx + ch.len_utf8();
            if out.len() >= max_claims {
                return out;
            }
        }
    }

    push_extracted_sentence(
        &mut out,
        &text[segment_start..],
        segment_start,
        min_sentence_chars,
    );
    if out.len() > max_claims {
        out.truncate(max_claims);
    }
    out
}

fn push_extracted_sentence(
    out: &mut Vec<ExtractedSentence>,
    raw_segment: &str,
    base_offset: usize,
    min_sentence_chars: usize,
) {
    let trim_start_delta = raw_segment.len() - raw_segment.trim_start().len();
    let trim_end_delta = raw_segment.len() - raw_segment.trim_end().len();
    let trimmed = raw_segment.trim();
    if trimmed.is_empty() {
        return;
    }

    let canonical_text = normalize_whitespace(trimmed);
    if canonical_text.chars().count() < min_sentence_chars {
        return;
    }

    let start = base_offset.saturating_add(trim_start_delta);
    let end_exclusive = base_offset
        .saturating_add(raw_segment.len())
        .saturating_sub(trim_end_delta);
    let span_start = u32::try_from(start).ok();
    let span_end = end_exclusive
        .checked_sub(1)
        .and_then(|value| u32::try_from(value).ok());

    out.push(ExtractedSentence {
        canonical_text,
        span_start,
        span_end,
    });
}

fn normalize_whitespace(input: &str) -> String {
    input.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn sanitize_id_component(input: &str, max_len: usize) -> String {
    let mut out = String::with_capacity(max_len.min(input.len()));
    for ch in input.chars() {
        if out.len() >= max_len {
            break;
        }
        let c = if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':') {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        out.push(c);
    }
    let out = out.trim_matches('-').to_string();
    if out.is_empty() {
        "raw".to_string()
    } else {
        out
    }
}

impl ExtractionProvider {
    fn cache_key_component(&self) -> String {
        match self {
            Self::RuleSentence => "rule_sentence".to_string(),
            Self::AdapterCommand(command) => {
                let normalized = normalize_whitespace(command.trim());
                let compact = if normalized.len() > 48 {
                    normalized[..48].to_string()
                } else {
                    normalized
                };
                sanitize_id_component(&compact, 48)
            }
        }
    }
}

impl DocumentParserProvider {
    fn cache_key_component(&self) -> String {
        match self {
            Self::BuiltinUtf8 => "builtin_utf8".to_string(),
            Self::AdapterCommand(command) => {
                let normalized = normalize_whitespace(command.trim());
                let compact = if normalized.len() > 48 {
                    normalized[..48].to_string()
                } else {
                    normalized
                };
                sanitize_id_component(&compact, 48)
            }
        }
    }
}

impl EmbeddingProvider {
    fn cache_key_component(&self) -> String {
        match self {
            Self::Disabled => "disabled".to_string(),
            Self::HashVector { dimensions } => format!("hash{dimensions}"),
            Self::AdapterCommand(command) => {
                let normalized = normalize_whitespace(command.trim());
                let compact = if normalized.len() > 48 {
                    normalized[..48].to_string()
                } else {
                    normalized
                };
                sanitize_id_component(&compact, 48)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        ffi::{OsStr, OsString},
        sync::{Mutex, OnceLock},
    };

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[allow(unused_unsafe)]
    fn set_env_var_for_tests(key: &str, value: &OsStr) {
        unsafe {
            std::env::set_var(key, value);
        }
    }

    #[allow(unused_unsafe)]
    fn restore_env_var_for_tests(key: &str, value: Option<&OsStr>) {
        match value {
            Some(value) => unsafe {
                std::env::set_var(key, value);
            },
            None => unsafe {
                std::env::remove_var(key);
            },
        }
    }

    struct EnvVarGuard {
        key: &'static str,
        previous: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &OsStr) -> Self {
            let previous = std::env::var_os(key);
            set_env_var_for_tests(key, value);
            Self { key, previous }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            restore_env_var_for_tests(self.key, self.previous.as_deref());
        }
    }

    fn sample_request() -> IngestRawApiRequest {
        IngestRawApiRequest {
            tenant_id: "tenant-a".into(),
            document_id: "doc-100".into(),
            source_id: "source://doc-100".into(),
            text: "Company X acquired Company Y in 2024. Revenue rose by 20% in Q4.".into(),
            extraction_model: Some("rule-sentence-v1".into()),
            claim_confidence: Some(0.88),
            source_quality: Some(0.92),
            min_sentence_chars: Some(10),
            max_claims: Some(8),
            generate_embeddings: None,
            embedding_model: None,
        }
    }

    fn sample_document_request() -> IngestDocumentApiRequest {
        IngestDocumentApiRequest {
            tenant_id: "tenant-a".into(),
            document_id: "doc-200".into(),
            source_id: "source://doc-200".into(),
            mime_type: "text/plain".into(),
            text: Some("Company X acquired Company Y in 2024. Revenue rose by 20% in Q4.".into()),
            content_base64: None,
            extraction_model: Some("rule-sentence-v1".into()),
            claim_confidence: Some(0.88),
            source_quality: Some(0.92),
            min_sentence_chars: Some(10),
            max_claims: Some(8),
            generate_embeddings: None,
            embedding_model: None,
        }
    }

    #[test]
    fn build_ingest_batch_from_raw_request_extracts_sentence_claims() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("rule_sentence"),
        );
        let batch = build_ingest_batch_from_raw_request(&sample_request()).unwrap();
        assert_eq!(
            batch.commit_id.as_deref(),
            Some("raw:tenant-a:doc-100:prule_sentence:edisabled:m10:k8")
        );
        assert_eq!(batch.items.len(), 2);
        assert_eq!(batch.items[0].claim.tenant_id, "tenant-a");
        assert_eq!(
            batch.items[0].claim.canonical_text,
            "Company X acquired Company Y in 2024"
        );
        assert_eq!(batch.items[0].evidence.len(), 1);
        assert_eq!(batch.items[0].evidence[0].source_id, "source://doc-100");
        assert_eq!(
            batch.items[0].evidence[0].extraction_model.as_deref(),
            Some("rule-sentence-v1")
        );
    }

    #[test]
    fn build_ingest_batch_from_raw_request_rejects_when_nothing_extracts() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("rule_sentence"),
        );
        let mut req = sample_request();
        req.text = "short. tiny.".into();
        req.min_sentence_chars = Some(20);
        let err = build_ingest_batch_from_raw_request(&req).unwrap_err();
        assert!(err.contains("did not yield any claim-sized sentences"));
    }

    #[test]
    fn build_ingest_batch_from_document_request_accepts_inline_text() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("rule_sentence"),
        );
        let result = build_ingest_batch_from_document_request(&sample_document_request()).unwrap();
        assert_eq!(result.parser_provider, "inline_text");
        assert_eq!(result.mime_type, "text/plain");
        assert_eq!(result.batch.items.len(), 2);
    }

    #[test]
    fn build_ingest_batch_from_document_request_decodes_base64_payload() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("rule_sentence"),
        );
        let mut req = sample_document_request();
        req.text = None;
        req.content_base64 =
            Some("Q29tcGFueSBYIGFjcXVpcmVkIENvbXBhbnkgWS4gUmV2ZW51ZSByb3NlLg==".into());
        let result = build_ingest_batch_from_document_request(&req).unwrap();
        assert_eq!(result.parser_provider, "builtin_utf8");
        assert_eq!(result.batch.items.len(), 2);
    }

    #[test]
    fn build_ingest_batch_from_document_request_rejects_binary_without_adapter() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("rule_sentence"),
        );
        let mut req = sample_document_request();
        req.text = None;
        req.mime_type = "application/pdf".into();
        req.content_base64 = Some("JVBERi0xLjQK".into());
        let err = build_ingest_batch_from_document_request(&req).unwrap_err();
        assert!(err.contains("requires document parser adapter"));
    }

    #[test]
    fn build_ingest_batch_from_document_request_uses_adapter_for_pdf_bytes() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("rule_sentence"),
        );
        let _parser = EnvVarGuard::set(
            "DASH_INGEST_DOCUMENT_PARSER_PROVIDER",
            OsStr::new("adapter_command"),
        );
        let _cmd = EnvVarGuard::set("DASH_INGEST_DOCUMENT_ADAPTER_CMD", OsStr::new("cat"));
        let mut req = sample_document_request();
        req.text = None;
        req.mime_type = "application/pdf".into();
        req.content_base64 =
            Some("Q29tcGFueSBYIGFjcXVpcmVkIENvbXBhbnkgWS4gUmV2ZW51ZSByb3NlIGluIFE0Lg==".into());
        let result = build_ingest_batch_from_document_request(&req).unwrap();
        assert_eq!(result.parser_provider, "cat");
        assert_eq!(result.batch.items.len(), 2);
    }

    #[test]
    fn build_ingest_batch_from_raw_request_generates_hash_embeddings_when_enabled() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("rule_sentence"),
        );
        let _embedding_provider =
            EnvVarGuard::set("DASH_INGEST_EMBEDDING_PROVIDER", OsStr::new("hash_vector"));
        let _embedding_dims =
            EnvVarGuard::set("DASH_INGEST_EMBEDDING_DIMENSIONS", OsStr::new("16"));
        let mut request = sample_request();
        request.generate_embeddings = Some(true);
        request.embedding_model = Some("emb://hash-v1".into());
        let output = build_ingest_raw_output_from_request(&request).unwrap();
        assert_eq!(output.embedding_provider, "hash16");
        assert_eq!(output.embeddings_generated, 2);
        assert_eq!(output.embedding_dimensions, Some(16));
        assert_eq!(
            output.batch.commit_id.as_deref(),
            Some("raw:tenant-a:doc-100:prule_sentence:ehash16:m10:k8")
        );
        assert_eq!(
            output.batch.items[0].claim.embedding_ids,
            vec!["emb://hash-v1".to_string()]
        );
        assert_eq!(
            output.batch.items[0].claim_embedding.as_ref().map(Vec::len),
            Some(16)
        );
    }

    #[test]
    fn build_ingest_batch_from_document_request_adapter_requires_command() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("rule_sentence"),
        );
        let _parser = EnvVarGuard::set(
            "DASH_INGEST_DOCUMENT_PARSER_PROVIDER",
            OsStr::new("adapter_command"),
        );
        let previous_cmd = std::env::var_os("DASH_INGEST_DOCUMENT_ADAPTER_CMD");
        restore_env_var_for_tests("DASH_INGEST_DOCUMENT_ADAPTER_CMD", None);
        let mut req = sample_document_request();
        req.text = None;
        req.mime_type = "application/pdf".into();
        req.content_base64 = Some("JVBERi0xLjQK".into());
        let err = build_ingest_batch_from_document_request(&req).unwrap_err();
        assert!(err.contains("DASH_INGEST_DOCUMENT_ADAPTER_CMD"));
        restore_env_var_for_tests("DASH_INGEST_DOCUMENT_ADAPTER_CMD", previous_cmd.as_deref());
    }

    #[test]
    fn build_ingest_batch_from_document_request_rejects_invalid_base64() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("rule_sentence"),
        );
        let mut req = sample_document_request();
        req.text = None;
        req.content_base64 = Some("%%%invalid%%%".into());
        let err = build_ingest_batch_from_document_request(&req).unwrap_err();
        assert!(err.contains("content_base64 contains invalid character"));
    }

    #[test]
    fn extract_sentences_respects_max_claims() {
        let out = extract_sentences(
            "a very long claim text one. another long claim text two.",
            5,
            1,
        );
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn sanitize_id_component_normalizes_untrusted_text() {
        let out = sanitize_id_component("Doc ID / weird\tvalue", 32);
        assert_eq!(out, "doc-id---weird-value");
    }

    #[test]
    fn resolve_extraction_provider_rejects_unknown_provider() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("unknown-provider"),
        );
        let err = resolve_extraction_provider().unwrap_err();
        assert!(err.contains("unsupported extraction provider"));
    }

    #[test]
    #[cfg(not(feature = "model-extraction-adapter"))]
    fn build_ingest_batch_from_raw_request_adapter_provider_fails_without_feature() {
        let _guard = env_lock().lock().expect("env lock should be available");
        let _provider = EnvVarGuard::set(
            "DASH_INGEST_RAW_EXTRACTION_PROVIDER",
            OsStr::new("adapter_command"),
        );
        let _cmd = EnvVarGuard::set("DASH_INGEST_RAW_ADAPTER_CMD", OsStr::new("cat"));
        let err = build_ingest_batch_from_raw_request(&sample_request()).unwrap_err();
        assert!(err.contains("model-extraction-adapter"));
    }
}
