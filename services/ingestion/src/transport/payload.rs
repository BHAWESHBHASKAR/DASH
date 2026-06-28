use crate::api::{
    IngestApiRequest, IngestApiRequestWire, IngestApiResponse, IngestBatchApiRequest,
    IngestBatchApiRequestWire, IngestBatchApiResponse, IngestDocumentApiRequest,
    IngestDocumentApiResponse, IngestRawApiRequest, IngestRawApiResponse,
};

pub(super) fn build_ingest_request_from_json(body: &str) -> Result<IngestApiRequest, String> {
    let wire: IngestApiRequestWire = serde_json::from_str(body).map_err(|err| err.to_string())?;
    wire.into_runtime()
}

pub(super) fn build_ingest_batch_request_from_json(
    body: &str,
    max_items: usize,
) -> Result<IngestBatchApiRequest, String> {
    let wire: IngestBatchApiRequestWire =
        serde_json::from_str(body).map_err(|err| err.to_string())?;
    wire.into_runtime(max_items)
}

pub(super) fn build_ingest_raw_request_from_json(
    body: &str,
) -> Result<IngestRawApiRequest, String> {
    let mut req: IngestRawApiRequest = serde_json::from_str(body).map_err(|err| err.to_string())?;
    validate_raw_request_ranges(&mut req)?;
    normalize_optional_string(&mut req.extraction_model);
    normalize_optional_string(&mut req.embedding_model);
    Ok(req)
}

pub(super) fn build_ingest_document_request_from_json(
    body: &str,
) -> Result<IngestDocumentApiRequest, String> {
    let mut req: IngestDocumentApiRequest =
        serde_json::from_str(body).map_err(|err| err.to_string())?;
    validate_document_request_ranges(&mut req)?;
    validate_document_request_content(&req)?;
    normalize_optional_string(&mut req.extraction_model);
    normalize_optional_string(&mut req.embedding_model);
    Ok(req)
}

pub(super) fn render_ingest_response_json(resp: &IngestApiResponse) -> String {
    serde_json::to_string(resp).expect("IngestApiResponse is always serializable")
}

pub(super) fn render_ingest_batch_response_json(resp: &IngestBatchApiResponse) -> String {
    serde_json::to_string(resp).expect("IngestBatchApiResponse is always serializable")
}

pub(super) fn render_ingest_raw_response_json(resp: &IngestRawApiResponse) -> String {
    serde_json::to_string(resp).expect("IngestRawApiResponse is always serializable")
}

pub(super) fn render_ingest_document_response_json(resp: &IngestDocumentApiResponse) -> String {
    serde_json::to_string(resp).expect("IngestDocumentApiResponse is always serializable")
}

fn validate_raw_request_ranges(req: &mut IngestRawApiRequest) -> Result<(), String> {
    if let Some(value) = req.claim_confidence
        && !(0.0..=1.0).contains(&value)
    {
        return Err("claim_confidence must be in [0, 1]".to_string());
    }
    if let Some(value) = req.source_quality
        && !(0.0..=1.0).contains(&value)
    {
        return Err("source_quality must be in [0, 1]".to_string());
    }
    if req.min_sentence_chars == Some(0) {
        return Err("min_sentence_chars must be >= 1".to_string());
    }
    if req.max_claims == Some(0) {
        return Err("max_claims must be >= 1".to_string());
    }
    Ok(())
}

fn validate_document_request_ranges(req: &mut IngestDocumentApiRequest) -> Result<(), String> {
    if let Some(value) = req.claim_confidence
        && !(0.0..=1.0).contains(&value)
    {
        return Err("claim_confidence must be in [0, 1]".to_string());
    }
    if let Some(value) = req.source_quality
        && !(0.0..=1.0).contains(&value)
    {
        return Err("source_quality must be in [0, 1]".to_string());
    }
    if req.min_sentence_chars == Some(0) {
        return Err("min_sentence_chars must be >= 1".to_string());
    }
    if req.max_claims == Some(0) {
        return Err("max_claims must be >= 1".to_string());
    }
    Ok(())
}

fn validate_document_request_content(req: &IngestDocumentApiRequest) -> Result<(), String> {
    let text = req
        .text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let content = req
        .content_base64
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    if text.is_none() && content.is_none() {
        return Err("either text or content_base64 is required".to_string());
    }
    Ok(())
}

fn normalize_optional_string(field: &mut Option<String>) {
    if let Some(value) = field.as_mut() {
        *value = value.trim().to_string();
        if value.is_empty() {
            *field = None;
        }
    }
}
