use std::collections::HashMap;

use schema::{Claim, ClaimEdge, Evidence, Relation, Stance};

use crate::api::{
    IngestApiRequest, IngestApiResponse, IngestBatchApiRequest, IngestBatchApiResponse,
};

use super::json::{JsonValue, json_escape, parse_json};

pub(super) fn build_ingest_request_from_json(body: &str) -> Result<IngestApiRequest, String> {
    let value = parse_json(body)?;
    if !matches!(value, JsonValue::Object(_)) {
        return Err("request body must be a JSON object".to_string());
    }
    build_ingest_request_from_json_value(&value)
}

pub(super) fn build_ingest_batch_request_from_json(
    body: &str,
    max_items: usize,
) -> Result<IngestBatchApiRequest, String> {
    let value = parse_json(body)?;
    let object = match value {
        JsonValue::Object(map) => map,
        _ => return Err("request body must be a JSON object".to_string()),
    };
    let items = match object.get("items") {
        Some(JsonValue::Array(items)) => items,
        Some(_) => return Err("items must be an array".to_string()),
        None => return Err("items is required".to_string()),
    };
    if items.is_empty() {
        return Err("items must not be empty".to_string());
    }
    if items.len() > max_items {
        return Err(format!(
            "items length {} exceeds max batch size {}",
            items.len(),
            max_items
        ));
    }
    let commit_id = parse_optional_string(object.get("commit_id"), "commit_id")?
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    let mut parsed = Vec::with_capacity(items.len());
    let mut expected_tenant: Option<String> = None;
    for item in items {
        let req = build_ingest_request_from_json_value(item)?;
        if let Some(tenant_id) = expected_tenant.as_deref() {
            if tenant_id != req.claim.tenant_id {
                return Err("all batch items must share the same claim.tenant_id".to_string());
            }
        } else {
            expected_tenant = Some(req.claim.tenant_id.clone());
        }
        parsed.push(req);
    }

    Ok(IngestBatchApiRequest {
        commit_id,
        items: parsed,
    })
}

pub(super) fn render_ingest_response_json(resp: &IngestApiResponse) -> String {
    let checkpoint_snapshot_records = resp
        .checkpoint_snapshot_records
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());
    let checkpoint_truncated_wal_records = resp
        .checkpoint_truncated_wal_records
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());
    format!(
        "{{\"ingested_claim_id\":\"{}\",\"claims_total\":{},\"checkpoint_triggered\":{},\"checkpoint_snapshot_records\":{},\"checkpoint_truncated_wal_records\":{}}}",
        json_escape(&resp.ingested_claim_id),
        resp.claims_total,
        resp.checkpoint_triggered,
        checkpoint_snapshot_records,
        checkpoint_truncated_wal_records
    )
}

pub(super) fn render_ingest_batch_response_json(resp: &IngestBatchApiResponse) -> String {
    let checkpoint_snapshot_records = resp
        .checkpoint_snapshot_records
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());
    let checkpoint_truncated_wal_records = resp
        .checkpoint_truncated_wal_records
        .map(|v| v.to_string())
        .unwrap_or_else(|| "null".to_string());
    let ingested_claim_ids = resp
        .ingested_claim_ids
        .iter()
        .map(|value| format!("\"{}\"", json_escape(value)))
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "{{\"commit_id\":\"{}\",\"idempotent_replay\":{},\"batch_size\":{},\"ingested_claim_ids\":[{}],\"claims_total\":{},\"checkpoint_triggered\":{},\"checkpoint_snapshot_records\":{},\"checkpoint_truncated_wal_records\":{}}}",
        json_escape(&resp.commit_id),
        resp.idempotent_replay,
        resp.batch_size,
        ingested_claim_ids,
        resp.claims_total,
        resp.checkpoint_triggered,
        checkpoint_snapshot_records,
        checkpoint_truncated_wal_records
    )
}

fn build_ingest_request_from_json_value(value: &JsonValue) -> Result<IngestApiRequest, String> {
    let object = match value {
        JsonValue::Object(map) => map,
        _ => return Err("ingest item must be a JSON object".to_string()),
    };

    let claim_obj = require_object(object, "claim")?;
    let claim = Claim {
        claim_id: require_string(&claim_obj, "claim_id")?,
        tenant_id: require_string(&claim_obj, "tenant_id")?,
        canonical_text: require_string(&claim_obj, "canonical_text")?,
        confidence: parse_f32(
            match claim_obj.get("confidence") {
                Some(JsonValue::Number(raw)) => raw,
                _ => return Err("claim.confidence must be a number".to_string()),
            },
            "claim.confidence",
        )?,
        event_time_unix: parse_optional_i64(
            claim_obj.get("event_time_unix"),
            "claim.event_time_unix",
        )?,
        entities: parse_optional_string_array(claim_obj.get("entities"), "claim.entities")?,
        embedding_ids: parse_optional_string_array(
            claim_obj.get("embedding_ids"),
            "claim.embedding_ids",
        )?,
        claim_type: None,
        valid_from: parse_optional_i64(claim_obj.get("valid_from"), "claim.valid_from")?,
        valid_to: parse_optional_i64(claim_obj.get("valid_to"), "claim.valid_to")?,
        created_at: None,
        updated_at: None,
    };

    let evidence = match object.get("evidence") {
        Some(JsonValue::Array(items)) => parse_evidence_array(items)?,
        Some(_) => return Err("evidence must be an array".to_string()),
        None => Vec::new(),
    };
    let edges = match object.get("edges") {
        Some(JsonValue::Array(items)) => parse_edges_array(items)?,
        Some(_) => return Err("edges must be an array".to_string()),
        None => Vec::new(),
    };

    Ok(IngestApiRequest {
        claim,
        claim_embedding: parse_optional_f32_array(
            claim_obj.get("embedding_vector"),
            "claim.embedding_vector",
        )?,
        evidence,
        edges,
    })
}

fn parse_optional_f32_array(
    value: Option<&JsonValue>,
    field: &str,
) -> Result<Option<Vec<f32>>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                let raw = match item {
                    JsonValue::Number(raw) => raw,
                    _ => return Err(format!("{field} must be an array of numbers")),
                };
                let parsed = parse_f32(raw, field)?;
                if !parsed.is_finite() {
                    return Err(format!("{field} values must be finite numbers"));
                }
                out.push(parsed);
            }
            if out.is_empty() {
                return Err(format!("{field} must not be empty when provided"));
            }
            Ok(Some(out))
        }
        Some(_) => Err(format!("{field} must be an array or null")),
    }
}

fn parse_evidence_array(items: &[JsonValue]) -> Result<Vec<Evidence>, String> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let obj = match item {
            JsonValue::Object(map) => map,
            _ => return Err("evidence items must be objects".to_string()),
        };
        let stance = match require_string(obj, "stance")?.as_str() {
            "supports" => Stance::Supports,
            "contradicts" => Stance::Contradicts,
            "neutral" => Stance::Neutral,
            _ => {
                return Err("evidence.stance must be supports, contradicts, or neutral".to_string());
            }
        };

        out.push(Evidence {
            evidence_id: require_string(obj, "evidence_id")?,
            claim_id: require_string(obj, "claim_id")?,
            source_id: require_string(obj, "source_id")?,
            stance,
            source_quality: parse_f32(
                match obj.get("source_quality") {
                    Some(JsonValue::Number(raw)) => raw,
                    _ => return Err("evidence.source_quality must be a number".to_string()),
                },
                "evidence.source_quality",
            )?,
            chunk_id: parse_optional_string(obj.get("chunk_id"), "evidence.chunk_id")?,
            span_start: parse_optional_u32(obj.get("span_start"), "evidence.span_start")?,
            span_end: parse_optional_u32(obj.get("span_end"), "evidence.span_end")?,
            doc_id: parse_optional_string(obj.get("doc_id"), "evidence.doc_id")?,
            extraction_model: parse_optional_string(
                obj.get("extraction_model"),
                "evidence.extraction_model",
            )?,
            ingested_at: None,
        });
    }
    Ok(out)
}

fn parse_edges_array(items: &[JsonValue]) -> Result<Vec<ClaimEdge>, String> {
    let mut out = Vec::with_capacity(items.len());
    for item in items {
        let obj = match item {
            JsonValue::Object(map) => map,
            _ => return Err("edges items must be objects".to_string()),
        };
        let relation = match require_string(obj, "relation")?.as_str() {
            "supports" => Relation::Supports,
            "contradicts" => Relation::Contradicts,
            "refines" => Relation::Refines,
            "duplicates" => Relation::Duplicates,
            "depends_on" => Relation::DependsOn,
            _ => {
                return Err(
                    "edge.relation must be supports, contradicts, refines, duplicates, or depends_on"
                        .to_string(),
                );
            }
        };

        out.push(ClaimEdge {
            edge_id: require_string(obj, "edge_id")?,
            from_claim_id: require_string(obj, "from_claim_id")?,
            to_claim_id: require_string(obj, "to_claim_id")?,
            relation,
            strength: parse_f32(
                match obj.get("strength") {
                    Some(JsonValue::Number(raw)) => raw,
                    _ => return Err("edge.strength must be a number".to_string()),
                },
                "edge.strength",
            )?,
            reason_codes: parse_optional_string_array(
                obj.get("reason_codes"),
                "edge.reason_codes",
            )?,
            created_at: None,
        });
    }
    Ok(out)
}

fn require_object(
    map: &HashMap<String, JsonValue>,
    key: &str,
) -> Result<HashMap<String, JsonValue>, String> {
    match map.get(key) {
        Some(JsonValue::Object(value)) => Ok(value.clone()),
        Some(_) => Err(format!("{key} must be an object")),
        None => Err(format!("{key} is required")),
    }
}

fn require_string(map: &HashMap<String, JsonValue>, key: &str) -> Result<String, String> {
    match map.get(key) {
        Some(JsonValue::String(value)) => Ok(value.clone()),
        Some(_) => Err(format!("{key} must be a string")),
        None => Err(format!("{key} is required")),
    }
}

fn parse_f32(raw: &str, field_name: &str) -> Result<f32, String> {
    raw.parse::<f32>()
        .map_err(|_| format!("{field_name} must be a valid number"))
}

fn parse_optional_i64(value: Option<&JsonValue>, field_name: &str) -> Result<Option<i64>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(raw)) => raw
            .parse::<i64>()
            .map(Some)
            .map_err(|_| format!("{field_name} must be an i64 timestamp")),
        _ => Err(format!("{field_name} must be an i64 timestamp or null")),
    }
}

fn parse_optional_u32(value: Option<&JsonValue>, field_name: &str) -> Result<Option<u32>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::Number(raw)) => raw
            .parse::<u32>()
            .map(Some)
            .map_err(|_| format!("{field_name} must be a u32 offset or null")),
        _ => Err(format!("{field_name} must be a u32 offset or null")),
    }
}

fn parse_optional_string(
    value: Option<&JsonValue>,
    field_name: &str,
) -> Result<Option<String>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(None),
        Some(JsonValue::String(raw)) => Ok(Some(raw.clone())),
        _ => Err(format!("{field_name} must be a string or null")),
    }
}

fn parse_optional_string_array(
    value: Option<&JsonValue>,
    field_name: &str,
) -> Result<Vec<String>, String> {
    match value {
        None | Some(JsonValue::Null) => Ok(Vec::new()),
        Some(JsonValue::Array(items)) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    JsonValue::String(value) => out.push(value.clone()),
                    _ => return Err(format!("{field_name} items must be strings")),
                }
            }
            Ok(out)
        }
        _ => Err(format!("{field_name} must be an array of strings or null")),
    }
}
