use std::{
    collections::HashMap,
    fs::{OpenOptions, create_dir_all},
    io::{BufRead, BufReader, Write},
    path::Path,
    sync::{Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};

use auth::sha256_hex;

use super::{JsonValue, SharedRuntime, json_escape, parse_json};

#[derive(Debug, Clone, Copy)]
pub(super) struct AuditEvent<'a> {
    pub(super) action: &'a str,
    pub(super) tenant_id: Option<&'a str>,
    pub(super) claim_id: Option<&'a str>,
    pub(super) status: u16,
    pub(super) outcome: &'a str,
    pub(super) reason: &'a str,
}

const AUDIT_CHAIN_GENESIS_HASH: &str =
    "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Clone, PartialEq, Eq)]
struct AuditChainState {
    next_seq: u64,
    last_hash: String,
}

pub(super) fn emit_audit_event(
    runtime: &SharedRuntime,
    audit_log_path: Option<&str>,
    event: AuditEvent<'_>,
) {
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or_default();

    let mut write_error = false;
    if let Some(path) = audit_log_path
        && let Err(err) = append_audit_record(path, &event, timestamp_ms)
    {
        write_error = true;
        eprintln!("ingestion audit write failed: {err}");
    }

    if let Ok(mut guard) = runtime.lock() {
        guard.observe_audit_event();
        if write_error {
            guard.observe_audit_write_error();
        }
    }
}

pub(super) fn append_audit_record(
    path: &str,
    event: &AuditEvent<'_>,
    timestamp_ms: u64,
) -> Result<(), String> {
    if let Some(parent) = Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        create_dir_all(parent).map_err(|e| format!("creating audit directory failed: {e}"))?;
    }
    let mut chain_states = audit_chain_states()
        .lock()
        .map_err(|_| "acquiring audit chain lock failed".to_string())?;
    let state = if let Some(existing) = chain_states.get(path).cloned() {
        existing
    } else {
        let loaded = load_audit_chain_state(path)?;
        chain_states.insert(path.to_string(), loaded.clone());
        loaded
    };
    let (payload, next_state) = render_chained_audit_payload(event, timestamp_ms, &state);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("opening audit file failed: {e}"))?;
    writeln!(file, "{payload}").map_err(|e| format!("appending audit file failed: {e}"))?;
    chain_states.insert(path.to_string(), next_state);
    Ok(())
}

fn render_chained_audit_payload(
    event: &AuditEvent<'_>,
    timestamp_ms: u64,
    state: &AuditChainState,
) -> (String, AuditChainState) {
    let seq = state.next_seq;
    let prev_hash = state.last_hash.as_str();
    let canonical = canonical_audit_payload(seq, timestamp_ms, event, prev_hash);
    let hash = sha256_hex(canonical.as_bytes());
    let payload = format!(
        "{{\"seq\":{seq},\"ts_unix_ms\":{timestamp_ms},\"service\":\"ingestion\",\"action\":\"{}\",\"tenant_id\":{},\"claim_id\":{},\"status\":{},\"outcome\":\"{}\",\"reason\":\"{}\",\"prev_hash\":\"{}\",\"hash\":\"{}\"}}",
        json_escape(event.action),
        optional_json_string(event.tenant_id),
        optional_json_string(event.claim_id),
        event.status,
        json_escape(event.outcome),
        json_escape(event.reason),
        prev_hash,
        hash,
    );
    (
        payload,
        AuditChainState {
            next_seq: seq.saturating_add(1),
            last_hash: hash,
        },
    )
}

fn canonical_audit_payload(
    seq: u64,
    timestamp_ms: u64,
    event: &AuditEvent<'_>,
    prev_hash: &str,
) -> String {
    format!(
        "{{\"seq\":{seq},\"ts_unix_ms\":{timestamp_ms},\"service\":\"ingestion\",\"action\":\"{}\",\"tenant_id\":{},\"claim_id\":{},\"status\":{},\"outcome\":\"{}\",\"reason\":\"{}\",\"prev_hash\":\"{}\"}}",
        json_escape(event.action),
        optional_json_string(event.tenant_id),
        optional_json_string(event.claim_id),
        event.status,
        json_escape(event.outcome),
        json_escape(event.reason),
        prev_hash,
    )
}

fn optional_json_string(value: Option<&str>) -> String {
    value
        .map(|raw| format!("\"{}\"", json_escape(raw)))
        .unwrap_or_else(|| "null".to_string())
}

fn audit_chain_states() -> &'static Mutex<HashMap<String, AuditChainState>> {
    static STATES: OnceLock<Mutex<HashMap<String, AuditChainState>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(HashMap::new()))
}

#[cfg(test)]
pub(super) fn clear_cached_audit_chain_state(path: &str) {
    if let Ok(mut states) = audit_chain_states().lock() {
        states.remove(path);
    }
}

fn load_audit_chain_state(path: &str) -> Result<AuditChainState, String> {
    if !Path::new(path).exists() {
        return Ok(AuditChainState {
            next_seq: 1,
            last_hash: AUDIT_CHAIN_GENESIS_HASH.to_string(),
        });
    }
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|e| format!("opening audit file failed: {e}"))?;
    let reader = BufReader::new(file);
    let mut last_line: Option<String> = None;
    for line in reader.lines() {
        let line = line.map_err(|e| format!("reading audit file failed: {e}"))?;
        if line.trim().is_empty() {
            continue;
        }
        last_line = Some(line);
    }

    let Some(last_line) = last_line else {
        return Ok(AuditChainState {
            next_seq: 1,
            last_hash: AUDIT_CHAIN_GENESIS_HASH.to_string(),
        });
    };
    match parse_audit_chain_state_from_line(&last_line)? {
        Some(state) => Ok(state),
        None => Ok(AuditChainState {
            next_seq: 1,
            last_hash: AUDIT_CHAIN_GENESIS_HASH.to_string(),
        }),
    }
}

fn parse_audit_chain_state_from_line(line: &str) -> Result<Option<AuditChainState>, String> {
    let value = parse_json(line)?;
    let object = match value {
        JsonValue::Object(object) => object,
        _ => return Ok(None),
    };

    let seq_value = object.get("seq");
    let hash_value = object.get("hash");
    if seq_value.is_none() && hash_value.is_none() {
        return Ok(None);
    }
    let seq = match seq_value {
        Some(JsonValue::Number(raw)) => raw
            .parse::<u64>()
            .map_err(|_| "audit seq must be u64".to_string())?,
        _ => return Err("audit seq is missing or invalid".to_string()),
    };
    let hash = match hash_value {
        Some(JsonValue::String(raw)) if is_sha256_hex(raw) => raw.clone(),
        _ => return Err("audit hash is missing or invalid".to_string()),
    };

    Ok(Some(AuditChainState {
        next_seq: seq.saturating_add(1),
        last_hash: hash,
    }))
}

pub(super) fn is_sha256_hex(raw: &str) -> bool {
    raw.len() == 64 && raw.chars().all(|ch| ch.is_ascii_hexdigit())
}
