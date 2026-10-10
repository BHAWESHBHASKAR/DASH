//! Ingestion audit events. Chaining, canonical encoding, locking and torn-tail
//! recovery live in `dash_common::audit` (shared with retrieval and the
//! `audit-verify` tool); this module only adapts the ingestion call sites.

use std::time::{SystemTime, UNIX_EPOCH};

use dash_common::audit::{AuditInput, AuditOptions, append_record};

use super::SharedRuntime;

#[derive(Debug, Clone, Copy)]
pub(super) struct AuditEvent<'a> {
    pub(super) action: &'a str,
    pub(super) tenant_id: Option<&'a str>,
    pub(super) claim_id: Option<&'a str>,
    pub(super) status: u16,
    pub(super) outcome: &'a str,
    pub(super) reason: &'a str,
}

pub(super) fn audit_options() -> AuditOptions {
    AuditOptions::from_env("INGEST")
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
        if audit_options().fail_closed {
            // The mutation (if any) is already committed on this path; the
            // pre-mutation gate is `audit_gate`. Make the gap loud.
            eprintln!(
                "ingestion audit FAIL_CLOSED violation: action={} completed without an audit record",
                event.action
            );
        }
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
    append_record(
        path,
        &AuditInput {
            service: "ingestion",
            action: event.action,
            tenant_id: event.tenant_id,
            claim_id: event.claim_id,
            status: event.status,
            outcome: event.outcome,
            reason: event.reason,
        },
        timestamp_ms,
        &audit_options(),
    )
}

/// Pre-mutation gate for `DASH_INGEST_AUDIT_FAIL_CLOSED=1`: `Err` means the
/// audit log is currently unusable and the request must fail with 503 before
/// anything is committed.
pub(super) fn audit_gate(audit_log_path: Option<&str>) -> Result<(), String> {
    match audit_log_path {
        Some(path) if audit_options().fail_closed => dash_common::audit::preflight(path),
        _ => Ok(()),
    }
}

/// Chain state is re-read from the file under a file lock on every append, so
/// there is no process-local cache to clear. Kept for existing tests.
#[cfg(test)]
pub(super) fn clear_cached_audit_chain_state(_path: &str) {}

#[cfg(test)]
pub(super) use dash_common::audit::is_sha256_hex;

#[cfg(test)]
mod tests {
    use super::*;
    use dash_common::audit::{VerifyOptions, verify_file};

    fn temp_path(tag: &str) -> String {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "dash-ingest-audit-{tag}-{}-{nanos}.jsonl",
                std::process::id()
            ))
            .to_string_lossy()
            .to_string()
    }

    fn event(action: &'static str) -> AuditEvent<'static> {
        AuditEvent {
            action,
            tenant_id: Some("tenant-a"),
            claim_id: Some("c-1"),
            status: 200,
            outcome: "success",
            reason: "ok \"quoted\" \u{1}",
        }
    }

    /// Regression: ingestion hashed a sorted-key payload that the verifier
    /// could never reproduce; real ingestion records must now verify.
    #[test]
    fn real_ingestion_records_verify_with_shared_verifier() {
        let path = temp_path("verify");
        for i in 0..3u64 {
            append_audit_record(&path, &event("ingest"), 1_700_000_000_000 + i).expect("append");
        }
        let report = verify_file(
            &path,
            &VerifyOptions {
                service: Some("ingestion".into()),
                ..Default::default()
            },
        )
        .expect("ingestion log must verify");
        assert_eq!(report.chained_records, 3);
        assert_eq!(report.v2_records, 3);
        let _ = std::fs::remove_file(path);
    }

    /// Regression: records written by the pre-v2 ingestion code (serde_json
    /// sorted-key payload) are still accepted by the verifier.
    #[test]
    fn legacy_ingestion_records_still_verify() {
        use serde_json::json;
        let path = temp_path("legacy");
        let mut prev = dash_common::audit::GENESIS_HASH.to_string();
        let mut out = String::new();
        for seq in 1..=2u64 {
            let canonical = json!({
                "seq": seq, "ts_unix_ms": 5u64, "service": "ingestion", "action": "ingest",
                "tenant_id": "t", "claim_id": null, "status": 200, "outcome": "success",
                "reason": "ok", "prev_hash": prev,
            })
            .to_string();
            let hash = auth::sha256_hex(canonical.as_bytes());
            out.push_str(
                &json!({
                    "seq": seq, "ts_unix_ms": 5u64, "service": "ingestion", "action": "ingest",
                    "tenant_id": "t", "claim_id": null, "status": 200, "outcome": "success",
                    "reason": "ok", "prev_hash": prev, "hash": hash,
                })
                .to_string(),
            );
            out.push('\n');
            prev = hash;
        }
        std::fs::write(&path, out).expect("write legacy log");
        let report = verify_file(&path, &VerifyOptions::default()).expect("legacy verifies");
        assert_eq!(report.legacy_records, 2);
        // New writer continues a legacy chain seamlessly.
        append_audit_record(&path, &event("ingest"), 9).expect("append");
        let report = verify_file(&path, &VerifyOptions::default()).expect("mixed verifies");
        assert_eq!(
            (report.legacy_records, report.v2_records, report.last_seq),
            (2, 1, 3)
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn append_unwritable_path_counts_failure_and_gate_reports_it() {
        let dir = temp_path("unwritable");
        std::fs::create_dir_all(&dir).expect("dir");
        // A directory cannot be opened as an audit file.
        let before = dash_common::audit::write_failures_total();
        assert!(append_audit_record(&dir, &event("ingest"), 1).is_err());
        assert!(dash_common::audit::write_failures_total() > before);
        assert!(dash_common::audit::preflight(&dir).is_err());
        let _ = std::fs::remove_dir(dir);
    }
}
