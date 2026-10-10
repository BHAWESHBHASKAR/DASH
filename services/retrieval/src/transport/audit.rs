//! Retrieval audit events. Chaining, canonical encoding, locking and torn-tail
//! recovery live in `dash_common::audit` (shared with ingestion and the
//! `audit-verify` tool); this module only adapts the retrieval call sites.

use dash_common::audit::{AuditInput, AuditOptions, append_record};

#[derive(Debug, Clone, Copy)]
pub(super) struct AuditEvent<'a> {
    pub(super) action: &'a str,
    pub(super) tenant_id: Option<&'a str>,
    pub(super) status: u16,
    pub(super) outcome: &'a str,
    pub(super) reason: &'a str,
}

pub(super) fn audit_options() -> AuditOptions {
    AuditOptions::from_env("RETRIEVAL")
}

pub(super) fn append_audit_record(
    path: &str,
    timestamp_ms: u64,
    event: AuditEvent<'_>,
) -> Result<(), String> {
    let result = append_record(
        path,
        &AuditInput {
            service: "retrieval",
            action: event.action,
            tenant_id: event.tenant_id,
            claim_id: None,
            status: event.status,
            outcome: event.outcome,
            reason: event.reason,
        },
        timestamp_ms,
        &audit_options(),
    );
    if result.is_err() && audit_options().fail_closed {
        eprintln!(
            "retrieval audit FAIL_CLOSED violation: action={} completed without an audit record",
            event.action
        );
    }
    result
}

/// Pre-work gate for `DASH_RETRIEVAL_AUDIT_FAIL_CLOSED=1`: `Err` means the
/// audit log is currently unusable and the request must fail with 503.
pub(super) fn audit_gate(audit_log_path: Option<&str>) -> Result<(), String> {
    match audit_log_path {
        Some(path) if audit_options().fail_closed => dash_common::audit::preflight(path),
        _ => Ok(()),
    }
}

#[cfg(test)]
pub(super) use dash_common::audit::is_sha256_hex;

#[cfg(test)]
mod tests {
    use super::*;
    use dash_common::audit::{VerifyOptions, verify_file};

    fn temp_path(tag: &str) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "dash-retrieve-audit-{tag}-{}-{nanos}.jsonl",
                std::process::id()
            ))
            .to_string_lossy()
            .to_string()
    }

    fn event() -> AuditEvent<'static> {
        AuditEvent {
            action: "retrieve",
            tenant_id: Some("tenant-a"),
            status: 200,
            outcome: "success",
            reason: "ok\ttab \"q\"",
        }
    }

    #[test]
    fn real_retrieval_records_verify_with_shared_verifier() {
        let path = temp_path("verify");
        for i in 0..3u64 {
            append_audit_record(&path, 1_700_000_000_000 + i, event()).expect("append");
        }
        let report = verify_file(
            &path,
            &VerifyOptions {
                service: Some("retrieval".into()),
                ..Default::default()
            },
        )
        .expect("retrieval log must verify");
        assert_eq!(report.chained_records, 3);
        let _ = std::fs::remove_file(path);
    }

    /// Records written by the pre-v2 retrieval writer (insertion-order
    /// `format!` payload) still verify, and the new writer continues them.
    #[test]
    fn legacy_retrieval_records_still_verify() {
        let path = temp_path("legacy");
        let mut prev = dash_common::audit::GENESIS_HASH.to_string();
        let mut out = String::new();
        for seq in 1..=2u64 {
            let canonical = format!(
                "{{\"seq\":{seq},\"ts_unix_ms\":7,\"service\":\"retrieval\",\"action\":\"retrieve\",\"tenant_id\":\"t\",\"claim_id\":null,\"status\":200,\"outcome\":\"success\",\"reason\":\"a\\tb\",\"prev_hash\":\"{prev}\"}}"
            );
            let hash = auth::sha256_hex(canonical.as_bytes());
            out.push_str(&format!(
                "{{\"seq\":{seq},\"ts_unix_ms\":7,\"service\":\"retrieval\",\"action\":\"retrieve\",\"tenant_id\":\"t\",\"claim_id\":null,\"status\":200,\"outcome\":\"success\",\"reason\":\"a\\tb\",\"prev_hash\":\"{prev}\",\"hash\":\"{hash}\"}}\n"
            ));
            prev = hash;
        }
        std::fs::write(&path, out).expect("write");
        append_audit_record(&path, 8, event()).expect("append");
        let report = verify_file(&path, &VerifyOptions::default()).expect("verifies");
        assert_eq!((report.legacy_records, report.v2_records), (2, 1));
        let _ = std::fs::remove_file(path);
    }
}
