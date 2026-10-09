//! Audit chain regression tests: tamper detection, torn-tail recovery,
//! explicit chain restarts, concurrent writers and actor context.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dash_common::audit::{
    AuditContext, AuditInput, AuditOptions, CHAIN_RESTART_ACTION, GENESIS_HASH, VerifyOptions,
    append_record, context_from_headers, enter_context, verify_bytes, verify_file,
};

static COUNTER: AtomicU64 = AtomicU64::new(0);

fn temp_log(tag: &str) -> (tempfile::TempDir, String) {
    let dir = tempfile::tempdir().expect("tempdir");
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let path: PathBuf = dir.path().join(format!("{tag}-{n}.jsonl"));
    (dir, path.to_string_lossy().to_string())
}

fn input(service: &'static str, i: u64) -> (AuditInput<'static>, u64) {
    (
        AuditInput {
            service,
            action: "ingest",
            tenant_id: Some("tenant-a"),
            claim_id: if service == "ingestion" {
                Some("c-1")
            } else {
                None
            },
            status: 200,
            outcome: "success",
            reason: "ok",
        },
        1_700_000_000_000 + i,
    )
}

fn write_n(path: &str, service: &'static str, n: u64) {
    let opts = AuditOptions {
        fsync: false,
        fail_closed: false,
    };
    for i in 0..n {
        let (inp, ts) = input(service, i);
        append_record(path, &inp, ts, &opts).expect("append");
    }
}

fn lines(path: &str) -> Vec<String> {
    std::fs::read_to_string(path)
        .expect("read")
        .lines()
        .map(str::to_string)
        .collect()
}

fn write_lines(path: &str, lines: &[String]) {
    let mut out = lines.join("\n");
    out.push('\n');
    std::fs::write(path, out).expect("write");
}

fn verify(path: &str) -> Result<dash_common::audit::VerifyReport, dash_common::audit::VerifyError> {
    verify_file(path, &VerifyOptions::default())
}

#[test]
fn both_services_verify() {
    for service in ["ingestion", "retrieval"] {
        let (_d, path) = temp_log(service);
        write_n(&path, service, 5);
        let report = verify_file(
            &path,
            &VerifyOptions {
                service: Some(service.to_string()),
                ..Default::default()
            },
        )
        .expect("verifies");
        assert_eq!(report.chained_records, 5);
        assert_eq!(report.last_seq, 5);
    }
}

#[test]
fn service_filter_rejects_other_service() {
    let (_d, path) = temp_log("filter");
    write_n(&path, "retrieval", 2);
    let err = verify_file(
        &path,
        &VerifyOptions {
            service: Some("ingestion".into()),
            ..Default::default()
        },
    )
    .expect_err("filter mismatch");
    assert!(err.message.contains("service filter"));
}

#[test]
fn tamper_modified_record_detected() {
    let (_d, path) = temp_log("modify");
    write_n(&path, "ingestion", 4);
    let mut l = lines(&path);
    l[1] = l[1].replace("\"outcome\":\"success\"", "\"outcome\":\"denied\"");
    write_lines(&path, &l);
    let err = verify(&path).expect_err("modified");
    assert_eq!(err.line, 2);
    assert!(err.message.contains("hash mismatch"));
}

#[test]
fn tamper_unhashed_extra_field_rejected() {
    let (_d, path) = temp_log("extra");
    write_n(&path, "ingestion", 2);
    let mut l = lines(&path);
    l[0] = l[0].replacen("{\"v\":2,", "{\"v\":2,\"note\":\"x\",", 1);
    write_lines(&path, &l);
    assert!(
        verify(&path)
            .expect_err("extra field")
            .message
            .contains("unknown field")
    );
}

#[test]
fn tamper_deleted_middle_record_detected() {
    let (_d, path) = temp_log("delete");
    write_n(&path, "ingestion", 5);
    let mut l = lines(&path);
    l.remove(2);
    write_lines(&path, &l);
    let err = verify(&path).expect_err("gap");
    assert_eq!(err.line, 3);
    assert!(err.message.contains("seq expected 3"));
}

#[test]
fn tamper_reordered_records_detected() {
    let (_d, path) = temp_log("reorder");
    write_n(&path, "retrieval", 4);
    let mut l = lines(&path);
    l.swap(1, 2);
    write_lines(&path, &l);
    assert!(verify(&path).is_err());
}

#[test]
fn tail_truncation_detected_only_with_checkpoint() {
    let (_d, path) = temp_log("truncate");
    write_n(&path, "ingestion", 5);
    let checkpoint = verify(&path).expect("full log");
    let mut l = lines(&path);
    l.truncate(3);
    write_lines(&path, &l);

    // Without a checkpoint note the chain alone cannot see the loss.
    let report = verify(&path).expect("truncated chain is self-consistent");
    assert_eq!(report.last_seq, 3);

    // With the out-of-band note it is flagged.
    let err = verify_file(
        &path,
        &VerifyOptions {
            expect_tail: Some((checkpoint.last_seq, checkpoint.last_hash.clone())),
            ..Default::default()
        },
    )
    .expect_err("checkpoint mismatch");
    assert!(err.message.contains("checkpoint"));
}

/// Regression: a last line without seq/hash (`{}`) used to restart the chain
/// at genesis without a trace.
#[test]
fn empty_object_tail_forces_explicit_chain_restart() {
    let (_d, path) = temp_log("restart");
    write_n(&path, "ingestion", 3);
    let before = verify(&path).expect("ok");
    let mut text = std::fs::read_to_string(&path).expect("read");
    text.push_str("{}\n");
    std::fs::write(&path, text).expect("write");
    // As-is the verifier flags the corrupt tail.
    assert!(verify(&path).is_err());

    write_n(&path, "ingestion", 1);
    let l = lines(&path);
    assert!(
        l[4].contains(CHAIN_RESTART_ACTION),
        "restart record: {}",
        l[4]
    );
    assert!(l[4].contains(&before.last_hash));
    let report = verify(&path).expect("restart is explained");
    assert_eq!(report.restarts, 1);
    assert_eq!(report.quarantined_lines, 1);
    assert_eq!(report.chained_records, 5);

    // Chain continues normally afterwards.
    write_n(&path, "ingestion", 2);
    assert_eq!(verify(&path).expect("continues").last_seq, 4);
}

#[test]
fn unexplained_genesis_restart_is_flagged() {
    let (_d, a) = temp_log("a");
    let (_d2, b) = temp_log("b");
    write_n(&a, "ingestion", 2);
    write_n(&b, "ingestion", 2);
    // Splice a fresh chain after an existing one with no chain_restart event.
    let mut l = lines(&a);
    l.extend(lines(&b));
    write_lines(&a, &l);
    let err = verify(&a).expect_err("silent restart");
    assert!(err.message.contains("unexplained chain restart"));
}

#[test]
fn restart_with_wrong_tail_reference_is_flagged() {
    let (_d, path) = temp_log("badref");
    write_n(&path, "ingestion", 3);
    let mut text = std::fs::read_to_string(&path).expect("read");
    text.push_str("{}\n");
    std::fs::write(&path, text).expect("write");
    write_n(&path, "ingestion", 1);
    // Drop an earlier record so the restart no longer points at the tail.
    let mut l = lines(&path);
    l.remove(1);
    write_lines(&path, &l);
    assert!(verify(&path).is_err());
}

/// Regression: a torn last line (crash mid-write) used to make every later
/// append fail, disabling auditing permanently.
#[test]
fn torn_tail_is_truncated_and_auditing_continues() {
    let (_d, path) = temp_log("torn");
    write_n(&path, "ingestion", 3);
    let good = std::fs::read_to_string(&path).expect("read");
    let mut torn = good.clone();
    torn.push_str("{\"v\":2,\"seq\":4,\"ts_unix_ms\":17000000000");
    std::fs::write(&path, &torn).expect("write");
    assert!(verify(&path).is_err(), "verifier flags torn tail");

    write_n(&path, "ingestion", 1);
    let report = verify(&path).expect("recovered");
    assert_eq!(report.last_seq, 4);
    assert_eq!(report.chained_records, 4);
    assert_eq!(report.restarts, 0);
}

#[test]
fn complete_record_missing_only_newline_is_kept() {
    let (_d, path) = temp_log("nonl");
    write_n(&path, "retrieval", 2);
    let mut text = std::fs::read_to_string(&path).expect("read");
    text.pop();
    std::fs::write(&path, text).expect("write");
    write_n(&path, "retrieval", 1);
    assert_eq!(verify(&path).expect("ok").chained_records, 3);
}

#[test]
fn torn_only_line_restarts_at_genesis_cleanly() {
    let (_d, path) = temp_log("tornonly");
    std::fs::write(&path, "{\"v\":2,\"seq\":1,\"ts").expect("write");
    write_n(&path, "ingestion", 1);
    let l = lines(&path);
    assert_eq!(l.len(), 1);
    assert!(l[0].contains(GENESIS_HASH));
    verify(&path).expect("ok");
}

/// Two writers with independent file handles (as two processes would have)
/// must not fork the chain.
#[test]
fn concurrent_writers_do_not_fork_chain() {
    let (_d, path) = temp_log("concurrent");
    let path = Arc::new(path);
    let mut handles = Vec::new();
    for t in 0..4u64 {
        let path = Arc::clone(&path);
        handles.push(std::thread::spawn(move || {
            let opts = AuditOptions {
                fsync: false,
                fail_closed: false,
            };
            for i in 0..50 {
                let (inp, ts) = input("ingestion", i + t * 1000);
                append_record(&path, &inp, ts, &opts).expect("append");
            }
        }));
    }
    for h in handles {
        h.join().expect("join");
    }
    let report = verify(&path).expect("single linear chain");
    assert_eq!(report.chained_records, 200);
    assert_eq!(report.last_seq, 200);
}

#[test]
fn actor_context_is_recorded_without_the_secret() {
    let (_d, path) = temp_log("actor");
    let mut headers = HashMap::new();
    headers.insert(
        "x-api-key".to_string(),
        "super-secret-key-value".to_string(),
    );
    headers.insert("x-request-id".to_string(), "req-123".to_string());
    {
        let _g = enter_context(context_from_headers(&headers));
        write_n(&path, "ingestion", 1);
    }
    write_n(&path, "ingestion", 1);
    let text = std::fs::read_to_string(&path).expect("read");
    assert!(!text.contains("super-secret-key-value"));
    let l = lines(&path);
    assert!(l[0].contains("\"actor\":{\"kind\":\"api_key\",\"id\":\""));
    assert!(l[0].contains("\"request_id\":\"req-123\""));
    // Guard restored the empty context.
    assert!(l[1].contains("\"actor\":null"));
    verify(&path).expect("verifies");
}

#[test]
fn context_kinds() {
    let mut h = HashMap::new();
    assert_eq!(context_from_headers(&h).actor.expect("actor").kind, "none");
    h.insert("authorization".into(), "Bearer aaa.bbb.ccc".into());
    assert_eq!(context_from_headers(&h).actor.expect("actor").kind, "jwt");
    h.insert("authorization".into(), "Bearer plainkey".into());
    assert_eq!(
        context_from_headers(&h).actor.expect("actor").kind,
        "api_key"
    );
    let _ = AuditContext::default();
}

#[test]
fn verify_bytes_rejects_empty_and_non_chained() {
    assert!(verify_bytes(b"", &VerifyOptions::default()).is_err());
    assert!(verify_bytes(b"{}\n", &VerifyOptions::default()).is_err());
}
