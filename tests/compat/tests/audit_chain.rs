//! Audit log compatibility (services/common/src/audit.rs): the chains every
//! release wrote verify with the current verifier, and the current writer
//! continues an old chain (next `seq`, `prev_hash` = old tail) so the file
//! verifies end to end across the upgrade.

use dash_common::audit::{AuditInput, AuditOptions, VerifyOptions, append_record, verify_file};
use dash_compat::{Era, FIXTURES};

#[test]
fn old_audit_chains_verify_and_the_current_writer_continues_them() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        for (file, service) in [
            ("audit-ingestion.jsonl", "ingestion"),
            ("audit-retrieval.jsonl", "retrieval"),
        ] {
            let path = state.path().join(file);
            let path_str = path.to_str().expect("utf-8 path");
            let opts = VerifyOptions {
                service: Some(service.to_string()),
                expect_tail: None,
            };
            let before = verify_file(path_str, &opts)
                .unwrap_or_else(|e| panic!("{}: {file}: {e:?}", fixture.label));
            assert!(before.chained_records > 0, "{}: {file}", fixture.label);
            match fixture.era {
                Era::V0_2 => {
                    assert_eq!(before.v2_records, 0, "{}: {file}", fixture.label);
                    assert_eq!(
                        before.legacy_records, before.chained_records,
                        "{}",
                        fixture.label
                    );
                }
                Era::V0_3 => {
                    assert_eq!(before.legacy_records, 0, "{}: {file}", fixture.label);
                }
            }
            append_record(
                path_str,
                &AuditInput {
                    service,
                    action: "compat_upgrade_check",
                    tenant_id: Some("tenant-a"),
                    claim_id: None,
                    status: 200,
                    outcome: "success",
                    reason: "written by the upgraded release",
                },
                1_800_000_000_000,
                &AuditOptions::default(),
            )
            .expect("append to the old chain");
            let after = verify_file(path_str, &opts)
                .unwrap_or_else(|e| panic!("{}: {file} after append: {e:?}", fixture.label));
            assert_eq!(
                after.last_seq,
                before.last_seq + 1,
                "{}: {file}",
                fixture.label
            );
            assert_eq!(
                after.chained_records,
                before.chained_records + 1,
                "{}",
                fixture.label
            );
            assert_eq!(after.v2_records, before.v2_records + 1, "{}", fixture.label);
            assert_eq!(
                after.restarts, before.restarts,
                "{}: no chain restart",
                fixture.label
            );
            let last_line = std::fs::read_to_string(&path)
                .expect("read")
                .lines()
                .last()
                .expect("line")
                .to_string();
            let record: serde_json::Value = serde_json::from_str(&last_line).expect("json");
            assert_eq!(
                record["prev_hash"],
                before.last_hash.as_str(),
                "{}",
                fixture.label
            );
        }
    }
}

/// What a 0.2 ingestion node appends (copied from
/// `services/ingestion/src/transport/audit.rs` at `main` ae86667): it takes
/// `seq` and `hash` from the last line and hashes a `serde_json::json!`
/// payload, whose keys serde_json sorts alphabetically.
fn v0_2_ingestion_append(path: &std::path::Path, action: &str, ts: u64) {
    let text = std::fs::read_to_string(path).expect("read");
    let last: serde_json::Value =
        serde_json::from_str(text.lines().last().expect("tail")).expect("json");
    let seq = last["seq"].as_u64().expect("seq") + 1;
    let prev_hash = last["hash"].as_str().expect("hash").to_string();
    let canonical = format!(
        "{{\"action\":{},\"claim_id\":null,\"outcome\":\"success\",\"prev_hash\":\"{prev_hash}\",\
         \"reason\":\"ingest accepted\",\"seq\":{seq},\"service\":\"ingestion\",\"status\":200,\
         \"tenant_id\":\"tenant-a\",\"ts_unix_ms\":{ts}}}",
        serde_json::to_string(action).expect("string")
    );
    let hash = auth::sha256_hex(canonical.as_bytes());
    let mut record: serde_json::Value = serde_json::from_str(&canonical).expect("json");
    record["hash"] = serde_json::Value::String(hash);
    let mut out = text;
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out.push_str(&record.to_string());
    out.push('\n');
    std::fs::write(path, out).expect("write");
}

/// Rollback to 0.2 keeps the chain verifiable: 0.2 continues from the last
/// record's `seq` and `hash` (whatever its version), and the current
/// verifier accepts legacy records after version 2 records. (The 0.2 shell
/// verifier could not reproduce ingestion hashes at all; verify with
/// `audit-verify` from 0.3.)
#[test]
fn a_chain_continued_by_0_2_after_a_rollback_still_verifies() {
    let fixture = FIXTURES
        .iter()
        .find(|f| f.era == Era::V0_2)
        .expect("0.2 fixture");
    let state = fixture.scratch_state();
    let path = state.path().join("audit-ingestion.jsonl");
    let path_str = path.to_str().expect("utf-8");
    append_record(
        path_str,
        &AuditInput {
            service: "ingestion",
            action: "ingest",
            tenant_id: Some("tenant-a"),
            claim_id: Some("a-c01"),
            status: 200,
            outcome: "success",
            reason: "ingest accepted",
        },
        1_800_000_000_000,
        &AuditOptions::default(),
    )
    .expect("append v2");
    v0_2_ingestion_append(&path, "ingest", 1_800_000_000_500);
    let report = verify_file(path_str, &VerifyOptions::default()).expect("chain verifies");
    assert_eq!(report.v2_records, 1);
    assert_eq!(report.restarts, 0);
    assert_eq!(report.legacy_records, report.chained_records - 1);
}
