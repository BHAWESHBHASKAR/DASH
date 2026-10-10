//! Scenario 13: encryption at rest (ADR 0005, SEC-16), black box.
//!
//! The plaintext canary: a unique marker string goes in through the real
//! ingestion service with `DASH_ENCRYPTION_KEY_FILE` set, comes back out
//! through the real retrieval follower, survives checkpoints, a chunked
//! export / resync, segment publishing, vector index saves and restarts;
//! then every file under the deployment directory (WAL, snapshot, closed
//! generations, exports, part files, redb mirrors, vector indexes, segments,
//! audit logs and process logs) is scanned and the marker must not appear
//! in plaintext anywhere.
//!
//! Then fail closed: the same data directory without a key refuses to start.

use std::path::{Path, PathBuf};
use std::time::Duration;

use dash_e2e::*;
use serde_json::{Value, json};

const T: &str = "tenant-a";
const CHECKPOINT_EVERY: usize = 25;

fn key_file(dir: &Path) -> PathBuf {
    let path = dir.join("dash-kek.key");
    let mut hex = String::new();
    for _ in 0..4 {
        hex.push_str(&random_secret());
    }
    std::fs::write(&path, &hex[..64]).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    path
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(path) = stack.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path).unwrap() {
                stack.push(entry.unwrap().path());
            }
        } else {
            out.push(path);
        }
    }
    out.sort();
    out
}

fn name(path: &Path) -> String {
    path.file_name().unwrap().to_string_lossy().to_string()
}

fn starts_with(path: &Path, prefix: &[u8]) -> bool {
    std::fs::read(path).is_ok_and(|b| b.starts_with(prefix))
}

const LINE_HEADER: &[u8] = b"~DASHENC1 ";
const SEALED: &[u8] = b"DASHSEAL";

fn marker_bundle(marker: &str, i: usize) -> Value {
    let claim_id = format!("canary-{i:03}");
    json!({
        "claim": {
            "claim_id": claim_id,
            "tenant_id": T,
            "canonical_text": format!("the {marker} statement number {i} about encrypted storage"),
            "confidence": 0.9,
            "entities": [format!("{marker}-entity")],
        },
        "evidence": [{
            "evidence_id": format!("{claim_id}-e0"),
            "claim_id": claim_id,
            "source_id": format!("src://{marker}/{i}"),
            "stance": "supports",
            "source_quality": 0.9,
            "chunk_id": format!("{marker}-chunk-{i}"),
        }],
        "edges": [],
    })
}

fn assert_marker_retrievable(s: &Stack, marker: &str, what: &str) {
    let results = s.retrieve(T, marker, 50);
    assert!(
        !results.is_empty(),
        "{what}: retrieval found nothing for the marker\n{}",
        s.retrieval_log()
    );
    let text = serde_json::to_string(&results).unwrap();
    assert!(
        text.contains(marker),
        "{what}: results lack the marker: {text}"
    );
}

#[test]
fn plaintext_canary_never_reaches_the_disk_and_a_missing_key_fails_closed() {
    let keys = tempfile::tempdir().unwrap();
    let key = key_file(keys.path());
    let marker = format!("dashcanary{}", &random_secret()[..16]);

    let mut s = Stack::new(StackOpts {
        checkpoint_every: Some(CHECKPOINT_EVERY),
        ..Default::default()
    });
    let segments = s.path("segments");
    let enc = (
        "DASH_ENCRYPTION_KEY_FILE".to_string(),
        key.display().to_string(),
    );
    s.opts.extra_ingest_env = vec![
        enc.clone(),
        (
            "DASH_INGEST_SEGMENT_DIR".into(),
            segments.display().to_string(),
        ),
        (
            "DASH_INGEST_AUDIT_LOG_PATH".into(),
            s.path("ingest-audit.log").display().to_string(),
        ),
    ];
    s.opts.extra_retrieval_env = vec![
        enc,
        (
            "DASH_RETRIEVAL_SEGMENT_DIR".into(),
            segments.display().to_string(),
        ),
        (
            "DASH_RETRIEVAL_AUDIT_LOG_PATH".into(),
            s.path("retrieval-audit.log").display().to_string(),
        ),
        // Small chunks: the resync below downloads the export in pieces.
        (
            "DASH_RETRIEVAL_REPLICATION_EXPORT_CHUNK_BYTES".into(),
            "4096".into(),
        ),
    ];

    // Ingest through the real service until the leader has checkpointed
    // twice (snapshot, closed generation, new WAL).
    s.start_ingest();
    assert!(
        s.ingest_log().contains("encryption at rest: on"),
        "{}",
        s.ingest_log()
    );
    let mut checkpoints = 0;
    let mut i = 0;
    while checkpoints < 2 {
        let r = s.ingest_as(T, &marker_bundle(&marker, i));
        assert_eq!(r.status, 200, "ingest {i}: {}", r.body);
        if r.json()["checkpoint_triggered"] == true {
            checkpoints += 1;
        }
        i += 1;
        assert!(i < 400, "the leader never checkpointed");
    }
    for extra in 0..5 {
        let r = s.ingest_as(T, &marker_bundle(&marker, i + extra));
        assert_eq!(r.status, 200, "{}", r.body);
    }

    // A fresh follower behind a checkpointed leader rebuilds from the
    // leader's export (sealed on the leader, plaintext on the wire, its
    // part file and WAL encrypted on the follower).
    s.start_retrieval();
    s.wait_caught_up(Duration::from_secs(60));
    assert_marker_retrievable(&s, &marker, "after the resync");
    let replication = s.retrieval_replication();
    assert!(
        replication["resyncs_total"].as_u64().unwrap_or(0) >= 1,
        "the follower should have resynced from an export: {replication}"
    );

    // Cold start of both services with encryption on (vector indexes are
    // saved on shutdown and loaded on start).
    s.restart_ingest(true);
    s.restart_retrieval(true);
    s.wait_caught_up(Duration::from_secs(60));
    assert_marker_retrievable(&s, &marker, "after restarts");
    let r = s.ingest_as(T, &marker_bundle(&marker, 999));
    assert_eq!(r.status, 200, "{}", r.body);
    s.wait_caught_up(Duration::from_secs(30));

    // Stop both so every file is final, then scan everything.
    for p in [s.ingest.take(), s.retrieval.take()].into_iter().flatten() {
        let mut p = p;
        assert!(p.terminate(Duration::from_secs(15)), "{}", p.log());
    }
    let files = files_under(s.dir.path());
    let mut leaks = Vec::new();
    for path in &files {
        let bytes = std::fs::read(path).unwrap();
        if bytes.windows(marker.len()).any(|w| w == marker.as_bytes()) {
            leaks.push(path.display().to_string());
        }
    }
    assert!(
        leaks.is_empty(),
        "the marker appears in plaintext in: {leaks:?}"
    );

    // Every kind of data file exists and is encrypted.
    let find = |pred: &dyn Fn(&str) -> bool| -> Vec<&PathBuf> {
        files.iter().filter(|p| pred(&name(p))).collect()
    };
    for (what, pred, header) in [
        (
            "leader WAL",
            Box::new(|n: &str| n == "ingest.wal") as Box<dyn Fn(&str) -> bool>,
            LINE_HEADER,
        ),
        (
            "leader snapshot",
            Box::new(|n: &str| n == "ingest.wal.snapshot"),
            LINE_HEADER,
        ),
        (
            "leader closed generation",
            Box::new(|n: &str| n.starts_with("ingest.wal.closed.")),
            LINE_HEADER,
        ),
        (
            "follower WAL",
            Box::new(|n: &str| n == "retrieval.wal"),
            LINE_HEADER,
        ),
        (
            "leader export",
            Box::new(|n: &str| n.ends_with(".export")),
            SEALED,
        ),
        (
            "vector index",
            Box::new(|n: &str| n.ends_with(".vindex")),
            SEALED,
        ),
        (
            "segment file",
            Box::new(|n: &str| n.ends_with(".seg")),
            SEALED,
        ),
        (
            "segment manifest",
            Box::new(|n: &str| n == "segments.manifest"),
            SEALED,
        ),
    ] {
        let found = find(&*pred);
        assert!(
            !found.is_empty(),
            "no {what} under {}",
            s.dir.path().display()
        );
        for path in found {
            assert!(
                starts_with(path, header),
                "{what} {} is not encrypted",
                path.display()
            );
        }
    }
    for redb in ["ingest.redb", "retrieval.redb"] {
        assert!(s.path(redb).exists(), "{redb} missing");
    }
    println!(
        "canary: scanned {} files, marker absent; {} checkpoints",
        files.len(),
        checkpoints
    );

    // Fail closed: the same directory without the key refuses to start,
    // naming the problem.
    let env: Vec<(String, String)> = s
        .ingest_env()
        .into_iter()
        .filter(|(k, _)| k != "DASH_ENCRYPTION_KEY_FILE")
        .collect();
    let mut p = Proc::spawn(
        "ingestion",
        "ingestion",
        &[],
        &env,
        &s.path("ingestion-nokey.log"),
    );
    let status = p
        .wait_exit(Duration::from_secs(20))
        .expect("ingestion without the key must exit");
    assert_eq!(status.code(), Some(2), "{}", p.log());
    assert!(
        p.log().contains("no encryption key is configured"),
        "{}",
        p.log()
    );
}
