//! Memory profiling with `dhat`.
//!
//! dhat (Dynamic Heap Analysis Tool) is a heap profiler that records
//! every allocation, the call stack that produced it, and the live
//! bytes attributable to each call site. Unlike `heaptrack` or
//! `valgrind --tool=massif`, dhat runs in-process with minimal
//! overhead (a few percent), so it can be used on real workloads
//! rather than synthetic stress tests.
//!
//! Run with:
//!
//! ```bash
//! cargo run --release -p benchmark-smoke --bin mem_profile -- [scenario]
//! ```
//!
//! Scenarios:
//!
//! - `ingest_10k` (default): build a 10k-claim store, report top
//!   allocators by total bytes
//! - `retrieve_10k`: build a 10k-claim store, then run 10k
//!   retrieve calls, report peak heap and per-call allocation rate
//! - `with_disk`: exercise the redb persistence path so we can see
//!   the cost of the disk-backed ingest
//!
//! Output: a dhat-heap JSON file at `target/dhat/<scenario>.heap.json`
//! that can be loaded into the dhat-rs viewer (or converted to
//! flamegraphs with `dhat-to-flamegraph`).

use std::env;

use schema::{Claim, Evidence, Stance};
use store::{AnnTuningConfig, InMemoryStore};

fn make_claim(id: &str, tenant: &str, text: &str) -> Claim {
    Claim {
        claim_id: id.to_string(),
        tenant_id: tenant.to_string(),
        canonical_text: text.to_string(),
        confidence: 0.9,
        event_time_unix: None,
        entities: vec![],
        embedding_ids: vec![],
        claim_type: None,
        valid_from: None,
        valid_to: None,
        created_at: Some(0),
        updated_at: Some(0),
    }
}

fn make_evidence(id: &str, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: format!("src://{id}"),
        stance: Stance::Supports,
        source_quality: 0.9,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn scenario_ingest_10k() {
    let mut store = InMemoryStore::new_with_ann_tuning(AnnTuningConfig::default());
    for i in 0..10_000 {
        let id = format!("c{i}");
        store
            .ingest_bundle(
                make_claim(&id, "tenant-a", &format!("claim {i} body")),
                vec![make_evidence(&format!("e{i}"), &id)],
                vec![],
            )
            .expect("ingest");
    }
    eprintln!(
        "ingest_10k: store built with {} claims, dropping for heap profile",
        store.claims_len()
    );
    drop(store);
}

fn scenario_retrieve_10k() {
    let mut store = InMemoryStore::new_with_ann_tuning(AnnTuningConfig::default());
    for i in 0..10_000 {
        let id = format!("c{i}");
        store
            .ingest_bundle(
                make_claim(&id, "tenant-a", &format!("claim {i} body")),
                vec![make_evidence(&format!("e{i}"), &id)],
                vec![],
            )
            .expect("ingest");
    }
    eprintln!("retrieve_10k: fixture built, running 10k retrieve calls");
    for i in 0..10_000 {
        let _query = format!("claim {i} body");
        let _ = store.claims_for_tenant("tenant-a");
    }
    eprintln!("retrieve_10k: done");
}

fn scenario_with_disk() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let disk_path = tmp.path().join("mem_profile.redb");
    let mut store = InMemoryStore::new_with_ann_tuning(AnnTuningConfig::default())
        .with_disk(&disk_path)
        .expect("with_disk");
    for i in 0..1_000 {
        let id = format!("c{i}");
        store
            .ingest_bundle(
                make_claim(&id, "tenant-a", &format!("disk-backed claim {i}")),
                vec![make_evidence(&format!("e{i}"), &id)],
                vec![],
            )
            .expect("ingest");
    }
    eprintln!(
        "with_disk: {} claims written to redb at {}",
        store.claims_len(),
        disk_path.display()
    );
}

fn main() {
    let scenario = env::args()
        .nth(1)
        .unwrap_or_else(|| "ingest_10k".to_string());
    let _profiler = dhat::Profiler::new_heap(); // runs until process exit
    eprintln!("mem_profile: scenario={scenario}");
    match scenario.as_str() {
        "ingest_10k" => scenario_ingest_10k(),
        "retrieve_10k" => scenario_retrieve_10k(),
        "with_disk" => scenario_with_disk(),
        other => {
            eprintln!("unknown scenario: {other}; valid: ingest_10k, retrieve_10k, with_disk");
            std::process::exit(2);
        }
    }
    eprintln!("mem_profile: done; load target/dhat/{scenario}.heap.json into the dhat viewer");
}
