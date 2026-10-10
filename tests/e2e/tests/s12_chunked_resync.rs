//! Scenario 12: replication across checkpoints with a data set larger than
//! one export chunk.
//!
//! * A fresh retrieval follower of a leader that has checkpointed rebuilds
//!   its state from the leader's export downloaded in chunks
//!   (`/internal/replication/export/begin` + `/chunk`), each far smaller
//!   than the data set, and reaches exactly the leader's data.
//! * A follower that keeps up crosses later checkpoints without any resync:
//!   it finishes each closed generation from the leader's retained file and
//!   switches to the new generation.
//! * A follower killed and restarted resumes without a resync.

use std::collections::BTreeMap;
use std::time::Duration;

use dash_e2e::*;

const T: &str = "tenant-a";
const ALL: &str = "zzqx-matches-nothing";
const CHECKPOINT_EVERY: usize = 40;
const CHUNK_BYTES: usize = 4096;

struct Oracle {
    next: usize,
    expected: BTreeMap<String, Vec<String>>,
    checkpoints: usize,
}

impl Oracle {
    fn new() -> Self {
        Oracle {
            next: 0,
            expected: BTreeMap::new(),
            checkpoints: 0,
        }
    }

    /// One bundle with three evidence rows; `true` when it triggered a
    /// checkpoint.
    fn write(&mut self, s: &Stack) -> bool {
        let i = self.next;
        self.next += 1;
        let claim = format!("cr{i:04}");
        let r = s.ingest_as(
            T,
            &bundle(
                T,
                &claim,
                &format!("chunked resync scenario claim number {i} with some text to make it longer"),
                3,
            ),
        );
        assert_eq!(r.status, 200, "ingest {claim}: {}", r.body);
        let mut ev: Vec<String> = (0..3).map(|k| format!("{claim}-e{k}")).collect();
        ev.sort();
        self.expected.insert(claim, ev);
        let triggered = r.json()["checkpoint_triggered"] == true;
        if triggered {
            self.checkpoints += 1;
        }
        triggered
    }

    fn verify(&self, s: &Stack, what: &str) {
        let api: BTreeMap<String, Vec<String>> = s.claim_evidence_map(T, ALL, 5000);
        assert_eq!(
            api, self.expected,
            "{what}: retrieval differs from what the leader acknowledged"
        );
    }
}

fn metric(s: &Stack, name: &str) -> u64 {
    let r = s
        .rc()
        .get("/metrics", &[("x-api-key", s.ops_key.as_str())]);
    assert_eq!(r.status, 200, "retrieval metrics: {}", r.body);
    r.body
        .lines()
        .find_map(|l| l.strip_prefix(&format!("{name} ")))
        .unwrap_or_else(|| panic!("metric {name} missing"))
        .trim()
        .parse()
        .expect("numeric metric")
}

fn replication_u64(s: &Stack, key: &str) -> u64 {
    s.retrieval_replication()[key]
        .as_u64()
        .unwrap_or_else(|| panic!("replication.{key} missing"))
}

fn stack() -> Stack {
    Stack::new(StackOpts {
        checkpoint_every: Some(CHECKPOINT_EVERY),
        extra_retrieval_env: vec![
            ("DASH_RETRIEVAL_MAX_TOP_K".into(), "5000".into()),
            (
                "DASH_RETRIEVAL_REPLICATION_EXPORT_CHUNK_BYTES".into(),
                CHUNK_BYTES.to_string(),
            ),
        ],
        ..Default::default()
    })
}

#[test]
fn fresh_follower_resyncs_a_data_set_larger_than_one_chunk() {
    let mut s = stack();
    s.start_ingest();
    let mut o = Oracle::new();
    // Enough data that the export is many chunks, with a checkpoint (so a
    // snapshot exists and a fresh follower must resync) and a WAL tail.
    while o.next < 80 || o.checkpoints < 2 {
        o.write(&s);
    }
    o.write(&s);
    s.start_retrieval();
    s.wait_caught_up(Duration::from_secs(60));
    o.verify(&s, "after the chunked resync");
    assert_eq!(replication_u64(&s, "resyncs_total"), 1);
    let downloaded = metric(&s, "dash_retrieval_replication_export_bytes_total");
    assert!(
        downloaded > 8 * CHUNK_BYTES as u64,
        "the export ({downloaded} bytes) took many {CHUNK_BYTES}-byte chunks"
    );

    // A restart (kill -9) resumes from the follower's own WAL: no resync.
    s.restart_retrieval(false);
    o.write(&s);
    s.wait_caught_up(Duration::from_secs(30));
    o.verify(&s, "after a follower restart");
    assert_eq!(replication_u64(&s, "resyncs_total"), 0, "resync after restart");
}

#[test]
fn a_follower_that_keeps_up_crosses_checkpoints_without_resync() {
    let mut s = stack();
    s.start_all();
    s.wait_retrieval_ready(Duration::from_secs(30));
    let mut o = Oracle::new();
    for round in 1..=3 {
        while !o.write(&s) {}
        // A couple of writes in the new generation, then catch up.
        o.write(&s);
        s.wait_caught_up(Duration::from_secs(30));
        o.verify(&s, &format!("after checkpoint {round}"));
        assert_eq!(
            replication_u64(&s, "resyncs_total"),
            0,
            "checkpoint {round} caused a resync: {}",
            s.retrieval_replication()
        );
        assert_eq!(
            replication_u64(&s, "generation_switches_total"),
            round,
            "{}",
            s.retrieval_replication()
        );
    }
    assert_eq!(o.checkpoints, 3);
}
