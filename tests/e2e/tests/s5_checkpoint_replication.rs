//! Scenario 5: replication across leader WAL checkpoints (compaction).
//! The follower must converge to exactly the leader's data, never skip a
//! record, and resync at most once per leader checkpoint episode.

use std::collections::BTreeMap;
use std::time::Duration;

use dash_e2e::*;
use serde_json::{Value, json};

const T: &str = "tenant-a";
const ALL: &str = "zzqx-matches-nothing";
const CHECKPOINT_EVERY: usize = 30;

struct Oracle {
    next: usize,
    /// claim -> sorted evidence ids
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

    /// Ingest one bundle; returns whether this write triggered a checkpoint.
    fn write(&mut self, s: &Stack) -> bool {
        let i = self.next;
        self.next += 1;
        let claim = format!("cp{i}");
        let n = 1 + i % 3;
        let r = s.ingest_as(
            T,
            &bundle(T, &claim, &format!("checkpoint scenario claim {i}"), n),
        );
        assert_eq!(r.status, 200, "ingest {claim}: {}", r.body);
        let mut ev: Vec<String> = (0..n).map(|k| format!("{claim}-e{k}")).collect();
        ev.sort();
        self.expected.insert(claim, ev);
        let triggered = r.json()["checkpoint_triggered"] == true;
        if triggered {
            self.checkpoints += 1;
        }
        triggered
    }

    /// Retrieval must equal the oracle exactly, and the leader's exported
    /// state must carry the same ids.
    fn verify(&self, s: &Stack, what: &str) {
        let api: BTreeMap<String, Vec<String>> = s.claim_evidence_map(T, ALL, 1000);
        let missing: Vec<_> = self
            .expected
            .keys()
            .filter(|c| !api.contains_key(*c))
            .collect();
        assert!(
            missing.is_empty(),
            "{what}: retrieval skipped claims {missing:?}"
        );
        assert_eq!(
            api, self.expected,
            "{what}: retrieval differs from what the leader acknowledged"
        );
        let leader = s.leader_state();
        let leader_claims: Vec<&String> = leader.claims.keys().collect();
        let want_claims: Vec<&String> = self.expected.keys().collect();
        assert_eq!(
            leader_claims, want_claims,
            "{what}: leader export differs from acknowledged writes"
        );
        for (claim, ev) in &self.expected {
            for e in ev {
                assert_eq!(
                    leader.evidence.get(e),
                    Some(claim),
                    "{what}: leader lost evidence {e}"
                );
            }
        }
    }
}

fn resyncs(s: &Stack) -> u64 {
    s.retrieval_replication()["resyncs_total"]
        .as_u64()
        .expect("resyncs_total")
}

fn stack() -> Stack {
    let mut s = Stack::new(StackOpts {
        checkpoint_every: Some(CHECKPOINT_EVERY),
        extra_retrieval_env: vec![("DASH_RETRIEVAL_MAX_TOP_K".into(), "5000".into())],
        ..Default::default()
    });
    s.start_all();
    s
}

#[test]
fn live_follower_survives_one_leader_checkpoint() {
    let s = stack();
    let mut o = Oracle::new();
    // Fill the WAL right up to (but not past) the threshold.
    while o.checkpoints == 0 && o.next < 3 {
        o.write(&s);
    }
    s.wait_caught_up(Duration::from_secs(20));
    o.verify(&s, "before checkpoint");
    let (gen_before, _) = s.leader_position();
    let resyncs_before = resyncs(&s);

    // Write until the leader checkpoints exactly once.
    let mut guard = 0;
    while o.checkpoints == 0 {
        o.write(&s);
        guard += 1;
        assert!(
            guard < 40,
            "leader never checkpointed with DASH_CHECKPOINT_MAX_WAL_RECORDS={CHECKPOINT_EVERY}"
        );
    }
    assert_eq!(o.checkpoints, 1);
    // A few more writes after the compaction, below the next threshold.
    for _ in 0..2 {
        assert!(
            !o.write(&s),
            "second checkpoint triggered unexpectedly early"
        );
    }
    s.wait_caught_up(Duration::from_secs(30));
    o.verify(&s, "after one checkpoint");
    let (gen_after, _) = s.leader_position();
    let delta = resyncs(&s) - resyncs_before;
    assert!(
        delta <= 1,
        "follower resynced {delta} times for a single leader checkpoint"
    );
    println!("generation {gen_before} -> {gen_after}, resyncs during checkpoint: {delta}");
}

#[test]
fn follower_offline_across_several_checkpoints_converges_with_one_resync() {
    let mut s = stack();
    let mut o = Oracle::new();
    for _ in 0..4 {
        o.write(&s);
    }
    s.wait_caught_up(Duration::from_secs(20));
    o.verify(&s, "initial");

    // Follower goes away while the leader checkpoints repeatedly.
    s.retrieval.take().unwrap().kill9();
    let mut guard = 0;
    while o.checkpoints < 3 {
        o.write(&s);
        guard += 1;
        assert!(
            guard < 120,
            "leader checkpointed only {} times",
            o.checkpoints
        );
    }
    s.start_retrieval();
    s.wait_caught_up(Duration::from_secs(60));
    o.verify(&s, "after follower returned");
    let r = resyncs(&s);
    assert!(
        r <= 1,
        "follower resynced {r} times after missing {} checkpoints (must resync at most once)",
        o.checkpoints
    );

    // Keep writing (below the next threshold) and restart the follower once
    // more: no further resync, nothing skipped.
    for _ in 0..2 {
        o.write(&s);
    }
    s.wait_caught_up(Duration::from_secs(30));
    o.verify(&s, "after more writes");
    assert!(
        resyncs(&s) <= 1,
        "extra resync without a checkpoint: {}",
        s.retrieval_replication()
    );
    s.restart_retrieval(false);
    s.wait_caught_up(Duration::from_secs(30));
    o.verify(&s, "after follower restart");
    assert!(
        resyncs(&s) <= 1,
        "restart caused resync storm: {}",
        s.retrieval_replication()
    );
}

#[test]
fn leader_restart_after_checkpoint_keeps_follower_exact() {
    let mut s = stack();
    let mut o = Oracle::new();
    while o.checkpoints < 2 {
        o.write(&s);
    }
    s.wait_caught_up(Duration::from_secs(30));
    o.verify(&s, "after two checkpoints");
    for round in 0..3 {
        s.restart_ingest(round % 2 == 0);
        for _ in 0..2 {
            o.write(&s);
        }
        s.wait_caught_up(Duration::from_secs(60));
        o.verify(&s, &format!("after leader restart {round}"));
    }
    let rep: Value = s.retrieval_replication();
    assert_eq!(
        rep["consecutive_failures"],
        json!(0),
        "follower unhealthy: {rep}"
    );
}
