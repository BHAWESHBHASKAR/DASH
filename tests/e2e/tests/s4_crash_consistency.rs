//! Scenario 4: crash consistency smoke. Repeatedly SIGKILL the ingestion
//! process in the middle of an ingest stream and verify, after every restart,
//! that the WAL replays, every acknowledged write is present, no evidence is
//! duplicated and no bundle (single or batch) is half applied.
//!
//! `E2E_CRASH_CYCLES` (default 100) sets the number of kill cycles and
//! `E2E_SEED` fixes the RNG seed. The seed is printed on failure.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread;
use std::time::Duration;

use dash_e2e::*;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};

const T: &str = "tenant-a";
/// A word every claim text in this file contains, so a retrieve with it lists
/// every claim through lexical matching (a query that matches nothing returns
/// nothing).
const ALL: &str = "claim";

#[derive(Debug, Clone)]
struct Rec {
    claim: String,
    evidence: Vec<String>,
    edge_to: Option<String>,
    body: Value,
}

/// One request's worth of records: a single bundle or a whole batch.
#[derive(Debug, Clone)]
struct Group {
    recs: Vec<Rec>,
    batch: bool,
}

fn make_bundle(rng: &mut StdRng, claim: &str, edge_to: Option<&str>) -> (Value, Rec) {
    let n = rng.gen_range(1..=4);
    let mut b = bundle(
        T,
        claim,
        &format!(
            "crash test claim {claim} on turbine {}",
            rng.gen_range(0..50)
        ),
        n,
    );
    if let Some(to) = edge_to {
        b["edges"] = json!([{
            "edge_id": format!("g-{claim}"), "from_claim_id": claim, "to_claim_id": to,
            "relation": "supports", "strength": 0.6
        }]);
    }
    let evidence = (0..n).map(|i| format!("{claim}-e{i}")).collect();
    let rec = Rec {
        claim: claim.to_string(),
        evidence,
        edge_to: edge_to.map(str::to_string),
        body: b.clone(),
    };
    (b, rec)
}

struct Outcome {
    acked: Vec<Group>,
    /// The request that was in flight (or failed) when the process died.
    unknown: Option<Group>,
}

/// Send ingests until `stop` is set or the connection breaks.
fn stream(
    s_addr: std::net::SocketAddr,
    key: String,
    prefix: String,
    seed: u64,
    batch_mode: bool,
    stop: Arc<AtomicBool>,
) -> Outcome {
    let client = Client::new(s_addr);
    let mut rng = StdRng::seed_from_u64(seed);
    let mut out = Outcome {
        acked: vec![],
        unknown: None,
    };
    let mut prev: Option<String> = None;
    let mut i = 0usize;
    while !stop.load(Ordering::Relaxed) {
        i += 1;
        let (path, body, group) = if batch_mode {
            let mut items = vec![];
            let mut recs = vec![];
            for j in 0..3 {
                let (b, r) = make_bundle(&mut rng, &format!("{prefix}-{i}-{j}"), None);
                items.push(b);
                recs.push(r);
            }
            (
                "/v1/ingest/batch",
                json!({"commit_id": format!("{prefix}-commit-{i}"), "items": items}),
                Group { recs, batch: true },
            )
        } else {
            let claim = format!("{prefix}-{i}");
            let edge_to = if rng.gen_bool(0.5) {
                prev.clone()
            } else {
                None
            };
            let (b, r) = make_bundle(&mut rng, &claim, edge_to.as_deref());
            prev = Some(claim);
            (
                "/v1/ingest",
                b,
                Group {
                    recs: vec![r],
                    batch: false,
                },
            )
        };
        match client.try_post_json(path, &[("x-api-key", key.as_str())], &body) {
            Ok(r) if r.status == 200 => out.acked.push(group),
            Ok(r) => {
                // A rejection is not expected for these valid bundles; treat
                // it as unknown so the verifier still checks atomicity, and
                // surface it.
                eprintln!("unexpected status {} for {}: {}", r.status, path, r.body);
                out.unknown = Some(group);
                break;
            }
            Err(_) => {
                out.unknown = Some(group);
                break;
            }
        }
    }
    out
}

fn run(cycles: usize, seed: u64) {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut s = Stack::new(StackOpts {
        // Plenty of headroom for enumerating every claim through the API.
        extra_retrieval_env: vec![("DASH_RETRIEVAL_MAX_TOP_K".into(), "100000".into())],
        ..Default::default()
    });
    s.start_all();
    let key = s.ik(T).1;
    let mut expected: BTreeMap<String, Rec> = BTreeMap::new();
    let mut total_acked = 0usize;

    for cycle in 0..cycles {
        let stop = Arc::new(AtomicBool::new(false));
        let results: Arc<Mutex<Vec<Outcome>>> = Arc::new(Mutex::new(vec![]));
        let mut handles = vec![];
        for (idx, batch_mode) in [(0, false), (1, false), (2, true)] {
            let (addr, key, stop, results) =
                (s.ingest_addr(), key.clone(), stop.clone(), results.clone());
            let wseed: u64 = rng.r#gen();
            handles.push(thread::spawn(move || {
                let o = stream(
                    addr,
                    key,
                    format!("c{cycle}w{idx}"),
                    wseed,
                    batch_mode,
                    stop,
                );
                results.lock().unwrap().push(o);
            }));
        }
        // Kill at a random moment (sometimes immediately, sometimes late).
        let delay = rng.gen_range(0..350u64);
        thread::sleep(Duration::from_millis(delay));
        s.kill_ingest();
        stop.store(true, Ordering::Relaxed);
        for h in handles {
            h.join().unwrap();
        }
        let outcomes = std::mem::take(&mut *results.lock().unwrap());

        // (a) The process must restart on the WAL it left behind.
        s.start_ingest();

        let leader = s.leader_state();
        let mut acked_now = 0usize;
        for o in &outcomes {
            for g in &o.acked {
                for r in &g.recs {
                    acked_now += 1;
                    assert!(
                        leader.claims.contains_key(&r.claim),
                        "cycle {cycle}: acknowledged claim {} is missing after kill -9",
                        r.claim
                    );
                    expected.insert(r.claim.clone(), r.clone());
                }
            }
            // Unacknowledged request: all or nothing.
            if let Some(g) = &o.unknown {
                let present: Vec<&Rec> = g
                    .recs
                    .iter()
                    .filter(|r| leader.claims.contains_key(&r.claim))
                    .collect();
                if g.batch {
                    assert!(
                        present.is_empty() || present.len() == g.recs.len(),
                        "cycle {cycle}: partial batch after kill -9 ({} of {} claims present): {:?}",
                        present.len(),
                        g.recs.len(),
                        g.recs.iter().map(|r| &r.claim).collect::<Vec<_>>()
                    );
                }
                for r in present {
                    expected.insert(r.claim.clone(), r.clone());
                }
            }
        }
        total_acked += acked_now;

        // (c)/(d) Exactly the expected claims, each with its full evidence
        // list exactly once, and the acknowledged edges.
        let unexpected: Vec<&String> = leader
            .claims
            .keys()
            .filter(|c| !expected.contains_key(*c))
            .collect();
        assert!(
            unexpected.is_empty(),
            "cycle {cycle}: claims present that were never sent: {unexpected:?}"
        );
        for (claim, rec) in &expected {
            assert!(
                leader.claims.contains_key(claim),
                "cycle {cycle}: {claim} vanished"
            );
            for e in &rec.evidence {
                assert_eq!(
                    leader.evidence.get(e).map(String::as_str),
                    Some(claim.as_str()),
                    "cycle {cycle}: bundle {claim} is partial: evidence {e} missing (acknowledged {} items)",
                    rec.evidence.len()
                );
                assert_eq!(
                    leader.evidence_lines.get(e).copied(),
                    Some(1),
                    "cycle {cycle}: evidence {e} is written more than once in the WAL"
                );
            }
            if let Some(to) = &rec.edge_to {
                assert!(
                    leader
                        .edges
                        .contains(&(claim.clone(), to.clone(), "supports".to_string())),
                    "cycle {cycle}: edge {claim}->{to} lost"
                );
            }
        }
        let known_evidence: BTreeSet<&String> =
            expected.values().flat_map(|r| r.evidence.iter()).collect();
        let stray: Vec<&String> = leader
            .evidence
            .keys()
            .filter(|e| !known_evidence.contains(e))
            .collect();
        assert!(
            stray.is_empty(),
            "cycle {cycle}: evidence with no expected claim: {stray:?}"
        );

        // The retrieval follower must converge to exactly the same picture,
        // with no duplicated citations.
        if cycle % 5 == 4 || cycle + 1 == cycles || cycle < 5 {
            s.wait_caught_up(Duration::from_secs(60));
            let api = s.claim_evidence_map(T, ALL, 100_000);
            assert_eq!(
                api.len(),
                expected.len(),
                "cycle {cycle}: retrieval claim count differs from the leader"
            );
            for (claim, rec) in &expected {
                let mut want = rec.evidence.clone();
                want.sort();
                assert_eq!(
                    api.get(claim),
                    Some(&want),
                    "cycle {cycle}: retrieval evidence for {claim} differs (duplicate or missing)"
                );
            }
        }

        // Acknowledged work must also be recognised by the restarted leader
        // as already stored: re-sending a sample must not grow the WAL.
        // Single-ingest bundles only (workers w0/w1): batch items are keyed by
        // commit_id, so replaying one as a plain ingest legitimately rewrites it.
        let sample: Vec<&Rec> = expected
            .values()
            .filter(|r| !r.claim.contains("w2-") && rng.gen_bool(0.03))
            .take(5)
            .collect();
        for r in sample {
            let before = s.leader_position();
            let resp = s.ingest_as(T, &r.body);
            assert_eq!(
                resp.status, 200,
                "cycle {cycle}: replay of {} rejected: {}",
                r.claim, resp.body
            );
            assert_eq!(
                s.leader_position(),
                before,
                "cycle {cycle}: replaying acknowledged bundle {} (edge_to={:?}, {} evidence) changed the WAL",
                r.claim,
                r.edge_to,
                r.evidence.len()
            );
        }
    }
    println!(
        "crash cycles={cycles} acknowledged_claims={total_acked} final_claims={}",
        expected.len()
    );
    assert!(
        total_acked > 0,
        "no ingest was ever acknowledged; the test did not exercise anything"
    );
}

#[test]
fn kill9_during_ingest_loses_nothing_and_duplicates_nothing() {
    let cycles: usize = std::env::var("E2E_CRASH_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);
    let seed: u64 = std::env::var("E2E_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| rand::thread_rng().r#gen());
    println!("E2E_SEED={seed} E2E_CRASH_CYCLES={cycles}");
    if let Err(e) = catch_unwind(AssertUnwindSafe(|| run(cycles, seed))) {
        eprintln!(
            "crash-consistency FAILED; reproduce with E2E_SEED={seed} E2E_CRASH_CYCLES={cycles}"
        );
        resume_unwind(e);
    }
}
