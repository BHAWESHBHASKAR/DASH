//! `crash-test --failover`: the leader-failover scenario (ADR 0006).
//!
//! A control plane and three ingestion nodes run with synchronous
//! replication (`DASH_INGEST_MIN_SYNC_REPLICAS=1`). Every cycle drives
//! concurrent writers against the current leader, SIGKILLs it at a random
//! moment, waits for a follower to be promoted and checks:
//!
//! * exactly one node accepts writes at any time (`/v1/ready/leader`);
//! * every write the old leader acknowledged is on the new leader, with all
//!   of its evidence, and the request in flight at the kill is there
//!   completely or not at all;
//! * the killed node restarts, rejoins as a follower and converges to the
//!   new leader, and so does the other follower.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use dash_e2e::LeaderState;
use dash_e2e::cluster::{Cluster, ClusterOpts};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde_json::{Value, json};

use super::{Config, Group, writer};

#[derive(Debug, Default)]
pub(crate) struct FailoverTotals {
    pub(crate) acked_requests: u64,
    pub(crate) acked_claims: u64,
    pub(crate) unknown_requests: u64,
    pub(crate) unknown_applied: u64,
    /// Acknowledged writes missing after a failover (allowed, and only
    /// counted, with asynchronous replication).
    pub(crate) acked_lost: u64,
    pub(crate) failover_ms: Vec<u64>,
    pub(crate) rejoin_ms: Vec<u64>,
}

fn state_of(cluster: &Cluster, index: usize) -> LeaderState {
    let r = cluster.nodes[index].client().get(
        "/internal/replication/export",
        &[("x-replication-token", cluster.replication_token.as_str())],
    );
    assert_eq!(
        r.status, 200,
        "export of {}: {}",
        cluster.nodes[index].id, r.body
    );
    LeaderState::parse(&r.body)
}

fn group_present(state: &LeaderState, group: &Group) -> Result<bool, String> {
    let present: Vec<bool> = group
        .recs
        .iter()
        .map(|rec| {
            state.claims.contains_key(&rec.claim)
                && (0..rec.evidence).all(|i| {
                    state
                        .evidence
                        .get(&format!("{}-e{i}", rec.claim))
                        .is_some_and(|claim| *claim == rec.claim)
                })
        })
        .collect();
    if present.iter().all(|p| *p) {
        Ok(true)
    } else if present.iter().all(|p| !*p) {
        Ok(false)
    } else {
        Err(format!("request applied partially: {group:?}"))
    }
}

/// Synchronous replication is on unless `--env DASH_INGEST_MIN_SYNC_REPLICAS=0`
/// turned it off (then lost acknowledged writes are measured, not failed).
pub(crate) fn synchronous(cfg: &Config) -> bool {
    !cfg.envs
        .iter()
        .rev()
        .find(|(key, _)| key == "DASH_INGEST_MIN_SYNC_REPLICAS")
        .is_some_and(|(_, value)| value.trim() == "0")
}

fn cycle(
    cfg: &Config,
    cluster: &mut Cluster,
    leader: usize,
    n: usize,
    rng: &mut StdRng,
    totals: &mut FailoverTotals,
) -> Result<usize, String> {
    let stop = Arc::new(AtomicBool::new(false));
    let handles: Vec<_> = (0..cfg.writers)
        .map(|w| {
            let addr = cluster.nodes[leader].addr();
            let key = cluster.ingest_key.clone();
            let stop = Arc::clone(&stop);
            let seed = rng.r#gen();
            let percent = cfg.batch_percent;
            thread::spawn(move || writer(addr, key, format!("f{n}w{w}"), seed, percent, stop))
        })
        .collect();
    thread::sleep(Duration::from_millis(
        rng.gen_range(50..=cfg.max_kill_delay_ms.max(51)),
    ));
    let killed_at = Instant::now();
    cluster.nodes[leader]
        .proc
        .as_mut()
        .ok_or("leader process missing")?
        .kill9();
    stop.store(true, Ordering::SeqCst);
    let outcomes: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().map_err(|_| "writer panicked".to_string()))
        .collect::<Result<_, _>>()?;

    let others: Vec<usize> = (0..cluster.nodes.len()).filter(|i| *i != leader).collect();
    let new_leader = cluster.wait_leader(&others, Duration::from_secs(60));
    let failover_ms = killed_at.elapsed().as_millis() as u64;
    totals.failover_ms.push(failover_ms);
    let window = cluster.opts.lease_ms + cluster.opts.grace_ms;
    if failover_ms > 3 * window {
        // Slow failover: show what the control plane and the nodes said.
        eprintln!("cycle {n}: slow failover ({failover_ms} ms):");
        for line in cluster.logs().lines().filter(|line| {
            line.starts_with("control-plane: ") || line.starts_with("ingestion failover: ")
        }) {
            eprintln!("  {line}");
        }
    }

    let state = state_of(cluster, new_leader);
    for outcome in &outcomes {
        if let Some(unexpected) = outcome.unexpected.first() {
            return Err(format!("unexpected answer before the kill: {unexpected}"));
        }
        for group in &outcome.acked {
            totals.acked_requests += 1;
            totals.acked_claims += group.recs.len() as u64;
            if !group_present(&state, group)? {
                totals.acked_lost += 1;
                if synchronous(cfg) {
                    return Err(format!(
                        "cycle {n}: acknowledged write lost in the failover from {} to {}: {group:?}",
                        cluster.nodes[leader].id, cluster.nodes[new_leader].id
                    ));
                }
            }
        }
        if let Some(group) = &outcome.unknown {
            totals.unknown_requests += 1;
            if group_present(&state, group)? {
                totals.unknown_applied += 1;
            }
        }
    }

    // The killed node rejoins as a follower and converges.
    let rejoin_started = Instant::now();
    cluster.start_node(leader);
    cluster.wait_ready(leader, Duration::from_secs(60));
    if cluster.nodes[leader].status("/v1/ready/leader") == Some(200) {
        return Err(format!(
            "cycle {n}: the restarted {} claims leadership",
            cluster.nodes[leader].id
        ));
    }
    for follower in others.iter().copied().filter(|i| *i != new_leader) {
        cluster.wait_same_claims(follower, new_leader, Duration::from_secs(60));
    }
    cluster.wait_same_claims(leader, new_leader, Duration::from_secs(60));
    totals
        .rejoin_ms
        .push(rejoin_started.elapsed().as_millis() as u64);
    Ok(new_leader)
}

pub(crate) fn run(cfg: &Config) -> (Result<(), String>, FailoverTotals, usize) {
    let mut totals = FailoverTotals::default();
    let mut rng = StdRng::seed_from_u64(cfg.seed);
    let mut opts = ClusterOpts::default();
    opts.extra_node_env.extend(cfg.envs.iter().cloned());
    if let Some(every) = cfg.checkpoint_every {
        opts.extra_node_env
            .push(("DASH_CHECKPOINT_MAX_WAL_RECORDS".into(), every.to_string()));
    }
    let mut cluster = Cluster::new(opts);
    let started = catch_unwind(AssertUnwindSafe(|| cluster.start_all()));
    let mut leader = match started {
        Ok(leader) => leader,
        Err(panic) => return (Err(panic_text(&panic)), totals, 0),
    };
    for n in 0..cfg.cycles {
        let result = catch_unwind(AssertUnwindSafe(|| {
            cycle(cfg, &mut cluster, leader, n, &mut rng, &mut totals)
        }));
        match result {
            Ok(Ok(next)) => {
                println!(
                    "cycle {n}: leader {} killed, {} promoted after {} ms",
                    cluster.nodes[leader].id,
                    cluster.nodes[next].id,
                    totals.failover_ms.last().copied().unwrap_or(0)
                );
                leader = next;
            }
            Ok(Err(err)) => {
                eprintln!("{}", cluster.logs());
                return (Err(err), totals, n);
            }
            Err(panic) => {
                eprintln!("{}", cluster.logs());
                return (Err(panic_text(&panic)), totals, n);
            }
        }
    }
    (Ok(()), totals, cfg.cycles)
}

fn panic_text(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
        .unwrap_or_else(|| "panic".to_string())
}

fn percentile(values: &[u64], p: f64) -> u64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    super::percentile(&sorted, p)
}

pub(crate) fn summary(
    cfg: &Config,
    result: &Result<(), String>,
    totals: &FailoverTotals,
    cycles_done: usize,
    duration: Duration,
) -> Value {
    let stats = |values: &[u64]| {
        json!({
            "p50": percentile(values, 0.50),
            "p95": percentile(values, 0.95),
            "max": values.iter().max().copied().unwrap_or(0),
        })
    };
    json!({
        "ok": result.is_ok(),
        "error": result.as_ref().err(),
        "mode": "failover",
        "seed": cfg.seed,
        "cycles_requested": cfg.cycles,
        "cycles_completed": cycles_done,
        "writers": cfg.writers,
        "acked_requests": totals.acked_requests,
        "acked_claims": totals.acked_claims,
        "unknown_requests": totals.unknown_requests,
        "unknown_applied": totals.unknown_applied,
        "synchronous_replication": synchronous(cfg),
        "acked_lost": totals.acked_lost,
        "failover_ms": stats(&totals.failover_ms),
        "rejoin_ms": stats(&totals.rejoin_ms),
        "duration_s": duration.as_secs_f64(),
    })
}
