//! Scenario 14: automatic failover of the ingestion leader (ADR 0006) with
//! the real binaries: one control plane and three ingestion nodes with
//! synchronous replication (`DASH_INGEST_MIN_SYNC_REPLICAS=1`).
//!
//! * The leader is killed with SIGKILL while a client writes to it; a
//!   follower is promoted within the configured window, every write the
//!   old leader acknowledged is on the new leader, writes resume there,
//!   the placement moves with it, and the followers converge.
//! * The old leader restarts and rejoins as a follower: it refuses writes
//!   (pointing at the new leader), keeps a copy of its WAL and resyncs.
//! * A leader that is paused (SIGSTOP, isolated from everyone) and resumed
//!   after a failover refuses every write: two nodes never accept writes
//!   at the same time.
//!
//! Waits poll real state with deadlines; nothing sleeps to synchronise.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dash_e2e::cluster::{Cluster, ClusterOpts, TENANT};
use dash_e2e::*;
use serde_json::Value;

const T: &str = TENANT;
const LEASE_MS: u64 = 2_000;
const GRACE_MS: u64 = 500;
const HEARTBEAT_MS: u64 = 200;

fn cluster() -> Cluster {
    Cluster::new(ClusterOpts {
        lease_ms: LEASE_MS,
        grace_ms: GRACE_MS,
        heartbeat_ms: HEARTBEAT_MS,
        ..ClusterOpts::default()
    })
}

fn metric(text: &str, name: &str) -> Option<f64> {
    text.lines()
        .find_map(|line| line.strip_prefix(&format!("{name} ")))
        .and_then(|value| value.trim().parse().ok())
}

#[test]
fn leader_kill_promotes_a_follower_without_losing_acknowledged_writes() {
    let mut cluster = cluster();
    cluster.start_all();

    // Before the failover: writes go to n1 and are confirmed by a follower;
    // a follower refuses writes and names the leader.
    for i in 0..10 {
        let r = cluster.ingest(0, &format!("pre-{i}")).unwrap();
        assert_eq!(r.status, 200, "{}", r.body);
        assert!(
            r.header("x-dash-sync-replicas")
                .and_then(|v| v.parse::<u32>().ok())
                .is_some_and(|n| n >= 1),
            "synchronous write confirmed by a follower: {:?}",
            r.headers
        );
    }
    let refused = cluster.ingest(1, "to-a-follower").unwrap();
    assert_eq!(refused.status, 503, "{}", refused.body);
    assert_eq!(refused.header("x-dash-leader"), Some("false"));
    assert_eq!(
        refused.header("x-dash-leader-url"),
        Some(cluster.nodes[0].url().as_str())
    );
    assert!(refused.header("retry-after").is_some());

    // A client keeps writing to n1 while it is killed.
    let acked: Arc<Mutex<Vec<String>>> =
        Arc::new(Mutex::new((0..10).map(|i| format!("pre-{i}")).collect()));
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let acked = Arc::clone(&acked);
        let stop = Arc::clone(&stop);
        let client = cluster.nodes[0].client();
        let key = cluster.ingest_key.clone();
        std::thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::SeqCst) {
                let id = format!("live-{i}");
                i += 1;
                match client.try_post_json(
                    "/v1/ingest",
                    &[("x-api-key", key.as_str())],
                    &bundle(T, &id, &format!("failover claim {id}"), 1),
                ) {
                    Ok(r) if r.status == 200 => acked.lock().unwrap().push(id),
                    _ => break,
                }
            }
        })
    };
    // Let some live writes land, then kill the leader mid-stream.
    let end = Instant::now() + Duration::from_secs(20);
    while acked.lock().unwrap().len() < 40 {
        assert!(
            Instant::now() < end,
            "writes are not progressing\n{}",
            cluster.logs()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    let killed_at = Instant::now();
    cluster.nodes[0].proc.as_mut().unwrap().kill9();
    stop.store(true, Ordering::SeqCst);
    writer.join().unwrap();
    let acked: Vec<String> = acked.lock().unwrap().clone();

    let leader = cluster.wait_leader(&[1, 2], Duration::from_secs(30));
    let failover = killed_at.elapsed();
    eprintln!(
        "failover: {} promoted {} ms after SIGKILL of n1 (lease {LEASE_MS} ms, grace {GRACE_MS} ms, heartbeat {HEARTBEAT_MS} ms)",
        cluster.nodes[leader].id,
        failover.as_millis()
    );
    assert!(
        failover <= Duration::from_millis(LEASE_MS + GRACE_MS + 4 * HEARTBEAT_MS + 3_000),
        "failover took {failover:?}"
    );
    let follower = if leader == 1 { 2 } else { 1 };

    // Every acknowledged write survived the leader's death.
    let on_leader = cluster.claims(leader);
    let lost: Vec<&String> = acked.iter().filter(|id| !on_leader.contains(*id)).collect();
    assert!(lost.is_empty(), "acknowledged writes lost: {lost:?}");

    // Writes resume on the new leader (and are confirmed by the follower,
    // which now follows it).
    for i in 0..10 {
        let r = cluster.ingest(leader, &format!("post-{i}")).unwrap();
        assert_eq!(r.status, 200, "{}\n{}", r.body, cluster.logs());
    }
    cluster.wait_same_claims(follower, leader, Duration::from_secs(30));

    // The control plane moved the placement and bumped the term.
    let placement = cluster.cp_get("/v1/control-plane/placement?format=csv");
    assert_eq!(placement.status, 200);
    let leader_line = placement
        .body
        .lines()
        .find(|line| line.contains(",leader,"))
        .unwrap_or_default()
        .to_string();
    assert!(
        leader_line.contains(&cluster.nodes[leader].id) && leader_line.contains(",2,"),
        "placement: {}",
        placement.body
    );
    let status: Value = cluster.cp_get("/v1/control-plane/ingest").json();
    assert_eq!(status["term"], 2, "{status}");
    assert_eq!(status["leader_node_id"], cluster.nodes[leader].id.as_str());

    // The old leader restarts: it rejoins as a follower, refuses writes,
    // keeps its WAL aside and converges to the new leader.
    cluster.start_node(0);
    cluster.wait_ready(0, Duration::from_secs(30));
    assert_eq!(cluster.nodes[0].status("/v1/ready/leader"), Some(503));
    let refused = cluster.ingest(0, "to-the-old-leader").unwrap();
    assert_eq!(refused.status, 503, "{}", refused.body);
    assert_eq!(
        refused.header("x-dash-leader-url"),
        Some(cluster.nodes[leader].url().as_str())
    );
    cluster.wait_same_claims(0, leader, Duration::from_secs(30));
    let r = cluster.ingest(leader, "after-rejoin").unwrap();
    assert_eq!(r.status, 200, "{}", r.body);
    cluster.wait_same_claims(0, leader, Duration::from_secs(30));
    let deposed = std::fs::read_dir(cluster.dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .any(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("n1.wal.deposed-t1-")
        });
    assert!(
        deposed,
        "the old leader's WAL is kept aside before the resync"
    );

    let metrics = cluster.cp_get("/metrics").body;
    assert_eq!(
        metric(&metrics, "dash_control_plane_ingest_term"),
        Some(2.0)
    );
    assert_eq!(
        metric(&metrics, "dash_control_plane_ingest_promotions_total"),
        Some(2.0),
        "bootstrap + one failover"
    );
}

#[cfg(unix)]
#[test]
fn a_paused_leader_that_resumes_cannot_write_and_rejoins_as_follower() {
    let mut cluster = cluster();
    cluster.start_all();
    for i in 0..5 {
        assert_eq!(cluster.ingest(0, &format!("a-{i}")).unwrap().status, 200);
    }
    // Isolate n1 completely: it can neither heartbeat nor serve.
    let pid = cluster.nodes[0].proc.as_ref().unwrap().pid() as libc::pid_t;
    // SAFETY: plain signal delivery to our own child process.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0);
    let leader = cluster.wait_leader(&[1, 2], Duration::from_secs(30));
    let r = cluster.ingest(leader, "b-0").unwrap();
    assert_eq!(r.status, 200, "{}\n{}", r.body, cluster.logs());

    // n1 resumes believing nothing happened. Its lease ran out while it was
    // stopped (the monotonic clock kept going), so it refuses writes even
    // before it hears about the new term.
    // SAFETY: as above.
    assert_eq!(unsafe { libc::kill(pid, libc::SIGCONT) }, 0);
    let refused = cluster.ingest(0, "split-brain").unwrap();
    assert_eq!(refused.status, 503, "{}", refused.body);
    assert!(
        cluster.claims(leader).iter().all(|id| id != "split-brain")
            && cluster.claims(0).iter().all(|id| id != "split-brain"),
        "no node accepted the write"
    );
    // It learns the new term, follows the new leader and converges.
    cluster.wait_ready(0, Duration::from_secs(30));
    let end = Instant::now() + Duration::from_secs(30);
    loop {
        let r = cluster.ingest(0, "still-refused").unwrap();
        assert_eq!(r.status, 503, "{}", r.body);
        if r.header("x-dash-leader-url") == Some(cluster.nodes[leader].url().as_str()) {
            break;
        }
        assert!(Instant::now() < end, "n1 never learned the new leader");
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!(cluster.ingest(leader, "b-1").unwrap().status, 200);
    cluster.wait_same_claims(0, leader, Duration::from_secs(30));
    let claims = cluster.claims(0);
    assert!(claims.contains("a-4") && claims.contains("b-1"));
}
