//! Failover on the ingestion side, driven deterministically: lease expiry
//! is produced by back-dating the heartbeat send time (no sleeping), and
//! the synchronous-replication test uses a follower thread that long-polls
//! the leader's real HTTP handler.

use super::super::tests::env_lock;
use super::super::*;
use super::*;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::Path;
use std::sync::atomic::AtomicBool;

const LEASE: Duration = Duration::from_millis(5_000);

fn config(node: &str) -> FailoverConfig {
    FailoverConfig::new(
        "http://127.0.0.1:1".to_string(),
        format!("http://{node}:8081"),
        node.to_string(),
        1_000,
        true,
    )
    .unwrap()
}

fn member(mut runtime: IngestionRuntime, node: &str, state_path: Option<PathBuf>) -> SharedRuntime {
    runtime.failover = FailoverState::member(&config(node), state_path);
    Arc::new(Mutex::new(runtime))
}

fn persistent(dir: &Path) -> IngestionRuntime {
    let wal = FileWal::open(dir.join("wal.log")).expect("wal opens");
    IngestionRuntime::persistent(InMemoryStore::new(), wal, CheckpointPolicy::default())
}

fn leader_reply(term: u64) -> HeartbeatReply {
    HeartbeatReply {
        term,
        leader: true,
        leader_node_id: None,
        leader_url: None,
        lease: LEASE,
    }
}

fn follower_reply(term: u64, leader: &str) -> HeartbeatReply {
    HeartbeatReply {
        term,
        leader: false,
        leader_node_id: Some(leader.to_string()),
        leader_url: Some(format!("http://{leader}:8081")),
        lease: LEASE,
    }
}

fn ingest_request(claim_id: &str) -> HttpRequest {
    HttpRequest {
        method: "POST".to_string(),
        target: "/v1/ingest".to_string(),
        headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
        body: format!(
            r#"{{"claim":{{"claim_id":"{claim_id}","tenant_id":"tenant-a","canonical_text":"failover claim {claim_id}","confidence":0.9}}}}"#
        )
        .into_bytes(),
    }
}

fn get(target: &str) -> HttpRequest {
    HttpRequest {
        method: "GET".to_string(),
        target: target.to_string(),
        headers: HashMap::new(),
        body: Vec::new(),
    }
}

fn header<'a>(response: &'a HttpResponse, name: &str) -> Option<&'a str> {
    response
        .headers
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn kv(body: &str, key: &str) -> u64 {
    body.lines()
        .find_map(|line| line.strip_prefix(&format!("{key}=")))
        .unwrap_or_else(|| panic!("frame lacks {key}: {body}"))
        .parse()
        .unwrap()
}

#[test]
fn write_gate_follows_role_and_lease() {
    let now = Instant::now();
    let mut state = FailoverState::default();
    assert!(
        state.check_write(now).is_ok(),
        "failover off: always writable"
    );
    state = FailoverState::member(&config("a"), None);
    assert_eq!(
        state.check_write(now).unwrap_err().reason,
        "no_leader_assigned"
    );
    state.role = NodeRole::Leader;
    state.lease_until = Some(now + Duration::from_millis(10));
    assert!(state.check_write(now).is_ok());
    assert_eq!(
        state
            .check_write(now + Duration::from_millis(10))
            .unwrap_err()
            .reason,
        "leader_lease_expired",
        "the lease ends exactly at lease_until"
    );
    state.role = NodeRole::Follower;
    state.leader_node_id = Some("b".into());
    state.leader_url = Some("http://b:8081".into());
    state.term = 7;
    let refused = state.check_write(now).unwrap_err();
    let response = refused.response();
    assert_eq!(response.status, 503);
    assert_eq!(response.retry_after_secs, Some(1));
    assert_eq!(header(&response, "X-Dash-Leader"), Some("false"));
    assert_eq!(header(&response, "X-Dash-Leader-Node"), Some("b"));
    assert_eq!(
        header(&response, "X-Dash-Leader-Url"),
        Some("http://b:8081")
    );
    assert_eq!(header(&response, "X-Dash-Term"), Some("7"));
    assert!(response.body.contains("not_leader"), "{}", response.body);
}

#[test]
fn a_member_writes_only_between_promotion_and_lease_expiry() {
    let _env = env_lock().lock().unwrap();
    let runtime = member(IngestionRuntime::in_memory(InMemoryStore::new()), "a", None);
    let refused = handle_request(&runtime, &ingest_request("c0"));
    assert_eq!(refused.status, 503, "{}", refused.body);
    assert!(
        refused.body.contains("no_leader_assigned"),
        "{}",
        refused.body
    );

    runtime
        .lock()
        .unwrap()
        .apply_heartbeat_reply(&leader_reply(1), Instant::now());
    assert_eq!(handle_request(&runtime, &ingest_request("c1")).status, 200);
    let ready = handle_request(&runtime, &get("/v1/ready/leader"));
    assert_eq!(ready.status, 200, "{}", ready.body);
    assert!(ready.body.contains("\"role\":\"leader\""), "{}", ready.body);

    // The control plane stays silent: model a renewal whose heartbeat was
    // sent one full lease ago. Writes stop at once (no sleep involved).
    let long_ago = Instant::now().checked_sub(LEASE).expect("monotonic clock");
    runtime
        .lock()
        .unwrap()
        .apply_heartbeat_reply(&leader_reply(1), long_ago);
    let expired = handle_request(&runtime, &ingest_request("c2"));
    assert_eq!(expired.status, 503);
    assert!(
        expired.body.contains("leader_lease_expired"),
        "{}",
        expired.body
    );
    assert_eq!(
        handle_request(&runtime, &get("/v1/ready/leader")).status,
        503
    );
    // A renewal brings writes back.
    runtime
        .lock()
        .unwrap()
        .apply_heartbeat_reply(&leader_reply(1), Instant::now());
    assert_eq!(handle_request(&runtime, &ingest_request("c3")).status, 200);
    let metrics = runtime
        .lock()
        .unwrap()
        .failover
        .metrics_text(Instant::now());
    assert!(
        metrics.contains("dash_ingest_failover_promotions_total 1"),
        "{metrics}"
    );
}

#[test]
fn a_deposed_leader_refuses_writes_keeps_its_wal_and_resyncs() {
    let _env = env_lock().lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("wal.log.failover");
    let runtime = member(persistent(dir.path()), "a", Some(state_path.clone()));
    runtime
        .lock()
        .unwrap()
        .apply_heartbeat_reply(&leader_reply(3), Instant::now());
    assert_eq!(handle_request(&runtime, &ingest_request("c1")).status, 200);
    assert_eq!(std::fs::read_to_string(&state_path).unwrap(), "term=3\n");

    // The control plane promoted b at term 4.
    runtime
        .lock()
        .unwrap()
        .apply_heartbeat_reply(&follower_reply(4, "b"), Instant::now());
    let refused = handle_request(&runtime, &ingest_request("c2"));
    assert_eq!(refused.status, 503);
    assert_eq!(header(&refused, "X-Dash-Leader-Url"), Some("http://b:8081"));
    let guard = runtime.lock().unwrap();
    assert_eq!(guard.failover.role, NodeRole::Follower);
    assert_eq!(guard.failover.term, 4);
    assert_eq!(guard.failover.pull_source(), Some("http://b:8081"));
    assert!(guard.replication_follower.enabled);
    assert!(
        guard.replication_follower.force_resync,
        "must resync from b"
    );
    assert_eq!(guard.failover.demotions_total, 1);
    drop(guard);
    let copies: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .filter(|name| name.starts_with("wal.log.deposed-t3-"))
        .collect();
    assert_eq!(copies.len(), 1, "WAL kept aside: {copies:?}");
    assert_eq!(std::fs::read_to_string(&state_path).unwrap(), "term=4\n");
    // A restarted process remembers the term.
    assert_eq!(
        FailoverState::member(&config("a"), Some(state_path)).term,
        4
    );
}

#[test]
fn a_poll_with_a_newer_term_deposes_the_leader_immediately() {
    let _env = env_lock().lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let runtime = member(persistent(dir.path()), "a", None);
    runtime
        .lock()
        .unwrap()
        .apply_heartbeat_reply(&leader_reply(3), Instant::now());
    assert_eq!(handle_request(&runtime, &ingest_request("c1")).status, 200);
    let ok = handle_request(
        &runtime,
        &get("/internal/replication/wal?from_offset=0&max_records=10&gen_switch=1&term=3"),
    );
    assert_eq!(ok.status, 200);
    assert_eq!(kv(&ok.body, "term"), 3, "the leader answers with its term");
    let stale = handle_request(
        &runtime,
        &get("/internal/replication/wal?from_offset=0&max_records=10&gen_switch=1&term=4"),
    );
    assert_eq!(stale.status, 409, "{}", stale.body);
    assert!(stale.body.contains("stale_leader_term"));
    let refused = handle_request(&runtime, &ingest_request("c2"));
    assert_eq!(refused.status, 503);
    assert!(
        refused.body.contains("leader_lease_expired"),
        "{}",
        refused.body
    );
}

#[test]
fn a_promoted_follower_fences_its_lineage() {
    let _env = env_lock().lock().unwrap();
    let dir_a = tempfile::tempdir().unwrap();
    let dir_b = tempfile::tempdir().unwrap();
    // Leader a (no failover needed for the source side of this test).
    let leader = Arc::new(Mutex::new(persistent(dir_a.path())));
    for id in ["c1", "c2", "c3"] {
        assert_eq!(handle_request(&leader, &ingest_request(id)).status, 200);
    }
    let frame = leader
        .lock()
        .unwrap()
        .replication_delta_for_followers(None, 0, 1_000, true)
        .unwrap();
    let generation_a = frame.generation;
    let n = frame.next_offset;
    assert!(n >= 6, "three single-ingest groups");

    // Follower b applies everything, then is promoted at term 2.
    let follower = member(persistent(dir_b.path()), "b", None);
    {
        let mut b = follower.lock().unwrap();
        b.failover.role = NodeRole::Follower;
        b.apply_replication_delta_frame(&replication::ReplicationDeltaFrame {
            generation: frame.generation,
            needs_resync: false,
            switch_from: None,
            from_offset: 0,
            next_offset: n,
            total_records: frame.total_records,
            wal_lines: frame.wal_lines.clone(),
            term: None,
        })
        .unwrap();
        b.replication_follower.synced_once = true;
        assert_eq!(b.replication_follower.generation, Some(generation_a));
        b.apply_heartbeat_reply(&leader_reply(2), Instant::now());
        assert_eq!(b.failover.role, NodeRole::Leader, "promotion succeeded");
        let transition = *lock_wal(b.wal.as_ref().unwrap())
            .generation_transitions()
            .last()
            .unwrap();
        assert_eq!(transition.from_generation, generation_a);
        assert_eq!(transition.from_records, n);
    }
    assert_eq!(handle_request(&follower, &ingest_request("c4")).status, 200);
    let mut b = follower.lock().unwrap();
    // A follower of a exactly at b's offset crosses with a generation switch.
    let at = b
        .replication_delta_for_followers(Some(generation_a), n, 1_000, true)
        .unwrap();
    assert!(at.switched_from.is_some(), "switch, not resync");
    assert!(!at.needs_resync);
    assert!(at.wal_lines.iter().any(|line| line.contains("c4")));
    // One behind gets the rest of the old generation from the closed file.
    let behind = b
        .replication_delta_for_followers(Some(generation_a), n - 2, 1_000, true)
        .unwrap();
    assert_eq!(behind.generation, generation_a);
    assert_eq!(behind.wal_lines.len(), 2);
    // One that holds records b never received (ahead) must resync.
    let ahead = b
        .replication_delta_for_followers(Some(generation_a), n + 1, 1_000, true)
        .unwrap();
    assert!(ahead.needs_resync, "a divergent follower is resynced");
}

#[test]
fn promotion_is_refused_when_the_wal_does_not_match_the_cursor() {
    let dir = tempfile::tempdir().unwrap();
    let runtime = member(persistent(dir.path()), "b", None);
    let mut b = runtime.lock().unwrap();
    b.failover.role = NodeRole::Follower;
    b.replication_follower.generation = Some(42);
    b.replication_last_offset = 5; // the WAL is empty
    b.apply_heartbeat_reply(&leader_reply(2), Instant::now());
    assert_eq!(b.failover.role, NodeRole::Follower);
    assert!(b.failover.promotion_refused);
    assert_eq!(b.failover.promotion_failures_total, 1);
    let pull = ReplicationPullConfig::new("http://unused");
    assert!(b.failover_heartbeat_query(&pull).contains("synced=0"));
}

/// Serves one canned delta frame to every request.
fn canned_source(body: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(
                format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .as_bytes(),
            );
        }
    });
    url
}

#[test]
fn frames_from_a_deposed_leader_are_refused() {
    let line = "B2\tgroup-1\t0\t0";
    let body = format!(
        "status=ok\ngeneration=9\nneeds_resync=0\nswitch_from=none\nterm=4\nfrom_offset=0\nnext_offset=1\ntotal_records=1\nrecords=1\n{line}\n"
    );
    let url = canned_source(body);
    let runtime = member(IngestionRuntime::in_memory(InMemoryStore::new()), "c", None);
    {
        let mut guard = runtime.lock().unwrap();
        guard.failover.role = NodeRole::Follower;
        guard.failover.term = 5;
        guard.failover.leader_url = Some(url.clone());
        guard.replication_follower.enabled = true;
    }
    let base = ReplicationPullConfig::new("http://unused");
    let config = runtime.lock().unwrap().pull_config_for_tick(&base).unwrap();
    assert_eq!(config.source_base_url, url);
    assert_eq!(config.term, Some(5));
    assert_eq!(run_replication_pull_tick(&runtime, &config), 1);
    let guard = runtime.lock().unwrap();
    assert_eq!(guard.failover.stale_term_frames_total, 1);
    assert_eq!(guard.replication_last_offset, 0, "nothing applied");
}

#[test]
fn replica_progress_confirms_by_position_generation_chain_and_term() {
    let progress = ReplicaProgress::default();
    let target = SyncTarget {
        generation: 1,
        records: 10,
        transitions: vec![(1, 2), (2, 3)],
        term: Some(4),
    };
    let none = Duration::ZERO;
    assert_eq!(progress.wait_confirmed(&target, 1, none), 0);
    progress.record("f1", 1, 9, 4);
    assert_eq!(progress.wait_confirmed(&target, 1, none), 0, "one short");
    progress.record("f1", 1, 10, 4);
    assert_eq!(progress.wait_confirmed(&target, 1, none), 1);
    progress.record("f2", 3, 0, 4);
    assert_eq!(
        progress.wait_confirmed(&target, 2, none),
        2,
        "a follower two checkpoints later holds everything"
    );
    progress.record("f3", 1, 50, 3);
    assert_eq!(progress.wait_confirmed(&target, 3, none), 2, "older term");
    progress.record("f4", 77, 50, 4);
    assert_eq!(
        progress.wait_confirmed(&target, 3, none),
        2,
        "unknown generation"
    );
}

/// A follower that speaks the replication protocol against the leader's
/// HTTP handler: it long-polls and reports its position on every poll.
fn spawn_follower(leader: SharedRuntime, stop: Arc<AtomicBool>) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let mut generation: Option<u64> = None;
        let mut offset = 0usize;
        while !stop.load(Ordering::SeqCst) {
            let mut target = format!(
                "/internal/replication/wal?from_offset={offset}&max_records=1000&gen_switch=1&term=0&replica_id=f1&durable=1&wait_ms=200"
            );
            if let Some(generation) = generation {
                target.push_str(&format!("&from_generation={generation}"));
            }
            let response = handle_request(&leader, &get(&target));
            assert_eq!(response.status, 200, "{}", response.body);
            generation = Some(kv(&response.body, "generation"));
            offset = kv(&response.body, "next_offset") as usize;
        }
    })
}

fn sync_leader(dir: &Path, timeout_ms: u64, on_timeout: SyncTimeoutPolicy) -> SharedRuntime {
    let mut runtime = persistent(dir);
    runtime.sync_replication = SyncReplicationConfig {
        min_replicas: 1,
        timeout: Duration::from_millis(timeout_ms),
        on_timeout,
    };
    Arc::new(Mutex::new(runtime))
}

#[test]
fn a_synchronous_write_is_answered_once_a_follower_holds_it() {
    let _env = env_lock().lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    // Long timeout: the answer comes from the follower's confirmation.
    let leader = sync_leader(dir.path(), 30_000, SyncTimeoutPolicy::Fail);
    let stop = Arc::new(AtomicBool::new(false));
    let follower = spawn_follower(Arc::clone(&leader), Arc::clone(&stop));
    for id in ["s1", "s2", "s3"] {
        let response = handle_request(&leader, &ingest_request(id));
        assert_eq!(response.status, 200, "{}", response.body);
        assert_eq!(header(&response, "X-Dash-Sync-Replicas"), Some("1"));
    }
    stop.store(true, Ordering::SeqCst);
    leader.lock().unwrap().replica_progress.note_wal_advanced();
    follower.join().unwrap();
    let metrics = leader.lock().unwrap().sync_replication_metrics_text();
    assert!(
        metrics.contains("dash_ingest_sync_replication_confirmed_total 3"),
        "{metrics}"
    );
    assert!(
        metrics.contains("dash_ingest_sync_replication_timeouts_total 0"),
        "{metrics}"
    );
}

#[test]
fn without_a_follower_a_synchronous_write_fails_or_degrades_per_policy() {
    let _env = env_lock().lock().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let leader = sync_leader(dir.path(), 20, SyncTimeoutPolicy::Fail);
    let failed = handle_request(&leader, &ingest_request("t1"));
    assert_eq!(failed.status, 503, "{}", failed.body);
    assert!(
        failed.body.contains("sync_replication_timeout"),
        "{}",
        failed.body
    );
    assert_eq!(failed.retry_after_secs, Some(1));
    // The write itself is in the leader's WAL; a retry is idempotent.
    assert_eq!(leader.lock().unwrap().store.claims_len(), 1);

    let dir = tempfile::tempdir().unwrap();
    let leader = sync_leader(dir.path(), 20, SyncTimeoutPolicy::Degrade);
    let degraded = handle_request(&leader, &ingest_request("t2"));
    assert_eq!(degraded.status, 200);
    assert!(
        degraded
            .body
            .contains("\"commit_status\":\"sync_degraded\""),
        "{}",
        degraded.body
    );
    assert_eq!(
        header(&degraded, "X-Dash-Sync-Replication"),
        Some("degraded")
    );
    let metrics = leader.lock().unwrap().sync_replication_metrics_text();
    assert!(
        metrics.contains("dash_ingest_sync_replication_degraded_total 1"),
        "{metrics}"
    );
}

#[test]
fn heartbeat_reply_and_config_parsing() {
    let reply = parse_heartbeat_reply(
        r#"{"term":3,"role":"follower","leader_node_id":"b","leader_url":"http://b:1","lease_ms":5000}"#,
    )
    .unwrap();
    assert_eq!(reply, follower_reply_with(3, "b", "http://b:1"));
    assert!(parse_heartbeat_reply(r#"{"term":3,"role":"boss","lease_ms":1}"#).is_err());
    assert!(parse_heartbeat_reply("nope").is_err());
    assert!(
        FailoverConfig::new("http://cp".into(), "ftp://x".into(), "a".into(), 1, true).is_err()
    );
    assert!(
        FailoverConfig::new("http://cp".into(), "http://x".into(), "a,b".into(), 1, true).is_err()
    );
}

#[test]
fn a_follower_reports_its_cursor_and_recent_crossings() {
    let runtime = member(IngestionRuntime::in_memory(InMemoryStore::new()), "c", None);
    let mut c = runtime.lock().unwrap();
    c.failover.role = NodeRole::Follower;
    c.failover.term = 3;
    c.replication_follower.state_loaded = true;
    c.replication_follower.synced_once = true;
    c.replication_follower.generation = Some(7);
    c.replication_last_offset = 4;
    for (from, to) in [(5, 6), (6, 7)] {
        c.failover.record_switch(RecordedSwitch {
            from: store::WalPosition {
                generation: from,
                records: 30,
            },
            to,
            term: 3,
        });
    }
    let query = c.failover_heartbeat_query(&ReplicationPullConfig::new("http://unused"));
    assert!(query.contains("&generation=7&records=4"), "{query}");
    assert!(query.contains("&synced=1"), "{query}");
    assert!(
        query.contains("&fchain=5%3A30%3A6%3A3%2C6%3A30%3A7%3A3"),
        "{query}"
    );
    assert!(query.contains("role=follower"), "{query}");
}

fn follower_reply_with(term: u64, node: &str, url: &str) -> HeartbeatReply {
    HeartbeatReply {
        term,
        leader: false,
        leader_node_id: Some(node.to_string()),
        leader_url: Some(url.to_string()),
        lease: LEASE,
    }
}
