//! Deterministic tests of the ingestion failover coordinator. Time is an
//! explicit millisecond counter: nothing sleeps.

use super::*;
use std::sync::atomic::{AtomicU64, Ordering};

const LEASE: u64 = 5_000;
const GRACE: u64 = 1_000;
const G1: u64 = 0x1111;
const G2: u64 = 0x2222;
const G3: u64 = 0x3333;

fn test_clock(start: u64) -> (Clock, Arc<AtomicU64>) {
    let now = Arc::new(AtomicU64::new(start));
    let reader = Arc::clone(&now);
    (Arc::new(move || reader.load(Ordering::SeqCst)), now)
}

fn coordinator(state_path: Option<PathBuf>) -> IngestFailover {
    let (clock, _) = test_clock(0);
    IngestFailover::new(
        IngestFailoverConfig {
            lease_ms: LEASE,
            grace_ms: GRACE,
            state_path,
        },
        clock,
    )
    .unwrap()
}

fn report(
    node: &str,
    term: u64,
    role: ReportedRole,
    position: Option<(u64, u64)>,
) -> HeartbeatReport {
    HeartbeatReport {
        node_id: node.to_string(),
        url: format!("http://{node}:8081"),
        instance_id: format!("{node}-i1"),
        term,
        role,
        position: position.map(|(generation, records)| Position {
            generation,
            records,
        }),
        prev: None,
        prev_term: None,
        follower_chain: Vec::new(),
        chain: Vec::new(),
        synced: true,
        bootstrap: false,
    }
}

fn bootstrap_report(node: &str, records: u64) -> HeartbeatReport {
    HeartbeatReport {
        bootstrap: true,
        synced: true,
        ..report(node, 0, ReportedRole::Unknown, Some((G1, records)))
    }
}

/// A cluster with leader `a` (term 1, lineage [G1]) and followers `b`, `c`.
fn running_cluster() -> IngestFailover {
    let mut f = coordinator(None);
    f.heartbeat(bootstrap_report("a", 0), 0).unwrap();
    let (_, promotion) = f.heartbeat(bootstrap_report("a", 0), LEASE).unwrap();
    assert_eq!(promotion.unwrap().term, 1);
    assert_eq!(f.leader_node_id(), Some("a"));
    leader_beat(&mut f, "a", 10, LEASE);
    f.heartbeat(
        report("b", 1, ReportedRole::Follower, Some((G1, 10))),
        LEASE,
    )
    .unwrap();
    f.heartbeat(
        report("c", 1, ReportedRole::Follower, Some((G1, 10))),
        LEASE,
    )
    .unwrap();
    f
}

/// Followers heartbeat shortly before the lease lapses (as they do every
/// second in production), so they count as live.
fn beat_followers(f: &mut IngestFailover, now: u64, members: &[(&str, u64)]) {
    for (node, records) in members {
        let term = f.term();
        f.heartbeat(
            report(node, term, ReportedRole::Follower, Some((G1, *records))),
            now,
        )
        .unwrap();
    }
}

fn leader_beat(f: &mut IngestFailover, node: &str, records: u64, now: u64) -> HeartbeatReply {
    let term = f.term();
    let (reply, promotion) = f
        .heartbeat(
            report(node, term, ReportedRole::Leader, Some((G1, records))),
            now,
        )
        .unwrap();
    assert!(promotion.is_none());
    reply
}

#[test]
fn bootstrap_waits_one_lease_then_picks_the_writer_with_the_most_data() {
    let mut f = coordinator(None);
    let (reply, promotion) = f.heartbeat(bootstrap_report("b", 0), 0).unwrap();
    assert!(promotion.is_none(), "no leader before the bootstrap wait");
    assert_eq!(reply.role, AssignedRole::Follower);
    assert!(reply.leader_node_id.is_none());
    f.heartbeat(bootstrap_report("a", 0), 100).unwrap();
    f.heartbeat(bootstrap_report("c", 7), 200).unwrap();
    // A non-bootstrap member (configured as a follower) is never chosen.
    let mut follower = bootstrap_report("d", 99);
    follower.bootstrap = false;
    f.heartbeat(follower, 300).unwrap();
    let (reply, promotion) = f.heartbeat(bootstrap_report("a", 0), LEASE).unwrap();
    let promotion = promotion.expect("bootstrap election");
    assert_eq!(promotion.new_leader, "c");
    assert_eq!(promotion.term, 1);
    assert!(promotion.old_leader.is_none());
    assert_eq!(reply.role, AssignedRole::Follower);
    assert_eq!(reply.leader_url.as_deref(), Some("http://c:8081"));
}

#[test]
fn bootstrap_ties_go_to_the_smallest_node_id() {
    let mut f = coordinator(None);
    for node in ["n3", "n1", "n2"] {
        f.heartbeat(bootstrap_report(node, 0), 0).unwrap();
    }
    let (reply, promotion) = f.heartbeat(bootstrap_report("n1", 0), LEASE).unwrap();
    assert_eq!(promotion.unwrap().new_leader, "n1");
    assert_eq!(reply.role, AssignedRole::Leader);
    assert_eq!(reply.term, 1);
    assert_eq!(reply.lease_ms, LEASE);
}

#[test]
fn a_renewing_leader_is_never_replaced() {
    let mut f = running_cluster();
    let mut now = LEASE;
    for _ in 0..50 {
        now += 1_000;
        let reply = leader_beat(&mut f, "a", 10, now);
        assert_eq!(reply.role, AssignedRole::Leader);
        let (reply, promotion) = f
            .heartbeat(report("b", 1, ReportedRole::Follower, Some((G1, 10))), now)
            .unwrap();
        assert!(promotion.is_none());
        assert_eq!(reply.role, AssignedRole::Follower);
        assert_eq!(reply.leader_node_id.as_deref(), Some("a"));
    }
    assert_eq!(f.term(), 1);
}

#[test]
fn dead_leader_is_replaced_by_the_most_up_to_date_follower_within_the_window() {
    let mut f = running_cluster();
    let last_renewal = LEASE + 1_000;
    leader_beat(&mut f, "a", 20, last_renewal);
    let lapse_at = last_renewal + LEASE + GRACE;
    // Followers keep heartbeating every second; the leader is silent.
    let mut now = last_renewal;
    let mut promoted_at = None;
    while now < lapse_at + 10_000 {
        now += 1_000;
        for (node, records) in [("b", 18), ("c", 20)] {
            let (_, promotion) = f
                .heartbeat(
                    report(node, 1, ReportedRole::Follower, Some((G1, records))),
                    now,
                )
                .unwrap();
            if let Some(promotion) = promotion {
                assert!(now > lapse_at, "promoted before the lease lapsed");
                assert_eq!(promotion.new_leader, "c", "highest position wins");
                assert_eq!(promotion.old_leader.as_deref(), Some("a"));
                assert_eq!(promotion.term, 2);
                promoted_at = Some(now);
            }
        }
        if promoted_at.is_some() {
            break;
        }
    }
    let promoted_at = promoted_at.expect("a follower must be promoted");
    // Detection window: lease + grace + one heartbeat interval for every
    // live member to report after the lapse.
    assert!(
        promoted_at - last_renewal <= LEASE + GRACE + 2 * 1_000,
        "failover took {} ms",
        promoted_at - last_renewal
    );
    let (reply, _) = f
        .heartbeat(
            report("c", 1, ReportedRole::Follower, Some((G1, 20))),
            promoted_at + 1,
        )
        .unwrap();
    assert_eq!(reply.role, AssignedRole::Leader);
    assert_eq!(reply.term, 2);
}

#[test]
fn promotion_waits_for_every_live_member_to_report_after_the_lapse() {
    // The synchronous-replication guarantee depends on this: `c` confirmed
    // the last acknowledged write, but its last report is older than `b`'s.
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 30, 6_000);
    beat_followers(&mut f, 11_500, &[("b", 20), ("c", 25)]);
    let lapse_at = 6_000 + LEASE + GRACE;
    let (_, promotion) = f
        .heartbeat(
            report("b", 1, ReportedRole::Follower, Some((G1, 28))),
            lapse_at + 10,
        )
        .unwrap();
    assert!(
        promotion.is_none(),
        "c is live and has not reported since the lapse"
    );
    assert_eq!(f.blocked(), Some(Blocked::WaitingForReports));
    let (_, promotion) = f
        .heartbeat(
            report("c", 1, ReportedRole::Follower, Some((G1, 30))),
            lapse_at + 20,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "c");
}

#[test]
fn a_member_that_died_too_stops_blocking_after_one_lease() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 30, 6_000);
    beat_followers(&mut f, 11_500, &[("b", 20), ("c", 30)]);
    let lapse_at = 6_000 + LEASE + GRACE;
    let (_, promotion) = f
        .heartbeat(
            report("b", 1, ReportedRole::Follower, Some((G1, 28))),
            lapse_at + 10,
        )
        .unwrap();
    assert!(promotion.is_none());
    // c never reports again: once it has been silent for a lease it no
    // longer counts as live and b is promoted.
    let (_, promotion) = f
        .heartbeat(
            report("b", 1, ReportedRole::Follower, Some((G1, 28))),
            11_500 + LEASE + 1,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "b");
}

#[test]
fn unsynced_wrong_term_and_unknown_generation_members_are_not_eligible() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 10, 6_000);
    let after = 6_000 + LEASE + GRACE + 1;
    let mut unsynced = report("b", 1, ReportedRole::Follower, Some((G1, 50)));
    unsynced.synced = false;
    f.heartbeat(unsynced, after).unwrap();
    let (_, promotion) = f
        .heartbeat(
            report("c", 1, ReportedRole::Follower, Some((G3, 99))),
            after,
        )
        .unwrap();
    assert!(promotion.is_none());
    assert_eq!(f.blocked(), Some(Blocked::NoEligibleCandidate));
    let (_, promotion) = f
        .heartbeat(
            report("c", 0, ReportedRole::Follower, Some((G1, 10))),
            after + 1,
        )
        .unwrap();
    assert!(
        promotion.is_none(),
        "a member of an older term is not eligible"
    );
    let (_, promotion) = f
        .heartbeat(
            report("c", 1, ReportedRole::Follower, Some((G1, 10))),
            after + 2,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "c");
}

#[test]
fn deposed_leader_that_returns_is_told_to_follow_the_new_leader() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 10, 6_000);
    beat_followers(&mut f, 11_500, &[("b", 10), ("c", 9)]);
    let after = 6_000 + LEASE + GRACE + 1;
    f.heartbeat(
        report("b", 1, ReportedRole::Follower, Some((G1, 10))),
        after,
    )
    .unwrap();
    let (_, promotion) = f
        .heartbeat(report("c", 1, ReportedRole::Follower, Some((G1, 9))), after)
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "b");
    // The old leader process comes back (same instance, old term).
    let (reply, promotion) = f
        .heartbeat(
            report("a", 1, ReportedRole::Leader, Some((G1, 12))),
            after + 100,
        )
        .unwrap();
    assert!(promotion.is_none());
    assert_eq!(reply.role, AssignedRole::Follower);
    assert_eq!(reply.term, 2);
    assert_eq!(reply.leader_node_id.as_deref(), Some("b"));
    assert_eq!(reply.leader_url.as_deref(), Some("http://b:8081"));
}

#[test]
fn restarted_leader_waits_for_its_old_lease_then_wins_with_the_most_data() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 40, 6_000);
    // New process for node a (new instance) while the old lease is held.
    let mut restarted = report("a", 1, ReportedRole::Unknown, Some((G1, 40)));
    restarted.instance_id = "a-i2".to_string();
    let (reply, _) = f.heartbeat(restarted.clone(), 6_500).unwrap();
    assert_eq!(
        reply.role,
        AssignedRole::Follower,
        "duplicate instance refused"
    );
    beat_followers(&mut f, 11_500, &[("b", 38), ("c", 39)]);
    f.heartbeat(restarted.clone(), 11_500).unwrap();
    let after = 6_000 + LEASE + GRACE + 1;
    f.heartbeat(
        report("b", 1, ReportedRole::Follower, Some((G1, 38))),
        after,
    )
    .unwrap();
    f.heartbeat(
        report("c", 1, ReportedRole::Follower, Some((G1, 39))),
        after,
    )
    .unwrap();
    let (reply, promotion) = f.heartbeat(restarted, after + 1).unwrap();
    let promotion = promotion.expect("re-election");
    assert_eq!(
        promotion.new_leader, "a",
        "the old leader's WAL holds the most"
    );
    assert_eq!(promotion.term, 2);
    assert_eq!(reply.role, AssignedRole::Leader);
}

#[test]
fn a_follower_ahead_of_the_new_leader_holds_a_divergent_history() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 10, 6_000);
    beat_followers(&mut f, 11_500, &[("b", 10)]);
    let after = 6_000 + LEASE + GRACE + 1;
    // c is live but not synced (say, resyncing), b is promoted at 10.
    let mut c = report("c", 1, ReportedRole::Follower, Some((G1, 12)));
    c.synced = false;
    f.heartbeat(c, after).unwrap();
    let (_, promotion) = f
        .heartbeat(
            report("b", 1, ReportedRole::Follower, Some((G1, 10))),
            after,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "b");
    // The new leader fenced G1 at 10 with a checkpoint into G2.
    let mut b = report("b", 2, ReportedRole::Leader, Some((G2, 0)));
    b.chain = vec![Transition {
        from: G1,
        records: 10,
        to: G2,
    }];
    f.heartbeat(b, after + 500).unwrap();
    assert!(
        f.rank(Position {
            generation: G1,
            records: 12
        })
        .is_none()
    );
    assert!(
        f.rank(Position {
            generation: G1,
            records: 10
        })
        .is_some()
    );
    assert!(
        f.rank(Position {
            generation: G2,
            records: 0
        }) > f.rank(Position {
            generation: G1,
            records: 10
        })
    );
}

#[test]
fn a_follower_extends_the_lineage_through_a_checkpoint_the_leader_never_reported() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 10, 6_000);
    beat_followers(&mut f, 11_500, &[("b", 15), ("c", 15)]);
    let after = 6_000 + LEASE + GRACE + 1;
    // The leader checkpointed G1 at 15 into G2 and died before reporting
    // it; c crossed the checkpoint and holds G2:3.
    let mut c = report("c", 1, ReportedRole::Follower, Some((G2, 3)));
    c.prev = Some(Position {
        generation: G1,
        records: 15,
    });
    c.prev_term = Some(1);
    f.heartbeat(c, after).unwrap();
    let (_, promotion) = f
        .heartbeat(
            report("b", 1, ReportedRole::Follower, Some((G1, 15))),
            after,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "c");
}

#[test]
fn step_down_stops_renewals_and_promotes_the_preferred_member() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 10, 6_000);
    assert!(f.step_down(Some("a".into())).is_err());
    assert_eq!(f.step_down(Some("c".into())).unwrap(), "a");
    let (reply, _) = f
        .heartbeat(report("a", 1, ReportedRole::Leader, Some((G1, 10))), 6_500)
        .unwrap();
    assert_eq!(reply.role, AssignedRole::Follower);
    assert!(
        reply.leader_url.is_none(),
        "a revoked leader must not follow itself"
    );
    beat_followers(&mut f, 11_500, &[("b", 10), ("c", 10)]);
    let after = 6_000 + LEASE + GRACE + 1;
    // The deposed node is excluded even though it holds the most.
    f.heartbeat(
        report("a", 1, ReportedRole::Follower, Some((G1, 10))),
        after,
    )
    .unwrap();
    f.heartbeat(
        report("b", 1, ReportedRole::Follower, Some((G1, 10))),
        after,
    )
    .unwrap();
    let (_, promotion) = f
        .heartbeat(
            report("c", 1, ReportedRole::Follower, Some((G1, 10))),
            after,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "c", "preferred wins the tie");
}

#[test]
fn term_and_lineage_survive_a_reload_and_terms_never_go_back() {
    let dir = tempfile_dir();
    let path = dir.join("ingest.failover");
    let mut f = coordinator(Some(path.clone()));
    f.heartbeat(bootstrap_report("a", 0), 0).unwrap();
    f.heartbeat(bootstrap_report("a", 0), LEASE).unwrap();
    assert_eq!(f.term(), 1);
    drop(f);
    let mut f = coordinator(Some(path.clone()));
    assert_eq!(f.term(), 1);
    assert_eq!(f.leader_node_id(), Some("a"));
    assert!(
        f.rank(Position {
            generation: G1,
            records: 0
        })
        .is_some()
    );
    // A node that saw a higher term (this record was lost or rolled back)
    // moves the term forward.
    f.heartbeat(report("z", 9, ReportedRole::Follower, None), 10)
        .unwrap();
    assert_eq!(f.term(), 9);
    drop(f);
    assert_eq!(coordinator(Some(path.clone())).term(), 9);
    std::fs::write(&path, "garbage\n").unwrap();
    assert!(
        IngestFailover::new(
            IngestFailoverConfig {
                state_path: Some(path),
                ..IngestFailoverConfig::default()
            },
            test_clock(0).0
        )
        .is_err()
    );
}

#[test]
fn the_leader_of_a_term_is_recognised_after_the_record_is_lost() {
    let mut f = coordinator(None);
    let (reply, _) = f
        .heartbeat(report("b", 4, ReportedRole::Leader, Some((G1, 5))), 0)
        .unwrap();
    assert_eq!(reply.role, AssignedRole::Leader);
    assert_eq!(reply.term, 4);
    let (reply, _) = f
        .heartbeat(report("a", 3, ReportedRole::Leader, Some((G1, 9))), 1)
        .unwrap();
    assert_eq!(reply.role, AssignedRole::Follower, "older term");
    assert_eq!(reply.leader_node_id.as_deref(), Some("b"));
}

#[test]
fn heartbeat_query_is_validated() {
    let query = |pairs: &[(&str, &str)]| {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect::<std::collections::HashMap<_, _>>()
    };
    let ok = parse_heartbeat_query(&query(&[
        ("node_id", "a"),
        ("url", "http://a:1"),
        ("instance", "x"),
        ("term", "3"),
        ("role", "leader"),
        ("generation", "255"),
        ("records", "7"),
        ("prev_generation", "238"),
        ("prev_records", "2"),
        ("synced", "1"),
    ]))
    .unwrap();
    assert_eq!(ok.term, 3);
    assert_eq!(
        ok.position,
        Some(Position {
            generation: 0xff,
            records: 7
        })
    );
    assert_eq!(
        ok.prev,
        Some(Position {
            generation: 0xee,
            records: 2
        })
    );
    assert!(ok.synced && !ok.bootstrap);
    for bad in [
        query(&[("url", "http://a:1"), ("instance", "x"), ("role", "leader")]),
        query(&[
            ("node_id", "a"),
            ("url", "http://a:1"),
            ("instance", "x"),
            ("role", "boss"),
        ]),
        query(&[
            ("node_id", "a"),
            ("url", "http://a:1"),
            ("instance", "x"),
            ("role", "leader"),
            ("term", "-1"),
        ]),
    ] {
        assert!(parse_heartbeat_query(&bad).is_err());
    }
    let mut f = coordinator(None);
    let mut bad_url = report("a", 0, ReportedRole::Unknown, None);
    bad_url.url = "ftp://a".into();
    assert!(f.heartbeat(bad_url, 0).is_err());
    let mut bad_id = report("a,b", 0, ReportedRole::Unknown, None);
    bad_id.url = "http://a:1".into();
    assert!(f.heartbeat(bad_id, 0).is_err());
}

#[test]
fn heartbeat_endpoint_promotes_and_moves_the_placement_atomically() {
    use crate::{ControlPlanePlacementState, handle_http_request_bytes};
    use metadata_router::{ReplicaHealth, ReplicaPlacement, ReplicaRole, ShardPlacement};
    use std::sync::Mutex;

    let (clock, now) = test_clock(0);
    let dir = tempfile_dir();
    let placements = vec![ShardPlacement {
        tenant_id: "tenant-a".into(),
        shard_id: 0,
        epoch: 3,
        replicas: ["a", "b"]
            .iter()
            .enumerate()
            .map(|(i, node)| ReplicaPlacement {
                node_id: node.to_string(),
                role: if i == 0 {
                    ReplicaRole::Leader
                } else {
                    ReplicaRole::Follower
                },
                health: ReplicaHealth::Healthy,
            })
            .collect(),
    }];
    let failover = IngestFailover::new(
        IngestFailoverConfig {
            lease_ms: LEASE,
            grace_ms: GRACE,
            state_path: Some(dir.join("ingest.failover")),
        },
        clock,
    )
    .unwrap();
    let state = Arc::new(Mutex::new(
        ControlPlanePlacementState::new(placements)
            .with_auth_token("tok")
            .with_ingest_failover(failover),
    ));
    let beat = |query: &str| {
        let raw = format!(
            "POST /v1/control-plane/ingest/heartbeat?{query} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok\r\nContent-Length: 0\r\n\r\n"
        );
        String::from_utf8(handle_http_request_bytes(&state, raw.as_bytes()).unwrap()).unwrap()
    };
    let base = |node: &str, term: u64, role: &str, records: u64| {
        format!(
            "node_id={node}&url=http://{node}:1&instance={node}-1&term={term}&role={role}&generation=1111&records={records}&synced=1&bootstrap=1"
        )
    };
    assert!(beat(&base("a", 0, "unknown", 0)).contains("\"role\":\"follower\""));
    now.store(LEASE, Ordering::SeqCst);
    let reply = beat(&base("a", 0, "unknown", 0));
    assert!(reply.contains("\"role\":\"leader\""), "{reply}");
    assert!(reply.contains("\"term\":1"), "{reply}");
    assert!(reply.contains("X-Dash-Leader: true"), "{reply}");
    beat(&base("b", 1, "follower", 0));
    now.store(LEASE + 500, Ordering::SeqCst);
    beat(&base("a", 1, "leader", 4));
    beat(&base("b", 1, "follower", 4));
    // a dies; b reports after the lapse and is promoted.
    now.store(LEASE + 500 + LEASE + GRACE + 1, Ordering::SeqCst);
    let reply = beat(&base("b", 1, "follower", 4));
    assert!(reply.contains("\"role\":\"leader\""), "{reply}");
    assert!(reply.contains("\"term\":2"), "{reply}");
    {
        let guard = state.lock().unwrap();
        let placement = &guard.placements()[0];
        assert_eq!(placement.epoch, 4, "placement epoch bumped");
        let leader = placement
            .replicas
            .iter()
            .find(|r| r.role == ReplicaRole::Leader)
            .unwrap();
        assert_eq!(leader.node_id, "b");
    }
    let raw =
        "GET /v1/control-plane/ingest HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok\r\n\r\n";
    let status =
        String::from_utf8(handle_http_request_bytes(&state, raw.as_bytes()).unwrap()).unwrap();
    assert!(status.contains("\"leader_node_id\":\"b\""), "{status}");
    let raw = "GET /metrics HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok\r\n\r\n";
    let metrics =
        String::from_utf8(handle_http_request_bytes(&state, raw.as_bytes()).unwrap()).unwrap();
    assert!(
        metrics.contains("dash_control_plane_ingest_term 2"),
        "{metrics}"
    );
    assert!(
        metrics.contains("dash_control_plane_ingest_promotions_total"),
        "{metrics}"
    );
    // Unauthenticated heartbeats are refused.
    let raw = format!(
        "POST /v1/control-plane/ingest/heartbeat?{} HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n",
        base("b", 2, "leader", 4)
    );
    let refused =
        String::from_utf8(handle_http_request_bytes(&state, raw.as_bytes()).unwrap()).unwrap();
    assert!(refused.starts_with("HTTP/1.1 401"), "{refused}");
}

#[test]
fn heartbeat_endpoint_is_absent_when_failover_is_disabled() {
    use crate::{ControlPlanePlacementState, handle_http_request_bytes};
    use std::sync::Mutex;
    let state = Arc::new(Mutex::new(
        ControlPlanePlacementState::new(Vec::new()).with_auth_token("tok"),
    ));
    let raw = "POST /v1/control-plane/ingest/heartbeat?node_id=a&url=http://a:1&instance=i&role=unknown HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer tok\r\nContent-Length: 0\r\n\r\n";
    let reply =
        String::from_utf8(handle_http_request_bytes(&state, raw.as_bytes()).unwrap()).unwrap();
    assert!(reply.starts_with("HTTP/1.1 404"), "{reply}");
}

fn tempfile_dir() -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "dash-ingest-failover-{}-{nanos}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn heartbeat_values_are_percent_decoded() {
    let query = [
        ("node_id", "n1"),
        ("url", "http%3A%2F%2F127.0.0.1%3A8081"),
        ("instance", "abc"),
        ("role", "unknown"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect::<std::collections::HashMap<_, _>>();
    let report = parse_heartbeat_query(&query).unwrap();
    assert_eq!(report.url, "http://127.0.0.1:8081");
    assert_eq!(percent_decode("a%2"), "a%2", "a truncated escape is kept");
    assert_eq!(percent_decode("%zz"), "%zz");
}

#[test]
fn a_named_leader_that_never_takes_over_is_replaced_after_its_lease() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 10, 6_000);
    beat_followers(&mut f, 11_500, &[("b", 10), ("c", 9)]);
    let after = 6_000 + LEASE + GRACE + 1;
    f.heartbeat(report("c", 1, ReportedRole::Follower, Some((G1, 9))), after)
        .unwrap();
    let (_, promotion) = f
        .heartbeat(
            report("b", 1, ReportedRole::Follower, Some((G1, 10))),
            after,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "b");
    // b cannot take over (say its WAL no longer matches its cursor): it
    // keeps reporting as an unsynced follower, which is not a renewal.
    let mut now = after;
    let mut replaced = None;
    while now < after + 3 * (LEASE + GRACE) {
        now += 1_000;
        let mut b = report("b", 2, ReportedRole::Follower, Some((G1, 10)));
        b.synced = false;
        let (reply, _) = f.heartbeat(b, now).unwrap();
        if f.term() == 2 {
            assert_eq!(
                reply.role,
                AssignedRole::Leader,
                "still named until replaced"
            );
        }
        let (_, promotion) = f
            .heartbeat(report("c", 2, ReportedRole::Follower, Some((G1, 9))), now)
            .unwrap();
        if let Some(promotion) = promotion {
            replaced = Some((promotion, now));
            break;
        }
    }
    let (promotion, at) = replaced.expect("b must be replaced");
    assert_eq!(promotion.new_leader, "c");
    assert_eq!(promotion.term, 3);
    assert!(at > after + LEASE + GRACE, "not before b's lease lapsed");
}

#[test]
fn checkpoints_between_two_leader_reports_are_learned_from_its_chain() {
    let mut f = running_cluster();
    // The leader checkpointed twice (G1 at 30 -> G2, G2 at 30 -> G3) since
    // its last report; c is still inside G2.
    let mut a = report("a", 1, ReportedRole::Leader, Some((G3, 5)));
    a.chain = vec![
        Transition {
            from: G1,
            records: 30,
            to: G2,
        },
        Transition {
            from: G2,
            records: 30,
            to: G3,
        },
    ];
    f.heartbeat(a, 6_000).unwrap();
    let g1 = f.rank(Position {
        generation: G1,
        records: 30,
    });
    let g2 = f.rank(Position {
        generation: G2,
        records: 20,
    });
    let g3 = f.rank(Position {
        generation: G3,
        records: 1,
    });
    assert!(g1.is_some() && g1 < g2 && g2 < g3, "{g1:?} {g2:?} {g3:?}");
    assert!(
        f.rank(Position {
            generation: G1,
            records: 31
        })
        .is_none()
    );
    beat_followers(&mut f, 11_500, &[("b", 30)]);
    f.heartbeat(
        report("c", 1, ReportedRole::Follower, Some((G2, 20))),
        11_500,
    )
    .unwrap();
    let after = 6_000 + LEASE + GRACE + 1;
    f.heartbeat(
        report("b", 1, ReportedRole::Follower, Some((G1, 30))),
        after,
    )
    .unwrap();
    let (_, promotion) = f
        .heartbeat(
            report("c", 1, ReportedRole::Follower, Some((G2, 20))),
            after,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "c", "G2 is ahead of G1");
}

#[test]
fn an_old_leaders_checkpoint_never_extends_the_new_leaders_lineage() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 10, 6_000);
    beat_followers(&mut f, 11_500, &[("b", 10), ("c", 8)]);
    let after = 6_000 + LEASE + GRACE + 1;
    f.heartbeat(report("c", 1, ReportedRole::Follower, Some((G1, 8))), after)
        .unwrap();
    let (_, promotion) = f
        .heartbeat(
            report("b", 1, ReportedRole::Follower, Some((G1, 10))),
            after,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "b");
    // c kept pulling from the old leader, crossed its checkpoint at 12 into
    // GX and only then learned term 2.
    const GX: u64 = 0x9999;
    let mut c = report("c", 2, ReportedRole::Follower, Some((GX, 3)));
    c.prev = Some(Position {
        generation: G1,
        records: 12,
    });
    c.prev_term = Some(1);
    f.heartbeat(c, after + 100).unwrap();
    assert!(
        f.rank(Position {
            generation: GX,
            records: 3
        })
        .is_none(),
        "a crossing served by the old leader is a divergent history"
    );
    // A crossing served by the new leader extends it, even before the new
    // leader reported (its fencing checkpoint at 12 >= its position 10).
    let mut c = report("c", 2, ReportedRole::Follower, Some((G2, 0)));
    c.prev = Some(Position {
        generation: G1,
        records: 12,
    });
    c.prev_term = Some(2);
    f.heartbeat(c, after + 200).unwrap();
    assert!(
        f.rank(Position {
            generation: G2,
            records: 0
        })
        .is_some()
    );
    assert!(
        f.rank(Position {
            generation: G1,
            records: 12
        })
        .is_some()
    );
    assert!(
        f.rank(Position {
            generation: G1,
            records: 13
        })
        .is_none()
    );
}

#[test]
fn a_follower_reports_several_crossings_the_dead_leader_never_reported() {
    let mut f = running_cluster();
    leader_beat(&mut f, "a", 10, 6_000);
    beat_followers(&mut f, 11_500, &[("b", 30), ("c", 30)]);
    let after = 6_000 + LEASE + GRACE + 1;
    // The leader checkpointed G1 -> G2 -> G3 within its last heartbeat
    // interval and died; c crossed both checkpoints, b stayed in G1.
    let mut c = report("c", 1, ReportedRole::Follower, Some((G3, 4)));
    c.follower_chain = vec![
        (
            Transition {
                from: G1,
                records: 30,
                to: G2,
            },
            1,
        ),
        (
            Transition {
                from: G2,
                records: 30,
                to: G3,
            },
            1,
        ),
    ];
    f.heartbeat(c, after).unwrap();
    let (_, promotion) = f
        .heartbeat(
            report("b", 1, ReportedRole::Follower, Some((G1, 30))),
            after,
        )
        .unwrap();
    assert_eq!(promotion.unwrap().new_leader, "c", "c holds the most");
}
