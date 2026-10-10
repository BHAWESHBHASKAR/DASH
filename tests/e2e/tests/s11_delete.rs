//! Scenario 11: deletes on the leader replicate to the retrieval follower,
//! survive a crash of either process, and tenant erasure needs admin.

use std::time::Duration;

use dash_e2e::*;

const WAIT: Duration = Duration::from_secs(30);

fn delete(s: &Stack, target: &str, key: &str) -> Resp {
    s.ic()
        .request("DELETE", target, &[("x-api-key", key)], None)
        .unwrap_or_else(|e| panic!("DELETE {target} failed: {e}"))
}

fn ids(s: &Stack, tenant: &str) -> Vec<String> {
    let mut out: Vec<String> = s
        .claim_evidence_map(tenant, "erasable statement", 100)
        .into_keys()
        .collect();
    out.sort();
    out
}

#[test]
fn deletes_replicate_survive_crashes_and_erasure_needs_admin() {
    let mut s = Stack::new(StackOpts::default());
    s.start_all();
    for id in ["a1", "a2", "a3"] {
        let r = s.ingest_as("tenant-a", &bundle("tenant-a", id, "erasable statement", 2));
        assert_eq!(r.status, 200, "{}", r.body);
    }
    let r = s.ingest_as(
        "tenant-b",
        &bundle("tenant-b", "b1", "erasable statement", 1),
    );
    assert_eq!(r.status, 200, "{}", r.body);
    s.wait_caught_up(WAIT);
    assert_eq!(ids(&s, "tenant-a"), ["a1", "a2", "a3"]);

    let key_a = s.ingest_keys["tenant-a"].clone();
    let key_b = s.ingest_keys["tenant-b"].clone();

    // Another tenant's key cannot delete; the owner's can, idempotently.
    let r = delete(&s, "/v1/claims/a1?tenant_id=tenant-a", &key_b);
    assert_eq!(r.status, 403, "{}", r.body);
    let r = delete(&s, "/v1/claims/a1?tenant_id=tenant-a", &key_a);
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.json()["deleted"], true);
    assert_eq!(r.json()["evidence_deleted"], 2);
    let r = delete(&s, "/v1/claims/a1?tenant_id=tenant-a", &key_a);
    assert_eq!((r.status, r.json()["deleted"].clone()), (200, false.into()));
    let r = delete(&s, "/v1/evidence/a2-e0?tenant_id=tenant-a", &key_a);
    assert_eq!(r.json()["evidence_deleted"], 1, "{}", r.body);

    s.wait_caught_up(WAIT);
    assert_eq!(ids(&s, "tenant-a"), ["a2", "a3"]);
    let map = s.claim_evidence_map("tenant-a", "erasable statement", 100);
    assert_eq!(map["a2"], ["a2-e1"]);

    // Crash both processes: the leader replays its tombstones, the follower
    // its replicated copy.
    s.restart_ingest(false);
    s.restart_retrieval(false);
    s.wait_caught_up(WAIT);
    assert_eq!(ids(&s, "tenant-a"), ["a2", "a3"]);
    assert_eq!(
        s.claim_evidence_map("tenant-a", "erasable statement", 100)["a2"],
        ["a2-e1"]
    );

    // Erasure: a tenant ingest key is not enough, the admin credential is.
    let r = delete(&s, "/v1/tenants/tenant-a", &key_a);
    assert_eq!(r.status, 403, "{}", r.body);
    let ops = s.ops_key.clone();
    let r = delete(&s, "/v1/tenants/tenant-a", &ops);
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.json()["claims_deleted"], 2);
    s.wait_caught_up(WAIT);
    assert!(ids(&s, "tenant-a").is_empty());
    assert_eq!(ids(&s, "tenant-b"), ["b1"]);

    s.restart_ingest(true);
    s.restart_retrieval(true);
    s.wait_caught_up(WAIT);
    assert!(ids(&s, "tenant-a").is_empty());
    assert_eq!(ids(&s, "tenant-b"), ["b1"]);
    // Without a checkpoint the log still holds the original records, followed
    // by the tombstones that undo them.
    let export = s.ic().get(
        "/internal/replication/export",
        &[("x-replication-token", s.replication_token.as_str())],
    );
    assert!(
        export.body.contains("T2\ttenant\ttenant-a\t"),
        "{}",
        export.body
    );
}

/// With a checkpoint after the delete, the erased tenant's records are gone
/// from the leader's log and snapshot, and the follower resyncs to the same.
#[test]
fn a_checkpoint_after_erasure_drops_the_tenant_from_the_log() {
    let mut s = Stack::new(StackOpts {
        checkpoint_every: Some(1),
        ..StackOpts::default()
    });
    s.start_all();
    for (tenant, id) in [("tenant-a", "a1"), ("tenant-a", "a2"), ("tenant-b", "b1")] {
        let r = s.ingest_as(tenant, &bundle(tenant, id, "erasable statement", 1));
        assert_eq!(r.status, 200, "{}", r.body);
    }
    s.wait_caught_up(WAIT);
    assert_eq!(ids(&s, "tenant-a"), ["a1", "a2"]);

    let ops = s.ops_key.clone();
    let r = delete(&s, "/v1/tenants/tenant-a", &ops);
    assert_eq!(r.status, 200, "{}", r.body);
    assert_eq!(r.json()["checkpoint_triggered"], true, "{}", r.body);
    s.wait_caught_up(WAIT);
    assert!(ids(&s, "tenant-a").is_empty());
    assert_eq!(ids(&s, "tenant-b"), ["b1"]);

    let state = s.leader_state();
    assert_eq!(state.claims.keys().cloned().collect::<Vec<_>>(), ["b1"]);
    let export = s.ic().get(
        "/internal/replication/export",
        &[("x-replication-token", s.replication_token.as_str())],
    );
    assert!(!export.body.contains("tenant-a"), "{}", export.body);
}
