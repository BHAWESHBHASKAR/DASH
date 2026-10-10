//! Control-plane compatibility: the lease file and the persisted placement
//! state written by every release are read by the current control plane;
//! fencing tokens keep increasing across the upgrade; a lease written by the
//! current code cannot be read by 0.2 (the documented reason not to run
//! mixed control-plane versions on one lease path, and to delete the lease
//! files when rolling back).

use std::fs;

use control_plane::ControlPlanePlacementState;
use control_plane::leader::LeaderLease;
use dash_compat::{Era, FIXTURES, copy_tree, old_readers, repo_root};

#[test]
fn the_current_control_plane_takes_over_an_old_lease_with_a_higher_fencing_token() {
    for fixture in FIXTURES {
        let dir = tempfile::tempdir().expect("tempdir");
        copy_tree(&fixture.path("control-plane"), dir.path());
        let lease_path = dir.path().join("leader.lease");
        let old_text = fs::read_to_string(&lease_path).expect("lease");
        let lease = LeaderLease::with_defaults("cp-new-1", &lease_path);
        // The fixture's lease expired long ago: it parses and names no
        // current leader.
        assert_eq!(lease.current_leader().expect("old lease parses"), None, "{}", fixture.label);
        let fields: Vec<&str> = old_text.trim().split(',').collect();
        assert_eq!(fields[0], "cp-old-1", "{}", fixture.label);
        let old_epoch: u64 = fields[1].parse().expect("epoch");
        match fixture.era {
            Era::V0_2 => {
                assert_eq!(fields.len(), 3, "{}: node_id,epoch,expires_at_ms", fixture.label);
                assert!(old_readers::v0_2_reads_lease(&old_text).is_ok());
            }
            Era::V0_3 => assert_eq!(fields.len(), 4, "{}: plus instance_id", fixture.label),
        }
        // The fixture's lease expired long ago: the new node takes over.
        let acquired = lease
            .acquire()
            .expect("acquire")
            .expect("expired lease is taken over");
        assert!(acquired.newly_acquired, "{}", fixture.label);
        assert!(
            acquired.record.epoch > old_epoch,
            "{}: fencing token {} must exceed the old {}",
            fixture.label,
            acquired.record.epoch,
            old_epoch
        );
        assert!(lease.is_leader().expect("is_leader"), "{}", fixture.label);
        // What the current code wrote is unreadable for 0.2.
        let new_text = fs::read_to_string(&lease_path).expect("lease");
        assert!(
            old_readers::v0_2_reads_lease(&new_text).is_err(),
            "{}: {new_text}",
            fixture.label
        );
    }
    let guide = fs::read_to_string(repo_root().join("docs/operations/upgrades.md")).expect("guide");
    assert!(guide.contains("leader.lease.epoch"), "the guide names the lease files to remove");
}

#[test]
fn persisted_placement_state_loads_unchanged() {
    for fixture in FIXTURES {
        let placements = ControlPlanePlacementState::load_persisted_csv(
            &fixture.path("control-plane/placement-state.csv"),
        )
        .expect("placement state parses");
        let recorded = fixture.read_jsonl("control-plane/responses.jsonl");
        let served = recorded
            .iter()
            .find(|row| row["route"] == "GET /v1/control-plane/placement")
            .expect("placement response");
        let served = served["body"]["placements"].as_array().expect("placements");
        assert_eq!(placements.len(), served.len(), "{}", fixture.label);
        for (loaded, old) in placements.iter().zip(served) {
            assert_eq!(loaded.tenant_id, old["tenant_id"].as_str().unwrap_or(""), "{}", fixture.label);
            assert_eq!(u64::from(loaded.shard_id), old["shard_id"].as_u64().unwrap_or(u64::MAX));
            assert_eq!(loaded.epoch, old["epoch"].as_u64().unwrap_or(u64::MAX));
            assert_eq!(
                loaded.replicas.len(),
                old["replicas"].as_array().map_or(0, Vec::len),
                "{}",
                fixture.label
            );
        }
    }
}
