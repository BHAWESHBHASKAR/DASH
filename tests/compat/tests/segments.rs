//! Segment directory compatibility (services/indexer): manifests and
//! segment files (`DASHSEG-MANIFEST 1`, `DASHSEG 1`) written by every
//! release load and verify with the current reader; the maintenance pass
//! accepts an old root; a legacy tenant directory whose name the current
//! mapping changes (0.2 had no tenant marker) is left untouched and the
//! tenant is republished into its new directory.

mod support;

use std::collections::BTreeSet;
use std::fs;
use std::time::Duration;

use dash_compat::{Era, FIXTURES};
use indexer::{
    CompactionSchedulerConfig, SegmentPublishOptions, legacy_tenant_dir_name, load_current_segments,
    maintain_segment_root_strict, publish_claims_to_dir, read_tenant_marker, resolve_tenant_dir,
    tenant_dir_name,
};
use support::load_strict;

#[test]
fn every_old_manifest_and_segment_file_verifies() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let (store, _, _) = load_strict(&state);
        let mut tenant_dirs = 0;
        for entry in fs::read_dir(state.segments()).expect("segments root") {
            let dir = entry.expect("entry").path();
            let (manifest, segments) = load_current_segments(&dir)
                .expect("manifest and segments verify")
                .expect("manifest present");
            // An empty manifest is what a tenant delete publishes.
            let tenant_has_claims = match read_tenant_marker(&dir).expect("marker read") {
                Some(tenant) => !store.claims_for_tenant(&tenant).is_empty(),
                None => true,
            };
            assert_eq!(
                manifest.entries.is_empty(),
                !tenant_has_claims,
                "{}: {}",
                fixture.label,
                dir.display()
            );
            tenant_dirs += 1;
            let in_segments: BTreeSet<String> =
                segments.iter().flat_map(|s| s.claim_ids.iter().cloned()).collect();
            // Every segment claim id is a claim the store knows, unless the
            // tenant was deleted after the publish (0.3 republishes the
            // tenant's segments on delete, so the set stays consistent).
            let known: BTreeSet<String> = store
                .tenant_ids()
                .iter()
                .flat_map(|t| store.claims_for_tenant(t))
                .map(|c| c.claim_id)
                .collect();
            assert!(
                in_segments.is_subset(&known),
                "{}: {} lists claims the store does not hold: {:?}",
                fixture.label,
                dir.display(),
                in_segments.difference(&known).collect::<Vec<_>>()
            );
        }
        assert!(tenant_dirs >= 2, "{}", fixture.label);
    }
}

#[test]
fn the_maintenance_pass_accepts_an_old_segment_root() {
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let stats = maintain_segment_root_strict(&state.segments(), Duration::from_secs(3600))
            .expect("maintenance over an old root");
        assert!(stats.tenant_manifests_found >= 2, "{}: {stats:?}", fixture.label);
    }
}

/// `tenant_b` was stored by 0.2 in `tenant_b/` (lossy sanitizer). The current
/// mapping is `tenant_5fb/`. 0.2 wrote no tenant marker, so the old
/// directory's owner is ambiguous: it is left untouched (segments are
/// derived data) and the next publish writes the new directory, which the
/// reader then uses.
#[test]
fn a_legacy_tenant_directory_is_left_alone_and_the_tenant_republished() {
    let tenant = "tenant_b";
    assert_eq!(legacy_tenant_dir_name(tenant), "tenant_b");
    assert_eq!(tenant_dir_name(tenant), "tenant_5fb");
    for fixture in FIXTURES {
        let state = fixture.scratch_state();
        let root = state.segments();
        let legacy = root.join(legacy_tenant_dir_name(tenant));
        match fixture.era {
            Era::V0_2 => {
                assert!(legacy.is_dir(), "{}", fixture.label);
                assert_eq!(read_tenant_marker(&legacy).expect("marker read"), None);
            }
            Era::V0_3 => {
                assert!(!legacy.exists(), "{}", fixture.label);
                let dir = root.join(tenant_dir_name(tenant));
                assert_eq!(
                    read_tenant_marker(&dir).expect("marker").as_deref(),
                    Some(tenant),
                    "{}",
                    fixture.label
                );
            }
        }
        let legacy_before = list_files(&legacy);
        let resolved = resolve_tenant_dir(&root, tenant);
        assert_eq!(resolved, root.join("tenant_5fb"), "{}", fixture.label);
        let (store, _, _) = load_strict(&state);
        let claims = store.claims_for_tenant(tenant);
        let options = SegmentPublishOptions {
            max_segment_size: 10_000,
            scheduler: CompactionSchedulerConfig::default(),
            prune_grace: Duration::from_secs(60),
        };
        publish_claims_to_dir(&resolved, &claims, &options).expect("publish");
        let (_, segments) = load_current_segments(&resolved)
            .expect("verify")
            .expect("manifest");
        let ids: BTreeSet<String> = segments.into_iter().flat_map(|s| s.claim_ids).collect();
        let want: BTreeSet<String> = claims.into_iter().map(|c| c.claim_id).collect();
        assert_eq!(ids, want, "{}", fixture.label);
        assert_eq!(list_files(&legacy), legacy_before, "{}: legacy dir untouched", fixture.label);
    }
}

fn list_files(dir: &std::path::Path) -> Vec<(String, Vec<u8>)> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut out: Vec<(String, Vec<u8>)> = entries
        .map(|e| e.expect("entry").path())
        .map(|p| {
            (
                p.file_name().expect("name").to_string_lossy().to_string(),
                fs::read(&p).expect("read"),
            )
        })
        .collect();
    out.sort();
    out
}
