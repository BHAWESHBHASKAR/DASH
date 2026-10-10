//! Encryption at rest with a key manager (ADR 0005): a store whose data keys
//! are wrapped by Vault Transit (an in-process mock that encrypts for real)
//! writes its WAL, snapshot, redb mirror and vector index, is reopened by a
//! fresh keyring (no cached keys, so every data key comes back through
//! Vault) and reads the same state. A node that started on a local key file
//! migrates to Vault with the old key listed as previous, `rewrap`s the
//! remaining files, and then runs on Vault alone.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use schema::{Claim, Evidence, Stance, claim_builder};
use store::encryption::kms::mock::{MockVault, write_token};
use store::encryption::{
    self, FileFormat, Keyring, RewrapOutcome, keyring_from_lookup, with_keyring,
};
use store::{AnnTuningConfig, DiskBackedStore, FileWal, InMemoryStore, ReplayPolicy};
use tempfile::TempDir;

const TENANT: &str = "tenant-a";
const MARKER: &str = "kms-canary-41c9";

fn claim(id: &str, n: usize) -> Claim {
    claim_builder(id, TENANT, &format!("{MARKER} claim {id} number {n}"), 0.9)
}

fn evidence(id: &str, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: format!("source://{MARKER}/{id}"),
        stance: Stance::Supports,
        source_quality: 0.8,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn write(store: &mut InMemoryStore, wal: &mut FileWal, from: usize, count: usize) {
    for n in from..from + count {
        let id = format!("c{n}");
        store
            .ingest_atomic_persistent(
                wal,
                claim(&id, n),
                vec![evidence(&format!("e{n}"), &id)],
                vec![],
                Some(vec![0.25, (n % 5) as f32 / 5.0, 0.5, 1.0]),
                1_700_000_000_000 + n as u64,
            )
            .unwrap();
    }
}

fn state(store: &InMemoryStore) -> Vec<String> {
    let mut out: Vec<String> = store
        .claims_for_tenant(TENANT)
        .into_iter()
        .map(|c| format!("{}|{}", c.claim_id, c.canonical_text))
        .collect();
    out.sort();
    out
}

fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.is_file())
        .collect();
    out.sort();
    out
}

fn key_file(dir: &Path, byte: u8) -> String {
    let path = dir.join(format!("local-{byte}.key"));
    std::fs::write(&path, hex::encode([byte; 32])).unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    path.display().to_string()
}

fn keyring(settings: &[(&str, String)]) -> Arc<Keyring> {
    let map: HashMap<String, String> = settings
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect();
    keyring_from_lookup(&|name| map.get(name).cloned())
        .unwrap()
        .expect("encryption on")
}

fn vault_settings(vault: &MockVault, token: &Path) -> Vec<(&'static str, String)> {
    vec![
        (encryption::PROVIDER_ENV, "vault".to_string()),
        (encryption::VAULT_ADDR_ENV, vault.addr()),
        (
            encryption::VAULT_TOKEN_FILE_ENV,
            token.display().to_string(),
        ),
        (encryption::VAULT_KEY_ENV, "dash".to_string()),
    ]
}

/// Opens everything under `dir` with `keyring` and returns the state.
fn reopen(dir: &Path, keyring: Arc<Keyring>) -> Vec<String> {
    with_keyring(Some(keyring), || {
        let wal = FileWal::open(dir.join("dash.wal")).unwrap();
        let (store, stats) = InMemoryStore::load_from_wal_with_vector_index(
            &wal,
            AnnTuningConfig::default(),
            ReplayPolicy::Strict,
            Some(&dir.join("dash.vindex")),
        )
        .unwrap();
        assert!(stats.vector_index.is_loaded(), "{:?}", stats.vector_index);
        let disk = DiskBackedStore::new(dir.join("dash.redb")).unwrap();
        let mut from_disk = InMemoryStore::new();
        disk.bulk_load_claims_into(&mut from_disk).unwrap();
        assert_eq!(state(&from_disk), state(&store));
        state(&store)
    })
}

/// Writes, checkpoints, mirrors to redb and saves the vector index.
fn write_node(dir: &Path, keyring: Arc<Keyring>, from: usize, count: usize) -> Vec<String> {
    with_keyring(Some(keyring), || {
        let mut wal = FileWal::open(dir.join("dash.wal")).unwrap();
        let mut store = InMemoryStore::load_from_wal(&wal).unwrap();
        write(&mut store, &mut wal, from, count);
        store.checkpoint_and_compact(&mut wal).unwrap();
        write(&mut store, &mut wal, from + count, 3);
        let disk = DiskBackedStore::new(dir.join("dash.redb")).unwrap();
        disk.checkpoint_from(&store).unwrap();
        store
            .vector_index_snapshot(wal.position())
            .save(&dir.join("dash.vindex"))
            .unwrap();
        state(&store)
    })
}

fn assert_no_marker(dir: &Path) {
    for path in files_under(dir) {
        let bytes = std::fs::read(&path).unwrap();
        assert!(
            !bytes.windows(MARKER.len()).any(|w| w == MARKER.as_bytes()),
            "{} holds the marker in plaintext",
            path.display()
        );
    }
}

fn key_id_of(path: &Path) -> Option<String> {
    if path.extension().is_some_and(|e| e == "redb") {
        return DiskBackedStore::stored_encryption_key_id(path).unwrap();
    }
    match encryption::sniff_file(path).unwrap() {
        FileFormat::Plain => None,
        FileFormat::Lines(h) | FileFormat::Sealed(h) => Some(h.key_id),
    }
}

#[test]
fn a_vault_wrapped_store_round_trips_through_a_fresh_keyring() {
    let dir = TempDir::new().unwrap();
    let secrets = TempDir::new().unwrap();
    let vault = MockVault::start("s.e2e-token");
    let token = write_token(secrets.path(), "vault-token", "s.e2e-token");

    let writer = keyring(&vault_settings(&vault, &token));
    assert_eq!(writer.provider_name(), "vault");
    let expected = write_node(dir.path(), writer, 0, 20);
    assert_eq!(expected.len(), 23);

    // Every data file names the Vault key; nothing is plaintext.
    let mut encrypted = 0;
    for path in files_under(dir.path()) {
        if let Some(id) = key_id_of(&path) {
            assert_eq!(id, "vault:transit/dash", "{}", path.display());
            encrypted += 1;
        }
    }
    assert!(
        encrypted >= 4,
        "WAL, snapshot, redb and vector index: {encrypted}"
    );
    assert_no_marker(dir.path());

    // A fresh keyring (new process, empty cache) unwraps through Vault.
    let decrypts_before = vault.requests("decrypt");
    let reader = keyring(&vault_settings(&vault, &token));
    assert_eq!(reopen(dir.path(), reader), expected);
    assert!(vault.requests("decrypt") > decrypts_before + 1);

    // Rotating the key inside Vault keeps the files readable and new files
    // pick up the new key version.
    vault.rotate("dash");
    let rotated = keyring(&vault_settings(&vault, &token));
    let more = write_node(dir.path(), rotated, 100, 2);
    assert_eq!(more.len(), expected.len() + 5);
    assert_eq!(
        reopen(dir.path(), keyring(&vault_settings(&vault, &token))),
        more
    );

    // A Vault that refuses the token stops start-up (the start-up check).
    drop(vault);
    let map: HashMap<&str, String> = vault_settings(&MockVault::start("x"), &token)
        .into_iter()
        .collect();
    let err = keyring_from_lookup(&|n| map.get(n).cloned())
        .unwrap_err()
        .to_string();
    assert!(err.contains("start-up check"), "{err}");
}

#[test]
fn a_local_key_node_migrates_to_vault() {
    let dir = TempDir::new().unwrap();
    let secrets = TempDir::new().unwrap();
    let vault = MockVault::start("s.migrate");
    let token = write_token(secrets.path(), "vault-token", "s.migrate");
    let old_key = key_file(secrets.path(), 0x5a);

    // Day 1: local key file.
    let local = keyring(&[(encryption::KEY_FILE_ENV, old_key.clone())]);
    let local_id = local.active_key_id().to_string();
    let day1 = write_node(dir.path(), local, 0, 10);

    // Day 2: Vault active, the local key listed as previous. New writes go
    // to Vault; old files stay readable.
    let mut migrating = vault_settings(&vault, &token);
    migrating.push((encryption::PREVIOUS_KEY_FILES_ENV, old_key.clone()));
    let both = keyring(&migrating);
    assert_eq!(
        both.key_ids(),
        vec!["vault:transit/dash".to_string(), local_id.clone()]
    );
    assert_eq!(reopen(dir.path(), both.clone()), day1);

    // Rewrap what is left on the local key (what `wal-inspect rewrap` does).
    let mut moved = 0;
    for path in files_under(dir.path()) {
        if path.extension().is_some_and(|e| e == "redb") {
            if key_id_of(&path).as_deref() == Some(local_id.as_str()) {
                // Opening the redb mirror rewraps its data key.
                DiskBackedStore::new_with_keyring(&path, Some(both.clone())).unwrap();
                moved += 1;
            }
            continue;
        }
        if let RewrapOutcome::Rewrapped { from } = encryption::rewrap_file(&both, &path).unwrap() {
            assert_eq!(from, local_id);
            moved += 1;
        }
    }
    assert!(moved >= 3, "{moved}");
    for path in files_under(dir.path()) {
        if let Some(id) = key_id_of(&path) {
            assert_eq!(id, "vault:transit/dash", "{}", path.display());
        }
    }

    // Day 3: Vault alone, the local key gone.
    let vault_only = keyring(&vault_settings(&vault, &token));
    assert_eq!(vault_only.key_ids().len(), 1);
    assert_eq!(reopen(dir.path(), vault_only), day1);
    assert_no_marker(dir.path());
}
