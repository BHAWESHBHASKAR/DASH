//! DATA-02 / DATA-06: WAL records must round-trip any accepted string and
//! preserve edge `reason_codes` / `created_at`.

use rand::Rng;
use schema::{Claim, ClaimEdge, Relation, claim_builder};
use store::{FileWal, InMemoryStore};
use tempfile::TempDir;

const CONTROLS: &[&str] = &["\t", "\n", "\r", "\r\n", "\0"];
const SAFE: &[&str] = &[
    "\\",
    "\\t",
    "\\n",
    "null",
    "crc=deadbeef",
    "a",
    "b",
    "1",
    ":",
    " ",
    "\u{e9}",
    "\u{65e5}",
    "\u{1f600}",
];

/// Free text may contain control characters; identifier-like fields may not.
fn random_string(rng: &mut impl Rng) -> String {
    random_from(rng, true)
}

fn random_identifier(rng: &mut impl Rng) -> String {
    random_from(rng, false)
}

fn random_from(rng: &mut impl Rng, allow_controls: bool) -> String {
    let mut pool: Vec<&str> = SAFE.to_vec();
    if allow_controls {
        pool.extend_from_slice(CONTROLS);
    }
    let pool_ref = &pool;
    let n = rng.gen_range(1..7);
    let mut s: String = (0..n)
        .map(|_| pool_ref[rng.gen_range(0..pool_ref.len())])
        .collect::<Vec<_>>()
        .concat();
    s.push('x'); // never empty
    s
}

#[test]
fn claims_and_edges_with_hostile_strings_survive_append_and_replay() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let mut rng = rand::thread_rng();
    let mut claims: Vec<Claim> = Vec::new();
    let mut wal = FileWal::open(&path).unwrap();
    for i in 0..300 {
        let mut c = claim_builder(
            &format!("id-{i}"),
            "tenant-a",
            &random_string(&mut rng),
            0.5,
        );
        c.entities = (0..rng.gen_range(0..3))
            .map(|_| random_identifier(&mut rng))
            .collect();
        c.embedding_ids = (0..rng.gen_range(0..3))
            .map(|_| random_identifier(&mut rng))
            .collect();
        wal.append_claim(&c).unwrap();
        claims.push(c);
    }
    drop(wal);

    let wal = FileWal::open(&path).unwrap();
    let store = InMemoryStore::load_from_wal(&wal).unwrap();
    for c in &claims {
        assert_eq!(store.claim_by_id(&c.claim_id), Some(c), "{c:?}");
    }
}

#[test]
fn claim_text_with_tabs_and_newlines_does_not_poison_replay() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let mut wal = FileWal::open(&path).unwrap();
    let mut c = claim_builder("c1", "tenant-a", "a\tb\nc\r\\d\r\n", 0.5);
    c.entities = vec!["a\\tb".to_string(), "c\\nd".to_string()];
    wal.append_claim(&c).unwrap();
    drop(wal);
    let wal = FileWal::open(&path).unwrap();
    let store = InMemoryStore::load_from_wal(&wal).unwrap();
    assert_eq!(store.claim_by_id("c1"), Some(&c));
}

#[test]
fn edge_reason_codes_and_created_at_survive_restart() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    let mut wal = FileWal::open(&path).unwrap();
    wal.append_claim(&claim_builder("a", "tenant-a", "alpha", 0.9))
        .unwrap();
    wal.append_claim(&claim_builder("b", "tenant-a", "beta", 0.9))
        .unwrap();
    let edge = ClaimEdge {
        edge_id: "g1".to_string(),
        from_claim_id: "a".to_string(),
        to_claim_id: "b".to_string(),
        relation: Relation::Contradicts,
        strength: 0.75,
        reason_codes: vec!["negation".to_string(), "back\\slash".to_string()],
        created_at: Some(1_700_000_123),
    };
    wal.append_edge(&edge).unwrap();
    drop(wal);

    let wal = FileWal::open(&path).unwrap();
    let store = InMemoryStore::load_from_wal(&wal).unwrap();
    assert_eq!(store.edges_for_claim("a"), vec![edge]);
}
