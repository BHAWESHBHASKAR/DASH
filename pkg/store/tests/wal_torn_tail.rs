//! DATA-07: torn-tail tolerance and per-record checksums.

use schema::{Claim, ClaimEdge, Relation, claim_builder};
use store::{FileWal, InMemoryStore};
use tempfile::TempDir;

fn claim(id: &str, text: &str) -> Claim {
    let mut c = claim_builder(id, "tenant-a", text, 0.9);
    c.entities = vec!["Acme\\Corp".to_string(), "line\\nbreak".to_string()];
    c.created_at = Some(1_700_000_000);
    c
}

fn edge(id: &str, from: &str, to: &str) -> ClaimEdge {
    ClaimEdge {
        edge_id: id.to_string(),
        from_claim_id: from.to_string(),
        to_claim_id: to.to_string(),
        relation: Relation::Supports,
        strength: 0.5,
        reason_codes: vec!["because".to_string()],
        created_at: Some(42),
    }
}

fn write_reference_wal(path: &std::path::Path) -> usize {
    let mut wal = FileWal::open(path).unwrap();
    wal.append_claim(&claim("c1", "first claim")).unwrap();
    wal.append_claim(&claim("c2", "second\tclaim\nwith controls"))
        .unwrap();
    wal.append_edge(&edge("g1", "c1", "c2")).unwrap();
    wal.append_claim(&claim("c3", "third claim 123456"))
        .unwrap();
    wal.append_batch_commit(
        "commit-1",
        3,
        1_700_000_000_000,
        &["c1".into(), "c2".into()],
    )
    .unwrap();
    wal.flush_pending_sync().unwrap();
    5
}

#[test]
fn truncating_the_wal_at_every_byte_offset_never_panics_or_invents_records() {
    let dir = TempDir::new().unwrap();
    let full_path = dir.path().join("full.wal");
    let total = write_reference_wal(&full_path);
    let bytes = std::fs::read(&full_path).unwrap();
    let full_lines: Vec<String> = String::from_utf8(bytes.clone())
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    assert_eq!(full_lines.len(), total);

    // End offset (exclusive, newline NOT included) of each record.
    let mut ends = Vec::new();
    let mut pos = 0usize;
    for line in &full_lines {
        pos += line.len();
        ends.push(pos);
        pos += 1;
    }

    for cut in 0..=bytes.len() {
        let case = dir.path().join(format!("cut-{cut}"));
        std::fs::create_dir_all(&case).unwrap();
        let wal_path = case.join("dash.wal");
        std::fs::write(&wal_path, &bytes[..cut]).unwrap();

        let mut wal = FileWal::open(&wal_path)
            .unwrap_or_else(|e| panic!("open must tolerate a tail cut at {cut}: {e:?}"));
        let kept = wal.replication_export().unwrap().wal_lines;

        // Records fully written (terminator may be missing only for the last).
        let complete_with_newline = ends.iter().filter(|&&e| e < cut).count();
        let complete_without_newline = ends.iter().filter(|&&e| e <= cut).count();
        assert!(
            kept.len() == complete_with_newline || kept.len() == complete_without_newline,
            "cut {cut}: kept {} records, expected {complete_with_newline} or {complete_without_newline}",
            kept.len()
        );
        assert_eq!(kept, full_lines[..kept.len()].to_vec(), "cut {cut}");

        // Replay must succeed and the log must remain appendable.
        InMemoryStore::load_from_wal(&wal)
            .unwrap_or_else(|e| panic!("replay must succeed after cut {cut}: {e:?}"));
        let before = kept.len();
        wal.append_claim(&claim("late", "after repair")).unwrap();
        wal.flush_pending_sync().unwrap();
        drop(wal);
        let mut reopened = FileWal::open(&wal_path).unwrap();
        let again = reopened.replication_export().unwrap().wal_lines;
        assert_eq!(again.len(), before + 1, "cut {cut}");
        assert_eq!(again[..before].to_vec(), kept, "cut {cut}");
    }
}

#[test]
fn torn_tail_is_counted_and_truncated_on_disk() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    write_reference_wal(&path);
    let len = std::fs::metadata(&path).unwrap().len();
    // Chop the final record in the middle.
    let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(len - 15).unwrap();
    drop(file);

    let wal = FileWal::open(&path).unwrap();
    assert_eq!(wal.torn_tail_dropped(), 1);
    assert_eq!(wal.wal_record_count().unwrap(), 4);
    let on_disk = std::fs::read_to_string(&path).unwrap();
    assert!(on_disk.ends_with('\n'));
    assert_eq!(on_disk.lines().count(), 4);
    let (_, stats) = InMemoryStore::load_from_wal_with_stats(&wal).unwrap();
    assert_eq!(stats.replay.torn_tail_dropped, 1);
}

#[test]
fn corrupt_line_in_the_middle_is_an_error_naming_the_line() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    write_reference_wal(&path);
    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    lines[1] = lines[1].replacen("second", "sec0nd", 1); // checksum mismatch
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();

    let wal = FileWal::open(&path).unwrap();
    let err = InMemoryStore::load_from_wal(&wal).err().expect("must fail");
    let msg = format!("{err:?}");
    assert!(msg.contains("line 2"), "{msg}");
}

#[test]
fn flipped_byte_in_the_final_record_is_treated_as_torn() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    write_reference_wal(&path);
    let text = std::fs::read_to_string(&path).unwrap();
    let mut lines: Vec<String> = text.lines().map(str::to_string).collect();
    let last = lines.len() - 1;
    lines[last] = lines[last].replacen("commit-1", "commit-2", 1);
    std::fs::write(&path, lines.join("\n") + "\n").unwrap();

    let wal = FileWal::open(&path).unwrap();
    assert_eq!(wal.torn_tail_dropped(), 1);
    assert_eq!(wal.wal_record_count().unwrap(), 4);
}

#[test]
fn legacy_records_without_checksum_replay() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("dash.wal");
    std::fs::write(
        &path,
        "C\tc1\ttenant-a\tLegacy claim\t0.9\tnull\nE\te1\tc1\tsource://legacy\tsupports\t0.8\nG\tg1\tc1\tc1\tsupports\t0.5\n",
    )
    .unwrap();
    let wal = FileWal::open(&path).unwrap();
    assert_eq!(wal.torn_tail_dropped(), 0);
    let store = InMemoryStore::load_from_wal(&wal).unwrap();
    assert!(store.claim_by_id("c1").is_some());
    let edges = store.edges_for_claim("c1");
    assert_eq!(edges.len(), 1);
    assert!(edges[0].reason_codes.is_empty());
}
