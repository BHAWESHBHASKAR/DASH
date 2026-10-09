//! Interior WAL corruption must never silently destroy acknowledged groups,
//! and length/count fields read from untrusted bytes must never panic.

use schema::{Claim, Evidence, Stance, claim_builder};
use std::path::{Path, PathBuf};
use store::{FileWal, InMemoryStore, ReplayPolicy};
use tempfile::TempDir;

fn evidence(id: &str, claim_id: &str) -> Evidence {
    Evidence {
        evidence_id: id.to_string(),
        claim_id: claim_id.to_string(),
        source_id: "src".to_string(),
        stance: Stance::Supports,
        source_quality: 0.5,
        chunk_id: None,
        span_start: None,
        span_end: None,
        doc_id: None,
        extraction_model: None,
        ingested_at: None,
    }
}

fn claim(i: u64) -> Claim {
    claim_builder(&format!("c{i}"), "t1", &format!("text {i} body"), 0.5)
}

fn build(path: &Path, groups: u64) {
    let mut wal = FileWal::open(path).unwrap();
    let mut st = InMemoryStore::new();
    for i in 0..groups {
        st.ingest_atomic_persistent(
            &mut wal,
            claim(i),
            vec![evidence(&format!("e{i}"), &format!("c{i}"))],
            vec![],
            Some(vec![1.0, 0.5, i as f32]),
            1000 + i,
        )
        .unwrap();
    }
    wal.flush_pending_sync().unwrap();
}

fn load(path: &Path) -> Result<usize, String> {
    let wal = FileWal::open(path).map_err(|e| format!("open {e:?}"))?;
    let (s, _) =
        InMemoryStore::load_from_wal_with_policy(&wal, Default::default(), ReplayPolicy::Lenient)
            .map_err(|e| format!("load {e:?}"))?;
    Ok(s.claims_len())
}

fn sidecars(path: &Path) -> Vec<PathBuf> {
    let prefix = format!("{}.truncated-", path.file_name().unwrap().to_string_lossy());
    std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .is_some_and(|n| n.to_string_lossy().starts_with(&prefix))
        })
        .collect()
}

#[test]
fn corrupt_interior_group_marker_does_not_truncate_wal() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wal");
    build(&path, 4);
    let mut bytes = std::fs::read(&path).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    // Newline terminating the END marker of the third group: flipping it joins
    // that marker with the next group's BEGIN marker, which hides both.
    let mut pos = 0usize;
    let mut ends = 0;
    let mut target = None;
    for l in text.split_inclusive('\n') {
        pos += l.len();
        if l.starts_with("B2\t~tx:") {
            ends += 1;
            if ends == 3 {
                target = Some(pos - 1);
            }
        }
    }
    bytes[target.unwrap()] ^= 1;
    std::fs::write(&path, &bytes).unwrap();

    let err = load(&path).expect_err("interior marker corruption must be a hard error");
    assert!(err.contains("wal line"), "error must name the line: {err}");
    let after = std::fs::read(&path).unwrap();
    assert_eq!(after, bytes, "WAL must not be modified");
}

#[test]
fn flipping_any_byte_of_any_marker_line_never_silently_loses_groups() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wal");
    build(&path, 4);
    let bytes = std::fs::read(&path).unwrap();
    let text = String::from_utf8(bytes.clone()).unwrap();
    let mut marker_ranges = Vec::new();
    let mut pos = 0usize;
    let mut last_marker_start = 0usize;
    for l in text.split_inclusive('\n') {
        if l.starts_with("B2\t") {
            marker_ranges.push((pos, pos + l.len()));
            last_marker_start = pos;
        }
        pos += l.len();
    }
    assert!(marker_ranges.len() >= 8);
    let mut errors = 0usize;
    for (s, e) in marker_ranges {
        for i in s..e {
            for bit in 0..8 {
                let d2 = TempDir::new().unwrap();
                let p2 = d2.path().join("wal");
                let mut m = bytes.clone();
                m[i] ^= 1 << bit;
                std::fs::write(&p2, &m).unwrap();
                match load(&p2) {
                    Err(msg) => {
                        errors += 1;
                        assert!(msg.contains("wal line"), "byte {i} bit {bit}: {msg}");
                    }
                    Ok(4) => {}
                    Ok(n) => {
                        // Only a corrupt LAST marker may cost the final group,
                        // and then the removed bytes must be preserved.
                        assert!(
                            i >= last_marker_start && n == 3,
                            "silent loss: byte {i} bit {bit} -> {n} claims"
                        );
                        assert!(
                            !sidecars(&p2).is_empty(),
                            "byte {i} bit {bit}: removed bytes were not preserved"
                        );
                    }
                }
            }
        }
    }
    assert!(errors > 0);
}

#[test]
fn joined_final_lines_are_preserved_in_a_truncated_sidecar() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wal");
    build(&path, 2);
    let mut bytes = std::fs::read(&path).unwrap();
    // Flip the newline that separates the last two lines.
    let last_nl = bytes.len() - 1;
    let prev_nl = bytes[..last_nl].iter().rposition(|b| *b == b'\n').unwrap();
    let prev_start = bytes[..prev_nl]
        .iter()
        .rposition(|b| *b == b'\n')
        .map_or(0, |p| p + 1);
    bytes[prev_nl] = b' ';
    std::fs::write(&path, &bytes).unwrap();
    let probe: Vec<u8> = bytes[prev_start..prev_start + 40].to_vec();
    let _ = load(&path);
    let cars = sidecars(&path);
    assert!(!cars.is_empty(), "joined lines must be saved");
    let saved: Vec<u8> = cars
        .iter()
        .flat_map(|p| std::fs::read(p).unwrap())
        .collect();
    assert!(
        saved.windows(probe.len()).any(|w| w == probe.as_slice()),
        "sidecar must contain the dropped earlier record"
    );
}

fn with_crc(body: &str) -> String {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in body.as_bytes() {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    format!("{body}\tcrc={:08x}", !crc)
}

#[test]
fn huge_packed_list_length_is_an_error_not_a_panic() {
    for len in [
        "18446744073709551615",
        "18446744073709551614",
        "9223372036854775808",
    ] {
        let line = with_crc(&format!("B2\tcid\t1\t0\t{len}:abc"));
        let mut st = InMemoryStore::new();
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            st.apply_persisted_record_line(&line)
        }));
        let r = r.unwrap_or_else(|_| panic!("panic for len {len}"));
        assert!(r.is_err(), "len {len} must be rejected");
    }
}

#[test]
fn huge_packed_list_length_in_a_wal_file_is_an_error_not_a_panic() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wal");
    build(&path, 1);
    let good = std::fs::read_to_string(&path).unwrap();
    let line = with_crc("B2\tcid\t1\t0\t18446744073709551614:abc");
    std::fs::write(&path, format!("{line}\n{good}")).unwrap();
    let r = std::panic::catch_unwind(|| load(&path));
    assert!(r.expect("must not panic").is_err());
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

#[test]
fn mutated_record_lines_never_panic_apply() {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("wal");
    build(&path, 3);
    let seeds: Vec<String> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(str::to_string)
        .collect();
    let digits = [
        "18446744073709551615",
        "18446744073709551614",
        "99999999999",
        "-1",
        "4294967296",
    ];
    let mut rng = Rng(0x9E37_79B9_7F4A_7C15);
    for _ in 0..20_000 {
        let seed = &seeds[rng.below(seeds.len())];
        let mut b = seed.clone().into_bytes();
        for _ in 0..=rng.below(3) {
            match rng.below(4) {
                0 if !b.is_empty() => {
                    let i = rng.below(b.len());
                    b[i] = (rng.next() & 0x7f) as u8;
                }
                1 if !b.is_empty() => {
                    let i = rng.below(b.len());
                    b.remove(i);
                }
                2 => {
                    let i = rng.below(b.len() + 1);
                    let d = digits[rng.below(digits.len())];
                    let mut tail = b.split_off(i);
                    b.extend_from_slice(d.as_bytes());
                    b.extend_from_slice(b":");
                    b.append(&mut tail);
                }
                _ => {
                    let i = rng.below(b.len() + 1);
                    b.insert(i, b"\t:\\,"[rng.below(4)]);
                }
            }
        }
        let Ok(mutated) = String::from_utf8(b) else {
            continue;
        };
        // Re-seal the checksum half the time so the parser body is reached.
        let candidates = match mutated.rfind("\tcrc=") {
            Some(idx) if rng.below(2) == 0 => vec![with_crc(&mutated[..idx]), mutated.clone()],
            _ => vec![mutated.clone()],
        };
        for line in candidates {
            let mut st = InMemoryStore::new();
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _ = st.apply_persisted_record_line(&line);
            }));
            assert!(
                r.is_ok(),
                "apply_persisted_record_line panicked on {line:?}"
            );
        }
    }
}
