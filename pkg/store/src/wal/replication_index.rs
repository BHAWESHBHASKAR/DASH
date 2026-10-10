//! Incremental index over the replication view of the WAL file.
//!
//! A replication frame is a window `[from, from + max_records)` of the WAL
//! lines a follower may apply (the lines lenient replay would quarantine are
//! left out, see [`ReplicationFilter`]). Building that view by reading the
//! whole file on every poll makes each poll cost O(WAL) time and memory: a
//! leader without checkpoints allocated a copy of its entire log several
//! times a second, and its resident memory grew with the log. The index
//! below reads each WAL byte once, as it is appended, and keeps only a
//! sparse table of line offsets, so a poll reads just the lines it ships.
//!
//! Memory held per WAL: one `u64` per [`ANCHOR_STRIDE`] replication lines,
//! one `u64` per left-out line (only unreadable legacy records), and the
//! claim ids of quarantined legacy claims. It is reset whenever the file is
//! rewritten or truncated (checkpoint, rollback, replication export).

use std::collections::{BTreeSet, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    QuarantineSink, ReplayParser, ReplayPolicy, is_legacy_kind, is_valid_tail, record_kind,
};
use crate::StoreError;

/// One anchor (byte offset of a replication line) every this many lines.
/// Serving a frame reads at most `ANCHOR_STRIDE - 1` lines before its start.
pub(super) const ANCHOR_STRIDE: usize = 64;

/// Decides, line by line and in log order, which WAL lines belong to the
/// replication view: lenient replay's own parser drops unparseable legacy
/// lines, their continuation fragments and records that depend on a
/// quarantined legacy claim. Lines that only fail validation against the
/// store state are kept; followers skip those (see
/// `InMemoryStore::apply_persisted_record_line_lenient`).
pub(super) struct ReplicationFilter {
    parser: ReplayParser,
    sink: QuarantineSink,
    bad_claims: HashSet<String>,
}

impl ReplicationFilter {
    pub(super) fn new() -> Self {
        let mut parser = ReplayParser::new(ReplayPolicy::Lenient);
        parser.quiet = true;
        Self {
            parser,
            sink: QuarantineSink::detached(),
            bad_claims: HashSet::new(),
        }
    }

    /// `true` when `line` belongs to the replication view.
    pub(super) fn keep(&mut self, line: &str) -> bool {
        // Fast path: only legacy lines (and fragments following a failed
        // one) can be quarantined at parse level.
        if !self.parser.prev_failed
            && self.bad_claims.is_empty()
            && !is_legacy_kind(record_kind(line))
        {
            return true;
        }
        let keep = match self
            .parser
            .parse(line.to_string(), String::new(), &mut self.sink)
        {
            Ok(Some(item)) => self.bad_claims.is_empty() || !item.depends_on(&self.bad_claims),
            Ok(None) => {
                self.bad_claims
                    .extend(self.parser.quarantined_claim_ids.iter().cloned());
                false
            }
            // Not quarantinable: serve it unchanged, the receiver rejects it.
            Err(_) => true,
        };
        // The sink is never flushed; do not let it accumulate lines.
        self.sink.pending.clear();
        self.sink.seen.clear();
        keep
    }
}

/// See the module documentation.
pub(super) struct ReplicationIndex {
    /// Length of the WAL prefix covered by the index; always the end of a
    /// line.
    indexed_bytes: u64,
    /// Replication lines in the covered prefix.
    kept: usize,
    /// Lines of the covered prefix left out of the replication view.
    skipped: usize,
    /// `anchors[i]` is the byte offset of replication line
    /// `i * ANCHOR_STRIDE`.
    anchors: Vec<u64>,
    /// Byte offsets of the left-out lines.
    dropped: BTreeSet<u64>,
    filter: ReplicationFilter,
    /// Bytes read from the file for the replication view since the WAL was
    /// opened (kept across resets).
    read_bytes: AtomicU64,
}

impl Default for ReplicationIndex {
    fn default() -> Self {
        Self {
            indexed_bytes: 0,
            kept: 0,
            skipped: 0,
            anchors: Vec::new(),
            dropped: BTreeSet::new(),
            filter: ReplicationFilter::new(),
            read_bytes: AtomicU64::new(0),
        }
    }
}

/// A terminated, non-blank line read during [`ReplicationIndex::refresh`]
/// that is not yet known to be interior (the final line of the file is only
/// part of the view when it is a complete record).
struct PendingLine {
    start: u64,
    end: u64,
    text: Option<String>,
}

impl ReplicationIndex {
    pub(super) fn reset(&mut self) {
        let read_bytes = self.read_bytes.load(Ordering::Relaxed);
        *self = Self::default();
        self.read_bytes = AtomicU64::new(read_bytes);
    }

    /// Moves the index out (its file was renamed, see the closed generation
    /// in `FileWal`) and leaves an empty one that keeps the read counter.
    pub(super) fn take_for_renamed_file(&mut self) -> Self {
        let read_bytes = self.read_bytes();
        let taken = std::mem::take(self);
        self.read_bytes = AtomicU64::new(read_bytes);
        taken
    }

    /// Bytes read from the file for the replication view so far.
    pub(super) fn read_bytes(&self) -> u64 {
        self.read_bytes.load(Ordering::Relaxed)
    }

    /// Counts `bytes` read for the replication view outside the index (the
    /// full-scan fallback).
    pub(super) fn note_read(&self, bytes: u64) {
        self.read_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Number of lines in the replication view.
    pub(super) fn total(&self) -> usize {
        self.kept
    }

    /// Number of WAL lines left out of the replication view.
    pub(super) fn skipped(&self) -> usize {
        self.skipped
    }

    /// Size of the sparse offset table (for tests and diagnostics).
    #[cfg(test)]
    pub(super) fn anchor_count(&self) -> usize {
        self.anchors.len()
    }

    /// Indexes the lines appended to `path` since the previous call, reading
    /// only the new bytes. Returns `Ok(false)` when the file cannot be
    /// indexed this way (it ends in an unterminated record, or an interior
    /// line is not UTF-8); the caller then builds the view from a full scan,
    /// which also produces the exact error for a corrupt line. An open
    /// [`super::FileWal`] never leaves either state behind (opening repairs
    /// the tail and every append is newline-terminated).
    pub(super) fn refresh(&mut self, path: &Path) -> Result<bool, StoreError> {
        let len = std::fs::metadata(path)?.len();
        if len < self.indexed_bytes {
            // Truncated behind the index's back: start over.
            self.reset();
        }
        if len == self.indexed_bytes {
            return Ok(true);
        }
        let mut reader = BufReader::new(File::open(path)?);
        reader.seek(SeekFrom::Start(self.indexed_bytes))?;
        let mut pos = self.indexed_bytes;
        let mut pending: Option<PendingLine> = None;
        let mut buf = Vec::new();
        loop {
            buf.clear();
            let n = reader.read_until(b'\n', &mut buf)?;
            if n == 0 {
                break;
            }
            self.read_bytes.fetch_add(n as u64, Ordering::Relaxed);
            let start = pos;
            pos += n as u64;
            let terminated = buf.last() == Some(&b'\n');
            let body = if terminated { &buf[..n - 1] } else { &buf[..] };
            let text = decode_line(body);
            let blank = text.as_deref().is_some_and(|t| t.trim().is_empty());
            if blank {
                if !terminated {
                    // Trailing whitespace that is not a line yet.
                    break;
                }
                if pending.is_none() {
                    self.indexed_bytes = pos;
                }
                continue;
            }
            // A content line after `pending` makes `pending` interior.
            if let Some(line) = pending.take()
                && !self.index_interior(line)
            {
                return Ok(false);
            }
            if !terminated {
                return Ok(false);
            }
            pending = Some(PendingLine {
                start,
                end: pos,
                text,
            });
        }
        // The last line of the file: part of the view only when complete, as
        // in a full scan. A line that is not is left unindexed and examined
        // again once more lines follow it.
        if let Some(line) = pending
            && line
                .text
                .as_deref()
                .is_some_and(|text| is_valid_tail(text, true))
        {
            self.index_interior(line);
        }
        Ok(true)
    }

    /// Adds one complete line to the index. `false` when it is not UTF-8.
    fn index_interior(&mut self, line: PendingLine) -> bool {
        let Some(text) = line.text else {
            return false;
        };
        if self.filter.keep(&text) {
            if self.kept.is_multiple_of(ANCHOR_STRIDE) {
                self.anchors.push(line.start);
            }
            self.kept += 1;
        } else {
            self.dropped.insert(line.start);
            self.skipped += 1;
        }
        self.indexed_bytes = line.end;
        true
    }

    /// The replication lines from position `from` to the end of the view,
    /// read lazily from `path`.
    pub(super) fn lines_from<'a>(
        &'a self,
        path: &Path,
        from: usize,
    ) -> Result<ViewLines<'a>, StoreError> {
        if from >= self.kept {
            return Ok(ViewLines {
                index: self,
                reader: None,
                pos: self.indexed_bytes,
                buf: Vec::new(),
            });
        }
        let anchor = self.anchors[from / ANCHOR_STRIDE];
        let mut reader = BufReader::new(File::open(path)?);
        reader.seek(SeekFrom::Start(anchor))?;
        let mut lines = ViewLines {
            index: self,
            reader: Some(reader),
            pos: anchor,
            buf: Vec::new(),
        };
        for _ in 0..from % ANCHOR_STRIDE {
            if lines.next().transpose()?.is_none() {
                break;
            }
        }
        Ok(lines)
    }
}

/// Iterator over replication lines; see [`ReplicationIndex::lines_from`].
pub(super) struct ViewLines<'a> {
    index: &'a ReplicationIndex,
    reader: Option<BufReader<File>>,
    pos: u64,
    buf: Vec<u8>,
}

impl ViewLines<'_> {
    fn read_next(&mut self) -> Result<Option<String>, StoreError> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };
        while self.pos < self.index.indexed_bytes {
            self.buf.clear();
            let n = reader.read_until(b'\n', &mut self.buf)?;
            if n == 0 {
                break;
            }
            self.index.read_bytes.fetch_add(n as u64, Ordering::Relaxed);
            let start = self.pos;
            self.pos += n as u64;
            let body = self.buf.strip_suffix(b"\n").unwrap_or(&self.buf);
            let Some(text) = decode_line(body) else {
                return Err(StoreError::Parse(format!(
                    "wal bytes at offset {start}: invalid UTF-8"
                )));
            };
            if text.trim().is_empty() || self.index.dropped.contains(&start) {
                continue;
            }
            return Ok(Some(text));
        }
        Ok(None)
    }
}

impl Iterator for ViewLines<'_> {
    type Item = Result<String, StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.read_next().transpose()
    }
}

/// Text of one physical line (without its newline); a trailing `\r` is
/// dropped. `None` for invalid UTF-8.
pub(super) fn decode_line(body: &[u8]) -> Option<String> {
    let body = body.strip_suffix(b"\r").unwrap_or(body);
    std::str::from_utf8(body).ok().map(str::to_string)
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;
    use std::io::Write;

    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use schema::claim_builder;
    use tempfile::TempDir;

    use super::super::{FileWal, PersistedRecord, record_to_line};
    use super::ANCHOR_STRIDE;

    /// One single-claim update as the ingestion service writes it: a commit
    /// group holding the claim and its vector.
    fn append_update(wal: &mut FileWal, claim: &str, round: u64) {
        wal.begin_group(&format!("{claim}-{round}"), round).unwrap();
        wal.append_claim(&claim_builder(
            claim,
            "tenant-a",
            &format!("text of {claim}, revision {round}"),
            0.9,
        ))
        .unwrap();
        wal.append_claim_vector(claim, &[round as f32 + 1.0, 2.0, 3.0])
            .unwrap();
        wal.append_batch_commit(
            &format!("~tx:{claim}-{round}"),
            1,
            round,
            &[claim.to_string()],
        )
        .unwrap();
    }

    fn append_raw_bytes(wal: &FileWal, bytes: &[u8]) {
        let mut file = OpenOptions::new().append(true).open(wal.path()).unwrap();
        file.write_all(bytes).unwrap();
    }

    /// The frame served from the index and the frame built from a full scan
    /// must be identical (or fail identically).
    fn assert_same_frame(wal: &mut FileWal, generation: Option<u64>, from: usize, max: usize) {
        let indexed = wal.replication_frame_from(generation, from, max);
        let scanned = wal.replication_frame_full_scan(generation, from, max, true);
        assert_eq!(
            format!("{indexed:?}"),
            format!("{scanned:?}"),
            "frame from={from} max={max} generation={generation:?}"
        );
    }

    #[test]
    fn frames_read_only_new_lines_while_the_same_claims_are_updated() {
        let dir = TempDir::new().unwrap();
        let mut wal =
            FileWal::open_with_sync_every_records(dir.path().join("leader.wal"), 256).unwrap();
        let generation = wal.generation();
        let mut offset = 0;
        let mut served = Vec::new();
        let mut polls = 0usize;
        // 20 claims updated 200 times each, drained by a follower with small
        // frames after every round.
        for round in 0..200u64 {
            for claim in 0..20 {
                append_update(&mut wal, &format!("claim-{claim}"), round);
            }
            loop {
                let frame = wal
                    .replication_frame_from(Some(generation), offset, 64)
                    .unwrap();
                assert!(!frame.needs_resync);
                polls += 1;
                served.extend(frame.wal_lines);
                offset = frame.next_offset;
                if offset == frame.total_records {
                    break;
                }
            }
        }
        // The follower received the log exactly.
        let full = wal.replay_wal_lines_raw().unwrap();
        assert_eq!(full.len(), 200 * 20 * 4);
        assert_eq!(served, full);

        // Reading the whole file on every poll would read it about
        // `polls / 2` times over (here more than 100 times). Indexing reads
        // each byte once, serving reads each frame once plus fewer than
        // ANCHOR_STRIDE lines before it.
        let wal_bytes = wal.wal_size_bytes().unwrap();
        let read = wal.replication_read_bytes_total();
        assert!(polls > 200, "{polls} polls");
        assert!(
            read <= 4 * wal_bytes,
            "replication read {read} bytes for a {wal_bytes}-byte WAL in {polls} polls"
        );

        // A caught-up follower costs no reads at all.
        let before = wal.replication_read_bytes_total();
        for _ in 0..10 {
            let frame = wal
                .replication_frame_from(Some(generation), offset, 64)
                .unwrap();
            assert!(frame.wal_lines.is_empty());
        }
        assert_eq!(wal.replication_read_bytes_total(), before);

        // The index holds one offset per ANCHOR_STRIDE lines and nothing per
        // update.
        let index = &wal.replication_index;
        assert_eq!(index.total(), full.len());
        assert_eq!(index.anchor_count(), full.len().div_ceil(ANCHOR_STRIDE));
        assert!(index.dropped.is_empty());
        assert_eq!(index.skipped(), 0);

        // A checkpoint empties the index.
        wal.compact_with_snapshot(&[]).unwrap();
        let frame = wal
            .replication_frame_from(Some(wal.generation()), 0, 64)
            .unwrap();
        assert_eq!(frame.total_records, 0);
        assert_eq!(wal.replication_index.anchor_count(), 0);
    }

    const TAIL: &str = "null\tnull\tnull\tnull\tnull";

    /// Legacy lines lenient replay quarantines (and their dependents) next
    /// to readable ones.
    fn legacy_lines() -> Vec<String> {
        vec![
            format!("C\tc-ok\ttenant-a\ttext of c-ok\t0.9\tnull\t3:foo\t\t{TAIL}"),
            format!("C\tc-ent\ttenant-a\ttext of c-ent\t0.9\tnull\t5:a\tb c\t\t{TAIL}"),
            "E\te-dep\tc-ent\tsource-1\tsupports\t0.8".to_string(),
            "E\te-ok\tc-ok\tsource-1\tsupports\t0.8".to_string(),
            "G\tg-dep\tc-ok\tc-ent\tsupports\t0.5".to_string(),
            "V\tc-ent\t1,2,3".to_string(),
            "V\tc-ok\t1,NaN,3".to_string(),
            "this is not a record".to_string(),
        ]
    }

    #[test]
    fn indexed_frames_match_full_scan_frames_under_random_operations() {
        for seed in 0..4u64 {
            let mut rng = StdRng::seed_from_u64(seed);
            let dir = TempDir::new().unwrap();
            let mut wal =
                FileWal::open_with_sync_every_records(dir.path().join("leader.wal"), 1).unwrap();
            let legacy = legacy_lines();
            let mut round = 0u64;
            for _ in 0..300 {
                round += 1;
                match rng.gen_range(0..100) {
                    0..=44 => {
                        let claim = format!("claim-{}", rng.gen_range(0..5));
                        append_update(&mut wal, &claim, round);
                    }
                    45..=59 => {
                        let line = &legacy[rng.gen_range(0..legacy.len())];
                        if line.contains('\t') {
                            wal.append_raw_record_line(line).unwrap();
                        } else {
                            append_raw_bytes(&wal, format!("{line}\n").as_bytes());
                        }
                    }
                    60..=66 => {
                        let blank: &[u8] = match rng.gen_range(0..3) {
                            0 => b"\n",
                            1 => b"   \n",
                            _ => b"\r\n",
                        };
                        append_raw_bytes(&wal, blank);
                    }
                    67..=71 => {
                        // A group left open: a write in progress.
                        wal.begin_group(&format!("open-{round}"), round).unwrap();
                        wal.append_claim(&claim_builder("claim-open", "tenant-a", "t", 0.5))
                            .unwrap();
                    }
                    72..=77 => {
                        let point = wal.begin_rollback_point().unwrap();
                        append_update(&mut wal, "claim-rolled-back", round);
                        // Index the lines that are about to be rolled back.
                        let generation = Some(wal.generation());
                        assert_same_frame(&mut wal, generation, 0, 64);
                        wal.rollback_to(point).unwrap();
                        // Longer lines in their place: the file grows past
                        // the old indexed length before the next frame.
                        append_update(&mut wal, "claim-written-after-rollback", round);
                    }
                    78..=80 => {
                        wal.compact_with_snapshot(&[]).unwrap();
                    }
                    81..=85 => {
                        // A terminated line that is not a record: torn while
                        // it is the last line, served once lines follow it.
                        append_raw_bytes(&wal, b"C2\ttorn\n");
                    }
                    86..=90 => {
                        // A complete record without its newline (only a
                        // crash leaves one; opening the WAL repairs it).
                        let line = record_to_line(&PersistedRecord::Claim(claim_builder(
                            "claim-tail",
                            "tenant-a",
                            "unterminated",
                            0.5,
                        )));
                        append_raw_bytes(&wal, line.as_bytes());
                        let generation = Some(wal.generation());
                        assert_same_frame(&mut wal, generation, 0, 1_000_000);
                        append_raw_bytes(&wal, b"\n");
                    }
                    91..=94 => {
                        wal.set_replication_group_cap(rng.gen_range(1..8));
                    }
                    _ => {
                        wal.set_replication_group_cap(1_000_000);
                    }
                }
                let generation = wal.generation();
                let total = wal
                    .replication_frame_full_scan(Some(generation), 0, 1, true)
                    .map(|f| f.total_records)
                    .unwrap_or(0);
                assert_same_frame(&mut wal, None, 0, 64);
                assert_same_frame(&mut wal, Some(generation ^ 1), 0, 64);
                for _ in 0..4 {
                    let from = rng.gen_range(0..=total + 1);
                    let max = [1, 3, 7, 64, 1_000_000][rng.gen_range(0..5)];
                    assert_same_frame(&mut wal, Some(generation), from, max);
                }
                assert_same_frame(&mut wal, Some(generation), total, 64);
            }
        }
    }

    #[test]
    fn an_interior_line_that_is_not_utf8_fails_like_a_full_scan() {
        let dir = TempDir::new().unwrap();
        let mut wal = FileWal::open(dir.path().join("leader.wal")).unwrap();
        append_update(&mut wal, "claim-a", 1);
        append_raw_bytes(&wal, b"C2\t\xff\xfe\n");
        append_update(&mut wal, "claim-b", 2);
        let generation = Some(wal.generation());
        let err = wal.replication_frame_from(generation, 0, 64).unwrap_err();
        assert!(format!("{err:?}").contains("invalid UTF-8"), "{err:?}");
        assert_same_frame(&mut wal, generation, 0, 64);
    }
}
