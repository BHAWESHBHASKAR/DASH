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
        *self = Self::default();
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
            if self.kept % ANCHOR_STRIDE == 0 {
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
