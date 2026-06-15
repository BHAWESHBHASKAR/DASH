//! Write-Ahead Log (WAL) types and the [`FileWal`] implementation.
//!
//! The WAL is the canonical source of truth for crash recovery. On
//! restart, [`super::InMemoryStore::load_from_disk_and_wal`] replays
//! the snapshot (if any) plus the WAL delta. Reordering, batching,
//! and rotation all live behind this module's public API.
//!
//! Two top-level types are exported:
//! - [`WalEvent`] — an in-memory record of one mutation, used by
//!   `InMemoryStore.wal` for the segment-cache replay path.
//! - [`FileWal`] — the on-disk append-only log.
//!
//! All other types in this module are either private helpers
//! ([`PersistedRecord`], [`ClaimVectorRecord`], [`BatchCommitRecord`])
//! or wire/stats types ([`WalReplayBoundary`], [`WalReplicationDelta`],
//! etc.) that the rest of the crate consumes via re-exports from
//! `lib.rs`.

use std::fs::{create_dir_all, rename, OpenOptions};
use std::io::{BufRead, BufReader, Write};

const SNAPSHOT_HEADER: &str = "SNAP\t1";
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use schema::{Claim, ClaimEdge, ClaimType, Evidence, Relation, Stance};

use crate::StoreError;

#[derive(Debug, Clone, PartialEq)]
pub enum WalEvent {
    ClaimUpsert(String),
    EvidenceUpsert(String),
    EdgeUpsert(String),
    ClaimVectorUpsert(String),
    BatchCommit(String),
}

#[derive(Debug, Clone)]
pub(crate) enum PersistedRecord {
    Claim(Claim),
    Evidence(Evidence),
    Edge(ClaimEdge),
    ClaimVector(ClaimVectorRecord),
    BatchCommit(BatchCommitRecord),
}

#[derive(Debug, Clone)]
pub(crate) struct ClaimVectorRecord {
    pub(crate) claim_id: String,
    pub(crate) values: Vec<f32>,
}

#[derive(Debug, Clone)]
pub(crate) struct BatchCommitRecord {
    pub(crate) commit_id: String,
    pub(crate) batch_size: usize,
    pub(crate) ts_unix_ms: u64,
    pub(crate) claim_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalCheckpointStats {
    pub snapshot_records: usize,
    pub truncated_wal_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CheckpointPolicy {
    pub max_wal_records: Option<usize>,
    pub max_wal_bytes: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WalReplayStats {
    pub snapshot_records: usize,
    pub wal_records: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct WalReplayBoundary {
    pub snapshot_active: bool,
    pub snapshot_record_count: usize,
    pub wal_delta_record_count: usize,
    pub total_replay_record_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalReplicationDelta {
    pub from_offset: usize,
    pub next_offset: usize,
    pub total_records: usize,
    pub needs_resync: bool,
    pub wal_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalReplicationExport {
    pub snapshot_lines: Vec<String>,
    pub wal_lines: Vec<String>,
}

pub struct FileWal {
    path: PathBuf,
    wal_records: usize,
    sync_every_records: usize,
    append_buffer_max_records: usize,
    sync_interval: Option<Duration>,
    background_flush_only: bool,
    append_buffer: Vec<String>,
    pub(crate) unsynced_records: usize,
    last_sync_at: Instant,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalWritePolicy {
    pub sync_every_records: usize,
    pub append_buffer_max_records: usize,
    pub sync_interval: Option<Duration>,
    pub background_flush_only: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRollbackPoint {
    file_len_bytes: u64,
    wal_records: usize,
}

impl Default for WalWritePolicy {
    fn default() -> Self {
        Self {
            sync_every_records: 1,
            append_buffer_max_records: 1,
            sync_interval: None,
            background_flush_only: false,
        }
    }
}

impl FileWal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Self::open_with_sync_every_records(path, 1)
    }

    pub fn open_with_sync_every_records(
        path: impl AsRef<Path>,
        sync_every_records: usize,
    ) -> Result<Self, StoreError> {
        Self::open_with_policy(
            path,
            WalWritePolicy {
                sync_every_records,
                ..WalWritePolicy::default()
            },
        )
    }

    pub fn open_with_policy(
        path: impl AsRef<Path>,
        policy: WalWritePolicy,
    ) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent()
            && !parent.as_os_str().is_empty()
        {
            create_dir_all(parent)?;
        }
        OpenOptions::new().create(true).append(true).open(&path)?;
        let wal_records = count_non_empty_lines(&path)?;
        Ok(Self {
            path,
            wal_records,
            sync_every_records: policy.sync_every_records.max(1),
            append_buffer_max_records: policy.append_buffer_max_records.max(1),
            sync_interval: policy.sync_interval,
            background_flush_only: policy.background_flush_only,
            append_buffer: Vec::new(),
            unsynced_records: 0,
            last_sync_at: Instant::now(),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn sync_every_records(&self) -> usize {
        self.sync_every_records
    }

    pub fn append_buffer_max_records(&self) -> usize {
        self.append_buffer_max_records
    }

    pub fn sync_interval(&self) -> Option<Duration> {
        self.sync_interval
    }

    pub fn background_flush_only(&self) -> bool {
        self.background_flush_only
    }

    pub fn unsynced_record_count(&self) -> usize {
        self.unsynced_records
    }

    pub fn buffered_record_count(&self) -> usize {
        self.append_buffer.len()
    }

    pub fn snapshot_path(&self) -> PathBuf {
        let mut path = self.path.clone().into_os_string();
        path.push(".snapshot");
        PathBuf::from(path)
    }

    pub fn append_claim(&mut self, claim: &Claim) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::Claim(claim.clone()))
    }

    pub fn append_evidence(&mut self, evidence: &Evidence) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::Evidence(evidence.clone()))
    }

    pub fn append_edge(&mut self, edge: &ClaimEdge) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::Edge(edge.clone()))
    }

    pub fn append_claim_vector(
        &mut self,
        claim_id: &str,
        values: &[f32],
    ) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::ClaimVector(ClaimVectorRecord {
            claim_id: claim_id.to_string(),
            values: values.to_vec(),
        }))
    }

    pub fn append_batch_commit(
        &mut self,
        commit_id: &str,
        batch_size: usize,
        ts_unix_ms: u64,
        claim_ids: &[String],
    ) -> Result<(), StoreError> {
        self.append_record(&PersistedRecord::BatchCommit(BatchCommitRecord {
            commit_id: commit_id.to_string(),
            batch_size,
            ts_unix_ms,
            claim_ids: claim_ids.to_vec(),
        }))
    }

    pub fn wal_record_count(&self) -> Result<usize, StoreError> {
        Ok(self.wal_records)
    }

    pub fn wal_size_bytes(&self) -> Result<u64, StoreError> {
        Ok(std::fs::metadata(&self.path)?.len())
    }

    pub fn replay_boundary(&self) -> Result<WalReplayBoundary, StoreError> {
        let snapshot_record_count = self.replay_snapshot_lines_raw()?.len();
        let mut wal_delta_record_count = self.replay_wal_lines_raw()?.len();
        wal_delta_record_count = wal_delta_record_count.saturating_add(self.append_buffer.len());
        Ok(WalReplayBoundary {
            snapshot_active: snapshot_record_count > 0,
            snapshot_record_count,
            wal_delta_record_count,
            total_replay_record_count: snapshot_record_count.saturating_add(wal_delta_record_count),
        })
    }

    pub fn begin_rollback_point(&mut self) -> Result<WalRollbackPoint, StoreError> {
        self.flush_pending_sync()?;
        Ok(WalRollbackPoint {
            file_len_bytes: self.wal_size_bytes()?,
            wal_records: self.wal_records,
        })
    }

    pub fn rollback_to(&mut self, point: WalRollbackPoint) -> Result<(), StoreError> {
        self.append_buffer.clear();
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&self.path)?;
        file.set_len(point.file_len_bytes)?;
        file.sync_data()?;
        self.wal_records = point.wal_records;
        self.unsynced_records = 0;
        self.last_sync_at = Instant::now();
        Ok(())
    }

    pub fn append_raw_record_line(&mut self, line: &str) -> Result<(), StoreError> {
        let line = line.trim();
        if line.is_empty() {
            return Err(StoreError::Parse(
                "raw WAL record line must not be empty".to_string(),
            ));
        }
        let _ = line_to_record(line)?;
        self.append_raw_record_line_unchecked(line.to_string())
    }

    pub fn replication_delta_from(
        &mut self,
        from_offset: usize,
        max_records: usize,
    ) -> Result<WalReplicationDelta, StoreError> {
        self.flush_pending_sync()?;
        let wal_lines = self.replay_wal_lines_raw()?;
        let total_records = wal_lines.len();
        if from_offset > total_records {
            return Ok(WalReplicationDelta {
                from_offset,
                next_offset: total_records,
                total_records,
                needs_resync: true,
                wal_lines: Vec::new(),
            });
        }
        let limit = max_records.max(1);
        let next_offset = from_offset.saturating_add(limit).min(total_records);
        Ok(WalReplicationDelta {
            from_offset,
            next_offset,
            total_records,
            needs_resync: false,
            wal_lines: wal_lines[from_offset..next_offset].to_vec(),
        })
    }

    pub fn replication_export(&mut self) -> Result<WalReplicationExport, StoreError> {
        self.flush_pending_sync()?;
        Ok(WalReplicationExport {
            snapshot_lines: self.replay_snapshot_lines_raw()?,
            wal_lines: self.replay_wal_lines_raw()?,
        })
    }

    pub fn replace_with_replication_export(
        &mut self,
        export: &WalReplicationExport,
    ) -> Result<(), StoreError> {
        self.flush_pending_sync()?;
        for line in &export.snapshot_lines {
            let _ = line_to_record(line)?;
        }
        for line in &export.wal_lines {
            let _ = line_to_record(line)?;
        }

        self.write_snapshot_lines_raw(&export.snapshot_lines)?;
        self.write_wal_lines_raw(&export.wal_lines)?;
        self.wal_records = export.wal_lines.len();
        self.unsynced_records = 0;
        self.last_sync_at = Instant::now();
        self.append_buffer.clear();
        Ok(())
    }

    fn append_record(&mut self, record: &PersistedRecord) -> Result<(), StoreError> {
        self.append_raw_record_line_unchecked(record_to_line(record))
    }

    fn append_raw_record_line_unchecked(&mut self, line: String) -> Result<(), StoreError> {
        self.append_buffer.push(line);
        self.wal_records += 1;
        self.unsynced_records += 1;
        if self.background_flush_only {
            return Ok(());
        }
        let interval_elapsed = self
            .sync_interval
            .is_some_and(|interval| self.last_sync_at.elapsed() >= interval);
        if self.unsynced_records >= self.sync_every_records || interval_elapsed {
            self.flush_pending_sync()?;
            return Ok(());
        }
        if self.append_buffer.len() >= self.append_buffer_max_records {
            self.flush_append_buffer()?;
        }
        Ok(())
    }

    pub fn flush_pending_sync_if_interval_elapsed(&mut self) -> Result<bool, StoreError> {
        let Some(interval) = self.sync_interval else {
            return Ok(false);
        };
        if self.unsynced_records == 0 || self.last_sync_at.elapsed() < interval {
            return Ok(false);
        }
        self.flush_pending_sync()?;
        Ok(true)
    }

    pub fn flush_pending_sync_if_unsynced(&mut self) -> Result<bool, StoreError> {
        if self.unsynced_records == 0 {
            return Ok(false);
        }
        self.flush_pending_sync()?;
        Ok(true)
    }

    fn flush_append_buffer(&mut self) -> Result<(), StoreError> {
        if self.append_buffer.is_empty() {
            return Ok(());
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        for line in self.append_buffer.drain(..) {
            writeln!(file, "{line}")?;
        }
        Ok(())
    }

    pub fn flush_pending_sync(&mut self) -> Result<(), StoreError> {
        if self.unsynced_records == 0 && self.append_buffer.is_empty() {
            return Ok(());
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        for line in self.append_buffer.drain(..) {
            writeln!(file, "{line}")?;
        }
        if self.unsynced_records > 0 {
            file.sync_data()?;
            self.unsynced_records = 0;
            self.last_sync_at = Instant::now();
        }
        Ok(())
    }

    pub(crate) fn replay_records_with_stats(
        &self,
    ) -> Result<(Vec<PersistedRecord>, WalReplayStats), StoreError> {
        let snapshot_records = self.replay_snapshot_records()?;
        let mut wal_records = self.replay_wal_records()?;
        if !self.append_buffer.is_empty() {
            for line in &self.append_buffer {
                wal_records.push(line_to_record(line)?);
            }
        }
        let stats = WalReplayStats {
            snapshot_records: snapshot_records.len(),
            wal_records: wal_records.len(),
        };

        let mut out = snapshot_records;
        out.extend(wal_records);
        Ok((out, stats))
    }

    fn replay_snapshot_records(&self) -> Result<Vec<PersistedRecord>, StoreError> {
        self.replay_snapshot_lines_raw()?
            .into_iter()
            .map(|line| line_to_record(&line))
            .collect()
    }

    fn replay_snapshot_lines_raw(&self) -> Result<Vec<String>, StoreError> {
        let snapshot_path = self.snapshot_path();
        if !snapshot_path.exists() {
            return Ok(Vec::new());
        }
        let file = OpenOptions::new().read(true).open(snapshot_path)?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let header = loop {
            match lines.next() {
                Some(line) => {
                    let line = line?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    break line;
                }
                None => {
                    return Err(StoreError::Parse("snapshot file is empty".to_string()));
                }
            }
        };
        if header != SNAPSHOT_HEADER {
            return Err(StoreError::Parse(
                "snapshot file has invalid header".to_string(),
            ));
        }

        let mut out = Vec::new();
        for line in lines {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            out.push(line);
        }
        Ok(out)
    }

    fn replay_wal_records(&self) -> Result<Vec<PersistedRecord>, StoreError> {
        self.replay_wal_lines_raw()?
            .into_iter()
            .map(|line| line_to_record(&line))
            .collect()
    }

    fn replay_wal_lines_raw(&self) -> Result<Vec<String>, StoreError> {
        let file = OpenOptions::new().read(true).open(&self.path)?;
        let reader = BufReader::new(file);
        let mut out = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            out.push(line);
        }
        Ok(out)
    }

    fn write_snapshot_records(&self, records: &[PersistedRecord]) -> Result<(), StoreError> {
        self.write_snapshot_lines_raw(&records.iter().map(record_to_line).collect::<Vec<String>>())
    }

    fn write_snapshot_lines_raw(&self, lines: &[String]) -> Result<(), StoreError> {
        let snapshot_path = self.snapshot_path();
        if let Some(parent) = snapshot_path.parent()
            && !parent.as_os_str().is_empty()
        {
            create_dir_all(parent)?;
        }

        let mut tmp_path = snapshot_path.clone().into_os_string();
        tmp_path.push(".tmp");
        let tmp_path = PathBuf::from(tmp_path);

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        writeln!(file, "{SNAPSHOT_HEADER}")?;
        for line in lines {
            writeln!(file, "{line}")?;
        }
        file.sync_all()?;
        rename(tmp_path, snapshot_path)?;
        Ok(())
    }

    fn write_wal_lines_raw(&self, lines: &[String]) -> Result<(), StoreError> {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        for line in lines {
            writeln!(file, "{line}")?;
        }
        file.sync_data()?;
        Ok(())
    }

    fn truncate_wal(&mut self) -> Result<(), StoreError> {
        self.append_buffer.clear();
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        self.wal_records = 0;
        self.unsynced_records = 0;
        self.last_sync_at = Instant::now();
        Ok(())
    }

    pub(crate) fn compact_with_snapshot(
        &mut self,
        snapshot_records: &[PersistedRecord],
    ) -> Result<WalCheckpointStats, StoreError> {
        let truncated_wal_records = self.wal_records;
        self.flush_pending_sync()?;
        self.write_snapshot_records(snapshot_records)?;
        self.truncate_wal()?;
        Ok(WalCheckpointStats {
            snapshot_records: snapshot_records.len(),
            truncated_wal_records,
        })
    }
}

impl Drop for FileWal {
    fn drop(&mut self) {
        let _ = self.flush_pending_sync();
    }
}



fn count_non_empty_lines(path: &Path) -> Result<usize, StoreError> {
    let file = OpenOptions::new().read(true).open(path)?;
    let reader = BufReader::new(file);
    let mut count = 0usize;
    for line in reader.lines() {
        let line = line?;
        if !line.trim().is_empty() {
            count += 1;
        }
    }
    Ok(count)
}
pub(crate) fn record_to_line(record: &PersistedRecord) -> String {
    match record {
        PersistedRecord::Claim(c) => format!(
            "C\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            escape_field(&c.claim_id),
            escape_field(&c.tenant_id),
            escape_field(&c.canonical_text),
            c.confidence,
            c.event_time_unix
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            pack_string_list(&c.entities),
            pack_string_list(&c.embedding_ids),
            c.claim_type
                .as_ref()
                .map(claim_type_to_str)
                .unwrap_or("null"),
            c.valid_from
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            c.valid_to
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            c.created_at
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            c.updated_at
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        PersistedRecord::Evidence(e) => format!(
            "E\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            escape_field(&e.evidence_id),
            escape_field(&e.claim_id),
            escape_field(&e.source_id),
            stance_to_str(&e.stance),
            e.source_quality,
            e.chunk_id
                .as_ref()
                .map(|v| escape_field(v))
                .unwrap_or_else(|| "null".to_string()),
            e.span_start
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            e.span_end
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string()),
            e.doc_id
                .as_ref()
                .map(|v| escape_field(v))
                .unwrap_or_else(|| "null".to_string()),
            e.extraction_model
                .as_ref()
                .map(|v| escape_field(v))
                .unwrap_or_else(|| "null".to_string()),
            e.ingested_at
                .map(|v| v.to_string())
                .unwrap_or_else(|| "null".to_string())
        ),
        PersistedRecord::Edge(edge) => format!(
            "G\t{}\t{}\t{}\t{}\t{}",
            escape_field(&edge.edge_id),
            escape_field(&edge.from_claim_id),
            escape_field(&edge.to_claim_id),
            relation_to_str(&edge.relation),
            edge.strength
        ),
        PersistedRecord::ClaimVector(record) => format!(
            "V\t{}\t{}",
            escape_field(&record.claim_id),
            pack_f32_list(&record.values)
        ),
        PersistedRecord::BatchCommit(record) => format!(
            "B\t{}\t{}\t{}\t{}",
            escape_field(&record.commit_id),
            record.batch_size,
            record.ts_unix_ms,
            pack_string_list(&record.claim_ids)
        ),
    }
}

pub(crate) fn line_to_record(line: &str) -> Result<PersistedRecord, StoreError> {
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.is_empty() {
        return Err(StoreError::Parse("empty wal record".to_string()));
    }
    match parts[0] {
        "C" => {
            if !(parts.len() == 6 || parts.len() == 8 || parts.len() == 13) {
                return Err(StoreError::Parse(
                    "claim record has invalid field count".to_string(),
                ));
            }
            let event_time_unix = if parts[5] == "null" {
                None
            } else {
                Some(parts[5].parse::<i64>().map_err(|_| {
                    StoreError::Parse("claim record has invalid event_time".to_string())
                })?)
            };
            let entities = if parts.len() >= 8 {
                unpack_string_list(parts[6])?
            } else {
                Vec::new()
            };
            let embedding_ids = if parts.len() >= 8 {
                unpack_string_list(parts[7])?
            } else {
                Vec::new()
            };
            let claim_type = if parts.len() >= 13 {
                parse_optional_claim_type_field(parts[8])?
            } else {
                None
            };
            let valid_from = if parts.len() >= 13 {
                parse_optional_i64_field(parts[9], "valid_from")?
            } else {
                None
            };
            let valid_to = if parts.len() >= 13 {
                parse_optional_i64_field(parts[10], "valid_to")?
            } else {
                None
            };
            let created_at = if parts.len() >= 13 {
                parse_optional_i64_field(parts[11], "created_at")?
            } else {
                None
            };
            let updated_at = if parts.len() >= 13 {
                parse_optional_i64_field(parts[12], "updated_at")?
            } else {
                None
            };
            Ok(PersistedRecord::Claim(Claim {
                claim_id: unescape_field(parts[1])?,
                tenant_id: unescape_field(parts[2])?,
                canonical_text: unescape_field(parts[3])?,
                confidence: parts[4].parse::<f32>().map_err(|_| {
                    StoreError::Parse("claim record has invalid confidence".to_string())
                })?,
                event_time_unix,
                entities,
                embedding_ids,
                claim_type,
                valid_from,
                valid_to,
                created_at,
                updated_at,
            }))
        }
        "E" => {
            if !(parts.len() == 6 || parts.len() == 9 || parts.len() == 12) {
                return Err(StoreError::Parse(
                    "evidence record has invalid field count".to_string(),
                ));
            }
            let chunk_id = if parts.len() >= 9 {
                parse_optional_escaped_field(parts[6])?
            } else {
                None
            };
            let span_start = if parts.len() >= 9 {
                parse_optional_u32_field(parts[7], "span_start")?
            } else {
                None
            };
            let span_end = if parts.len() >= 9 {
                parse_optional_u32_field(parts[8], "span_end")?
            } else {
                None
            };
            let doc_id = if parts.len() >= 12 {
                parse_optional_escaped_field(parts[9])?
            } else {
                None
            };
            let extraction_model = if parts.len() >= 12 {
                parse_optional_escaped_field(parts[10])?
            } else {
                None
            };
            let ingested_at = if parts.len() >= 12 {
                parse_optional_i64_field(parts[11], "ingested_at")?
            } else {
                None
            };
            Ok(PersistedRecord::Evidence(Evidence {
                evidence_id: unescape_field(parts[1])?,
                claim_id: unescape_field(parts[2])?,
                source_id: unescape_field(parts[3])?,
                stance: str_to_stance(parts[4])?,
                source_quality: parts[5].parse::<f32>().map_err(|_| {
                    StoreError::Parse("evidence record has invalid source_quality".to_string())
                })?,
                chunk_id,
                span_start,
                span_end,
                doc_id,
                extraction_model,
                ingested_at,
            }))
        }
        "G" => {
            if parts.len() != 6 {
                return Err(StoreError::Parse(
                    "edge record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::Edge(ClaimEdge {
                edge_id: unescape_field(parts[1])?,
                from_claim_id: unescape_field(parts[2])?,
                to_claim_id: unescape_field(parts[3])?,
                relation: str_to_relation(parts[4])?,
                strength: parts[5].parse::<f32>().map_err(|_| {
                    StoreError::Parse("edge record has invalid strength".to_string())
                })?,
                reason_codes: vec![],
                created_at: None,
            }))
        }
        "V" => {
            if parts.len() != 3 {
                return Err(StoreError::Parse(
                    "vector record has invalid field count".to_string(),
                ));
            }
            Ok(PersistedRecord::ClaimVector(ClaimVectorRecord {
                claim_id: unescape_field(parts[1])?,
                values: unpack_f32_list(parts[2])?,
            }))
        }
        "B" => {
            if parts.len() != 5 {
                return Err(StoreError::Parse(
                    "batch commit record has invalid field count".to_string(),
                ));
            }
            let batch_size = parts[2].parse::<usize>().map_err(|_| {
                StoreError::Parse("batch commit record has invalid batch_size".to_string())
            })?;
            let ts_unix_ms = parts[3].parse::<u64>().map_err(|_| {
                StoreError::Parse("batch commit record has invalid ts_unix_ms".to_string())
            })?;
            Ok(PersistedRecord::BatchCommit(BatchCommitRecord {
                commit_id: unescape_field(parts[1])?,
                batch_size,
                ts_unix_ms,
                claim_ids: unpack_string_list(parts[4])?,
            }))
        }
        _ => Err(StoreError::Parse("unknown wal record kind".to_string())),
    }
}

fn escape_field(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\t', "\\t")
        .replace('\n', "\\n")
}

fn pack_string_list(values: &[String]) -> String {
    let mut out = String::new();
    for value in values {
        out.push_str(&format!("{}:", value.len()));
        out.push_str(value);
    }
    out
}

fn unpack_string_list(raw: &str) -> Result<Vec<String>, StoreError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let bytes = raw.as_bytes();
    let mut offset = 0usize;
    let mut out = Vec::new();
    while offset < bytes.len() {
        let len_start = offset;
        while offset < bytes.len() && bytes[offset].is_ascii_digit() {
            offset += 1;
        }
        if offset == len_start || offset >= bytes.len() || bytes[offset] != b':' {
            return Err(StoreError::Parse(
                "invalid packed list field in wal".to_string(),
            ));
        }
        let len = raw[len_start..offset]
            .parse::<usize>()
            .map_err(|_| StoreError::Parse("invalid packed list length in wal".to_string()))?;
        offset += 1;
        if offset + len > bytes.len() {
            return Err(StoreError::Parse(
                "packed list length exceeds wal field size".to_string(),
            ));
        }
        let value = std::str::from_utf8(&bytes[offset..offset + len])
            .map_err(|_| StoreError::Parse("invalid UTF-8 in packed list field".to_string()))?;
        out.push(value.to_string());
        offset += len;
    }
    Ok(out)
}

fn pack_f32_list(values: &[f32]) -> String {
    values
        .iter()
        .map(|value| value.to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn unpack_f32_list(raw: &str) -> Result<Vec<f32>, StoreError> {
    if raw.trim().is_empty() {
        return Err(StoreError::Parse("vector list cannot be empty".to_string()));
    }
    let mut values = Vec::new();
    for part in raw.split(',') {
        let parsed = part
            .parse::<f32>()
            .map_err(|_| StoreError::Parse("invalid vector value in wal".to_string()))?;
        if !parsed.is_finite() {
            return Err(StoreError::Parse(
                "non-finite vector value in wal".to_string(),
            ));
        }
        values.push(parsed);
    }
    if values.is_empty() {
        return Err(StoreError::Parse("vector list cannot be empty".to_string()));
    }
    Ok(values)
}

fn parse_optional_escaped_field(raw: &str) -> Result<Option<String>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    Ok(Some(unescape_field(raw)?))
}

fn parse_optional_u32_field(raw: &str, field: &str) -> Result<Option<u32>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    raw.parse::<u32>()
        .map(Some)
        .map_err(|_| StoreError::Parse(format!("evidence record has invalid {field}")))
}

fn parse_optional_i64_field(raw: &str, field: &str) -> Result<Option<i64>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    raw.parse::<i64>()
        .map(Some)
        .map_err(|_| StoreError::Parse(format!("claim record has invalid {field}")))
}

fn parse_optional_claim_type_field(raw: &str) -> Result<Option<ClaimType>, StoreError> {
    if raw == "null" {
        return Ok(None);
    }
    Ok(Some(str_to_claim_type(raw)?))
}

fn unescape_field(value: &str) -> Result<String, StoreError> {
    let mut output = String::with_capacity(value.len());
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            match ch {
                '\\' => output.push('\\'),
                't' => output.push('\t'),
                'n' => output.push('\n'),
                other => {
                    return Err(StoreError::Parse(format!(
                        "invalid escape sequence: \\{other}"
                    )));
                }
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        return Err(StoreError::Parse(
            "unterminated escape sequence in wal field".to_string(),
        ));
    }
    Ok(output)
}

fn stance_to_str(stance: &Stance) -> &'static str {
    match stance {
        Stance::Supports => "supports",
        Stance::Contradicts => "contradicts",
        Stance::Neutral => "neutral",
    }
}

fn claim_type_to_str(value: &ClaimType) -> &'static str {
    match value {
        ClaimType::Factual => "factual",
        ClaimType::Opinion => "opinion",
        ClaimType::Prediction => "prediction",
        ClaimType::Temporal => "temporal",
        ClaimType::Causal => "causal",
    }
}

fn str_to_claim_type(value: &str) -> Result<ClaimType, StoreError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "factual" => Ok(ClaimType::Factual),
        "opinion" => Ok(ClaimType::Opinion),
        "prediction" => Ok(ClaimType::Prediction),
        "temporal" => Ok(ClaimType::Temporal),
        "causal" => Ok(ClaimType::Causal),
        _ => Err(StoreError::Parse(
            "claim record has invalid claim_type".to_string(),
        )),
    }
}

fn str_to_stance(raw: &str) -> Result<Stance, StoreError> {
    match raw {
        "supports" => Ok(Stance::Supports),
        "contradicts" => Ok(Stance::Contradicts),
        "neutral" => Ok(Stance::Neutral),
        _ => Err(StoreError::Parse("invalid stance in wal".to_string())),
    }
}

fn relation_to_str(relation: &Relation) -> &'static str {
    match relation {
        Relation::Supports => "supports",
        Relation::Contradicts => "contradicts",
        Relation::Refines => "refines",
        Relation::Duplicates => "duplicates",
        Relation::DependsOn => "depends_on",
    }
}

fn str_to_relation(raw: &str) -> Result<Relation, StoreError> {
    match raw {
        "supports" => Ok(Relation::Supports),
        "contradicts" => Ok(Relation::Contradicts),
        "refines" => Ok(Relation::Refines),
        "duplicates" => Ok(Relation::Duplicates),
        "depends_on" => Ok(Relation::DependsOn),
        _ => Err(StoreError::Parse("invalid relation in wal".to_string())),
    }
}
