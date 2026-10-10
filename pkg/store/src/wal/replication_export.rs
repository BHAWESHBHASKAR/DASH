//! Chunked replication export: a follower that must rebuild its state
//! downloads the leader's export in bounded chunks instead of one response.
//!
//! Leader side ([`ReplicationExportStore`]):
//!
//! * `begin` freezes the replication view under the WAL lock: it opens the
//!   snapshot file (a checkpoint renames a new snapshot over it, so the open
//!   handle keeps reading the old one) and copies the WAL's replication lines
//!   into a temporary file. The WAL lock is then released and the export file
//!   `<wal>.exports/<id>.export` is written from the frozen inputs, streaming,
//!   followed by `<id>.manifest` (generation, record counts, size, SHA-256).
//!   Memory stays bounded by one line; disk holds one extra copy of the data
//!   set per retained export.
//! * The export file has the layout of the single-response export
//!   (`status`, `generation`, `snapshot_records`, `wal_records`, `SNAPSHOT`,
//!   snapshot lines, `WAL`, WAL lines), with the counts zero-padded.
//! * `read_chunk` serves `[offset, offset + max_bytes)` cut back to the last
//!   complete line, straight from the file (no WAL lock, no copy of the
//!   whole export). An export that is no longer retained answers
//!   [`ChunkRead::NotFound`]; the follower then starts over.
//! * An export is reused for the next `begin` while the leader's generation
//!   and replication view length are unchanged; the newest
//!   [`EXPORTS_RETAINED`] exports are kept, older ones and exports idle for
//!   longer than the TTL are deleted.
//!
//! Follower side ([`download_export`]): chunks are appended to
//! `<base>.part` (fsync per chunk) next to `<base>.manifest`, so an
//! interrupted download resumes from the part file's length with the same
//! export id. A complete part file is verified against the manifest's
//! SHA-256 before anything is applied; a mismatch discards it and asks the
//! leader for a different export. [`ReplicationExportFile`] then streams the
//! verified file into a fresh store and into the local WAL.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use super::replication_index::{ReplicationFilter, decode_line};
use super::{FileWal, SNAPSHOT_HEADER, sync_file, sync_parent_dir};
use crate::StoreError;

/// Chunk size a follower asks for unless configured otherwise.
pub const EXPORT_CHUNK_DEFAULT_BYTES: usize = 4 * 1024 * 1024;
/// Largest chunk the leader serves; larger requests are clamped.
pub const EXPORT_CHUNK_MAX_BYTES: usize = 32 * 1024 * 1024;
/// A chunk is extended past `max_bytes` to the end of a line at most up to
/// this size (a single WAL record is far smaller).
pub const EXPORT_LINE_MAX_BYTES: usize = 64 * 1024 * 1024;
/// Exports kept on the leader (newest first).
pub const EXPORTS_RETAINED: usize = 2;
/// An export nobody read for this long is deleted (also across restarts,
/// measured from its creation then).
pub const EXPORT_IDLE_TTL: Duration = Duration::from_secs(15 * 60);
/// Bytes reserved for the chunk response header in the follower's response
/// size limit.
pub const EXPORT_CHUNK_HEADER_RESERVE: usize = 4096;

const COUNT_WIDTH: usize = 20;
const EXPORT_SUFFIX: &str = ".export";
const MANIFEST_SUFFIX: &str = ".manifest";
const TMP_SUFFIX: &str = ".tmp";

/// What a follower needs to download, verify and continue from an export.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationExportManifest {
    pub export_id: String,
    /// WAL generation the export was frozen at.
    pub generation: u64,
    pub snapshot_records: usize,
    /// Replication view length at the freeze: the follower continues with
    /// delta frames from `(generation, wal_records)`.
    pub wal_records: usize,
    pub total_bytes: u64,
    /// Lowercase hex SHA-256 of the whole export file.
    pub sha256: String,
    pub created_unix_ms: u64,
}

impl ReplicationExportManifest {
    /// `key=value` lines (the on-disk manifest and, after `status=ok`, the
    /// body of `/internal/replication/export/begin`).
    pub fn render(&self) -> String {
        format!(
            "export_id={}\ngeneration={}\nsnapshot_records={}\nwal_records={}\ntotal_bytes={}\nsha256={}\ncreated_unix_ms={}\n",
            self.export_id,
            self.generation,
            self.snapshot_records,
            self.wal_records,
            self.total_bytes,
            self.sha256,
            self.created_unix_ms
        )
    }

    /// Wire body of `/internal/replication/export/begin`.
    pub fn render_response(&self) -> String {
        format!("status=ok\n{}", self.render())
    }

    /// Parses [`Self::render`] or [`Self::render_response`] output. Every
    /// field is required and validated.
    pub fn parse(body: &str) -> Result<Self, String> {
        let mut fields: HashMap<&str, &str> = HashMap::new();
        for line in body.lines() {
            if line.is_empty() {
                continue;
            }
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| "replication export manifest has a malformed line".to_string())?;
            if fields.insert(key, value).is_some() {
                return Err(format!("replication export manifest repeats '{key}'"));
            }
        }
        if let Some(status) = fields.get("status")
            && *status != "ok"
        {
            return Err("replication export manifest status is not ok".to_string());
        }
        let get = |key: &str| -> Result<&str, String> {
            fields
                .get(key)
                .copied()
                .ok_or_else(|| format!("replication export manifest missing '{key}'"))
        };
        let num = |key: &str| -> Result<u64, String> {
            get(key)?
                .parse::<u64>()
                .map_err(|_| format!("replication export manifest has invalid '{key}'"))
        };
        let export_id = get("export_id")?.to_string();
        if !valid_export_id(&export_id) {
            return Err("replication export manifest has an invalid export_id".to_string());
        }
        let sha256 = get("sha256")?.to_string();
        if sha256.len() != 64 || !sha256.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err("replication export manifest has an invalid sha256".to_string());
        }
        Ok(Self {
            export_id,
            generation: num("generation")?,
            snapshot_records: usize::try_from(num("snapshot_records")?)
                .map_err(|_| "snapshot_records out of range".to_string())?,
            wal_records: usize::try_from(num("wal_records")?)
                .map_err(|_| "wal_records out of range".to_string())?,
            total_bytes: num("total_bytes")?,
            sha256: sha256.to_ascii_lowercase(),
            created_unix_ms: num("created_unix_ms")?,
        })
    }
}

/// Export ids are 1 to 32 hex digits, so they are safe as file names.
pub fn valid_export_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 32 && id.bytes().all(|b| b.is_ascii_hexdigit())
}

/// One chunk of an export file. `data` is a run of complete lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationExportChunk {
    pub export_id: String,
    pub offset: u64,
    pub total_bytes: u64,
    pub data: String,
}

impl ReplicationExportChunk {
    pub fn next_offset(&self) -> u64 {
        self.offset + self.data.len() as u64
    }

    /// Wire body of `/internal/replication/export/chunk`.
    pub fn render_response(&self) -> String {
        let mut out = format!(
            "status=ok\nexport_id={}\noffset={}\nnext_offset={}\ntotal_bytes={}\ndata_bytes={}\nDATA\n",
            self.export_id,
            self.offset,
            self.next_offset(),
            self.total_bytes,
            self.data.len()
        );
        out.push_str(&self.data);
        out
    }

    pub fn parse_response(body: &str) -> Result<Self, String> {
        let marker = "\nDATA\n";
        let at = body
            .find(marker)
            .ok_or_else(|| "replication export chunk missing DATA marker".to_string())?;
        let (head, data) = (&body[..at], &body[at + marker.len()..]);
        let mut fields: HashMap<&str, &str> = HashMap::new();
        for line in head.lines() {
            let (key, value) = line
                .split_once('=')
                .ok_or_else(|| "replication export chunk has a malformed header".to_string())?;
            fields.insert(key, value);
        }
        if fields.get("status") != Some(&"ok") {
            return Err("replication export chunk status is not ok".to_string());
        }
        let num = |key: &str| -> Result<u64, String> {
            fields
                .get(key)
                .ok_or_else(|| format!("replication export chunk missing '{key}'"))?
                .parse::<u64>()
                .map_err(|_| format!("replication export chunk has invalid '{key}'"))
        };
        let export_id = fields
            .get("export_id")
            .ok_or_else(|| "replication export chunk missing 'export_id'".to_string())?
            .to_string();
        let offset = num("offset")?;
        let next_offset = num("next_offset")?;
        let total_bytes = num("total_bytes")?;
        let data_bytes = num("data_bytes")?;
        if data.len() as u64 != data_bytes || offset.checked_add(data_bytes) != Some(next_offset) {
            return Err("replication export chunk is truncated or inconsistent".to_string());
        }
        if next_offset > total_bytes {
            return Err("replication export chunk runs past the export".to_string());
        }
        if next_offset < total_bytes && !data.ends_with('\n') {
            return Err("replication export chunk does not end on a line boundary".to_string());
        }
        Ok(Self {
            export_id,
            offset,
            total_bytes,
            data: data.to_string(),
        })
    }
}

/// Outcome of [`ReplicationExportStore::read_chunk`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkRead {
    Chunk(ReplicationExportChunk),
    /// Unknown or no longer retained export: start over with `begin`.
    NotFound,
    /// The offset is past the end or not at a line boundary.
    BadOffset(String),
}

/// Leader-side store of exports, one per WAL (`<wal>.exports`). See the
/// module documentation.
#[derive(Debug)]
pub struct ReplicationExportStore {
    dir: PathBuf,
    /// Serialises `begin`, so concurrent resyncs share one export.
    build: Mutex<()>,
    last_access: Mutex<HashMap<String, Instant>>,
    retained: usize,
    idle_ttl: Duration,
}

impl ReplicationExportStore {
    /// The export directory of `wal_path`: `<wal>.exports`.
    pub fn dir_for_wal(wal_path: &Path) -> PathBuf {
        let mut dir = wal_path.as_os_str().to_owned();
        dir.push(".exports");
        PathBuf::from(dir)
    }

    /// Store for `wal_path`. Leftover temporary files of an interrupted
    /// build are removed.
    pub fn for_wal(wal_path: &Path) -> Self {
        let store = Self {
            dir: Self::dir_for_wal(wal_path),
            build: Mutex::new(()),
            last_access: Mutex::new(HashMap::new()),
            retained: EXPORTS_RETAINED,
            idle_ttl: EXPORT_IDLE_TTL,
        };
        store.remove_temporaries();
        store
    }

    /// Overrides retention (tests and tuning).
    pub fn with_retention(mut self, retained: usize, idle_ttl: Duration) -> Self {
        self.retained = retained.max(1);
        self.idle_ttl = idle_ttl;
        self
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn export_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}{EXPORT_SUFFIX}"))
    }

    fn manifest_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}{MANIFEST_SUFFIX}"))
    }

    fn remove_temporaries(&self) {
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return;
        };
        for entry in entries.flatten() {
            if entry.file_name().to_string_lossy().ends_with(TMP_SUFFIX) {
                let _ = fs::remove_file(entry.path());
            }
        }
    }

    /// The retained manifests, newest first.
    pub fn manifests(&self) -> Vec<ReplicationExportManifest> {
        let mut out = Vec::new();
        let Ok(entries) = fs::read_dir(&self.dir) else {
            return out;
        };
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(id) = name.strip_suffix(MANIFEST_SUFFIX) else {
                continue;
            };
            if !valid_export_id(id) {
                continue;
            }
            if let Some(manifest) = self.manifest(id) {
                out.push(manifest);
            }
        }
        out.sort_by(|a, b| {
            b.created_unix_ms
                .cmp(&a.created_unix_ms)
                .then_with(|| b.export_id.cmp(&a.export_id))
        });
        out
    }

    /// The manifest of a retained export whose file is present.
    pub fn manifest(&self, id: &str) -> Option<ReplicationExportManifest> {
        if !valid_export_id(id) {
            return None;
        }
        let text = fs::read_to_string(self.manifest_path(id)).ok()?;
        let manifest = ReplicationExportManifest::parse(&text).ok()?;
        let len = fs::metadata(self.export_path(id)).ok()?.len();
        (manifest.export_id == id && len == manifest.total_bytes).then_some(manifest)
    }

    /// Returns an export of the leader's current state: the newest retained
    /// one when the WAL generation and replication view length are
    /// unchanged and its id is not `avoid`, otherwise a new one. `wal` is
    /// locked only while the view is frozen, never while the export file is
    /// written.
    pub fn begin(
        &self,
        wal: &Mutex<FileWal>,
        avoid: Option<&str>,
    ) -> Result<ReplicationExportManifest, StoreError> {
        self.begin_with_hook(wal, avoid, &mut || {})
    }

    /// [`Self::begin`] that runs `after_freeze` once the view is frozen and
    /// the WAL lock released, before the export file is written (tests use
    /// it to checkpoint the leader in between).
    pub(crate) fn begin_with_hook(
        &self,
        wal: &Mutex<FileWal>,
        avoid: Option<&str>,
        after_freeze: &mut dyn FnMut(),
    ) -> Result<ReplicationExportManifest, StoreError> {
        let _build = self.build.lock().unwrap_or_else(|e| e.into_inner());
        self.prune();
        fs::create_dir_all(&self.dir)?;
        let id = loop {
            let candidate = format!("{:016x}", rand::random::<u64>());
            if !self.manifest_path(&candidate).exists() && Some(candidate.as_str()) != avoid {
                break candidate;
            }
        };
        let wal_tmp = self.dir.join(format!("{id}.wal{TMP_SUFFIX}"));
        let frozen = {
            let mut wal = wal.lock().unwrap_or_else(|e| e.into_inner());
            let (generation, view_len) = wal.replication_position()?;
            if let Some(latest) = self.manifests().into_iter().next()
                && latest.generation == generation
                && latest.wal_records == view_len
                && Some(latest.export_id.as_str()) != avoid
            {
                self.touch(&latest.export_id);
                return Ok(latest);
            }
            let mut out = BufWriter::new(File::create(&wal_tmp)?);
            let frozen = wal.freeze_for_export(&mut out);
            let frozen = frozen.and_then(|frozen| {
                out.flush()?;
                Ok(frozen)
            });
            match frozen {
                Ok(frozen) => frozen,
                Err(err) => {
                    drop(out);
                    let _ = fs::remove_file(&wal_tmp);
                    return Err(err);
                }
            }
        };
        after_freeze();
        let built = self.build_export(&id, frozen, &wal_tmp);
        let _ = fs::remove_file(&wal_tmp);
        let manifest = match built {
            Ok(manifest) => manifest,
            Err(err) => {
                let _ = fs::remove_file(self.export_path(&id));
                let _ = fs::remove_file(self.export_tmp_path(&id));
                return Err(err);
            }
        };
        self.touch(&id);
        self.prune();
        Ok(manifest)
    }

    fn export_tmp_path(&self, id: &str) -> PathBuf {
        self.dir.join(format!("{id}{EXPORT_SUFFIX}{TMP_SUFFIX}"))
    }

    fn build_export(
        &self,
        id: &str,
        frozen: ExportFreeze,
        wal_tmp: &Path,
    ) -> Result<ReplicationExportManifest, StoreError> {
        let tmp = self.export_tmp_path(id);
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        let mut out = BufWriter::new(&mut file);
        write_export_header(&mut out, frozen.generation, 0, 0)?;
        out.write_all(b"SNAPSHOT\n")?;
        let mut snapshot_records = 0usize;
        if let Some(snapshot) = frozen.snapshot {
            let mut filter = ReplicationFilter::new();
            let mut seen_header = false;
            for line in BufReader::new(snapshot).split(b'\n') {
                let line = line?;
                let Some(text) = decode_line(&line) else {
                    return Err(StoreError::Parse(
                        "snapshot file holds invalid UTF-8".to_string(),
                    ));
                };
                if text.trim().is_empty() {
                    continue;
                }
                if !seen_header {
                    if text != SNAPSHOT_HEADER {
                        return Err(StoreError::Parse(
                            "snapshot file has invalid header".to_string(),
                        ));
                    }
                    seen_header = true;
                    continue;
                }
                if filter.keep(&text) {
                    out.write_all(text.as_bytes())?;
                    out.write_all(b"\n")?;
                    snapshot_records += 1;
                }
            }
            if !seen_header {
                return Err(StoreError::Parse("snapshot file is empty".to_string()));
            }
        }
        out.write_all(b"WAL\n")?;
        let mut wal_records = 0usize;
        for line in BufReader::new(File::open(wal_tmp)?).split(b'\n') {
            let line = line?;
            out.write_all(&line)?;
            out.write_all(b"\n")?;
            wal_records += 1;
        }
        if wal_records != frozen.wal_records {
            return Err(StoreError::Io(format!(
                "replication export of {wal_records} WAL lines does not match the frozen view of {}",
                frozen.wal_records
            )));
        }
        out.flush()?;
        drop(out);
        file.seek(SeekFrom::Start(0))?;
        write_export_header(&mut file, frozen.generation, snapshot_records, wal_records)?;
        failpoint!("export.tmp_written");
        sync_file(&file)?;
        drop(file);
        let (total_bytes, sha256) = hash_file(&tmp)?;
        fs::rename(&tmp, self.export_path(id))?;
        let manifest = ReplicationExportManifest {
            export_id: id.to_string(),
            generation: frozen.generation,
            snapshot_records,
            wal_records,
            total_bytes,
            sha256,
            created_unix_ms: now_unix_ms(),
        };
        write_atomically(&self.manifest_path(id), manifest.render().as_bytes())?;
        Ok(manifest)
    }

    fn touch(&self, id: &str) {
        if let Ok(mut map) = self.last_access.lock() {
            map.insert(id.to_string(), Instant::now());
        }
    }

    /// Deletes exports beyond the retention count, exports idle for longer
    /// than the TTL and export files without a manifest.
    pub fn prune(&self) {
        let manifests = self.manifests();
        let now_ms = now_unix_ms();
        let mut keep = Vec::new();
        for (rank, manifest) in manifests.iter().enumerate() {
            let idle = match self
                .last_access
                .lock()
                .ok()
                .and_then(|map| map.get(&manifest.export_id).copied())
            {
                Some(at) => at.elapsed(),
                None => Duration::from_millis(now_ms.saturating_sub(manifest.created_unix_ms)),
            };
            if rank < self.retained && idle <= self.idle_ttl {
                keep.push(manifest.export_id.clone());
            } else {
                self.remove_export(&manifest.export_id);
            }
        }
        // Export files whose manifest is gone (or was never written).
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if let Some(id) = name.strip_suffix(EXPORT_SUFFIX)
                    && !keep.iter().any(|k| k == id)
                {
                    let _ = fs::remove_file(entry.path());
                }
            }
        }
    }

    fn remove_export(&self, id: &str) {
        let _ = fs::remove_file(self.manifest_path(id));
        let _ = fs::remove_file(self.export_path(id));
        if let Ok(mut map) = self.last_access.lock() {
            map.remove(id);
        }
    }

    /// Reads `[offset, offset + max_bytes)` of export `id`, cut back to the
    /// last complete line (extended to the end of the line when a single
    /// line is longer than `max_bytes`).
    pub fn read_chunk(
        &self,
        id: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<ChunkRead, StoreError> {
        let Some(manifest) = self.manifest(id) else {
            return Ok(ChunkRead::NotFound);
        };
        self.touch(id);
        let total = manifest.total_bytes;
        if offset > total {
            return Ok(ChunkRead::BadOffset(format!(
                "offset {offset} is past the end of the export ({total} bytes)"
            )));
        }
        let mut file = match File::open(self.export_path(id)) {
            Ok(file) => file,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Ok(ChunkRead::NotFound);
            }
            Err(err) => return Err(err.into()),
        };
        if offset > 0 {
            file.seek(SeekFrom::Start(offset - 1))?;
            let mut prev = [0u8; 1];
            file.read_exact(&mut prev)?;
            if prev[0] != b'\n' {
                return Ok(ChunkRead::BadOffset(format!(
                    "offset {offset} is not at a line boundary"
                )));
            }
        } else {
            file.seek(SeekFrom::Start(0))?;
        }
        let max_bytes = max_bytes.clamp(1, EXPORT_CHUNK_MAX_BYTES);
        let want = (total - offset).min(max_bytes as u64) as usize;
        let mut buf = vec![0u8; want];
        file.read_exact(&mut buf)?;
        let at_end = offset + want as u64 == total;
        if !at_end {
            match buf.iter().rposition(|b| *b == b'\n') {
                Some(pos) => buf.truncate(pos + 1),
                None => {
                    // One line longer than the chunk: extend to its end.
                    let mut reader = BufReader::new(file.take(EXPORT_LINE_MAX_BYTES as u64));
                    let mut rest = Vec::new();
                    reader.read_until(b'\n', &mut rest)?;
                    if rest.last() != Some(&b'\n') && offset + (want + rest.len()) as u64 != total {
                        return Err(StoreError::Io(format!(
                            "replication export line at offset {offset} exceeds {EXPORT_LINE_MAX_BYTES} bytes"
                        )));
                    }
                    buf.extend_from_slice(&rest);
                }
            }
        }
        let data = String::from_utf8(buf)
            .map_err(|_| StoreError::Parse("replication export holds invalid UTF-8".to_string()))?;
        Ok(ChunkRead::Chunk(ReplicationExportChunk {
            export_id: manifest.export_id,
            offset,
            total_bytes: total,
            data,
        }))
    }
}

/// The inputs of an export, frozen under the WAL lock.
pub(crate) struct ExportFreeze {
    pub(crate) generation: u64,
    pub(crate) wal_records: usize,
    /// Open handle on the snapshot at the freeze (`None`: no snapshot).
    pub(crate) snapshot: Option<File>,
}

fn write_export_header(
    out: &mut impl Write,
    generation: u64,
    snapshot_records: usize,
    wal_records: usize,
) -> std::io::Result<()> {
    write!(
        out,
        "status=ok\ngeneration={generation}\nsnapshot_records={snapshot_records:0width$}\nwal_records={wal_records:0width$}\n",
        width = COUNT_WIDTH
    )
}

/// `(length, lowercase hex SHA-256)` of a file, read in 1 MiB blocks.
pub fn hash_file(path: &Path) -> Result<(u64, String), StoreError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, hex::encode(hasher.finalize())))
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Temp file, fsync, rename, directory fsync.
fn write_atomically(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(TMP_SUFFIX);
    let tmp = PathBuf::from(tmp);
    {
        let mut file = File::create(&tmp)?;
        file.write_all(bytes)?;
        sync_file(&file)?;
    }
    fs::rename(&tmp, path)?;
    sync_parent_dir(path)?;
    Ok(())
}

// ---------------------------------------------------------------------
// Reading an export file
// ---------------------------------------------------------------------

/// Section of an export file a line belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportSection {
    Snapshot,
    Wal,
}

/// A downloaded (or locally built) export file: the single-response export
/// layout, read line by line so applying it needs no copy of the file in
/// memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicationExportFile {
    path: PathBuf,
    pub generation: u64,
    pub snapshot_records: usize,
    pub wal_records: usize,
}

impl ReplicationExportFile {
    /// Opens `path` and validates its header and section structure (one
    /// streaming pass; the record lines themselves are checked when they
    /// are applied).
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let path = path.as_ref().to_path_buf();
        let mut reader = BufReader::new(File::open(&path)?);
        if read_header(&mut reader, "status")? != "ok" {
            return Err(StoreError::Parse(
                "replication export status is not ok".to_string(),
            ));
        }
        let generation = parse_header_num("generation", read_header(&mut reader, "generation")?)?;
        let snapshot_records = parse_header_num(
            "snapshot_records",
            read_header(&mut reader, "snapshot_records")?,
        )? as usize;
        let wal_records =
            parse_header_num("wal_records", read_header(&mut reader, "wal_records")?)? as usize;
        expect_marker(&mut reader, "SNAPSHOT")?;
        skip_lines(&mut reader, snapshot_records, "snapshot")?;
        expect_marker(&mut reader, "WAL")?;
        skip_lines(&mut reader, wal_records, "WAL")?;
        let mut rest = Vec::new();
        reader.read_to_end(&mut rest)?;
        if rest.iter().any(|b| !b.is_ascii_whitespace()) {
            return Err(StoreError::Parse(
                "replication export has data after its last WAL line".to_string(),
            ));
        }
        Ok(Self {
            path,
            generation,
            snapshot_records,
            wal_records,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Calls `f` for every record line in file order (snapshot first).
    pub fn for_each_line(
        &self,
        mut f: impl FnMut(ExportSection, &str) -> Result<(), StoreError>,
    ) -> Result<(), StoreError> {
        let reader = BufReader::new(File::open(&self.path)?);
        let mut lines = reader.split(b'\n');
        // status, generation, snapshot_records, wal_records, SNAPSHOT
        for _ in 0..5 {
            lines.next().transpose()?;
        }
        for (section, count) in [
            (ExportSection::Snapshot, self.snapshot_records),
            (ExportSection::Wal, self.wal_records),
        ] {
            if section == ExportSection::Wal {
                lines.next().transpose()?; // WAL marker
            }
            for _ in 0..count {
                let raw = lines.next().transpose()?.ok_or_else(|| {
                    StoreError::Parse("replication export ended early".to_string())
                })?;
                let text = std::str::from_utf8(&raw).map_err(|_| {
                    StoreError::Parse("replication export holds invalid UTF-8".to_string())
                })?;
                f(section, text)?;
            }
        }
        Ok(())
    }
}

fn read_text_line(reader: &mut impl BufRead) -> Result<Option<String>, StoreError> {
    let mut line = String::new();
    if reader.read_line(&mut line)? == 0 {
        return Ok(None);
    }
    if line.ends_with('\n') {
        line.pop();
    }
    Ok(Some(line))
}

fn read_header(reader: &mut impl BufRead, key: &str) -> Result<String, StoreError> {
    read_text_line(reader)?
        .and_then(|line| line.strip_prefix(&format!("{key}=")).map(str::to_string))
        .ok_or_else(|| StoreError::Parse(format!("replication export missing '{key}'")))
}

fn parse_header_num(key: &str, raw: String) -> Result<u64, StoreError> {
    raw.parse::<u64>()
        .map_err(|_| StoreError::Parse(format!("replication export has invalid '{key}'")))
}

fn expect_marker(reader: &mut impl BufRead, marker: &str) -> Result<(), StoreError> {
    match read_text_line(reader)? {
        Some(line) if line == marker => Ok(()),
        _ => Err(StoreError::Parse(format!(
            "replication export missing {marker} marker"
        ))),
    }
}

fn skip_lines(reader: &mut impl BufRead, count: usize, what: &str) -> Result<(), StoreError> {
    let mut line = Vec::new();
    for _ in 0..count {
        line.clear();
        if reader.read_until(b'\n', &mut line)? == 0 || line.last() != Some(&b'\n') {
            return Err(StoreError::Parse(format!(
                "replication export is missing {what} lines"
            )));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Follower side: resumable download
// ---------------------------------------------------------------------

/// What the leader answered to a chunk request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkFetch {
    Chunk(ReplicationExportChunk),
    /// The export is gone on the leader (pruned or restarted elsewhere).
    NotFound,
}

/// The leader as seen by [`download_export`]. Implemented over HTTP by
/// the followers and directly over a [`ReplicationExportStore`] in tests.
pub trait ExportSource {
    /// `Ok(None)`: the leader has no chunked export (an older release).
    fn begin(&mut self, avoid: Option<&str>) -> Result<Option<ReplicationExportManifest>, String>;
    fn chunk(
        &mut self,
        export_id: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<ChunkFetch, String>;
}

/// A verified download.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadedExport {
    pub manifest: ReplicationExportManifest,
    pub file: ReplicationExportFile,
    /// Bytes fetched by this call (0 when a complete download was found).
    pub fetched_bytes: u64,
    /// Whether this call resumed a partial download.
    pub resumed: bool,
}

/// Result of [`download_export`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DownloadOutcome {
    Complete(DownloadedExport),
    /// The leader does not serve chunked exports.
    Unsupported,
}

/// Local files of a download: `<base>.part` and `<base>.manifest`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DownloadPaths {
    pub part: PathBuf,
    pub manifest: PathBuf,
}

impl DownloadPaths {
    pub fn new(base: impl AsRef<Path>) -> Self {
        let base = base.as_ref().as_os_str().to_owned();
        let mut part = base.clone();
        part.push(".part");
        let mut manifest = base;
        manifest.push(".manifest");
        Self {
            part: PathBuf::from(part),
            manifest: PathBuf::from(manifest),
        }
    }

    /// Download files next to a follower WAL: `<wal>.resync.*`.
    pub fn for_wal(wal_path: &Path) -> Self {
        let mut base = wal_path.as_os_str().to_owned();
        base.push(".resync");
        Self::new(PathBuf::from(base))
    }

    /// Deletes both files (after the export was applied, or to restart).
    pub fn remove(&self) {
        let _ = fs::remove_file(&self.part);
        let _ = fs::remove_file(&self.manifest);
    }

    fn saved_manifest(&self) -> Option<ReplicationExportManifest> {
        let text = fs::read_to_string(&self.manifest).ok()?;
        ReplicationExportManifest::parse(&text).ok()
    }
}

/// Downloads the leader's export into `paths.part`, resuming a partial
/// download of the same export, and verifies it against the manifest's
/// SHA-256. Network errors leave the part file in place for the next call;
/// a checksum mismatch or an export the leader no longer has discards it.
pub fn download_export(
    source: &mut dyn ExportSource,
    paths: &DownloadPaths,
    chunk_bytes: usize,
) -> Result<DownloadOutcome, String> {
    let chunk_bytes = chunk_bytes.clamp(1, EXPORT_CHUNK_MAX_BYTES);
    let io =
        |what: &str, err: std::io::Error| format!("replication export download: {what}: {err}");
    if let Some(parent) = paths.part.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent).map_err(|e| io("create directory", e))?;
    }
    let mut avoid: Option<String> = None;
    let mut fetched = 0u64;
    let mut resumed = false;
    // Each restart (lost export, checksum mismatch) asks for a new export;
    // a leader that keeps failing is reported after a few attempts.
    for _attempt in 0..4 {
        let manifest = match paths.saved_manifest().filter(|_| paths.part.exists()) {
            Some(saved) => {
                resumed = resumed || fetched == 0;
                saved
            }
            None => {
                paths.remove();
                let Some(manifest) = source.begin(avoid.as_deref())? else {
                    return Ok(DownloadOutcome::Unsupported);
                };
                write_atomically(&paths.manifest, manifest.render().as_bytes())
                    .map_err(|e| format!("replication export download: save manifest: {e:?}"))?;
                File::create(&paths.part).map_err(|e| io("create part file", e))?;
                resumed = false;
                manifest
            }
        };
        let mut part = OpenOptions::new()
            .append(true)
            .open(&paths.part)
            .map_err(|e| io("open part file", e))?;
        let mut have = part.metadata().map_err(|e| io("stat part file", e))?.len();
        let mut lost = false;
        while have < manifest.total_bytes {
            match source.chunk(&manifest.export_id, have, chunk_bytes)? {
                ChunkFetch::NotFound => {
                    lost = true;
                    break;
                }
                ChunkFetch::Chunk(chunk) => {
                    if chunk.export_id != manifest.export_id
                        || chunk.offset != have
                        || chunk.total_bytes != manifest.total_bytes
                        || chunk.data.is_empty()
                    {
                        return Err(
                            "replication export chunk does not continue the download".to_string()
                        );
                    }
                    part.write_all(chunk.data.as_bytes())
                        .map_err(|e| io("write part file", e))?;
                    sync_file(&part).map_err(|e| io("fsync part file", e))?;
                    have = chunk.next_offset();
                    fetched += chunk.data.len() as u64;
                }
            }
        }
        drop(part);
        if lost {
            eprintln!(
                "replication export {} is no longer available on the leader; starting a new download",
                manifest.export_id
            );
            paths.remove();
            continue;
        }
        let (len, sha256) =
            hash_file(&paths.part).map_err(|e| format!("replication export download: {e:?}"))?;
        if len != manifest.total_bytes || sha256 != manifest.sha256 {
            eprintln!(
                "replication export {} failed verification (bytes {len} of {}, checksum {}); discarding it",
                manifest.export_id,
                manifest.total_bytes,
                if sha256 == manifest.sha256 {
                    "ok"
                } else {
                    "mismatch"
                }
            );
            paths.remove();
            avoid = Some(manifest.export_id.clone());
            continue;
        }
        let file = ReplicationExportFile::open(&paths.part)
            .map_err(|e| format!("replication export download: invalid export: {e:?}"))?;
        if file.generation != manifest.generation
            || file.snapshot_records != manifest.snapshot_records
            || file.wal_records != manifest.wal_records
        {
            paths.remove();
            return Err("replication export header does not match its manifest".to_string());
        }
        return Ok(DownloadOutcome::Complete(DownloadedExport {
            manifest,
            file,
            fetched_bytes: fetched,
            resumed,
        }));
    }
    Err("replication export download failed verification repeatedly".to_string())
}

/// [`ExportSource`] over the leader's HTTP endpoints. `fetch` performs one
/// GET of a path relative to the leader (for example
/// `/internal/replication/export/begin`) and returns `(status, body)`.
pub struct HttpExportSource<F> {
    fetch: F,
}

impl<F> HttpExportSource<F>
where
    F: FnMut(&str) -> Result<(u16, String), String>,
{
    pub fn new(fetch: F) -> Self {
        Self { fetch }
    }
}

impl<F> ExportSource for HttpExportSource<F>
where
    F: FnMut(&str) -> Result<(u16, String), String>,
{
    fn begin(&mut self, avoid: Option<&str>) -> Result<Option<ReplicationExportManifest>, String> {
        let path = match avoid {
            Some(id) => format!("/internal/replication/export/begin?avoid={id}"),
            None => "/internal/replication/export/begin".to_string(),
        };
        let (status, body) = (self.fetch)(&path)?;
        match status {
            200 => ReplicationExportManifest::parse(&body).map(Some),
            404 => Ok(None),
            other => Err(format!(
                "replication export begin returned status {other} ({})",
                body.chars().take(200).collect::<String>()
            )),
        }
    }

    fn chunk(
        &mut self,
        export_id: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<ChunkFetch, String> {
        let path = format!(
            "/internal/replication/export/chunk?export_id={export_id}&offset={offset}&max_bytes={max_bytes}"
        );
        let (status, body) = (self.fetch)(&path)?;
        match status {
            200 => ReplicationExportChunk::parse_response(&body).map(ChunkFetch::Chunk),
            404 | 410 => Ok(ChunkFetch::NotFound),
            other => Err(format!(
                "replication export chunk returned status {other} ({})",
                body.chars().take(200).collect::<String>()
            )),
        }
    }
}

/// [`ExportSource`] reading a [`ReplicationExportStore`] directly (tests
/// and tools; no HTTP).
pub struct LocalExportSource<'a> {
    pub store: &'a ReplicationExportStore,
    pub wal: &'a Mutex<FileWal>,
}

impl ExportSource for LocalExportSource<'_> {
    fn begin(&mut self, avoid: Option<&str>) -> Result<Option<ReplicationExportManifest>, String> {
        self.store
            .begin(self.wal, avoid)
            .map(Some)
            .map_err(|e| format!("{e:?}"))
    }

    fn chunk(
        &mut self,
        export_id: &str,
        offset: u64,
        max_bytes: usize,
    ) -> Result<ChunkFetch, String> {
        match self
            .store
            .read_chunk(export_id, offset, max_bytes)
            .map_err(|e| format!("{e:?}"))?
        {
            ChunkRead::Chunk(chunk) => Ok(ChunkFetch::Chunk(chunk)),
            ChunkRead::NotFound => Ok(ChunkFetch::NotFound),
            ChunkRead::BadOffset(reason) => Err(reason),
        }
    }
}
