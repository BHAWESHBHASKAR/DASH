//! Shared helpers for the upgrade/downgrade compatibility tests.
//!
//! Every fixture under `tests/compat/fixtures/<label>/` was captured from a
//! released build by `scripts/compat/generate_fixtures.sh` (see
//! `tests/compat/README.md`). The tests start the CURRENT code on a scratch
//! copy of each fixture and compare what it serves with what the old build
//! answered for the same requests.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use serde_json::Value;

/// On-disk format generation a fixture was written in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Era {
    /// 0.2.x (`main` before the hardening work): unchecksummed WAL records
    /// (`C`, `E`, `G`, `V`, `B`), no WAL generation file, bincode redb
    /// values without a header, segment directories named by the lossy
    /// sanitizer, unchained-by-version audit lines, 3-field lease file,
    /// replication frames without a generation.
    V0_2,
    /// 0.3.0 (hardening): checksummed records (`C2`..`T2`), commit groups,
    /// `<wal>.gen`, `DASHv2` redb header, persisted vector index (format 1),
    /// tenant markers, audit record version 2, 4-field lease + epoch floor,
    /// replication frames tagged with the generation.
    V0_3,
}

/// One captured fixture.
#[derive(Debug, Clone, Copy)]
pub struct Fixture {
    pub label: &'static str,
    pub era: Era,
    /// The release had deletes (the fixture holds `T2` tombstones).
    pub has_deletes: bool,
}

/// Every fixture the tests run against, oldest first. A new release adds its
/// fixture here (the `every_fixture_directory_is_registered` test fails
/// until it does).
pub const FIXTURES: &[Fixture] = &[
    Fixture {
        label: "v0.2-main-ae86667",
        era: Era::V0_2,
        has_deletes: false,
    },
    Fixture {
        label: "v0.3.0-dev",
        era: Era::V0_3,
        has_deletes: true,
    },
];

/// Credentials the scenario configured (scripts/compat/run_scenario.py).
pub const INGEST_KEY: &str = "compat-ingest-key-0123456789abcdef0123";
pub const RETRIEVE_KEY: &str = "compat-retrieve-key-0123456789abcdef01";
pub const REPLICATION_TOKEN: &str = "compat-replication-token-0123456789ab";

pub fn compat_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn repo_root() -> PathBuf {
    compat_root()
        .parent()
        .and_then(Path::parent)
        .expect("tests/compat lives two levels below the repository root")
        .to_path_buf()
}

pub fn fixtures_dir() -> PathBuf {
    compat_root().join("fixtures")
}

/// Serializes tests that change process-wide environment variables.
pub fn env_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[allow(unused_unsafe)]
pub fn set_env(key: &str, value: &str) {
    unsafe { std::env::set_var(key, value) };
}

#[allow(unused_unsafe)]
pub fn remove_env(key: &str) {
    unsafe { std::env::remove_var(key) };
}

impl Fixture {
    pub fn dir(&self) -> PathBuf {
        fixtures_dir().join(self.label)
    }

    pub fn path(&self, relative: &str) -> PathBuf {
        self.dir().join(relative)
    }

    pub fn read_jsonl(&self, relative: &str) -> Vec<Value> {
        read_jsonl(&self.path(relative))
    }

    /// The fixture's `state/` copied into a fresh temporary directory (the
    /// tests mutate it), with `ingest.redb.gz` decompressed to `ingest.redb`.
    pub fn scratch_state(&self) -> ScratchState {
        let dir = tempfile::tempdir().expect("tempdir");
        copy_tree(&self.path("state"), dir.path());
        let gz = dir.path().join("ingest.redb.gz");
        if gz.exists() {
            let mut decoder = flate2::read::GzDecoder::new(fs::File::open(&gz).expect("open gz"));
            let mut bytes = Vec::new();
            decoder.read_to_end(&mut bytes).expect("decompress redb");
            fs::write(dir.path().join("ingest.redb"), bytes).expect("write redb");
            fs::remove_file(&gz).expect("remove gz copy");
        }
        ScratchState { dir }
    }

    /// Claims the node held when the fixture was captured: the last
    /// `claims_total` the old build reported (after its deletes, if any).
    pub fn expected_claims_total(&self) -> usize {
        let deletes = self.read_jsonl("http/delete-responses.jsonl");
        let last = deletes
            .iter()
            .rev()
            .find_map(|row| row.pointer("/body/claims_total").and_then(Value::as_u64))
            .or_else(|| {
                self.read_jsonl("http/ingest-responses.jsonl")
                    .iter()
                    .rev()
                    .find_map(|row| row.pointer("/body/claims_total").and_then(Value::as_u64))
            })
            .expect("fixture records claims_total");
        usize::try_from(last).expect("fits usize")
    }
}

/// A scratch copy of a fixture's state directory.
pub struct ScratchState {
    dir: tempfile::TempDir,
}

impl ScratchState {
    pub fn path(&self) -> &Path {
        self.dir.path()
    }

    pub fn wal(&self) -> PathBuf {
        self.dir.path().join("ingest.wal")
    }

    pub fn snapshot(&self) -> PathBuf {
        self.dir.path().join("ingest.wal.snapshot")
    }

    pub fn generation_file(&self) -> PathBuf {
        self.dir.path().join("ingest.wal.gen")
    }

    pub fn vindex(&self) -> PathBuf {
        self.dir.path().join("ingest.wal.vindex")
    }

    pub fn redb(&self) -> PathBuf {
        self.dir.path().join("ingest.redb")
    }

    pub fn segments(&self) -> PathBuf {
        self.dir.path().join("segments")
    }
}

pub fn read_jsonl(path: &Path) -> Vec<Value> {
    let text = fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    text.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).expect("valid JSON line"))
        .collect()
}

pub fn copy_tree(from: &Path, to: &Path) {
    fs::create_dir_all(to).expect("create dir");
    let mut entries: Vec<_> = fs::read_dir(from)
        .unwrap_or_else(|e| panic!("read {}: {e}", from.display()))
        .map(|entry| entry.expect("dir entry"))
        .collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}

/// The dataset's retrieve requests (tests/compat/dataset/retrieve.jsonl).
pub fn retrieve_requests() -> Vec<Value> {
    read_jsonl(&compat_root().join("dataset/retrieve.jsonl"))
}

/// The dataset's ingest requests (tests/compat/dataset/ingest.jsonl).
pub fn ingest_requests() -> Vec<Value> {
    read_jsonl(&compat_root().join("dataset/ingest.jsonl"))
}

/// Raw HTTP/1.1 request bytes for one dataset row.
pub fn raw_request(
    method: &str,
    path: &str,
    body: Option<&Value>,
    headers: &[(&str, &str)],
) -> Vec<u8> {
    let body = body.map(|b| b.to_string()).unwrap_or_default();
    let mut out = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n",
        body.len()
    );
    if !body.is_empty() {
        out.push_str("Content-Type: application/json\r\n");
    }
    for (name, value) in headers {
        out.push_str(&format!("{name}: {value}\r\n"));
    }
    out.push_str("\r\n");
    out.push_str(&body);
    out.into_bytes()
}

/// Splits a raw HTTP response into status code and parsed JSON body.
pub fn parse_response(raw: &[u8]) -> (u16, Value) {
    let text = String::from_utf8_lossy(raw);
    let (head, body) = text.split_once("\r\n\r\n").unwrap_or((&text, ""));
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    let value = serde_json::from_str(body).unwrap_or_else(|_| Value::String(body.to_string()));
    (status, value)
}

/// Record kind of a WAL or snapshot line (`C`, `C2`, `T2`, ...).
pub fn record_kind(line: &str) -> &str {
    line.split('\t').next().unwrap_or("")
}

/// Record kinds of every non-empty line of `path`, skipping the snapshot
/// header.
pub fn record_kinds(path: &Path) -> Vec<String> {
    let text = fs::read_to_string(path).unwrap_or_default();
    text.lines()
        .filter(|line| !line.trim().is_empty() && *line != "SNAP\t1")
        .map(|line| record_kind(line).to_string())
        .collect()
}

/// Whether the operator upgrade guide (`docs/operations/upgrades.md`) states
/// `phrase` (case and line breaks ignored). Downgrade rules that cannot be
/// executed against an old binary are asserted to be documented there.
pub fn upgrade_guide_mentions(phrase: &str) -> bool {
    let normalize = |text: &str| {
        text.split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
    };
    let guide = fs::read_to_string(repo_root().join("docs/operations/upgrades.md"))
        .expect("docs/operations/upgrades.md exists");
    normalize(&guide).contains(&normalize(phrase))
}

/// Oracles that reproduce what OLDER releases accept, copied from their
/// source, so downgrade constraints are checked by executing the old rules
/// rather than only stated in prose.
pub mod old_readers {
    /// Record kinds the 0.2 WAL reader understands
    /// (`pkg/store/src/wal.rs::line_to_record` at `main` ae86667: any other
    /// kind is `unknown wal record type` and fails the whole replay).
    pub const V0_2_WAL_KINDS: &[&str] = &["C", "E", "G", "V", "B"];

    /// Record kinds a 0.3 build from before deletes understands: the legacy
    /// kinds plus the checksummed ones, without `T2` (a checksummed record of
    /// an unknown kind fails its replay).
    pub const V0_3_PRE_DELETE_WAL_KINDS: &[&str] =
        &["C", "E", "G", "V", "B", "C2", "E2", "G2", "V2", "B2"];

    /// The 0.2 lease reader (`services/control-plane/src/leader.rs::read_lease`
    /// at `main` ae86667): exactly `node_id,epoch,expires_at_ms`.
    pub fn v0_2_reads_lease(text: &str) -> Result<(String, u64, u64), String> {
        let line = text.lines().next().unwrap_or("").trim();
        let parts: Vec<&str> = line.split(',').collect();
        if parts.len() != 3 {
            return Err(
                "lease file has invalid format (expected node_id,epoch,expires_at_ms)".into(),
            );
        }
        let epoch = parts[1]
            .parse::<u64>()
            .map_err(|_| "invalid epoch".to_string())?;
        let expires = parts[2]
            .parse::<u64>()
            .map_err(|_| "invalid expires_at_ms".to_string())?;
        Ok((parts[0].to_string(), epoch, expires))
    }

    /// Header parsing of the 0.2 replication follower
    /// (`services/ingestion/src/transport/replication.rs::parse_replication_delta_frame`
    /// at `main` ae86667): the second line must be `needs_resync=`.
    pub fn v0_2_parses_delta_header(body: &str) -> Result<(), String> {
        let mut lines = body.lines();
        for key in [
            "status",
            "needs_resync",
            "from_offset",
            "next_offset",
            "total_records",
            "records",
        ] {
            let line = lines.next().ok_or_else(|| format!("missing {key}"))?;
            let (found, _) = line
                .split_once('=')
                .ok_or_else(|| format!("invalid {key} line"))?;
            if found != key {
                return Err(format!("expected key '{key}', found '{found}'"));
            }
        }
        Ok(())
    }

    /// Whether a 0.2 build can decode an evidence or edge blob from redb.
    /// 0.2 reads the claim's current blob (`Vec<Evidence>` /
    /// `Vec<ClaimEdge>`) with `bincode` 1.x on EVERY evidence or edge write
    /// (`apply_evidence` / `apply_edge` at `main` ae86667) and fails the
    /// write when it cannot. bincode starts a `Vec` with its element count
    /// as a little-endian `u64`; a count larger than the bytes that follow
    /// can never decode (each element takes at least one byte). The
    /// `DASHv2\0\xff` header read as that count is above 2^63.
    pub fn v0_2_decodes_redb_blob(bytes: &[u8]) -> bool {
        let Some(prefix) = bytes.get(..8) else {
            return false;
        };
        let count = u64::from_le_bytes(prefix.try_into().expect("8 bytes"));
        count <= (bytes.len() - 8) as u64
    }
}
