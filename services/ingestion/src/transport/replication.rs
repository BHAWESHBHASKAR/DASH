//! Ingestion-side replication: the leader endpoints' wire format and the
//! pull follower.
//!
//! Protocol rules (REP-01..REP-06):
//!
//! * Every WAL frame and export carries the leader's WAL generation. The
//!   follower persists `(generation, offset)` together (atomic write +
//!   fsync) and sends `from_generation` on each poll; a mismatch makes the
//!   leader answer `needs_resync=1`.
//! * A resync REPLACES the follower's state with the export (fresh store,
//!   swapped in while keeping the redb handle, which is rewritten from the
//!   new state) and continues from `offset = export.wal_lines.len()`.
//! * A delta batch is applied to a detached clone and committed only if
//!   every record applied.
//! * Responses are size- and time-bounded and numeric headers are
//!   validated before use; failures back off and surface in `/ready` and
//!   `/metrics`.

use std::{
    collections::HashSet,
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    panic::{AssertUnwindSafe, catch_unwind},
    time::{Duration, Instant},
};

use store::{InMemoryStore, StoreError, WalReplicationExport, WalReplicationFrame};

use super::{IngestionRuntime, SharedRuntime, http::HttpRequest};

const DEFAULT_REPLICATION_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_REPLICATION_MAX_RECORDS: usize = 512;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_BACKOFF_MS: u64 = 30_000;
const DEFAULT_MAX_LAG_RECORDS: usize = 100_000;
const DEFAULT_MAX_STALENESS_MS: u64 = 300_000;
const ACK_MAX_RESPONSE_BYTES: usize = 64 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const IO_TIMEOUT: Duration = Duration::from_secs(10);
const REQUEST_DEADLINE: Duration = Duration::from_secs(60);
const HEADER_SLACK_BYTES: usize = 16 * 1024;
const PREALLOC_CAP: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationPullConfig {
    pub(crate) source_base_url: String,
    pub(crate) poll_interval: Duration,
    pub(crate) max_records: usize,
    pub(crate) token: Option<String>,
    pub(crate) local_replica_id: Option<String>,
    /// Explicit state-file path. `None` derives `<wal path>.replication`
    /// from the ingestion WAL; with no WAL the state is in-memory only.
    pub(crate) offset_path: Option<String>,
    pub(crate) max_response_bytes: usize,
    pub(crate) max_backoff: Duration,
    pub(crate) max_lag_records: usize,
    pub(crate) max_staleness_ms: u64,
}

impl ReplicationPullConfig {
    pub(crate) fn new(source_base_url: impl Into<String>) -> Self {
        Self {
            source_base_url: source_base_url
                .into()
                .trim()
                .trim_end_matches('/')
                .to_string(),
            poll_interval: Duration::from_millis(DEFAULT_REPLICATION_POLL_INTERVAL_MS),
            max_records: DEFAULT_REPLICATION_MAX_RECORDS,
            token: None,
            local_replica_id: None,
            offset_path: None,
            max_response_bytes: DEFAULT_MAX_RESPONSE_BYTES,
            max_backoff: Duration::from_millis(DEFAULT_MAX_BACKOFF_MS),
            max_lag_records: DEFAULT_MAX_LAG_RECORDS,
            max_staleness_ms: DEFAULT_MAX_STALENESS_MS,
        }
    }

    pub(crate) fn from_env() -> Option<Self> {
        let source_base_url = env_with_fallback(
            "DASH_INGEST_REPLICATION_SOURCE_URL",
            "EME_INGEST_REPLICATION_SOURCE_URL",
        )?;
        let mut config = Self::new(source_base_url);
        if config.source_base_url.is_empty() {
            return None;
        }
        if let Some(ms) = env_u64(
            "DASH_INGEST_REPLICATION_POLL_INTERVAL_MS",
            "EME_INGEST_REPLICATION_POLL_INTERVAL_MS",
        ) {
            config.poll_interval = Duration::from_millis(ms);
        }
        if let Some(n) = env_u64(
            "DASH_INGEST_REPLICATION_MAX_RECORDS",
            "EME_INGEST_REPLICATION_MAX_RECORDS",
        ) {
            config.max_records = n as usize;
        }
        if let Some(n) = env_u64(
            "DASH_INGEST_REPLICATION_MAX_RESPONSE_BYTES",
            "EME_INGEST_REPLICATION_MAX_RESPONSE_BYTES",
        ) {
            config.max_response_bytes = n as usize;
        }
        if let Some(ms) = env_u64(
            "DASH_INGEST_REPLICATION_MAX_BACKOFF_MS",
            "EME_INGEST_REPLICATION_MAX_BACKOFF_MS",
        ) {
            config.max_backoff = Duration::from_millis(ms);
        }
        if let Some(n) = env_u64(
            "DASH_INGEST_REPLICATION_MAX_LAG_RECORDS",
            "EME_INGEST_REPLICATION_MAX_LAG_RECORDS",
        ) {
            config.max_lag_records = n as usize;
        }
        if let Some(ms) = env_u64(
            "DASH_INGEST_REPLICATION_MAX_STALENESS_MS",
            "EME_INGEST_REPLICATION_MAX_STALENESS_MS",
        ) {
            config.max_staleness_ms = ms;
        }
        config.token = replication_token();
        config.offset_path = env_with_fallback(
            "DASH_INGEST_REPLICATION_OFFSET_PATH",
            "EME_INGEST_REPLICATION_OFFSET_PATH",
        )
        .filter(|value| !value.trim().is_empty());
        config.local_replica_id =
            env_with_fallback("DASH_ROUTER_LOCAL_NODE_ID", "EME_ROUTER_LOCAL_NODE_ID")
                .or_else(|| env_with_fallback("DASH_NODE_ID", "EME_NODE_ID"))
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty());
        Some(config)
    }

    pub(crate) fn wal_pull_url(&self, from_offset: usize, from_generation: Option<u64>) -> String {
        let mut url = format!(
            "{}/internal/replication/wal?from_offset={from_offset}&max_records={}",
            self.source_base_url, self.max_records
        );
        if let Some(generation) = from_generation {
            url.push_str(&format!("&from_generation={generation}"));
        }
        url
    }

    pub(crate) fn export_url(&self) -> String {
        format!("{}/internal/replication/export", self.source_base_url)
    }

    pub(crate) fn ack_url(&self, commit_id: &str) -> Option<String> {
        let replica_id = self.local_replica_id.as_deref()?;
        Some(format!(
            "{}/internal/replication/ack?commit_id={}&replica_id={}",
            self.source_base_url,
            url_encode_component(commit_id),
            url_encode_component(replica_id)
        ))
    }

    /// Delay before the next pull after `consecutive_failures` failures in
    /// a row: the poll interval doubled per failure, capped at
    /// `max_backoff`.
    pub(crate) fn backoff_delay(&self, consecutive_failures: u64) -> Duration {
        if consecutive_failures == 0 {
            return self.poll_interval;
        }
        let shift = (consecutive_failures - 1).min(16) as u32;
        let scaled = self.poll_interval.saturating_mul(1u32 << shift);
        scaled.min(self.max_backoff.max(self.poll_interval))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationSourceResponse {
    pub(crate) status: u16,
    pub(crate) body: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationDeltaFrame {
    pub(crate) generation: Option<u64>,
    pub(crate) needs_resync: bool,
    pub(crate) from_offset: usize,
    pub(crate) next_offset: usize,
    pub(crate) total_records: usize,
    pub(crate) wal_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationExportFrame {
    pub(crate) generation: Option<u64>,
    pub(crate) snapshot_lines: Vec<String>,
    pub(crate) wal_lines: Vec<String>,
}

pub(crate) fn request_replication_source(
    url: &str,
    token: Option<&str>,
    max_body_bytes: usize,
) -> Result<ReplicationSourceResponse, String> {
    request_replication_source_with_method(url, token, "GET", max_body_bytes)
}

fn request_replication_ack(
    url: &str,
    token: Option<&str>,
) -> Result<ReplicationSourceResponse, String> {
    request_replication_source_with_method(url, token, "POST", ACK_MAX_RESPONSE_BYTES)
}

fn request_replication_source_with_method(
    url: &str,
    token: Option<&str>,
    method: &str,
    max_body_bytes: usize,
) -> Result<ReplicationSourceResponse, String> {
    let (authority, path) = parse_http_url(url)?;
    let addrs = authority
        .to_socket_addrs()
        .map_err(|err| format!("failed resolving replication source '{authority}': {err}"))?;
    let mut stream = None;
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, CONNECT_TIMEOUT) {
            Ok(s) => {
                stream = Some(s);
                break;
            }
            Err(err) => last_err = Some(err),
        }
    }
    let mut stream = stream.ok_or_else(|| {
        format!(
            "failed connecting replication source '{authority}': {}",
            last_err
                .map(|e| e.to_string())
                .unwrap_or_else(|| "no addresses".to_string())
        )
    })?;
    stream
        .set_read_timeout(Some(IO_TIMEOUT))
        .and_then(|()| stream.set_write_timeout(Some(IO_TIMEOUT)))
        .map_err(|err| format!("failed setting socket timeouts: {err}"))?;
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: 0\r\n"
    );
    if let Some(token) = token {
        request.push_str(&format!("x-replication-token: {token}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .and_then(|()| stream.flush())
        .map_err(|err| format!("failed sending replication request: {err}"))?;

    let limit = max_body_bytes.saturating_add(HEADER_SLACK_BYTES);
    let deadline = Instant::now() + REQUEST_DEADLINE;
    let mut bytes = Vec::new();
    let mut chunk = [0u8; 64 * 1024];
    loop {
        if Instant::now() > deadline {
            return Err("replication response timed out".to_string());
        }
        let n = stream
            .read(&mut chunk)
            .map_err(|err| format!("failed reading replication response: {err}"))?;
        if n == 0 {
            break;
        }
        if bytes.len() + n > limit {
            return Err(format!(
                "replication response exceeds {max_body_bytes} byte limit"
            ));
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    parse_http_response(&bytes, max_body_bytes)
}

fn parse_http_response(
    bytes: &[u8],
    max_body_bytes: usize,
) -> Result<ReplicationSourceResponse, String> {
    let split = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| "replication response missing HTTP header terminator".to_string())?;
    let header_block = std::str::from_utf8(&bytes[..split])
        .map_err(|_| "replication response headers are not valid UTF-8".to_string())?;
    let body_bytes = &bytes[split + 4..];
    let mut header_lines = header_block.lines();
    let status_line = header_lines
        .next()
        .ok_or_else(|| "replication response missing status line".to_string())?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "replication response status line missing code".to_string())?
        .parse::<u16>()
        .map_err(|_| "replication response has invalid status code".to_string())?;
    for line in header_lines {
        if let Some((name, value)) = line.split_once(':')
            && name.trim().eq_ignore_ascii_case("content-length")
        {
            let declared = value
                .trim()
                .parse::<usize>()
                .map_err(|_| "replication response has invalid Content-Length".to_string())?;
            if declared > max_body_bytes {
                return Err(format!(
                    "replication response exceeds {max_body_bytes} byte limit"
                ));
            }
            if declared != body_bytes.len() {
                return Err("replication response body is truncated".to_string());
            }
        }
    }
    if body_bytes.len() > max_body_bytes {
        return Err(format!(
            "replication response exceeds {max_body_bytes} byte limit"
        ));
    }
    let body = String::from_utf8(body_bytes.to_vec())
        .map_err(|_| "replication response is not valid UTF-8".to_string())?;
    Ok(ReplicationSourceResponse { status, body })
}

pub(crate) fn render_replication_delta_frame(frame: &WalReplicationFrame) -> String {
    let mut out = format!(
        "status=ok\ngeneration={}\nneeds_resync={}\nfrom_offset={}\nnext_offset={}\ntotal_records={}\nrecords={}\n",
        frame.generation,
        if frame.needs_resync { 1 } else { 0 },
        frame.from_offset,
        frame.next_offset,
        frame.total_records,
        frame.wal_lines.len()
    );
    for line in &frame.wal_lines {
        out.push_str(line);
        out.push('\n');
    }
    out
}

pub(crate) fn parse_replication_delta_frame(
    body: &str,
    max_records: usize,
) -> Result<ReplicationDeltaFrame, String> {
    let mut lines = body.lines().peekable();
    expect_kv(&mut lines, "status", "ok")?;
    let generation = parse_optional_generation(&mut lines)?;
    let needs_resync = parse_kv_bool01(&mut lines, "needs_resync")?;
    let from_offset = parse_kv_usize(&mut lines, "from_offset")?;
    let next_offset = parse_kv_usize(&mut lines, "next_offset")?;
    let total_records = parse_kv_usize(&mut lines, "total_records")?;
    let records = parse_kv_usize(&mut lines, "records")?;
    if records > max_records.max(1) || records > body.len() {
        return Err(format!(
            "replication delta advertises {records} records (limit {max_records})"
        ));
    }
    if !needs_resync && from_offset.checked_add(records) != Some(next_offset) {
        return Err("replication delta next_offset does not match record count".to_string());
    }
    let mut wal_lines = Vec::with_capacity(records.min(PREALLOC_CAP));
    for _ in 0..records {
        let line = lines
            .next()
            .ok_or_else(|| "replication delta missing WAL line".to_string())?;
        wal_lines.push(line.to_string());
    }
    Ok(ReplicationDeltaFrame {
        generation,
        needs_resync,
        from_offset,
        next_offset,
        total_records,
        wal_lines,
    })
}

pub(crate) fn render_replication_export_frame(
    export: &WalReplicationExport,
    generation: u64,
) -> String {
    let mut out = format!(
        "status=ok\ngeneration={generation}\nsnapshot_records={}\nwal_records={}\nSNAPSHOT\n",
        export.snapshot_lines.len(),
        export.wal_lines.len()
    );
    for line in &export.snapshot_lines {
        out.push_str(line);
        out.push('\n');
    }
    out.push_str("WAL\n");
    for line in &export.wal_lines {
        out.push_str(line);
        out.push('\n');
    }
    out
}

pub(crate) fn parse_replication_export_frame(body: &str) -> Result<ReplicationExportFrame, String> {
    let mut lines = body.lines().peekable();
    expect_kv(&mut lines, "status", "ok")?;
    let generation = parse_optional_generation(&mut lines)?;
    let snapshot_records = parse_kv_usize(&mut lines, "snapshot_records")?;
    let wal_records = parse_kv_usize(&mut lines, "wal_records")?;
    if snapshot_records > body.len() || wal_records > body.len() {
        return Err("replication export advertises more records than it carries".to_string());
    }
    let snapshot_marker = lines
        .next()
        .ok_or_else(|| "replication export missing SNAPSHOT marker".to_string())?;
    if snapshot_marker != "SNAPSHOT" {
        return Err("replication export has invalid SNAPSHOT marker".to_string());
    }
    let mut snapshot_lines = Vec::with_capacity(snapshot_records.min(PREALLOC_CAP));
    for _ in 0..snapshot_records {
        let line = lines
            .next()
            .ok_or_else(|| "replication export missing snapshot line".to_string())?;
        snapshot_lines.push(line.to_string());
    }
    let wal_marker = lines
        .next()
        .ok_or_else(|| "replication export missing WAL marker".to_string())?;
    if wal_marker != "WAL" {
        return Err("replication export has invalid WAL marker".to_string());
    }
    let mut wal_lines = Vec::with_capacity(wal_records.min(PREALLOC_CAP));
    for _ in 0..wal_records {
        let line = lines
            .next()
            .ok_or_else(|| "replication export missing WAL line".to_string())?;
        wal_lines.push(line.to_string());
    }
    Ok(ReplicationExportFrame {
        generation,
        snapshot_lines,
        wal_lines,
    })
}

fn parse_optional_generation<'a, I>(
    lines: &mut std::iter::Peekable<I>,
) -> Result<Option<u64>, String>
where
    I: Iterator<Item = &'a str>,
{
    match lines.peek() {
        Some(line) if line.starts_with("generation=") => {
            let line = lines.next().unwrap_or_default();
            let (_, value) = parse_kv_line(line, "generation")?;
            value
                .parse::<u64>()
                .map(Some)
                .map_err(|_| "replication payload has invalid generation".to_string())
        }
        _ => Ok(None),
    }
}

/// Replication endpoints expose every tenant's data and accept acks, so they
/// are closed unless a replication token is configured and presented. With no
/// token configured they stay closed, except in explicit dev mode
/// (`DASH_INSECURE_DEV_MODE=1`). The token is compared in constant time.
pub(super) fn is_replication_request_authorized(request: &HttpRequest) -> bool {
    let Some(expected_token) = replication_token() else {
        return dash_common::insecure_dev_mode_enabled();
    };
    request
        .headers
        .get("x-replication-token")
        .is_some_and(|value| {
            dash_common::constant_time_eq(value.trim().as_bytes(), expected_token.as_bytes())
        })
}

/// Follower-side replication state kept on the runtime.
#[derive(Debug)]
pub(crate) struct ReplicationFollowerState {
    pub(crate) enabled: bool,
    pub(crate) generation: Option<u64>,
    pub(crate) state_loaded: bool,
    pub(crate) state_path: Option<String>,
    pub(crate) force_resync: bool,
    pub(crate) leader_total_records: usize,
    pub(crate) consecutive_failures: u64,
    pub(crate) synced_once: bool,
    pub(crate) last_success: Option<Instant>,
    pub(crate) started: Instant,
    pub(crate) max_lag_records: usize,
    pub(crate) max_staleness_ms: u64,
}

impl Default for ReplicationFollowerState {
    fn default() -> Self {
        Self {
            enabled: false,
            generation: None,
            state_loaded: false,
            state_path: None,
            force_resync: false,
            leader_total_records: 0,
            consecutive_failures: 0,
            synced_once: false,
            last_success: None,
            started: Instant::now(),
            max_lag_records: DEFAULT_MAX_LAG_RECORDS,
            max_staleness_ms: DEFAULT_MAX_STALENESS_MS,
        }
    }
}

impl IngestionRuntime {
    /// Mark this runtime as a replication follower (called once at startup
    /// when a source URL is configured) so `/ready` and `/metrics` report it.
    pub(super) fn enable_replication_follower(&mut self, config: &ReplicationPullConfig) {
        self.replication_follower.enabled = true;
        self.replication_follower.max_lag_records = config.max_lag_records;
        self.replication_follower.max_staleness_ms = config.max_staleness_ms;
        self.replication_follower.started = Instant::now();
    }

    /// `(from_offset, from_generation, force_resync)` for the next pull.
    /// Loads the persisted state the first time it is called.
    fn replication_cursor(&mut self, config: &ReplicationPullConfig) -> (usize, Option<u64>, bool) {
        if !self.replication_follower.state_loaded {
            self.replication_follower.state_loaded = true;
            let path = config
                .offset_path
                .clone()
                .or_else(|| self.wal.as_ref().map(|wal| wal_state_path(wal.path())));
            if let Some(path) = path.as_deref() {
                match read_state(path) {
                    Some(saved) => {
                        self.replication_last_offset = saved.offset;
                        self.replication_follower.generation = saved.generation;
                        self.replication_follower.leader_total_records = saved.offset;
                        self.replication_follower.synced_once = true;
                        self.replication_follower.last_success = Some(Instant::now());
                        eprintln!(
                            "ingestion replication follower resuming from generation={:?} offset={}",
                            saved.generation, saved.offset
                        );
                    }
                    None => {
                        // A populated WAL with no saved cursor cannot be
                        // reconciled with the leader incrementally.
                        let populated = self
                            .wal
                            .as_ref()
                            .and_then(|wal| wal.wal_record_count().ok())
                            .is_some_and(|count| count > 0);
                        if populated {
                            self.replication_follower.force_resync = true;
                        }
                    }
                }
            }
            self.replication_follower.state_path = path;
        }
        (
            self.replication_last_offset,
            self.replication_follower.generation,
            self.replication_follower.force_resync,
        )
    }

    fn persist_replication_state(&mut self) {
        let Some(path) = self.replication_follower.state_path.clone() else {
            return;
        };
        let state = FollowerState {
            generation: self.replication_follower.generation,
            offset: self.replication_last_offset,
        };
        // The WAL must be durable before the cursor claims it.
        if let Some(wal) = self.wal.as_mut()
            && let Err(err) = wal.flush_pending_sync_if_unsynced()
        {
            eprintln!("ingestion replication: WAL flush before persisting cursor failed: {err:?}");
            return;
        }
        if let Err(err) = write_state(&path, &state) {
            eprintln!("ingestion replication follower failed to persist state: {err}");
        }
    }

    fn note_leader_total(&mut self, total_records: usize) {
        self.replication_follower.leader_total_records = total_records;
    }

    fn record_replication_pull_success(&mut self) {
        self.replication_follower.consecutive_failures = 0;
        self.replication_follower.synced_once = true;
        self.replication_follower.last_success = Some(Instant::now());
    }

    /// Apply one delta batch atomically: stage on a detached clone, mirror
    /// to the WAL, then commit (keeping the redb handle).
    pub(super) fn apply_replication_delta_frame(
        &mut self,
        frame: &ReplicationDeltaFrame,
    ) -> Result<(), StoreError> {
        if !frame.wal_lines.is_empty() {
            let mut staged_store = self.store.clone_detached();
            for line in &frame.wal_lines {
                staged_store.apply_persisted_record_line(line)?;
            }

            if let Some(wal) = self.wal.as_mut() {
                let rollback_point = wal.begin_rollback_point()?;
                let append_result = (|| {
                    for line in &frame.wal_lines {
                        wal.append_raw_record_line(line)?;
                    }
                    Ok::<(), StoreError>(())
                })();
                if let Err(err) = append_result {
                    if let Err(rollback_err) = wal.rollback_to(rollback_point) {
                        eprintln!(
                            "replication rollback failed after WAL append error: {rollback_err:?}"
                        );
                    }
                    return Err(err);
                }
            }

            if let Err(err) = self.store.commit_staged(staged_store) {
                eprintln!("replication: disk mirror degraded after commit: {err:?}");
            }
            for tenant_id in self.store.tenant_ids() {
                self.publish_segments_for_tenant(&tenant_id);
            }
            self.replication_applied_records_total = self
                .replication_applied_records_total
                .saturating_add(frame.wal_lines.len() as u64);
        }
        self.replication_pull_success_total = self.replication_pull_success_total.saturating_add(1);
        self.replication_last_offset = frame.next_offset;
        self.replication_follower.generation =
            frame.generation.or(self.replication_follower.generation);
        self.replication_last_error = None;
        self.persist_replication_state();
        Ok(())
    }

    /// Replace this node's state with the leader's export. The redb handle
    /// is kept and rewritten from the new state.
    pub(super) fn apply_replication_export_frame(
        &mut self,
        frame: ReplicationExportFrame,
    ) -> Result<(), StoreError> {
        let export = WalReplicationExport {
            snapshot_lines: frame.snapshot_lines,
            wal_lines: frame.wal_lines,
        };
        let mut fresh = InMemoryStore::new_with_ann_tuning(self.store.ann_tuning().clone());
        for line in export.snapshot_lines.iter().chain(export.wal_lines.iter()) {
            fresh.apply_persisted_record_line(line)?;
        }
        if let Some(wal) = self.wal.as_mut() {
            wal.replace_with_replication_export(&export)?;
        }
        if let Err(err) = self.store.replace_state_from(fresh) {
            eprintln!("replication resync: disk rewrite degraded: {err:?}");
        }
        for tenant_id in self.store.tenant_ids() {
            self.publish_segments_for_tenant(&tenant_id);
        }
        self.replication_pull_success_total = self.replication_pull_success_total.saturating_add(1);
        self.replication_applied_records_total = self
            .replication_applied_records_total
            .saturating_add((export.snapshot_lines.len() + export.wal_lines.len()) as u64);
        self.replication_resync_total = self.replication_resync_total.saturating_add(1);
        // Offsets count WAL lines only, the unit the leader reports.
        self.replication_last_offset = export.wal_lines.len();
        self.replication_follower.generation = frame.generation;
        self.replication_follower.force_resync = false;
        self.replication_follower.leader_total_records = export.wal_lines.len();
        self.replication_last_error = None;
        self.persist_replication_state();
        Ok(())
    }

    fn replication_lag_records(&self) -> usize {
        self.replication_follower
            .leader_total_records
            .saturating_sub(self.replication_last_offset)
    }

    fn replication_last_success_age_ms(&self) -> u64 {
        let reference = self
            .replication_follower
            .last_success
            .unwrap_or(self.replication_follower.started);
        reference.elapsed().as_millis() as u64
    }

    /// `None` when this node is not a replication follower.
    pub(super) fn replication_readiness(&self) -> Option<Result<(), &'static str>> {
        let follower = &self.replication_follower;
        if !follower.enabled {
            return None;
        }
        Some(if !follower.synced_once {
            Err("replication_initial_sync_pending")
        } else if self.replication_lag_records() > follower.max_lag_records {
            Err("replication_lag_exceeded")
        } else if self.replication_last_success_age_ms() > follower.max_staleness_ms {
            Err("replication_stale")
        } else {
            Ok(())
        })
    }

    pub(super) fn replication_ready_json(&self) -> Option<String> {
        if !self.replication_follower.enabled {
            return None;
        }
        let generation = self
            .replication_follower
            .generation
            .map(|g| g.to_string())
            .unwrap_or_else(|| "null".to_string());
        let last_error = match self.replication_last_error.as_deref() {
            Some(err) => format!("\"{}\"", json_escape(err)),
            None => "null".to_string(),
        };
        Some(format!(
            "{{\"generation\":{generation},\"offset\":{},\"leader_total_records\":{},\"lag_records\":{},\"last_success_age_ms\":{},\"consecutive_failures\":{},\"resyncs_total\":{},\"last_error\":{last_error}}}",
            self.replication_last_offset,
            self.replication_follower.leader_total_records,
            self.replication_lag_records(),
            self.replication_last_success_age_ms(),
            self.replication_follower.consecutive_failures,
            self.replication_resync_total,
        ))
    }

    pub(super) fn replication_follower_metrics_text(&self) -> String {
        if !self.replication_follower.enabled {
            return String::new();
        }
        format!(
            "# TYPE dash_ingest_replication_lag_records gauge\n\
dash_ingest_replication_lag_records {}\n\
# TYPE dash_ingest_replication_last_success_age_ms gauge\n\
dash_ingest_replication_last_success_age_ms {}\n\
# TYPE dash_ingest_replication_consecutive_failures gauge\n\
dash_ingest_replication_consecutive_failures {}\n\
# TYPE dash_ingest_replication_generation gauge\n\
dash_ingest_replication_generation {}\n",
            self.replication_lag_records(),
            self.replication_last_success_age_ms(),
            self.replication_follower.consecutive_failures,
            self.replication_follower.generation.unwrap_or(0),
        )
    }
}

/// Run one pull. Returns the consecutive failure count afterwards so the
/// caller can back off. Never panics: a panic inside the tick is caught,
/// logged and counted as a failure.
pub(super) fn run_replication_pull_tick(
    runtime: &SharedRuntime,
    config: &ReplicationPullConfig,
) -> u64 {
    let outcome = match catch_unwind(AssertUnwindSafe(|| pull_tick(runtime, config))) {
        Ok(outcome) => outcome,
        Err(panic) => {
            if let Ok(mut guard) = runtime.lock() {
                guard.replication_follower.force_resync = true;
            }
            Err(format!(
                "replication pull panicked: {}",
                panic_message(&panic)
            ))
        }
    };
    let Ok(mut guard) = runtime.lock() else {
        return 0;
    };
    match outcome {
        Ok(()) => {
            guard.record_replication_pull_success();
            0
        }
        Err(err) => {
            eprintln!("replication pull tick failed: {err}");
            guard.observe_replication_pull_failure(err);
            guard.replication_follower.consecutive_failures = guard
                .replication_follower
                .consecutive_failures
                .saturating_add(1);
            guard.replication_follower.consecutive_failures
        }
    }
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(msg) = panic.downcast_ref::<&str>() {
        (*msg).to_string()
    } else if let Some(msg) = panic.downcast_ref::<String>() {
        msg.clone()
    } else {
        "unknown panic".to_string()
    }
}

fn pull_tick(runtime: &SharedRuntime, config: &ReplicationPullConfig) -> Result<(), String> {
    let (from_offset, from_generation, force_resync) = runtime
        .lock()
        .map_err(|_| "replication runtime lock unavailable".to_string())?
        .replication_cursor(config);
    if force_resync {
        return resync_from_export(runtime, config);
    }
    let delta_response = request_replication_source(
        &config.wal_pull_url(from_offset, from_generation),
        config.token.as_deref(),
        config.max_response_bytes,
    )?;
    if delta_response.status != 200 {
        return Err(format!(
            "replication source WAL delta returned status {}",
            delta_response.status
        ));
    }
    let delta_frame = parse_replication_delta_frame(&delta_response.body, config.max_records)?;
    if delta_frame.from_offset != from_offset {
        return Err(format!(
            "replication frame from_offset {} does not match requested {from_offset}",
            delta_frame.from_offset
        ));
    }
    runtime
        .lock()
        .map_err(|_| "replication runtime lock unavailable".to_string())?
        .note_leader_total(delta_frame.total_records);
    let generation_changed = matches!(
        (from_generation, delta_frame.generation),
        (Some(ours), Some(theirs)) if ours != theirs
    );
    if delta_frame.needs_resync || generation_changed {
        return resync_from_export(runtime, config);
    }
    if delta_frame.next_offset > delta_frame.total_records {
        return Err("replication frame next_offset exceeds total_records".to_string());
    }
    let commit_ids = extract_batch_commit_ids_from_wal_lines(&delta_frame.wal_lines)?;
    runtime
        .lock()
        .map_err(|_| StoreError::Io("replication runtime lock unavailable".to_string()))
        .and_then(|mut guard| guard.apply_replication_delta_frame(&delta_frame))
        .map_err(|err| format!("replication delta apply failed: {err:?}"))?;
    acknowledge_replication_commits(config, &commit_ids)
        .map_err(|err| format!("replication delta commit ack failed: {err}"))
}

fn resync_from_export(
    runtime: &SharedRuntime,
    config: &ReplicationPullConfig,
) -> Result<(), String> {
    let export_response = request_replication_source(
        &config.export_url(),
        config.token.as_deref(),
        config.max_response_bytes,
    )?;
    if export_response.status != 200 {
        return Err(format!(
            "replication export returned non-200 status: {}",
            export_response.status
        ));
    }
    let export_frame = parse_replication_export_frame(&export_response.body)?;
    let mut combined_lines =
        Vec::with_capacity(export_frame.snapshot_lines.len() + export_frame.wal_lines.len());
    combined_lines.extend(export_frame.snapshot_lines.iter().cloned());
    combined_lines.extend(export_frame.wal_lines.iter().cloned());
    let commit_ids = extract_batch_commit_ids_from_wal_lines(&combined_lines)?;
    runtime
        .lock()
        .map_err(|_| StoreError::Io("replication runtime lock unavailable".to_string()))
        .and_then(|mut guard| guard.apply_replication_export_frame(export_frame))
        .map_err(|err| format!("replication resync apply failed: {err:?}"))?;
    acknowledge_replication_commits(config, &commit_ids)
        .map_err(|err| format!("replication resync commit ack failed: {err}"))
}

fn acknowledge_replication_commits(
    config: &ReplicationPullConfig,
    commit_ids: &[String],
) -> Result<(), String> {
    if commit_ids.is_empty() {
        return Ok(());
    }
    if config.local_replica_id.is_none() {
        return Ok(());
    }
    for commit_id in commit_ids {
        let Some(url) = config.ack_url(commit_id) else {
            continue;
        };
        let response = request_replication_ack(&url, config.token.as_deref())?;
        if response.status != 200 {
            return Err(format!(
                "replication ack failed for commit_id '{}' with status {}",
                commit_id, response.status
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// Persisted follower state
// ---------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct FollowerState {
    generation: Option<u64>,
    offset: usize,
}

fn wal_state_path(wal_path: &std::path::Path) -> String {
    format!("{}.replication", wal_path.display())
}

/// Parse the state file. The legacy format (a bare offset number) yields a
/// state without a generation, which the leader answers with a resync.
fn read_state(path: &str) -> Option<FollowerState> {
    let contents = std::fs::read_to_string(path).ok()?;
    parse_state(&contents)
}

fn parse_state(contents: &str) -> Option<FollowerState> {
    let trimmed = contents.trim();
    if let Ok(offset) = trimmed.parse::<usize>() {
        return Some(FollowerState {
            generation: None,
            offset,
        });
    }
    let mut generation = None;
    let mut offset = None;
    for line in trimmed.lines() {
        let (key, value) = line.split_once('=')?;
        match key.trim() {
            "generation" => {
                generation = match value.trim() {
                    "none" => None,
                    other => Some(other.parse::<u64>().ok()?),
                }
            }
            "offset" => offset = Some(value.trim().parse::<usize>().ok()?),
            _ => {}
        }
    }
    Some(FollowerState {
        generation,
        offset: offset?,
    })
}

fn render_state(state: &FollowerState) -> String {
    let generation = state
        .generation
        .map(|g| g.to_string())
        .unwrap_or_else(|| "none".to_string());
    format!("generation={generation}\noffset={}\n", state.offset)
}

/// Atomic write: temp file, fsync, rename, fsync of the directory.
fn write_state(path: &str, state: &FollowerState) -> std::io::Result<()> {
    let target = std::path::Path::new(path);
    let dir = target.parent().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "offset path has no parent directory",
        )
    })?;
    if !dir.as_os_str().is_empty() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = target.with_extension("tmp-write");
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(render_state(state).as_bytes())?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, target)?;
    if !dir.as_os_str().is_empty()
        && let Ok(dir_file) = std::fs::File::open(dir)
    {
        let _ = dir_file.sync_all();
    }
    Ok(())
}

fn json_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn env_u64(primary: &str, fallback: &str) -> Option<u64> {
    env_with_fallback(primary, fallback)
        .and_then(|value| value.trim().parse::<u64>().ok())
        .filter(|value| *value > 0)
}

fn extract_batch_commit_ids_from_wal_lines(lines: &[String]) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for line in lines {
        let Some(commit_id) = parse_batch_commit_id_from_wal_line(line)? else {
            continue;
        };
        if seen.insert(commit_id.clone()) {
            out.push(commit_id);
        }
    }
    Ok(out)
}

fn parse_batch_commit_id_from_wal_line(line: &str) -> Result<Option<String>, String> {
    if !line.starts_with("B\t") {
        return Ok(None);
    }
    let parts: Vec<&str> = line.split('\t').collect();
    if parts.len() != 5 {
        return Err("batch commit wal line has invalid field count".to_string());
    }
    Ok(Some(unescape_wal_field(parts[1])?))
}

fn unescape_wal_field(value: &str) -> Result<String, String> {
    let mut output = String::with_capacity(value.len());
    let mut escaped = false;
    for ch in value.chars() {
        if escaped {
            match ch {
                '\\' => output.push('\\'),
                't' => output.push('\t'),
                'n' => output.push('\n'),
                other => return Err(format!("invalid escape sequence in WAL field: \\{other}")),
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            output.push(ch);
        }
    }
    if escaped {
        return Err("unterminated escape sequence in WAL field".to_string());
    }
    Ok(output)
}

fn url_encode_component(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for byte in raw.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(byte as char);
        } else {
            out.push('%');
            out.push_str(&format!("{byte:02X}"));
        }
    }
    out
}

fn parse_http_url(url: &str) -> Result<(String, String), String> {
    let without_scheme = url
        .strip_prefix("http://")
        .ok_or_else(|| "replication source URL must start with http://".to_string())?;
    let (authority, path_and_query) = match without_scheme.split_once('/') {
        Some((authority, suffix)) => (authority, format!("/{}", suffix)),
        None => (without_scheme, "/".to_string()),
    };
    if authority.trim().is_empty() {
        return Err("replication source URL missing host:port authority".to_string());
    }
    Ok((authority.to_string(), path_and_query))
}

fn parse_kv_usize<'a, I>(lines: &mut I, key: &str) -> Result<usize, String>
where
    I: Iterator<Item = &'a str>,
{
    let line = lines
        .next()
        .ok_or_else(|| format!("replication payload missing '{key}'"))?;
    let (_, value) = parse_kv_line(line, key)?;
    value
        .parse::<usize>()
        .map_err(|_| format!("replication payload has invalid numeric value for '{key}'"))
}

fn parse_kv_bool01<'a, I>(lines: &mut I, key: &str) -> Result<bool, String>
where
    I: Iterator<Item = &'a str>,
{
    let line = lines
        .next()
        .ok_or_else(|| format!("replication payload missing '{key}'"))?;
    let (_, value) = parse_kv_line(line, key)?;
    match value {
        "0" => Ok(false),
        "1" => Ok(true),
        _ => Err(format!(
            "replication payload has invalid boolean value for '{key}'"
        )),
    }
}

fn expect_kv<'a, I>(lines: &mut I, key: &str, expected_value: &str) -> Result<(), String>
where
    I: Iterator<Item = &'a str>,
{
    let line = lines
        .next()
        .ok_or_else(|| format!("replication payload missing '{key}'"))?;
    let (_, value) = parse_kv_line(line, key)?;
    if value != expected_value {
        return Err(format!(
            "replication payload has invalid '{key}' value (expected '{expected_value}')"
        ));
    }
    Ok(())
}

fn parse_kv_line<'a>(line: &'a str, expected_key: &str) -> Result<(&'a str, &'a str), String> {
    let (key, value) = line
        .split_once('=')
        .ok_or_else(|| "replication payload has malformed key=value line".to_string())?;
    if key != expected_key {
        return Err(format!(
            "replication payload key mismatch (expected '{expected_key}', got '{key}')"
        ));
    }
    Ok((key, value))
}

fn replication_token() -> Option<String> {
    env_with_fallback(
        "DASH_INGEST_REPLICATION_TOKEN",
        "EME_INGEST_REPLICATION_TOKEN",
    )
    .map(|value| value.trim().to_string())
    .filter(|value| !value.is_empty())
}

fn env_with_fallback(primary: &str, fallback: &str) -> Option<String> {
    std::env::var(primary)
        .ok()
        .or_else(|| std::env::var(fallback).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::{Arc, Mutex, mpsc},
    };

    fn read_http_request_head(stream: &mut std::net::TcpStream) -> String {
        let mut raw = Vec::new();
        let mut chunk = [0_u8; 1024];
        loop {
            let read = stream
                .read(&mut chunk)
                .expect("mock replication source should read request bytes");
            if read == 0 {
                break;
            }
            raw.extend_from_slice(&chunk[..read]);
            if raw.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        String::from_utf8(raw).expect("request should be UTF-8")
    }

    fn render_http_response(status_line: &str, body: &str) -> String {
        format!(
            "{status_line}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn spawn_mock_replication_source(
        delta_response_body: String,
        expected_requests: usize,
    ) -> (String, mpsc::Receiver<String>, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .expect("mock replication source should bind a random local port");
        let address = listener
            .local_addr()
            .expect("mock replication source should have local address");
        let (request_tx, request_rx) = mpsc::channel::<String>();
        let handle = std::thread::spawn(move || {
            for _ in 0..expected_requests {
                let (mut stream, _) = listener
                    .accept()
                    .expect("mock replication source should accept connection");
                let request = read_http_request_head(&mut stream);
                let request_line = request.lines().next().unwrap_or_default().to_string();
                request_tx
                    .send(request_line.clone())
                    .expect("mock replication source should publish request line");
                let response = if request_line.starts_with("GET /internal/replication/wal?") {
                    render_http_response("HTTP/1.1 200 OK", &delta_response_body)
                } else if request_line.starts_with("POST /internal/replication/ack?") {
                    render_http_response("HTTP/1.1 200 OK", "status=ok\n")
                } else {
                    render_http_response("HTTP/1.1 404 Not Found", "status=not_found\n")
                };
                stream
                    .write_all(response.as_bytes())
                    .expect("mock replication source should write response");
                stream
                    .flush()
                    .expect("mock replication source should flush response");
            }
        });
        (format!("http://{address}"), request_rx, handle)
    }

    #[test]
    fn parse_and_render_replication_delta_round_trip() {
        let body = render_replication_delta_frame(&WalReplicationFrame {
            generation: 9,
            from_offset: 2,
            next_offset: 4,
            total_records: 10,
            needs_resync: false,
            wal_lines: vec![
                "C\tc1\ttenant-a\ttext\t0.9\tnull\t\t".to_string(),
                "B\tcommit-1\t1\t1700000000000\t2:c1".to_string(),
            ],
        });
        let frame = parse_replication_delta_frame(&body, 512).expect("delta frame should parse");
        assert_eq!(frame.generation, Some(9));
        assert!(!frame.needs_resync);
        assert_eq!(frame.next_offset, 4);
        assert_eq!(frame.wal_lines.len(), 2);
    }

    #[test]
    fn parse_and_render_replication_export_round_trip() {
        let body = render_replication_export_frame(
            &WalReplicationExport {
                snapshot_lines: vec!["C\tc1\ttenant-a\ttext\t0.9\tnull\t\t".to_string()],
                wal_lines: vec![
                    "E\te1\tc1\tsource://x\tsupports\t0.8\tnull\tnull\tnull".to_string(),
                ],
            },
            11,
        );
        let frame = parse_replication_export_frame(&body).expect("export frame should parse");
        assert_eq!(frame.generation, Some(11));
        assert_eq!(frame.snapshot_lines.len(), 1);
        assert_eq!(frame.wal_lines.len(), 1);
    }

    #[test]
    fn parse_http_url_accepts_authority_and_path() {
        let (authority, path) =
            parse_http_url("http://127.0.0.1:8081/internal/replication/wal?from_offset=0")
                .expect("url should parse");
        assert_eq!(authority, "127.0.0.1:8081");
        assert_eq!(path, "/internal/replication/wal?from_offset=0");
    }

    #[test]
    fn extract_batch_commit_ids_from_wal_lines_deduplicates_and_unescapes() {
        let lines = vec![
            "C\tclaim-1\ttenant-a\ttext\t0.9\tnull\t\t".to_string(),
            "B\tcommit-1\t1\t1700000000000\t2:c1".to_string(),
            "B\tcommit-1\t1\t1700000000001\t2:c1".to_string(),
            "B\tcommit\\t2\t1\t1700000000002\t2:c2".to_string(),
        ];
        let commit_ids = extract_batch_commit_ids_from_wal_lines(&lines).expect("ids should parse");
        assert_eq!(
            commit_ids,
            vec!["commit-1".to_string(), "commit\t2".to_string()]
        );
    }

    #[test]
    fn replication_pull_config_ack_url_encodes_query_values() {
        let cfg = ReplicationPullConfig {
            source_base_url: "http://127.0.0.1:8081".to_string(),
            poll_interval: Duration::from_millis(500),
            max_records: 128,
            token: None,
            local_replica_id: Some("node a".to_string()),
            ..ReplicationPullConfig::new("http://127.0.0.1:1")
        };
        let url = cfg
            .ack_url("commit/1")
            .expect("ack url should be present when local replica id is set");
        assert_eq!(
            url,
            "http://127.0.0.1:8081/internal/replication/ack?commit_id=commit%2F1&replica_id=node%20a"
        );
    }

    #[test]
    fn run_replication_pull_tick_applies_delta_and_acks_commit_to_source() {
        let runtime = Arc::new(Mutex::new(super::super::IngestionRuntime::in_memory(
            store::InMemoryStore::new(),
        )));
        let delta_body = "status=ok\nneeds_resync=0\nfrom_offset=0\nnext_offset=2\ntotal_records=2\nrecords=2\nC\tclaim-1\ttenant-a\ttext\t0.9\tnull\t\t\nB\tcommit-1\t1\t1700000000000\t7:claim-1\n".to_string();
        let (source_base_url, requests, source_handle) =
            spawn_mock_replication_source(delta_body, 2);
        let config = ReplicationPullConfig {
            source_base_url,
            poll_interval: Duration::from_millis(500),
            max_records: 64,
            token: None,
            local_replica_id: Some("node-b".to_string()),
            ..ReplicationPullConfig::new("http://127.0.0.1:1")
        };

        run_replication_pull_tick(&runtime, &config);

        let pull_request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("source should receive WAL pull request");
        assert!(
            pull_request
                .starts_with("GET /internal/replication/wal?from_offset=0&max_records=64 HTTP/1.1"),
            "unexpected pull request line: {pull_request}"
        );
        let ack_request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("source should receive replication ack request");
        assert!(
            ack_request.starts_with(
                "POST /internal/replication/ack?commit_id=commit-1&replica_id=node-b HTTP/1.1"
            ),
            "unexpected ack request line: {ack_request}"
        );
        source_handle
            .join()
            .expect("mock replication source should join cleanly");

        let guard = runtime
            .lock()
            .expect("replication runtime should be lockable after pull tick");
        assert_eq!(guard.claims_len(), 1);
        assert_eq!(guard.replication_last_offset, 2);
        assert_eq!(guard.replication_pull_success_total, 1);
        assert_eq!(guard.replication_pull_failure_total, 0);
    }

    #[test]
    fn run_replication_pull_tick_skips_ack_without_local_replica_id() {
        let runtime = Arc::new(Mutex::new(super::super::IngestionRuntime::in_memory(
            store::InMemoryStore::new(),
        )));
        let delta_body = "status=ok\nneeds_resync=0\nfrom_offset=0\nnext_offset=2\ntotal_records=2\nrecords=2\nC\tclaim-2\ttenant-a\ttext\t0.9\tnull\t\t\nB\tcommit-2\t1\t1700000000000\t7:claim-2\n".to_string();
        let (source_base_url, requests, source_handle) =
            spawn_mock_replication_source(delta_body, 1);
        let config = ReplicationPullConfig {
            source_base_url,
            poll_interval: Duration::from_millis(500),
            max_records: 64,
            token: None,
            local_replica_id: None,
            ..ReplicationPullConfig::new("http://127.0.0.1:1")
        };

        run_replication_pull_tick(&runtime, &config);

        let pull_request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("source should receive WAL pull request");
        assert!(
            pull_request
                .starts_with("GET /internal/replication/wal?from_offset=0&max_records=64 HTTP/1.1"),
            "unexpected pull request line: {pull_request}"
        );
        assert!(
            requests.recv_timeout(Duration::from_millis(200)).is_err(),
            "source should not receive ack request when local replica id is unset"
        );
        source_handle
            .join()
            .expect("mock replication source should join cleanly");

        let guard = runtime
            .lock()
            .expect("replication runtime should be lockable after pull tick");
        assert_eq!(guard.claims_len(), 1);
        assert_eq!(guard.replication_last_offset, 2);
        assert_eq!(guard.replication_pull_success_total, 1);
        assert_eq!(guard.replication_pull_failure_total, 0);
    }

    #[test]
    fn run_replication_pull_tick_rejects_replay_payload_divergence_without_ack() {
        let runtime = Arc::new(Mutex::new(super::super::IngestionRuntime::in_memory(
            store::InMemoryStore::new(),
        )));
        {
            let mut guard = runtime
                .lock()
                .expect("runtime lock should be available for test setup");
            guard
                .store
                .observe_batch_commit(
                    "commit-diverge-1",
                    1,
                    1_700_000_000_000,
                    &["c-existing".to_string()],
                )
                .expect("initial batch commit metadata should seed runtime");
        }
        let delta_body = "status=ok\nneeds_resync=0\nfrom_offset=0\nnext_offset=1\ntotal_records=1\nrecords=1\nB\tcommit-diverge-1\t1\t1700000000001\t5:c-new\n".to_string();
        let (source_base_url, requests, source_handle) =
            spawn_mock_replication_source(delta_body, 1);
        let config = ReplicationPullConfig {
            source_base_url,
            poll_interval: Duration::from_millis(500),
            max_records: 64,
            token: None,
            local_replica_id: Some("node-b".to_string()),
            ..ReplicationPullConfig::new("http://127.0.0.1:1")
        };

        run_replication_pull_tick(&runtime, &config);

        let pull_request = requests
            .recv_timeout(Duration::from_secs(2))
            .expect("source should receive WAL pull request");
        assert!(
            pull_request
                .starts_with("GET /internal/replication/wal?from_offset=0&max_records=64 HTTP/1.1"),
            "unexpected pull request line: {pull_request}"
        );
        assert!(
            requests.recv_timeout(Duration::from_millis(200)).is_err(),
            "source should not receive ack request when replication apply fails"
        );
        source_handle
            .join()
            .expect("mock replication source should join cleanly");

        let guard = runtime
            .lock()
            .expect("replication runtime should be lockable after pull tick");
        assert_eq!(guard.replication_pull_success_total, 0);
        assert_eq!(guard.replication_pull_failure_total, 1);
        let error = guard
            .replication_last_error
            .as_deref()
            .expect("replication failure should record error");
        assert!(error.contains("existing_fingerprint="));
        assert!(error.contains("incoming_fingerprint="));
    }
}
