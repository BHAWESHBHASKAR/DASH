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
    io::Write,
    panic::{AssertUnwindSafe, catch_unwind},
    time::{Duration, Instant},
};

use store::{InMemoryStore, StoreError, WalReplicationExport, WalReplicationFrame};

use super::{IngestionRuntime, SharedRuntime, group_commit, http::HttpRequest, lock_wal};

const DEFAULT_REPLICATION_POLL_INTERVAL_MS: u64 = 500;
const DEFAULT_REPLICATION_MAX_RECORDS: usize = 512;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_BACKOFF_MS: u64 = 30_000;
const DEFAULT_MAX_LAG_RECORDS: usize = 100_000;
const DEFAULT_MAX_STALENESS_MS: u64 = 300_000;
const ACK_MAX_RESPONSE_BYTES: usize = 64 * 1024;
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
    /// Bytes requested per chunk of a full resync (chunked export), capped
    /// so a chunk response fits in `max_response_bytes`.
    pub(crate) export_chunk_bytes: usize,
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
            export_chunk_bytes: store::EXPORT_CHUNK_DEFAULT_BYTES,
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
        if let Some(n) = env_u64(
            "DASH_INGEST_REPLICATION_EXPORT_CHUNK_BYTES",
            "EME_INGEST_REPLICATION_EXPORT_CHUNK_BYTES",
        ) {
            config.export_chunk_bytes = n as usize;
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
        // `gen_switch=1`: this follower can continue across a checkpoint
        // from the exact end of the previous generation.
        let mut url = format!(
            "{}/internal/replication/wal?from_offset={from_offset}&max_records={}&gen_switch=1",
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

    /// Chunk size actually requested: the configured size, capped so the
    /// chunk plus its header fits in one response.
    fn effective_chunk_bytes(&self) -> usize {
        self.export_chunk_bytes
            .min(
                self.max_response_bytes
                    .saturating_sub(store::EXPORT_CHUNK_HEADER_RESERVE),
            )
            .max(1)
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
    pub(crate) generation: u64,
    pub(crate) needs_resync: bool,
    /// The leader moved this follower from the exact end of an earlier
    /// generation (`switch_from=<generation>:<offset>`) to offset 0.
    pub(crate) switch_from: Option<store::WalPosition>,
    pub(crate) from_offset: usize,
    pub(crate) next_offset: usize,
    pub(crate) total_records: usize,
    pub(crate) wal_lines: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReplicationExportFrame {
    pub(crate) generation: u64,
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
    let response = dash_common::replication_client::request(
        method,
        url,
        token,
        max_body_bytes,
        &dash_common::replication_client::ClientOptions::from_env(),
    )?;
    Ok(ReplicationSourceResponse {
        status: response.status,
        body: response.body,
    })
}

/// Renders a delta frame. `with_switch` (the follower sent `gen_switch=1`)
/// adds the `switch_from=` line after `needs_resync=`: `none`, or
/// `<generation>:<offset>` when the frame moves the follower from the end
/// of that generation to offset 0 of the current one. Without it the layout
/// is the one older followers parse.
pub(crate) fn render_replication_delta_frame(
    frame: &WalReplicationFrame,
    with_switch: bool,
) -> String {
    let switch_line = if with_switch {
        match frame.switched_from {
            Some(from) => format!("switch_from={}:{}\n", from.generation, from.records),
            None => "switch_from=none\n".to_string(),
        }
    } else {
        String::new()
    };
    let mut out = format!(
        "status=ok\ngeneration={}\nneeds_resync={}\n{switch_line}from_offset={}\nnext_offset={}\ntotal_records={}\nrecords={}\n",
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
    let generation = parse_generation(&mut lines)?;
    let needs_resync = parse_kv_bool01(&mut lines, "needs_resync")?;
    let switch_from = parse_switch_from(&mut lines)?;
    let from_offset = parse_kv_usize(&mut lines, "from_offset")?;
    let next_offset = parse_kv_usize(&mut lines, "next_offset")?;
    let total_records = parse_kv_usize(&mut lines, "total_records")?;
    let records = parse_kv_usize(&mut lines, "records")?;
    // Frames may run past `max_records` to end on a commit-group boundary.
    let limit = max_records
        .max(1)
        .saturating_add(store::REPLICATION_GROUP_EXTENSION_MAX);
    if records > limit || records > body.len() {
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
        switch_from,
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
    let generation = parse_generation(&mut lines)?;
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

/// The optional `switch_from=` line (see [`render_replication_delta_frame`]).
fn parse_switch_from<'a, I>(
    lines: &mut std::iter::Peekable<I>,
) -> Result<Option<store::WalPosition>, String>
where
    I: Iterator<Item = &'a str>,
{
    match lines.peek() {
        Some(line) if line.starts_with("switch_from=") => {
            let line = lines.next().unwrap_or_default();
            let (_, value) = parse_kv_line(line, "switch_from")?;
            if value == "none" {
                return Ok(None);
            }
            let invalid = || "replication payload has invalid switch_from".to_string();
            let (generation, records) = value.split_once(':').ok_or_else(invalid)?;
            Ok(Some(store::WalPosition {
                generation: generation.parse().map_err(|_| invalid())?,
                records: records.parse().map_err(|_| invalid())?,
            }))
        }
        _ => Ok(None),
    }
}

/// The WAL generation line every frame of a 0.3.0+ leader carries. A frame
/// without it comes from an older leader whose offset-only protocol cannot
/// tell a follower that the WAL was compacted (and serves a fresh follower
/// only the WAL tail, never the snapshot), so following it could silently
/// skip records: refuse it.
fn parse_generation<'a, I>(lines: &mut std::iter::Peekable<I>) -> Result<u64, String>
where
    I: Iterator<Item = &'a str>,
{
    match lines.peek() {
        Some(line) if line.starts_with("generation=") => {
            let line = lines.next().unwrap_or_default();
            let (_, value) = parse_kv_line(line, "generation")?;
            value
                .parse::<u64>()
                .map_err(|_| "replication payload has invalid generation".to_string())
        }
        _ => Err(dash_common::replication_client::LEGACY_LEADER_ERROR.to_string()),
    }
}

/// Replication endpoints expose every tenant's data and accept acks, so they
/// are closed unless a replication token is configured and presented. With no
/// token configured they stay closed, except on a service that runs in
/// explicit dev mode (`DASH_INSECURE_DEV_MODE=1`) with no authentication
/// configured at all: dev mode never bypasses a configured credential. The
/// token is compared in constant time.
///
/// With a [`ReplicationClientCertPolicy`] in force the request must also have
/// arrived with a verified client certificate (and, with an allowlist, one of
/// the listed fingerprints); this check comes on top of the token.
pub(super) fn is_replication_request_authorized(
    request: &HttpRequest,
    auth_policy: &dash_common::AuthPolicy,
) -> bool {
    if !ReplicationClientCertPolicy::from_env().admits(request) {
        return false;
    }
    let Some(expected_token) = replication_token() else {
        return auth_policy.is_open_dev_mode();
    };
    request
        .headers
        .get("x-replication-token")
        .is_some_and(|value| {
            dash_common::constant_time_eq(value.trim().as_bytes(), expected_token.as_bytes())
        })
}

/// Client-certificate requirement for `/internal/replication/*`:
/// `DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT` (any certificate that chains
/// to `DASH_INGEST_TLS_CLIENT_CA_FILE`) and
/// `DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS` (comma-separated SHA-256
/// fingerprints of the follower certificates; implies the requirement).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReplicationClientCertPolicy {
    require: bool,
    allowed: Vec<String>,
    /// Set when a value is malformed; startup refuses, requests are denied.
    pub(crate) invalid: Option<String>,
}

impl ReplicationClientCertPolicy {
    pub(crate) fn from_env() -> Self {
        Self::from_values(
            std::env::var("DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT")
                .ok()
                .as_deref(),
            std::env::var("DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS")
                .ok()
                .as_deref(),
        )
    }

    pub(crate) fn from_values(require: Option<&str>, allowed: Option<&str>) -> Self {
        let mut policy = Self::default();
        match require.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            None | Some("" | "0" | "false" | "no" | "off") => {}
            Some("1" | "true" | "yes" | "on") => policy.require = true,
            Some(_) => {
                policy.invalid = Some(
                    "DASH_INGEST_REPLICATION_REQUIRE_CLIENT_CERT must be a boolean".to_string(),
                )
            }
        }
        for item in allowed.unwrap_or("").split(',') {
            let item = item.trim().replace(':', "").to_ascii_lowercase();
            if item.is_empty() {
                continue;
            }
            if item.len() != 64 || !item.bytes().all(|b| b.is_ascii_hexdigit()) {
                policy.invalid = Some(
                    "DASH_INGEST_REPLICATION_ALLOWED_CLIENT_CERTS must list SHA-256 \
                     certificate fingerprints (64 hex digits, colons allowed)"
                        .to_string(),
                );
                continue;
            }
            policy.allowed.push(item);
        }
        policy
    }

    pub(crate) fn required(&self) -> bool {
        self.require || !self.allowed.is_empty()
    }

    fn admits(&self, request: &HttpRequest) -> bool {
        if self.invalid.is_some() {
            return false;
        }
        if !self.required() {
            return true;
        }
        let Some(fingerprint) = request.headers.get(dash_common::tls::CLIENT_CERT_HEADER) else {
            return false;
        };
        self.allowed.is_empty()
            || self.allowed.iter().any(|allowed| {
                dash_common::constant_time_eq(allowed.as_bytes(), fingerprint.as_bytes())
            })
    }
}

#[cfg(test)]
mod client_cert_policy_tests {
    use super::*;
    use std::collections::HashMap;

    fn request(fingerprint: Option<&str>) -> HttpRequest {
        let mut headers = HashMap::new();
        if let Some(fp) = fingerprint {
            headers.insert(
                dash_common::tls::CLIENT_CERT_HEADER.to_string(),
                fp.to_string(),
            );
        }
        HttpRequest {
            method: "GET".into(),
            target: "/internal/replication/wal".into(),
            headers,
            body: Vec::new(),
        }
    }

    #[test]
    fn off_by_default_and_required_needs_a_verified_certificate() {
        let off = ReplicationClientCertPolicy::from_values(None, None);
        assert!(off.admits(&request(None)));
        let required = ReplicationClientCertPolicy::from_values(Some("true"), None);
        assert!(!required.admits(&request(None)));
        assert!(required.admits(&request(Some(&"a".repeat(64)))));
    }

    #[test]
    fn allowlist_admits_only_listed_fingerprints() {
        let listed = "AB:".repeat(31) + "AB";
        let policy = ReplicationClientCertPolicy::from_values(None, Some(&format!(" {listed} ,")));
        assert!(policy.required());
        assert!(policy.admits(&request(Some(&"ab".repeat(32)))));
        assert!(!policy.admits(&request(Some(&"cd".repeat(32)))));
        assert!(!policy.admits(&request(None)));
    }

    #[test]
    fn malformed_values_deny_everything() {
        let policy = ReplicationClientCertPolicy::from_values(Some("sometimes"), None);
        assert!(policy.invalid.is_some());
        assert!(!policy.admits(&request(Some(&"a".repeat(64)))));
        let policy = ReplicationClientCertPolicy::from_values(None, Some("abc"));
        assert!(policy.invalid.is_some());
    }
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
    /// Replicated lines skipped because lenient replay would quarantine
    /// them (legacy poison), cumulative.
    pub(crate) skipped_records_total: u64,
    /// Leader checkpoints crossed without a resync (generation switches).
    pub(crate) generation_switches_total: u64,
    /// Bytes of chunked exports downloaded.
    pub(crate) export_bytes_total: u64,
    /// Set while the follower cannot make progress for a reason retrying
    /// will not fix (oversized response / commit group); reported by
    /// `/ready` and `/metrics`.
    pub(crate) blocked_reason: Option<&'static str>,
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
            skipped_records_total: 0,
            generation_switches_total: 0,
            export_bytes_total: 0,
            blocked_reason: None,
        }
    }
}

const BLOCKED_RESPONSE_TOO_LARGE: &str = "replication_response_too_large";
const BLOCKED_GROUP_TOO_LARGE: &str = "replication_group_too_large";
/// The leader runs a release older than 0.3.0 (see
/// [`dash_common::replication_client::LEGACY_LEADER_ERROR`]).
const BLOCKED_LEADER_TOO_OLD: &str = "replication_leader_too_old";
const RESPONSE_TOO_LARGE_MARKER: &str = "byte limit";

/// Failures that retrying cannot fix: the leader's answer is permanently
/// larger than this follower accepts, or a commit group cannot be shipped.
fn classify_blocking_error(error: &str) -> Option<&'static str> {
    if error.contains(BLOCKED_GROUP_TOO_LARGE) {
        Some(BLOCKED_GROUP_TOO_LARGE)
    } else if error.contains(dash_common::replication_client::LEGACY_LEADER_ERROR) {
        Some(BLOCKED_LEADER_TOO_OLD)
    } else if error.contains(RESPONSE_TOO_LARGE_MARKER) {
        Some(BLOCKED_RESPONSE_TOO_LARGE)
    } else {
        None
    }
}

/// First bytes of a non-200 response body, for the error message.
fn body_excerpt(body: &str) -> String {
    body.chars().take(200).collect()
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
            let path = config.offset_path.clone().or_else(|| {
                self.wal
                    .as_ref()
                    .map(|wal| wal_state_path(lock_wal(wal).path()))
            });
            if let Some(path) = path.as_deref() {
                match read_state(path) {
                    Some(saved)
                        if self
                            .wal
                            .as_ref()
                            .and_then(|wal| lock_wal(wal).wal_record_count().ok())
                            != Some(saved.offset) =>
                    {
                        // The local WAL does not hold what the cursor claims
                        // (restored/truncated WAL, or no WAL at all): resuming
                        // would silently skip records, so rebuild from the
                        // leader's export.
                        eprintln!(
                            "ingestion replication follower: saved cursor (generation={:?} offset={}) does not match the local WAL; forcing full resync",
                            saved.generation, saved.offset
                        );
                        self.replication_follower.force_resync = true;
                    }
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
                            .and_then(|wal| lock_wal(wal).wal_record_count().ok())
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
        if let Some(wal) = self.wal.as_ref()
            && let Err(err) = lock_wal(wal).flush_pending_sync_if_unsynced()
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

    /// Apply one delta batch: mirror it to the WAL (one write, one fsync),
    /// then apply it to the store in place (time and memory proportional to
    /// the frame, one redb transaction). Every line is parsed first, so a
    /// frame with an unreadable record changes nothing. A record that
    /// parses but cannot be applied means this follower diverged from the
    /// leader: the WAL append is rolled back and the follower is flagged for
    /// a full resync, which replaces its state wholesale.
    pub(super) fn apply_replication_delta_frame(
        &mut self,
        frame: &ReplicationDeltaFrame,
    ) -> Result<(), StoreError> {
        if frame.switch_from.is_some() {
            self.switch_replication_generation(frame.generation)?;
        }
        if !frame.wal_lines.is_empty() {
            for line in &frame.wal_lines {
                store::check_replicated_line(line)?;
            }
            #[cfg(test)]
            failpoint::maybe_panic();
            let mut rollback_point = None;
            if let Some(wal) = self.wal.as_ref() {
                let mut wal = lock_wal(wal);
                let point = wal.begin_rollback_point()?;
                if let Err(err) = wal.append_replicated_lines(&frame.wal_lines) {
                    if let Err(rollback_err) = wal.rollback_to(point) {
                        eprintln!(
                            "replication rollback failed after WAL append error: {rollback_err:?}"
                        );
                    }
                    return Err(err);
                }
                rollback_point = Some(point);
            }
            let outcome = match self.store.apply_replicated_lines(&frame.wal_lines, true) {
                Ok(outcome) => outcome,
                Err(err) => {
                    if let (Some(wal), Some(point)) = (self.wal.as_ref(), rollback_point)
                        && let Err(rollback_err) = lock_wal(wal).rollback_to(point)
                    {
                        eprintln!(
                            "replication rollback failed after apply error: {rollback_err:?}"
                        );
                    }
                    self.replication_follower.force_resync = true;
                    return Err(err);
                }
            };
            if let Some(reason) = outcome.disk_error {
                eprintln!("replication: disk mirror degraded after commit: {reason}");
            }
            // A tenant whose last claim a replicated tombstone removed is no
            // longer listed by the store, but its segments must be refreshed
            // too (to an empty claim set).
            let mut tenants: std::collections::BTreeSet<String> =
                self.store.tenant_ids().into_iter().collect();
            tenants.extend(
                frame
                    .wal_lines
                    .iter()
                    .filter_map(|line| store::tombstone_from_wal_line(line))
                    .map(|tombstone| tombstone.tenant_id().to_string()),
            );
            for tenant_id in tenants {
                self.publish_segments_for_tenant(&tenant_id);
            }
            self.replication_applied_records_total = self
                .replication_applied_records_total
                .saturating_add(outcome.applied as u64);
            self.replication_follower.skipped_records_total = self
                .replication_follower
                .skipped_records_total
                .saturating_add(outcome.skipped as u64);
        }
        self.replication_pull_success_total = self.replication_pull_success_total.saturating_add(1);
        self.replication_last_offset = frame.next_offset;
        self.replication_follower.generation = Some(frame.generation);
        self.replication_last_error = None;
        self.persist_replication_state();
        Ok(())
    }

    /// The leader closed our generation with a checkpoint exactly at our
    /// position, so this node's state is the leader's new snapshot. Compact
    /// the local WAL the same way (so the saved offset keeps matching the
    /// local WAL) and continue at offset 0 of `generation`. Like a leader's
    /// checkpoint, only the rotation happens here; the snapshot is written in
    /// the background (a crash before it is published replays the base
    /// snapshot and the closed WAL, and the local WAL still matches the
    /// cursor). A failed local checkpoint flags a full resync.
    fn switch_replication_generation(&mut self, generation: u64) -> Result<(), StoreError> {
        if let Some(wal) = self.wal.as_ref().map(std::sync::Arc::clone) {
            // The previous switch's snapshot may still be being written.
            self.settle_checkpoint();
            let begun = self.store.begin_checkpoint(&mut lock_wal(&wal));
            match begun {
                Ok(job) => self.spawn_checkpoint_writer(job, "replication switch"),
                Err(err) => {
                    self.replication_follower.force_resync = true;
                    return Err(err);
                }
            }
            if let Some(persistence) = self.vector_index_persistence.as_ref() {
                persistence.request_save();
            }
        }
        self.replication_last_offset = 0;
        self.replication_follower.generation = Some(generation);
        self.replication_follower.generation_switches_total = self
            .replication_follower
            .generation_switches_total
            .saturating_add(1);
        self.persist_replication_state();
        eprintln!(
            "ingestion replication follower: leader checkpoint crossed without resync, now at generation={generation} offset=0"
        );
        Ok(())
    }

    /// Replace this node's state with the leader's export (single-response
    /// form, from leaders without chunked export). The redb handle is kept
    /// and rewritten from the new state.
    pub(super) fn apply_replication_export_frame(
        &mut self,
        frame: ReplicationExportFrame,
    ) -> Result<(), StoreError> {
        let export = WalReplicationExport {
            snapshot_lines: frame.snapshot_lines,
            wal_lines: frame.wal_lines,
        };
        let mut fresh = InMemoryStore::new_with_ann_tuning(self.store.ann_tuning().clone());
        let mut skipped = 0u64;
        for line in export.snapshot_lines.iter().chain(export.wal_lines.iter()) {
            if !fresh.apply_persisted_record_line_lenient(line)? {
                skipped += 1;
            }
        }
        self.replication_follower.skipped_records_total = self
            .replication_follower
            .skipped_records_total
            .saturating_add(skipped);
        if let Some(wal) = self.wal.as_ref().map(std::sync::Arc::clone) {
            // A local checkpoint still writing its snapshot finishes first.
            self.settle_checkpoint();
            self.clear_replication_state()?;
            lock_wal(&wal).replace_with_replication_export(&export)?;
        }
        self.finish_replication_resync(
            fresh,
            frame.generation,
            export.wal_lines.len(),
            (export.snapshot_lines.len() + export.wal_lines.len()) as u64,
        );
        Ok(())
    }

    /// Replace this node's state with a verified chunked export file. The
    /// saved cursor is removed before the local WAL is replaced and written
    /// again after, so a crash in between restarts with a resync (which
    /// reuses the downloaded file) instead of resuming a stale cursor.
    pub(super) fn apply_replication_export_file(
        &mut self,
        download: &store::DownloadedExport,
    ) -> Result<(), StoreError> {
        let mut fresh = InMemoryStore::new_with_ann_tuning(self.store.ann_tuning().clone());
        let mut skipped = 0u64;
        let mut applied = 0u64;
        download.file.for_each_line(|_, line| {
            if !fresh.apply_persisted_record_line_lenient(line)? {
                skipped += 1;
            }
            applied += 1;
            Ok(())
        })?;
        self.replication_follower.skipped_records_total = self
            .replication_follower
            .skipped_records_total
            .saturating_add(skipped);
        if let Some(wal) = self.wal.as_ref().map(std::sync::Arc::clone) {
            // A local checkpoint still writing its snapshot finishes first.
            self.settle_checkpoint();
            self.clear_replication_state()?;
            #[cfg(test)]
            crash_point::hit("ingest.resync.cursor_cleared")?;
            lock_wal(&wal).replace_with_replication_export_file(&download.file)?;
            #[cfg(test)]
            crash_point::hit("ingest.resync.wal_replaced")?;
        }
        self.finish_replication_resync(
            fresh,
            download.manifest.generation,
            download.manifest.wal_records,
            applied,
        );
        self.replication_follower.export_bytes_total = self
            .replication_follower
            .export_bytes_total
            .saturating_add(download.fetched_bytes);
        Ok(())
    }

    /// Swap in a resynced state and continue from `(generation, offset)`.
    fn finish_replication_resync(
        &mut self,
        fresh: InMemoryStore,
        generation: u64,
        offset: usize,
        applied: u64,
    ) {
        // Tenants the export no longer holds (erased on the leader) get their
        // segments refreshed to an empty claim set as well.
        let mut tenants: std::collections::BTreeSet<String> =
            self.store.tenant_ids().into_iter().collect();
        if let Err(err) = self.store.replace_state_from(fresh) {
            eprintln!("replication resync: disk rewrite degraded: {err:?}");
        }
        tenants.extend(self.store.tenant_ids());
        for tenant_id in tenants {
            self.publish_segments_for_tenant(&tenant_id);
        }
        self.replication_pull_success_total = self.replication_pull_success_total.saturating_add(1);
        self.replication_applied_records_total = self
            .replication_applied_records_total
            .saturating_add(applied);
        self.replication_resync_total = self.replication_resync_total.saturating_add(1);
        // Offsets count WAL lines only, the unit the leader reports.
        self.replication_last_offset = offset;
        self.replication_follower.generation = Some(generation);
        self.replication_follower.force_resync = false;
        self.replication_follower.leader_total_records = offset;
        self.replication_last_error = None;
        self.persist_replication_state();
    }

    /// Removes the persisted cursor (directory synced) before the local
    /// files are replaced.
    fn clear_replication_state(&self) -> Result<(), StoreError> {
        let Some(path) = self.replication_follower.state_path.clone() else {
            return Ok(());
        };
        match std::fs::remove_file(&path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(err) => return Err(err.into()),
        }
        if let Some(dir) = std::path::Path::new(&path).parent()
            && !dir.as_os_str().is_empty()
            && let Ok(dir_file) = std::fs::File::open(dir)
        {
            dir_file.sync_all()?;
        }
        Ok(())
    }

    /// Where a chunked export is downloaded: next to the WAL, next to the
    /// state file, or in the temp directory.
    fn replication_download_paths(&self) -> store::DownloadPaths {
        if let Some(wal) = self.wal.as_ref() {
            return store::DownloadPaths::for_wal(lock_wal(wal).path());
        }
        match self.replication_follower.state_path.as_deref() {
            Some(path) => store::DownloadPaths::new(format!("{path}.resync")),
            None => store::DownloadPaths::new(
                std::env::temp_dir().join(format!("dash-ingest-resync-{}", std::process::id())),
            ),
        }
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
        Some(if let Some(reason) = follower.blocked_reason {
            Err(reason)
        } else if !follower.synced_once {
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
            // A stable code, never the raw text (hosts, paths, upstream
            // bodies); the raw error stays in logs and metrics only.
            Some(err) => format!("\"{}\"", dash_common::replication_client::error_code(err)),
            None => "null".to_string(),
        };
        let blocked = match self.replication_follower.blocked_reason {
            Some(reason) => format!("\"{reason}\""),
            None => "null".to_string(),
        };
        Some(format!(
            "{{\"generation\":{generation},\"offset\":{},\"leader_total_records\":{},\"lag_records\":{},\"last_success_age_ms\":{},\"consecutive_failures\":{},\"resyncs_total\":{},\"generation_switches_total\":{},\"skipped_records_total\":{},\"blocked_reason\":{blocked},\"last_error\":{last_error}}}",
            self.replication_last_offset,
            self.replication_follower.leader_total_records,
            self.replication_lag_records(),
            self.replication_last_success_age_ms(),
            self.replication_follower.consecutive_failures,
            self.replication_resync_total,
            self.replication_follower.generation_switches_total,
            self.replication_follower.skipped_records_total,
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
dash_ingest_replication_generation {}\n\
# TYPE dash_ingest_replication_skipped_records_total counter\n\
dash_ingest_replication_skipped_records_total {}\n\
# TYPE dash_ingest_replication_generation_switches_total counter\n\
dash_ingest_replication_generation_switches_total {}\n\
# TYPE dash_ingest_replication_export_bytes_total counter\n\
dash_ingest_replication_export_bytes_total {}\n\
# TYPE dash_ingest_replication_blocked_response_too_large gauge\n\
dash_ingest_replication_blocked_response_too_large {}\n\
# TYPE dash_ingest_replication_blocked_group_too_large gauge\n\
dash_ingest_replication_blocked_group_too_large {}\n",
            self.replication_lag_records(),
            self.replication_last_success_age_ms(),
            self.replication_follower.consecutive_failures,
            self.replication_follower.generation.unwrap_or(0),
            self.replication_follower.skipped_records_total,
            self.replication_follower.generation_switches_total,
            self.replication_follower.export_bytes_total,
            (self.replication_follower.blocked_reason == Some(BLOCKED_RESPONSE_TOO_LARGE)) as u8,
            (self.replication_follower.blocked_reason == Some(BLOCKED_GROUP_TOO_LARGE)) as u8,
        )
    }

    /// Size and eviction counters of the leader's commit-status table.
    pub(super) fn replication_commit_status_metrics_text(&mut self) -> String {
        self.replication_commit_status
            .expire(std::time::Instant::now());
        format!(
            "# TYPE dash_ingest_replication_commit_status_entries gauge\n\
dash_ingest_replication_commit_status_entries {}\n\
# TYPE dash_ingest_replication_commit_status_evicted_total counter\n\
dash_ingest_replication_commit_status_evicted_total {}\n",
            self.replication_commit_status.len(),
            self.replication_commit_status.evicted_total(),
        )
    }

    /// Leader-side replication metrics (served from the WAL).
    pub(super) fn replication_leader_metrics_text(&self) -> String {
        let Some(wal) = self.wal.as_ref() else {
            return String::new();
        };
        let wal = lock_wal(wal);
        let mut text = format!(
            "# TYPE dash_ingest_replication_group_too_large_total counter\n\
dash_ingest_replication_group_too_large_total {}\n\
# TYPE dash_ingest_replication_view_skipped_lines gauge\n\
dash_ingest_replication_view_skipped_lines {}\n\
# TYPE dash_ingest_replication_closed_generation_retained gauge\n\
dash_ingest_replication_closed_generation_retained {}\n",
            wal.replication_group_too_large_total(),
            wal.replication_skipped_lines(),
            wal.closed_generation().is_some() as u8,
        );
        drop(wal);
        if let Some(exports) = self.replication_exports.as_ref() {
            let stats = exports.stats();
            text.push_str(&format!(
                "# TYPE dash_ingest_replication_exports_built_total counter\n\
dash_ingest_replication_exports_built_total {}\n\
# TYPE dash_ingest_replication_exports_reused_total counter\n\
dash_ingest_replication_exports_reused_total {}\n\
# TYPE dash_ingest_replication_export_chunks_served_total counter\n\
dash_ingest_replication_export_chunks_served_total {}\n\
# TYPE dash_ingest_replication_export_bytes_served_total counter\n\
dash_ingest_replication_export_bytes_served_total {}\n\
# TYPE dash_ingest_replication_exports_retained gauge\n\
dash_ingest_replication_exports_retained {}\n\
# TYPE dash_ingest_replication_exports_retained_bytes gauge\n\
dash_ingest_replication_exports_retained_bytes {}\n",
                stats.built_total,
                stats.reused_total,
                stats.chunks_served_total,
                stats.bytes_served_total,
                stats.retained,
                stats.retained_bytes,
            ));
        }
        text
    }
}

/// Test-only failpoint: makes the next replication apply on this thread
/// panic, to prove a panic never wedges the shared runtime.
#[cfg(test)]
mod failpoint {
    use std::cell::Cell;

    thread_local! {
        static PANIC_NEXT_APPLY: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) fn arm() {
        PANIC_NEXT_APPLY.with(|flag| flag.set(true));
    }

    pub(super) fn maybe_panic() {
        if PANIC_NEXT_APPLY.with(|flag| flag.replace(false)) {
            panic!("injected replication apply panic");
        }
    }
}

/// Test-only crash points inside a resync: an armed point makes the apply
/// stop there with an error, leaving the files as a crash would.
#[cfg(test)]
pub(super) mod crash_point {
    use std::sync::Mutex;

    static ARMED: Mutex<Option<&'static str>> = Mutex::new(None);

    pub(crate) fn arm(name: &'static str) {
        *ARMED.lock().unwrap_or_else(|p| p.into_inner()) = Some(name);
    }

    pub(crate) fn hit(name: &'static str) -> Result<(), store::StoreError> {
        let mut armed = ARMED.lock().unwrap_or_else(|p| p.into_inner());
        if *armed == Some(name) {
            *armed = None;
            return Err(store::StoreError::Io(format!("crash point {name}")));
        }
        Ok(())
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
            // The follower rebuilds its state from a full export, so the
            // possibly half-updated state is replaced wholesale: it is safe
            // to recover a mutex poisoned by this panic.
            let mut guard = runtime
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            guard.replication_follower.force_resync = true;
            drop(guard);
            runtime.clear_poison();
            Err(format!(
                "replication pull panicked: {}",
                panic_message(&panic)
            ))
        }
    };
    let mut guard = runtime
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match outcome {
        Ok(()) => {
            guard.replication_follower.blocked_reason = None;
            guard.record_replication_pull_success();
            0
        }
        Err(err) => {
            eprintln!("replication pull tick failed: {err}");
            guard.replication_follower.blocked_reason = classify_blocking_error(&err);
            guard.observe_replication_pull_failure(err);
            guard.replication_follower.consecutive_failures = guard
                .replication_follower
                .consecutive_failures
                .saturating_add(1);
            guard.replication_follower.consecutive_failures
        }
    }
}

/// Runs `apply` on the runtime under its lock without ever poisoning the
/// mutex: a panic is caught while the guard is still held, so the guard is
/// dropped on the normal path. State may be half-updated after a panic, so
/// the follower is flagged for a full resync (which replaces it wholesale).
fn apply_guarded<T>(
    runtime: &SharedRuntime,
    apply: impl FnOnce(&mut IngestionRuntime) -> Result<T, StoreError>,
) -> Result<T, StoreError> {
    // Replication writes the WAL and the store directly: wait for pipelined
    // single ingests to finish first.
    let mut guard = group_commit::lock_drained(runtime)
        .map_err(|_| StoreError::Io("replication runtime lock unavailable".to_string()))?;
    match catch_unwind(AssertUnwindSafe(|| apply(&mut guard))) {
        Ok(result) => result,
        Err(panic) => {
            guard.replication_follower.force_resync = true;
            Err(StoreError::Io(format!(
                "replication apply panicked: {}",
                panic_message(&panic)
            )))
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
            "replication source WAL delta returned status {} ({})",
            delta_response.status,
            body_excerpt(&delta_response.body)
        ));
    }
    let delta_frame = parse_replication_delta_frame(&delta_response.body, config.max_records)?;
    if let Some(from) = delta_frame.switch_from {
        // The leader checkpointed exactly at our position: continue in the
        // new generation from offset 0. Only accepted for our own position.
        if delta_frame.needs_resync
            || delta_frame.from_offset != 0
            || Some(from.generation) != from_generation
            || from.records != from_offset
        {
            return Err(format!(
                "replication frame switches generation from {}:{}, but this follower is at {from_generation:?}:{from_offset}",
                from.generation, from.records
            ));
        }
    } else if delta_frame.from_offset != from_offset {
        return Err(format!(
            "replication frame from_offset {} does not match requested {from_offset}",
            delta_frame.from_offset
        ));
    }
    runtime
        .lock()
        .map_err(|_| "replication runtime lock unavailable".to_string())?
        .note_leader_total(delta_frame.total_records);
    let generation_changed = from_generation.is_some_and(|ours| ours != delta_frame.generation);
    if delta_frame.needs_resync || (generation_changed && delta_frame.switch_from.is_none()) {
        return resync_from_export(runtime, config);
    }
    if delta_frame.next_offset > delta_frame.total_records {
        return Err("replication frame next_offset exceeds total_records".to_string());
    }
    // Apply only whole commit groups: an unterminated group at the end of
    // the frame is held back and re-fetched from its first line next poll.
    let mut delta_frame = delta_frame;
    let keep = store::complete_group_prefix_len(&delta_frame.wal_lines);
    if keep < delta_frame.wal_lines.len() {
        delta_frame.wal_lines.truncate(keep);
        delta_frame.next_offset = delta_frame.from_offset + keep;
    }
    let commit_ids = extract_batch_commit_ids_from_wal_lines(&delta_frame.wal_lines)?;
    apply_guarded(runtime, |rt| rt.apply_replication_delta_frame(&delta_frame))
        .map_err(|err| format!("replication delta apply failed: {err:?}"))?;
    acknowledge_replication_commits(config, &commit_ids)
        .map_err(|err| format!("replication delta commit ack failed: {err}"))
}

/// Full resync: download the leader's export in chunks (resumable,
/// checksummed) and apply it; leaders without chunked export are read with
/// the single-response export.
fn resync_from_export(
    runtime: &SharedRuntime,
    config: &ReplicationPullConfig,
) -> Result<(), String> {
    let paths = runtime
        .lock()
        .map_err(|_| "replication runtime lock unavailable".to_string())?
        .replication_download_paths();
    let base = config.source_base_url.clone();
    let token = config.token.clone();
    let max_bytes = config.max_response_bytes;
    let mut source = store::HttpExportSource::new(|path: &str| {
        request_replication_source(&format!("{base}{path}"), token.as_deref(), max_bytes)
            .map(|response| (response.status, response.body))
    });
    let download =
        match store::download_export(&mut source, &paths, config.effective_chunk_bytes())? {
            store::DownloadOutcome::Unsupported => {
                return resync_from_single_export(runtime, config);
            }
            store::DownloadOutcome::Complete(download) => download,
        };
    let mut commit_ids = Vec::new();
    let mut seen = HashSet::new();
    download
        .file
        .for_each_line(|_, line| {
            if let Some(commit_id) = store::batch_commit_id_from_wal_line(line)
                && seen.insert(commit_id.clone())
            {
                commit_ids.push(commit_id);
            }
            Ok(())
        })
        .map_err(|err| format!("replication resync read failed: {err:?}"))?;
    apply_guarded(runtime, |rt| rt.apply_replication_export_file(&download))
        .map_err(|err| format!("replication resync apply failed: {err:?}"))?;
    paths.remove();
    acknowledge_replication_commits(config, &commit_ids)
        .map_err(|err| format!("replication resync commit ack failed: {err}"))
}

fn resync_from_single_export(
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
            "replication export returned non-200 status: {} ({})",
            export_response.status,
            body_excerpt(&export_response.body)
        ));
    }
    let export_frame = parse_replication_export_frame(&export_response.body)?;
    let mut combined_lines =
        Vec::with_capacity(export_frame.snapshot_lines.len() + export_frame.wal_lines.len());
    combined_lines.extend(export_frame.snapshot_lines.iter().cloned());
    combined_lines.extend(export_frame.wal_lines.iter().cloned());
    let commit_ids = extract_batch_commit_ids_from_wal_lines(&combined_lines)?;
    apply_guarded(runtime, |rt| {
        rt.apply_replication_export_frame(export_frame)
    })
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
        // 404: the leader no longer tracks this commit (evicted by its
        // retention policy or restarted). The data is already applied, so a
        // forgotten commit must not fail the pull.
        if response.status == 404 {
            continue;
        }
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
    Ok(store::batch_commit_id_from_wal_line(line))
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
        spawn_mock_replication_source_with_ack(
            delta_response_body,
            expected_requests,
            "HTTP/1.1 200 OK",
        )
    }

    fn spawn_mock_replication_source_with_ack(
        delta_response_body: String,
        expected_requests: usize,
        ack_status_line: &'static str,
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
                    render_http_response(ack_status_line, "status=ok\n")
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
        let body = render_replication_delta_frame(
            &WalReplicationFrame {
                generation: 9,
                from_offset: 2,
                next_offset: 4,
                total_records: 10,
                needs_resync: false,
                wal_lines: vec![
                    "C\tc1\ttenant-a\ttext\t0.9\tnull\t\t".to_string(),
                    "B\tcommit-1\t1\t1700000000000\t2:c1".to_string(),
                ],
                switched_from: None,
            },
            false,
        );
        let frame = parse_replication_delta_frame(&body, 512).expect("delta frame should parse");
        assert_eq!(frame.generation, 9);
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
        assert_eq!(frame.generation, 11);
        assert_eq!(frame.snapshot_lines.len(), 1);
        assert_eq!(frame.wal_lines.len(), 1);
    }

    /// Frames of a pre-0.3.0 leader carry no generation. Following such a
    /// leader could silently skip records, so both frame kinds are refused.
    #[test]
    fn frames_without_a_generation_from_a_pre_0_3_leader_are_refused() {
        let delta = "status=ok\nneeds_resync=0\nfrom_offset=0\nnext_offset=1\ntotal_records=1\nrecords=1\nC\tc1\ttenant-a\ttext\t0.9\tnull\t\t\n";
        let err = parse_replication_delta_frame(delta, 512).expect_err("legacy delta");
        assert_eq!(err, dash_common::replication_client::LEGACY_LEADER_ERROR);
        let export = "status=ok\nsnapshot_records=0\nwal_records=0\nSNAPSHOT\nWAL\n";
        let err = parse_replication_export_frame(export).expect_err("legacy export");
        assert_eq!(err, dash_common::replication_client::LEGACY_LEADER_ERROR);
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
        let delta_body = "status=ok\ngeneration=1\nneeds_resync=0\nfrom_offset=0\nnext_offset=2\ntotal_records=2\nrecords=2\nC\tclaim-1\ttenant-a\ttext\t0.9\tnull\t\t\nB\tcommit-1\t1\t1700000000000\t7:claim-1\n".to_string();
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
            pull_request.starts_with(
                "GET /internal/replication/wal?from_offset=0&max_records=64&gen_switch=1 HTTP/1.1"
            ),
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
    fn ack_for_a_commit_the_leader_forgot_does_not_fail_the_pull() {
        let runtime = Arc::new(Mutex::new(super::super::IngestionRuntime::in_memory(
            store::InMemoryStore::new(),
        )));
        let delta_body = "status=ok\ngeneration=1\nneeds_resync=0\nfrom_offset=0\nnext_offset=2\ntotal_records=2\nrecords=2\nC\tclaim-1\ttenant-a\ttext\t0.9\tnull\t\t\nB\tcommit-1\t1\t1700000000000\t7:claim-1\n".to_string();
        let (source_base_url, requests, source_handle) =
            spawn_mock_replication_source_with_ack(delta_body, 2, "HTTP/1.1 404 Not Found");
        let config = ReplicationPullConfig {
            source_base_url,
            local_replica_id: Some("node-b".to_string()),
            ..ReplicationPullConfig::new("http://127.0.0.1:1")
        };

        run_replication_pull_tick(&runtime, &config);

        let _pull = requests.recv_timeout(Duration::from_secs(2)).expect("pull");
        let ack = requests.recv_timeout(Duration::from_secs(2)).expect("ack");
        assert!(ack.starts_with("POST /internal/replication/ack?"), "{ack}");
        source_handle.join().expect("mock source joins");
        let guard = runtime.lock().expect("lock");
        assert_eq!(guard.replication_pull_failure_total, 0);
        assert_eq!(guard.replication_pull_success_total, 1);
    }

    #[test]
    fn run_replication_pull_tick_skips_ack_without_local_replica_id() {
        let runtime = Arc::new(Mutex::new(super::super::IngestionRuntime::in_memory(
            store::InMemoryStore::new(),
        )));
        let delta_body = "status=ok\ngeneration=1\nneeds_resync=0\nfrom_offset=0\nnext_offset=2\ntotal_records=2\nrecords=2\nC\tclaim-2\ttenant-a\ttext\t0.9\tnull\t\t\nB\tcommit-2\t1\t1700000000000\t7:claim-2\n".to_string();
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
            pull_request.starts_with(
                "GET /internal/replication/wal?from_offset=0&max_records=64&gen_switch=1 HTTP/1.1"
            ),
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
    fn extract_batch_commit_ids_reads_v2_lines_and_skips_group_markers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("wal");
        let mut wal = store::FileWal::open(&path).expect("open wal");
        wal.begin_group("grp-1", 1).expect("begin group");
        wal.append_batch_commit("batch-v2", 1, 2, &["c1".to_string()])
            .expect("batch commit");
        wal.append_batch_commit("grp-1", 0, 3, &[])
            .expect("end group");
        wal.begin_group("c9", 5).expect("begin single group");
        wal.append_batch_commit("~tx:c9", 1, 6, &["c9".to_string()])
            .expect("single-ingest end marker");
        wal.append_batch_commit("cr\rid", 1, 4, &["c2".to_string()])
            .expect("batch commit with CR");
        wal.flush_pending_sync().expect("flush");
        let lines: Vec<String> = std::fs::read_to_string(&path)
            .expect("read wal")
            .lines()
            .map(str::to_string)
            .collect();
        assert!(lines.iter().all(|l| l.starts_with("B2\t")), "{lines:?}");
        let ids = extract_batch_commit_ids_from_wal_lines(&lines).expect("ids");
        assert_eq!(
            ids,
            vec![
                "batch-v2".to_string(),
                "grp-1".to_string(),
                "cr\rid".to_string()
            ],
            "group begin/single-ingest markers are framing, not batch commits"
        );
    }

    #[test]
    fn blocking_errors_are_classified_and_make_the_follower_not_ready() {
        assert_eq!(
            classify_blocking_error("replication response exceeds 100 byte limit"),
            Some("replication_response_too_large")
        );
        assert_eq!(
            classify_blocking_error(
                "replication source WAL delta returned status 500 (internal persistence error: replication_group_too_large: ...)"
            ),
            Some("replication_group_too_large")
        );
        assert_eq!(classify_blocking_error("connection refused"), None);

        let mut runtime = super::super::IngestionRuntime::in_memory(store::InMemoryStore::new());
        runtime.replication_follower.enabled = true;
        runtime.replication_follower.synced_once = true;
        runtime.replication_follower.last_success = Some(Instant::now());
        assert_eq!(runtime.replication_readiness(), Some(Ok(())));
        runtime.replication_follower.blocked_reason = Some("replication_group_too_large");
        assert_eq!(
            runtime.replication_readiness(),
            Some(Err("replication_group_too_large"))
        );
        assert!(
            runtime
                .replication_ready_json()
                .expect("json")
                .contains("\"blocked_reason\":\"replication_group_too_large\"")
        );
    }

    struct XorShift(u64);
    impl XorShift {
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

    /// A crash in the middle of a resync's swap restarts into a resync that
    /// applies the verified download again (no network), with nothing lost
    /// or duplicated and a cursor that matches the WAL.
    #[test]
    fn a_crash_during_the_resync_swap_recovers_from_the_downloaded_export() {
        use schema::claim_builder;
        for point in ["ingest.resync.cursor_cleared", "ingest.resync.wal_replaced"] {
            let dir = tempfile::tempdir().expect("tempdir");
            let leader_path = dir.path().join("leader.wal");
            let mut leader_wal = store::FileWal::open(&leader_path).expect("leader wal");
            let mut leader = store::InMemoryStore::new();
            for i in 0..10 {
                let claim = claim_builder(&format!("l{i}"), "tenant-r", "leader claim", 0.9);
                leader
                    .ingest_bundle_persistent(&mut leader_wal, claim, vec![], vec![])
                    .expect("leader write");
                if i == 6 {
                    leader
                        .checkpoint_and_compact(&mut leader_wal)
                        .expect("checkpoint");
                }
            }
            let exports = store::ReplicationExportStore::for_wal(&leader_path);
            let leader_wal = Mutex::new(leader_wal);

            let follower_path = dir.path().join("follower.wal");
            {
                let mut wal = store::FileWal::open(&follower_path).expect("follower wal");
                let claim = claim_builder("stale", "tenant-r", "stale claim", 0.9);
                store::InMemoryStore::new()
                    .ingest_bundle_persistent(&mut wal, claim, vec![], vec![])
                    .expect("stale write");
            }
            let paths = store::DownloadPaths::for_wal(&follower_path);
            let mut source = store::LocalExportSource {
                store: &exports,
                wal: &leader_wal,
            };
            let store::DownloadOutcome::Complete(download) =
                store::download_export(&mut source, &paths, 256).expect("download")
            else {
                panic!("complete");
            };
            // Unreachable leader: a resync must come from the download.
            let config = ReplicationPullConfig::new("http://127.0.0.1:9");
            let start = || -> SharedRuntime {
                let wal = store::FileWal::open(&follower_path).expect("follower wal");
                let store = store::InMemoryStore::load_from_wal(&wal).expect("load");
                let runtime = super::super::IngestionRuntime::persistent(
                    store,
                    wal,
                    store::CheckpointPolicy::default(),
                );
                Arc::new(Mutex::new(runtime))
            };

            let runtime = start();
            let (_, _, force_resync) = runtime.lock().unwrap().replication_cursor(&config);
            assert!(force_resync, "{point}: populated WAL without a cursor");
            crash_point::arm(point);
            let err = resync_from_export(&runtime, &config).expect_err("crash point");
            assert!(err.contains("crash point"), "{point}: {err}");
            assert!(
                !dir.path().join("follower.wal.replication").exists(),
                "{point}: the cursor is gone before the files change"
            );
            drop(runtime);

            let runtime = start();
            let (_, _, force_resync) = runtime.lock().unwrap().replication_cursor(&config);
            assert!(force_resync, "{point}: the restart resyncs");
            resync_from_export(&runtime, &config).expect("resync from the download");
            let saved = read_state(&format!("{}.replication", follower_path.display()))
                .expect("cursor written");
            assert_eq!(saved.generation, Some(download.manifest.generation));
            assert_eq!(saved.offset, download.manifest.wal_records);
            assert!(!paths.part.exists(), "{point}: download removed");
            drop(runtime);
            let wal = store::FileWal::open(&follower_path).expect("reopen");
            assert_eq!(wal.wal_record_count().unwrap(), saved.offset);
            let replayed = store::InMemoryStore::load_from_wal(&wal).expect("replay");
            let mut ids: Vec<String> = replayed
                .claims_for_tenant("tenant-r")
                .into_iter()
                .map(|c| c.claim_id)
                .collect();
            ids.sort();
            let mut want: Vec<String> = (0..10).map(|i| format!("l{i}")).collect();
            want.sort();
            assert_eq!(ids, want, "{point}: exactly the leader's claims");
        }
    }

    #[test]
    fn mutated_frames_never_panic_the_frame_parsers() {
        let delta = render_replication_delta_frame(
            &WalReplicationFrame {
                generation: 9,
                from_offset: 2,
                next_offset: 4,
                total_records: 10,
                needs_resync: false,
                wal_lines: vec![
                    "C\tc1\ttenant-a\ttext\t0.9\tnull\t\t".to_string(),
                    "B\tcommit-1\t1\t1700000000000\t2:c1".to_string(),
                ],
                switched_from: None,
            },
            false,
        );
        let export = render_replication_export_frame(
            &WalReplicationExport {
                snapshot_lines: vec!["C\tc1\ttenant-a\ttext\t0.9\tnull\t\t".to_string()],
                wal_lines: vec!["E\te1\tc1\tsource://x\tsupports\t0.8".to_string()],
            },
            11,
        );
        let huge = [
            "18446744073709551615",
            "18446744073709551614",
            "9223372036854775808",
            "-1",
            "",
        ];
        let mut rng = XorShift(0x9E37_79B9_7F4A_7C15);
        for iteration in 0..20_000 {
            let seed = if iteration % 2 == 0 { &delta } else { &export };
            let mut bytes = seed.clone().into_bytes();
            for _ in 0..=rng.below(3) {
                match rng.below(3) {
                    0 if !bytes.is_empty() => {
                        let i = rng.below(bytes.len());
                        bytes[i] = (rng.next() & 0x7f) as u8;
                    }
                    1 if !bytes.is_empty() => {
                        let i = rng.below(bytes.len());
                        bytes.remove(i);
                    }
                    _ => {
                        let text = String::from_utf8_lossy(&bytes).into_owned();
                        let replaced = text.replacen(
                            |c: char| c.is_ascii_digit(),
                            huge[rng.below(huge.len())],
                            1,
                        );
                        bytes = replaced.into_bytes();
                    }
                }
            }
            let Ok(body) = String::from_utf8(bytes) else {
                continue;
            };
            let outcome = catch_unwind(AssertUnwindSafe(|| {
                let _ = parse_replication_delta_frame(&body, 512);
                let _ = parse_replication_export_frame(&body);
            }));
            assert!(outcome.is_ok(), "frame parser panicked on {body:?}");
        }
    }

    #[test]
    fn panic_during_replication_apply_does_not_poison_the_runtime() {
        let runtime = Arc::new(Mutex::new(super::super::IngestionRuntime::in_memory(
            store::InMemoryStore::new(),
        )));
        let delta_body = "status=ok\ngeneration=1\nneeds_resync=0\nfrom_offset=0\nnext_offset=2\ntotal_records=2\nrecords=2\nC\tclaim-p\ttenant-a\ttext\t0.9\tnull\t\t\nB\tcommit-p\t1\t1700000000000\t7:claim-p\n".to_string();
        let (source_base_url, _requests, source_handle) =
            spawn_mock_replication_source(delta_body, 1);
        let config = ReplicationPullConfig {
            source_base_url,
            poll_interval: Duration::from_millis(500),
            max_records: 64,
            token: None,
            local_replica_id: None,
            ..ReplicationPullConfig::new("http://127.0.0.1:1")
        };

        failpoint::arm();
        let failures = run_replication_pull_tick(&runtime, &config);
        source_handle
            .join()
            .expect("mock replication source should join cleanly");

        assert_eq!(failures, 1, "the panic counts as one failed pull");
        assert!(
            !runtime.is_poisoned(),
            "a panic during apply must not poison the runtime mutex"
        );
        let guard = runtime
            .lock()
            .expect("runtime must stay lockable after a panic in replication apply");
        assert_eq!(guard.claims_len(), 0, "the panicked batch is not applied");
        assert!(guard.replication_follower.force_resync, "state is rebuilt");
        assert!(
            guard
                .replication_last_error
                .as_deref()
                .is_some_and(|e| e.contains("panicked")),
            "{:?}",
            guard.replication_last_error
        );
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
        let delta_body = "status=ok\ngeneration=1\nneeds_resync=0\nfrom_offset=0\nnext_offset=1\ntotal_records=1\nrecords=1\nB\tcommit-diverge-1\t1\t1700000000001\t5:c-new\n".to_string();
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
            pull_request.starts_with(
                "GET /internal/replication/wal?from_offset=0&max_records=64&gen_switch=1 HTTP/1.1"
            ),
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
