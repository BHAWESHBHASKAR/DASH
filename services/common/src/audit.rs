//! Shared audit-chain writer and verifier (SEC-17).
//!
//! # Canonical encoding (record version 2)
//!
//! Every chained audit record is a single JSON line. The SHA-256 `hash` of a
//! record is computed over its *canonical payload*: compact JSON with the
//! fields below **in exactly this order**, strings escaped as by
//! `serde_json` (RFC 8259, non-ASCII left as UTF-8), integers in decimal, and
//! absent values as `null`:
//!
//! ```text
//! {"v":2,"seq":N,"ts_unix_ms":N,"service":S,"action":S,"tenant_id":S|null,
//!  "claim_id":S|null,"status":N,"outcome":S,"reason":S,
//!  "actor":null|{"kind":S,"id":S|null},"request_id":S|null,"client_ip":S|null,
//!  "restart":null|{"prev_tail_seq":N,"prev_tail_hash":S,"why":S},
//!  "prev_hash":H}
//! ```
//!
//! The stored line is the canonical payload with `,"hash":"<H>"` appended
//! before the closing brace, so the key order on disk is the hashed order.
//! The verifier nevertheless rebuilds the payload from parsed values, so it
//! does not depend on how a reader orders keys. Records with unknown keys are
//! rejected (an unhashed field would be tamperable).
//!
//! Legacy records (no `"v"`) are still verified: the retrieval service hashed
//! a fixed insertion-order payload with a hand-rolled escaper; the ingestion
//! service hashed a `serde_json::json!` payload with alphabetically sorted
//! keys. Both forms are accepted for records without a version tag.
//!
//! The chain is an *unkeyed* SHA-256 chain: it detects accidental damage and
//! naive edits, not a writer with file access who recomputes every hash. See
//! `docs/operations/audit-chain.md` for the exact guarantees.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs::{DirBuilder, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::Instant;

use auth::sha256_hex;
use serde_json::{Map, Value};

pub const GENESIS_HASH: &str = "0000000000000000000000000000000000000000000000000000000000000000";
pub const RECORD_VERSION: u64 = 2;
pub const CHAIN_RESTART_ACTION: &str = "chain_restart";

static RECORDS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_FAILURES_TOTAL: AtomicU64 = AtomicU64::new(0);
static DENIALS_DROPPED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Longest request-derived string stored verbatim in an audit record. Longer
/// values are replaced by a bounded prefix plus their length and a SHA-256
/// prefix, so an unauthenticated caller cannot inflate the log.
pub const MAX_AUDIT_FIELD_BYTES: usize = 256;

/// Number of denial records dropped by the throttle in this process.
pub fn denials_dropped_total() -> u64 {
    DENIALS_DROPPED_TOTAL.load(Ordering::Relaxed)
}

/// Number of audit records successfully appended by this process.
pub fn records_total() -> u64 {
    RECORDS_TOTAL.load(Ordering::Relaxed)
}

/// Number of audit appends (or fail-closed preflights) that failed in this process.
pub fn write_failures_total() -> u64 {
    WRITE_FAILURES_TOTAL.load(Ordering::Relaxed)
}

/// Prometheus text for the process-wide audit counters.
pub fn render_prometheus_counters() -> String {
    format!(
        "# TYPE dash_audit_records_total counter\ndash_audit_records_total {}\n\
# TYPE dash_audit_write_failures_total counter\ndash_audit_write_failures_total {}\n\
# TYPE dash_audit_denials_dropped_total counter\ndash_audit_denials_dropped_total {}\n",
        records_total(),
        write_failures_total(),
        denials_dropped_total()
    )
}

// ---------------------------------------------------------------------------
// Options and actor context
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuditOptions {
    /// `fdatasync` after every append (default on).
    pub fsync: bool,
    /// Reject requests when the audit log cannot persist (default off).
    pub fail_closed: bool,
}

impl Default for AuditOptions {
    fn default() -> Self {
        Self {
            fsync: true,
            fail_closed: false,
        }
    }
}

fn env_flag(name: &str, default: bool) -> bool {
    match std::env::var(name) {
        Ok(raw) => match raw.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "on" => true,
            "0" | "false" | "no" | "off" => false,
            _ => default,
        },
        Err(_) => default,
    }
}

impl AuditOptions {
    /// Reads `DASH_<PREFIX>_AUDIT_FSYNC` and `DASH_<PREFIX>_AUDIT_FAIL_CLOSED`
    /// (`prefix` is e.g. `INGEST` or `RETRIEVAL`).
    pub fn from_env(prefix: &str) -> Self {
        Self {
            fsync: env_flag(&format!("DASH_{prefix}_AUDIT_FSYNC"), true),
            fail_closed: env_flag(&format!("DASH_{prefix}_AUDIT_FAIL_CLOSED"), false),
        }
    }
}

/// Startup warning text when an audit log is configured but failures to write
/// it do not stop requests (`DASH_<PREFIX>_AUDIT_FAIL_CLOSED` off).
pub fn fail_open_warning(
    prefix: &str,
    log_configured: bool,
    opts: &AuditOptions,
) -> Option<String> {
    (log_configured && !opts.fail_closed).then(|| {
        format!(
            "DASH_{prefix}_AUDIT_LOG_PATH is set but DASH_{prefix}_AUDIT_FAIL_CLOSED is off: \
             requests keep being served when the audit log cannot be written (failures are \
             only logged and counted). Set DASH_{prefix}_AUDIT_FAIL_CLOSED=1 for production."
        )
    })
}

/// Log [`fail_open_warning`] for the service with env infix `prefix` (`INGEST`,
/// `RETRIEVAL`). Call once at startup.
pub fn warn_if_fail_open(prefix: &str) {
    let configured = [
        format!("DASH_{prefix}_AUDIT_LOG_PATH"),
        format!("EME_{prefix}_AUDIT_LOG_PATH"),
    ]
    .iter()
    .any(|name| std::env::var(name).is_ok_and(|v| !v.trim().is_empty()));
    if let Some(message) = fail_open_warning(prefix, configured, &AuditOptions::from_env(prefix)) {
        tracing::warn!("{message}");
    }
}

/// Who performed the action. `kind` is one of `api_key`, `jwt`, `oidc`, `none`.
/// `id` is a short credential fingerprint (never the credential) or a subject.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    pub kind: String,
    pub id: Option<String>,
}

/// HMAC-SHA256 (RFC 2104) built on SHA-256.
pub(crate) fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    const BLOCK: usize = 64;
    let mut block = [0u8; BLOCK];
    if key.len() > BLOCK {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(block.map(|b| b ^ 0x36));
    inner.update(message);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(block.map(|b| b ^ 0x5c));
    outer.update(inner);
    outer.finalize().into()
}

/// Lower-case hex of `bytes`.
pub(crate) fn hex_lower(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[usize::from(b >> 4)] as char);
        out.push(DIGITS[usize::from(b & 0x0f)] as char);
    }
    out
}

/// Fill an array from the operating system's CSPRNG.
pub(crate) fn random_bytes<const N: usize>() -> [u8; N] {
    use rand::RngCore;
    let mut buf = [0u8; N];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    buf
}

/// Key used for credential fingerprints.
#[derive(Clone)]
pub struct FingerprintKey {
    key: Vec<u8>,
    /// True when the key came from `DASH_AUDIT_FINGERPRINT_KEY` (fingerprints
    /// correlate across restarts and replicas); false for a random per-process key.
    pub configured: bool,
}

impl FingerprintKey {
    /// A configured key (`Some` non-blank value) or a fresh random one.
    pub fn resolve(configured: Option<&str>) -> Self {
        match configured.map(str::trim).filter(|v| !v.is_empty()) {
            Some(value) => Self {
                key: value.as_bytes().to_vec(),
                configured: true,
            },
            None => Self {
                key: random_bytes::<32>().to_vec(),
                configured: false,
            },
        }
    }

    /// 16 hex chars of HMAC-SHA256(key, credential). Keyed MAC over a
    /// high-entropy API key or token to identify it in audit records; this is
    /// not password storage.
    pub fn fingerprint(&self, credential: &str) -> String {
        hex_lower(&hmac_sha256(&self.key, credential.as_bytes())[..8])
    }
}

fn process_fingerprint_key() -> &'static FingerprintKey {
    static KEY: OnceLock<FingerprintKey> = OnceLock::new();
    KEY.get_or_init(|| {
        let key =
            FingerprintKey::resolve(std::env::var("DASH_AUDIT_FINGERPRINT_KEY").ok().as_deref());
        if !key.configured {
            tracing::warn!(
                "DASH_AUDIT_FINGERPRINT_KEY is not set: audit credential fingerprints use a \
                 random per-process key and cannot be correlated across restarts or replicas"
            );
        }
        key
    })
}

/// Keyed credential fingerprint (16 hex chars of HMAC-SHA256 under the
/// deployment's audit key). Never an unsalted hash of the credential.
pub fn key_fingerprint(secret: &str) -> String {
    process_fingerprint_key().fingerprint(secret)
}

/// Per-request audit context, installed by the request entry point.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AuditContext {
    pub actor: Option<Actor>,
    pub request_id: Option<String>,
    pub client_ip: Option<String>,
}

thread_local! {
    static CONTEXT: RefCell<AuditContext> = RefCell::new(AuditContext::default());
    static CONTEXT_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Restores the previous context on drop.
pub struct AuditContextGuard {
    previous: AuditContext,
}

impl Drop for AuditContextGuard {
    fn drop(&mut self) {
        CONTEXT_DEPTH.with(|d| d.set(d.get().saturating_sub(1)));
        let previous = std::mem::take(&mut self.previous);
        CONTEXT.with(|c| *c.borrow_mut() = previous);
    }
}

/// Installs `ctx` for the current thread until the guard is dropped.
pub fn enter_context(ctx: AuditContext) -> AuditContextGuard {
    let previous = CONTEXT.with(|c| std::mem::replace(&mut *c.borrow_mut(), ctx));
    CONTEXT_DEPTH.with(|d| d.set(d.get() + 1));
    AuditContextGuard { previous }
}

/// Replace the actor of the context installed on this thread with the
/// credential that the authorization policy actually evaluated. A no-op when
/// no context is installed, so state never leaks between requests.
pub fn set_actor(kind: &str, credential: &str) {
    if CONTEXT_DEPTH.with(Cell::get) == 0 {
        return;
    }
    let actor = Actor {
        kind: kind.to_string(),
        id: Some(key_fingerprint(credential)),
    };
    CONTEXT.with(|c| c.borrow_mut().actor = Some(actor));
}

/// The context installed on this thread (default when none).
pub fn current_context() -> AuditContext {
    CONTEXT.with(|c| c.borrow().clone())
}

/// Best-effort request context from (lower-cased) request headers, installed
/// before authorization. It mirrors the policy's credential precedence (a
/// JWT-shaped bearer, then `x-api-key`, then any other bearer token); once the
/// policy evaluates the request it replaces the actor with the credential that
/// actually authenticated it (see [`set_actor`]). JWTs are fingerprinted
/// rather than decoded (their `sub` is unverified here).
pub fn context_from_headers(headers: &HashMap<String, String>) -> AuditContext {
    let bearer = headers.get("authorization").and_then(|v| {
        let (scheme, token) = v.split_once(' ')?;
        scheme
            .eq_ignore_ascii_case("bearer")
            .then(|| token.trim())
            .filter(|t| !t.is_empty())
    });
    let jwt_bearer = bearer.filter(|token| {
        let parts: Vec<&str> = token.split('.').collect();
        parts.len() == 3 && parts.iter().all(|p| !p.is_empty())
    });
    let api_key = headers
        .get("x-api-key")
        .map(|k| k.trim())
        .filter(|k| !k.is_empty());
    let actor = if let Some(token) = jwt_bearer {
        Actor {
            kind: "jwt".to_string(),
            id: Some(key_fingerprint(token)),
        }
    } else if let Some(key) = api_key.or(bearer) {
        Actor {
            kind: "api_key".to_string(),
            id: Some(key_fingerprint(key)),
        }
    } else {
        Actor {
            kind: "none".to_string(),
            id: None,
        }
    };
    let request_id = ["x-request-id", "x-correlation-id"]
        .iter()
        .filter_map(|name| headers.get(*name))
        .map(|v| v.trim())
        .find(|v| !v.is_empty() && v.len() <= 128 && v.chars().all(|c| c.is_ascii_graphic()))
        .map(str::to_string);
    AuditContext {
        actor: Some(actor),
        request_id,
        client_ip: None,
    }
}

// ---------------------------------------------------------------------------
// Record model and canonical encoding
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Restart {
    pub prev_tail_seq: u64,
    pub prev_tail_hash: String,
    pub why: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordView {
    pub seq: u64,
    pub ts_unix_ms: u64,
    pub service: String,
    pub action: String,
    pub tenant_id: Option<String>,
    pub claim_id: Option<String>,
    pub status: u64,
    pub outcome: String,
    pub reason: String,
    pub actor: Option<Actor>,
    pub request_id: Option<String>,
    pub client_ip: Option<String>,
    pub restart: Option<Restart>,
    pub prev_hash: String,
}

/// What a service hands to [`append_record`].
#[derive(Debug, Clone, Copy)]
pub struct AuditInput<'a> {
    pub service: &'a str,
    pub action: &'a str,
    pub tenant_id: Option<&'a str>,
    pub claim_id: Option<&'a str>,
    pub status: u16,
    pub outcome: &'a str,
    pub reason: &'a str,
}

fn js(raw: &str) -> String {
    serde_json::to_string(raw).expect("string serialization cannot fail")
}

fn js_opt(raw: Option<&str>) -> String {
    raw.map(js).unwrap_or_else(|| "null".to_string())
}

/// Canonical v2 payload (the bytes that are hashed). See module docs.
pub fn canonical_v2(r: &RecordView) -> String {
    let actor = match &r.actor {
        None => "null".to_string(),
        Some(a) => format!(
            "{{\"kind\":{},\"id\":{}}}",
            js(&a.kind),
            js_opt(a.id.as_deref())
        ),
    };
    let restart = match &r.restart {
        None => "null".to_string(),
        Some(x) => format!(
            "{{\"prev_tail_seq\":{},\"prev_tail_hash\":{},\"why\":{}}}",
            x.prev_tail_seq,
            js(&x.prev_tail_hash),
            js(&x.why)
        ),
    };
    format!(
        "{{\"v\":{RECORD_VERSION},\"seq\":{},\"ts_unix_ms\":{},\"service\":{},\"action\":{},\
\"tenant_id\":{},\"claim_id\":{},\"status\":{},\"outcome\":{},\"reason\":{},\"actor\":{},\
\"request_id\":{},\"client_ip\":{},\"restart\":{},\"prev_hash\":{}}}",
        r.seq,
        r.ts_unix_ms,
        js(&r.service),
        js(&r.action),
        js_opt(r.tenant_id.as_deref()),
        js_opt(r.claim_id.as_deref()),
        r.status,
        js(&r.outcome),
        js(&r.reason),
        actor,
        js_opt(r.request_id.as_deref()),
        js_opt(r.client_ip.as_deref()),
        restart,
        js(&r.prev_hash),
    )
}

/// Legacy retrieval escaper: only `\\ " \n \r \t` were escaped.
fn legacy_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c => out.push(c),
        }
    }
    out
}

fn legacy_opt(raw: Option<&str>) -> String {
    raw.map(|s| format!("\"{}\"", legacy_escape(s)))
        .unwrap_or_else(|| "null".to_string())
}

/// Pre-v2 insertion-order payload (retrieval writer and the old shell verifier).
pub fn canonical_legacy_insertion(r: &RecordView) -> String {
    format!(
        "{{\"seq\":{},\"ts_unix_ms\":{},\"service\":\"{}\",\"action\":\"{}\",\"tenant_id\":{},\
\"claim_id\":{},\"status\":{},\"outcome\":\"{}\",\"reason\":\"{}\",\"prev_hash\":\"{}\"}}",
        r.seq,
        r.ts_unix_ms,
        legacy_escape(&r.service),
        legacy_escape(&r.action),
        legacy_opt(r.tenant_id.as_deref()),
        legacy_opt(r.claim_id.as_deref()),
        r.status,
        legacy_escape(&r.outcome),
        legacy_escape(&r.reason),
        r.prev_hash,
    )
}

/// Pre-v2 ingestion payload: `serde_json::json!` with alphabetically sorted keys.
pub fn canonical_legacy_sorted(r: &RecordView) -> String {
    format!(
        "{{\"action\":{},\"claim_id\":{},\"outcome\":{},\"prev_hash\":{},\"reason\":{},\
\"seq\":{},\"service\":{},\"status\":{},\"tenant_id\":{},\"ts_unix_ms\":{}}}",
        js(&r.action),
        js_opt(r.claim_id.as_deref()),
        js(&r.outcome),
        js(&r.prev_hash),
        js(&r.reason),
        r.seq,
        js(&r.service),
        r.status,
        js_opt(r.tenant_id.as_deref()),
        r.ts_unix_ms,
    )
}

fn render_line(r: &RecordView) -> (String, String) {
    let canonical = canonical_v2(r);
    let hash = sha256_hex(canonical.as_bytes());
    let mut line = canonical;
    line.pop(); // closing brace
    line.push_str(&format!(",\"hash\":\"{hash}\"}}"));
    (line, hash)
}

pub fn is_sha256_hex(raw: &str) -> bool {
    raw.len() == 64 && raw.chars().all(|c| c.is_ascii_hexdigit())
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
struct Tail {
    next_seq: u64,
    last_hash: String,
    /// Bytes to write before the next record (newline for a complete but
    /// unterminated last record).
    prefix: &'static str,
    /// Set when the tail was corrupt and a `chain_restart` must be written.
    restart: Option<Restart>,
}

enum TailLine {
    Chained { seq: u64, hash: String },
    NotChained,
    Invalid,
}

fn classify_line(line: &str) -> TailLine {
    let Ok(Value::Object(obj)) = serde_json::from_str::<Value>(line) else {
        return TailLine::Invalid;
    };
    let seq = obj.get("seq").and_then(Value::as_u64);
    let hash = obj.get("hash").and_then(Value::as_str);
    match (seq, hash) {
        (Some(seq), Some(hash)) if is_sha256_hex(hash) => TailLine::Chained {
            seq,
            hash: hash.to_ascii_lowercase(),
        },
        _ => TailLine::NotChained,
    }
}

fn read_range(file: &mut File, start: u64, end: u64) -> Result<Vec<u8>, String> {
    file.seek(SeekFrom::Start(start))
        .map_err(|e| format!("seeking audit file failed: {e}"))?;
    let mut buf = vec![0u8; (end - start) as usize];
    file.read_exact(&mut buf)
        .map_err(|e| format!("reading audit file failed: {e}"))?;
    Ok(buf)
}

/// Last valid chained record anywhere in the file (slow path, corruption only).
fn last_valid_chained(file: &mut File, len: u64) -> Result<Option<(u64, String)>, String> {
    let data = read_range(file, 0, len)?;
    let text = String::from_utf8_lossy(&data);
    let mut found = None;
    for line in text.lines() {
        if let TailLine::Chained { seq, hash } = classify_line(line) {
            found = Some((seq, hash));
        }
    }
    Ok(found)
}

/// Inspects the file tail under the lock, repairing a torn final line.
fn recover_tail(file: &mut File) -> Result<Tail, String> {
    let genesis = Tail {
        next_seq: 1,
        last_hash: GENESIS_HASH.to_string(),
        prefix: "",
        restart: None,
    };
    let mut len = file
        .metadata()
        .map_err(|e| format!("stat audit file failed: {e}"))?
        .len();
    if len == 0 {
        return Ok(genesis);
    }

    let mut window: u64 = 64 * 1024;
    let mut prefix: &'static str = "";
    let mut torn_checked = false;
    loop {
        let base = len.saturating_sub(window);
        let buf = read_range(file, base, len)?;
        let whole = base == 0;

        // Locate the final (possibly unterminated) segment.
        if !torn_checked {
            let last_nl = buf.iter().rposition(|b| *b == b'\n');
            let (seg_start, seg) = match last_nl {
                Some(i) => (base + i as u64 + 1, &buf[i + 1..]),
                None if whole => (0, &buf[..]),
                None => {
                    window = window.saturating_mul(4);
                    continue;
                }
            };
            if !seg.iter().all(|b| b.is_ascii_whitespace()) {
                let text = String::from_utf8_lossy(seg);
                if matches!(classify_line(text.trim()), TailLine::Chained { .. }) {
                    // Complete record that only lost its newline.
                    prefix = "\n";
                } else {
                    tracing::warn!(
                        truncated_bytes = seg.len(),
                        "audit log ends in an incomplete record; truncating it"
                    );
                    eprintln!(
                        "audit log ends in an incomplete record ({} bytes); truncating it",
                        seg.len()
                    );
                    file.set_len(seg_start)
                        .map_err(|e| format!("truncating torn audit tail failed: {e}"))?;
                    len = seg_start;
                    if len == 0 {
                        return Ok(genesis);
                    }
                    torn_checked = true;
                    window = 64 * 1024;
                    continue;
                }
            }
            torn_checked = true;
        }

        // Last non-blank line fully inside the window.
        let text = String::from_utf8_lossy(&buf);
        let mut candidate: Option<&str> = None;
        for (idx, line) in text
            .split('\n')
            .enumerate()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
        {
            if line.trim().is_empty() {
                continue;
            }
            // The first split element is only a whole line at the file start.
            if idx == 0 && !whole {
                break;
            }
            candidate = Some(line);
            break;
        }
        let Some(line) = candidate else {
            if whole {
                return Ok(genesis);
            }
            window = window.saturating_mul(4);
            continue;
        };
        return match classify_line(line.trim()) {
            TailLine::Chained { seq, hash } => Ok(Tail {
                next_seq: seq.saturating_add(1),
                last_hash: hash,
                prefix,
                restart: None,
            }),
            TailLine::NotChained | TailLine::Invalid => {
                // Corrupt tail: never silently restart at genesis.
                match last_valid_chained(file, len)? {
                    Some((seq, hash)) => Ok(Tail {
                        next_seq: 1,
                        last_hash: GENESIS_HASH.to_string(),
                        prefix,
                        restart: Some(Restart {
                            prev_tail_seq: seq,
                            prev_tail_hash: hash,
                            why: "corrupt_tail".to_string(),
                        }),
                    }),
                    // Nothing chained yet: legacy unchained content only.
                    None => Ok(Tail { prefix, ..genesis }),
                }
            }
        };
    }
}

/// Bound a request-derived string: values longer than
/// [`MAX_AUDIT_FIELD_BYTES`] become a short prefix plus their byte length and
/// a SHA-256 prefix.
pub fn bound_field(raw: &str) -> String {
    if raw.len() <= MAX_AUDIT_FIELD_BYTES {
        return raw.to_string();
    }
    let mut cut = 64;
    while !raw.is_char_boundary(cut) {
        cut -= 1;
    }
    let digest = sha256_hex(raw.as_bytes());
    format!(
        "{}...[len={} sha256={}]",
        &raw[..cut],
        raw.len(),
        &digest[..16]
    )
}

fn bound_opt(raw: Option<&str>) -> Option<String> {
    raw.map(bound_field)
}

/// Token bucket for denial records of one audit file.
struct DenialBucket {
    tokens: f64,
    updated: Instant,
    last_summary: Instant,
    dropped_since_summary: u64,
}

fn denial_limits() -> (f64, f64) {
    let rate = std::env::var("DASH_AUDIT_DENIAL_MAX_PER_SEC")
        .ok()
        .and_then(|v| v.trim().parse::<f64>().ok())
        .unwrap_or(50.0);
    (rate, (rate * 10.0).max(1.0))
}

/// Decide whether a denial record may be written now. Returns false (and
/// counts the drop) once the file's denial budget is exhausted; a single
/// summary line is logged at most every 10 seconds.
fn denial_allowed(path: &str, now: Instant, rate: f64, burst: f64) -> bool {
    if rate <= 0.0 {
        return true;
    }
    static BUCKETS: OnceLock<Mutex<HashMap<String, DenialBucket>>> = OnceLock::new();
    let mut map = BUCKETS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|p| p.into_inner());
    if map.len() > 64 && !map.contains_key(path) {
        map.clear();
    }
    let bucket = map.entry(path.to_string()).or_insert(DenialBucket {
        tokens: burst,
        updated: now,
        last_summary: now,
        dropped_since_summary: 0,
    });
    let elapsed = now.saturating_duration_since(bucket.updated).as_secs_f64();
    bucket.tokens = (bucket.tokens + elapsed * rate).min(burst);
    bucket.updated = now;
    if bucket.tokens >= 1.0 {
        bucket.tokens -= 1.0;
        return true;
    }
    DENIALS_DROPPED_TOTAL.fetch_add(1, Ordering::Relaxed);
    bucket.dropped_since_summary += 1;
    if now.saturating_duration_since(bucket.last_summary).as_secs() >= 10 {
        tracing::warn!(
            dropped = bucket.dropped_since_summary,
            "audit denial records throttled (DASH_AUDIT_DENIAL_MAX_PER_SEC={rate}); \
             see dash_audit_denials_dropped_total"
        );
        bucket.last_summary = now;
        bucket.dropped_since_summary = 0;
    }
    false
}

fn is_denial(input: &AuditInput<'_>) -> bool {
    matches!(input.status, 401 | 403 | 429) || input.outcome == "denied"
}

fn open_locked(path: &str) -> Result<File, String> {
    if let Some(parent) = Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        let mut dirs = DirBuilder::new();
        dirs.recursive(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            dirs.mode(0o700);
        }
        dirs.create(parent)
            .map_err(|e| format!("creating audit directory failed: {e}"))?;
    }
    let mut options = OpenOptions::new();
    options.read(true).append(true).create(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = options
        .open(path)
        .map_err(|e| format!("opening audit file failed: {e}"))?;
    file.lock()
        .map_err(|e| format!("locking audit file failed: {e}"))?;
    Ok(file)
}

/// Appends one chained record. Takes an exclusive advisory file lock around
/// the tail read and the append so concurrent writers (threads or processes)
/// cannot fork the chain. The record is written with a single `write` call.
pub fn append_record(
    path: &str,
    input: &AuditInput<'_>,
    timestamp_ms: u64,
    opts: &AuditOptions,
) -> Result<(), String> {
    if is_denial(input) {
        let (rate, burst) = denial_limits();
        if !denial_allowed(path, Instant::now(), rate, burst) {
            return Ok(());
        }
    }
    let result = append_inner(path, input, timestamp_ms, opts);
    match &result {
        Ok(()) => {
            RECORDS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        Err(_) => {
            WRITE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
    }
    result
}

fn append_inner(
    path: &str,
    input: &AuditInput<'_>,
    timestamp_ms: u64,
    opts: &AuditOptions,
) -> Result<(), String> {
    let mut file = open_locked(path)?;
    let tail = recover_tail(&mut file)?;
    let ctx = current_context();

    let mut buf = String::from(tail.prefix);
    let mut seq = tail.next_seq;
    let mut prev_hash = tail.last_hash.clone();
    if let Some(restart) = tail.restart.clone() {
        let rec = RecordView {
            seq,
            ts_unix_ms: timestamp_ms,
            service: input.service.to_string(),
            action: CHAIN_RESTART_ACTION.to_string(),
            tenant_id: None,
            claim_id: None,
            status: 0,
            outcome: "restart".to_string(),
            reason: restart.why.clone(),
            actor: None,
            request_id: None,
            client_ip: None,
            restart: Some(restart),
            prev_hash: prev_hash.clone(),
        };
        let (line, hash) = render_line(&rec);
        buf.push_str(&line);
        buf.push('\n');
        seq += 1;
        prev_hash = hash;
    }
    let rec = RecordView {
        seq,
        ts_unix_ms: timestamp_ms,
        service: input.service.to_string(),
        action: bound_field(input.action),
        tenant_id: bound_opt(input.tenant_id),
        claim_id: bound_opt(input.claim_id),
        status: u64::from(input.status),
        outcome: bound_field(input.outcome),
        reason: bound_field(input.reason),
        actor: ctx.actor,
        request_id: ctx.request_id,
        client_ip: ctx.client_ip,
        restart: None,
        prev_hash,
    };
    let (line, _hash) = render_line(&rec);
    buf.push_str(&line);
    buf.push('\n');

    // Single write; the file is opened O_APPEND so it lands at the end.
    file.write_all(buf.as_bytes())
        .map_err(|e| format!("appending audit file failed: {e}"))?;
    if opts.fsync {
        file.sync_data()
            .map_err(|e| format!("fsync of audit file failed: {e}"))?;
    }
    Ok(())
}

/// Fail-closed preflight: confirms the log can be opened, locked and its tail
/// recovered *without appending*. Callers run it before acknowledging a
/// mutation. It cannot guarantee that the later append succeeds (disk full
/// between preflight and append), only that the log is currently usable.
pub fn preflight(path: &str) -> Result<(), String> {
    let result = (|| {
        let mut file = open_locked(path)?;
        recover_tail(&mut file).map(|_| ())
    })();
    if result.is_err() {
        WRITE_FAILURES_TOTAL.fetch_add(1, Ordering::Relaxed);
    }
    result
}

// ---------------------------------------------------------------------------
// Verifier
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Default)]
pub struct VerifyOptions {
    /// Only accept records written by this service.
    pub service: Option<String>,
    /// Out-of-band checkpoint note (`last_seq`, `last_hash` from an earlier
    /// verification). When set, the verified tail must equal it, which detects
    /// truncation of the tail.
    pub expect_tail: Option<(u64, String)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyReport {
    pub chained_records: u64,
    pub v2_records: u64,
    pub legacy_records: u64,
    /// Lines without chain fields before the first chained record.
    pub unchained_prefix_lines: u64,
    pub restarts: u64,
    /// Corrupt lines explained by an immediately following `chain_restart`.
    pub quarantined_lines: u64,
    pub last_seq: u64,
    pub last_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyError {
    /// 1-based line number, 0 when not tied to a line.
    pub line: usize,
    pub message: String,
}

impl std::fmt::Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.line > 0 {
            write!(f, "line {}: {}", self.line, self.message)
        } else {
            write!(f, "{}", self.message)
        }
    }
}

impl std::error::Error for VerifyError {}

fn verr<T>(line: usize, message: impl Into<String>) -> Result<T, VerifyError> {
    Err(VerifyError {
        line,
        message: message.into(),
    })
}

const V2_KEYS: &[&str] = &[
    "v",
    "seq",
    "ts_unix_ms",
    "service",
    "action",
    "tenant_id",
    "claim_id",
    "status",
    "outcome",
    "reason",
    "actor",
    "request_id",
    "client_ip",
    "restart",
    "prev_hash",
    "hash",
];
const LEGACY_KEYS: &[&str] = &[
    "seq",
    "ts_unix_ms",
    "service",
    "action",
    "tenant_id",
    "claim_id",
    "status",
    "outcome",
    "reason",
    "prev_hash",
    "hash",
];

fn req_u64(o: &Map<String, Value>, k: &str, line: usize) -> Result<u64, VerifyError> {
    match o.get(k).and_then(Value::as_u64) {
        Some(n) => Ok(n),
        None => verr(line, format!("'{k}' must be a non-negative integer")),
    }
}

fn req_str(o: &Map<String, Value>, k: &str, line: usize) -> Result<String, VerifyError> {
    match o.get(k) {
        Some(Value::String(s)) => Ok(s.clone()),
        _ => verr(line, format!("'{k}' must be a string")),
    }
}

fn opt_str(o: &Map<String, Value>, k: &str, line: usize) -> Result<Option<String>, VerifyError> {
    match o.get(k) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        _ => verr(line, format!("'{k}' must be a string or null")),
    }
}

fn parse_view(
    o: &Map<String, Value>,
    v2: bool,
    line: usize,
) -> Result<(RecordView, String), VerifyError> {
    let allowed = if v2 { V2_KEYS } else { LEGACY_KEYS };
    if let Some(k) = o.keys().find(|k| !allowed.contains(&k.as_str())) {
        return verr(
            line,
            format!("unknown field '{k}' (not covered by the hash)"),
        );
    }
    let prev_hash = req_str(o, "prev_hash", line)?;
    let hash = req_str(o, "hash", line)?;
    if !is_sha256_hex(&prev_hash) {
        return verr(line, "prev_hash must be 64-char hex");
    }
    if !is_sha256_hex(&hash) {
        return verr(line, "hash must be 64-char hex");
    }
    let (actor, request_id, client_ip, restart) = if v2 {
        let actor = match o.get("actor") {
            None | Some(Value::Null) => None,
            Some(Value::Object(a)) => {
                if let Some(k) = a.keys().find(|k| *k != "kind" && *k != "id") {
                    return verr(line, format!("unknown actor field '{k}'"));
                }
                Some(Actor {
                    kind: req_str(a, "kind", line)?,
                    id: opt_str(a, "id", line)?,
                })
            }
            _ => return verr(line, "'actor' must be an object or null"),
        };
        let restart = match o.get("restart") {
            None | Some(Value::Null) => None,
            Some(Value::Object(x)) => {
                if let Some(k) = x
                    .keys()
                    .find(|k| !["prev_tail_seq", "prev_tail_hash", "why"].contains(&k.as_str()))
                {
                    return verr(line, format!("unknown restart field '{k}'"));
                }
                Some(Restart {
                    prev_tail_seq: req_u64(x, "prev_tail_seq", line)?,
                    prev_tail_hash: req_str(x, "prev_tail_hash", line)?,
                    why: req_str(x, "why", line)?,
                })
            }
            _ => return verr(line, "'restart' must be an object or null"),
        };
        (
            actor,
            opt_str(o, "request_id", line)?,
            opt_str(o, "client_ip", line)?,
            restart,
        )
    } else {
        (None, None, None, None)
    };
    Ok((
        RecordView {
            seq: req_u64(o, "seq", line)?,
            ts_unix_ms: req_u64(o, "ts_unix_ms", line)?,
            service: req_str(o, "service", line)?,
            action: req_str(o, "action", line)?,
            tenant_id: opt_str(o, "tenant_id", line)?,
            claim_id: opt_str(o, "claim_id", line)?,
            status: req_u64(o, "status", line)?,
            outcome: req_str(o, "outcome", line)?,
            reason: req_str(o, "reason", line)?,
            actor,
            request_id,
            client_ip,
            restart,
            prev_hash,
        },
        hash,
    ))
}

/// Verifies a whole audit log file.
pub fn verify_file(path: &str, opts: &VerifyOptions) -> Result<VerifyReport, VerifyError> {
    let data = std::fs::read(path).map_err(|e| VerifyError {
        line: 0,
        message: format!("cannot read audit file {path}: {e}"),
    })?;
    verify_bytes(&data, opts)
}

/// Verifies audit log content.
pub fn verify_bytes(data: &[u8], opts: &VerifyOptions) -> Result<VerifyReport, VerifyError> {
    let text = match std::str::from_utf8(data) {
        Ok(t) => t,
        Err(_) => return verr(0, "audit file is not valid UTF-8"),
    };
    let ends_with_newline = text.is_empty() || text.ends_with('\n');
    let mut report = VerifyReport {
        chained_records: 0,
        v2_records: 0,
        legacy_records: 0,
        unchained_prefix_lines: 0,
        restarts: 0,
        quarantined_lines: 0,
        last_seq: 0,
        last_hash: String::new(),
    };
    let mut last: Option<(u64, String)> = None;
    let mut pending_corrupt: Vec<usize> = Vec::new();
    let total_lines = text.split('\n').count();

    for (idx, raw) in text.split('\n').enumerate() {
        let lineno = idx + 1;
        let line = raw.trim();
        if line.is_empty() {
            continue;
        }
        let is_last_segment = idx + 1 == total_lines;
        if is_last_segment && !ends_with_newline {
            return verr(
                lineno,
                "torn tail: last line has no trailing newline (incomplete write)",
            );
        }
        let obj = match serde_json::from_str::<Value>(line) {
            Ok(Value::Object(o)) => o,
            Ok(_) => {
                if last.is_some() {
                    pending_corrupt.push(lineno);
                    continue;
                }
                return verr(lineno, "record must be a JSON object");
            }
            Err(e) => {
                if last.is_some() {
                    pending_corrupt.push(lineno);
                    continue;
                }
                return verr(lineno, format!("invalid JSON ({e})"));
            }
        };
        let has_chain = ["seq", "prev_hash", "hash"]
            .iter()
            .filter(|k| obj.contains_key(**k))
            .count();
        if has_chain == 0 {
            if last.is_some() {
                pending_corrupt.push(lineno);
            } else {
                report.unchained_prefix_lines += 1;
            }
            continue;
        }
        if has_chain != 3 {
            return verr(lineno, "seq/prev_hash/hash must be present together");
        }
        let v2 = match obj.get("v") {
            None => false,
            Some(Value::Number(n)) if n.as_u64() == Some(RECORD_VERSION) => true,
            Some(_) => return verr(lineno, "unsupported record version"),
        };
        let (rec, hash) = parse_view(&obj, v2, lineno)?;
        if let Some(svc) = &opts.service
            && &rec.service != svc
        {
            return verr(
                lineno,
                format!("service filter '{svc}' mismatch (found '{}')", rec.service),
            );
        }
        let hash_lc = hash.to_ascii_lowercase();
        let hash_ok = if v2 {
            sha256_hex(canonical_v2(&rec).as_bytes()) == hash_lc
        } else {
            sha256_hex(canonical_legacy_insertion(&rec).as_bytes()) == hash_lc
                || sha256_hex(canonical_legacy_sorted(&rec).as_bytes()) == hash_lc
        };
        if !hash_ok {
            return verr(lineno, "hash mismatch");
        }

        let is_restart = v2
            && rec.action == CHAIN_RESTART_ACTION
            && rec.restart.is_some()
            && rec.seq == 1
            && rec.prev_hash == GENESIS_HASH;
        match &last {
            None => {
                if rec.seq < 1 {
                    return verr(lineno, "first chained seq must be >= 1");
                }
                if is_restart {
                    return verr(lineno, "chain_restart without a previous chain");
                }
            }
            Some((lseq, lhash)) => {
                if is_restart {
                    let r = rec.restart.as_ref().expect("checked");
                    if r.prev_tail_hash.to_ascii_lowercase() != *lhash || r.prev_tail_seq != *lseq {
                        return verr(
                            lineno,
                            "chain_restart does not reference the previous tail (seq/hash)",
                        );
                    }
                    report.restarts += 1;
                    report.quarantined_lines += pending_corrupt.len() as u64;
                    pending_corrupt.clear();
                } else {
                    if let Some(first) = pending_corrupt.first() {
                        return verr(
                            *first,
                            "unchained or corrupt line inside the chain not explained by a chain_restart",
                        );
                    }
                    if rec.seq == 1 && rec.prev_hash == GENESIS_HASH {
                        return verr(
                            lineno,
                            "unexplained chain restart at genesis (no chain_restart event)",
                        );
                    }
                    if rec.seq != lseq + 1 {
                        return verr(
                            lineno,
                            format!("seq expected {}, found {}", lseq + 1, rec.seq),
                        );
                    }
                    if rec.prev_hash.to_ascii_lowercase() != *lhash {
                        return verr(lineno, "prev_hash does not match previous hash");
                    }
                }
            }
        }
        report.chained_records += 1;
        if v2 {
            report.v2_records += 1;
        } else {
            report.legacy_records += 1;
        }
        last = Some((rec.seq, hash_lc));
    }

    if let Some(first) = pending_corrupt.first() {
        return verr(
            *first,
            "unchained or corrupt line after the last chained record (corrupt tail)",
        );
    }
    let Some((seq, hash)) = last else {
        return verr(0, "no chained audit records found");
    };
    if let Some((eseq, ehash)) = &opts.expect_tail
        && (*eseq != seq || ehash.to_ascii_lowercase() != hash)
    {
        return verr(
            0,
            format!(
                "tail does not match checkpoint: expected seq={eseq} hash={ehash}, found seq={seq} hash={hash} (truncated or rewritten tail)"
            ),
        );
    }
    report.last_seq = seq;
    report.last_hash = hash;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input<'a>(tenant: &'a str, status: u16, outcome: &'a str) -> AuditInput<'a> {
        AuditInput {
            service: "ingestion",
            action: "ingest",
            tenant_id: Some(tenant),
            claim_id: Some(tenant),
            status,
            outcome,
            reason: "no credentials",
        }
    }

    fn log_path(dir: &tempfile::TempDir) -> String {
        dir.path().join("audit.jsonl").to_string_lossy().to_string()
    }

    #[test]
    fn hmac_matches_rfc4231_case_1() {
        let mac = hmac_sha256(&[0x0b; 20], b"Hi There");
        assert_eq!(
            hex_lower(&mac),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
        // A key longer than the block size is hashed first (RFC 4231 case 6).
        let mac = hmac_sha256(
            &[0xaa; 131],
            b"Test Using Larger Than Block-Size Key - Hash Key First",
        );
        assert_eq!(
            hex_lower(&mac),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn fingerprints_are_keyed_16_hex_and_not_plain_sha256() {
        let a = FingerprintKey::resolve(Some("deployment-key-a"));
        let b = FingerprintKey::resolve(Some("deployment-key-b"));
        assert!(a.configured);
        let fp = a.fingerprint("api-key-123");
        assert_eq!(fp.len(), 16);
        assert!(fp.chars().all(|c| c.is_ascii_hexdigit()));
        assert_eq!(fp, a.fingerprint("api-key-123"));
        assert_ne!(fp, b.fingerprint("api-key-123"));
        let plain = sha256_hex(b"api-key-123");
        assert!(!plain.starts_with(&fp) && !plain.starts_with(&fp[..8]));
        // Without a configured key each resolution is random and flagged.
        let r1 = FingerprintKey::resolve(None);
        let r2 = FingerprintKey::resolve(Some("  "));
        assert!(!r1.configured && !r2.configured);
        assert_ne!(r1.fingerprint("x"), r2.fingerprint("x"));
        // The process-wide helper is keyed too.
        assert_eq!(key_fingerprint("x").len(), 16);
        assert_ne!(key_fingerprint("x"), sha256_hex(b"x")[..16].to_string());
    }

    #[test]
    fn oversized_request_fields_are_bounded_in_the_record() {
        let dir = tempfile::tempdir().expect("dir");
        let path = log_path(&dir);
        let huge = "T".repeat(1024 * 1024);
        append_record(
            &path,
            &input(&huge, 401, "denied"),
            1,
            &AuditOptions::default(),
        )
        .expect("append");
        let text = std::fs::read_to_string(&path).expect("read");
        assert!(text.len() < 2048, "record is {} bytes", text.len());
        assert!(text.contains("len=1048576"), "{text}");
        assert!(text.contains("sha256="), "{text}");
        let bounded = bound_field(&huge);
        assert!(bounded.len() < MAX_AUDIT_FIELD_BYTES);
        assert_eq!(bound_field("short"), "short");
        // Exactly at the limit is stored verbatim.
        let edge = "e".repeat(MAX_AUDIT_FIELD_BYTES);
        assert_eq!(bound_field(&edge), edge);
        // Multi-byte input is cut on a char boundary.
        let wide = "é".repeat(500);
        assert!(bound_field(&wide).contains("len=1000"));
        // The bounded log still verifies.
        verify_file(&path, &VerifyOptions::default()).expect("verifies");
    }

    #[test]
    fn denial_records_are_throttled_and_counted() {
        let dir = tempfile::tempdir().expect("dir");
        let path = log_path(&dir);
        let before = denials_dropped_total();
        let opts = AuditOptions {
            fsync: false,
            fail_closed: false,
        };
        for i in 0..700u64 {
            append_record(&path, &input("t", 401, "denied"), i, &opts).expect("append");
        }
        let lines = std::fs::read_to_string(&path)
            .expect("read")
            .lines()
            .count();
        assert!(lines <= 520, "{lines} denial records written");
        assert!(lines >= 400, "{lines}");
        assert!(denials_dropped_total() - before >= 150);
        assert!(render_prometheus_counters().contains("dash_audit_denials_dropped_total"));
        // Non-denial records are never throttled.
        let path2 = dir.path().join("ok.jsonl").to_string_lossy().to_string();
        for i in 0..700u64 {
            append_record(&path2, &input("t", 200, "success"), i, &opts).expect("append");
        }
        assert_eq!(
            std::fs::read_to_string(&path2)
                .expect("read")
                .lines()
                .count(),
            700
        );
    }

    #[test]
    fn denial_bucket_refills_over_time() {
        let t0 = Instant::now();
        let path = "throttle-refill-test";
        for _ in 0..10 {
            assert!(denial_allowed(path, t0, 1.0, 10.0));
        }
        assert!(!denial_allowed(path, t0, 1.0, 10.0));
        assert!(denial_allowed(
            path,
            t0 + std::time::Duration::from_secs(3),
            1.0,
            10.0
        ));
    }

    #[cfg(unix)]
    #[test]
    fn audit_file_is_private_and_created_directory_is_0700() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("dir");
        let nested = dir.path().join("new-audit-dir");
        let path = nested.join("audit.jsonl").to_string_lossy().to_string();
        append_record(
            &path,
            &input("t", 200, "success"),
            1,
            &AuditOptions::default(),
        )
        .expect("append");
        let file_mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
        let dir_mode = std::fs::metadata(&nested)
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(file_mode, 0o600);
        assert_eq!(dir_mode, 0o700);
    }

    #[test]
    fn fail_open_audit_configuration_warns() {
        let off = AuditOptions {
            fsync: true,
            fail_closed: false,
        };
        let on = AuditOptions {
            fail_closed: true,
            ..off
        };
        let warning = fail_open_warning("INGEST", true, &off).expect("warning");
        assert!(warning.contains("DASH_INGEST_AUDIT_FAIL_CLOSED=1"));
        assert!(fail_open_warning("INGEST", true, &on).is_none());
        assert!(fail_open_warning("INGEST", false, &off).is_none());
    }

    #[test]
    fn set_actor_only_applies_inside_a_context() {
        set_actor("api_key", "ignored");
        assert_eq!(current_context().actor, None);
        {
            let _guard = enter_context(AuditContext::default());
            set_actor("jwt", "tok");
            let actor = current_context().actor.expect("actor");
            assert_eq!(actor.kind, "jwt");
            assert_eq!(actor.id, Some(key_fingerprint("tok")));
        }
        assert_eq!(current_context().actor, None);
    }

    #[test]
    fn context_follows_policy_credential_precedence() {
        let mut h = HashMap::new();
        h.insert("x-api-key".to_string(), "the-api-key".to_string());
        h.insert("authorization".to_string(), "Bearer other-key".to_string());
        let actor = context_from_headers(&h).actor.expect("actor");
        assert_eq!(actor.kind, "api_key");
        assert_eq!(actor.id, Some(key_fingerprint("the-api-key")));
        h.insert("authorization".to_string(), "Bearer a.b.c".to_string());
        let actor = context_from_headers(&h).actor.expect("actor");
        assert_eq!(actor.kind, "jwt");
        assert_eq!(actor.id, Some(key_fingerprint("a.b.c")));
    }
}
