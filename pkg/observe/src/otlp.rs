//! OTLP/HTTP (protobuf) span exporter with a batch processor on its own
//! thread.
//!
//! The DASH services serve HTTP from a pool of std threads, not from an async
//! runtime, so the exporter does not need one either: finished spans are
//! handed to a bounded channel (never blocking the request thread; a full
//! queue drops the span and counts it), and one `dash-otlp-export` thread
//! batches them and POSTs `ExportTraceServiceRequest` messages with a
//! blocking HTTP client (`ureq`, rustls). The protobuf encoding is the small,
//! stable subset of `opentelemetry/proto/trace/v1` that spans need, written
//! by hand so the build does not pull in a protobuf code generator or a gRPC
//! stack.
//!
//! Configuration follows the OpenTelemetry environment variables (see
//! [`TracingConfig::from_lookup`] and `docs/operations/tracing.md`).

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::exposition::{MetricKind, MetricsWriter};
use crate::trace::{
    self, AttrValue, Sampler, SpanData, SpanKind, SpanSink, SpanStatus, TracerConfig,
};

/// Path appended to `OTEL_EXPORTER_OTLP_ENDPOINT` for traces.
pub const TRACES_PATH: &str = "/v1/traces";
/// Endpoint used when tracing is enabled without one.
pub const DEFAULT_TRACES_ENDPOINT: &str = "http://localhost:4318/v1/traces";
/// The only OTLP protocol this exporter speaks.
pub const PROTOCOL_HTTP_PROTOBUF: &str = "http/protobuf";

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_SCHEDULE_DELAY: Duration = Duration::from_secs(5);
const DEFAULT_MAX_QUEUE: usize = 2048;
const DEFAULT_MAX_BATCH: usize = 512;
const SHUTDOWN_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Exporter and tracer settings resolved from the environment.
#[derive(Clone, Debug, PartialEq)]
pub struct TracingConfig {
    /// Full URL spans are POSTed to.
    pub endpoint: String,
    /// Extra request headers (`OTEL_EXPORTER_OTLP_HEADERS`), for example the
    /// collector's credentials. Never logged.
    pub extra_headers: Vec<(String, String)>,
    pub timeout: Duration,
    /// PEM bundle of CA certificates trusted for an `https://` endpoint, in
    /// addition to the public web roots.
    pub ca_file: Option<PathBuf>,
    /// `service.name` and the other resource attributes.
    pub resource: Vec<(String, String)>,
    pub tracer: TracerConfig,
    pub max_queue: usize,
    pub max_batch: usize,
    pub schedule_delay: Duration,
}

/// Outcome of reading the configuration.
#[derive(Clone, Debug, PartialEq)]
pub struct TracingSettings {
    /// `Some` when tracing is enabled.
    pub config: Option<TracingConfig>,
    /// Why tracing is off (for the startup log).
    pub disabled_reason: Option<String>,
    /// Problems that did not prevent a decision (a bad sampler falls back
    /// to the default, an unparsable header entry is skipped, ...).
    pub warnings: Vec<String>,
}

fn parse_bool(raw: &str) -> Option<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

/// Minimal `%XX` decoding for header and resource values (the W3C baggage
/// octet encoding the OpenTelemetry specification uses).
fn percent_decode(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = raw.get(i + 1..i + 3)?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// Parse a `k=v,k2=v2` list (`OTEL_EXPORTER_OTLP_HEADERS`,
/// `OTEL_RESOURCE_ATTRIBUTES`). Malformed entries are skipped and reported
/// by position only, so a credential never reaches a log line.
fn parse_pairs(
    raw: &str,
    variable: &str,
    header_names: bool,
    warnings: &mut Vec<String>,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (index, entry) in raw.split(',').enumerate() {
        if entry.trim().is_empty() {
            continue;
        }
        let parsed = entry.split_once('=').and_then(|(name, value)| {
            let name = name.trim();
            let value = percent_decode(value.trim())?;
            let name_ok = !name.is_empty()
                && (!header_names
                    || name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)));
            let value_ok = !header_names || !value.bytes().any(|b| b == b'\r' || b == b'\n');
            (name_ok && value_ok).then(|| (name.to_string(), value))
        });
        match parsed {
            Some(pair) => out.push(pair),
            None => warnings.push(format!(
                "{variable}: entry {} is not a valid name=value pair and is ignored",
                index + 1
            )),
        }
    }
    out
}

fn is_loopback_host(endpoint: &str) -> bool {
    let rest = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = match authority.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => authority.rsplit_once(':').map_or(authority, |(h, _)| h),
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

impl TracingConfig {
    /// Read the settings through `lookup` (the process environment in
    /// production).
    ///
    /// Tracing is **on** when `DASH_OTEL_ENABLED` is true, **off** when it is
    /// false, and otherwise on exactly when `OTEL_EXPORTER_OTLP_ENDPOINT` or
    /// `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` is set. `OTEL_SDK_DISABLED=true`
    /// and `OTEL_TRACES_EXPORTER=none` turn it off in every case. An
    /// unsupported protocol, exporter or endpoint keeps tracing off (the
    /// service still starts; observability never blocks the data path).
    pub fn from_lookup(
        lookup: &dyn Fn(&str) -> Option<String>,
        default_service_name: &str,
    ) -> TracingSettings {
        let get = |name: &str| {
            lookup(name)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };
        let mut warnings = Vec::new();
        let off = |reason: String, warnings: Vec<String>| TracingSettings {
            config: None,
            disabled_reason: Some(reason),
            warnings,
        };

        if get("OTEL_SDK_DISABLED").as_deref().and_then(parse_bool) == Some(true) {
            return off("OTEL_SDK_DISABLED is true".to_string(), warnings);
        }
        let traces_endpoint = get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT");
        let base_endpoint = get("OTEL_EXPORTER_OTLP_ENDPOINT");
        let enabled = match get("DASH_OTEL_ENABLED") {
            Some(raw) => match parse_bool(&raw) {
                Some(value) => value,
                None => {
                    return off(
                        format!("DASH_OTEL_ENABLED has an unrecognized value '{raw}'"),
                        warnings,
                    );
                }
            },
            None => traces_endpoint.is_some() || base_endpoint.is_some(),
        };
        if !enabled {
            return off(
                "set DASH_OTEL_ENABLED=1 or OTEL_EXPORTER_OTLP_ENDPOINT to export traces"
                    .to_string(),
                warnings,
            );
        }
        match get("OTEL_TRACES_EXPORTER").map(|v| v.to_ascii_lowercase()) {
            None => {}
            Some(v) if v == "otlp" => {}
            Some(v) if v == "none" => {
                return off("OTEL_TRACES_EXPORTER is none".to_string(), warnings);
            }
            Some(other) => {
                return off(
                    format!("OTEL_TRACES_EXPORTER '{other}' is not supported (only otlp)"),
                    warnings,
                );
            }
        }
        let protocol = get("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")
            .or_else(|| get("OTEL_EXPORTER_OTLP_PROTOCOL"))
            .unwrap_or_else(|| PROTOCOL_HTTP_PROTOBUF.to_string());
        if !protocol.eq_ignore_ascii_case(PROTOCOL_HTTP_PROTOBUF) {
            return off(
                format!(
                    "OTLP protocol '{protocol}' is not supported (only {PROTOCOL_HTTP_PROTOBUF}); \
                     point the service at a collector's OTLP/HTTP port (4318)"
                ),
                warnings,
            );
        }
        let endpoint = match (traces_endpoint, base_endpoint) {
            (Some(full), _) => full,
            (None, Some(base)) => format!("{}{TRACES_PATH}", base.trim_end_matches('/')),
            (None, None) => DEFAULT_TRACES_ENDPOINT.to_string(),
        };
        let scheme_ok = endpoint.starts_with("http://") || endpoint.starts_with("https://");
        if !scheme_ok || endpoint.contains(char::is_whitespace) {
            return off(
                "the OTLP traces endpoint must be an http:// or https:// URL".to_string(),
                warnings,
            );
        }

        let extra_headers = get("OTEL_EXPORTER_OTLP_TRACES_HEADERS")
            .map(|raw| (raw, "OTEL_EXPORTER_OTLP_TRACES_HEADERS"))
            .or_else(|| {
                get("OTEL_EXPORTER_OTLP_HEADERS").map(|raw| (raw, "OTEL_EXPORTER_OTLP_HEADERS"))
            })
            .map(|(raw, name)| parse_pairs(&raw, name, true, &mut warnings))
            .unwrap_or_default();
        if !extra_headers.is_empty()
            && endpoint.starts_with("http://")
            && !is_loopback_host(&endpoint)
        {
            warnings.push(
                "OTLP headers are sent over plain http:// to a non-loopback collector; \
                 use an https:// endpoint or a local collector"
                    .to_string(),
            );
        }

        let millis = |names: &[&str], default: Duration, warnings: &mut Vec<String>| {
            for name in names {
                if let Some(raw) = get(name) {
                    return match raw.parse::<u64>() {
                        Ok(ms) if ms > 0 => Duration::from_millis(ms),
                        _ => {
                            warnings.push(format!(
                                "{name} must be a positive number of milliseconds; using the default"
                            ));
                            default
                        }
                    };
                }
            }
            default
        };
        let count = |name: &str, default: usize, warnings: &mut Vec<String>| match get(name) {
            None => default,
            Some(raw) => match raw.parse::<usize>() {
                Ok(n) if n > 0 => n,
                _ => {
                    warnings.push(format!(
                        "{name} must be a positive integer; using the default"
                    ));
                    default
                }
            },
        };
        let timeout = millis(
            &[
                "OTEL_EXPORTER_OTLP_TRACES_TIMEOUT",
                "OTEL_EXPORTER_OTLP_TIMEOUT",
            ],
            DEFAULT_TIMEOUT,
            &mut warnings,
        );
        let schedule_delay = millis(
            &["OTEL_BSP_SCHEDULE_DELAY"],
            DEFAULT_SCHEDULE_DELAY,
            &mut warnings,
        );
        let max_queue = count("OTEL_BSP_MAX_QUEUE_SIZE", DEFAULT_MAX_QUEUE, &mut warnings);
        let mut max_batch = count(
            "OTEL_BSP_MAX_EXPORT_BATCH_SIZE",
            DEFAULT_MAX_BATCH,
            &mut warnings,
        );
        if max_batch > max_queue {
            max_batch = max_queue;
        }
        let ca_file = get("OTEL_EXPORTER_OTLP_TRACES_CERTIFICATE")
            .or_else(|| get("OTEL_EXPORTER_OTLP_CERTIFICATE"))
            .map(PathBuf::from);

        let sampler = match Sampler::from_config(
            get("OTEL_TRACES_SAMPLER").as_deref(),
            get("OTEL_TRACES_SAMPLER_ARG").as_deref(),
        ) {
            Ok(sampler) => sampler,
            Err(reason) => {
                warnings.push(format!("{reason}; using parentbased_always_on"));
                Sampler::default()
            }
        };
        let background_ratio = match get("DASH_OTEL_BACKGROUND_SAMPLE_RATIO") {
            None => 1.0,
            Some(raw) => match raw.parse::<f64>() {
                Ok(r) if (0.0..=1.0).contains(&r) => r,
                _ => {
                    warnings.push(
                        "DASH_OTEL_BACKGROUND_SAMPLE_RATIO must be a number between 0 and 1; using 1"
                            .to_string(),
                    );
                    1.0
                }
            },
        };

        let mut resource: Vec<(String, String)> = get("OTEL_RESOURCE_ATTRIBUTES")
            .map(|raw| parse_pairs(&raw, "OTEL_RESOURCE_ATTRIBUTES", false, &mut warnings))
            .unwrap_or_default();
        let service_name = get("OTEL_SERVICE_NAME")
            .or_else(|| {
                resource
                    .iter()
                    .find(|(k, _)| k == "service.name")
                    .map(|(_, v)| v.clone())
            })
            .unwrap_or_else(|| default_service_name.to_string());
        resource.retain(|(k, _)| k != "service.name");
        resource.insert(0, ("service.name".to_string(), service_name));

        TracingSettings {
            config: Some(TracingConfig {
                endpoint,
                extra_headers,
                timeout,
                ca_file,
                resource,
                tracer: TracerConfig {
                    sampler,
                    background_ratio,
                },
                max_queue,
                max_batch,
                schedule_delay,
            }),
            disabled_reason: None,
            warnings,
        }
    }

    /// [`Self::from_lookup`] over the process environment.
    pub fn from_env(default_service_name: &str) -> TracingSettings {
        Self::from_lookup(&|name| std::env::var(name).ok(), default_service_name)
    }
}

// ---------------------------------------------------------------------------
// Protobuf encoding (opentelemetry/proto/trace/v1, collector/trace/v1)
// ---------------------------------------------------------------------------

mod pb {
    pub const VARINT: u8 = 0;
    pub const FIXED64: u8 = 1;
    pub const LEN: u8 = 2;
    pub const FIXED32: u8 = 5;

    pub fn varint(buf: &mut Vec<u8>, mut v: u64) {
        while v >= 0x80 {
            buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        buf.push(v as u8);
    }

    pub fn tag(buf: &mut Vec<u8>, field: u32, wire: u8) {
        varint(buf, (u64::from(field) << 3) | u64::from(wire));
    }

    pub fn bytes(buf: &mut Vec<u8>, field: u32, data: &[u8]) {
        tag(buf, field, LEN);
        varint(buf, data.len() as u64);
        buf.extend_from_slice(data);
    }

    pub fn string(buf: &mut Vec<u8>, field: u32, s: &str) {
        if !s.is_empty() {
            bytes(buf, field, s.as_bytes());
        }
    }

    pub fn uint(buf: &mut Vec<u8>, field: u32, v: u64) {
        if v != 0 {
            tag(buf, field, VARINT);
            varint(buf, v);
        }
    }

    pub fn fixed64(buf: &mut Vec<u8>, field: u32, v: u64) {
        tag(buf, field, FIXED64);
        buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn fixed32(buf: &mut Vec<u8>, field: u32, v: u32) {
        tag(buf, field, FIXED32);
        buf.extend_from_slice(&v.to_le_bytes());
    }

    pub fn message(buf: &mut Vec<u8>, field: u32, fill: impl FnOnce(&mut Vec<u8>)) {
        let mut inner = Vec::new();
        fill(&mut inner);
        bytes(buf, field, &inner);
    }
}

/// `KeyValue { key = 1; AnyValue value = 2 }`,
/// `AnyValue { string_value = 1; bool_value = 2; int_value = 3 }`.
fn encode_key_value(buf: &mut Vec<u8>, field: u32, name: &str, value: &AttrValue) {
    pb::message(buf, field, |kv| {
        pb::string(kv, 1, name);
        pb::message(kv, 2, |any| match value {
            AttrValue::Str(s) => pb::bytes(any, 1, s.as_bytes()),
            AttrValue::Bool(b) => {
                pb::tag(any, 2, pb::VARINT);
                pb::varint(any, u64::from(*b));
            }
            AttrValue::Int(i) => {
                pb::tag(any, 3, pb::VARINT);
                pb::varint(any, *i as u64);
            }
        });
    });
}

fn span_kind_code(kind: SpanKind) -> u64 {
    match kind {
        SpanKind::Internal => 1,
        SpanKind::Server => 2,
        SpanKind::Client => 3,
    }
}

/// Span flags: W3C trace flags in bits 0-7, `HAS_IS_REMOTE` (bit 8) and
/// `IS_REMOTE` (bit 9) describing the parent.
fn span_flags(sampled: bool, parent_is_remote: bool) -> u32 {
    let mut flags = u32::from(sampled) | 0x100;
    if parent_is_remote {
        flags |= 0x200;
    }
    flags
}

fn encode_span(buf: &mut Vec<u8>, span: &SpanData) {
    pb::message(buf, 2, |s| {
        pb::bytes(s, 1, &span.trace_id.0);
        pb::bytes(s, 2, &span.span_id.0);
        if let Some(state) = span.trace_state.as_deref() {
            pb::string(s, 3, state);
        }
        if let Some(parent) = span.parent_span_id {
            pb::bytes(s, 4, &parent.0);
        }
        pb::string(s, 5, &span.name);
        pb::uint(s, 6, span_kind_code(span.kind));
        pb::fixed64(s, 7, span.start_unix_nanos);
        pb::fixed64(s, 8, span.end_unix_nanos);
        for (name, value) in &span.attributes {
            encode_key_value(s, 9, name, value);
        }
        pb::uint(s, 10, u64::from(span.dropped_attributes));
        for link in &span.links {
            // Link { trace_id = 1; span_id = 2; trace_state = 3; flags = 6 }
            pb::message(s, 13, |l| {
                pb::bytes(l, 1, &link.trace_id.0);
                pb::bytes(l, 2, &link.span_id.0);
                if let Some(state) = link.trace_state.as_deref() {
                    pb::string(l, 3, state);
                }
                pb::fixed32(l, 6, span_flags(link.sampled, false));
            });
        }
        if let SpanStatus::Error(description) = &span.status {
            // Status { message = 2; code = 3 (ERROR = 2) }
            pb::message(s, 15, |st| {
                pb::string(st, 2, description);
                pb::uint(st, 3, 2);
            });
        }
        pb::fixed32(s, 16, span_flags(span.sampled, span.parent_is_remote));
    });
}

/// Encode one `ExportTraceServiceRequest` holding `spans` under `resource`.
pub fn encode_export_request(resource: &[(String, String)], spans: &[SpanData]) -> Vec<u8> {
    let mut out = Vec::with_capacity(256 + spans.len() * 160);
    // ResourceSpans resource_spans = 1
    pb::message(&mut out, 1, |rs| {
        // Resource resource = 1 { repeated KeyValue attributes = 1 }
        pb::message(rs, 1, |r| {
            for (name, value) in resource {
                encode_key_value(r, 1, name, &AttrValue::Str(value.clone().into()));
            }
            for (name, value) in [
                ("telemetry.sdk.name", "dash-observe"),
                ("telemetry.sdk.language", "rust"),
                ("telemetry.sdk.version", env!("CARGO_PKG_VERSION")),
            ] {
                encode_key_value(r, 1, name, &AttrValue::from(value));
            }
        });
        // ScopeSpans scope_spans = 2 { InstrumentationScope scope = 1; spans = 2 }
        pb::message(rs, 2, |ss| {
            pb::message(ss, 1, |scope| {
                pb::string(scope, 1, "dash");
                pb::string(scope, 2, env!("CARGO_PKG_VERSION"));
            });
            for span in spans {
                encode_span(ss, span);
            }
        });
    });
    out
}

// ---------------------------------------------------------------------------
// Batch exporter
// ---------------------------------------------------------------------------

static EXPORTED: AtomicU64 = AtomicU64::new(0);
static DROPPED_QUEUE_FULL: AtomicU64 = AtomicU64::new(0);
static DROPPED_EXPORT_FAILED: AtomicU64 = AtomicU64::new(0);
static EXPORT_FAILURES: AtomicU64 = AtomicU64::new(0);

enum Msg {
    Span(Box<SpanData>),
    Flush(SyncSender<bool>),
    Shutdown(SyncSender<bool>),
}

struct QueueSink {
    tx: SyncSender<Msg>,
}

impl SpanSink for QueueSink {
    fn export(&self, span: SpanData) {
        match self.tx.try_send(Msg::Span(Box::new(span))) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                DROPPED_QUEUE_FULL.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

/// The running exporter. Dropping it flushes queued spans (bounded by a
/// few seconds) and stops the export thread.
pub struct Exporter {
    tx: SyncSender<Msg>,
    thread: Option<JoinHandle<()>>,
}

struct Transport {
    agent: ureq::Agent,
    endpoint: String,
    extra_headers: Vec<(String, String)>,
    resource: Vec<(String, String)>,
}

impl Transport {
    fn new(config: &TracingConfig) -> Result<Self, String> {
        let mut builder = ureq::AgentBuilder::new()
            .timeout(config.timeout)
            .redirects(0);
        if config.endpoint.starts_with("https://")
            && let Some(ca) = config.ca_file.as_deref()
        {
            let tls = dash_http::client_config(Some(ca), None).map_err(|err| {
                format!(
                    "OTEL_EXPORTER_OTLP_CERTIFICATE ({}) rejected: {err}",
                    ca.display()
                )
            })?;
            builder = builder.tls_config(tls);
        }
        Ok(Self {
            agent: builder.build(),
            endpoint: config.endpoint.clone(),
            extra_headers: config.extra_headers.clone(),
            resource: config.resource.clone(),
        })
    }

    fn send(&self, spans: &[SpanData]) -> Result<(), String> {
        let body = encode_export_request(&self.resource, spans);
        let mut request = self
            .agent
            .post(&self.endpoint)
            .set("Content-Type", "application/x-protobuf");
        for (name, value) in &self.extra_headers {
            request = request.set(name, value);
        }
        match request.send_bytes(&body) {
            Ok(_) => Ok(()),
            Err(ureq::Error::Status(code, _)) => Err(format!("collector answered HTTP {code}")),
            Err(ureq::Error::Transport(err)) => {
                Err(format!("collector unreachable ({})", err.kind()))
            }
        }
    }
}

fn flush_batch(
    transport: &Transport,
    batch: &mut Vec<SpanData>,
    max_batch: usize,
    consecutive_failures: &mut u64,
) {
    while !batch.is_empty() {
        let take = batch.len().min(max_batch);
        let chunk: Vec<SpanData> = batch.drain(..take).collect();
        match transport.send(&chunk) {
            Ok(()) => {
                EXPORTED.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                if *consecutive_failures > 0 {
                    tracing::info!("trace export recovered");
                }
                *consecutive_failures = 0;
            }
            Err(reason) => {
                EXPORT_FAILURES.fetch_add(1, Ordering::Relaxed);
                DROPPED_EXPORT_FAILED.fetch_add(chunk.len() as u64, Ordering::Relaxed);
                *consecutive_failures += 1;
                // First failure, then every 100th, so a down collector does
                // not flood the log.
                if *consecutive_failures % 100 == 1 {
                    tracing::warn!(
                        spans = chunk.len(),
                        failures = *consecutive_failures,
                        "trace export failed: {reason}"
                    );
                }
            }
        }
    }
}

fn run_export_loop(rx: Receiver<Msg>, transport: Transport, max_batch: usize, delay: Duration) {
    let mut batch: Vec<SpanData> = Vec::with_capacity(max_batch);
    let mut deadline = Instant::now() + delay;
    let mut consecutive_failures = 0u64;
    let flush = |batch: &mut Vec<SpanData>, failures: &mut u64| {
        flush_batch(&transport, batch, max_batch, failures);
    };
    loop {
        let wait = deadline.saturating_duration_since(Instant::now());
        match rx.recv_timeout(wait) {
            Ok(Msg::Span(span)) => {
                batch.push(*span);
                if batch.len() >= max_batch {
                    flush(&mut batch, &mut consecutive_failures);
                    deadline = Instant::now() + delay;
                }
            }
            Ok(Msg::Flush(done)) => {
                flush(&mut batch, &mut consecutive_failures);
                let _ = done.send(consecutive_failures == 0);
                deadline = Instant::now() + delay;
            }
            Ok(Msg::Shutdown(done)) => {
                flush(&mut batch, &mut consecutive_failures);
                let _ = done.send(consecutive_failures == 0);
                return;
            }
            Err(RecvTimeoutError::Timeout) => {
                flush(&mut batch, &mut consecutive_failures);
                deadline = Instant::now() + delay;
            }
            Err(RecvTimeoutError::Disconnected) => {
                flush(&mut batch, &mut consecutive_failures);
                return;
            }
        }
    }
}

impl Exporter {
    /// Start the export thread and install the tracer for this process.
    pub fn install(config: &TracingConfig) -> Result<Self, String> {
        let transport = Transport::new(config)?;
        let (tx, rx) = mpsc::sync_channel::<Msg>(config.max_queue);
        let max_batch = config.max_batch.max(1);
        let delay = config.schedule_delay;
        let thread = std::thread::Builder::new()
            .name("dash-otlp-export".to_string())
            .spawn(move || run_export_loop(rx, transport, max_batch, delay))
            .map_err(|err| format!("could not start the trace export thread: {err}"))?;
        trace::install(
            config.tracer.clone(),
            Arc::new(QueueSink { tx: tx.clone() }),
        );
        Ok(Self {
            tx,
            thread: Some(thread),
        })
    }

    /// Export every span queued so far and wait (up to `timeout`) for the
    /// collector to accept them. `true` when the export succeeded in time.
    pub fn force_flush(&self, timeout: Duration) -> bool {
        let (done, wait) = mpsc::sync_channel(1);
        if self.tx.send(Msg::Flush(done)).is_err() {
            return false;
        }
        wait.recv_timeout(timeout).unwrap_or(false)
    }

    /// Uninstall the tracer, flush and stop the export thread.
    pub fn shutdown(mut self) -> bool {
        self.stop()
    }

    fn stop(&mut self) -> bool {
        let Some(thread) = self.thread.take() else {
            return true;
        };
        trace::uninstall();
        let (done, wait) = mpsc::sync_channel(1);
        let ok = self.tx.send(Msg::Shutdown(done)).is_ok()
            && wait.recv_timeout(SHUTDOWN_FLUSH_TIMEOUT).unwrap_or(false);
        if ok {
            let _ = thread.join();
        }
        ok
    }
}

impl Drop for Exporter {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Keeps the exporter of a service alive; flushes on drop.
pub struct TracingGuard {
    exporter: Option<Exporter>,
}

impl TracingGuard {
    pub fn is_enabled(&self) -> bool {
        self.exporter.is_some()
    }

    pub fn exporter(&self) -> Option<&Exporter> {
        self.exporter.as_ref()
    }
}

/// Read the environment, start the exporter when tracing is enabled and log
/// the outcome. Call once at startup, after logging is initialized, and keep
/// the guard for the life of the process.
pub fn init_from_env(default_service_name: &str) -> TracingGuard {
    let settings = TracingConfig::from_env(default_service_name);
    for warning in &settings.warnings {
        tracing::warn!("tracing configuration: {warning}");
    }
    let Some(config) = settings.config else {
        tracing::debug!(
            "distributed tracing is off: {}",
            settings.disabled_reason.unwrap_or_default()
        );
        return TracingGuard { exporter: None };
    };
    match Exporter::install(&config) {
        Ok(exporter) => {
            tracing::info!(
                endpoint = %redact_endpoint(&config.endpoint),
                sampler = ?config.tracer.sampler,
                "distributed tracing enabled (OTLP/HTTP protobuf)"
            );
            TracingGuard {
                exporter: Some(exporter),
            }
        }
        Err(reason) => {
            tracing::error!("distributed tracing disabled: {reason}");
            TracingGuard { exporter: None }
        }
    }
}

/// Scheme, host and port of an endpoint (no user info, path or query).
fn redact_endpoint(endpoint: &str) -> String {
    let (scheme, rest) = endpoint.split_once("://").unwrap_or(("", endpoint));
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    format!("{scheme}://{host}")
}

/// Exporter counters, rendered with the service metrics when tracing is on.
pub fn render_metrics(w: &mut MetricsWriter) {
    if !trace::enabled() {
        return;
    }
    w.header(
        "dash_trace_spans_started_total",
        "Sampled spans started by this process.",
        MetricKind::Counter,
    );
    w.sample(
        "dash_trace_spans_started_total",
        &[],
        trace::spans_started_total() as f64,
    );
    w.header(
        "dash_trace_spans_exported_total",
        "Spans accepted by the OTLP collector.",
        MetricKind::Counter,
    );
    w.sample(
        "dash_trace_spans_exported_total",
        &[],
        EXPORTED.load(Ordering::Relaxed) as f64,
    );
    w.header(
        "dash_trace_spans_dropped_total",
        "Spans not exported, by reason: queue_full (export queue full), span_limit (per-request span cap), export_failed (collector rejected or unreachable).",
        MetricKind::Counter,
    );
    for (reason, value) in [
        ("queue_full", DROPPED_QUEUE_FULL.load(Ordering::Relaxed)),
        ("span_limit", trace::spans_dropped_span_limit_total()),
        (
            "export_failed",
            DROPPED_EXPORT_FAILED.load(Ordering::Relaxed),
        ),
    ] {
        w.sample(
            "dash_trace_spans_dropped_total",
            &[("reason", reason)],
            value as f64,
        );
    }
    w.header(
        "dash_trace_export_failures_total",
        "OTLP export requests that failed.",
        MetricKind::Counter,
    );
    w.sample(
        "dash_trace_export_failures_total",
        &[],
        EXPORT_FAILURES.load(Ordering::Relaxed) as f64,
    );
}

// ---------------------------------------------------------------------------
// Test support: a protobuf reader and an in-process OTLP/HTTP collector
// ---------------------------------------------------------------------------

/// Decoding of exported requests and a mock collector, for tests in this
/// crate and in the services.
pub mod testing {
    use super::*;

    /// One decoded span.
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct DecodedSpan {
        pub trace_id: String,
        pub span_id: String,
        pub parent_span_id: String,
        pub trace_state: String,
        pub name: String,
        pub kind: u64,
        pub start_unix_nanos: u64,
        pub end_unix_nanos: u64,
        /// Attribute values rendered as text.
        pub attributes: HashMap<String, String>,
        pub links: Vec<(String, String)>,
        pub status_code: u64,
        pub flags: u32,
    }

    impl DecodedSpan {
        pub fn attr(&self, name: &str) -> Option<&str> {
            self.attributes.get(name).map(String::as_str)
        }
    }

    /// A decoded request: resource attributes and spans.
    #[derive(Clone, Debug, Default, PartialEq)]
    pub struct DecodedRequest {
        pub resource: HashMap<String, String>,
        pub spans: Vec<DecodedSpan>,
    }

    enum Field<'a> {
        Varint(u64),
        Fixed64(u64),
        Fixed32(u32),
        Bytes(&'a [u8]),
    }

    fn read_varint(data: &[u8], pos: &mut usize) -> Result<u64, String> {
        let mut out = 0u64;
        for shift in (0..64).step_by(7) {
            let byte = *data.get(*pos).ok_or("truncated varint")?;
            *pos += 1;
            out |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return Ok(out);
            }
        }
        Err("varint too long".to_string())
    }

    fn fields(data: &[u8]) -> Result<Vec<(u32, Field<'_>)>, String> {
        let mut pos = 0;
        let mut out = Vec::new();
        while pos < data.len() {
            let key = read_varint(data, &mut pos)?;
            let field = (key >> 3) as u32;
            let value = match (key & 7) as u8 {
                pb::VARINT => Field::Varint(read_varint(data, &mut pos)?),
                pb::FIXED64 => {
                    let raw = data.get(pos..pos + 8).ok_or("truncated fixed64")?;
                    pos += 8;
                    Field::Fixed64(u64::from_le_bytes(raw.try_into().map_err(|_| "fixed64")?))
                }
                pb::FIXED32 => {
                    let raw = data.get(pos..pos + 4).ok_or("truncated fixed32")?;
                    pos += 4;
                    Field::Fixed32(u32::from_le_bytes(raw.try_into().map_err(|_| "fixed32")?))
                }
                pb::LEN => {
                    let len = read_varint(data, &mut pos)? as usize;
                    let raw = data.get(pos..pos + len).ok_or("truncated bytes")?;
                    pos += len;
                    Field::Bytes(raw)
                }
                other => return Err(format!("unsupported wire type {other}")),
            };
            out.push((field, value));
        }
        Ok(out)
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    fn text(bytes: &[u8]) -> String {
        String::from_utf8_lossy(bytes).into_owned()
    }

    fn key_value(data: &[u8]) -> Result<(String, String), String> {
        let mut name = String::new();
        let mut value = String::new();
        for (field, v) in fields(data)? {
            match (field, v) {
                (1, Field::Bytes(b)) => name = text(b),
                (2, Field::Bytes(any)) => {
                    for (f, inner) in fields(any)? {
                        value = match (f, inner) {
                            (1, Field::Bytes(s)) => text(s),
                            (2, Field::Varint(b)) => (b != 0).to_string(),
                            (3, Field::Varint(i)) => (i as i64).to_string(),
                            _ => value,
                        };
                    }
                }
                _ => {}
            }
        }
        Ok((name, value))
    }

    fn span(data: &[u8]) -> Result<DecodedSpan, String> {
        let mut s = DecodedSpan::default();
        for (field, v) in fields(data)? {
            match (field, v) {
                (1, Field::Bytes(b)) => s.trace_id = hex(b),
                (2, Field::Bytes(b)) => s.span_id = hex(b),
                (3, Field::Bytes(b)) => s.trace_state = text(b),
                (4, Field::Bytes(b)) => s.parent_span_id = hex(b),
                (5, Field::Bytes(b)) => s.name = text(b),
                (6, Field::Varint(k)) => s.kind = k,
                (7, Field::Fixed64(t)) => s.start_unix_nanos = t,
                (8, Field::Fixed64(t)) => s.end_unix_nanos = t,
                (9, Field::Bytes(b)) => {
                    let (k, v) = key_value(b)?;
                    s.attributes.insert(k, v);
                }
                (13, Field::Bytes(b)) => {
                    let mut link = (String::new(), String::new());
                    for (f, lv) in fields(b)? {
                        match (f, lv) {
                            (1, Field::Bytes(t)) => link.0 = hex(t),
                            (2, Field::Bytes(t)) => link.1 = hex(t),
                            _ => {}
                        }
                    }
                    s.links.push(link);
                }
                (15, Field::Bytes(b)) => {
                    for (f, sv) in fields(b)? {
                        if let (3, Field::Varint(code)) = (f, sv) {
                            s.status_code = code;
                        }
                    }
                }
                (16, Field::Fixed32(flags)) => s.flags = flags,
                _ => {}
            }
        }
        Ok(s)
    }

    /// Decode an `ExportTraceServiceRequest`.
    pub fn decode_export_request(data: &[u8]) -> Result<DecodedRequest, String> {
        let mut out = DecodedRequest::default();
        for (field, rs) in fields(data)? {
            let (1, Field::Bytes(rs)) = (field, rs) else {
                continue;
            };
            for (f, v) in fields(rs)? {
                match (f, v) {
                    (1, Field::Bytes(resource)) => {
                        for (rf, kv) in fields(resource)? {
                            if let (1, Field::Bytes(kv)) = (rf, kv) {
                                let (k, v) = key_value(kv)?;
                                out.resource.insert(k, v);
                            }
                        }
                    }
                    (2, Field::Bytes(scope_spans)) => {
                        for (sf, sv) in fields(scope_spans)? {
                            if let (2, Field::Bytes(raw)) = (sf, sv) {
                                out.spans.push(span(raw)?);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(out)
    }

    /// One request the collector received.
    #[derive(Clone, Debug)]
    pub struct ReceivedExport {
        pub path: String,
        pub content_type: String,
        /// Lower-cased header names and values.
        pub headers: HashMap<String, String>,
        pub request: DecodedRequest,
    }

    /// An OTLP/HTTP collector on loopback that accepts every POST and keeps
    /// the decoded requests.
    pub struct MockCollector {
        addr: String,
        received: Arc<Mutex<Vec<ReceivedExport>>>,
        status: Arc<AtomicU64>,
    }

    impl MockCollector {
        pub fn start() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock collector");
            let addr = listener.local_addr().expect("collector addr").to_string();
            let received = Arc::new(Mutex::new(Vec::new()));
            let status = Arc::new(AtomicU64::new(200));
            let sink = Arc::clone(&received);
            let answer = Arc::clone(&status);
            std::thread::Builder::new()
                .name("mock-otlp-collector".to_string())
                .spawn(move || {
                    for stream in listener.incoming().flatten() {
                        let sink = Arc::clone(&sink);
                        let code = answer.load(Ordering::Relaxed) as u16;
                        std::thread::spawn(move || serve(stream, &sink, code));
                    }
                })
                .expect("spawn mock collector");
            Self {
                addr,
                received,
                status,
            }
        }

        /// `http://127.0.0.1:<port>` (append [`TRACES_PATH`] for the traces
        /// endpoint).
        pub fn base_url(&self) -> String {
            format!("http://{}", self.addr)
        }

        /// Answer every later request with `code`.
        pub fn set_status(&self, code: u16) {
            self.status.store(u64::from(code), Ordering::Relaxed);
        }

        pub fn exports(&self) -> Vec<ReceivedExport> {
            self.received
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()
        }

        /// Every span received so far.
        pub fn spans(&self) -> Vec<DecodedSpan> {
            self.exports()
                .into_iter()
                .flat_map(|e| e.request.spans)
                .collect()
        }
    }

    fn serve(mut stream: TcpStream, sink: &Mutex<Vec<ReceivedExport>>, code: u16) {
        let _ = stream.set_read_timeout(Some(Duration::from_secs(10)));
        let mut raw = Vec::new();
        let mut chunk = [0u8; 8192];
        let header_end = loop {
            if let Some(pos) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                break pos + 4;
            }
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => return,
                Ok(n) => raw.extend_from_slice(&chunk[..n]),
            }
        };
        let head = String::from_utf8_lossy(&raw[..header_end]).into_owned();
        let mut lines = head.lines();
        let path = lines
            .next()
            .and_then(|l| l.split_whitespace().nth(1))
            .unwrap_or("")
            .to_string();
        let headers: HashMap<String, String> = lines
            .filter_map(|l| l.split_once(':'))
            .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_string()))
            .collect();
        let length: usize = headers
            .get("content-length")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut body = raw[header_end..].to_vec();
        while body.len() < length {
            match stream.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => body.extend_from_slice(&chunk[..n]),
            }
        }
        if let Ok(request) = decode_export_request(&body) {
            sink.lock()
                .unwrap_or_else(|p| p.into_inner())
                .push(ReceivedExport {
                    path,
                    content_type: headers.get("content-type").cloned().unwrap_or_default(),
                    headers,
                    request,
                });
        }
        let reason = if code == 200 { "OK" } else { "Error" };
        let _ = stream.write_all(
            format!(
                "HTTP/1.1 {code} {reason}\r\nContent-Type: application/x-protobuf\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            )
            .as_bytes(),
        );
    }
}

#[cfg(test)]
mod tests {
    use super::testing::*;
    use super::*;
    use crate::trace::{SpanContext, SpanId, TraceId};
    use std::borrow::Cow;

    fn lookup(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn tracing_is_off_unless_enabled_or_an_endpoint_is_set() {
        let s = TracingConfig::from_lookup(&lookup(&[]), "svc");
        assert!(s.config.is_none());
        assert!(s.disabled_reason.unwrap().contains("DASH_OTEL_ENABLED"));

        let s = TracingConfig::from_lookup(&lookup(&[("DASH_OTEL_ENABLED", "1")]), "svc");
        let c = s.config.unwrap();
        assert_eq!(c.endpoint, DEFAULT_TRACES_ENDPOINT);
        assert_eq!(
            c.resource[0],
            ("service.name".to_string(), "svc".to_string())
        );
        assert_eq!(c.tracer.sampler, Sampler::default());

        let s = TracingConfig::from_lookup(
            &lookup(&[("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318/")]),
            "svc",
        );
        assert_eq!(
            s.config.unwrap().endpoint,
            "http://collector:4318/v1/traces"
        );

        for (name, value) in [
            ("DASH_OTEL_ENABLED", "0"),
            ("OTEL_SDK_DISABLED", "true"),
            ("OTEL_TRACES_EXPORTER", "none"),
        ] {
            let s = TracingConfig::from_lookup(
                &lookup(&[
                    ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://c:4318"),
                    (name, value),
                ]),
                "svc",
            );
            assert!(s.config.is_none(), "{name}={value}");
        }
    }

    #[test]
    fn signal_specific_settings_win_and_unsupported_protocols_stay_off() {
        let s = TracingConfig::from_lookup(
            &lookup(&[
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://base:4318"),
                (
                    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
                    "https://traces.example/custom",
                ),
                ("OTEL_EXPORTER_OTLP_HEADERS", "x-generic=1"),
                (
                    "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
                    "x-tenant=a%20b, x-other = 2",
                ),
                ("OTEL_EXPORTER_OTLP_TIMEOUT", "2500"),
                ("OTEL_SERVICE_NAME", "custom"),
                (
                    "OTEL_RESOURCE_ATTRIBUTES",
                    "service.name=ignored,deployment.environment=prod",
                ),
                ("OTEL_TRACES_SAMPLER", "parentbased_traceidratio"),
                ("OTEL_TRACES_SAMPLER_ARG", "0.1"),
                ("OTEL_BSP_MAX_QUEUE_SIZE", "100"),
                ("OTEL_BSP_MAX_EXPORT_BATCH_SIZE", "500"),
                ("DASH_OTEL_BACKGROUND_SAMPLE_RATIO", "0.5"),
            ]),
            "svc",
        );
        assert!(s.warnings.is_empty(), "{:?}", s.warnings);
        let c = s.config.unwrap();
        assert_eq!(c.endpoint, "https://traces.example/custom");
        assert_eq!(
            c.extra_headers,
            vec![
                ("x-tenant".to_string(), "a b".to_string()),
                ("x-other".to_string(), "2".to_string())
            ]
        );
        assert_eq!(c.timeout, Duration::from_millis(2500));
        assert_eq!(
            c.resource,
            vec![
                ("service.name".to_string(), "custom".to_string()),
                ("deployment.environment".to_string(), "prod".to_string())
            ]
        );
        assert_eq!(
            c.tracer.sampler,
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatio(0.1)))
        );
        assert_eq!(c.tracer.background_ratio, 0.5);
        assert_eq!((c.max_queue, c.max_batch), (100, 100));

        for protocol in ["grpc", "http/json"] {
            let s = TracingConfig::from_lookup(
                &lookup(&[
                    ("DASH_OTEL_ENABLED", "1"),
                    ("OTEL_EXPORTER_OTLP_PROTOCOL", protocol),
                ]),
                "svc",
            );
            assert!(s.config.is_none(), "{protocol}");
        }
        let s = TracingConfig::from_lookup(
            &lookup(&[("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "collector:4318")]),
            "svc",
        );
        assert!(s.config.is_none());
    }

    #[test]
    fn bad_values_fall_back_with_warnings_that_never_echo_header_values() {
        let s = TracingConfig::from_lookup(
            &lookup(&[
                ("DASH_OTEL_ENABLED", "yes"),
                ("OTEL_EXPORTER_OTLP_ENDPOINT", "http://10.0.0.5:4318"),
                (
                    "OTEL_EXPORTER_OTLP_HEADERS",
                    "authorization=Bearer%20zz-credential,broken",
                ),
                ("OTEL_TRACES_SAMPLER", "traceidratio"),
                ("OTEL_TRACES_SAMPLER_ARG", "2"),
                ("OTEL_BSP_SCHEDULE_DELAY", "soon"),
            ]),
            "svc",
        );
        let c = s.config.unwrap();
        assert_eq!(c.tracer.sampler, Sampler::default());
        assert_eq!(c.schedule_delay, DEFAULT_SCHEDULE_DELAY);
        assert_eq!(c.extra_headers.len(), 1);
        let all = s.warnings.join("\n");
        assert!(all.contains("entry 2"), "{all}");
        assert!(all.contains("plain http://"), "{all}");
        assert!(!all.contains("zz-credential"), "{all}");
        assert_eq!(
            redact_endpoint("https://u:p@host:4318/v1/traces?x"),
            "https://host:4318"
        );
    }

    fn sample_span() -> SpanData {
        SpanData {
            trace_id: TraceId([0x11; 16]),
            span_id: SpanId([0x22; 8]),
            parent_span_id: Some(SpanId([0x33; 8])),
            parent_is_remote: true,
            sampled: true,
            trace_state: Some(Arc::from("v=1")),
            name: Cow::Borrowed("POST ingest"),
            kind: SpanKind::Server,
            start_unix_nanos: 1_000,
            end_unix_nanos: 2_500,
            attributes: vec![
                ("http.route", AttrValue::from("ingest")),
                ("http.response.status_code", AttrValue::from(200u16)),
                ("dash.ok", AttrValue::from(true)),
                ("negative", AttrValue::Int(-3)),
            ],
            dropped_attributes: 0,
            links: vec![SpanContext {
                trace_id: TraceId([0x44; 16]),
                span_id: SpanId([0x55; 8]),
                sampled: true,
                trace_state: None,
            }],
            status: SpanStatus::Error("failed"),
        }
    }

    #[test]
    fn export_requests_encode_and_decode() {
        let resource = vec![("service.name".to_string(), "unit".to_string())];
        let body = encode_export_request(&resource, &[sample_span()]);
        let decoded = decode_export_request(&body).unwrap();
        assert_eq!(decoded.resource["service.name"], "unit");
        assert_eq!(decoded.resource["telemetry.sdk.name"], "dash-observe");
        assert_eq!(decoded.spans.len(), 1);
        let s = &decoded.spans[0];
        assert_eq!(s.trace_id, "11".repeat(16));
        assert_eq!(s.span_id, "22".repeat(8));
        assert_eq!(s.parent_span_id, "33".repeat(8));
        assert_eq!(s.trace_state, "v=1");
        assert_eq!(s.name, "POST ingest");
        assert_eq!(s.kind, 2);
        assert_eq!((s.start_unix_nanos, s.end_unix_nanos), (1_000, 2_500));
        assert_eq!(s.attr("http.route"), Some("ingest"));
        assert_eq!(s.attr("http.response.status_code"), Some("200"));
        assert_eq!(s.attr("dash.ok"), Some("true"));
        assert_eq!(s.attr("negative"), Some("-3"));
        assert_eq!(s.links, vec![("44".repeat(16), "55".repeat(8))]);
        assert_eq!(s.status_code, 2);
        assert_eq!(s.flags, 0x301);
    }

    #[test]
    fn the_export_loop_batches_flushes_and_counts_failures() {
        let collector = MockCollector::start();
        let config = TracingConfig {
            endpoint: format!("{}{TRACES_PATH}", collector.base_url()),
            extra_headers: vec![("x-collector-tenant".to_string(), "unit".to_string())],
            timeout: Duration::from_secs(5),
            ca_file: None,
            resource: vec![("service.name".to_string(), "unit".to_string())],
            tracer: TracerConfig::default(),
            max_queue: 16,
            max_batch: 2,
            schedule_delay: Duration::from_secs(3600),
        };
        let transport = Transport::new(&config).unwrap();
        let (tx, rx) = mpsc::sync_channel(16);
        let thread = std::thread::spawn(move || {
            run_export_loop(rx, transport, 2, Duration::from_secs(3600))
        });
        let sink = QueueSink { tx: tx.clone() };
        for _ in 0..3 {
            sink.export(sample_span());
        }
        let (done, wait) = mpsc::sync_channel(1);
        tx.send(Msg::Flush(done)).unwrap();
        assert!(wait.recv_timeout(Duration::from_secs(10)).unwrap());
        let exports = collector.exports();
        // A full batch of two went out as soon as it filled, the third on
        // the flush.
        assert_eq!(exports.len(), 2);
        assert_eq!(exports[0].path, TRACES_PATH);
        assert_eq!(exports[0].content_type, "application/x-protobuf");
        assert_eq!(exports[0].headers["x-collector-tenant"], "unit");
        assert_eq!(collector.spans().len(), 3);

        let failures_before = EXPORT_FAILURES.load(Ordering::Relaxed);
        collector.set_status(503);
        sink.export(sample_span());
        let (done, wait) = mpsc::sync_channel(1);
        tx.send(Msg::Flush(done)).unwrap();
        assert!(!wait.recv_timeout(Duration::from_secs(10)).unwrap());
        assert!(EXPORT_FAILURES.load(Ordering::Relaxed) > failures_before);

        let (done, wait) = mpsc::sync_channel(1);
        tx.send(Msg::Shutdown(done)).unwrap();
        let _ = wait.recv_timeout(Duration::from_secs(10));
        thread.join().unwrap();
    }

    #[test]
    fn a_full_queue_drops_instead_of_blocking() {
        let (tx, _rx) = mpsc::sync_channel(1);
        let sink = QueueSink { tx };
        let before = DROPPED_QUEUE_FULL.load(Ordering::Relaxed);
        sink.export(sample_span());
        sink.export(sample_span());
        assert_eq!(DROPPED_QUEUE_FULL.load(Ordering::Relaxed), before + 1);
    }
}
