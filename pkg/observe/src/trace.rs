//! Distributed tracing core: W3C Trace Context, sampling and span recording.
//!
//! This module has no dependencies beyond `std`, so the storage crates can
//! open spans without pulling in an exporter. A process turns tracing on by
//! calling [`install`] with a [`SpanSink`] (the OTLP exporter in
//! [`crate::otlp`], or a test sink). Until then every entry point returns
//! after one relaxed atomic load: no id is generated, no clock is read, no
//! thread-local is touched and nothing is allocated.
//!
//! # Model
//!
//! * A [`Span`] is opened on the current thread and becomes the thread's
//!   *current* span until it is dropped (spans must be dropped in reverse
//!   order of creation, which Rust scoping gives for free). The DASH HTTP
//!   server runs every request on one worker thread, so the request span
//!   opened by the HTTP instrumentation is the parent of every span the
//!   handler opens.
//! * A [`DetachedSpan`] is not tied to a thread. It is used for work that
//!   crosses threads (a checkpoint started on a request thread and written on
//!   a background thread, a group-commit batch written by the committer
//!   thread on behalf of several requests). [`enter_context`] makes a context
//!   current on another thread.
//! * [`span`] and [`client_span`] open children of the current span and do
//!   nothing when there is none, so library code can be instrumented without
//!   creating stray root traces during startup replay or tests.
//!   [`server_span`] and [`background_root`] start a new local root.
//!
//! # Bounds
//!
//! Spans are opened at fixed points (never per record or per candidate), and
//! each local scope (one request on one thread, one background tick, one
//! entered context) is additionally capped at [`MAX_SPANS_PER_SCOPE`]
//! children; spans over the cap are counted in
//! `dash_trace_spans_dropped_total{reason="span_limit"}` instead of being
//! recorded. Attributes are capped at [`MAX_ATTRIBUTES`] per span and links at
//! [`MAX_LINKS`].
//!
//! # What may go into a span
//!
//! Attribute keys are `&'static str`. Values are bounded, low-cardinality
//! facts: route labels, status codes, counts, sizes, outcomes, the request id.
//! Claim text, query text, evidence, tenant ids, credentials and headers are
//! never recorded.

use std::borrow::Cow;
use std::cell::{Cell, RefCell};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Request header carrying the W3C trace parent (lower case, as the DASH
/// parser stores header names).
pub const TRACEPARENT_HEADER: &str = "traceparent";
/// Request header carrying vendor trace state.
pub const TRACESTATE_HEADER: &str = "tracestate";
/// Children recorded per local scope; further spans are dropped and counted.
pub const MAX_SPANS_PER_SCOPE: u32 = 64;
/// Attributes recorded per span; further attributes are dropped and counted
/// in the span's `dropped_attributes_count`.
pub const MAX_ATTRIBUTES: usize = 16;
/// Links recorded per span.
pub const MAX_LINKS: usize = 16;
/// Longest `tracestate` value that is propagated (the W3C limit).
pub const MAX_TRACESTATE_LEN: usize = 512;

// ---------------------------------------------------------------------------
// Identifiers and context
// ---------------------------------------------------------------------------

/// 16-byte trace id.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct TraceId(pub [u8; 16]);

/// 8-byte span id.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SpanId(pub [u8; 8]);

fn hex_into(out: &mut String, bytes: &[u8]) {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0x0f) as usize] as char);
    }
}

fn parse_hex<const N: usize>(text: &str) -> Option<[u8; N]> {
    let raw = text.as_bytes();
    if raw.len() != N * 2 {
        return None;
    }
    let digit = |c: u8| match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        _ => None,
    };
    let mut out = [0u8; N];
    for (i, pair) in raw.chunks_exact(2).enumerate() {
        out[i] = (digit(pair[0])? << 4) | digit(pair[1])?;
    }
    Some(out)
}

impl TraceId {
    pub fn to_hex(&self) -> String {
        let mut out = String::with_capacity(32);
        hex_into(&mut out, &self.0);
        out
    }

    pub fn from_hex(text: &str) -> Option<Self> {
        parse_hex::<16>(text).map(TraceId)
    }

    pub fn is_valid(&self) -> bool {
        self.0 != [0; 16]
    }
}

impl SpanId {
    pub fn to_hex(&self) -> String {
        let mut out = String::with_capacity(16);
        hex_into(&mut out, &self.0);
        out
    }

    pub fn from_hex(text: &str) -> Option<Self> {
        parse_hex::<8>(text).map(SpanId)
    }

    pub fn is_valid(&self) -> bool {
        self.0 != [0; 8]
    }
}

/// The propagated part of a span: what goes into `traceparent` and
/// `tracestate`.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct SpanContext {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub sampled: bool,
    /// Validated vendor `tracestate`, passed through unchanged.
    pub trace_state: Option<Arc<str>>,
}

impl SpanContext {
    /// The `traceparent` header value (version `00`).
    pub fn traceparent(&self) -> String {
        format_traceparent(&self.trace_id, &self.span_id, self.sampled)
    }
}

/// `00-<trace-id>-<span-id>-<flags>`.
pub fn format_traceparent(trace_id: &TraceId, span_id: &SpanId, sampled: bool) -> String {
    let mut out = String::with_capacity(55);
    out.push_str("00-");
    hex_into(&mut out, &trace_id.0);
    out.push('-');
    hex_into(&mut out, &span_id.0);
    out.push_str(if sampled { "-01" } else { "-00" });
    out
}

/// Parse a `traceparent` header per W3C Trace Context level 1.
///
/// Returns `None` for anything malformed: wrong length, upper-case or
/// non-hex digits, the forbidden version `ff`, an all-zero trace or span id,
/// or extra data after a version-`00` value. Values of a higher version are
/// accepted when their first 55 characters parse, as the specification asks.
pub fn parse_traceparent(value: &str) -> Option<SpanContext> {
    let value = value.trim_matches(|c| c == ' ' || c == '\t');
    let raw = value.as_bytes();
    if raw.len() < 55 {
        return None;
    }
    let version = parse_hex::<1>(value.get(0..2)?)?[0];
    if version == 0xff {
        return None;
    }
    if version == 0 && raw.len() != 55 {
        return None;
    }
    if raw.len() > 55 && raw[55] != b'-' {
        return None;
    }
    if raw[2] != b'-' || raw[35] != b'-' || raw[52] != b'-' {
        return None;
    }
    let trace_id = TraceId::from_hex(value.get(3..35)?)?;
    let span_id = SpanId::from_hex(value.get(36..52)?)?;
    let flags = parse_hex::<1>(value.get(53..55)?)?[0];
    if !trace_id.is_valid() || !span_id.is_valid() {
        return None;
    }
    Some(SpanContext {
        trace_id,
        span_id,
        sampled: flags & 0x01 == 0x01,
        trace_state: None,
    })
}

/// Accept a `tracestate` value for propagation: at most
/// [`MAX_TRACESTATE_LEN`] bytes of printable ASCII and at most 32 list
/// members. Anything else is dropped (the trace still continues).
pub fn sanitize_tracestate(value: &str) -> Option<Arc<str>> {
    let value = value.trim_matches(|c| c == ' ' || c == '\t');
    if value.is_empty()
        || value.len() > MAX_TRACESTATE_LEN
        || !value.bytes().all(|b| (0x20..=0x7e).contains(&b))
    {
        return None;
    }
    let members = value.split(',').filter(|m| !m.trim().is_empty()).count();
    let malformed_member = value
        .split(',')
        .any(|m| !m.trim().is_empty() && !m.contains('='));
    if members == 0 || members > 32 || malformed_member {
        return None;
    }
    Some(Arc::from(value))
}

/// The remote parent described by incoming `traceparent` / `tracestate`
/// headers. `tracestate` is only kept with a valid `traceparent`.
pub fn remote_context(traceparent: Option<&str>, tracestate: Option<&str>) -> Option<SpanContext> {
    let mut ctx = parse_traceparent(traceparent?)?;
    ctx.trace_state = tracestate.and_then(sanitize_tracestate);
    Some(ctx)
}

// ---------------------------------------------------------------------------
// Sampling
// ---------------------------------------------------------------------------

/// Head sampler, configured with `OTEL_TRACES_SAMPLER` /
/// `OTEL_TRACES_SAMPLER_ARG`.
#[derive(Clone, Debug, PartialEq)]
pub enum Sampler {
    AlwaysOn,
    AlwaysOff,
    /// Sample a fixed fraction of traces, decided from the trace id so every
    /// service makes the same decision for the same trace.
    TraceIdRatio(f64),
    /// Follow the parent's decision when there is a parent; use the inner
    /// sampler for new traces.
    ParentBased(Box<Sampler>),
}

impl Default for Sampler {
    /// `parentbased_always_on`, the OpenTelemetry default.
    fn default() -> Self {
        Sampler::ParentBased(Box::new(Sampler::AlwaysOn))
    }
}

impl Sampler {
    /// Parse the standard sampler names. An empty or missing name is the
    /// default (`parentbased_always_on`). A ratio argument must be a number
    /// in `0..=1`; a missing argument means `1.0`.
    pub fn from_config(name: Option<&str>, arg: Option<&str>) -> Result<Sampler, String> {
        let ratio = || -> Result<f64, String> {
            match arg.map(str::trim).filter(|a| !a.is_empty()) {
                None => Ok(1.0),
                Some(raw) => match raw.parse::<f64>() {
                    Ok(r) if (0.0..=1.0).contains(&r) => Ok(r),
                    _ => Err(format!(
                        "OTEL_TRACES_SAMPLER_ARG must be a number between 0 and 1 (got '{raw}')"
                    )),
                },
            }
        };
        let name = name
            .map(|n| n.trim().to_ascii_lowercase())
            .unwrap_or_default();
        Ok(match name.as_str() {
            "" | "parentbased_always_on" => Sampler::default(),
            "always_on" => Sampler::AlwaysOn,
            "always_off" => Sampler::AlwaysOff,
            "traceidratio" => Sampler::TraceIdRatio(ratio()?),
            "parentbased_always_off" => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
            "parentbased_traceidratio" => {
                Sampler::ParentBased(Box::new(Sampler::TraceIdRatio(ratio()?)))
            }
            other => {
                return Err(format!(
                    "OTEL_TRACES_SAMPLER '{other}' is not supported (use always_on, always_off, \
                     traceidratio, parentbased_always_on, parentbased_always_off or \
                     parentbased_traceidratio)"
                ));
            }
        })
    }

    /// The sampling decision for a span of `trace_id` with `parent`.
    pub fn should_sample(&self, parent: Option<&SpanContext>, trace_id: &TraceId) -> bool {
        match self {
            Sampler::AlwaysOn => true,
            Sampler::AlwaysOff => false,
            Sampler::TraceIdRatio(ratio) => ratio_admits(*ratio, trace_id),
            Sampler::ParentBased(root) => match parent {
                Some(parent) => parent.sampled,
                None => root.should_sample(None, trace_id),
            },
        }
    }
}

/// Compare the low 56 bits of the trace id (the random part under W3C
/// level 2) with `ratio * 2^56`.
fn ratio_admits(ratio: f64, trace_id: &TraceId) -> bool {
    if ratio >= 1.0 {
        return true;
    }
    if ratio <= 0.0 {
        return false;
    }
    let mut low = [0u8; 8];
    low[1..].copy_from_slice(&trace_id.0[9..16]);
    let value = u64::from_be_bytes(low);
    let threshold = (ratio * (1u64 << 56) as f64) as u64;
    value < threshold
}

// ---------------------------------------------------------------------------
// Span data
// ---------------------------------------------------------------------------

/// Span kind (the subset DASH emits).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum SpanKind {
    Internal,
    Server,
    Client,
}

/// An attribute value. Construct it from `&'static str`, an owned `String`
/// (only for bounded values such as the request id), integers or `bool`.
#[derive(Clone, PartialEq, Debug)]
pub enum AttrValue {
    Str(Cow<'static, str>),
    Int(i64),
    Bool(bool),
}

impl From<&'static str> for AttrValue {
    fn from(v: &'static str) -> Self {
        AttrValue::Str(Cow::Borrowed(v))
    }
}
impl From<String> for AttrValue {
    fn from(v: String) -> Self {
        AttrValue::Str(Cow::Owned(v))
    }
}
impl From<bool> for AttrValue {
    fn from(v: bool) -> Self {
        AttrValue::Bool(v)
    }
}
impl From<i64> for AttrValue {
    fn from(v: i64) -> Self {
        AttrValue::Int(v)
    }
}
impl From<u64> for AttrValue {
    fn from(v: u64) -> Self {
        AttrValue::Int(i64::try_from(v).unwrap_or(i64::MAX))
    }
}
impl From<usize> for AttrValue {
    fn from(v: usize) -> Self {
        AttrValue::Int(i64::try_from(v).unwrap_or(i64::MAX))
    }
}
impl From<u32> for AttrValue {
    fn from(v: u32) -> Self {
        AttrValue::Int(i64::from(v))
    }
}
impl From<u16> for AttrValue {
    fn from(v: u16) -> Self {
        AttrValue::Int(i64::from(v))
    }
}

/// Span outcome.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub enum SpanStatus {
    #[default]
    Unset,
    /// The operation failed; the description is a fixed phrase, never
    /// request data.
    Error(&'static str),
}

/// A finished span, handed to the [`SpanSink`].
#[derive(Clone, Debug)]
pub struct SpanData {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    pub parent_span_id: Option<SpanId>,
    /// The parent came from another process (`traceparent`).
    pub parent_is_remote: bool,
    pub sampled: bool,
    pub trace_state: Option<Arc<str>>,
    pub name: Cow<'static, str>,
    pub kind: SpanKind,
    pub start_unix_nanos: u64,
    pub end_unix_nanos: u64,
    pub attributes: Vec<(&'static str, AttrValue)>,
    pub dropped_attributes: u32,
    pub links: Vec<SpanContext>,
    pub status: SpanStatus,
}

/// Receives finished spans. Implementations must not block for long: spans
/// are handed over on the thread that ends them.
pub trait SpanSink: Send + Sync + 'static {
    fn export(&self, span: SpanData);
}

// ---------------------------------------------------------------------------
// Global tracer
// ---------------------------------------------------------------------------

/// Settings of the installed tracer.
#[derive(Clone, Debug, PartialEq)]
pub struct TracerConfig {
    pub sampler: Sampler,
    /// Extra fraction (`0..=1`) applied to new traces started by background
    /// work (replication polls, failover heartbeats, checkpoints) on top of
    /// the sampler, so idle background loops can be thinned without lowering
    /// request sampling.
    pub background_ratio: f64,
}

impl Default for TracerConfig {
    fn default() -> Self {
        Self {
            sampler: Sampler::default(),
            background_ratio: 1.0,
        }
    }
}

struct Tracer {
    config: TracerConfig,
    sink: Arc<dyn SpanSink>,
}

static ENABLED: AtomicBool = AtomicBool::new(false);
static TRACER: RwLock<Option<Arc<Tracer>>> = RwLock::new(None);

static SPANS_STARTED: AtomicU64 = AtomicU64::new(0);
static DROPPED_SPAN_LIMIT: AtomicU64 = AtomicU64::new(0);

/// Turn tracing on for this process with `sink` receiving finished spans.
/// Calling it again replaces the previous sink and settings.
pub fn install(config: TracerConfig, sink: Arc<dyn SpanSink>) {
    let tracer = Arc::new(Tracer { config, sink });
    *TRACER.write().unwrap_or_else(|p| p.into_inner()) = Some(tracer);
    ENABLED.store(true, Ordering::Release);
}

/// Turn tracing off. Spans still open finish into the sink they started
/// with.
pub fn uninstall() {
    ENABLED.store(false, Ordering::Release);
    *TRACER.write().unwrap_or_else(|p| p.into_inner()) = None;
}

/// Whether tracing is on. This is the only cost every instrumented point
/// pays while tracing is off.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

fn tracer() -> Option<Arc<Tracer>> {
    TRACER
        .read()
        .unwrap_or_else(|p| p.into_inner())
        .as_ref()
        .map(Arc::clone)
}

/// Spans recorded (sampled and started) by this process.
pub fn spans_started_total() -> u64 {
    SPANS_STARTED.load(Ordering::Relaxed)
}

/// Spans not recorded because their scope hit [`MAX_SPANS_PER_SCOPE`].
pub fn spans_dropped_span_limit_total() -> u64 {
    DROPPED_SPAN_LIMIT.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Thread-local state
// ---------------------------------------------------------------------------

thread_local! {
    static CURRENT: RefCell<Option<SpanContext>> = const { RefCell::new(None) };
    static SCOPE_SPANS: Cell<u32> = const { Cell::new(0) };
    static RNG: Cell<(u64, u64)> = const { Cell::new((0, 0)) };
}

/// xorshift128+ seeded per thread from the OS-keyed [`RandomState`] and the
/// clock. Trace ids need uniqueness and a uniform low half (for ratio
/// sampling), not secrecy.
fn next_u64() -> u64 {
    RNG.with(|cell| {
        let (mut s0, mut s1) = cell.get();
        if s0 == 0 && s1 == 0 {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            let seed = |salt: u64| {
                let mut h = RandomState::new().build_hasher();
                h.write_u128(nanos);
                h.write_u64(salt);
                h.finish() | 1
            };
            s0 = seed(0x9e37_79b9_7f4a_7c15);
            s1 = seed(0xbf58_476d_1ce4_e5b9);
        }
        let mut x = s0;
        let y = s1;
        x ^= x << 23;
        x ^= x >> 17;
        x ^= y ^ (y >> 26);
        cell.set((y, x));
        x.wrapping_add(y)
    })
}

fn new_trace_id() -> TraceId {
    loop {
        let mut out = [0u8; 16];
        out[..8].copy_from_slice(&next_u64().to_be_bytes());
        out[8..].copy_from_slice(&next_u64().to_be_bytes());
        let id = TraceId(out);
        if id.is_valid() {
            return id;
        }
    }
}

fn new_span_id() -> SpanId {
    loop {
        let id = SpanId(next_u64().to_be_bytes());
        if id.is_valid() {
            return id;
        }
    }
}

fn unix_nanos_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// The current span's context on this thread, if any (sampled or not).
pub fn current() -> Option<SpanContext> {
    if !enabled() {
        return None;
    }
    CURRENT.with(|c| c.borrow().clone())
}

/// The `traceparent` value for an outgoing request made from the current
/// span (carries the sampling decision, so an unsampled trace stays
/// unsampled downstream).
pub fn current_traceparent() -> Option<String> {
    current().map(|ctx| ctx.traceparent())
}

/// Both propagation headers for an outgoing request: `traceparent` and,
/// when the trace carries one, `tracestate`.
pub fn outgoing_headers() -> Option<(String, Option<Arc<str>>)> {
    current().map(|ctx| (ctx.traceparent(), ctx.trace_state.clone()))
}

// ---------------------------------------------------------------------------
// Spans
// ---------------------------------------------------------------------------

struct Recording {
    tracer: Arc<Tracer>,
    data: SpanData,
    started: Instant,
}

impl Recording {
    fn finish(mut self: Box<Self>) {
        let elapsed = u64::try_from(self.started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        self.data.end_unix_nanos = self.data.start_unix_nanos.saturating_add(elapsed);
        let tracer = Arc::clone(&self.tracer);
        tracer.sink.export(self.data);
    }
}

/// A span that is not bound to a thread (see the module docs). Ends when
/// dropped.
#[must_use = "a span ends when it is dropped"]
pub struct DetachedSpan {
    ctx: Option<SpanContext>,
    recording: Option<Box<Recording>>,
}

impl DetachedSpan {
    /// A span that records nothing (tracing off, or not sampled).
    pub const fn none() -> Self {
        Self {
            ctx: None,
            recording: None,
        }
    }

    /// The span's context, for propagation and for children on other
    /// threads. `None` when tracing is off.
    pub fn context(&self) -> Option<&SpanContext> {
        self.ctx.as_ref()
    }

    /// Whether the span will be exported.
    pub fn is_recording(&self) -> bool {
        self.recording.is_some()
    }

    /// Set an attribute (see the module docs for what may be recorded).
    pub fn set_attr(&mut self, key: &'static str, value: impl Into<AttrValue>) {
        if let Some(rec) = self.recording.as_mut() {
            let attrs = &mut rec.data.attributes;
            if let Some(slot) = attrs.iter_mut().find(|(k, _)| *k == key) {
                slot.1 = value.into();
            } else if attrs.len() < MAX_ATTRIBUTES {
                attrs.push((key, value.into()));
            } else {
                rec.data.dropped_attributes = rec.data.dropped_attributes.saturating_add(1);
            }
        }
    }

    /// Mark the span failed with a fixed description.
    pub fn set_error(&mut self, description: &'static str) {
        if let Some(rec) = self.recording.as_mut() {
            rec.data.status = SpanStatus::Error(description);
        }
    }

    /// Make this span current on the calling thread until the guard drops.
    pub fn enter(&self) -> ContextGuard {
        enter_context(self.ctx.clone())
    }
}

impl Drop for DetachedSpan {
    fn drop(&mut self) {
        if let Some(rec) = self.recording.take() {
            rec.finish();
        }
    }
}

/// Restores the previous current context (and scope budget) when dropped.
#[must_use = "the context is current only while the guard lives"]
pub struct ContextGuard {
    restore: Option<(Option<SpanContext>, u32)>,
    _not_send: PhantomData<*const ()>,
}

impl ContextGuard {
    const fn inert() -> Self {
        Self {
            restore: None,
            _not_send: PhantomData,
        }
    }
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        if let Some((prev, budget)) = self.restore.take() {
            CURRENT.with(|c| *c.borrow_mut() = prev);
            SCOPE_SPANS.with(|s| s.set(budget));
        }
    }
}

/// Make `ctx` the current context on this thread (a new local scope with a
/// fresh span budget) until the guard drops. Used to continue a trace on a
/// thread other than the one that started it. `None` is a no-op.
pub fn enter_context(ctx: Option<SpanContext>) -> ContextGuard {
    if !enabled() {
        return ContextGuard::inert();
    }
    let Some(ctx) = ctx else {
        return ContextGuard::inert();
    };
    let prev = CURRENT.with(|c| c.borrow_mut().replace(ctx));
    let budget = SCOPE_SPANS.with(|s| s.replace(0));
    ContextGuard {
        restore: Some((prev, budget)),
        _not_send: PhantomData,
    }
}

/// A span opened on the current thread and current until dropped.
#[must_use = "a span ends when it is dropped"]
pub struct Span {
    // Field order matters: the guard restores the previous context before
    // the span is exported.
    _guard: ContextGuard,
    inner: DetachedSpan,
}

impl Span {
    /// A span that records nothing.
    pub const fn none() -> Self {
        Self {
            _guard: ContextGuard::inert(),
            inner: DetachedSpan::none(),
        }
    }

    pub fn context(&self) -> Option<&SpanContext> {
        self.inner.context()
    }

    pub fn is_recording(&self) -> bool {
        self.inner.is_recording()
    }

    pub fn set_attr(&mut self, key: &'static str, value: impl Into<AttrValue>) {
        self.inner.set_attr(key, value);
    }

    pub fn set_error(&mut self, description: &'static str) {
        self.inner.set_error(description);
    }
}

/// Where a new span hangs.
enum Parent {
    /// Child of a span of this process.
    Local(SpanContext),
    /// Child of a span of another process (`traceparent`).
    Remote(SpanContext),
    /// A new trace; `background` applies the background ratio.
    Root { background: bool },
}

fn start(
    tracer: Arc<Tracer>,
    name: Cow<'static, str>,
    kind: SpanKind,
    parent: Parent,
    links: &[SpanContext],
) -> DetachedSpan {
    let (trace_id, parent_span_id, parent_is_remote, sampled, trace_state) = match parent {
        Parent::Local(p) => {
            if !p.sampled {
                return DetachedSpan::none();
            }
            (p.trace_id, Some(p.span_id), false, true, p.trace_state)
        }
        Parent::Remote(p) => {
            let sampled = tracer.config.sampler.should_sample(Some(&p), &p.trace_id);
            (p.trace_id, Some(p.span_id), true, sampled, p.trace_state)
        }
        Parent::Root { background } => {
            let trace_id = new_trace_id();
            let mut sampled = tracer.config.sampler.should_sample(None, &trace_id);
            if sampled && background && tracer.config.background_ratio < 1.0 {
                let draw = (next_u64() >> 11) as f64 / (1u64 << 53) as f64;
                sampled = draw < tracer.config.background_ratio;
            }
            (trace_id, None, false, sampled, None)
        }
    };
    let ctx = SpanContext {
        trace_id,
        span_id: new_span_id(),
        sampled,
        trace_state,
    };
    if !sampled {
        return DetachedSpan {
            ctx: Some(ctx),
            recording: None,
        };
    }
    SPANS_STARTED.fetch_add(1, Ordering::Relaxed);
    let data = SpanData {
        trace_id: ctx.trace_id,
        span_id: ctx.span_id,
        parent_span_id,
        parent_is_remote,
        sampled: true,
        trace_state: ctx.trace_state.clone(),
        name,
        kind,
        start_unix_nanos: unix_nanos_now(),
        end_unix_nanos: 0,
        attributes: Vec::new(),
        dropped_attributes: 0,
        links: links
            .iter()
            .filter(|l| l.sampled)
            .take(MAX_LINKS)
            .cloned()
            .collect(),
        status: SpanStatus::Unset,
    };
    DetachedSpan {
        ctx: Some(ctx),
        recording: Some(Box::new(Recording {
            tracer,
            data,
            started: Instant::now(),
        })),
    }
}

/// Take one slot of the current scope's budget.
fn take_budget() -> bool {
    SCOPE_SPANS.with(|s| {
        let used = s.get();
        if used >= MAX_SPANS_PER_SCOPE {
            DROPPED_SPAN_LIMIT.fetch_add(1, Ordering::Relaxed);
            false
        } else {
            s.set(used + 1);
            true
        }
    })
}

/// Enter `span` on this thread. `new_scope` resets the span budget (local
/// roots); children share their scope's budget.
fn enter_span(inner: DetachedSpan, new_scope: bool) -> Span {
    let Some(ctx) = inner.ctx.clone() else {
        return Span {
            _guard: ContextGuard::inert(),
            inner,
        };
    };
    let prev = CURRENT.with(|c| c.borrow_mut().replace(ctx));
    let budget = SCOPE_SPANS.with(|s| s.get());
    if new_scope {
        SCOPE_SPANS.with(|s| s.set(0));
    }
    Span {
        _guard: ContextGuard {
            restore: Some((prev, budget)),
            _not_send: PhantomData,
        },
        inner,
    }
}

fn child(name: &'static str, kind: SpanKind) -> Span {
    if !enabled() {
        return Span::none();
    }
    let Some(parent) = CURRENT.with(|c| c.borrow().clone()) else {
        return Span::none();
    };
    if !parent.sampled || !take_budget() {
        return Span::none();
    }
    let Some(tracer) = tracer() else {
        return Span::none();
    };
    enter_span(
        start(
            tracer,
            Cow::Borrowed(name),
            kind,
            Parent::Local(parent),
            &[],
        ),
        false,
    )
}

/// An internal child of the current span; a no-op without one.
pub fn span(name: &'static str) -> Span {
    child(name, SpanKind::Internal)
}

/// A client child of the current span, for an outgoing request (inject
/// [`current_traceparent`] while it is open); a no-op without a current span.
pub fn client_span(name: &'static str) -> Span {
    child(name, SpanKind::Client)
}

/// The server span of one incoming request: a child of the remote parent in
/// `traceparent` when it is valid, otherwise the root of a new trace. Starts
/// a new local scope.
pub fn server_span(
    name: impl Into<Cow<'static, str>>,
    traceparent: Option<&str>,
    tracestate: Option<&str>,
) -> Span {
    if !enabled() {
        return Span::none();
    }
    let Some(tracer) = tracer() else {
        return Span::none();
    };
    let parent = match remote_context(traceparent, tracestate) {
        Some(remote) => Parent::Remote(remote),
        None => Parent::Root { background: false },
    };
    enter_span(
        start(tracer, name.into(), SpanKind::Server, parent, &[]),
        true,
    )
}

/// A span for background work (a replication poll, a heartbeat): a child of
/// the current span when there is one, otherwise the root of a new trace
/// subject to the sampler and the background ratio. Starts a new local
/// scope.
pub fn background_root(name: &'static str) -> Span {
    if !enabled() {
        return Span::none();
    }
    match CURRENT.with(|c| c.borrow().clone()) {
        Some(_) => child(name, SpanKind::Internal),
        None => {
            let Some(tracer) = tracer() else {
                return Span::none();
            };
            enter_span(
                start(
                    tracer,
                    Cow::Borrowed(name),
                    SpanKind::Internal,
                    Parent::Root { background: true },
                    &[],
                ),
                true,
            )
        }
    }
}

/// A detached child of `parent` (no-op when `parent` is `None` or not
/// sampled), linked to `links` (sampled ones only, at most [`MAX_LINKS`]).
pub fn detached_child(
    name: &'static str,
    parent: Option<&SpanContext>,
    links: &[SpanContext],
) -> DetachedSpan {
    if !enabled() {
        return DetachedSpan::none();
    }
    let Some(parent) = parent.filter(|p| p.sampled) else {
        return DetachedSpan::none();
    };
    let Some(tracer) = tracer() else {
        return DetachedSpan::none();
    };
    start(
        tracer,
        Cow::Borrowed(name),
        SpanKind::Internal,
        Parent::Local(parent.clone()),
        links,
    )
}

/// A detached span for work that may outlive the current request: a child of
/// the current span when there is one, otherwise a new background root.
pub fn detached_root_or_child(name: &'static str) -> DetachedSpan {
    if !enabled() {
        return DetachedSpan::none();
    }
    let Some(tracer) = tracer() else {
        return DetachedSpan::none();
    };
    let parent = match CURRENT.with(|c| c.borrow().clone()) {
        Some(p) if !p.sampled => return DetachedSpan::none(),
        Some(p) => Parent::Local(p),
        None => Parent::Root { background: true },
    };
    start(tracer, Cow::Borrowed(name), SpanKind::Internal, parent, &[])
}

// ---------------------------------------------------------------------------
// Test support
// ---------------------------------------------------------------------------

/// A sink that keeps every span in memory (tests and examples).
#[derive(Default)]
pub struct CollectingSink {
    spans: std::sync::Mutex<Vec<SpanData>>,
}

impl CollectingSink {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Spans of `trace_id` exported so far.
    pub fn spans_of(&self, trace_id: &TraceId) -> Vec<SpanData> {
        self.spans
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|s| s.trace_id == *trace_id)
            .cloned()
            .collect()
    }

    /// Every span exported so far.
    pub fn all(&self) -> Vec<SpanData> {
        self.spans.lock().unwrap_or_else(|p| p.into_inner()).clone()
    }
}

impl SpanSink for CollectingSink {
    fn export(&self, span: SpanData) {
        self.spans
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(span);
    }
}

/// Unit tests of this crate share one process-wide tracer (always
/// sampling) and tell their spans apart by trace id.
#[cfg(test)]
pub(crate) fn test_sink() -> &'static Arc<CollectingSink> {
    static SINK: std::sync::OnceLock<Arc<CollectingSink>> = std::sync::OnceLock::new();
    SINK.get_or_init(|| {
        let sink = CollectingSink::new();
        install(TracerConfig::default(), sink.clone());
        sink
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sink() -> &'static Arc<CollectingSink> {
        test_sink()
    }

    const VALID: &str = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    #[test]
    fn traceparent_round_trips() {
        let ctx = parse_traceparent(VALID).unwrap();
        assert_eq!(ctx.trace_id.to_hex(), "4bf92f3577b34da6a3ce929d0e0e4736");
        assert_eq!(ctx.span_id.to_hex(), "00f067aa0ba902b7");
        assert!(ctx.sampled);
        assert_eq!(ctx.traceparent(), VALID);
        let unsampled = parse_traceparent(&VALID.replace("-01", "-00")).unwrap();
        assert!(!unsampled.sampled);
        // Other flag bits are ignored for the sampling decision.
        assert!(
            parse_traceparent(&VALID.replace("-01", "-03"))
                .unwrap()
                .sampled
        );
        // Surrounding optional whitespace is accepted.
        assert!(parse_traceparent(&format!("  {VALID}\t")).is_some());
    }

    #[test]
    fn malformed_traceparents_are_rejected() {
        let bad = [
            "",
            "garbage",
            // upper case
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
            // forbidden version
            "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            // zero ids
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",
            // short id, wrong separators, non-hex
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01",
            "00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902bz-01",
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0g",
            // version 00 with trailing data
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            // future version, trailing data without a dash
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01x",
        ];
        for value in bad {
            assert!(parse_traceparent(value).is_none(), "{value}");
        }
        // A future version with extra fields is accepted (first 55 chars).
        let future = "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-what-the-future";
        assert!(parse_traceparent(future).is_some());
    }

    #[test]
    fn tracestate_is_kept_only_when_well_formed() {
        assert_eq!(
            sanitize_tracestate("vendor=abc,other=1").as_deref(),
            Some("vendor=abc,other=1")
        );
        assert!(sanitize_tracestate("").is_none());
        assert!(sanitize_tracestate("novalue").is_none());
        assert!(sanitize_tracestate("bad=\u{7f}").is_none());
        assert!(sanitize_tracestate(&format!("a={}", "x".repeat(600))).is_none());
        let many: Vec<String> = (0..33).map(|i| format!("k{i}=v")).collect();
        assert!(sanitize_tracestate(&many.join(",")).is_none());
        // tracestate without a valid traceparent is ignored.
        assert!(remote_context(None, Some("a=b")).is_none());
        let ctx = remote_context(Some(VALID), Some("a=b")).unwrap();
        assert_eq!(ctx.trace_state.as_deref(), Some("a=b"));
    }

    #[test]
    fn sampler_names_and_ratios_parse() {
        assert_eq!(
            Sampler::from_config(None, None).unwrap(),
            Sampler::default()
        );
        assert_eq!(
            Sampler::from_config(Some("always_off"), None).unwrap(),
            Sampler::AlwaysOff
        );
        assert_eq!(
            Sampler::from_config(Some("TraceIdRatio"), Some("0.25")).unwrap(),
            Sampler::TraceIdRatio(0.25)
        );
        assert_eq!(
            Sampler::from_config(Some("parentbased_traceidratio"), None).unwrap(),
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatio(1.0)))
        );
        assert_eq!(
            Sampler::from_config(Some("parentbased_always_off"), None).unwrap(),
            Sampler::ParentBased(Box::new(Sampler::AlwaysOff))
        );
        assert!(Sampler::from_config(Some("traceidratio"), Some("1.5")).is_err());
        assert!(Sampler::from_config(Some("traceidratio"), Some("half")).is_err());
        assert!(Sampler::from_config(Some("jaeger_remote"), None).is_err());
    }

    #[test]
    fn sampler_decisions_follow_parent_and_ratio() {
        let parent_on = parse_traceparent(VALID).unwrap();
        let parent_off = parse_traceparent(&VALID.replace("-01", "-00")).unwrap();
        let id = parent_on.trace_id;
        let pb_off = Sampler::ParentBased(Box::new(Sampler::AlwaysOff));
        assert!(pb_off.should_sample(Some(&parent_on), &id));
        assert!(!pb_off.should_sample(None, &id));
        let pb_on = Sampler::default();
        assert!(!pb_on.should_sample(Some(&parent_off), &id));
        assert!(pb_on.should_sample(None, &id));
        assert!(Sampler::AlwaysOn.should_sample(Some(&parent_off), &id));

        assert!(!Sampler::TraceIdRatio(0.0).should_sample(None, &id));
        assert!(Sampler::TraceIdRatio(1.0).should_sample(None, &id));
        // The decision is a pure function of the trace id.
        let low = TraceId([0x01; 16]);
        assert!(Sampler::TraceIdRatio(0.5).should_sample(None, &low));
        let high = TraceId([0xff; 16]);
        assert!(!Sampler::TraceIdRatio(0.5).should_sample(None, &high));
        // Roughly the configured fraction of random ids is admitted.
        let admitted = (0..20_000)
            .filter(|_| Sampler::TraceIdRatio(0.25).should_sample(None, &new_trace_id()))
            .count();
        assert!((4_000..6_000).contains(&admitted), "{admitted}");
    }

    #[test]
    fn spans_nest_on_one_thread_and_propagate() {
        let sink = sink();
        let parent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        let (trace_id, server_id, child_id, client_header);
        {
            let mut server = server_span("GET unit", Some(parent), Some("v=1"));
            server.set_attr("http.response.status_code", 200u16);
            server_id = server.context().unwrap().span_id;
            trace_id = server.context().unwrap().trace_id;
            {
                let child = span("unit.child");
                child_id = child.context().unwrap().span_id;
                let _client = client_span("unit.client");
                client_header = current_traceparent().unwrap();
            }
            assert_eq!(current().unwrap().span_id, server_id);
        }
        assert!(current().is_none());
        let spans = sink.spans_of(&trace_id);
        assert_eq!(spans.len(), 3, "{spans:?}");
        let by_name = |n: &str| spans.iter().find(|s| s.name == n).unwrap().clone();
        let server = by_name("GET unit");
        assert_eq!(server.kind, SpanKind::Server);
        assert_eq!(server.parent_span_id.unwrap().to_hex(), "b7ad6b7169203331");
        assert!(server.parent_is_remote);
        assert_eq!(server.trace_state.as_deref(), Some("v=1"));
        assert_eq!(trace_id.to_hex(), "0af7651916cd43dd8448eb211c80319c");
        let child = by_name("unit.child");
        assert_eq!(child.parent_span_id, Some(server_id));
        assert!(!child.parent_is_remote);
        let client = by_name("unit.client");
        assert_eq!(client.kind, SpanKind::Client);
        assert_eq!(client.parent_span_id, Some(child_id));
        assert_eq!(
            client_header,
            format_traceparent(&trace_id, &client.span_id, true)
        );
        assert!(server.end_unix_nanos >= server.start_unix_nanos);
        assert!(server.end_unix_nanos >= client.end_unix_nanos);
    }

    #[test]
    fn unsampled_parents_propagate_without_recording() {
        let sink = sink();
        let parent = "00-1af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00";
        let header;
        {
            let server = server_span("GET unsampled", Some(parent), None);
            assert!(!server.is_recording());
            let child = span("unit.child");
            assert!(!child.is_recording());
            header = current_traceparent().unwrap();
        }
        assert!(header.ends_with("-00"), "{header}");
        assert!(header.starts_with("00-1af7651916cd43dd8448eb211c80319c-"));
        let id = TraceId::from_hex("1af7651916cd43dd8448eb211c80319c").unwrap();
        assert!(sink.spans_of(&id).is_empty());
    }

    #[test]
    fn children_without_a_current_span_are_no_ops() {
        sink();
        let s = span("orphan");
        assert!(s.context().is_none());
        assert!(current().is_none());
        assert!(detached_child("x", None, &[]).context().is_none());
    }

    #[test]
    fn scope_budget_caps_children_and_resets_per_scope() {
        let sink = sink();
        let before = spans_dropped_span_limit_total();
        let trace_id;
        {
            let server = server_span("GET budget", None, None);
            trace_id = server.context().unwrap().trace_id;
            for _ in 0..(MAX_SPANS_PER_SCOPE + 10) {
                let _s = span("unit.loop");
            }
        }
        let spans = sink.spans_of(&trace_id);
        assert_eq!(spans.len(), MAX_SPANS_PER_SCOPE as usize + 1);
        assert!(spans_dropped_span_limit_total() >= before + 10);
        // A new scope gets a fresh budget.
        let server = server_span("GET budget-2", None, None);
        assert!(span("unit.after").is_recording());
        drop(server);
    }

    #[test]
    fn detached_spans_continue_a_trace_on_another_thread_with_links() {
        let sink = sink();
        let (trace_id, root_id, other) = {
            let root = server_span("POST detached", None, None);
            let other = server_span("POST other", None, None);
            let other_ctx = other.context().unwrap().clone();
            drop(other);
            let ctx = root.context().unwrap().clone();
            let links = vec![other_ctx.clone()];
            let ctx_for_thread = ctx.clone();
            std::thread::spawn(move || {
                let mut batch = detached_child("unit.batch", Some(&ctx_for_thread), &links);
                batch.set_attr("dash.batch.entries", 2usize);
                let _entered = batch.enter();
                let _fsync = span("unit.fsync");
            })
            .join()
            .unwrap();
            (ctx.trace_id, ctx.span_id, other_ctx)
        };
        let spans = sink.spans_of(&trace_id);
        let batch = spans.iter().find(|s| s.name == "unit.batch").unwrap();
        assert_eq!(batch.parent_span_id, Some(root_id));
        assert_eq!(batch.links, vec![other]);
        let fsync = spans.iter().find(|s| s.name == "unit.fsync").unwrap();
        assert_eq!(fsync.parent_span_id, Some(batch.span_id));
    }

    #[test]
    fn attributes_are_capped_and_overwritten_in_place() {
        let sink = sink();
        let trace_id;
        {
            let mut server = server_span("GET attrs", None, None);
            trace_id = server.context().unwrap().trace_id;
            const KEYS: [&str; 20] = [
                "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "b0", "b1", "b2", "b3",
                "b4", "b5", "b6", "b7", "b8", "b9",
            ];
            for key in KEYS {
                server.set_attr(key, 1i64);
            }
            server.set_attr("a0", "replaced");
            server.set_error("failed");
        }
        let span = &sink.spans_of(&trace_id)[0];
        assert_eq!(span.attributes.len(), MAX_ATTRIBUTES);
        assert_eq!(span.dropped_attributes, 4);
        assert_eq!(span.attributes[0].1, AttrValue::from("replaced"));
        assert_eq!(span.status, SpanStatus::Error("failed"));
    }

    #[test]
    fn generated_ids_are_unique_and_valid() {
        let ids: std::collections::HashSet<_> = (0..10_000).map(|_| new_span_id()).collect();
        assert_eq!(ids.len(), 10_000);
        assert!(new_trace_id().is_valid());
    }
}
