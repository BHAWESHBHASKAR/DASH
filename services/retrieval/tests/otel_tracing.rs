//! Distributed tracing end to end: a real ingestion leader, a retrieval
//! follower and a retrieval server export spans over OTLP/HTTP to an
//! in-process mock collector.
//!
//! Checks that
//! * an incoming `traceparent` is continued by the ingestion server span and
//!   the WAL spans of the write hang under it;
//! * a replication poll on the follower starts a trace whose pull request
//!   carries `traceparent`, so the leader's server span is the child of the
//!   follower's client span (propagation across an internal call), and the
//!   apply step is part of the same trace;
//! * a retrieve request has its planning and scoring stages as children of
//!   its server span;
//! * no span carries query text, claim text or the tenant id.
//!
//! Every assertion runs after `force_flush`, so the result does not depend
//! on the batch schedule.

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use dash_observe::otlp::testing::{DecodedSpan, MockCollector};
use dash_observe::otlp::{Exporter, TRACES_PATH, TracingConfig};
use ingestion::transport::{IngestionRuntime, serve_http_with_workers as serve_ingestion};
use retrieval::replication::{ReplicationFollowerConfig, start_follower};
use retrieval::transport::serve_http_with_workers as serve_retrieval;
use store::{CheckpointPolicy, FileWal, InMemoryStore};

const TOKEN: &str = "otel-replication-token-0123456789";
const TENANT: &str = "tenant-otel-private";
const QUERY_WORD: &str = "zebracorn";

const SERVER: u64 = 2;
const CLIENT: u64 = 3;

fn free_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind probe listener");
    format!("127.0.0.1:{}", listener.local_addr().expect("addr").port())
}

fn http(
    addr: &str,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> (u16, String) {
    let mut stream = TcpStream::connect(addr).expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("timeout");
    let mut request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body.len()
    );
    for (name, value) in headers {
        request.push_str(&format!("{name}: {value}\r\n"));
    }
    request.push_str("\r\n");
    request.push_str(body);
    stream.write_all(request.as_bytes()).expect("write");
    let mut raw = Vec::new();
    let _ = stream.read_to_end(&mut raw);
    let text = String::from_utf8_lossy(&raw).to_string();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse().ok())
        .unwrap_or(0);
    (status, text)
}

fn wait_until(what: &str, timeout: Duration, mut cond: impl FnMut() -> bool) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if cond() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("timed out waiting for: {what}");
}

struct Server {
    shutdown: Arc<dash_common::ShutdownSignal>,
    thread: Option<JoinHandle<()>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.shutdown.trigger();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn start_server(
    addr: &str,
    run: impl FnOnce(String, Arc<dash_common::ShutdownSignal>) + Send + 'static,
) -> Server {
    let shutdown = dash_common::ShutdownSignal::manual();
    let thread = {
        let addr = addr.to_string();
        let shutdown = Arc::clone(&shutdown);
        thread::spawn(move || run(addr, shutdown))
    };
    wait_until("server accepting", Duration::from_secs(10), || {
        TcpStream::connect(addr).is_ok()
    });
    Server {
        shutdown,
        thread: Some(thread),
    }
}

fn ingest_body(claim_id: &str) -> String {
    format!(
        r#"{{"claim":{{"claim_id":"{claim_id}","tenant_id":"{TENANT}","canonical_text":"the {QUERY_WORD} statement {claim_id}","confidence":0.9}},"evidence":[{{"evidence_id":"ev-{claim_id}","claim_id":"{claim_id}","source_id":"source://{claim_id}","stance":"supports","source_quality":0.9}}]}}"#
    )
}

/// Spans by id, plus helpers to walk parents.
struct Spans(Vec<DecodedSpan>);

impl Spans {
    fn of_trace(&self, trace_id: &str) -> Vec<&DecodedSpan> {
        self.0.iter().filter(|s| s.trace_id == trace_id).collect()
    }

    fn by_id(&self, span_id: &str) -> Option<&DecodedSpan> {
        self.0.iter().find(|s| s.span_id == span_id)
    }

    /// True when `span` descends from `ancestor` (same trace, parent chain).
    fn descends_from(&self, span: &DecodedSpan, ancestor: &str) -> bool {
        let mut current = span;
        for _ in 0..64 {
            if current.parent_span_id == ancestor {
                return true;
            }
            match self.by_id(&current.parent_span_id) {
                Some(parent) => current = parent,
                None => return false,
            }
        }
        false
    }
}

#[test]
fn spans_link_across_services_and_internal_calls() {
    // SAFETY: set before any thread of this test binary reads them (this
    // file holds a single test).
    #[allow(unused_unsafe)]
    unsafe {
        std::env::set_var("DASH_INSECURE_DEV_MODE", "1");
        std::env::set_var("DASH_STRICT_SECRETS", "0");
        std::env::set_var("DASH_INGEST_REPLICATION_TOKEN", TOKEN);
    }
    let collector = MockCollector::start();
    let lookup = {
        let endpoint = format!("{}{TRACES_PATH}", collector.base_url());
        move |name: &str| -> Option<String> {
            match name {
                "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT" => Some(endpoint.clone()),
                "OTEL_EXPORTER_OTLP_HEADERS" => Some("x-collector-tenant=dash-tests".to_string()),
                "OTEL_TRACES_SAMPLER" => Some("parentbased_always_on".to_string()),
                // A long schedule: only force_flush exports.
                "OTEL_BSP_SCHEDULE_DELAY" => Some("600000".to_string()),
                "OTEL_BSP_MAX_QUEUE_SIZE" => Some("65536".to_string()),
                _ => None,
            }
        }
    };
    let settings = TracingConfig::from_lookup(&lookup, "dash-otel-test");
    assert!(settings.warnings.is_empty(), "{:?}", settings.warnings);
    let exporter = Exporter::install(&settings.config.expect("tracing enabled")).expect("exporter");
    let flush = || assert!(exporter.force_flush(Duration::from_secs(10)), "flush");

    // --- ingestion leader, write with an incoming traceparent ---------------
    let dir = tempfile::tempdir().expect("tempdir");
    let wal = FileWal::open_with_sync_every_records(dir.path().join("leader.wal"), 1)
        .expect("open leader wal");
    let store = InMemoryStore::load_from_wal(&wal).expect("load leader wal");
    let runtime = IngestionRuntime::persistent(store, wal, CheckpointPolicy::default());
    let leader_addr = free_addr();
    let _leader = start_server(&leader_addr, move |addr, shutdown| {
        serve_ingestion(runtime, &addr, 2, shutdown).expect("leader server");
    });

    let ingest_trace = "4bf92f3577b34da6a3ce929d0e0e4736";
    let caller_span = "00f067aa0ba902b7";
    let traceparent = format!("00-{ingest_trace}-{caller_span}-01");
    let (status, text) = http(
        &leader_addr,
        "POST",
        "/v1/ingest",
        &[
            ("traceparent", &traceparent),
            ("tracestate", "vendor=1"),
            ("x-request-id", "rid-otel-1"),
        ],
        &ingest_body("c1"),
    );
    assert_eq!(status, 200, "{text}");
    flush();
    let spans = Spans(collector.spans());
    let trace = spans.of_trace(ingest_trace);
    let server = trace
        .iter()
        .find(|s| s.name == "POST ingest")
        .unwrap_or_else(|| panic!("no ingest server span in {trace:#?}"));
    assert_eq!(server.kind, SERVER);
    assert_eq!(server.parent_span_id, caller_span);
    assert_eq!(server.trace_state, "vendor=1");
    assert_eq!(server.flags & 0x301, 0x301, "sampled, parent is remote");
    assert_eq!(server.attr("http.route"), Some("ingest"));
    assert_eq!(server.attr("http.response.status_code"), Some("200"));
    assert_eq!(server.attr("dash.request_id"), Some("rid-otel-1"));
    let fsync = trace
        .iter()
        .find(|s| s.name == "wal.fsync")
        .unwrap_or_else(|| panic!("no wal.fsync span in {trace:#?}"));
    assert!(spans.descends_from(fsync, &server.span_id), "{fsync:#?}");
    for span in &trace {
        if span.span_id != server.span_id {
            assert!(spans.descends_from(span, &server.span_id), "{span:#?}");
        }
    }
    let export = &collector.exports()[0];
    assert_eq!(export.path, TRACES_PATH);
    assert_eq!(export.headers["x-collector-tenant"], "dash-tests");
    assert_eq!(export.request.resource["service.name"], "dash-otel-test");

    // --- retrieval follower: poll -> pull (client) -> leader (server) -------
    let mut config = ReplicationFollowerConfig::new(format!("http://{leader_addr}"));
    config.poll_interval = Duration::from_millis(20);
    config.max_backoff = Duration::from_millis(200);
    config.token = Some(TOKEN.to_string());
    let follower_store = Arc::new(RwLock::new(InMemoryStore::new()));
    let follower = start_follower(Arc::clone(&follower_store), config, None);
    let replicated = |id: &str| {
        follower_store
            .read()
            .unwrap()
            .claims_for_tenant(TENANT)
            .iter()
            .any(|c| c.claim_id == id)
    };
    wait_until("initial sync", Duration::from_secs(10), || replicated("c1"));
    // A delta after the initial resync goes through the WAL pull and apply.
    let (status, text) = http(&leader_addr, "POST", "/v1/ingest", &[], &ingest_body("c2"));
    assert_eq!(status, 200, "{text}");
    wait_until("delta sync", Duration::from_secs(10), || replicated("c2"));

    let mut found = None;
    wait_until("replication trace", Duration::from_secs(10), || {
        flush();
        let spans = Spans(collector.spans());
        let apply = spans.0.iter().find(|s| {
            s.name == "replication.apply"
                && spans
                    .by_id(&s.parent_span_id)
                    .is_some_and(|p| p.name == "replication.poll")
        });
        let Some(apply) = apply else {
            return false;
        };
        let poll_id = apply.parent_span_id.clone();
        let trace = spans.of_trace(&apply.trace_id);
        let client = trace
            .iter()
            .find(|s| s.name == "replication.http_request" && s.parent_span_id == poll_id);
        let leader_side = client.and_then(|client| {
            trace
                .iter()
                .find(|s| s.parent_span_id == client.span_id)
                .map(|server| ((*client).clone(), (*server).clone()))
        });
        match leader_side {
            Some((client, server)) => {
                found = Some((spans.by_id(&poll_id).cloned().unwrap(), client, server));
                true
            }
            None => false,
        }
    });
    let (poll, client, leader_server) = found.unwrap();
    assert!(
        poll.parent_span_id.is_empty(),
        "the poll starts its own trace"
    );
    assert_eq!(poll.attr("dash.component"), Some("retrieval"));
    assert_eq!(client.kind, CLIENT);
    assert_eq!(client.attr("http.request.method"), Some("GET"));
    assert_eq!(client.attr("http.response.status_code"), Some("200"));
    assert_eq!(leader_server.kind, SERVER);
    assert_eq!(leader_server.name, "GET replication_wal");
    assert_eq!(leader_server.trace_id, poll.trace_id);
    assert_eq!(leader_server.flags & 0x300, 0x300, "parent is remote");
    follower.stop();

    // --- retrieve request: planning and scoring stages ----------------------
    let retrieval_addr = free_addr();
    let retrieval_store = Arc::clone(&follower_store);
    let _retrieval = start_server(&retrieval_addr, move |addr, shutdown| {
        serve_retrieval(retrieval_store, &addr, 2, shutdown).expect("retrieval server");
    });
    let retrieve_trace = "5bf92f3577b34da6a3ce929d0e0e4736";
    let traceparent = format!("00-{retrieve_trace}-{caller_span}-01");
    let (status, text) = http(
        &retrieval_addr,
        "GET",
        &format!("/v1/retrieve?tenant_id={TENANT}&query={QUERY_WORD}+statement&top_k=3"),
        &[("traceparent", &traceparent)],
        "",
    );
    assert_eq!(status, 200, "{text}");
    flush();
    let spans = Spans(collector.spans());
    let trace = spans.of_trace(retrieve_trace);
    let server = trace
        .iter()
        .find(|s| s.name == "GET retrieve")
        .unwrap_or_else(|| panic!("no retrieve server span in {trace:#?}"));
    assert_eq!(server.parent_span_id, caller_span);
    let names: HashSet<&str> = trace.iter().map(|s| s.name.as_str()).collect();
    for stage in [
        "retrieve.plan",
        "retrieve.candidates",
        "retrieve.bm25",
        "retrieve.fuse",
    ] {
        assert!(names.contains(stage), "{stage} missing from {names:?}");
    }
    for span in &trace {
        if span.span_id != server.span_id {
            assert!(spans.descends_from(span, &server.span_id), "{span:#?}");
        }
    }
    let fuse = trace.iter().find(|s| s.name == "retrieve.fuse").unwrap();
    assert_eq!(fuse.attr("dash.retrieve.results"), Some("2"));

    // --- nothing sensitive in any span ---------------------------------------
    let all = collector.spans();
    let mut per_trace: HashMap<&str, usize> = HashMap::new();
    for span in &all {
        *per_trace.entry(span.trace_id.as_str()).or_default() += 1;
        for (name, value) in span
            .attributes
            .iter()
            .chain(std::iter::once((&span.name, &span.name)))
        {
            for forbidden in [QUERY_WORD, TENANT, TOKEN, "statement"] {
                assert!(
                    !value.contains(forbidden) && !name.contains(forbidden),
                    "span {} carries '{forbidden}': {name}={value}",
                    span.name
                );
            }
        }
    }
    let largest = per_trace.values().max().copied().unwrap_or(0);
    assert!(
        largest <= dash_observe::trace::MAX_SPANS_PER_SCOPE as usize + 8,
        "{largest} spans in one trace"
    );
    drop(exporter);
}
