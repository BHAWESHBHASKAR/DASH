//! Request instrumentation for `dash-http` handlers.
//!
//! [`instrument`] wraps a service handler and, for every request:
//!
//! * counts it in `dash_http_server_requests_total{service,route,method,code}`;
//! * records its latency in the
//!   `dash_http_server_request_duration_seconds{service,route,method}`
//!   histogram;
//! * tracks `dash_http_server_requests_in_flight{service}`;
//! * runs the handler inside an `http_request` tracing span carrying the
//!   service, route, method and request id (resolved by `dash-http`), so every
//!   log event emitted while handling the request is correlated; and
//! * emits one `dash_access` event when the request completes (`debug` for
//!   non-5xx answers, `warn` for 5xx), enabled with for example
//!   `RUST_LOG=info,dash_access=debug`.
//!
//! Labels are bounded: `route` comes from the service's [`RouteClassifier`]
//! (a fixed set of names, unknown paths fold to `other`), `method` is folded
//! by [`normalize_method`], and `code` is the HTTP status the server renders
//! (a fixed set, see `dash_http::status_line`).

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Instant;

use dash_http::request_id::REQUEST_ID_HEADER;
use dash_http::{Handler, Request, Response};

use crate::exposition::{MetricKind, MetricsWriter};
use crate::histogram::{LATENCY_SECONDS_BUCKETS, LocalHistogram};

/// Maps a request method and path to a bounded route label.
pub type RouteClassifier = fn(method: &str, path: &str) -> &'static str;

/// Fold a method to a fixed label set.
pub fn normalize_method(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "PATCH" => "PATCH",
        "OPTIONS" => "OPTIONS",
        _ => "OTHER",
    }
}

/// Status code label: the code itself for the statuses the server can
/// render, `other` for anything else, so the label set stays fixed.
fn code_label(status: u16) -> u16 {
    if (100..600).contains(&status) {
        status
    } else {
        0
    }
}

#[derive(Debug)]
struct RouteSeries {
    duration: LocalHistogram,
    codes: BTreeMap<u16, u64>,
}

/// Request metrics of one service.
#[derive(Debug)]
pub struct HttpMetrics {
    service: &'static str,
    classify: RouteClassifier,
    in_flight: AtomicI64,
    series: Mutex<BTreeMap<(&'static str, &'static str), RouteSeries>>,
}

impl HttpMetrics {
    pub fn new(service: &'static str, classify: RouteClassifier) -> Self {
        Self {
            service,
            classify,
            in_flight: AtomicI64::new(0),
            series: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn service(&self) -> &'static str {
        self.service
    }

    pub fn route_of(&self, method: &str, path: &str) -> &'static str {
        (self.classify)(method, path)
    }

    pub fn in_flight(&self) -> i64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Record one completed request.
    pub fn observe(&self, route: &'static str, method: &'static str, status: u16, seconds: f64) {
        let mut series = self.series.lock().unwrap_or_else(|p| p.into_inner());
        let entry = series
            .entry((route, method))
            .or_insert_with(|| RouteSeries {
                duration: LocalHistogram::new(LATENCY_SECONDS_BUCKETS),
                codes: BTreeMap::new(),
            });
        entry.duration.observe(seconds);
        *entry.codes.entry(code_label(status)).or_insert(0) += 1;
    }

    /// Requests counted so far for `route` (all methods and codes).
    pub fn requests_for_route(&self, route: &str) -> u64 {
        let series = self.series.lock().unwrap_or_else(|p| p.into_inner());
        series
            .iter()
            .filter(|((r, _), _)| *r == route)
            .map(|(_, s)| s.codes.values().sum::<u64>())
            .sum()
    }

    pub fn render(&self, w: &mut MetricsWriter) {
        let series = self.series.lock().unwrap_or_else(|p| p.into_inner());
        w.header(
            "dash_http_server_requests_total",
            "HTTP requests handled, by route, method and status code.",
            MetricKind::Counter,
        );
        for ((route, method), s) in series.iter() {
            for (code, count) in &s.codes {
                let code = if *code == 0 {
                    "other".to_string()
                } else {
                    code.to_string()
                };
                w.sample(
                    "dash_http_server_requests_total",
                    &[
                        ("service", self.service),
                        ("route", route),
                        ("method", method),
                        ("code", &code),
                    ],
                    *count as f64,
                );
            }
        }
        w.header(
            "dash_http_server_request_duration_seconds",
            "HTTP request latency in seconds, from the parsed request to the rendered response.",
            MetricKind::Histogram,
        );
        for ((route, method), s) in series.iter() {
            w.histogram_series(
                "dash_http_server_request_duration_seconds",
                &[
                    ("service", self.service),
                    ("route", route),
                    ("method", method),
                ],
                &s.duration.snapshot(),
            );
        }
        drop(series);
        w.header(
            "dash_http_server_requests_in_flight",
            "HTTP requests currently being handled.",
            MetricKind::Gauge,
        );
        w.sample(
            "dash_http_server_requests_in_flight",
            &[("service", self.service)],
            self.in_flight().max(0) as f64,
        );
    }
}

fn registry() -> &'static Mutex<Vec<&'static HttpMetrics>> {
    static REGISTRY: OnceLock<Mutex<Vec<&'static HttpMetrics>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// The process-wide metrics of `service`, created on first use. Calling it
/// again with the same service returns the same instance (the classifier of
/// the first call is kept).
pub fn register(service: &'static str, classify: RouteClassifier) -> &'static HttpMetrics {
    let mut registry = registry().lock().unwrap_or_else(|p| p.into_inner());
    if let Some(existing) = registry.iter().find(|m| m.service == service) {
        return existing;
    }
    let metrics: &'static HttpMetrics = Box::leak(Box::new(HttpMetrics::new(service, classify)));
    registry.push(metrics);
    metrics
}

/// The metrics registered for `service`, if any.
pub fn registered(service: &str) -> Option<&'static HttpMetrics> {
    let registry = registry().lock().unwrap_or_else(|p| p.into_inner());
    registry.iter().find(|m| m.service == service).copied()
}

/// Counts a request as in flight until dropped; a request whose handler
/// unwinds is recorded as a 500.
struct InFlight<'a> {
    metrics: &'a HttpMetrics,
    route: &'static str,
    method: &'static str,
    started: Instant,
    done: bool,
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.metrics.in_flight.fetch_sub(1, Ordering::Relaxed);
        if !self.done {
            self.metrics.observe(
                self.route,
                self.method,
                500,
                self.started.elapsed().as_secs_f64(),
            );
        }
    }
}

/// Handle one request with instrumentation (see the module docs).
pub fn handle_instrumented(
    metrics: &HttpMetrics,
    request: Request,
    handler: impl FnOnce(Request) -> Response,
) -> Response {
    let method = normalize_method(&request.method);
    let route = metrics.route_of(&request.method, request.path());
    let request_id = request
        .header(REQUEST_ID_HEADER)
        .unwrap_or_default()
        .to_string();
    let span = tracing::info_span!(
        "http_request",
        service = metrics.service,
        request_id = %request_id,
        method = method,
        route = route,
    );
    let _entered = span.enter();
    metrics.in_flight.fetch_add(1, Ordering::Relaxed);
    let mut guard = InFlight {
        metrics,
        route,
        method,
        started: Instant::now(),
        done: false,
    };
    let response = handler(request);
    let elapsed = guard.started.elapsed();
    guard.done = true;
    metrics.observe(route, method, response.status, elapsed.as_secs_f64());
    drop(guard);
    let duration_ms = elapsed.as_secs_f64() * 1000.0;
    if response.status >= 500 {
        tracing::warn!(
            target: "dash_access",
            status = response.status,
            duration_ms,
            "request completed with a server error"
        );
    } else {
        tracing::debug!(
            target: "dash_access",
            status = response.status,
            duration_ms,
            "request completed"
        );
    }
    response
}

/// Wrap `handler` with [`handle_instrumented`] using the metrics of `service`.
pub fn instrument(service: &'static str, classify: RouteClassifier, handler: Handler) -> Handler {
    let metrics = register(service, classify);
    Arc::new(move |request| handle_instrumented(metrics, request, |r| handler(r)))
}

/// The common families every service exposes: its HTTP request metrics
/// (when registered), process metrics and `dash_build_info`.
pub fn render_service_metrics(service: &str, version: &str) -> String {
    let mut w = MetricsWriter::new();
    if let Some(metrics) = registered(service) {
        metrics.render(&mut w);
    }
    crate::process::render(&mut w, service, version);
    w.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exposition::validate;
    use std::collections::HashMap;

    fn classify(_method: &str, path: &str) -> &'static str {
        match path {
            "/a" => "a",
            "/boom" => "boom",
            _ => "other",
        }
    }

    fn request(method: &str, target: &str) -> Request {
        Request {
            method: method.to_string(),
            target: target.to_string(),
            headers: HashMap::from([(REQUEST_ID_HEADER.to_string(), "rid-xyz".to_string())]),
            body: Vec::new(),
            peer: None,
            tls: None,
        }
    }

    #[test]
    fn requests_are_counted_by_route_method_and_code_with_bounded_labels() {
        let metrics = HttpMetrics::new("unit", classify);
        let ok = |_r: Request| Response::json(200, "{}".into());
        let missing = |_r: Request| Response::error(404, "nope");
        handle_instrumented(&metrics, request("GET", "/a?x=1"), ok);
        handle_instrumented(&metrics, request("GET", "/a"), ok);
        handle_instrumented(&metrics, request("BREW", "/tenant/123/claim/456"), missing);
        assert_eq!(metrics.in_flight(), 0);

        let mut w = MetricsWriter::new();
        metrics.render(&mut w);
        let text = w.finish();
        let report = validate(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        let labels = |route, method, code| {
            [
                ("service", "unit"),
                ("route", route),
                ("method", method),
                ("code", code),
            ]
        };
        assert_eq!(
            report.value(
                "dash_http_server_requests_total",
                &labels("a", "GET", "200")
            ),
            Some(2.0)
        );
        assert_eq!(
            report.value(
                "dash_http_server_requests_total",
                &labels("other", "OTHER", "404")
            ),
            Some(1.0)
        );
        assert_eq!(
            report.value(
                "dash_http_server_request_duration_seconds_count",
                &[("service", "unit"), ("route", "a"), ("method", "GET")]
            ),
            Some(2.0)
        );
        assert!(
            !text.contains("123"),
            "path segments must not become labels"
        );
        assert_eq!(
            report.value(
                "dash_http_server_requests_in_flight",
                &[("service", "unit")]
            ),
            Some(0.0)
        );
    }

    #[test]
    fn a_panicking_handler_is_counted_as_500_and_leaves_no_request_in_flight() {
        let metrics = HttpMetrics::new("unit-panic", classify);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            handle_instrumented(&metrics, request("POST", "/boom"), |_r| panic!("boom"))
        }));
        assert!(result.is_err());
        assert_eq!(metrics.in_flight(), 0);
        let mut w = MetricsWriter::new();
        metrics.render(&mut w);
        let report = validate(&w.finish()).unwrap();
        assert_eq!(
            report.value(
                "dash_http_server_requests_total",
                &[
                    ("service", "unit-panic"),
                    ("route", "boom"),
                    ("method", "POST"),
                    ("code", "500")
                ]
            ),
            Some(1.0)
        );
    }

    #[test]
    fn the_handler_runs_inside_a_span_carrying_the_request_id() {
        use std::io::Write;
        #[derive(Clone, Default)]
        struct Buf(Arc<Mutex<Vec<u8>>>);
        impl Write for Buf {
            fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(data);
                Ok(data.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_max_level(tracing::Level::DEBUG)
            .with_writer(move || writer.clone())
            .finish();
        let metrics = HttpMetrics::new("unit-span", classify);
        tracing::subscriber::with_default(subscriber, || {
            handle_instrumented(&metrics, request("GET", "/a"), |_r| {
                tracing::info!("inside handler");
                Response::json(503, "{}".into())
            });
        });
        let logs = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
        let lines: Vec<&str> = logs.lines().collect();
        assert_eq!(lines.len(), 2, "{logs}");
        for line in &lines {
            assert!(line.contains("\"request_id\":\"rid-xyz\""), "{line}");
            assert!(line.contains("\"route\":\"a\""), "{line}");
        }
        assert!(
            lines[1].contains("\"target\":\"dash_access\""),
            "{}",
            lines[1]
        );
        assert!(lines[1].contains("\"status\":503"), "{}", lines[1]);
        assert!(lines[1].contains("WARN"), "{}", lines[1]);
    }

    #[test]
    fn register_returns_one_instance_per_service() {
        let a = register("unit-registry", classify);
        let b = register("unit-registry", classify);
        assert!(std::ptr::eq(a, b));
        assert!(registered("unit-registry").is_some());
        assert!(registered("unit-missing").is_none());
        a.observe("a", "GET", 200, 0.001);
        let text = render_service_metrics("unit-registry", "0.0.0");
        let report = validate(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        assert!(report.has_family("dash_http_server_requests_total"));
        assert!(report.has_family("dash_build_info"));
    }
}
