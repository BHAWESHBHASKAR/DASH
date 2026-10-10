//! Process-wide embedding provider metrics.
//!
//! [`InstrumentedProvider`] wraps the network providers built by
//! [`crate::with_resilience`] and records every call: latency (histogram),
//! outcome and error kind (counters). The circuit breaker of each provider is
//! registered so its state can be rendered as a gauge. Labels are bounded:
//! `provider` is one of `hash`, `ollama`, `openai`, `other`, and `kind` is one
//! of the fixed error kinds below.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::Instant;

use dash_observe::{Histogram, LATENCY_SECONDS_BUCKETS, MetricKind, MetricsWriter};

use crate::{CircuitBreaker, CircuitState, EmbeddingError, EmbeddingProvider};

const PROVIDERS: [&str; 4] = ["hash", "ollama", "openai", "other"];

const ERROR_KINDS: [&str; 11] = [
    "io",
    "timeout",
    "http_4xx",
    "http_429",
    "http_5xx",
    "parse",
    "dimension_mismatch",
    "response_too_large",
    "circuit_open",
    "overloaded",
    "invalid_config",
];

fn provider_index(name: &str) -> usize {
    let lower = name.to_ascii_lowercase();
    PROVIDERS
        .iter()
        .take(3)
        .position(|p| lower.contains(p))
        .unwrap_or(3)
}

fn error_kind_index(err: &EmbeddingError) -> usize {
    match err {
        EmbeddingError::Io(_) => 0,
        EmbeddingError::Timeout(_) => 1,
        EmbeddingError::Http { status: 429, .. } => 3,
        EmbeddingError::Http { status, .. } if *status >= 500 => 4,
        EmbeddingError::Http { .. } => 2,
        EmbeddingError::Parse(_) => 5,
        EmbeddingError::DimensionMismatch { .. } => 6,
        EmbeddingError::ResponseTooLarge { .. } => 7,
        EmbeddingError::CircuitOpen { .. } => 8,
        EmbeddingError::Overloaded => 9,
        EmbeddingError::InvalidConfig(_) => 10,
    }
}

struct ProviderMetrics {
    latency: Histogram,
    requests_total: AtomicU64,
    texts_total: AtomicU64,
    errors: [AtomicU64; ERROR_KINDS.len()],
}

impl ProviderMetrics {
    fn new() -> Self {
        Self {
            latency: Histogram::new(LATENCY_SECONDS_BUCKETS),
            requests_total: AtomicU64::new(0),
            texts_total: AtomicU64::new(0),
            errors: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
}

static METRICS: LazyLock<[ProviderMetrics; PROVIDERS.len()]> =
    LazyLock::new(|| std::array::from_fn(|_| ProviderMetrics::new()));

/// Latest breaker per provider (a provider rebuilt after a configuration
/// change replaces the previous registration).
static BREAKERS: LazyLock<Mutex<[Option<Weak<CircuitBreaker>>; PROVIDERS.len()]>> =
    LazyLock::new(|| Mutex::new(std::array::from_fn(|_| None)));

pub(crate) fn register_breaker(provider: &str, breaker: &Arc<CircuitBreaker>) {
    let mut slots = BREAKERS.lock().unwrap_or_else(|p| p.into_inner());
    slots[provider_index(provider)] = Some(Arc::downgrade(breaker));
}

/// Record one provider call.
pub fn observe_call(provider: &str, texts: usize, seconds: f64, error: Option<&EmbeddingError>) {
    let m = &METRICS[provider_index(provider)];
    m.requests_total.fetch_add(1, Ordering::Relaxed);
    m.texts_total.fetch_add(texts as u64, Ordering::Relaxed);
    m.latency.observe(seconds);
    if let Some(err) = error {
        m.errors[error_kind_index(err)].fetch_add(1, Ordering::Relaxed);
    }
}

/// Wraps a provider and records every call in the process-wide metrics.
pub struct InstrumentedProvider<P: EmbeddingProvider> {
    inner: P,
}

impl<P: EmbeddingProvider> InstrumentedProvider<P> {
    pub fn new(inner: P) -> Self {
        Self { inner }
    }
}

impl<P: EmbeddingProvider> EmbeddingProvider for InstrumentedProvider<P> {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn dimensions(&self) -> usize {
        self.inner.dimensions()
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
        let started = Instant::now();
        let result = self.inner.embed(texts);
        observe_call(
            self.inner.name(),
            texts.len(),
            started.elapsed().as_secs_f64(),
            result.as_ref().err(),
        );
        result
    }
}

/// Render the embedding families into `w`. Providers that were never called
/// (and have no breaker) are omitted, so a hash-only deployment shows no
/// network-provider series.
pub fn render_into(w: &mut MetricsWriter) {
    let breakers: Vec<(usize, Arc<CircuitBreaker>)> = {
        let slots = BREAKERS.lock().unwrap_or_else(|p| p.into_inner());
        slots
            .iter()
            .enumerate()
            .filter_map(|(i, slot)| slot.as_ref()?.upgrade().map(|b| (i, b)))
            .collect()
    };
    let active: Vec<usize> = (0..PROVIDERS.len())
        .filter(|i| {
            METRICS[*i].requests_total.load(Ordering::Relaxed) > 0
                || breakers.iter().any(|(b, _)| b == i)
        })
        .collect();

    w.header(
        "dash_embedding_requests_total",
        "Embedding provider calls (each call may embed several texts).",
        MetricKind::Counter,
    );
    for i in &active {
        w.sample(
            "dash_embedding_requests_total",
            &[("provider", PROVIDERS[*i])],
            METRICS[*i].requests_total.load(Ordering::Relaxed) as f64,
        );
    }
    w.header(
        "dash_embedding_texts_total",
        "Texts sent to the embedding provider.",
        MetricKind::Counter,
    );
    for i in &active {
        w.sample(
            "dash_embedding_texts_total",
            &[("provider", PROVIDERS[*i])],
            METRICS[*i].texts_total.load(Ordering::Relaxed) as f64,
        );
    }
    w.header(
        "dash_embedding_errors_total",
        "Failed embedding provider calls by error kind.",
        MetricKind::Counter,
    );
    for i in &active {
        for (k, kind) in ERROR_KINDS.iter().enumerate() {
            w.sample(
                "dash_embedding_errors_total",
                &[("provider", PROVIDERS[*i]), ("kind", kind)],
                METRICS[*i].errors[k].load(Ordering::Relaxed) as f64,
            );
        }
    }
    w.header(
        "dash_embedding_request_duration_seconds",
        "Embedding provider call latency in seconds, including retries and breaker rejections.",
        MetricKind::Histogram,
    );
    for i in &active {
        w.histogram_series(
            "dash_embedding_request_duration_seconds",
            &[("provider", PROVIDERS[*i])],
            &METRICS[*i].latency.snapshot(),
        );
    }
    w.header(
        "dash_embedding_breaker_state",
        "Circuit breaker state: 0 closed, 1 open, 2 half-open (a probe may be admitted).",
        MetricKind::Gauge,
    );
    for (i, breaker) in &breakers {
        let state = match breaker.state() {
            CircuitState::Closed => 0.0,
            CircuitState::Open => 1.0,
            CircuitState::HalfOpen => 2.0,
        };
        w.sample(
            "dash_embedding_breaker_state",
            &[("provider", PROVIDERS[*i])],
            state,
        );
    }
    w.header(
        "dash_embedding_breaker_consecutive_failures",
        "Consecutive upstream failures counted by the circuit breaker.",
        MetricKind::Gauge,
    );
    for (i, breaker) in &breakers {
        w.sample(
            "dash_embedding_breaker_consecutive_failures",
            &[("provider", PROVIDERS[*i])],
            f64::from(breaker.consecutive_failures()),
        );
    }
}

/// The embedding families as Prometheus text.
pub fn render_prometheus() -> String {
    let mut w = MetricsWriter::new();
    render_into(&mut w);
    w.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct Failing;
    impl EmbeddingProvider for Failing {
        fn name(&self) -> &str {
            "openai"
        }
        fn dimensions(&self) -> usize {
            4
        }
        fn embed(&self, _texts: &[String]) -> Result<Vec<Vec<f32>>, EmbeddingError> {
            Err(EmbeddingError::Http {
                status: 503,
                body: String::new(),
                retry_after_secs: None,
            })
        }
    }

    #[test]
    fn calls_errors_and_breaker_state_are_rendered_with_bounded_labels() {
        let breaker = Arc::new(CircuitBreaker::new(1, Duration::from_secs(60)));
        register_breaker("openai", &breaker);
        let provider = InstrumentedProvider::new(crate::CircuitBreakerProvider::new(
            Failing,
            Arc::clone(&breaker),
        ));
        let texts = vec!["a".to_string(), "b".to_string()];
        assert!(provider.embed(&texts).is_err());
        assert!(matches!(
            provider.embed(&texts),
            Err(EmbeddingError::CircuitOpen { .. })
        ));

        let text = render_prometheus();
        let report = dash_observe::validate(&text).unwrap_or_else(|e| panic!("{e}\n{text}"));
        let openai = [("provider", "openai")];
        assert!(
            report
                .value("dash_embedding_requests_total", &openai)
                .unwrap()
                >= 2.0
        );
        assert!(report.value("dash_embedding_texts_total", &openai).unwrap() >= 4.0);
        assert!(
            report
                .value(
                    "dash_embedding_errors_total",
                    &[("provider", "openai"), ("kind", "http_5xx")]
                )
                .unwrap()
                >= 1.0
        );
        assert!(
            report
                .value(
                    "dash_embedding_errors_total",
                    &[("provider", "openai"), ("kind", "circuit_open")]
                )
                .unwrap()
                >= 1.0
        );
        // Other tests may register their own "openai" breaker concurrently,
        // so only the family (not this breaker's sample) is asserted here.
        assert_eq!(breaker.state(), CircuitState::Open);
        assert_eq!(
            report.kind("dash_embedding_breaker_state"),
            Some(MetricKind::Gauge)
        );
        assert!(
            report
                .value("dash_embedding_request_duration_seconds_count", &openai)
                .unwrap()
                >= 2.0
        );
        assert_eq!(provider_index("my-custom-provider"), 3);
    }
}
