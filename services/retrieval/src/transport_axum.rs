use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{Request, Response, StatusCode, header::CONTENT_TYPE},
    response::IntoResponse,
    routing::any,
};
use store::InMemoryStore;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::transport::{
    HttpRequest, HttpResponse, TransportBackpressureMetrics, TransportMetrics,
    backpressure_rejection_response, handle_request_with_metrics, resolve_http_queue_capacity,
};

const MAX_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    store: Arc<InMemoryStore>,
    metrics: Arc<Mutex<TransportMetrics>>,
    backpressure_limiter: Arc<Semaphore>,
    backpressure_metrics: Arc<TransportBackpressureMetrics>,
}

pub fn serve_http_with_axum(
    store: Arc<InMemoryStore>,
    bind_addr: &str,
    worker_threads: usize,
) -> Result<(), String> {
    let worker_threads = worker_threads.max(1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .map_err(|e| format!("failed to build tokio runtime: {e}"))?;

    let bind_addr = bind_addr.to_string();
    runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;

        let queue_capacity = resolve_http_queue_capacity(worker_threads);
        let state = AppState {
            store,
            metrics: Arc::new(Mutex::new(TransportMetrics::default())),
            backpressure_limiter: Arc::new(Semaphore::new(queue_capacity)),
            backpressure_metrics: Arc::new(TransportBackpressureMetrics::new(queue_capacity)),
        };
        if let Ok(mut guard) = state.metrics.lock() {
            guard.set_transport_backpressure_metrics(Arc::clone(&state.backpressure_metrics));
        }

        let app = Router::new()
            .fallback(any(dispatch))
            .with_state(state)
            .layer(axum::extract::DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES));

        axum::serve(listener, app)
            .await
            .map_err(|e| format!("axum server failed: {e}"))
    })
}

async fn dispatch(State(state): State<AppState>, request: Request<Body>) -> impl IntoResponse {
    let admission = match try_acquire_backpressure_permit(&state) {
        Some(admission) => admission,
        None => return response_from_transport(backpressure_rejection_response()),
    };

    let method = request.method().to_string();
    let target = request
        .uri()
        .path_and_query()
        .map(|value| value.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());

    let mut headers = HashMap::new();
    for (name, value) in request.headers() {
        if let Ok(value) = value.to_str() {
            headers.insert(name.as_str().to_ascii_lowercase(), value.to_string());
        }
    }

    let body = match to_bytes(request.into_body(), MAX_HTTP_BODY_BYTES).await {
        Ok(bytes) => bytes.to_vec(),
        Err(err) => {
            let message = format!("request body error: {err}");
            return response_from_transport(HttpResponse {
                status: 400,
                content_type: "application/json",
                body: format!("{{\"error\":\"{}\"}}", message.replace('"', "\\\"")),
            });
        }
    };

    let request = HttpRequest {
        method,
        target,
        headers,
        body,
    };

    let response = handle_request_with_metrics(&state.store, &request, &state.metrics);
    drop(admission);
    response_from_transport(response)
}

struct BackpressureAdmission {
    _permit: OwnedSemaphorePermit,
    metrics: Arc<TransportBackpressureMetrics>,
}

impl Drop for BackpressureAdmission {
    fn drop(&mut self) {
        self.metrics.observe_dequeued();
    }
}

fn try_acquire_backpressure_permit(state: &AppState) -> Option<BackpressureAdmission> {
    match Arc::clone(&state.backpressure_limiter).try_acquire_owned() {
        Ok(permit) => {
            state.backpressure_metrics.observe_enqueued();
            Some(BackpressureAdmission {
                _permit: permit,
                metrics: Arc::clone(&state.backpressure_metrics),
            })
        }
        Err(_) => {
            state.backpressure_metrics.observe_rejected();
            None
        }
    }
}

fn response_from_transport(response: HttpResponse) -> Response<Body> {
    let status = StatusCode::from_u16(response.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let mut out = Response::new(Body::from(response.body));
    *out.status_mut() = status;
    out.headers_mut().insert(
        CONTENT_TYPE,
        response
            .content_type
            .parse()
            .unwrap_or(axum::http::HeaderValue::from_static("application/json")),
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use schema::{Claim, Evidence, Stance};

    fn sample_state_with_queue_capacity(queue_capacity: usize) -> AppState {
        let mut store = InMemoryStore::new();
        store
            .ingest_bundle(
                Claim {
                    claim_id: "c1".into(),
                    tenant_id: "tenant-a".into(),
                    canonical_text: "Company X acquired Company Y".into(),
                    confidence: 0.9,
                    event_time_unix: None,
                    entities: vec![],
                    embedding_ids: vec![],
                    claim_type: None,
                    valid_from: None,
                    valid_to: None,
                    created_at: None,
                    updated_at: None,
                },
                vec![Evidence {
                    evidence_id: "e1".into(),
                    claim_id: "c1".into(),
                    source_id: "source://doc-1".into(),
                    stance: Stance::Supports,
                    source_quality: 0.9,
                    chunk_id: None,
                    span_start: None,
                    span_end: None,
                    doc_id: None,
                    extraction_model: None,
                    ingested_at: None,
                }],
                vec![],
            )
            .unwrap();

        let metrics = Arc::new(Mutex::new(TransportMetrics::default()));
        let backpressure_metrics = Arc::new(TransportBackpressureMetrics::new(queue_capacity));
        {
            let mut guard = metrics.lock().unwrap();
            guard.set_transport_backpressure_metrics(Arc::clone(&backpressure_metrics));
        }
        AppState {
            store: Arc::new(store),
            metrics,
            backpressure_limiter: Arc::new(Semaphore::new(queue_capacity)),
            backpressure_metrics,
        }
    }

    fn sample_state() -> AppState {
        sample_state_with_queue_capacity(8)
    }

    async fn body_text(response: Response<Body>) -> String {
        let bytes = to_bytes(response.into_body(), MAX_HTTP_BODY_BYTES)
            .await
            .unwrap();
        String::from_utf8(bytes.to_vec()).unwrap()
    }

    #[tokio::test]
    async fn dispatch_health_returns_ok_json() {
        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let response = dispatch(State(sample_state()), request)
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"status\":\"ok\""));
    }

    #[tokio::test]
    async fn dispatch_post_retrieve_returns_results_payload() {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/retrieve")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"tenant_id":"tenant-a","query":"company x","top_k":1}"#,
            ))
            .unwrap();
        let response = dispatch(State(sample_state()), request)
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"results\""));
        assert!(body.contains("\"claim_id\":\"c1\""));
    }

    #[tokio::test]
    async fn dispatch_metrics_reflects_retrieve_request() {
        let state = sample_state();

        let request = Request::builder()
            .method("GET")
            .uri("/v1/retrieve?tenant_id=tenant-a&query=company+x&top_k=1")
            .body(Body::empty())
            .unwrap();
        let response = dispatch(State(state.clone()), request)
            .await
            .into_response();
        assert_eq!(response.status(), StatusCode::OK);

        let metrics_request = Request::builder()
            .method("GET")
            .uri("/metrics")
            .body(Body::empty())
            .unwrap();
        let metrics_response = dispatch(State(state), metrics_request)
            .await
            .into_response();
        assert_eq!(metrics_response.status(), StatusCode::OK);
        let body = body_text(metrics_response).await;
        assert!(body.contains("dash_retrieve_success_total 1"));
        assert!(body.contains("dash_retrieve_transport_queue_capacity 8"));
        assert!(body.contains("dash_retrieve_transport_queue_depth "));
        assert!(body.contains("dash_retrieve_transport_queue_full_reject_total 0"));
    }

    #[tokio::test]
    async fn dispatch_rejects_request_when_queue_is_full() {
        let state = sample_state_with_queue_capacity(1);
        let _held = try_acquire_backpressure_permit(&state)
            .expect("first backpressure permit should be available");
        assert_eq!(
            state
                .backpressure_metrics
                .queue_depth
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );

        let request = Request::builder()
            .method("GET")
            .uri("/health")
            .body(Body::empty())
            .unwrap();
        let response = dispatch(State(state.clone()), request)
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let body = body_text(response).await;
        assert!(body.contains("retrieval worker queue full"));
        assert_eq!(
            state
                .backpressure_metrics
                .queue_full_reject_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
}
