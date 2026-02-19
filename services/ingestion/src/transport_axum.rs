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
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::transport::{
    HttpRequest, HttpResponse, IngestionRuntime, SharedRuntime, TransportBackpressureMetrics,
    backpressure_rejection_response, handle_request, resolve_http_queue_capacity,
};

const MAX_HTTP_BODY_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone)]
struct AppState {
    runtime: SharedRuntime,
    backpressure_limiter: Arc<Semaphore>,
    backpressure_metrics: Arc<TransportBackpressureMetrics>,
}

pub fn serve_http_with_axum(
    ingestion_runtime: IngestionRuntime,
    bind_addr: &str,
    worker_threads: usize,
) -> Result<(), String> {
    let worker_threads = worker_threads.max(1);
    let tokio_runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .map_err(|e| format!("failed to build tokio runtime: {e}"))?;

    let bind_addr = bind_addr.to_string();
    tokio_runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(&bind_addr)
            .await
            .map_err(|e| format!("failed to bind {bind_addr}: {e}"))?;

        let runtime = Arc::new(Mutex::new(ingestion_runtime));
        let queue_capacity = resolve_http_queue_capacity(worker_threads);
        let backpressure_metrics = Arc::new(TransportBackpressureMetrics::new(queue_capacity));
        if let Ok(mut guard) = runtime.lock() {
            guard.set_transport_backpressure_metrics(Arc::clone(&backpressure_metrics));
        }
        let wal_async_flush_interval = runtime
            .lock()
            .ok()
            .and_then(|guard| guard.wal_async_flush_interval());
        let (flush_shutdown_tx, mut flush_shutdown_rx) = tokio::sync::watch::channel(false);
        let flush_handle = wal_async_flush_interval.map(|interval| {
            let runtime = Arc::clone(&runtime);
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = ticker.tick() => {
                            let Ok(mut guard) = runtime.lock() else {
                                break;
                            };
                            guard.flush_wal_for_async_tick();
                            guard.refresh_placement_if_due();
                        }
                        changed = flush_shutdown_rx.changed() => {
                            if changed.is_err() || *flush_shutdown_rx.borrow() {
                                break;
                            }
                        }
                    }
                }
            })
        });

        let state = AppState {
            runtime,
            backpressure_limiter: Arc::new(Semaphore::new(queue_capacity)),
            backpressure_metrics,
        };

        let app = Router::new()
            .fallback(any(dispatch))
            .with_state(state)
            .layer(axum::extract::DefaultBodyLimit::max(MAX_HTTP_BODY_BYTES));

        let serve_result = axum::serve(listener, app)
            .await
            .map_err(|e| format!("axum server failed: {e}"));

        let _ = flush_shutdown_tx.send(true);
        if let Some(handle) = flush_handle {
            let _ = handle.await;
        }
        serve_result
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

    let response = handle_request(&state.runtime, &request);
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
    use store::InMemoryStore;

    fn sample_state_with_queue_capacity(queue_capacity: usize) -> AppState {
        let runtime = Arc::new(Mutex::new(
            IngestionRuntime::in_memory(InMemoryStore::new()),
        ));
        let backpressure_metrics = Arc::new(TransportBackpressureMetrics::new(queue_capacity));
        {
            let mut guard = runtime.lock().unwrap();
            guard.set_transport_backpressure_metrics(Arc::clone(&backpressure_metrics));
        }
        AppState {
            runtime,
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
    async fn dispatch_post_ingest_returns_claims_total() {
        let request = Request::builder()
            .method("POST")
            .uri("/v1/ingest")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9}}"#,
            ))
            .unwrap();
        let response = dispatch(State(sample_state()), request)
            .await
            .into_response();

        assert_eq!(response.status(), StatusCode::OK);
        let body = body_text(response).await;
        assert!(body.contains("\"claims_total\":1"));
    }

    #[tokio::test]
    async fn dispatch_metrics_reflects_ingest_success() {
        let state = sample_state();

        let ingest_request = Request::builder()
            .method("POST")
            .uri("/v1/ingest")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"claim":{"claim_id":"c1","tenant_id":"tenant-a","canonical_text":"Company X acquired Company Y","confidence":0.9}}"#,
            ))
            .unwrap();
        let ingest_response = dispatch(State(state.clone()), ingest_request)
            .await
            .into_response();
        assert_eq!(ingest_response.status(), StatusCode::OK);

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
        assert!(body.contains("dash_ingest_success_total 1"));
        assert!(body.contains("dash_ingest_transport_queue_capacity 8"));
        assert!(body.contains("dash_ingest_transport_queue_depth "));
        assert!(body.contains("dash_ingest_transport_queue_full_reject_total 0"));
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
        assert!(body.contains("ingestion worker queue full"));
        assert_eq!(
            state
                .backpressure_metrics
                .queue_full_reject_total
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
    }
}
