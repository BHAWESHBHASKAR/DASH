use super::*;

pub(super) fn handle_request(runtime: &SharedRuntime, request: &HttpRequest) -> HttpResponse {
    let auth_policy = shared_auth_policy();
    // Audit context (actor fingerprint, request id) for every event emitted
    // while this request is handled on this thread.
    let _audit_ctx = dash_common::audit::enter_context(dash_common::audit::context_from_headers(
        &request.headers,
    ));
    let (path, _) = split_target(&request.target);
    if request.method == "POST" && path.starts_with("/v1/ingest") {
        // DASH_INGEST_AUDIT_FAIL_CLOSED=1: refuse before any mutation when the
        // audit log is unusable.
        let audit_log_path =
            env_with_fallback("DASH_INGEST_AUDIT_LOG_PATH", "EME_INGEST_AUDIT_LOG_PATH");
        if let Err(err) = audit::audit_gate(audit_log_path.as_deref()) {
            eprintln!("ingestion audit fail-closed gate rejected request: {err}");
            return HttpResponse::service_unavailable("audit log unavailable");
        }
    }
    let mut response = handle_request_with_policy(runtime, request, &auth_policy);
    if request.method == "GET" && path == "/metrics" && response.status == 200 {
        response
            .body
            .push_str(&dash_common::audit::render_prometheus_counters());
    }
    response
}

pub(super) fn handle_request_with_policy(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    let (path, query) = split_target(&request.target);
    if query_encoding_is_invalid(&request.target) {
        return HttpResponse::bad_request("invalid percent-encoding in query");
    }
    let audit_log_path =
        env_with_fallback("DASH_INGEST_AUDIT_LOG_PATH", "EME_INGEST_AUDIT_LOG_PATH");
    match (request.method.as_str(), path.as_str()) {
        ("GET", _) => read_routes::handle_get_request(runtime, request, &path, &query, auth_policy),
        ("POST", "/v1/ingest") => ingest_routes::handle_ingest_post(
            runtime,
            request,
            &query,
            auth_policy,
            audit_log_path.as_deref(),
        ),
        ("POST", "/v1/ingest/raw") => ingest_routes::handle_ingest_raw_post(
            runtime,
            request,
            &query,
            auth_policy,
            audit_log_path.as_deref(),
        ),
        ("POST", "/v1/ingest/document") => ingest_routes::handle_ingest_document_post(
            runtime,
            request,
            &query,
            auth_policy,
            audit_log_path.as_deref(),
        ),
        ("POST", "/v1/ingest/batch") => ingest_routes::handle_ingest_batch_post(
            runtime,
            request,
            &query,
            auth_policy,
            audit_log_path.as_deref(),
        ),
        ("POST", "/internal/replication/ack") => {
            read_routes::handle_replication_ack_post(runtime, request, &query, auth_policy)
        }
        (_, "/v1/ingest") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/v1/ingest/raw") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/v1/ingest/document") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/v1/ingest/batch") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/health")
        | (_, "/v1/health")
        | (_, "/live")
        | (_, "/v1/live")
        | (_, "/ready")
        | (_, "/v1/ready")
        | (_, "/metrics")
        | (_, "/debug/placement")
        | (_, "/debug/document-parser")
        | (_, "/internal/replication/wal")
        | (_, "/internal/replication/export")
        | (_, "/internal/replication/commit-status") => {
            HttpResponse::method_not_allowed("only GET is supported")
        }
        (_, "/internal/replication/ack") => {
            HttpResponse::method_not_allowed("only POST is supported")
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}
