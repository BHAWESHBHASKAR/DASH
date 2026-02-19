use super::*;

pub(super) fn handle_request(runtime: &SharedRuntime, request: &HttpRequest) -> HttpResponse {
    let (path, query) = split_target(&request.target);
    let auth_policy = AuthPolicy::from_env(
        env_with_fallback("DASH_INGEST_API_KEY", "EME_INGEST_API_KEY"),
        env_with_fallback("DASH_INGEST_API_KEYS", "EME_INGEST_API_KEYS"),
        env_with_fallback(
            "DASH_INGEST_REVOKED_API_KEYS",
            "EME_INGEST_REVOKED_API_KEYS",
        ),
        env_with_fallback("DASH_INGEST_ALLOWED_TENANTS", "EME_INGEST_ALLOWED_TENANTS"),
        env_with_fallback("DASH_INGEST_API_KEY_SCOPES", "EME_INGEST_API_KEY_SCOPES"),
    );
    let audit_log_path =
        env_with_fallback("DASH_INGEST_AUDIT_LOG_PATH", "EME_INGEST_AUDIT_LOG_PATH");
    match (request.method.as_str(), path.as_str()) {
        ("GET", _) => read_routes::handle_get_request(runtime, request, &path, &query),
        ("POST", "/v1/ingest") => ingest_routes::handle_ingest_post(
            runtime,
            request,
            &auth_policy,
            audit_log_path.as_deref(),
        ),
        ("POST", "/v1/ingest/batch") => ingest_routes::handle_ingest_batch_post(
            runtime,
            request,
            &auth_policy,
            audit_log_path.as_deref(),
        ),
        (_, "/v1/ingest") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/v1/ingest/batch") => HttpResponse::method_not_allowed("only POST is supported"),
        (_, "/health")
        | (_, "/metrics")
        | (_, "/debug/placement")
        | (_, "/internal/replication/wal")
        | (_, "/internal/replication/export") => {
            HttpResponse::method_not_allowed("only GET is supported")
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}
