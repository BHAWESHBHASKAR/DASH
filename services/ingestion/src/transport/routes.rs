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
        ("GET", "/health") => HttpResponse::ok_json("{\"status\":\"ok\"}".to_string()),
        ("GET", "/metrics") => {
            let body = match runtime.lock() {
                Ok(mut rt) => {
                    rt.flush_wal_if_due();
                    rt.refresh_placement_if_due();
                    rt.metrics_text()
                }
                Err(_) => "dash_ingest_metrics_unavailable 1\n".to_string(),
            };
            HttpResponse::ok_text(body)
        }
        ("GET", "/debug/placement") => match runtime.lock() {
            Ok(mut rt) => {
                rt.refresh_placement_if_due();
                HttpResponse::ok_json(render_placement_debug_json(&rt, &query))
            }
            Err(_) => {
                HttpResponse::internal_server_error("failed to acquire ingestion runtime lock")
            }
        },
        ("GET", "/internal/replication/wal") => {
            if !is_replication_request_authorized(request) {
                return HttpResponse::forbidden("replication request is not authorized");
            }
            let from_offset = match parse_query_usize(&query, "from_offset") {
                Ok(value) => value.unwrap_or(0),
                Err(err) => return HttpResponse::bad_request(&err),
            };
            let max_records = match parse_query_usize(&query, "max_records") {
                Ok(value) => value.unwrap_or(DEFAULT_REPLICATION_PULL_MAX_RECORDS),
                Err(err) => return HttpResponse::bad_request(&err),
            };
            match runtime.lock() {
                Ok(mut rt) => match rt.replication_delta_for_followers(from_offset, max_records) {
                    Ok(delta) => HttpResponse::ok_plain(render_replication_delta_frame(&delta)),
                    Err(err) => {
                        let (status, message) = map_store_error(&err);
                        HttpResponse::error_with_status(status, &message)
                    }
                },
                Err(_) => {
                    HttpResponse::internal_server_error("failed to acquire ingestion runtime lock")
                }
            }
        }
        ("GET", "/internal/replication/export") => {
            if !is_replication_request_authorized(request) {
                return HttpResponse::forbidden("replication request is not authorized");
            }
            match runtime.lock() {
                Ok(mut rt) => match rt.replication_export_for_followers() {
                    Ok(export) => HttpResponse::ok_plain(render_replication_export_frame(&export)),
                    Err(err) => {
                        let (status, message) = map_store_error(&err);
                        HttpResponse::error_with_status(status, &message)
                    }
                },
                Err(_) => {
                    HttpResponse::internal_server_error("failed to acquire ingestion runtime lock")
                }
            }
        }
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
