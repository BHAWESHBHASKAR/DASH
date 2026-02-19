use super::*;

pub(super) fn handle_get_request(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    path: &str,
    query: &HashMap<String, String>,
) -> HttpResponse {
    match path {
        "/health" => HttpResponse::ok_json("{\"status\":\"ok\"}".to_string()),
        "/metrics" => {
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
        "/debug/placement" => match runtime.lock() {
            Ok(mut rt) => {
                rt.refresh_placement_if_due();
                HttpResponse::ok_json(render_placement_debug_json(&rt, query))
            }
            Err(_) => {
                HttpResponse::internal_server_error("failed to acquire ingestion runtime lock")
            }
        },
        "/internal/replication/wal" => handle_replication_wal_get(runtime, request, query),
        "/internal/replication/export" => handle_replication_export_get(runtime, request),
        _ => HttpResponse::not_found("unknown path"),
    }
}

fn handle_replication_wal_get(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
) -> HttpResponse {
    if !is_replication_request_authorized(request) {
        return HttpResponse::forbidden("replication request is not authorized");
    }
    let from_offset = match parse_query_usize(query, "from_offset") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return HttpResponse::bad_request(&err),
    };
    let max_records = match parse_query_usize(query, "max_records") {
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
        Err(_) => HttpResponse::internal_server_error("failed to acquire ingestion runtime lock"),
    }
}

fn handle_replication_export_get(runtime: &SharedRuntime, request: &HttpRequest) -> HttpResponse {
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
        Err(_) => HttpResponse::internal_server_error("failed to acquire ingestion runtime lock"),
    }
}
