use super::*;

pub(super) fn handle_get_request(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    path: &str,
    query: &HashMap<String, String>,
) -> HttpResponse {
    match path {
        // Versioned + unversioned health endpoints. The unversioned
        // paths are kept for backward compat with existing k8s
        // probe configs; `/v1/*` is the new canonical versioned path
        // that matches every other `/v1/*` endpoint.
        "/health" | "/v1/health" => {
            HttpResponse::ok_json("{\"status\":\"ok\"}".to_string())
        }
        // Liveness: process is alive, not deadlocked. K8s restarts
        // the pod if this fails. No disk / network checks.
        "/live" | "/v1/live" => {
            HttpResponse::ok_json("{\"status\":\"alive\"}".to_string())
        }
        // Readiness: process is up AND can serve traffic. K8s
        // removes the pod from the service if this fails. We
        // check that the SharedRuntime mutex is reachable; a
        // poisoned mutex means something else is very wrong.
        "/ready" | "/v1/ready" => match runtime.lock() {
            Ok(_) => HttpResponse::ok_json("{\"status\":\"ready\"}".to_string()),
            Err(_) => HttpResponse::internal_server_error("runtime mutex poisoned"),
        },
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
        "/debug/document-parser" => HttpResponse::ok_json(render_document_parser_debug_json()),
        "/internal/replication/wal" => handle_replication_wal_get(runtime, request, query),
        "/internal/replication/export" => handle_replication_export_get(runtime, request),
        "/internal/replication/commit-status" => {
            handle_replication_commit_status_get(runtime, request, query)
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}

pub(super) fn handle_replication_ack_post(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
) -> HttpResponse {
    if !is_replication_request_authorized(request) {
        return HttpResponse::forbidden("replication request is not authorized");
    }
    let commit_id = match query.get("commit_id") {
        Some(value) if !value.trim().is_empty() => value.trim(),
        _ => return HttpResponse::bad_request("commit_id query parameter is required"),
    };
    let replica_id = match query.get("replica_id") {
        Some(value) if !value.trim().is_empty() => value.trim(),
        _ => return HttpResponse::bad_request("replica_id query parameter is required"),
    };
    let ack_epoch = match query.get("ack_epoch") {
        Some(value) if !value.trim().is_empty() => match value.trim().parse::<u64>() {
            Ok(parsed) => Some(parsed),
            Err(_) => return HttpResponse::bad_request("ack_epoch must be a valid u64"),
        },
        _ => None,
    };
    match runtime.lock() {
        Ok(mut rt) => match rt.apply_replication_ack(commit_id, replica_id, ack_epoch) {
            Ok(snapshot) => HttpResponse::ok_json(render_replication_commit_status_json(&snapshot)),
            Err(reason) => HttpResponse::error_with_status(404, &reason),
        },
        Err(_) => HttpResponse::internal_server_error("failed to acquire ingestion runtime lock"),
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

fn handle_replication_commit_status_get(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
) -> HttpResponse {
    if !is_replication_request_authorized(request) {
        return HttpResponse::forbidden("replication request is not authorized");
    }
    let commit_id = match query.get("commit_id") {
        Some(value) if !value.trim().is_empty() => value.trim(),
        _ => return HttpResponse::bad_request("commit_id query parameter is required"),
    };
    match runtime.lock() {
        Ok(rt) => match rt.replication_commit_status_snapshot(commit_id) {
            Some(snapshot) => {
                HttpResponse::ok_json(render_replication_commit_status_json(&snapshot))
            }
            None => {
                HttpResponse::error_with_status(404, &format!("unknown commit_id '{}'", commit_id))
            }
        },
        Err(_) => HttpResponse::internal_server_error("failed to acquire ingestion runtime lock"),
    }
}

fn render_replication_commit_status_json(snapshot: &ReplicationCommitStatusSnapshot) -> String {
    format!(
        "{{\"commit_id\":\"{}\",\"commit_epoch\":{},\"ack_count\":{},\"required_acks\":{},\"commit_status\":\"{}\"}}",
        escape_json(&snapshot.commit_id),
        snapshot
            .commit_epoch
            .map(|value| value.to_string())
            .unwrap_or_else(|| "null".to_string()),
        snapshot.ack_count,
        snapshot.required_acks,
        escape_json(&snapshot.commit_status)
    )
}

fn escape_json(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
