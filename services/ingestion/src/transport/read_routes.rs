use super::ingest_routes::{observe_auth_failure, observe_authz_denied};
use super::*;

pub(super) fn handle_get_request(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    path: &str,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    if matches!(path, "/debug/placement" | "/debug/document-parser")
        && let Some(denied) = deny_unless_allowed(
            runtime,
            authorize_request_ops(request, auth_policy, Role::ReadOnly),
        )
    {
        return denied;
    }
    match path {
        // Versioned + unversioned health endpoints. The unversioned
        // paths are kept for backward compat with existing k8s
        // probe configs; `/v1/*` is the new canonical versioned path
        // that matches every other `/v1/*` endpoint.
        "/health" | "/v1/health" => HttpResponse::ok_json("{\"status\":\"ok\"}".to_string()),
        // Liveness: process is alive, not deadlocked. K8s restarts
        // the pod if this fails. No disk / network checks.
        "/live" | "/v1/live" => HttpResponse::ok_json("{\"status\":\"alive\"}".to_string()),
        // Readiness: process is up AND can serve traffic. K8s
        // removes the pod from the service if this fails. We
        // check that the SharedRuntime mutex is reachable and that
        // disk persistence is healthy when a persistence path was
        // configured.
        "/ready" | "/v1/ready" => match runtime.lock() {
            Ok(mut rt) => {
                // After a failed fsync the WAL refuses writes until a restart
                // re-reads it from disk: fail closed and leave the pool.
                if let Some(reason) = rt.wal_poisoned_reason() {
                    eprintln!("ingestion /ready: WAL poisoned: {reason}");
                    return HttpResponse {
                        status: 503,
                        content_type: "application/json",
                        body: "{\"status\":\"not_ready\",\"reason\":\"wal_poisoned\"}".to_string(),
                        retry_after_secs: None,
                    };
                }
                // Writes are failing (full or broken WAL volume): leave the
                // load balancer until space is back.
                if let Err(reason) = rt.wal_write_readiness() {
                    return HttpResponse {
                        status: 503,
                        content_type: "application/json",
                        body: format!("{{\"status\":\"not_ready\",\"reason\":\"{reason}\"}}"),
                        retry_after_secs: None,
                    };
                }
                // A follower that is lagging, stale or never synced serves
                // outdated data and must leave the load balancer.
                if let Some(Err(reason)) = rt.replication_readiness() {
                    return HttpResponse {
                        status: 503,
                        content_type: "application/json",
                        body: format!(
                            "{{\"status\":\"not_ready\",\"reason\":\"{reason}\",\"replication\":{}}}",
                            rt.replication_ready_json()
                                .unwrap_or_else(|| "null".to_string())
                        ),
                        retry_after_secs: None,
                    };
                }
                let ready_body = match rt.replication_ready_json() {
                    Some(json) => format!("{{\"status\":\"ready\",\"replication\":{json}}}"),
                    None => "{\"status\":\"ready\"}".to_string(),
                };
                match rt.disk_status() {
                    DiskStatus::Available | DiskStatus::Recovering => {
                        HttpResponse::ok_json(ready_body)
                    }
                    DiskStatus::Unavailable { reason } => {
                        if persistence_path_configured() {
                            eprintln!("ingestion /ready: disk unavailable: {reason}");
                            HttpResponse {
                                status: 503,
                                content_type: "application/json",
                                body: "{\"status\":\"not_ready\",\"reason\":\"disk_unavailable\"}"
                                    .to_string(),
                                retry_after_secs: None,
                            }
                        } else {
                            HttpResponse::ok_json(ready_body)
                        }
                    }
                }
            }
            Err(_) => HttpResponse::internal_server_error("runtime_unavailable"),
        },
        "/metrics" => {
            if !auth_policy.metrics_public()
                && let Some(denied) = deny_unless_allowed(
                    runtime,
                    authorize_request_ops(request, auth_policy, Role::ReadOnly),
                )
            {
                return denied;
            }
            refresh_placement(runtime);
            let body = match runtime.lock() {
                Ok(mut rt) => {
                    rt.flush_wal_if_due();
                    let mut text = rt.metrics_text();
                    text.push_str(&rt.replication_follower_metrics_text());
                    text.push_str(&rt.replication_leader_metrics_text());
                    text.push_str(&rt.replication_commit_status_metrics_text());
                    text
                }
                Err(_) => "dash_ingest_metrics_unavailable 1\n".to_string(),
            };
            HttpResponse::ok_text(body)
        }
        "/debug/placement" => match locked_after_placement_refresh(runtime) {
            Ok(rt) => HttpResponse::ok_json(render_placement_debug_json(&rt, query)),
            Err(_) => {
                HttpResponse::internal_server_error("failed to acquire ingestion runtime lock")
            }
        },
        "/debug/document-parser" => HttpResponse::ok_json(render_document_parser_debug_json()),
        "/internal/replication/wal" => {
            handle_replication_wal_get(runtime, request, query, auth_policy)
        }
        "/internal/replication/export" => {
            handle_replication_export_get(runtime, request, auth_policy)
        }
        "/internal/replication/export/begin" => {
            handle_replication_export_begin(runtime, request, query, auth_policy)
        }
        "/internal/replication/export/chunk" => {
            handle_replication_export_chunk(runtime, request, query, auth_policy)
        }
        "/internal/replication/commit-status" => {
            handle_replication_commit_status_get(runtime, request, query, auth_policy)
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}

/// Map a non-`Allowed` decision to its response (`None` when allowed),
/// recording the denial metrics.
fn deny_unless_allowed(runtime: &SharedRuntime, decision: AuthDecision) -> Option<HttpResponse> {
    match decision {
        // Operational endpoints do not count towards the auth success counter.
        AuthDecision::Allowed => None,
        AuthDecision::Unauthorized(reason) => {
            observe_auth_failure(runtime);
            Some(HttpResponse::unauthorized(reason))
        }
        AuthDecision::Forbidden(reason) => {
            observe_authz_denied(runtime);
            Some(HttpResponse::forbidden(reason))
        }
        AuthDecision::RateLimited { retry_after_secs } => {
            observe_authz_denied(runtime);
            Some(HttpResponse::too_many_requests(
                "rate limit exceeded",
                retry_after_secs,
            ))
        }
    }
}

pub(super) fn handle_replication_ack_post(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    if !is_replication_request_authorized(request, auth_policy) {
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
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    if !is_replication_request_authorized(request, auth_policy) {
        return HttpResponse::forbidden("replication request is not authorized");
    }
    let from_offset = match parse_query_usize(query, "from_offset") {
        Ok(value) => value.unwrap_or(0),
        Err(err) => return HttpResponse::bad_request(&err),
    };
    let max_records = match parse_query_usize(query, "max_records") {
        Ok(value) => value
            .unwrap_or(DEFAULT_REPLICATION_PULL_MAX_RECORDS)
            .min(MAX_REPLICATION_PULL_MAX_RECORDS),
        Err(err) => return HttpResponse::bad_request(&err),
    };
    let from_generation = match query.get("from_generation") {
        None => None,
        Some(value) => match value.parse::<u64>() {
            Ok(parsed) => Some(parsed),
            Err(_) => {
                return HttpResponse::bad_request(
                    "query parameter 'from_generation' must be a valid u64",
                );
            }
        },
    };
    // Followers that understand generation switches say so; the frame then
    // carries a `switch_from=` line (older followers get the old layout).
    let allow_switch = query.get("gen_switch").is_some_and(|v| v == "1");
    match runtime.lock() {
        Ok(mut rt) => {
            match rt.replication_delta_for_followers(
                from_generation,
                from_offset,
                max_records,
                allow_switch,
            ) {
                Ok(delta) => {
                    HttpResponse::ok_plain(render_replication_delta_frame(&delta, allow_switch))
                }
                Err(err) => {
                    let (status, message) = map_store_error(&err);
                    HttpResponse::error_with_status(status, &message)
                }
            }
        }
        Err(_) => HttpResponse::internal_server_error("failed to acquire ingestion runtime lock"),
    }
}

fn handle_replication_export_get(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    if !is_replication_request_authorized(request, auth_policy) {
        return HttpResponse::forbidden("replication request is not authorized");
    }
    match runtime.lock() {
        Ok(mut rt) => match rt.replication_export_for_followers() {
            Ok((export, generation)) => {
                HttpResponse::ok_plain(render_replication_export_frame(&export, generation))
            }
            Err(err) => {
                let (status, message) = map_store_error(&err);
                HttpResponse::error_with_status(status, &message)
            }
        },
        Err(_) => HttpResponse::internal_server_error("failed to acquire ingestion runtime lock"),
    }
}

/// `GET /internal/replication/export/begin[?avoid=<export id>]`: freezes
/// (or reuses) a chunked export of the leader's state and answers its
/// manifest. The runtime lock is held only to look up the WAL; the WAL lock
/// only while the view is frozen.
fn handle_replication_export_begin(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    if !is_replication_request_authorized(request, auth_policy) {
        return HttpResponse::forbidden("replication request is not authorized");
    }
    let avoid = match query.get("avoid") {
        None => None,
        Some(id) if store::valid_export_id(id) => Some(id.as_str()),
        Some(_) => {
            return HttpResponse::bad_request("query parameter 'avoid' must be an export id");
        }
    };
    let handles = match runtime.lock() {
        Ok(rt) => rt.replication_export_handles(),
        Err(_) => {
            return HttpResponse::internal_server_error("failed to acquire ingestion runtime lock");
        }
    };
    let (exports, wal) = match handles {
        Ok(handles) => handles,
        Err(err) => {
            let (status, message) = map_store_error(&err);
            return HttpResponse::error_with_status(status, &message);
        }
    };
    match exports.begin(&wal, avoid) {
        Ok(manifest) => HttpResponse::ok_plain(manifest.render_response()),
        Err(err) => {
            eprintln!("ingestion replication export failed: {err:?}");
            let (status, message) = map_store_error(&err);
            HttpResponse::error_with_status(status, &message)
        }
    }
}

/// `GET /internal/replication/export/chunk?export_id=&offset=&max_bytes=`:
/// one chunk of a retained export, read from its file without any lock.
/// 404 when the export is no longer retained (the follower starts over).
fn handle_replication_export_chunk(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    if !is_replication_request_authorized(request, auth_policy) {
        return HttpResponse::forbidden("replication request is not authorized");
    }
    let export_id = match query.get("export_id") {
        Some(id) if store::valid_export_id(id) => id.clone(),
        _ => return HttpResponse::bad_request("query parameter 'export_id' must be an export id"),
    };
    let offset = match query.get("offset").map(|v| v.parse::<u64>()) {
        None => 0,
        Some(Ok(offset)) => offset,
        Some(Err(_)) => {
            return HttpResponse::bad_request("query parameter 'offset' must be a valid u64");
        }
    };
    let max_bytes = match parse_query_usize(query, "max_bytes") {
        Ok(value) => value.unwrap_or(store::EXPORT_CHUNK_DEFAULT_BYTES),
        Err(err) => return HttpResponse::bad_request(&err),
    };
    let exports = match runtime.lock() {
        Ok(rt) => rt.replication_export_handles().map(|(exports, _)| exports),
        Err(_) => {
            return HttpResponse::internal_server_error("failed to acquire ingestion runtime lock");
        }
    };
    let exports = match exports {
        Ok(exports) => exports,
        Err(err) => {
            let (status, message) = map_store_error(&err);
            return HttpResponse::error_with_status(status, &message);
        }
    };
    match exports.read_chunk(&export_id, offset, max_bytes) {
        Ok(store::ChunkRead::Chunk(chunk)) => HttpResponse::ok_plain(chunk.render_response()),
        Ok(store::ChunkRead::NotFound) => {
            HttpResponse::not_found("replication export is no longer available")
        }
        Ok(store::ChunkRead::BadOffset(reason)) => HttpResponse::bad_request(&reason),
        Err(err) => {
            let (status, message) = map_store_error(&err);
            HttpResponse::error_with_status(status, &message)
        }
    }
}

fn handle_replication_commit_status_get(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
) -> HttpResponse {
    if !is_replication_request_authorized(request, auth_policy) {
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

/// True when a persistence path was explicitly configured and disk
/// persistence was not disabled. Used by the /ready probe to decide
/// whether an `Unavailable` disk status should fail readiness.
fn persistence_path_configured() -> bool {
    let disabled = std::env::var("DASH_INGEST_PERSISTENCE_DISABLE")
        .ok()
        .or_else(|| std::env::var("EME_INGEST_PERSISTENCE_DISABLE").ok())
        .is_some_and(|value| matches!(value.trim().to_lowercase().as_str(), "1" | "true" | "yes"));
    let path_set = std::env::var("DASH_INGEST_PERSISTENCE_PATH")
        .or_else(|_| std::env::var("EME_INGEST_PERSISTENCE_PATH"))
        .is_ok_and(|value| !value.trim().is_empty());
    !disabled && path_set
}

fn escape_json(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('\"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

fn locked_after_placement_refresh(
    runtime: &SharedRuntime,
) -> Result<
    std::sync::MutexGuard<'_, IngestionRuntime>,
    std::sync::PoisonError<std::sync::MutexGuard<'_, IngestionRuntime>>,
> {
    refresh_placement(runtime);
    runtime.lock()
}
