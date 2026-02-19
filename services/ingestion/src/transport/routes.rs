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
        ("POST", "/v1/ingest") => {
            if let Some(content_type) = request.headers.get("content-type")
                && !content_type
                    .to_ascii_lowercase()
                    .contains("application/json")
            {
                return HttpResponse::bad_request(
                    "content-type must include application/json for POST /v1/ingest",
                );
            }
            let body = match std::str::from_utf8(&request.body) {
                Ok(body) => body,
                Err(_) => return HttpResponse::bad_request("request body must be valid UTF-8"),
            };
            match build_ingest_request_from_json(body) {
                Ok(api_req) => {
                    let tenant_id = api_req.claim.tenant_id.clone();
                    let claim_id = api_req.claim.claim_id.clone();
                    match authorize_request_for_tenant(request, &tenant_id, &auth_policy) {
                        AuthDecision::Unauthorized(reason) => {
                            observe_auth_failure(runtime);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: Some(&claim_id),
                                    status: 401,
                                    outcome: "denied",
                                    reason,
                                },
                            );
                            return HttpResponse::unauthorized(reason);
                        }
                        AuthDecision::Forbidden(reason) => {
                            observe_authz_denied(runtime);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: Some(&claim_id),
                                    status: 403,
                                    outcome: "denied",
                                    reason,
                                },
                            );
                            return HttpResponse::forbidden(reason);
                        }
                        AuthDecision::Allowed => {
                            observe_auth_success(runtime);
                        }
                    }

                    let mut audit_status = 500;
                    let mut audit_outcome = "error";
                    let mut audit_reason = "runtime lock unavailable".to_string();
                    let mut guard = match runtime.lock() {
                        Ok(guard) => guard,
                        Err(_) => {
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: Some(&claim_id),
                                    status: audit_status,
                                    outcome: audit_outcome,
                                    reason: &audit_reason,
                                },
                            );
                            return HttpResponse::internal_server_error(
                                "failed to acquire ingestion runtime lock",
                            );
                        }
                    };
                    guard.flush_wal_if_due();
                    let response = match guard.ensure_local_write_route_for_claim(&api_req.claim) {
                        Ok(()) => match guard.ingest(api_req) {
                            Ok(resp) => {
                                audit_status = 200;
                                audit_outcome = "success";
                                audit_reason = "ingest accepted".to_string();
                                HttpResponse::ok_json(render_ingest_response_json(&resp))
                            }
                            Err(err) => {
                                guard.observe_failure();
                                let (status, message) = map_store_error(&err);
                                audit_status = status;
                                audit_reason = message.clone();
                                HttpResponse::error_with_status(status, &message)
                            }
                        },
                        Err(route_err) => {
                            guard.observe_failure();
                            guard.observe_write_route_rejection(&route_err);
                            audit_outcome = "denied";
                            let (status, message) = map_write_route_error(&route_err);
                            audit_status = status;
                            audit_reason = message.clone();
                            HttpResponse::error_with_status(status, &message)
                        }
                    };
                    drop(guard);
                    emit_audit_event(
                        runtime,
                        audit_log_path.as_deref(),
                        AuditEvent {
                            action: "ingest",
                            tenant_id: Some(&tenant_id),
                            claim_id: Some(&claim_id),
                            status: audit_status,
                            outcome: audit_outcome,
                            reason: &audit_reason,
                        },
                    );
                    response
                }
                Err(err) => HttpResponse::bad_request(&err),
            }
        }
        ("POST", "/v1/ingest/batch") => {
            if let Some(content_type) = request.headers.get("content-type")
                && !content_type
                    .to_ascii_lowercase()
                    .contains("application/json")
            {
                return HttpResponse::bad_request(
                    "content-type must include application/json for POST /v1/ingest/batch",
                );
            }
            let body = match std::str::from_utf8(&request.body) {
                Ok(body) => body,
                Err(_) => return HttpResponse::bad_request("request body must be valid UTF-8"),
            };
            let max_items = resolve_ingest_batch_max_items(DEFAULT_INGEST_BATCH_MAX_ITEMS);
            match build_ingest_batch_request_from_json(body, max_items) {
                Ok(api_req) => {
                    let tenant_id = api_req.items[0].claim.tenant_id.clone();
                    match authorize_request_for_tenant(request, &tenant_id, &auth_policy) {
                        AuthDecision::Unauthorized(reason) => {
                            observe_auth_failure(runtime);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest_batch",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: None,
                                    status: 401,
                                    outcome: "denied",
                                    reason,
                                },
                            );
                            return HttpResponse::unauthorized(reason);
                        }
                        AuthDecision::Forbidden(reason) => {
                            observe_authz_denied(runtime);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest_batch",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: None,
                                    status: 403,
                                    outcome: "denied",
                                    reason,
                                },
                            );
                            return HttpResponse::forbidden(reason);
                        }
                        AuthDecision::Allowed => {
                            observe_auth_success(runtime);
                        }
                    }

                    let mut audit_status = 500;
                    let mut audit_outcome = "error";
                    let mut audit_reason = "runtime lock unavailable".to_string();
                    let mut guard = match runtime.lock() {
                        Ok(guard) => guard,
                        Err(_) => {
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest_batch",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: None,
                                    status: audit_status,
                                    outcome: audit_outcome,
                                    reason: &audit_reason,
                                },
                            );
                            return HttpResponse::internal_server_error(
                                "failed to acquire ingestion runtime lock",
                            );
                        }
                    };
                    guard.flush_wal_if_due();
                    for item in &api_req.items {
                        if let Err(route_err) =
                            guard.ensure_local_write_route_for_claim(&item.claim)
                        {
                            guard.observe_failure();
                            guard.observe_batch_failure();
                            guard.observe_write_route_rejection(&route_err);
                            audit_outcome = "denied";
                            let (status, message) = map_write_route_error(&route_err);
                            audit_status = status;
                            audit_reason = message.clone();
                            let response = HttpResponse::error_with_status(status, &message);
                            drop(guard);
                            emit_audit_event(
                                runtime,
                                audit_log_path.as_deref(),
                                AuditEvent {
                                    action: "ingest_batch",
                                    tenant_id: Some(&tenant_id),
                                    claim_id: None,
                                    status: audit_status,
                                    outcome: audit_outcome,
                                    reason: &audit_reason,
                                },
                            );
                            return response;
                        }
                    }
                    let response = match guard.ingest_batch(api_req) {
                        Ok(resp) => {
                            audit_status = 200;
                            audit_outcome = "success";
                            audit_reason =
                                format!("ingest batch accepted (commit_id={})", resp.commit_id);
                            HttpResponse::ok_json(render_ingest_batch_response_json(&resp))
                        }
                        Err(err) => {
                            guard.observe_failure();
                            guard.observe_batch_failure();
                            let (status, message) = map_store_error(&err);
                            audit_status = status;
                            audit_reason = message.clone();
                            HttpResponse::error_with_status(status, &message)
                        }
                    };
                    drop(guard);
                    emit_audit_event(
                        runtime,
                        audit_log_path.as_deref(),
                        AuditEvent {
                            action: "ingest_batch",
                            tenant_id: Some(&tenant_id),
                            claim_id: None,
                            status: audit_status,
                            outcome: audit_outcome,
                            reason: &audit_reason,
                        },
                    );
                    response
                }
                Err(err) => HttpResponse::bad_request(&err),
            }
        }
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

fn observe_auth_success(runtime: &SharedRuntime) {
    if let Ok(mut guard) = runtime.lock() {
        guard.observe_auth_success();
    }
}

fn observe_auth_failure(runtime: &SharedRuntime) {
    if let Ok(mut guard) = runtime.lock() {
        guard.observe_auth_failure();
    }
}

fn observe_authz_denied(runtime: &SharedRuntime) {
    if let Ok(mut guard) = runtime.lock() {
        guard.observe_authz_denied();
    }
}
