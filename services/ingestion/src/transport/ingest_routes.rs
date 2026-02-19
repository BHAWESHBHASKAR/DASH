use super::*;

pub(super) fn handle_ingest_post(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    auth_policy: &AuthPolicy,
    audit_log_path: Option<&str>,
) -> HttpResponse {
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
            match authorize_request_for_tenant(request, &tenant_id, auth_policy) {
                AuthDecision::Unauthorized(reason) => {
                    observe_auth_failure(runtime);
                    emit_audit_event(
                        runtime,
                        audit_log_path,
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
                        audit_log_path,
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
                        audit_log_path,
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
                audit_log_path,
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

pub(super) fn handle_ingest_batch_post(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    auth_policy: &AuthPolicy,
    audit_log_path: Option<&str>,
) -> HttpResponse {
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
            match authorize_request_for_tenant(request, &tenant_id, auth_policy) {
                AuthDecision::Unauthorized(reason) => {
                    observe_auth_failure(runtime);
                    emit_audit_event(
                        runtime,
                        audit_log_path,
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
                        audit_log_path,
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
                        audit_log_path,
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
                if let Err(route_err) = guard.ensure_local_write_route_for_claim(&item.claim) {
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
                        audit_log_path,
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
                    audit_reason = format!("ingest batch accepted (commit_id={})", resp.commit_id);
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
                audit_log_path,
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
