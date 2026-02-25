use super::*;

pub(super) fn handle_ingest_post(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
    audit_log_path: Option<&str>,
) -> HttpResponse {
    let write_consistency = match parse_write_consistency(query) {
        Ok(value) => value,
        Err(reason) => return HttpResponse::bad_request(&reason),
    };
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
            let response =
                match guard.ensure_local_write_route_for_claim(&api_req.claim, write_consistency) {
                    Ok(route_resolution) => match guard.ingest(api_req) {
                        Ok(mut resp) => {
                            resp.commit_epoch = if route_resolution.epoch > 0 {
                                Some(route_resolution.epoch)
                            } else {
                                None
                            };
                            resp.ack_count = route_resolution.ack_count;
                            resp.required_acks = route_resolution.required_acks;
                            resp.commit_status =
                                commit_status_for_progress(resp.ack_count, resp.required_acks)
                                    .to_string();
                            guard.record_commit_status(
                                &resp.ingested_claim_id,
                                resp.commit_epoch,
                                resp.ack_count,
                                resp.required_acks,
                            );
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

pub(super) fn handle_ingest_raw_post(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
    audit_log_path: Option<&str>,
) -> HttpResponse {
    let write_consistency = match parse_write_consistency(query) {
        Ok(value) => value,
        Err(reason) => return HttpResponse::bad_request(&reason),
    };
    if let Some(content_type) = request.headers.get("content-type")
        && !content_type
            .to_ascii_lowercase()
            .contains("application/json")
    {
        return HttpResponse::bad_request(
            "content-type must include application/json for POST /v1/ingest/raw",
        );
    }
    let body = match std::str::from_utf8(&request.body) {
        Ok(body) => body,
        Err(_) => return HttpResponse::bad_request("request body must be valid UTF-8"),
    };

    match build_ingest_raw_request_from_json(body) {
        Ok(api_req) => {
            let tenant_id = api_req.tenant_id.clone();
            let document_id = api_req.document_id.clone();
            match authorize_request_for_tenant(request, &tenant_id, auth_policy) {
                AuthDecision::Unauthorized(reason) => {
                    observe_auth_failure(runtime);
                    emit_audit_event(
                        runtime,
                        audit_log_path,
                        AuditEvent {
                            action: "ingest_raw",
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
                            action: "ingest_raw",
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

            let parsed = match build_ingest_raw_output_from_request(&api_req) {
                Ok(parsed) => parsed,
                Err(reason) => {
                    emit_audit_event(
                        runtime,
                        audit_log_path,
                        AuditEvent {
                            action: "ingest_raw",
                            tenant_id: Some(&tenant_id),
                            claim_id: None,
                            status: 400,
                            outcome: "denied",
                            reason: &reason,
                        },
                    );
                    return HttpResponse::bad_request(&reason);
                }
            };

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
                            action: "ingest_raw",
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
            let mut route_resolutions = Vec::with_capacity(parsed.batch.items.len());
            for item in &parsed.batch.items {
                match guard.ensure_local_write_route_for_claim(&item.claim, write_consistency) {
                    Ok(route_resolution) => route_resolutions.push(route_resolution),
                    Err(route_err) => {
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
                                action: "ingest_raw",
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
            }

            let parsed_batch = parsed.batch;
            let embedding_provider = parsed.embedding_provider;
            let embeddings_generated = parsed.embeddings_generated;
            let embedding_dimensions = parsed.embedding_dimensions;
            let response = match guard.ingest_batch(parsed_batch) {
                Ok(mut batch_resp) => {
                    let (commit_epoch, ack_count, required_acks) =
                        summarize_route_resolutions(&route_resolutions, write_consistency);
                    batch_resp.commit_epoch = commit_epoch;
                    batch_resp.ack_count = ack_count;
                    batch_resp.required_acks = required_acks;
                    batch_resp.commit_status =
                        commit_status_for_progress(batch_resp.ack_count, batch_resp.required_acks)
                            .to_string();
                    guard.record_commit_status(
                        &batch_resp.commit_id,
                        batch_resp.commit_epoch,
                        batch_resp.ack_count,
                        batch_resp.required_acks,
                    );
                    let raw_resp = IngestRawApiResponse {
                        document_id,
                        commit_id: batch_resp.commit_id,
                        idempotent_replay: batch_resp.idempotent_replay,
                        extracted_count: batch_resp.batch_size,
                        embedding_provider,
                        embeddings_generated,
                        embedding_dimensions,
                        ingested_claim_ids: batch_resp.ingested_claim_ids,
                        claims_total: batch_resp.claims_total,
                        commit_epoch: batch_resp.commit_epoch,
                        ack_count: batch_resp.ack_count,
                        required_acks: batch_resp.required_acks,
                        commit_status: batch_resp.commit_status,
                        checkpoint_triggered: batch_resp.checkpoint_triggered,
                        checkpoint_snapshot_records: batch_resp.checkpoint_snapshot_records,
                        checkpoint_truncated_wal_records: batch_resp
                            .checkpoint_truncated_wal_records,
                    };
                    audit_status = 200;
                    audit_outcome = "success";
                    audit_reason = format!(
                        "ingest raw accepted (document_id={}, extracted={})",
                        raw_resp.document_id, raw_resp.extracted_count
                    );
                    HttpResponse::ok_json(render_ingest_raw_response_json(&raw_resp))
                }
                Err(err) => {
                    guard.observe_failure();
                    guard.observe_batch_failure();
                    let (status, message) = map_store_error(&err);
                    audit_status = status;
                    audit_outcome = if status == 409 { "denied" } else { "error" };
                    audit_reason = message.clone();
                    HttpResponse::error_with_status(status, &message)
                }
            };
            drop(guard);
            emit_audit_event(
                runtime,
                audit_log_path,
                AuditEvent {
                    action: "ingest_raw",
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

pub(super) fn handle_ingest_batch_post(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
    audit_log_path: Option<&str>,
) -> HttpResponse {
    let write_consistency = match parse_write_consistency(query) {
        Ok(value) => value,
        Err(reason) => return HttpResponse::bad_request(&reason),
    };
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
            let mut route_resolutions = Vec::with_capacity(api_req.items.len());
            for item in &api_req.items {
                match guard.ensure_local_write_route_for_claim(&item.claim, write_consistency) {
                    Ok(route_resolution) => route_resolutions.push(route_resolution),
                    Err(route_err) => {
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
            }
            let response = match guard.ingest_batch(api_req) {
                Ok(mut resp) => {
                    let (commit_epoch, ack_count, required_acks) =
                        summarize_route_resolutions(&route_resolutions, write_consistency);
                    resp.commit_epoch = commit_epoch;
                    resp.ack_count = ack_count;
                    resp.required_acks = required_acks;
                    resp.commit_status =
                        commit_status_for_progress(resp.ack_count, resp.required_acks).to_string();
                    guard.record_commit_status(
                        &resp.commit_id,
                        resp.commit_epoch,
                        resp.ack_count,
                        resp.required_acks,
                    );
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
                    audit_outcome = if status == 409 { "denied" } else { "error" };
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

pub(super) fn handle_ingest_document_post(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
    audit_log_path: Option<&str>,
) -> HttpResponse {
    let write_consistency = match parse_write_consistency(query) {
        Ok(value) => value,
        Err(reason) => return HttpResponse::bad_request(&reason),
    };
    if let Some(content_type) = request.headers.get("content-type")
        && !content_type
            .to_ascii_lowercase()
            .contains("application/json")
    {
        return HttpResponse::bad_request(
            "content-type must include application/json for POST /v1/ingest/document",
        );
    }
    let body = match std::str::from_utf8(&request.body) {
        Ok(body) => body,
        Err(_) => return HttpResponse::bad_request("request body must be valid UTF-8"),
    };

    match build_ingest_document_request_from_json(body) {
        Ok(api_req) => {
            let tenant_id = api_req.tenant_id.clone();
            let document_id = api_req.document_id.clone();
            let requested_mime_type = api_req.mime_type.clone();
            match authorize_request_for_tenant(request, &tenant_id, auth_policy) {
                AuthDecision::Unauthorized(reason) => {
                    observe_auth_failure(runtime);
                    emit_audit_event(
                        runtime,
                        audit_log_path,
                        AuditEvent {
                            action: "ingest_document",
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
                            action: "ingest_document",
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

            let parsed = match build_ingest_batch_from_document_request(&api_req) {
                Ok(parsed) => parsed,
                Err(reason) => {
                    emit_audit_event(
                        runtime,
                        audit_log_path,
                        AuditEvent {
                            action: "ingest_document",
                            tenant_id: Some(&tenant_id),
                            claim_id: None,
                            status: 400,
                            outcome: "denied",
                            reason: &reason,
                        },
                    );
                    return HttpResponse::bad_request(&reason);
                }
            };

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
                            action: "ingest_document",
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
            let mut route_resolutions = Vec::with_capacity(parsed.batch.items.len());
            for item in &parsed.batch.items {
                match guard.ensure_local_write_route_for_claim(&item.claim, write_consistency) {
                    Ok(route_resolution) => route_resolutions.push(route_resolution),
                    Err(route_err) => {
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
                                action: "ingest_document",
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
            }

            let parsed_batch = parsed.batch;
            let parsed_mime_type = parsed.mime_type;
            let parsed_parser_provider = parsed.parser_provider;
            let embedding_provider = parsed.embedding_provider;
            let embeddings_generated = parsed.embeddings_generated;
            let embedding_dimensions = parsed.embedding_dimensions;
            let response = match guard.ingest_batch(parsed_batch) {
                Ok(mut batch_resp) => {
                    let (commit_epoch, ack_count, required_acks) =
                        summarize_route_resolutions(&route_resolutions, write_consistency);
                    batch_resp.commit_epoch = commit_epoch;
                    batch_resp.ack_count = ack_count;
                    batch_resp.required_acks = required_acks;
                    batch_resp.commit_status =
                        commit_status_for_progress(batch_resp.ack_count, batch_resp.required_acks)
                            .to_string();
                    guard.record_commit_status(
                        &batch_resp.commit_id,
                        batch_resp.commit_epoch,
                        batch_resp.ack_count,
                        batch_resp.required_acks,
                    );
                    let document_resp = IngestDocumentApiResponse {
                        document_id,
                        mime_type: parsed_mime_type,
                        parser_provider: parsed_parser_provider,
                        commit_id: batch_resp.commit_id,
                        idempotent_replay: batch_resp.idempotent_replay,
                        extracted_count: batch_resp.batch_size,
                        embedding_provider,
                        embeddings_generated,
                        embedding_dimensions,
                        ingested_claim_ids: batch_resp.ingested_claim_ids,
                        claims_total: batch_resp.claims_total,
                        commit_epoch: batch_resp.commit_epoch,
                        ack_count: batch_resp.ack_count,
                        required_acks: batch_resp.required_acks,
                        commit_status: batch_resp.commit_status,
                        checkpoint_triggered: batch_resp.checkpoint_triggered,
                        checkpoint_snapshot_records: batch_resp.checkpoint_snapshot_records,
                        checkpoint_truncated_wal_records: batch_resp
                            .checkpoint_truncated_wal_records,
                    };
                    audit_status = 200;
                    audit_outcome = "success";
                    audit_reason = format!(
                        "ingest document accepted (document_id={}, mime_type={}, extracted={})",
                        document_resp.document_id,
                        requested_mime_type,
                        document_resp.extracted_count
                    );
                    HttpResponse::ok_json(render_ingest_document_response_json(&document_resp))
                }
                Err(err) => {
                    guard.observe_failure();
                    guard.observe_batch_failure();
                    let (status, message) = map_store_error(&err);
                    audit_status = status;
                    audit_outcome = if status == 409 { "denied" } else { "error" };
                    audit_reason = message.clone();
                    HttpResponse::error_with_status(status, &message)
                }
            };
            drop(guard);
            emit_audit_event(
                runtime,
                audit_log_path,
                AuditEvent {
                    action: "ingest_document",
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

fn parse_write_consistency(
    query: &HashMap<String, String>,
) -> Result<WriteConsistencyPolicy, String> {
    let Some(raw) = query.get("write_consistency") else {
        return Ok(WriteConsistencyPolicy::One);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "" | "one" => Ok(WriteConsistencyPolicy::One),
        "quorum" => Ok(WriteConsistencyPolicy::Quorum),
        "all" => Ok(WriteConsistencyPolicy::All),
        _ => Err("write_consistency must be one, quorum, or all".to_string()),
    }
}

fn summarize_route_resolutions(
    route_resolutions: &[WriteRouteResolution],
    fallback_policy: WriteConsistencyPolicy,
) -> (Option<u64>, usize, usize) {
    if route_resolutions.is_empty() {
        let required = fallback_policy.required_acks(1);
        return (None, required, required);
    }
    let commit_epoch = route_resolutions
        .iter()
        .map(|item| item.epoch)
        .max()
        .filter(|value| *value > 0);
    let ack_count = route_resolutions
        .iter()
        .map(|item| item.ack_count)
        .min()
        .unwrap_or(1);
    let required_acks = route_resolutions
        .iter()
        .map(|item| item.required_acks)
        .max()
        .unwrap_or(1);
    (commit_epoch, ack_count, required_acks)
}

fn commit_status_for_progress(ack_count: usize, required_acks: usize) -> &'static str {
    if ack_count >= required_acks.max(1) {
        "replication_quorum_met"
    } else {
        "replication_pending"
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
