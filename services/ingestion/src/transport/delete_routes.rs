//! Delete routes:
//!
//! - `DELETE /v1/claims/{claim_id}?tenant_id=...` removes the claim, its
//!   vector, its evidence and every edge from or to it (role `ingest`).
//! - `DELETE /v1/evidence/{evidence_id}?tenant_id=...` removes every evidence
//!   row with that id on the tenant's claims (role `ingest`).
//! - `DELETE /v1/tenants/{tenant_id}` erases all of the tenant's data (role
//!   `admin` for that tenant).
//!
//! Every delete is idempotent and answers `200` with `"deleted": true` when
//! something was removed and `"deleted": false` when the target did not exist
//! (including a claim that belongs to another tenant). A delete that removes
//! something is one checksummed WAL tombstone, durable before the store (and
//! redb) changes, exactly like an ingest; a delete of nothing writes nothing.
//!
//! Ordering with group commit: a delete first drains the pipeline (see
//! `group_commit::drain`) and then uses the WAL exclusively, the rule every
//! non-pipelined writer follows. Every pipelined ingest enqueued before the
//! delete is applied before it, and every later one validates against the
//! post-delete state, so the outcome equals serial execution in WAL order.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::Serialize;
use store::{DeleteStats, Tombstone};

use super::ingest_routes::{
    observe_auth_failure, observe_auth_success, observe_authz_denied, parse_write_consistency,
    reject_oversized_identifiers,
};
use super::*;

/// Route prefixes of the delete API.
pub(super) const CLAIMS_PREFIX: &str = "/v1/claims/";
pub(super) const EVIDENCE_PREFIX: &str = "/v1/evidence/";
pub(super) const TENANTS_PREFIX: &str = "/v1/tenants/";

/// `true` for a path served by this module (whatever the method).
pub(super) fn is_delete_path(path: &str) -> bool {
    [CLAIMS_PREFIX, EVIDENCE_PREFIX, TENANTS_PREFIX]
        .iter()
        .any(|prefix| path.starts_with(prefix))
}

#[derive(Debug, Default)]
pub(crate) struct DeleteMetrics {
    /// Deletes that removed something, by scope (claim, evidence, tenant).
    applied: [AtomicU64; 3],
    /// Deletes whose target did not exist.
    noop: AtomicU64,
    claims_removed: AtomicU64,
    evidence_removed: AtomicU64,
    edges_removed: AtomicU64,
    vectors_removed: AtomicU64,
}

fn scope_index(tombstone: &Tombstone) -> usize {
    match tombstone {
        Tombstone::Claim { .. } => 0,
        Tombstone::Evidence { .. } => 1,
        Tombstone::Tenant { .. } => 2,
    }
}

impl DeleteMetrics {
    fn observe(&self, tombstone: &Tombstone, stats: &DeleteStats) {
        if stats.is_empty() {
            self.noop.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.applied[scope_index(tombstone)].fetch_add(1, Ordering::Relaxed);
        for (counter, value) in [
            (&self.claims_removed, stats.claims),
            (&self.evidence_removed, stats.evidence),
            (&self.edges_removed, stats.edges),
            (&self.vectors_removed, stats.vectors),
        ] {
            counter.fetch_add(value as u64, Ordering::Relaxed);
        }
    }

    pub(super) fn render(&self) -> String {
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        format!(
            "# TYPE dash_ingest_delete_total counter\n\
dash_ingest_delete_total{{scope=\"claim\"}} {}\n\
dash_ingest_delete_total{{scope=\"evidence\"}} {}\n\
dash_ingest_delete_total{{scope=\"tenant\"}} {}\n\
# TYPE dash_ingest_delete_noop_total counter\n\
dash_ingest_delete_noop_total {}\n\
# TYPE dash_ingest_delete_removed_total counter\n\
dash_ingest_delete_removed_total{{kind=\"claim\"}} {}\n\
dash_ingest_delete_removed_total{{kind=\"evidence\"}} {}\n\
dash_ingest_delete_removed_total{{kind=\"edge\"}} {}\n\
dash_ingest_delete_removed_total{{kind=\"vector\"}} {}\n",
            load(&self.applied[0]),
            load(&self.applied[1]),
            load(&self.applied[2]),
            load(&self.noop),
            load(&self.claims_removed),
            load(&self.evidence_removed),
            load(&self.edges_removed),
            load(&self.vectors_removed),
        )
    }
}

/// Response body of every delete route.
#[derive(Debug, Serialize)]
struct DeleteResponse<'a> {
    deleted: bool,
    scope: &'a str,
    tenant_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    claim_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence_id: Option<&'a str>,
    claims_deleted: usize,
    evidence_deleted: usize,
    edges_deleted: usize,
    vectors_deleted: usize,
    claims_total: usize,
    checkpoint_triggered: bool,
    checkpoint_deferred: bool,
}

/// Outcome of a delete that reached the store.
pub(super) struct DeleteResult {
    pub(super) stats: DeleteStats,
    pub(super) claims_total: usize,
    pub(super) checkpoint_triggered: bool,
    pub(super) checkpoint_deferred: bool,
}

enum DeleteFailure {
    Route(WriteRouteError),
    Store(StoreError),
}

/// Decodes one path segment (`%XX` escapes; `+` is literal in a path).
fn decode_segment(raw: &str) -> Result<String, String> {
    dash_http::percent_decode(&raw.replace('+', "%2B"))
        .map_err(|_| "invalid percent-encoding in path".to_string())
}

/// Parses the target of a delete request. `Ok(None)` is an unknown path.
fn parse_target(
    path: &str,
    query: &HashMap<String, String>,
) -> Result<Option<Tombstone>, HttpResponse> {
    let (prefix, raw_id) = match [CLAIMS_PREFIX, EVIDENCE_PREFIX, TENANTS_PREFIX]
        .iter()
        .find_map(|prefix| path.strip_prefix(prefix).map(|rest| (*prefix, rest)))
    {
        Some(found) => found,
        None => return Ok(None),
    };
    if raw_id.is_empty() || raw_id.contains('/') {
        return Ok(None);
    }
    let id = decode_segment(raw_id).map_err(|reason| HttpResponse::bad_request(&reason))?;
    if id.trim().is_empty() {
        return Err(HttpResponse::bad_request(
            "identifier in path must not be empty",
        ));
    }
    if prefix == TENANTS_PREFIX {
        if query.contains_key("tenant_id") {
            return Err(HttpResponse::bad_request(
                "DELETE /v1/tenants/{tenant_id} takes the tenant from the path, not the query",
            ));
        }
        return Ok(Some(Tombstone::Tenant { tenant_id: id }));
    }
    let tenant_id = match query.get("tenant_id").map(|t| t.trim()) {
        Some(tenant) if !tenant.is_empty() => tenant.to_string(),
        _ => {
            return Err(HttpResponse::bad_request(
                "query parameter 'tenant_id' is required",
            ));
        }
    };
    Ok(Some(if prefix == CLAIMS_PREFIX {
        Tombstone::Claim {
            tenant_id,
            claim_id: id,
        }
    } else {
        Tombstone::Evidence {
            tenant_id,
            evidence_id: id,
        }
    }))
}

fn audit_action(tombstone: &Tombstone) -> &'static str {
    match tombstone {
        Tombstone::Claim { .. } => "delete_claim",
        Tombstone::Evidence { .. } => "delete_evidence",
        Tombstone::Tenant { .. } => "delete_tenant",
    }
}

fn required_role(tombstone: &Tombstone) -> Role {
    match tombstone {
        Tombstone::Tenant { .. } => Role::Admin,
        Tombstone::Claim { .. } | Tombstone::Evidence { .. } => Role::Ingest,
    }
}

pub(super) fn handle_delete(
    runtime: &SharedRuntime,
    request: &HttpRequest,
    path: &str,
    query: &HashMap<String, String>,
    auth_policy: &AuthPolicy,
    audit_log_path: Option<&str>,
) -> HttpResponse {
    let tombstone = match parse_target(path, query) {
        Ok(Some(tombstone)) => tombstone,
        Ok(None) => return HttpResponse::not_found("unknown path"),
        Err(response) => return response,
    };
    let write_consistency = match parse_write_consistency(query) {
        Ok(value) => value,
        Err(reason) => return HttpResponse::bad_request(&reason),
    };
    if let Some(rejected) =
        reject_oversized_identifiers(&[tombstone.tenant_id(), tombstone.target_id()])
    {
        return rejected;
    }
    let action = audit_action(&tombstone);
    let tenant_id = tombstone.tenant_id().to_string();
    let claim_id = match &tombstone {
        Tombstone::Claim { claim_id, .. } => Some(claim_id.clone()),
        _ => None,
    };
    let audit = |status: u16, outcome: &str, reason: &str| {
        emit_audit_event(
            runtime,
            audit_log_path,
            AuditEvent {
                action,
                tenant_id: Some(&tenant_id),
                claim_id: claim_id.as_deref(),
                status,
                outcome,
                reason,
            },
        );
    };

    match authorize_request_for_tenant(request, &tenant_id, auth_policy, required_role(&tombstone))
    {
        AuthDecision::Unauthorized(reason) => {
            observe_auth_failure(runtime);
            audit(401, "denied", reason);
            return HttpResponse::unauthorized(reason);
        }
        AuthDecision::Forbidden(reason) => {
            observe_authz_denied(runtime);
            audit(403, "denied", reason);
            return HttpResponse::forbidden(reason);
        }
        AuthDecision::RateLimited { retry_after_secs } => {
            observe_authz_denied(runtime);
            audit(429, "denied", "rate limit exceeded");
            return HttpResponse::too_many_requests("rate limit exceeded", retry_after_secs);
        }
        AuthDecision::Allowed => observe_auth_success(runtime),
    }

    refresh_placement(runtime);
    let mut guard = match group_commit::lock_drained(runtime) {
        Ok(guard) => guard,
        Err(_) => {
            audit(500, "error", "runtime lock unavailable");
            return HttpResponse::internal_server_error("failed to acquire ingestion runtime lock");
        }
    };
    guard.flush_wal_if_due();
    let outcome = guard.delete(&tombstone, write_consistency);
    let (response, status, outcome_label, reason) = match outcome {
        Ok(result) => {
            guard.delete_metrics.observe(&tombstone, &result.stats);
            let stats = result.stats;
            let body = DeleteResponse {
                deleted: !stats.is_empty(),
                scope: tombstone.scope(),
                tenant_id: &tenant_id,
                claim_id: claim_id.as_deref(),
                evidence_id: match &tombstone {
                    Tombstone::Evidence { evidence_id, .. } => Some(evidence_id.as_str()),
                    _ => None,
                },
                claims_deleted: stats.claims,
                evidence_deleted: stats.evidence,
                edges_deleted: stats.edges,
                vectors_deleted: stats.vectors,
                claims_total: result.claims_total,
                checkpoint_triggered: result.checkpoint_triggered,
                checkpoint_deferred: result.checkpoint_deferred,
            };
            let reason = format!(
                "{} {} (deleted={}, claims={}, evidence={}, edges={}, vectors={})",
                tombstone.scope(),
                if stats.is_empty() {
                    "not found"
                } else {
                    "deleted"
                },
                !stats.is_empty(),
                stats.claims,
                stats.evidence,
                stats.edges,
                stats.vectors
            );
            let json = serde_json::to_string(&body).unwrap_or_else(|_| "{}".to_string());
            (HttpResponse::ok_json(json), 200, "success", reason)
        }
        Err(DeleteFailure::Store(err)) => {
            guard.observe_failure();
            let (status, message) = map_store_error(&err);
            (
                HttpResponse::error_with_status(status, &message),
                status,
                "error",
                message,
            )
        }
        Err(DeleteFailure::Route(route_err)) => {
            guard.observe_failure();
            guard.observe_write_route_rejection(&route_err);
            let (status, message) = map_write_route_error(&route_err);
            (
                HttpResponse::error_with_status(status, &message),
                status,
                "denied",
                message,
            )
        }
    };
    drop(guard);
    audit(status, outcome_label, &reason);
    response
}

impl IngestionRuntime {
    /// Checks that this node may apply `tombstone`, then deletes: WAL first,
    /// then memory and redb, then a checkpoint check and a segment refresh.
    /// The caller holds the drained runtime lock.
    fn delete(
        &mut self,
        tombstone: &Tombstone,
        write_consistency: WriteConsistencyPolicy,
    ) -> Result<DeleteResult, DeleteFailure> {
        self.ensure_local_delete_route(tombstone, write_consistency)
            .map_err(DeleteFailure::Route)?;
        let prepared = self
            .store
            .prepare_delete(tombstone.clone(), unix_timestamp_millis())
            .map_err(DeleteFailure::Store)?;
        if prepared.is_noop() {
            return Ok(DeleteResult {
                stats: DeleteStats::default(),
                claims_total: self.store.claims_len(),
                checkpoint_triggered: false,
                checkpoint_deferred: false,
            });
        }
        let (stats, checkpoint) = match self.wal.as_ref() {
            Some(wal) => {
                lock_wal(wal)
                    .append_group_lines(prepared.wal_lines())
                    .map_err(DeleteFailure::Store)?;
                let outcome = self
                    .store
                    .apply_prepared_delete(prepared)
                    .map_err(DeleteFailure::Store)?;
                if let Some(reason) = outcome.disk_error {
                    eprintln!("ingestion redb mirror failed after WAL commit: {reason}");
                }
                (outcome.stats, self.checkpoint_after_commit("delete"))
            }
            None => (
                self.store
                    .delete(tombstone.clone())
                    .map_err(DeleteFailure::Store)?,
                (None, false),
            ),
        };
        self.publish_segments_for_tenant(tombstone.tenant_id());
        Ok(DeleteResult {
            stats,
            claims_total: self.store.claims_len(),
            checkpoint_triggered: checkpoint.0.is_some(),
            checkpoint_deferred: checkpoint.1,
        })
    }

    /// With placement routing, a delete must run on the write leader:
    ///
    /// - a claim delete is routed like an ingest of the stored claim (a claim
    ///   this node does not hold is not deleted here: `deleted: false`);
    /// - an evidence or tenant delete touches every shard of the tenant this
    ///   node holds, so this node must lead each of them. In a sharded
    ///   deployment, send it to the leader of every shard of the tenant.
    fn ensure_local_delete_route(
        &mut self,
        tombstone: &Tombstone,
        write_consistency: WriteConsistencyPolicy,
    ) -> Result<(), WriteRouteError> {
        let routing_enabled = match self.placement_routing.as_ref() {
            Ok(state) => state.is_some(),
            Err(reason) => return Err(WriteRouteError::Config(reason.clone())),
        };
        if !routing_enabled {
            return Ok(());
        }
        if let Tombstone::Claim {
            tenant_id,
            claim_id,
        } = tombstone
        {
            let Some(claim) = self
                .store
                .claim_by_id(claim_id)
                .filter(|claim| claim.tenant_id == *tenant_id)
                .cloned()
            else {
                return Ok(());
            };
            return self
                .ensure_local_write_route_for_claim(&claim, write_consistency)
                .map(|_| ());
        }
        let Ok(Some(state)) = self.placement_routing.as_ref() else {
            return Ok(());
        };
        state.check_fresh()?;
        let routing = state.runtime();
        for placement in routing
            .placements
            .iter()
            .filter(|placement| placement.tenant_id == tombstone.tenant_id())
        {
            let Some(local) = placement
                .replicas
                .iter()
                .find(|replica| replica.node_id == routing.local_node_id)
            else {
                continue;
            };
            if local.role != ReplicaRole::Leader {
                let leader = placement
                    .replicas
                    .iter()
                    .find(|replica| replica.role == ReplicaRole::Leader);
                return Err(match leader {
                    Some(leader) => WriteRouteError::WrongNode {
                        local_node_id: routing.local_node_id.clone(),
                        target_node_id: leader.node_id.clone(),
                        shard_id: placement.shard_id,
                        epoch: placement.epoch,
                        role: local.role,
                    },
                    None => WriteRouteError::Placement(PlacementRouteError::NoWritableLeader {
                        tenant_id: placement.tenant_id.clone(),
                        shard_id: placement.shard_id,
                    }),
                });
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn query(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn targets_parse_from_path_and_query() {
        let q = query(&[("tenant_id", "t1")]);
        assert_eq!(
            parse_target("/v1/claims/c%2F1+x", &q).ok().flatten(),
            Some(Tombstone::Claim {
                tenant_id: "t1".into(),
                claim_id: "c/1+x".into()
            })
        );
        assert_eq!(
            parse_target("/v1/evidence/e1", &q).ok().flatten(),
            Some(Tombstone::Evidence {
                tenant_id: "t1".into(),
                evidence_id: "e1".into()
            })
        );
        assert_eq!(
            parse_target("/v1/tenants/t%201", &HashMap::new())
                .ok()
                .flatten(),
            Some(Tombstone::Tenant {
                tenant_id: "t 1".into()
            })
        );
        // Unknown shapes are 404s, bad input 400s.
        assert!(matches!(parse_target("/v1/claims/", &q), Ok(None)));
        assert!(matches!(parse_target("/v1/claims/a/b", &q), Ok(None)));
        assert!(matches!(parse_target("/v1/other/a", &q), Ok(None)));
        for (path, q) in [
            ("/v1/claims/c1", HashMap::new()),
            ("/v1/claims/c1", query(&[("tenant_id", " ")])),
            ("/v1/claims/%zz", q.clone()),
            ("/v1/claims/%20", q.clone()),
            ("/v1/tenants/t1", q.clone()),
        ] {
            let err = parse_target(path, &q).expect_err(path);
            assert_eq!(err.status, 400, "{path}");
        }
    }
}
