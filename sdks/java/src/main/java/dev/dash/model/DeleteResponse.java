package dev.dash.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body of {@code DELETE /v1/claims/{id}}, {@code DELETE
 * /v1/evidence/{id}} and {@code DELETE /v1/tenants/{id}} on the ingestion
 * service.
 *
 * <p>Deletes are idempotent: {@code deleted} is false (with HTTP 200) when
 * the target did not exist, including a claim of another tenant.
 * {@code claimId} and {@code evidenceId} are null unless the scope names
 * one.</p>
 */
public record DeleteResponse(
        @JsonProperty("deleted") boolean deleted,
        @JsonProperty("scope") String scope,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("claim_id") String claimId,
        @JsonProperty("evidence_id") String evidenceId,
        @JsonProperty("claims_deleted") int claimsDeleted,
        @JsonProperty("evidence_deleted") int evidenceDeleted,
        @JsonProperty("edges_deleted") int edgesDeleted,
        @JsonProperty("vectors_deleted") int vectorsDeleted,
        @JsonProperty("claims_total") int claimsTotal,
        @JsonProperty("checkpoint_triggered") boolean checkpointTriggered,
        @JsonProperty("checkpoint_deferred") boolean checkpointDeferred) {
}
