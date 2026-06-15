package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body for {@code POST /v1/delete}.
 *
 * <p>Reports per-claim delete status so callers can distinguish
 * "not found" (idempotent retry) from "rejected" (e.g. cross-tenant
 * id).</p>
 */
public record DeleteResponse(
        @JsonProperty("deleted") int deleted,
        @JsonProperty("missing") int missing,
        @JsonProperty("results") List<DeleteResult> results) {

    public record DeleteResult(
            @JsonProperty("claim_id") String claimId,
            @JsonProperty("status") String status) {
    }
}
