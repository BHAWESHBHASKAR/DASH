package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for {@code POST /v1/delete}.
 *
 * <p>Identifies a set of claims (and optionally their evidence) to
 * remove from the tenant namespace.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record DeleteRequest(
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("claim_ids") List<String> claimIds,
        @JsonProperty("delete_evidence") Boolean deleteEvidence) {

    public DeleteRequest(String tenantId, List<String> claimIds) {
        this(tenantId, claimIds, Boolean.TRUE);
    }
}
