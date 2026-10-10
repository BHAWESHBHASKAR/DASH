package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * An {@code edges} item of {@code POST /v1/ingest}.
 *
 * <p>Mirrors {@code ClaimEdgeWire}. {@code relation} is one of
 * {@code supports, contradicts, refines, duplicates, depends_on}.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record IngestEdge(
        @JsonProperty("edge_id") String edgeId,
        @JsonProperty("from_claim_id") String fromClaimId,
        @JsonProperty("to_claim_id") String toClaimId,
        @JsonProperty("relation") String relation,
        @JsonProperty("strength") double strength,
        @JsonProperty("reason_codes") List<String> reasonCodes,
        @JsonProperty("created_at") Long createdAt) {
}
