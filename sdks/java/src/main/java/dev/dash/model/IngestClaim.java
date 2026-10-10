package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The {@code claim} object of {@code POST /v1/ingest}.
 *
 * <p>Mirrors {@code ClaimWire} in {@code services/ingestion/src/api.rs}.
 * {@code claimId}, {@code tenantId}, {@code canonicalText} and
 * {@code confidence} (0..1) are required by the server; the rest are
 * optional and omitted from the JSON when null. {@code claimType} is one
 * of {@code factual, opinion, prediction, temporal, causal}.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record IngestClaim(
        @JsonProperty("claim_id") String claimId,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("canonical_text") String canonicalText,
        @JsonProperty("confidence") double confidence,
        @JsonProperty("event_time_unix") Long eventTimeUnix,
        @JsonProperty("entities") List<String> entities,
        @JsonProperty("embedding_ids") List<String> embeddingIds,
        @JsonProperty("claim_type") String claimType,
        @JsonProperty("valid_from") Long validFrom,
        @JsonProperty("valid_to") Long validTo,
        @JsonProperty("created_at") Long createdAt,
        @JsonProperty("updated_at") Long updatedAt,
        @JsonProperty("embedding_vector") List<Float> embeddingVector) {

    /** Minimal claim with only the required fields. */
    public IngestClaim(String claimId, String tenantId, String canonicalText, double confidence) {
        this(claimId, tenantId, canonicalText, confidence,
                null, null, null, null, null, null, null, null, null);
    }
}
