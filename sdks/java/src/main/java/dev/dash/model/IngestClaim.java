package dev.dash.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A claim being ingested.
 *
 * <p>Mirrors the canonical claim fields used by the DASH store.
 * {@code sourceQuality} is a 0..1 signal that downstream ranking
 * uses as a prior.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record IngestClaim(
        @JsonProperty("claim_id") String claimId,
        @JsonProperty("tenant_id") String tenantId,
        @JsonProperty("canonical_text") String canonicalText,
        @JsonProperty("source_quality") Double sourceQuality,
        @JsonProperty("status") String status) {
}
