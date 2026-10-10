package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single claim returned by {@code /v1/retrieve}
 * ({@code render_evidence_node_json} in the retrieval service).
 *
 * <p>{@code supports} and {@code contradicts} give the stance tally for
 * the claim without walking citations. Fields after {@code citations}
 * are optional and null when the server omits them.</p>
 */
public record RetrievalHit(
        @JsonProperty("claim_id") String claimId,
        @JsonProperty("canonical_text") String canonicalText,
        @JsonProperty("score") double score,
        @JsonProperty("supports") int supports,
        @JsonProperty("contradicts") int contradicts,
        @JsonProperty("citations") List<RetrievalScore> citations,
        @JsonProperty("claim_confidence") Double claimConfidence,
        @JsonProperty("confidence_band") String confidenceBand,
        @JsonProperty("dominant_stance") String dominantStance,
        @JsonProperty("contradiction_risk") Double contradictionRisk,
        @JsonProperty("graph_score") Double graphScore,
        @JsonProperty("support_path_count") Integer supportPathCount,
        @JsonProperty("contradiction_chain_depth") Integer contradictionChainDepth,
        @JsonProperty("event_time_unix") Long eventTimeUnix,
        @JsonProperty("temporal_match_mode") String temporalMatchMode,
        @JsonProperty("temporal_in_range") Boolean temporalInRange,
        @JsonProperty("claim_type") String claimType,
        @JsonProperty("valid_from") Long validFrom,
        @JsonProperty("valid_to") Long validTo,
        @JsonProperty("created_at") Long createdAt,
        @JsonProperty("updated_at") Long updatedAt) {
}
