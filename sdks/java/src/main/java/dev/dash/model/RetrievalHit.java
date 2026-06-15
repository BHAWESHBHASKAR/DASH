package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A single claim returned by {@code /v1/retrieve}.
 *
 * <p>Mirrors {@code schema::RetrievalResult}. The <b>Claim + Evidence +
 * Contradiction</b> differentiator lives here: {@code supports} and
 * {@code contradicts} give the caller the stance tally for the claim
 * without having to walk citations manually.</p>
 */
public record RetrievalHit(
        @JsonProperty("claim_id") String claimId,
        @JsonProperty("canonical_text") String canonicalText,
        @JsonProperty("score") double score,
        @JsonProperty("supports") int supports,
        @JsonProperty("contradicts") int contradicts,
        @JsonProperty("citations") List<RetrievalScore> citations) {
}
