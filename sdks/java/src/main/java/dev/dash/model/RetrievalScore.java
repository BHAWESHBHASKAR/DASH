package dev.dash.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A citation attached to a retrieval result.
 *
 * <p>Mirrors {@code schema::Citation}. Optional fields (chunk id,
 * span, doc id, extraction model, ingested-at) are nullable so the
 * JSON "absent" case round-trips as {@code null} rather than the
 * zero value.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record RetrievalScore(
        @JsonProperty("evidence_id") String evidenceId,
        @JsonProperty("source_id") String sourceId,
        @JsonProperty("stance") String stance,
        @JsonProperty("source_quality") double sourceQuality,
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("span_start") Integer spanStart,
        @JsonProperty("span_end") Integer spanEnd,
        @JsonProperty("doc_id") String docId,
        @JsonProperty("extraction_model") String extractionModel,
        @JsonProperty("ingested_at") Long ingestedAt) {
}
