package dev.dash.model;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A piece of evidence attached to an ingested claim.
 *
 * <p>{@code stance} is one of {@code "supports"}, {@code "contradicts"},
 * or {@code "neutral"} and mirrors the same enum used in
 * {@link RetrievalScore}.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record IngestEvidence(
        @JsonProperty("evidence_id") String evidenceId,
        @JsonProperty("source_id") String sourceId,
        @JsonProperty("stance") String stance,
        @JsonProperty("source_quality") Double sourceQuality,
        @JsonProperty("chunk_id") String chunkId,
        @JsonProperty("span_start") Integer spanStart,
        @JsonProperty("span_end") Integer spanEnd,
        @JsonProperty("doc_id") String docId,
        @JsonProperty("extraction_model") String extractionModel,
        @JsonProperty("raw_text") String rawText) {
}
