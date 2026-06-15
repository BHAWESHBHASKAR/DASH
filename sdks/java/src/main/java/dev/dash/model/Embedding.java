package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Single embedding record from a response.
 *
 * <p>Mirrors {@code OpenAIEmbeddingData}. This is an alias for
 * {@link EmbeddingData} kept for naming consistency with the wire
 * shape; both refer to the same on-the-wire JSON object.</p>
 */
public record Embedding(
        @JsonProperty("object") String object,
        @JsonProperty("embedding") List<Double> embedding,
        @JsonProperty("index") int index) {
}
