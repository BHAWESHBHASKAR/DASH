package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Single embedding record from a response.
 *
 * <p>Mirrors {@code OpenAIEmbeddingData}.</p>
 */
public record EmbeddingData(
        @JsonProperty("object") String object,
        @JsonProperty("embedding") List<Double> embedding,
        @JsonProperty("index") int index) {
}
