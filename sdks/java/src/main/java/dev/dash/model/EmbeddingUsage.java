package dev.dash.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Token usage block from an embeddings response.
 *
 * <p>Mirrors {@code OpenAIUsage}.</p>
 */
public record EmbeddingUsage(
        @JsonProperty("prompt_tokens") int promptTokens,
        @JsonProperty("total_tokens") int totalTokens) {
}
