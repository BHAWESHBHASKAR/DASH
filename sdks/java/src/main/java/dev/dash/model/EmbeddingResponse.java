package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Response body for {@code POST /v1/embeddings}.
 *
 * <p>Mirrors {@code OpenAIEmbeddingsResponse}.</p>
 */
public record EmbeddingResponse(
        @JsonProperty("object") String object,
        @JsonProperty("data") List<EmbeddingData> data,
        @JsonProperty("model") String model,
        @JsonProperty("usage") EmbeddingUsage usage) {
}
