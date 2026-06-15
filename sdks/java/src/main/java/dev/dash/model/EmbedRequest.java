package dev.dash.model;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Request body for {@code POST /v1/embeddings}.
 *
 * <p>Mirrors {@code OpenAIEmbeddingsRequest} in
 * {@code services/retrieval/src/openai_embeddings.rs}. The {@code input}
 * field accepts either a single string or a list of strings; the value
 * is passed through to the server unchanged so the wire body stays
 * byte-for-byte compatible with the OpenAI spec.</p>
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public record EmbedRequest(
        @JsonProperty("input") Object input,
        @JsonProperty("model") String model,
        @JsonProperty("encoding_format") String encodingFormat,
        @JsonProperty("user") String user) {

    public static EmbedRequest of(String input) {
        return new EmbedRequest(input, "text-embedding-3-small", null, null);
    }

    public static EmbedRequest of(List<String> inputs) {
        return new EmbedRequest(inputs, "text-embedding-3-small", null, null);
    }
}
