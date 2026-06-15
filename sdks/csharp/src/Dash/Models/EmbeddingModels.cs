using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Dash;

// ---------------------------------------------------------------------------
// OpenAI-compatible /v1/embeddings
// ---------------------------------------------------------------------------

/// <summary>
/// Request body for <c>POST /v1/embeddings</c>. Mirrors
/// <c>OpenAIEmbeddingsRequest</c> in
/// <c>services/retrieval/src/openai_embeddings.rs</c>.
/// </summary>
public sealed record EmbeddingRequest
{
    /// <summary>
    /// A single string or a list of strings to embed. The DASH
    /// server accepts both shapes; we send the value through
    /// unchanged so the wire body is byte-for-byte compatible with
    /// the OpenAI spec.
    /// </summary>
    [JsonPropertyName("input")]
    public required object Input { get; init; }

    /// <summary>
    /// Model name. Defaults to <c>"text-embedding-3-small"</c> when
    /// <c>null</c>. DASH uses its configured embedding provider for
    /// the actual vector; the value is echoed back in the response.
    /// </summary>
    [JsonPropertyName("model")]
    public string? Model { get; init; }

    /// <summary>
    /// Encoding format for the returned vectors. DASH currently only
    /// supports <c>"float"</c>; other values are rejected by the
    /// server. Omit (leave <c>null</c>) for the default behaviour.
    /// </summary>
    [JsonPropertyName("encoding_format")]
    public string? EncodingFormat { get; init; }

    /// <summary>
    /// OpenAI-style opaque user identifier. Omit for the default
    /// behaviour.
    /// </summary>
    [JsonPropertyName("user")]
    public string? User { get; init; }
}

/// <summary>
/// A single embedding vector. <see cref="EmbeddingData"/> wraps an
/// embedding with index metadata on the response; this record is
/// the bare vector type used by the request model and is also a
/// useful return type for callers that do not care about the
/// OpenAI-style envelope.
/// </summary>
public sealed record Embedding
{
    [JsonPropertyName("values")]
    public required IReadOnlyList<float> Values { get; init; }
}

/// <summary>
/// Single embedding record from a response. Mirrors
/// <c>OpenAIEmbeddingData</c>.
/// </summary>
public sealed record EmbeddingData
{
    [JsonPropertyName("object")]
    public string Object { get; init; } = "embedding";

    [JsonPropertyName("embedding")]
    public required IReadOnlyList<float> Values { get; init; }

    [JsonPropertyName("index")]
    public int Index { get; init; }
}

/// <summary>
/// Token usage block returned alongside the embedding vectors.
/// Mirrors <c>OpenAIUsage</c>.
/// </summary>
public sealed record EmbeddingUsage
{
    [JsonPropertyName("prompt_tokens")]
    public required int PromptTokens { get; init; }

    [JsonPropertyName("total_tokens")]
    public required int TotalTokens { get; init; }
}

/// <summary>
/// Response body for <c>POST /v1/embeddings</c>. Mirrors
/// <c>OpenAIEmbeddingsResponse</c>.
/// </summary>
public sealed record EmbeddingResponse
{
    [JsonPropertyName("object")]
    public required string Object { get; init; }

    [JsonPropertyName("data")]
    public required IReadOnlyList<EmbeddingData> Data { get; init; }

    [JsonPropertyName("model")]
    public required string Model { get; init; }

    [JsonPropertyName("usage")]
    public required EmbeddingUsage Usage { get; init; }
}
