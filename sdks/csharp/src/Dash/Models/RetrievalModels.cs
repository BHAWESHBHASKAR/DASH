using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Dash;

// ---------------------------------------------------------------------------
// /v1/retrieve
// ---------------------------------------------------------------------------

/// <summary>
/// A single citation attached to a retrieval result. Mirrors
/// <c>schema::Citation</c>.
/// </summary>
public sealed record Citation
{
    [JsonPropertyName("evidence_id")]
    public required string EvidenceId { get; init; }

    [JsonPropertyName("source_id")]
    public required string SourceId { get; init; }

    /// <summary>One of <c>"supports"</c>, <c>"contradicts"</c>, <c>"neutral"</c>.</summary>
    [JsonPropertyName("stance")]
    public required string Stance { get; init; }

    [JsonPropertyName("source_quality")]
    public required double SourceQuality { get; init; }

    [JsonPropertyName("chunk_id")]
    public string? ChunkId { get; init; }

    [JsonPropertyName("span_start")]
    public int? SpanStart { get; init; }

    [JsonPropertyName("span_end")]
    public int? SpanEnd { get; init; }

    [JsonPropertyName("doc_id")]
    public string? DocId { get; init; }

    [JsonPropertyName("extraction_model")]
    public string? ExtractionModel { get; init; }

    [JsonPropertyName("ingested_at")]
    public long? IngestedAt { get; init; }
}

/// <summary>
/// A breakdown of the score components that contributed to a hit's
/// overall ranking. <see cref="Overall"/> is the composite value
/// the server uses to sort hits; <see cref="Semantic"/> and
/// <see cref="Lexical"/> expose the individual signals when the
/// server is configured to return them.
/// </summary>
public sealed record RetrievalScore
{
    [JsonPropertyName("overall")]
    public required double Overall { get; init; }

    [JsonPropertyName("semantic")]
    public double? Semantic { get; init; }

    [JsonPropertyName("lexical")]
    public double? Lexical { get; init; }
}

/// <summary>
/// A single claim returned by <c>/v1/retrieve</c>. Mirrors
/// <c>schema::RetrievalResult</c>. The Claim + Evidence +
/// Contradiction differentiator lives in
/// <see cref="Supports"/> and <see cref="Contradicts"/>: callers
/// can filter on them without walking the <see cref="Citations"/>
/// list.
/// </summary>
public sealed record RetrievalHit
{
    [JsonPropertyName("claim_id")]
    public required string ClaimId { get; init; }

    [JsonPropertyName("canonical_text")]
    public required string CanonicalText { get; init; }

    [JsonPropertyName("score")]
    public required RetrievalScore Score { get; init; }

    [JsonPropertyName("supports")]
    public int Supports { get; init; }

    [JsonPropertyName("contradicts")]
    public int Contradicts { get; init; }

    [JsonPropertyName("citations")]
    public IReadOnlyList<Citation> Citations { get; init; } = new List<Citation>();
}

/// <summary>
/// Request body for <c>POST /v1/retrieve</c>.
/// </summary>
public sealed record RetrievalRequest
{
    [JsonPropertyName("tenant_id")]
    public required string TenantId { get; init; }

    [JsonPropertyName("query")]
    public required string Query { get; init; }

    /// <summary>Maximum number of claims to return. Defaults to 10.</summary>
    [JsonPropertyName("top_k")]
    public int TopK { get; init; } = 10;

    /// <summary>One of <c>"balanced"</c> (default) or <c>"support_only"</c>.</summary>
    [JsonPropertyName("stance_mode")]
    public string StanceMode { get; init; } = "balanced";

    /// <summary>
    /// When <c>true</c>, also returns the claim graph. Servers that
    /// do not implement it silently ignore the flag.
    /// </summary>
    [JsonPropertyName("return_graph")]
    public bool? ReturnGraph { get; init; }
}

/// <summary>
/// Response body for <c>POST /v1/retrieve</c>. Wire format is
/// <c>{"hits": [...]}</c>.
/// </summary>
public sealed record RetrievalResponse
{
    [JsonPropertyName("hits")]
    public required IReadOnlyList<RetrievalHit> Hits { get; init; }
}
