using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Dash;

// ---------------------------------------------------------------------------
// /v1/retrieve
//
// Wire contract: services/retrieval/src/transport/payload.rs
// (build_retrieve_transport_request_from_json and
// render_retrieve_response_json / render_evidence_node_json /
// render_citations_json). Response models are tolerant: unknown fields are
// ignored and missing optional fields keep their defaults.
// ---------------------------------------------------------------------------

/// <summary>
/// A single citation attached to a retrieval result.
/// </summary>
public sealed record Citation
{
    [JsonPropertyName("evidence_id")]
    public string EvidenceId { get; init; } = string.Empty;

    [JsonPropertyName("source_id")]
    public string SourceId { get; init; } = string.Empty;

    /// <summary>One of <c>"supports"</c>, <c>"contradicts"</c>, <c>"neutral"</c>.</summary>
    [JsonPropertyName("stance")]
    public string Stance { get; init; } = string.Empty;

    [JsonPropertyName("source_quality")]
    public double SourceQuality { get; init; }

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
/// A single claim returned by <c>/v1/retrieve</c> (an "evidence node" in
/// server terms). <see cref="Supports"/> and <see cref="Contradicts"/> are
/// stance tallies so callers can filter without walking
/// <see cref="Citations"/>. Properties after <see cref="Citations"/> are
/// optional and null when the server omits them.
/// </summary>
public sealed record RetrievalHit
{
    [JsonPropertyName("claim_id")]
    public string ClaimId { get; init; } = string.Empty;

    [JsonPropertyName("canonical_text")]
    public string CanonicalText { get; init; } = string.Empty;

    /// <summary>Ranking score; the server emits a plain number.</summary>
    [JsonPropertyName("score")]
    public double Score { get; init; }

    [JsonPropertyName("supports")]
    public int Supports { get; init; }

    [JsonPropertyName("contradicts")]
    public int Contradicts { get; init; }

    [JsonPropertyName("citations")]
    public IReadOnlyList<Citation> Citations { get; init; } = new List<Citation>();

    [JsonPropertyName("claim_confidence")]
    public double? ClaimConfidence { get; init; }

    [JsonPropertyName("confidence_band")]
    public string? ConfidenceBand { get; init; }

    [JsonPropertyName("dominant_stance")]
    public string? DominantStance { get; init; }

    [JsonPropertyName("contradiction_risk")]
    public double? ContradictionRisk { get; init; }

    [JsonPropertyName("graph_score")]
    public double? GraphScore { get; init; }

    [JsonPropertyName("support_path_count")]
    public int? SupportPathCount { get; init; }

    [JsonPropertyName("contradiction_chain_depth")]
    public int? ContradictionChainDepth { get; init; }

    [JsonPropertyName("event_time_unix")]
    public long? EventTimeUnix { get; init; }

    [JsonPropertyName("temporal_match_mode")]
    public string? TemporalMatchMode { get; init; }

    [JsonPropertyName("temporal_in_range")]
    public bool? TemporalInRange { get; init; }

    [JsonPropertyName("claim_type")]
    public string? ClaimType { get; init; }

    [JsonPropertyName("valid_from")]
    public long? ValidFrom { get; init; }

    [JsonPropertyName("valid_to")]
    public long? ValidTo { get; init; }

    [JsonPropertyName("created_at")]
    public long? CreatedAt { get; init; }

    [JsonPropertyName("updated_at")]
    public long? UpdatedAt { get; init; }
}

/// <summary>An edge of the evidence graph returned when <c>return_graph</c> is set.</summary>
public sealed record GraphEdge
{
    [JsonPropertyName("from_claim_id")]
    public string FromClaimId { get; init; } = string.Empty;

    [JsonPropertyName("to_claim_id")]
    public string ToClaimId { get; init; } = string.Empty;

    [JsonPropertyName("relation")]
    public string Relation { get; init; } = string.Empty;

    [JsonPropertyName("strength")]
    public double Strength { get; init; }
}

/// <summary>Evidence graph; present only when <c>return_graph</c> was true.</summary>
public sealed record RetrievalGraph
{
    [JsonPropertyName("nodes")]
    public IReadOnlyList<RetrievalHit> Nodes { get; init; } = new List<RetrievalHit>();

    [JsonPropertyName("edges")]
    public IReadOnlyList<GraphEdge> Edges { get; init; } = new List<GraphEdge>();
}

/// <summary>Inclusive unix-second window; either bound may be null.</summary>
public sealed record TimeRange
{
    [JsonPropertyName("from_unix")]
    public long? FromUnix { get; init; }

    [JsonPropertyName("to_unix")]
    public long? ToUnix { get; init; }
}

/// <summary>
/// Request body for <c>POST /v1/retrieve</c>. Every property except
/// <see cref="TenantId"/> and <see cref="Query"/> is optional and omitted
/// from the JSON when null, so server defaults apply (<c>top_k</c> 5,
/// <c>stance_mode</c> <c>balanced</c>, <c>return_graph</c> false).
/// </summary>
public sealed record RetrievalRequest
{
    [JsonPropertyName("tenant_id")]
    public required string TenantId { get; init; }

    [JsonPropertyName("query")]
    public required string Query { get; init; }

    /// <summary>Maximum number of claims to return. Omitted (server default 5) when null.</summary>
    [JsonPropertyName("top_k")]
    public int? TopK { get; init; }

    /// <summary><c>"balanced"</c> or <c>"support_only"</c>. Omitted when null.</summary>
    [JsonPropertyName("stance_mode")]
    public string? StanceMode { get; init; }

    [JsonPropertyName("return_graph")]
    public bool? ReturnGraph { get; init; }

    [JsonPropertyName("query_embedding")]
    public IReadOnlyList<float>? QueryEmbedding { get; init; }

    [JsonPropertyName("entity_filters")]
    public IReadOnlyList<string>? EntityFilters { get; init; }

    [JsonPropertyName("embedding_id_filters")]
    public IReadOnlyList<string>? EmbeddingIdFilters { get; init; }

    [JsonPropertyName("time_range")]
    public TimeRange? TimeRange { get; init; }

    [JsonPropertyName("read_consistency")]
    public string? ReadConsistency { get; init; }
}

/// <summary>
/// Response body for <c>POST /v1/retrieve</c>. Wire format is
/// <c>{"results": [...], "graph": ..., "read_policy": ..., ...}</c>.
/// </summary>
public sealed record RetrievalResponse
{
    [JsonPropertyName("results")]
    public IReadOnlyList<RetrievalHit> Results { get; init; } = new List<RetrievalHit>();

    [JsonPropertyName("graph")]
    public RetrievalGraph? Graph { get; init; }

    [JsonPropertyName("read_policy")]
    public string? ReadPolicy { get; init; }

    [JsonPropertyName("read_quorum_met")]
    public bool? ReadQuorumMet { get; init; }

    [JsonPropertyName("serving_replica")]
    public string? ServingReplica { get; init; }
}
