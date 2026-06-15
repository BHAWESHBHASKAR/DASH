using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Dash;

// ---------------------------------------------------------------------------
// /v1/ingest
// ---------------------------------------------------------------------------

/// <summary>
/// A single piece of evidence that supports, contradicts, or is
/// neutral toward a claim. Mirrors <c>schema::Citation</c>.
/// </summary>
public sealed record IngestEvidence
{
    [JsonPropertyName("evidence_id")]
    public required string EvidenceId { get; init; }

    /// <summary>One of <c>"supports"</c>, <c>"contradicts"</c>, <c>"neutral"</c>.</summary>
    [JsonPropertyName("stance")]
    public required string Stance { get; init; }

    [JsonPropertyName("source_id")]
    public required string SourceId { get; init; }

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
}

/// <summary>
/// A single claim with the evidence that backs it. The
/// <see cref="Evidence"/> list carries the stance tally; the
/// server aggregates <c>supports</c> and <c>contradicts</c> counts
/// for use by the retrieve endpoint.
/// </summary>
public sealed record IngestClaim
{
    [JsonPropertyName("claim_id")]
    public required string ClaimId { get; init; }

    [JsonPropertyName("text")]
    public required string Text { get; init; }

    [JsonPropertyName("evidence")]
    public IReadOnlyList<IngestEvidence>? Evidence { get; init; }
}

/// <summary>
/// A logical grouping of claims under a single source. Bundles are
/// the unit the server uses for batching and idempotency.
/// </summary>
public sealed record IngestBundle
{
    [JsonPropertyName("source_id")]
    public required string SourceId { get; init; }

    [JsonPropertyName("title")]
    public string? Title { get; init; }

    [JsonPropertyName("claims")]
    public required IReadOnlyList<IngestClaim> Claims { get; init; }
}

/// <summary>
/// Request body for <c>POST /v1/ingest</c>.
/// </summary>
public sealed record IngestRequest
{
    [JsonPropertyName("tenant_id")]
    public required string TenantId { get; init; }

    [JsonPropertyName("bundles")]
    public required IReadOnlyList<IngestBundle> Bundles { get; init; }
}

/// <summary>
/// Response body for <c>POST /v1/ingest</c>. Reports the number of
/// each entity type that the server actually persisted.
/// </summary>
public sealed record IngestResponse
{
    [JsonPropertyName("bundles_ingested")]
    public required int BundlesIngested { get; init; }

    [JsonPropertyName("claims_ingested")]
    public required int ClaimsIngested { get; init; }

    [JsonPropertyName("evidence_ingested")]
    public required int EvidenceIngested { get; init; }
}
