using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Dash;

// ---------------------------------------------------------------------------
// /v1/ingest  (EXPERIMENTAL)
//
// Served by the ingestion service (default port 8081), not the retrieval
// service. Wire contract: services/ingestion/src/api.rs
// (IngestApiRequestWire, ClaimWire, EvidenceWire, ClaimEdgeWire,
// IngestApiResponse).
// ---------------------------------------------------------------------------

/// <summary>The <c>claim</c> object of an ingest request.</summary>
public sealed record IngestClaim
{
    [JsonPropertyName("claim_id")]
    public required string ClaimId { get; init; }

    [JsonPropertyName("tenant_id")]
    public required string TenantId { get; init; }

    [JsonPropertyName("canonical_text")]
    public required string CanonicalText { get; init; }

    /// <summary>Claim confidence in [0, 1].</summary>
    [JsonPropertyName("confidence")]
    public required double Confidence { get; init; }

    [JsonPropertyName("event_time_unix")]
    public long? EventTimeUnix { get; init; }

    [JsonPropertyName("entities")]
    public IReadOnlyList<string>? Entities { get; init; }

    [JsonPropertyName("embedding_ids")]
    public IReadOnlyList<string>? EmbeddingIds { get; init; }

    /// <summary>One of factual, opinion, prediction, temporal, causal.</summary>
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

    [JsonPropertyName("embedding_vector")]
    public IReadOnlyList<float>? EmbeddingVector { get; init; }
}

/// <summary>An <c>evidence</c> item of an ingest request.</summary>
public sealed record IngestEvidence
{
    [JsonPropertyName("evidence_id")]
    public required string EvidenceId { get; init; }

    [JsonPropertyName("claim_id")]
    public required string ClaimId { get; init; }

    [JsonPropertyName("source_id")]
    public required string SourceId { get; init; }

    /// <summary>One of <c>"supports"</c>, <c>"contradicts"</c>, <c>"neutral"</c>.</summary>
    [JsonPropertyName("stance")]
    public required string Stance { get; init; }

    /// <summary>Source quality in [0, 1].</summary>
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

/// <summary>An <c>edges</c> item of an ingest request.</summary>
public sealed record IngestEdge
{
    [JsonPropertyName("edge_id")]
    public required string EdgeId { get; init; }

    [JsonPropertyName("from_claim_id")]
    public required string FromClaimId { get; init; }

    [JsonPropertyName("to_claim_id")]
    public required string ToClaimId { get; init; }

    /// <summary>One of supports, contradicts, refines, duplicates, depends_on.</summary>
    [JsonPropertyName("relation")]
    public required string Relation { get; init; }

    [JsonPropertyName("strength")]
    public required double Strength { get; init; }

    [JsonPropertyName("reason_codes")]
    public IReadOnlyList<string>? ReasonCodes { get; init; }

    [JsonPropertyName("created_at")]
    public long? CreatedAt { get; init; }
}

/// <summary>
/// Request body for <c>POST {IngestionBaseUrl}/v1/ingest</c>:
/// <c>{"claim": {...}, "evidence": [...], "edges": [...]}</c>.
/// </summary>
public sealed record IngestRequest
{
    [JsonPropertyName("claim")]
    public required IngestClaim Claim { get; init; }

    [JsonPropertyName("evidence")]
    public IReadOnlyList<IngestEvidence>? Evidence { get; init; }

    [JsonPropertyName("edges")]
    public IReadOnlyList<IngestEdge>? Edges { get; init; }
}

/// <summary>Response body for <c>POST /v1/ingest</c>.</summary>
public sealed record IngestResponse
{
    [JsonPropertyName("ingested_claim_id")]
    public string IngestedClaimId { get; init; } = string.Empty;

    [JsonPropertyName("claims_total")]
    public int ClaimsTotal { get; init; }

    [JsonPropertyName("commit_epoch")]
    public long? CommitEpoch { get; init; }

    [JsonPropertyName("ack_count")]
    public int AckCount { get; init; }

    [JsonPropertyName("required_acks")]
    public int RequiredAcks { get; init; }

    /// <summary>Replication progress, e.g. <c>"committed"</c>.</summary>
    [JsonPropertyName("commit_status")]
    public string CommitStatus { get; init; } = string.Empty;

    [JsonPropertyName("checkpoint_triggered")]
    public bool CheckpointTriggered { get; init; }

    [JsonPropertyName("checkpoint_snapshot_records")]
    public int? CheckpointSnapshotRecords { get; init; }

    [JsonPropertyName("checkpoint_truncated_wal_records")]
    public int? CheckpointTruncatedWalRecords { get; init; }
}
