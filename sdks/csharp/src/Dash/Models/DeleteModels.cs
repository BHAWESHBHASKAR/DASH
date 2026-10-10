using System.Text.Json.Serialization;

namespace Dash;

// ---------------------------------------------------------------------------
// DELETE /v1/claims/{claim_id}?tenant_id=...
// DELETE /v1/evidence/{evidence_id}?tenant_id=...
// DELETE /v1/tenants/{tenant_id}
//
// Served by the ingestion service (default port 8081). Deletes take no body.
// ---------------------------------------------------------------------------

/// <summary>
/// Response body of <c>DELETE /v1/claims/{id}</c>,
/// <c>DELETE /v1/evidence/{id}</c> and <c>DELETE /v1/tenants/{id}</c> on the
/// ingestion service.
///
/// Deletes are idempotent: <see cref="Deleted"/> is <c>false</c> (with HTTP
/// 200) when the target did not exist, including a claim of another tenant.
/// <see cref="ClaimId"/> and <see cref="EvidenceId"/> are <c>null</c> unless
/// the scope names one.
/// </summary>
public sealed record DeleteResponse
{
    /// <summary><c>true</c> when something was removed.</summary>
    [JsonPropertyName("deleted")]
    public bool Deleted { get; init; }

    /// <summary>One of <c>"claim"</c>, <c>"evidence"</c>, <c>"tenant"</c>.</summary>
    [JsonPropertyName("scope")]
    public string Scope { get; init; } = string.Empty;

    [JsonPropertyName("tenant_id")]
    public string TenantId { get; init; } = string.Empty;

    /// <summary>Set only for a claim delete.</summary>
    [JsonPropertyName("claim_id")]
    public string? ClaimId { get; init; }

    /// <summary>Set only for an evidence delete.</summary>
    [JsonPropertyName("evidence_id")]
    public string? EvidenceId { get; init; }

    [JsonPropertyName("claims_deleted")]
    public int ClaimsDeleted { get; init; }

    [JsonPropertyName("evidence_deleted")]
    public int EvidenceDeleted { get; init; }

    [JsonPropertyName("edges_deleted")]
    public int EdgesDeleted { get; init; }

    [JsonPropertyName("vectors_deleted")]
    public int VectorsDeleted { get; init; }

    /// <summary>Claims left on the node after the delete.</summary>
    [JsonPropertyName("claims_total")]
    public int ClaimsTotal { get; init; }

    [JsonPropertyName("checkpoint_triggered")]
    public bool CheckpointTriggered { get; init; }

    [JsonPropertyName("checkpoint_deferred")]
    public bool CheckpointDeferred { get; init; }
}
