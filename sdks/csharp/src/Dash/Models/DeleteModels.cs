using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Dash;

// ---------------------------------------------------------------------------
// /v1/delete
// ---------------------------------------------------------------------------

/// <summary>
/// Request body for <c>POST /v1/delete</c>. Either <see cref="ClaimIds"/>
/// or <see cref="SourceIds"/> may be supplied; when both are present
/// the server treats them as a union.
/// </summary>
public sealed record DeleteRequest
{
    [JsonPropertyName("tenant_id")]
    public required string TenantId { get; init; }

    [JsonPropertyName("claim_ids")]
    public IReadOnlyList<string>? ClaimIds { get; init; }

    [JsonPropertyName("source_ids")]
    public IReadOnlyList<string>? SourceIds { get; init; }
}

/// <summary>
/// Response body for <c>POST /v1/delete</c>. Reports the number of
/// entities that the server actually removed.
/// </summary>
public sealed record DeleteResponse
{
    [JsonPropertyName("claims_deleted")]
    public int ClaimsDeleted { get; init; }

    [JsonPropertyName("evidence_deleted")]
    public int EvidenceDeleted { get; init; }
}
