using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Dash;

// ---------------------------------------------------------------------------
// /health
// ---------------------------------------------------------------------------

/// <summary>
/// Response body for <c>GET /health</c>. <see cref="Status"/> is the
/// only field the SDK inspects; everything else is forwarded
/// verbatim so callers can read deployment-specific diagnostics.
/// </summary>
public sealed record HealthResponse
{
    /// <summary>One of <c>"ok"</c>, <c>"degraded"</c>, <c>"unhealthy"</c>.</summary>
    [JsonPropertyName("status")]
    public required string Status { get; init; }

    [JsonPropertyName("version")]
    public string? Version { get; init; }

    [JsonPropertyName("details")]
    public IReadOnlyDictionary<string, object>? Details { get; init; }
}
