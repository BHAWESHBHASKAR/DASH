using System.Collections.Generic;
using System.Text.Json.Serialization;

namespace Dash;

// ---------------------------------------------------------------------------
// /health
// ---------------------------------------------------------------------------

/// <summary>
/// Response body for <c>GET /health</c>. The server currently returns
/// <c>{"status":"ok"}</c>; every other field is optional.
/// </summary>
public sealed record HealthResponse
{
    [JsonPropertyName("status")]
    public string Status { get; init; } = string.Empty;

    [JsonPropertyName("version")]
    public string? Version { get; init; }

    [JsonPropertyName("details")]
    public IReadOnlyDictionary<string, object>? Details { get; init; }
}
