namespace Dash;

/// <summary>
/// Per-request options controlling retry behaviour.
///
/// The SDK only retries a failed request (network error, HTTP 429/5xx)
/// when it is safe to do so: the operation is naturally idempotent
/// (embeddings, retrieve, health) or the caller supplied an
/// <see cref="IdempotencyKey"/> or explicitly opted in with
/// <see cref="Retry"/>. <c>POST /v1/ingest</c> is therefore sent exactly
/// once by default.
/// </summary>
public sealed class RequestOptions
{
    /// <summary>Single attempt, no retries (default for writes).</summary>
    public static RequestOptions None { get; } = new();

    /// <summary>Naturally idempotent request; retries are allowed.</summary>
    internal static RequestOptions Idempotent { get; } = new() { Retry = true };

    /// <summary>
    /// Value sent as the <c>Idempotency-Key</c> header. Setting it allows
    /// retries.
    /// </summary>
    public string? IdempotencyKey { get; init; }

    /// <summary>Explicit opt-in to retries for a non-idempotent request.</summary>
    public bool Retry { get; init; }

    internal bool CanRetry => Retry || !string.IsNullOrWhiteSpace(IdempotencyKey);
}
