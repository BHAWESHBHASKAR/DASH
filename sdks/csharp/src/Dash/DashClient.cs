using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Dash.Internal;

namespace Dash;

/// <summary>
/// Top-level DASH client. Cheap to construct and safe for
/// concurrent use; the underlying <see cref="HttpClient"/> pools
/// connections.
///
/// Both synchronous and asynchronous APIs are provided. The sync
/// methods wrap the async ones via <c>GetAwaiter().GetResult()</c>
/// — for cancellation-aware or UI-bound code, prefer the
/// <c>*Async</c> variants.
///
/// <example>
/// <code>
/// using var client = new DashClient("http://localhost:8080");
/// var response = await client.EmbedAsync(new EmbeddingRequest
/// {
///     Input = "hello world",
/// });
/// foreach (var data in response.Data)
/// {
///     // data.Values is the embedding vector.
/// }
/// </code>
/// </example>
/// </summary>
public sealed class DashClient : IDisposable
{
    private const string EmbeddingsPath = "/v1/embeddings";
    private const string IngestPath = "/v1/ingest";
    private const string RetrievePath = "/v1/retrieve";
    private const string DeletePath = "/v1/delete";
    private const string HealthPath = "/health";

    private readonly HttpTransport _transport;
    private bool _disposed;

    /// <summary>
    /// Construct a client. The base URL is normalised to strip any
    /// trailing slash.
    /// </summary>
    /// <param name="baseUrl">
    /// Root URL of the DASH service, e.g. <c>"http://localhost:8080"</c>.
    /// </param>
    /// <param name="apiKey">
    /// Optional bearer token. When set, sent as
    /// <c>Authorization: Bearer &lt;api_key&gt;</c>. When <c>null</c>,
    /// no auth header is added.
    /// </param>
    /// <param name="options">
    /// Optional client configuration. When <c>null</c>, the SDK
    /// defaults are used (30s timeout, 3 retries, etc.).
    /// </param>
    public DashClient(string baseUrl, string? apiKey = null, DashClientOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(baseUrl))
        {
            throw new ArgumentException("baseUrl is required", nameof(baseUrl));
        }

        BaseUrl = baseUrl.TrimEnd('/');
        ApiKey = apiKey;
        Options = options ?? new DashClientOptions();

        _transport = new HttpTransport(BaseUrl, apiKey, Options);
    }

    /// <summary>
    /// Root URL of the DASH service, with any trailing slash
    /// stripped.
    /// </summary>
    public string BaseUrl { get; }

    /// <summary>
    /// Bearer token configured on this client, or <c>null</c> when
    /// no auth header is sent.
    /// </summary>
    public string? ApiKey { get; }

    /// <summary>
    /// Effective options for this client.
    /// </summary>
    public DashClientOptions Options { get; }

    // -----------------------------------------------------------------
    // Embeddings
    // -----------------------------------------------------------------

    /// <summary>
    /// Call <c>POST /v1/embeddings</c> and return a typed response.
    /// The endpoint is byte-for-byte compatible with OpenAI's
    /// <c>/v1/embeddings</c>.
    /// </summary>
    public Task<EmbeddingResponse> EmbedAsync(EmbeddingRequest req, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(req);
        return _transport.SendAsyncNonNull<EmbeddingResponse>(
            HttpMethod.Post, EmbeddingsPath, req, ct);
    }

    /// <summary>Synchronous variant of <see cref="EmbedAsync"/>.</summary>
    public EmbeddingResponse Embed(EmbeddingRequest req, CancellationToken ct = default)
    {
        return EmbedAsync(req, ct).GetAwaiter().GetResult();
    }

    // -----------------------------------------------------------------
    // Ingest
    // -----------------------------------------------------------------

    /// <summary>
    /// Call <c>POST /v1/ingest</c> and return the ingest summary.
    /// </summary>
    public Task<IngestResponse> IngestAsync(IngestRequest req, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(req);
        return _transport.SendAsyncNonNull<IngestResponse>(
            HttpMethod.Post, IngestPath, req, ct);
    }

    /// <summary>Synchronous variant of <see cref="IngestAsync"/>.</summary>
    public IngestResponse Ingest(IngestRequest req, CancellationToken ct = default)
    {
        return IngestAsync(req, ct).GetAwaiter().GetResult();
    }

    // -----------------------------------------------------------------
    // Retrieve
    // -----------------------------------------------------------------

    /// <summary>
    /// Call <c>POST /v1/retrieve</c> and return the structured
    /// Claim + Evidence + Contradiction response.
    /// </summary>
    public Task<RetrievalResponse> RetrieveAsync(RetrievalRequest req, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(req);
        return _transport.SendAsyncNonNull<RetrievalResponse>(
            HttpMethod.Post, RetrievePath, req, ct);
    }

    /// <summary>Synchronous variant of <see cref="RetrieveAsync"/>.</summary>
    public RetrievalResponse Retrieve(RetrievalRequest req, CancellationToken ct = default)
    {
        return RetrieveAsync(req, ct).GetAwaiter().GetResult();
    }

    // -----------------------------------------------------------------
    // Delete
    // -----------------------------------------------------------------

    /// <summary>
    /// Call <c>POST /v1/delete</c> to remove claims and evidence
    /// from a tenant.
    /// </summary>
    public Task<DeleteResponse> DeleteAsync(DeleteRequest req, CancellationToken ct = default)
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(req);
        return _transport.SendAsyncNonNull<DeleteResponse>(
            HttpMethod.Post, DeletePath, req, ct);
    }

    /// <summary>Synchronous variant of <see cref="DeleteAsync"/>.</summary>
    public DeleteResponse Delete(DeleteRequest req, CancellationToken ct = default)
    {
        return DeleteAsync(req, ct).GetAwaiter().GetResult();
    }

    // -----------------------------------------------------------------
    // Health
    // -----------------------------------------------------------------

    /// <summary>
    /// Call <c>GET /health</c> and return the typed response.
    /// </summary>
    public Task<HealthResponse> HealthAsync(CancellationToken ct = default)
    {
        ThrowIfDisposed();
        return _transport.SendAsyncNonNull<HealthResponse>(
            HttpMethod.Get, HealthPath, body: null, ct);
    }

    /// <summary>Synchronous variant of <see cref="HealthAsync"/>.</summary>
    public HealthResponse Health(CancellationToken ct = default)
    {
        return HealthAsync(ct).GetAwaiter().GetResult();
    }

    // -----------------------------------------------------------------
    // IDisposable
    // -----------------------------------------------------------------

    /// <summary>
    /// Release the underlying <see cref="HttpClient"/>. No-op when
    /// the client was constructed with a caller-supplied
    /// <see cref="HttpClient"/> (we never dispose something we
    /// don't own). Safe to call multiple times.
    /// </summary>
    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }
        _disposed = true;
        _transport.Dispose();
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(DashClient));
        }
    }
}
