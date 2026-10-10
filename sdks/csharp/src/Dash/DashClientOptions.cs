using System;

namespace Dash;

/// <summary>
/// Configuration for <see cref="DashClient"/>.
/// </summary>
public class DashClientOptions
{
    /// <summary>
    /// Per-request timeout. Defaults to 30 seconds, matching the
    /// Python and TypeScript SDKs.
    /// </summary>
    public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Root URL of the ingestion service (<c>POST /v1/ingest</c> and the
    /// <c>DELETE /v1/claims</c>, <c>/v1/evidence</c>, <c>/v1/tenants</c>
    /// routes), e.g.
    /// <c>"http://localhost:8081"</c>. Ingestion runs as a separate service
    /// from retrieval. When <c>null</c>, it is derived only for the
    /// conventional local layout (retrieval base URL on port 8080 maps to
    /// the same host on port 8081); otherwise ingest and delete calls throw
    /// <see cref="System.InvalidOperationException"/>. Must be an absolute
    /// http(s) URL.
    /// </summary>
    public string? IngestionBaseUrl { get; set; }

    /// <summary>
    /// Maximum number of retry attempts for transient failures
    /// (network errors, HTTP 5xx, HTTP 429) of idempotent requests. Capped
    /// at 10. Non-idempotent requests (ingest) are never retried unless the
    /// caller passes <see cref="RequestOptions"/>. Defaults to 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// User-Agent header sent on every request. Defaults to
    /// <c>"dash-csharp/0.2.0"</c>.
    /// </summary>
    public string UserAgent { get; set; } = "dash-csharp/0.2.0";

    /// <summary>
    /// Base delay used by the exponential backoff between retries.
    /// The Nth retry waits a random (jittered) delay up to
    /// <c>RetryBaseDelay * 2^(N-1)</c>, capped at 5 s; a server-sent
    /// <c>Retry-After</c> is honoured (up to 30 s). Defaults to 100 ms.
    /// </summary>
    public TimeSpan RetryBaseDelay { get; set; } = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// When set, all requests are routed through this client. Used
    /// by the test suite to inject a mock <see cref="System.Net.Http.HttpMessageHandler"/>
    /// at the <see cref="System.Net.Http.HttpClient"/> boundary. When
    /// <c>null</c> (the default), the client creates and disposes
    /// its own <see cref="System.Net.Http.HttpClient"/>.
    /// </summary>
    public System.Net.Http.HttpClient? HttpClient { get; set; }
}
