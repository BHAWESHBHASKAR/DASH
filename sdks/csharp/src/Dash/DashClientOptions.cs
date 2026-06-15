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
    /// Maximum number of retry attempts for transient failures
    /// (network errors, HTTP 5xx, HTTP 429). Defaults to 3.
    /// </summary>
    public int MaxRetries { get; set; } = 3;

    /// <summary>
    /// User-Agent header sent on every request. Defaults to
    /// <c>"dash-csharp/0.2.0"</c>.
    /// </summary>
    public string UserAgent { get; set; } = "dash-csharp/0.2.0";

    /// <summary>
    /// Base delay used by the exponential backoff between retries.
    /// The Nth retry waits <c>RetryBaseDelay * 2^(N-1)</c>. Defaults
    /// to 100 ms.
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
