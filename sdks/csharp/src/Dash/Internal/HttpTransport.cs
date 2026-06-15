using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;

namespace Dash.Internal;

/// <summary>
/// Low-level HTTP transport used by <see cref="DashClient"/>.
/// Responsibilities:
/// <list type="bullet">
///   <item>Set the <c>Authorization: Bearer &lt;key&gt;</c> header.</item>
///   <item>Set the configured <c>User-Agent</c> on every request.</item>
///   <item>Serialize JSON bodies with snake_case naming.</item>
///   <item>Retry on transient failures (network, 5xx, 429) with
///         exponential backoff.</item>
///   <item>Translate non-2xx responses into <see cref="DashException"/>
///         subclasses.</item>
/// </list>
/// </summary>
internal sealed class HttpTransport
{
    private static readonly JsonSerializerOptions JsonOptions = CreateJsonOptions();

    private readonly HttpClient _httpClient;
    private readonly bool _ownsHttpClient;
    private readonly DashClientOptions _options;
    private readonly string _baseUrl;
    private readonly string? _apiKey;

    public HttpTransport(string baseUrl, string? apiKey, DashClientOptions options)
    {
        _baseUrl = baseUrl.TrimEnd('/');
        _apiKey = apiKey;
        _options = options;

        if (options.HttpClient is not null)
        {
            _httpClient = options.HttpClient;
            _ownsHttpClient = false;
        }
        else
        {
            _httpClient = new HttpClient
            {
                Timeout = options.Timeout,
            };
            _ownsHttpClient = true;
        }
    }

    public TimeSpan Timeout => _options.Timeout;

    public int MaxRetries => Math.Max(0, _options.MaxRetries);

    public TimeSpan RetryBaseDelay => _options.RetryBaseDelay;

    public string UserAgent => _options.UserAgent;

    /// <summary>
    /// Send a request that returns a JSON body of type
    /// <typeparamref name="TResponse"/>. Returns the deserialised
    /// value, or <c>default</c> when the server returned no body.
    /// </summary>
    public async Task<TResponse?> SendAsync<TResponse>(
        HttpMethod method,
        string path,
        object? body,
        CancellationToken cancellationToken)
    {
        var (status, raw, requestId) = await SendRawAsync(method, path, body, cancellationToken)
            .ConfigureAwait(false);
        EnsureSuccess(status, raw, requestId, path);

        if (string.IsNullOrEmpty(raw) || raw == "null")
        {
            return default;
        }

        return JsonSerializer.Deserialize<TResponse>(raw, JsonOptions);
    }

    /// <summary>
    /// Send a request and return the deserialised JSON body as an
    /// <see cref="object"/> (used by methods that want a single
    /// code path for both typed and untyped parsing).
    /// </summary>
    public async Task<TResponse> SendAsyncNonNull<TResponse>(
        HttpMethod method,
        string path,
        object? body,
        CancellationToken cancellationToken)
        where TResponse : class
    {
        var result = await SendAsync<TResponse>(method, path, body, cancellationToken)
            .ConfigureAwait(false);
        if (result is null)
        {
            throw new DashException(
                $"DASH returned an empty body for {method} {path}",
                statusCode: null,
                errorCode: null,
                requestId: null);
        }
        return result;
    }

    private async Task<(int StatusCode, string Raw, string? RequestId)> SendRawAsync(
        HttpMethod method,
        string path,
        object? body,
        CancellationToken cancellationToken)
    {
        var url = _baseUrl + path;
        var maxAttempts = MaxRetries + 1;
        Exception? lastTransient = null;
        int? lastStatus = null;
        string? lastRaw = null;
        string? lastRequestId = null;

        for (var attempt = 1; attempt <= maxAttempts; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            using var request = BuildRequest(method, url, body);
            HttpResponseMessage? response = null;
            string? raw = null;
            string? requestId = null;
            try
            {
                response = await _httpClient
                    .SendAsync(request, HttpCompletionOption.ResponseContentRead, cancellationToken)
                    .ConfigureAwait(false);
                raw = response.Content is null
                    ? string.Empty
                    : await response.Content.ReadAsStringAsync().ConfigureAwait(false);
                requestId = TryGetRequestId(response);

                var status = (int)response.StatusCode;
                if (IsRetryableStatus(status) && attempt < maxAttempts)
                {
                    lastStatus = status;
                    lastRaw = raw;
                    lastRequestId = requestId;
                    response.Dispose();
                    await BackoffAsync(attempt, response, cancellationToken).ConfigureAwait(false);
                    continue;
                }

                return (status, raw ?? string.Empty, requestId);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (HttpRequestException ex) when (attempt < maxAttempts)
            {
                lastTransient = ex;
                response?.Dispose();
                await BackoffAsync(attempt, response: null, cancellationToken).ConfigureAwait(false);
                continue;
            }
            catch (TaskCanceledException ex) when (ex.InnerException is TimeoutException && attempt < maxAttempts)
            {
                lastTransient = ex;
                response?.Dispose();
                await BackoffAsync(attempt, response: null, cancellationToken).ConfigureAwait(false);
                continue;
            }
            catch (TaskCanceledException ex)
            {
                response?.Dispose();
                throw new DashConnectionException(
                    $"request to DASH timed out after {_options.Timeout.TotalSeconds:0.#}s: {ex.Message}",
                    ex);
            }
            catch (HttpRequestException ex)
            {
                response?.Dispose();
                throw new DashConnectionException(
                    $"failed to connect to DASH at {_baseUrl}: {ex.Message}",
                    ex);
            }
        }

        // All attempts exhausted on a transient error.
        if (lastStatus is not null)
        {
            throw DashException.FromHttp(
                lastStatus.Value,
                errorCode: null,
                requestId: lastRequestId,
                body: lastRaw);
        }
        throw new DashConnectionException(
            $"request to DASH failed after {maxAttempts} attempts: {lastTransient?.Message}",
            lastTransient ?? new InvalidOperationException("unknown transport failure"));
    }

    private HttpRequestMessage BuildRequest(HttpMethod method, string url, object? body)
    {
        var request = new HttpRequestMessage(method, url);
        request.Headers.UserAgent.ParseAdd(_options.UserAgent);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));

        if (!string.IsNullOrEmpty(_apiKey))
        {
            request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", _apiKey);
        }

        if (body is not null)
        {
            var json = JsonSerializer.Serialize(body, body.GetType(), JsonOptions);
            request.Content = new StringContent(json, Encoding.UTF8, "application/json");
        }
        else
        {
            request.Content = null;
        }

        return request;
    }

    private static void EnsureSuccess(int statusCode, string raw, string? requestId, string path)
    {
        if (statusCode >= 200 && statusCode < 300)
        {
            return;
        }

        var (errorCode, message) = ParseErrorBody(raw);
        var type = string.IsNullOrEmpty(errorCode) ? "api_error" : errorCode;
        var fullMessage = string.IsNullOrEmpty(message)
            ? $"DASH API error ({statusCode} {type}) on {path}"
            : $"DASH API error ({statusCode} {type}): {message}";

        throw statusCode switch
        {
            (int)HttpStatusCode.Unauthorized or (int)HttpStatusCode.Forbidden
                => new DashAuthException(fullMessage, statusCode, errorCode, requestId, raw),
            (int)HttpStatusCode.NotFound
                => new DashNotFoundException(fullMessage, statusCode, errorCode, requestId, raw),
            (int)HttpStatusCode.TooManyRequests
                => new DashRateLimitException(fullMessage, statusCode, errorCode, requestId, raw),
            _ => new DashException(fullMessage, statusCode, errorCode, requestId, raw),
        };
    }

    private static (string? ErrorCode, string? Message) ParseErrorBody(string? raw)
    {
        if (string.IsNullOrEmpty(raw))
        {
            return (null, null);
        }

        try
        {
            using var doc = JsonDocument.Parse(raw);
            if (doc.RootElement.ValueKind != JsonValueKind.Object)
            {
                return (null, raw);
            }

            if (doc.RootElement.TryGetProperty("error", out var err))
            {
                if (err.ValueKind == JsonValueKind.Object)
                {
                    string? code = null;
                    string? message = null;
                    if (err.TryGetProperty("type", out var t) && t.ValueKind == JsonValueKind.String)
                    {
                        code = t.GetString();
                    }
                    else if (err.TryGetProperty("code", out var c) && c.ValueKind == JsonValueKind.String)
                    {
                        code = c.GetString();
                    }
                    if (err.TryGetProperty("message", out var m) && m.ValueKind == JsonValueKind.String)
                    {
                        message = m.GetString();
                    }
                    return (code, message);
                }
                if (err.ValueKind == JsonValueKind.String)
                {
                    return ("api_error", err.GetString());
                }
            }
            if (doc.RootElement.TryGetProperty("message", out var top) && top.ValueKind == JsonValueKind.String)
            {
                return ("api_error", top.GetString());
            }
        }
        catch (JsonException)
        {
            // Non-JSON body, fall through.
        }
        return (null, raw);
    }

    private static bool IsRetryableStatus(int statusCode)
    {
        if (statusCode == (int)HttpStatusCode.TooManyRequests)
        {
            return true;
        }
        if (statusCode >= 500 && statusCode < 600)
        {
            return true;
        }
        return false;
    }

    private async Task BackoffAsync(int attempt, HttpResponseMessage? response, CancellationToken cancellationToken)
    {
        // Respect a server-supplied Retry-After header for 429.
        TimeSpan? retryAfter = null;
        if (response is not null && response.Headers.RetryAfter is { } ra)
        {
            if (ra.Delta is { } delta)
            {
                retryAfter = delta;
            }
            else if (ra.Date is { } date)
            {
                var diff = date - DateTimeOffset.UtcNow;
                retryAfter = diff > TimeSpan.Zero ? diff : TimeSpan.Zero;
            }
        }

        var delay = retryAfter ?? TimeSpan.FromMilliseconds(
            _options.RetryBaseDelay.TotalMilliseconds * Math.Pow(2, attempt - 1));

        try
        {
            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            throw;
        }
    }

    private static string? TryGetRequestId(HttpResponseMessage response)
    {
        if (response.Headers.TryGetValues("X-Request-Id", out var values))
        {
            return values.FirstOrDefault();
        }
        if (response.Headers.TryGetValues("X-Request-ID", out values))
        {
            return values.FirstOrDefault();
        }
        return null;
    }

    private static JsonSerializerOptions CreateJsonOptions()
    {
        var options = new JsonSerializerOptions
        {
            PropertyNamingPolicy = SnakeCaseLowerNamingPolicy.Instance,
            DictionaryKeyPolicy = SnakeCaseLowerNamingPolicy.Instance,
            PropertyNameCaseInsensitive = true,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        };
        return options;
    }

    public void Dispose()
    {
        if (_ownsHttpClient)
        {
            _httpClient.Dispose();
        }
    }
}
