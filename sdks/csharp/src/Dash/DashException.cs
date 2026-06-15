using System;
using System.Net;

namespace Dash;

/// <summary>
/// Base class for every error raised by the DASH client when the
/// server returns a non-2xx HTTP response.
///
/// Catch this if you want to handle "anything went wrong talking to
/// DASH at the HTTP layer" without caring about the specific failure
/// mode. Network-level failures (DNS, refused, timeout) raise
/// <see cref="DashConnectionException"/> instead.
/// </summary>
public class DashException : Exception
{
    /// <summary>
    /// HTTP status code returned by the server. <c>null</c> when no
    /// HTTP response was received (used by subclasses that synthesise
    /// the error from a transport failure).
    /// </summary>
    public int? StatusCode { get; }

    /// <summary>
    /// OpenAI-style error type string, e.g.
    /// <c>"invalid_request_error"</c>, <c>"server_error"</c>, or
    /// <c>"api_error"</c> as a fallback.
    /// </summary>
    public string? ErrorCode { get; }

    /// <summary>
    /// Value of the <c>X-Request-Id</c> response header when the
    /// server supplied one. Useful for correlating with server logs.
    /// </summary>
    public string? RequestId { get; }

    /// <summary>Raw response body (string), or <c>null</c>.</summary>
    public string? Body { get; }

    public DashException(string message)
        : base(message)
    {
    }

    public DashException(string message, Exception? innerException)
        : base(message, innerException)
    {
    }

    public DashException(
        string message,
        int? statusCode,
        string? errorCode,
        string? requestId,
        string? body = null,
        Exception? innerException = null)
        : base(message, innerException)
    {
        StatusCode = statusCode;
        ErrorCode = errorCode;
        RequestId = requestId;
        Body = body;
    }

    /// <summary>
    /// Convenience factory that maps an HTTP status code to the
    /// appropriate <see cref="DashException"/> subclass. Unknown
    /// status codes fall back to the base <see cref="DashException"/>.
    /// </summary>
    public static DashException FromHttp(
        int statusCode,
        string? errorCode,
        string? requestId,
        string? body,
        Exception? innerException = null)
    {
        var message = BuildMessage(statusCode, errorCode, body);
        return statusCode switch
        {
            (int)HttpStatusCode.Unauthorized or (int)HttpStatusCode.Forbidden
                => new DashAuthException(message, statusCode, errorCode, requestId, body, innerException),
            (int)HttpStatusCode.TooManyRequests
                => new DashRateLimitException(message, statusCode, errorCode, requestId, body, innerException),
            (int)HttpStatusCode.NotFound
                => new DashNotFoundException(message, statusCode, errorCode, requestId, body, innerException),
            _ => new DashException(message, statusCode, errorCode, requestId, body, innerException),
        };
    }

    private static string BuildMessage(int statusCode, string? errorCode, string? body)
    {
        var type = string.IsNullOrEmpty(errorCode) ? "api_error" : errorCode;
        if (!string.IsNullOrEmpty(body))
        {
            return $"DASH API error ({statusCode} {type}): {body}";
        }
        return $"DASH API error ({statusCode} {type})";
    }
}

/// <summary>
/// Raised when the server rejects the request because of an
/// authentication or authorisation problem. Typically HTTP 401 or
/// 403.
/// </summary>
public class DashAuthException : DashException
{
    public DashAuthException(
        string message,
        int? statusCode,
        string? errorCode,
        string? requestId,
        string? body = null,
        Exception? innerException = null)
        : base(message, statusCode, errorCode, requestId, body, innerException)
    {
    }
}

/// <summary>
/// Raised when the server returns HTTP 429. The caller is expected
/// to back off and retry; the response's <c>Retry-After</c> header
/// is preserved on <see cref="DashException.RequestId"/> when the
/// server emits one (use the raw <see cref="HttpResponseMessage"/>
/// for the canonical header value if you need the exact delay).
/// </summary>
public class DashRateLimitException : DashException
{
    public DashRateLimitException(
        string message,
        int? statusCode,
        string? errorCode,
        string? requestId,
        string? body = null,
        Exception? innerException = null)
        : base(message, statusCode, errorCode, requestId, body, innerException)
    {
    }
}

/// <summary>
/// Raised when the server returns HTTP 404 — typically because a
/// tenant or claim id does not exist.
/// </summary>
public class DashNotFoundException : DashException
{
    public DashNotFoundException(
        string message,
        int? statusCode,
        string? errorCode,
        string? requestId,
        string? body = null,
        Exception? innerException = null)
        : base(message, statusCode, errorCode, requestId, body, innerException)
    {
    }
}

/// <summary>
/// Raised when the client cannot reach DASH at the transport layer:
/// DNS failure, connection refused, read timeout, TLS error. The
/// underlying <see cref="Exception"/> is chained via
/// <see cref="Exception.InnerException"/>.
/// </summary>
public class DashConnectionException : Exception
{
    public DashConnectionException(string message)
        : base(message)
    {
    }

    public DashConnectionException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
