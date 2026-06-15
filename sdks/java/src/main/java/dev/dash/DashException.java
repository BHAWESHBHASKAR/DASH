package dev.dash;

import java.util.Objects;

/**
 * Exception thrown by the DASH client when something goes wrong
 * talking to the server.
 *
 * <p>Subclasses cover the two main failure modes:</p>
 * <ul>
 *   <li>{@link DashException} for non-2xx HTTP responses (mirrors the
 *       Python {@code DashAPIError} and Go {@code DashAPIError}). The
 *       HTTP status, server-supplied error code, and request id (when
 *       returned) are exposed via {@link #getStatusCode()},
 *       {@link #getErrorCode()}, and {@link #getRequestId()}.</li>
 *   <li>{@link DashConnectionException} (a sibling of this class) for
 *       network-level failures: DNS, refused, read timeout, TLS.</li>
 * </ul>
 *
 * <p>Callers can catch {@code DashException} (or its base
 * {@link RuntimeException}) to handle "anything went wrong talking to
 * DASH" without caring about the specific failure mode.</p>
 */
public class DashException extends RuntimeException {

    private final int statusCode;
    private final String errorCode;
    private final String requestId;

    public DashException(String message) {
        this(message, 0, null, null, null);
    }

    public DashException(String message, Throwable cause) {
        this(message, 0, null, null, cause);
    }

    public DashException(
            String message,
            int statusCode,
            String errorCode,
            String requestId,
            Throwable cause) {
        super(message, cause);
        this.statusCode = statusCode;
        this.errorCode = errorCode;
        this.requestId = requestId;
    }

    /**
     * The HTTP status code returned by the server. Returns {@code 0}
     * when the failure was not an HTTP-level response (network error).
     */
    public int getStatusCode() {
        return statusCode;
    }

    /**
     * The server-supplied error code (e.g. {@code "invalid_request_error"}
     * for OpenAI-style errors, or {@code "api_error"} as a fallback).
     * Returns {@code null} for network failures.
     */
    public String getErrorCode() {
        return errorCode;
    }

    /**
     * The {@code X-Request-Id} header value returned by the server, or
     * {@code null} when the server did not set one or the request never
     * reached it.
     */
    public String getRequestId() {
        return requestId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DashException that)) return false;
        return statusCode == that.statusCode
                && Objects.equals(getMessage(), that.getMessage())
                && Objects.equals(errorCode, that.errorCode)
                && Objects.equals(requestId, that.requestId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getMessage(), statusCode, errorCode, requestId);
    }
}
