package dev.dash;

/**
 * Raised when the client cannot reach DASH at all: DNS failure,
 * connection refused, read timeout, TLS error.
 *
 * <p>Mirrors the Python {@code DashConnectionError} and Go
 * {@code DashConnectionError}. The underlying network exception is
 * exposed via {@link #getCause()} so it remains inspectable.</p>
 */
public class DashConnectionException extends DashException {

    public DashConnectionException(String message) {
        super(message, 0, "connection_error", null, null);
    }

    public DashConnectionException(String message, Throwable cause) {
        super(message, 0, "connection_error", null, cause);
    }
}
