package dev.dash;

/**
 * Per-request options that control retry behaviour.
 *
 * <p>The transport only retries a failed request (HTTP 429/5xx or an I/O
 * error) when it is safe to do so: either the operation is naturally
 * idempotent ({@link #IDEMPOTENT}, used for embeddings, retrieve and health)
 * or the caller supplied an {@code Idempotency-Key}
 * ({@link #withIdempotencyKey(String)}). Everything else, notably
 * {@code POST /v1/ingest}, is sent exactly once.</p>
 *
 * @param idempotent      the operation can be repeated without side effects
 * @param idempotencyKey  value for the {@code Idempotency-Key} header, or null
 */
public record RequestOptions(boolean idempotent, String idempotencyKey) {

    /** Single attempt; no retries. This is the default for writes. */
    public static final RequestOptions NONE = new RequestOptions(false, null);

    /** Naturally idempotent request; retries are allowed. */
    public static final RequestOptions IDEMPOTENT = new RequestOptions(true, null);

    /** Send an {@code Idempotency-Key} header and allow retries. */
    public static RequestOptions withIdempotencyKey(String key) {
        if (key == null || key.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        return new RequestOptions(false, key);
    }

    /** Explicit caller opt-in to retries for a non-idempotent write. */
    public static RequestOptions retryable() {
        return new RequestOptions(true, null);
    }

    /** True when the transport may re-send the request after a failure. */
    public boolean canRetry() {
        return idempotent || (idempotencyKey != null && !idempotencyKey.isBlank());
    }
}
