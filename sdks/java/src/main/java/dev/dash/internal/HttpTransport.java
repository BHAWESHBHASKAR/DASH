package dev.dash.internal;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import okhttp3.Interceptor;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import dev.dash.DashConnectionException;
import dev.dash.DashException;
import dev.dash.RequestOptions;

/**
 * Low-level HTTP transport used by the public {@code DashClient}.
 *
 * <p>Wraps an {@link OkHttpClient} with a bearer-token auth interceptor,
 * exponential-backoff retry (3 attempts, 100ms base, jittered and capped; only for
 * idempotent requests; honours Retry-After), and
 * configurable timeouts. The transport is safe for concurrent use; the
 * underlying {@code OkHttpClient} is shared across calls.</p>
 *
 * <p>All exceptions are normalised into {@link DashException} /
 * {@link DashConnectionException} so service code never has to deal
 * with raw {@code IOException}s.</p>
 */
public class HttpTransport {

    /** Default connect timeout. */
    public static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ofSeconds(10);
    /** Default read timeout. */
    public static final Duration DEFAULT_READ_TIMEOUT = Duration.ofSeconds(30);
    /** Default write timeout. */
    public static final Duration DEFAULT_WRITE_TIMEOUT = Duration.ofSeconds(30);

    /** Default number of attempts (1 initial + 2 retries). */
    public static final int DEFAULT_MAX_ATTEMPTS = 3;
    /** Base backoff between retries, in milliseconds. */
    public static final long DEFAULT_BACKOFF_MS = 100L;
    /** Upper bound for the jittered exponential backoff, in milliseconds. */
    public static final long MAX_BACKOFF_MS = 5_000L;
    /** Upper bound honoured for a server-sent Retry-After, in milliseconds. */
    public static final long MAX_RETRY_AFTER_MS = 30_000L;
    /** Hard cap on attempts regardless of configuration. */
    public static final int MAX_ATTEMPTS_LIMIT = 10;

    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");
    private static final String USER_AGENT = "dash-java/0.2.0";

    private final String baseUrl;
    private final OkHttpClient client;
    private final ObjectMapper mapper;
    private final int maxAttempts;
    private final long backoffMs;

    public HttpTransport(String baseUrl, String apiKey) {
        this(baseUrl, apiKey, DEFAULT_CONNECT_TIMEOUT, DEFAULT_READ_TIMEOUT,
                DEFAULT_WRITE_TIMEOUT, DEFAULT_MAX_ATTEMPTS, DEFAULT_BACKOFF_MS);
    }

    public HttpTransport(
            String baseUrl,
            String apiKey,
            Duration connectTimeout,
            Duration readTimeout,
            Duration writeTimeout,
            int maxAttempts,
            long backoffMs) {
        this.baseUrl = Objects.requireNonNull(baseUrl, "baseUrl").replaceAll("/+$", "");
        this.maxAttempts = Math.min(MAX_ATTEMPTS_LIMIT, Math.max(1, maxAttempts));
        this.backoffMs = Math.max(0L, backoffMs);
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        this.client = new OkHttpClient.Builder()
                .connectTimeout(connectTimeout)
                .readTimeout(readTimeout)
                .writeTimeout(writeTimeout)
                .addInterceptor(new AuthInterceptor(apiKey))
                .addInterceptor(new UserAgentInterceptor(USER_AGENT))
                .build();
    }

    /**
     * POST a JSON-serialisable body to {@code path} and return the
     * deserialised response of type {@code responseType}. The request is
     * sent once (no retries); use the overload taking {@link RequestOptions}
     * to allow retries.
     */
    public <T> T post(String path, Object body, Class<T> responseType) {
        return post(path, body, responseType, RequestOptions.NONE);
    }

    public <T> T post(String path, Object body, Class<T> responseType, RequestOptions options) {
        RawResponse raw = execute(path, serialise(body), options);
        return decode(raw, mapper -> mapper.readValue(raw.body, responseType));
    }

    /**
     * POST a JSON-serialisable body and return the response decoded
     * against a {@link TypeReference} (e.g. {@code List<Foo>}).
     */
    public <T> T post(String path, Object body, TypeReference<T> typeRef) {
        return post(path, body, typeRef, RequestOptions.NONE);
    }

    public <T> T post(String path, Object body, TypeReference<T> typeRef, RequestOptions options) {
        RawResponse raw = execute(path, serialise(body), options);
        return decode(raw, mapper -> mapper.readValue(raw.body, typeRef));
    }

    /**
     * GET {@code path} and return the deserialised response. GET is
     * idempotent so it is retried on 429/5xx and I/O errors.
     */
    public <T> T get(String path, Class<T> responseType) {
        RawResponse raw = execute(path, null, RequestOptions.IDEMPOTENT);
        return decode(raw, mapper -> mapper.readValue(raw.body, responseType));
    }

    /**
     * DELETE {@code path} (no body) and return the deserialised response.
     * The DASH delete routes are idempotent, so the request is retried on
     * 429/5xx and I/O errors like a GET.
     */
    public <T> T delete(String path, Class<T> responseType) {
        RawResponse raw = execute("DELETE", path, null, RequestOptions.IDEMPOTENT);
        return decode(raw, mapper -> mapper.readValue(raw.body, responseType));
    }

    public ObjectMapper mapper() {
        return mapper;
    }

    public String baseUrl() {
        return baseUrl;
    }

    // ------------------------------------------------------------------
    // Internals
    // ------------------------------------------------------------------

    /** Fully-read response: the body is consumed exactly once and closed. */
    private static final class RawResponse {
        final int status;
        final String body;
        final String requestId;
        final String retryAfter;

        RawResponse(int status, String body, String requestId, String retryAfter) {
            this.status = status;
            this.body = body;
            this.requestId = requestId;
            this.retryAfter = retryAfter;
        }
    }

    private String serialise(Object body) {
        try {
            return mapper.writeValueAsString(body);
        } catch (IOException e) {
            throw new DashException("failed to serialise request body: " + e.getMessage(), e);
        }
    }

    private RawResponse execute(String rawPath, String jsonBody, RequestOptions options) {
        return execute(jsonBody != null ? "POST" : "GET", rawPath, jsonBody, options);
    }

    private RawResponse execute(String method, String rawPath, String jsonBody,
                                RequestOptions options) {
        String path = "/" + rawPath.replaceFirst("^/+", "");
        String url = baseUrl + path;
        int attempts = options.canRetry() ? maxAttempts : 1;
        for (int attempt = 1; ; attempt++) {
            Request.Builder builder = new Request.Builder().url(url);
            if (jsonBody != null) {
                builder.post(RequestBody.create(jsonBody, JSON));
            } else if ("DELETE".equals(method)) {
                builder.delete();
            } else {
                builder.get();
            }
            if (options.idempotencyKey() != null && !options.idempotencyKey().isBlank()) {
                builder.header("Idempotency-Key", options.idempotencyKey());
            }
            RawResponse raw;
            try (Response response = client.newCall(builder.build()).execute()) {
                ResponseBody body = response.body();
                raw = new RawResponse(
                        response.code(),
                        body == null ? "" : body.string(),
                        response.header("X-Request-Id"),
                        response.header("Retry-After"));
            } catch (IOException e) {
                if (attempt >= attempts) {
                    throw new DashConnectionException(
                            "failed to reach DASH at " + baseUrl + ": " + e.getMessage(), e);
                }
                sleepBackoff(attempt, null);
                continue;
            }
            if (!shouldRetry(raw.status) || attempt >= attempts) {
                return raw;
            }
            sleepBackoff(attempt, raw.retryAfter);
        }
    }

    private static boolean shouldRetry(int code) {
        return code == 429 || (code >= 500 && code < 600);
    }

    private void sleepBackoff(int attempt, String retryAfter) {
        // Exponential backoff with full jitter, capped at MAX_BACKOFF_MS.
        long cap = Math.min(MAX_BACKOFF_MS, backoffMs * (1L << Math.min(attempt - 1, 20)));
        long delay = cap <= 0 ? 0 : ThreadLocalRandom.current().nextLong(0, cap + 1);
        long serverDelay = parseRetryAfterMs(retryAfter);
        if (serverDelay >= 0) {
            delay = Math.max(delay, Math.min(serverDelay, MAX_RETRY_AFTER_MS));
        }
        if (delay <= 0) {
            return;
        }
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DashConnectionException("retry interrupted", e);
        }
    }

    /** Parse a Retry-After header (delta-seconds or HTTP-date); -1 if absent or invalid. */
    public static long parseRetryAfterMs(String value) {
        if (value == null || value.isBlank()) {
            return -1;
        }
        String v = value.trim();
        try {
            long seconds = Long.parseLong(v);
            return seconds < 0 ? -1 : seconds * 1000L;
        } catch (NumberFormatException ignore) {
            // fall through to HTTP-date
        }
        try {
            var when = java.time.ZonedDateTime.parse(v, java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME);
            long ms = java.time.Duration.between(java.time.ZonedDateTime.now(), when).toMillis();
            return Math.max(0L, ms);
        } catch (java.time.format.DateTimeParseException e) {
            return -1;
        }
    }

    private <T> T decode(RawResponse raw, Decoder<T> decoder) {
        try {
            if (raw.status >= 200 && raw.status < 300) {
                if (raw.body == null || raw.body.isEmpty()) {
                    return null;
                }
                return decoder.apply(mapper);
            }
            throw buildError(raw.status, raw.body, raw.requestId);
        } catch (IOException e) {
            throw new DashException(
                    "failed to decode DASH response: " + e.getMessage(), e);
        }
    }

    @FunctionalInterface
    private interface Decoder<T> {
        T apply(ObjectMapper mapper) throws IOException;
    }

    private DashException buildError(int status, String rawBody, String requestId) {
        String errorCode = "api_error";
        String errorMessage = rawBody;
        if (!rawBody.isEmpty()) {
            try {
                var node = mapper.readTree(rawBody);
                if (node.isObject()) {
                    var err = node.get("error");
                    if (err != null && err.isObject()) {
                        var t = err.get("type");
                        if (t == null) {
                            t = err.get("code");
                        }
                        if (t != null && t.isTextual() && !t.asText().isEmpty()) {
                            errorCode = t.asText();
                        }
                        var m = err.get("message");
                        if (m != null && m.isTextual() && !m.asText().isEmpty()) {
                            errorMessage = m.asText();
                        }
                    } else if (err != null && err.isTextual() && !err.asText().isEmpty()) {
                        errorMessage = err.asText();
                        errorCode = "api_error";
                    } else {
                        var m = node.get("message");
                        if (m != null && m.isTextual() && !m.asText().isEmpty()) {
                            errorMessage = m.asText();
                            errorCode = "api_error";
                        }
                    }
                }
            } catch (IOException ignore) {
                // Non-JSON body; keep the raw text as the message.
            }
        }
        if (errorMessage == null || errorMessage.isEmpty()) {
            errorMessage = "HTTP " + status;
        }
        return new DashException(
                "DASH API error (" + status + " " + errorCode + "): " + errorMessage,
                status, errorCode, requestId, null);
    }

    // ------------------------------------------------------------------
    // Interceptors
    // ------------------------------------------------------------------

    private static final class AuthInterceptor implements Interceptor {
        private final String apiKey;

        AuthInterceptor(String apiKey) {
            this.apiKey = apiKey;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            if (apiKey == null || apiKey.isEmpty()) {
                return chain.proceed(request);
            }
            Request authed = request.newBuilder()
                    .header("Authorization", "Bearer " + apiKey)
                    .build();
            return chain.proceed(authed);
        }
    }

    private static final class UserAgentInterceptor implements Interceptor {
        private final String userAgent;

        UserAgentInterceptor(String userAgent) {
            this.userAgent = userAgent;
        }

        @Override
        public Response intercept(Chain chain) throws IOException {
            Request request = chain.request();
            if (request.header("User-Agent") != null) {
                return chain.proceed(request);
            }
            Request ua = request.newBuilder()
                    .header("User-Agent", userAgent)
                    .header("Accept", "application/json")
                    .build();
            return chain.proceed(ua);
        }
    }
}
