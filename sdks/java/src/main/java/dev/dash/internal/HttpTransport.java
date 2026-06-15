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

/**
 * Low-level HTTP transport used by the public {@code DashClient}.
 *
 * <p>Wraps an {@link OkHttpClient} with a bearer-token auth interceptor,
 * exponential-backoff retry (3 attempts, 100ms base, jittered), and
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
        this.maxAttempts = Math.max(1, maxAttempts);
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
     * deserialised response of type {@code responseType}.
     */
    public <T> T post(String path, Object body, Class<T> responseType) {
        String json;
        try {
            json = mapper.writeValueAsString(body);
        } catch (IOException e) {
            throw new DashException("failed to serialise request body: " + e.getMessage(), e);
        }
        Response response = executeWithRetry("/" + path.replaceFirst("^/+", ""), json);
        return decode(response, responseType);
    }

    /**
     * POST a JSON-serialisable body and return the response decoded
     * against a {@link TypeReference} (e.g. {@code List<Foo>}).
     */
    public <T> T post(String path, Object body, TypeReference<T> typeRef) {
        String json;
        try {
            json = mapper.writeValueAsString(body);
        } catch (IOException e) {
            throw new DashException("failed to serialise request body: " + e.getMessage(), e);
        }
        Response response = executeWithRetry("/" + path.replaceFirst("^/+", ""), json);
        return decode(response, typeRef);
    }

    /**
     * GET {@code path} and return the deserialised response.
     */
    public <T> T get(String path, Class<T> responseType) {
        Response response = executeWithRetry("/" + path.replaceFirst("^/+", ""), null);
        return decode(response, responseType);
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

    private Response executeWithRetry(String path, String jsonBody) {
        String url = baseUrl + path;
        IOException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            Request.Builder builder = new Request.Builder().url(url);
            if (jsonBody != null) {
                builder.post(RequestBody.create(jsonBody, JSON));
            } else {
                builder.get();
            }
            Request request = builder.build();
            try {
                Response response = client.newCall(request).execute();
                if (!shouldRetry(response.code()) || attempt == maxAttempts) {
                    return response;
                }
                response.close();
                sleepBackoff(attempt);
            } catch (IOException e) {
                last = e;
                if (attempt == maxAttempts) {
                    throw new DashConnectionException(
                            "failed to reach DASH at " + baseUrl + ": " + e.getMessage(), e);
                }
                sleepBackoff(attempt);
            }
        }
        // Unreachable, but keep the compiler happy.
        throw new DashConnectionException(
                "failed to reach DASH at " + baseUrl
                        + (last != null ? ": " + last.getMessage() : ""));
    }

    private static boolean shouldRetry(int code) {
        return code == 429 || (code >= 500 && code < 600);
    }

    private void sleepBackoff(int attempt) {
        if (backoffMs <= 0) {
            return;
        }
        // Exponential backoff with full jitter: 0..(base * 2^(attempt-1))
        long cap = backoffMs * (1L << (attempt - 1));
        long delay = ThreadLocalRandom.current().nextLong(0, cap + 1);
        try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DashConnectionException("retry interrupted", e);
        }
    }

    private <T> T decode(Response response, Class<T> type) {
        return decodeInternal(response, mapper -> mapper.readValue(mapper.createParser(responseBody(response)), type));
    }

    private <T> T decode(Response response, TypeReference<T> typeRef) {
        return decodeInternal(response, mapper -> mapper.readValue(mapper.createParser(responseBody(response)), typeRef));
    }

    @FunctionalInterface
    private interface Decoder<T> {
        T apply(ObjectMapper mapper) throws IOException;
    }

    private <T> T decodeInternal(Response response, Decoder<T> decoder) {
        int status = response.code();
        String rawBody = responseBody(response);
        try {
            if (status >= 200 && status < 300) {
                if (rawBody == null || rawBody.isEmpty()) {
                    return null;
                }
                return decoder.apply(mapper);
            }
            throw buildError(status, rawBody, response.header("X-Request-Id"));
        } catch (IOException e) {
            throw new DashException(
                    "failed to decode DASH response: " + e.getMessage(), e);
        }
    }

    private static String responseBody(Response response) {
        try (ResponseBody body = response.body()) {
            return body == null ? "" : body.string();
        } catch (IOException e) {
            throw new DashException("failed to read DASH response body: " + e.getMessage(), e);
        }
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
