package dash

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewStripsTrailingSlash verifies that a base URL with a trailing
// slash is normalised so that request paths do not contain "//".
func TestNewStripsTrailingSlash(t *testing.T) {
	c := New("http://localhost:8080/")
	assert.Equal(t, "http://localhost:8080", c.BaseURL())
}

// TestNewPreservesBaseURLWithoutSlash verifies that a base URL
// without a trailing slash is preserved unchanged.
func TestNewPreservesBaseURLWithoutSlash(t *testing.T) {
	c := New("http://localhost:8080")
	assert.Equal(t, "http://localhost:8080", c.BaseURL())
}

// TestWithAPIKeySetsBearerHeader verifies that WithAPIKey is
// reflected in the Authorization header of every outgoing request.
func TestWithAPIKeySetsBearerHeader(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL, WithAPIKey("sk-test-123"))
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)

	got := h.lastRequest(t)
	assert.Equal(t, "Bearer sk-test-123", got.Header.Get("Authorization"))
}

// TestNoAPIKeyOmitsAuthorization verifies that, with no API key, no
// Authorization header is sent.
func TestNoAPIKeyOmitsAuthorization(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)

	got := h.lastRequest(t)
	assert.Empty(t, got.Header.Get("Authorization"))
}

// TestDefaultUserAgentIncludesSDKVersion verifies the default
// User-Agent is "dash-go/<version>" and Content-Type / Accept
// headers are present.
func TestDefaultUserAgentIncludesSDKVersion(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)

	got := h.lastRequest(t)
	assert.Equal(t, "dash-go/"+Version, got.Header.Get("User-Agent"))
	assert.Equal(t, "application/json", got.Header.Get("Content-Type"))
	assert.Equal(t, "application/json", got.Header.Get("Accept"))
}

// TestWithHeaderOverridesUserAgent verifies that a User-Agent
// supplied via WithHeader is forwarded verbatim and not overwritten
// by the SDK default.
func TestWithHeaderOverridesUserAgent(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL, WithHeader("User-Agent", "my-app/1.0"))
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)

	got := h.lastRequest(t)
	assert.Equal(t, "my-app/1.0", got.Header.Get("User-Agent"))
}

// TestWithHeaderCustomHeader verifies a custom (non-User-Agent)
// header propagates to the wire.
func TestWithHeaderCustomHeader(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL, WithHeader("X-Trace-Id", "abc-123"))
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)

	got := h.lastRequest(t)
	assert.Equal(t, "abc-123", got.Header.Get("X-Trace-Id"))
}

// TestWithTimeoutSetsPerRequestTimeout verifies that WithTimeout
// surfaces in the http.Client.Timeout so a slow server trips it.
func TestWithTimeoutSetsPerRequestTimeout(t *testing.T) {
	// A server that sleeps longer than the client's timeout. The
	// test passes if the client surfaces a DashConnectionError
	// instead of hanging.
	h := &recordingHandler{
		headersHandler: func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(200 * time.Millisecond)
			writeResponse(w, 200, sampleEmbeddingResponse, "")
		},
	}
	srv := newTestServer(t, h)

	c := New(srv.URL, WithTimeout(50*time.Millisecond))
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var connErr *DashConnectionError
	require.ErrorAs(t, err, &connErr)
	assert.Equal(t, 0, connErr.StatusCode())
	assert.Equal(t, "connection_error", connErr.ErrorType())
}

// TestEmbeddingsAndRetrieveAreSingletons verifies the two accessors
// return the same instance on every call (cheap getters, no
// allocations on the hot path).
func TestEmbeddingsAndRetrieveAreSingletons(t *testing.T) {
	c := New("http://localhost:8080")
	assert.Same(t, c.Embeddings(), c.Embeddings())
	assert.Same(t, c.Retrieve(), c.Retrieve())
	assert.NotSame(t, any(c.Embeddings()), any(c.Retrieve()))
}

// TestCloseIsIdempotentAndNoop verifies Close can be called any
// number of times without panicking.
func TestCloseIsIdempotentAndNoop(t *testing.T) {
	c := New("http://localhost:8080")
	require.NoError(t, c.Close())
	require.NoError(t, c.Close())
}

// TestContextCancellationPropagates verifies a cancelled context
// surfaces as a DashConnectionError instead of completing normally.
func TestContextCancellationPropagates(t *testing.T) {
	h := &recordingHandler{
		headersHandler: func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		},
	}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Embeddings().Create(ctx, EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var connErr *DashConnectionError
	require.ErrorAs(t, err, &connErr)
	assert.Equal(t, "connection_error", connErr.ErrorType())
}

// TestRequestBodyMarshalsJSON verifies the outgoing body is a
// well-formed JSON object whose keys match the wire contract.
func TestRequestBodyMarshalsJSON(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{
		Input: "hello",
		Model: "text-embedding-3-small",
	})
	require.NoError(t, err)

	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	assert.Equal(t, "hello", body["input"])
	assert.Equal(t, "text-embedding-3-small", body["model"])
}
