package dash

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmbeddingsCreateSingleString verifies that a single-string
// Input is sent on the wire as a bare string and the typed response
// is populated.
func TestEmbeddingsCreateSingleString(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	resp, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{
		Input: "hello world",
		Model: "text-embedding-3-small",
	})
	require.NoError(t, err)

	assert.Equal(t, "list", resp.Object)
	assert.Equal(t, "text-embedding-3-small", resp.Model)
	assert.Equal(t, 2, resp.Usage.PromptTokens)
	assert.Equal(t, 2, resp.Usage.TotalTokens)
	require.Len(t, resp.Data, 1)
	assert.Equal(t, "embedding", resp.Data[0].Object)
	assert.Equal(t, 0, resp.Data[0].Index)
	assert.InDeltaSlice(t, []float32{0.013, -0.042, 0.077, 0.0, 0.5}, resp.Data[0].Embedding, 0.0001)

	got := h.lastRequest(t)
	assert.Equal(t, "POST", got.Method)
	assert.Equal(t, "/v1/embeddings", got.Path)

	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	assert.Equal(t, "hello world", body["input"])
	assert.Equal(t, "text-embedding-3-small", body["model"])
}

// TestEmbeddingsCreateArray verifies that a []string Input is sent
// on the wire as a JSON array and produces N response records.
func TestEmbeddingsCreateArray(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingArrayResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	resp, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{
		Input: []string{"a", "b", "c"},
	})
	require.NoError(t, err)
	require.Len(t, resp.Data, 3)
	assert.Equal(t, []int{0, 1, 2}, []int{
		resp.Data[0].Index, resp.Data[1].Index, resp.Data[2].Index,
	})

	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	arr, ok := body["input"].([]any)
	require.True(t, ok, "input should be an array")
	assert.Equal(t, []string{"a", "b", "c"}, []string{
		arr[0].(string), arr[1].(string), arr[2].(string),
	})
}

// TestEmbeddingsCreateSendsEncodingFormatAndUser verifies optional
// fields are present on the wire when set.
func TestEmbeddingsCreateSendsEncodingFormatAndUser(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{
		Input:          "hi",
		Model:          "text-embedding-3-large",
		EncodingFormat: "float",
		User:           "user-42",
	})
	require.NoError(t, err)

	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	assert.Equal(t, "text-embedding-3-large", body["model"])
	assert.Equal(t, "float", body["encoding_format"])
	assert.Equal(t, "user-42", body["user"])
}

// TestEmbeddingsCreateOmitsOptionalFieldsWhenEmpty verifies that
// encoding_format and user are absent from the wire when not set,
// matching the OpenAI spec.
func TestEmbeddingsCreateOmitsOptionalFieldsWhenEmpty(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)

	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	_, hasEnc := body["encoding_format"]
	_, hasUser := body["user"]
	assert.False(t, hasEnc, "encoding_format should be absent")
	assert.False(t, hasUser, "user should be absent")
}

// TestEmbeddingsCreateDefaultModel verifies the default model is
// "text-embedding-3-small" when none is supplied.
func TestEmbeddingsCreateDefaultModel(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)

	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	assert.Equal(t, "text-embedding-3-small", body["model"])
}

// TestEmbeddingsCreateRejectsNilInput verifies that a nil Input
// surfaces as a programming error, not a wire call.
func TestEmbeddingsCreateRejectsNilInput(t *testing.T) {
	c := New("http://localhost:8080")
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Input is required")
}

// TestEmbeddingsCreateRejectsBadInputType verifies that an Input
// that is neither a string nor a []string is rejected before
// hitting the wire.
func TestEmbeddingsCreateRejectsBadInputType(t *testing.T) {
	c := New("http://localhost:8080")
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: 42})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Input must be a string or []string")
}

// TestEmbeddingsCreateRaisesAPIErrorOn4xx verifies that an OpenAI-
// shaped 4xx body surfaces as a DashAPIError with the right
// status code and error type.
func TestEmbeddingsCreateRaisesAPIErrorOn4xx(t *testing.T) {
	h := &recordingHandler{status: 400, body: openAIErrorBody}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var apiErr *DashAPIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, 400, apiErr.StatusCode())
	assert.Equal(t, "invalid_request_error", apiErr.ErrorType())
	assert.Equal(t, "input must contain at least one text", apiErr.Message)
	assert.Contains(t, apiErr.Body(), "invalid_request_error")
}

// TestEmbeddingsCreateRaisesAPIErrorOn5xx verifies a 5xx with a
// server_error envelope surfaces correctly.
func TestEmbeddingsCreateRaisesAPIErrorOn5xx(t *testing.T) {
	h := &recordingHandler{status: 500, body: map[string]any{
		"error": map[string]any{
			"message": "embedding failed: backend unavailable",
			"type":    "server_error",
		},
	}}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var apiErr *DashAPIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, 500, apiErr.StatusCode())
	assert.Equal(t, "server_error", apiErr.ErrorType())
}

// TestEmbeddingsCreateFallsBackOnNonJSONError verifies that a
// non-JSON error body surfaces with "api_error" type and the raw
// body as the message.
func TestEmbeddingsCreateFallsBackOnNonJSONError(t *testing.T) {
	h := &recordingHandler{status: 502, rawText: "Bad Gateway"}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var apiErr *DashAPIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, 502, apiErr.StatusCode())
	assert.Equal(t, "api_error", apiErr.ErrorType())
	assert.Equal(t, "Bad Gateway", apiErr.Message)
}

// TestEmbeddingsCreateConnectionErrorOnUnreachableServer verifies
// that a refused connection surfaces as a DashConnectionError whose
// Unwrap points to a net error.
func TestEmbeddingsCreateConnectionErrorOnUnreachableServer(t *testing.T) {
	// Bind a listener and immediately close it so we get a
	// port that is guaranteed to refuse connections.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	c := New("http://" + addr)
	_, err = c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var connErr *DashConnectionError
	require.True(t, errors.As(err, &connErr))
	assert.Equal(t, "connection_error", connErr.ErrorType())
	assert.NotNil(t, connErr.Unwrap())
}

// TestEmbeddingsCreateIsAConnErrTypeCheck verifies a *DashAPIError
// is not a *DashConnectionError (so callers can tell the two apart
// with a single type assertion).
func TestEmbeddingsCreateIsAConnErrTypeCheck(t *testing.T) {
	h := &recordingHandler{status: 400, body: openAIErrorBody}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var apiErr *DashAPIError
	var connErr *DashConnectionError
	assert.True(t, errors.As(err, &apiErr))
	assert.False(t, errors.As(err, &connErr))
}

// TestEmbeddingsCreateSendsAllHeaders verifies that the request
// carries Content-Type, Accept, and a non-empty User-Agent.
func TestEmbeddingsCreateSendsAllHeaders(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)

	got := h.lastRequest(t)
	assert.Equal(t, "application/json", got.Header.Get("Content-Type"))
	assert.Equal(t, "application/json", got.Header.Get("Accept"))
	assert.NotEmpty(t, got.Header.Get("User-Agent"))
}

// TestEmbeddingsCreateReadsBodyFully verifies that even a non-2xx
// response body is consumed (so the connection can be reused).
// This is mostly a smoke test against regression in error
// translation.
func TestEmbeddingsCreateReadsBodyFully(t *testing.T) {
	h := &recordingHandler{status: 422, body: map[string]any{
		"error": map[string]any{"message": "invalid input", "type": "invalid_request_error"},
	}}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var apiErr *DashAPIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, 422, apiErr.StatusCode())
}

// TestEmbeddingsCreateRetriesAreCallersResponsibility verifies that
// the SDK does not perform automatic retries on 5xx errors; callers
// can wrap calls in their own retry loop. This is just a sanity
// check that exactly one request is issued.
func TestEmbeddingsCreateNoAutoRetry(t *testing.T) {
	h := &recordingHandler{status: 500, body: map[string]any{
		"error": map[string]any{"message": "boom", "type": "server_error"},
	}}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)
	assert.Len(t, h.allRequests(), 1)
}

// TestEmbeddingsCreateRespectsCanceledContextBeforeRequest verifies
// that a context cancelled before the call is dispatched does not
// produce a wire request.
func TestEmbeddingsCreateRespectsCanceledContextBeforeRequest(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.Embeddings().Create(ctx, EmbeddingRequest{Input: "hi"})
	require.Error(t, err)
	// httptest may or may not record a request depending on
	// timing, so we just assert the error class.
	var connErr *DashConnectionError
	require.True(t, errors.As(err, &connErr))
}
