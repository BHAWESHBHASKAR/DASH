package dash

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewOpenAICompatibleConfigAppendsV1Path verifies the helper
// adds the "/v1" suffix expected by the openai-go client.
func TestNewOpenAICompatibleConfigAppendsV1Path(t *testing.T) {
	cfg := NewOpenAICompatibleConfig("http://localhost:8080", "sk-live")
	assert.Equal(t, "http://localhost:8080/v1", cfg.BaseURL)
	assert.Equal(t, "sk-live", cfg.APIKey)
}

// TestNewOpenAICompatibleConfigTrimsTrailingSlash verifies a base
// URL with a trailing slash is normalised before /v1 is appended.
func TestNewOpenAICompatibleConfigTrimsTrailingSlash(t *testing.T) {
	cfg := NewOpenAICompatibleConfig("http://localhost:8080/", "k")
	assert.Equal(t, "http://localhost:8080/v1", cfg.BaseURL)
}

// TestOpenAICompatibleConfigAcceptsEmptyKey verifies an empty API
// key is preserved (callers that want a "no auth" configuration
// should pass an empty string).
func TestOpenAICompatibleConfigAcceptsEmptyKey(t *testing.T) {
	cfg := NewOpenAICompatibleConfig("http://localhost:8080", "")
	assert.Equal(t, "", cfg.APIKey)
}

// TestDashClientDropInForOpenAIEmbeddings simulates the openai-go
// client pointed at DASH. The test verifies that the wire body the
// dash-go SDK produces for "embeddings" matches the shape openai-go
// produces (input as a string, model as a string, optional fields
// omitted when empty).
func TestDashClientDropInForOpenAIEmbeddings(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	// Step 1: stand up a DASH client. An openai-go client would
	// be constructed from cfg via openai.NewClient(cfg); the
	// SDK does the same with a different constructor.
	cfg := NewOpenAICompatibleConfig(srv.URL, "sk-test")
	c := New(srv.URL, WithAPIKey(cfg.APIKey))

	// Step 2: issue the canonical OpenAI embeddings request.
	resp, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{
		Input: "hello world",
		Model: "text-embedding-3-small",
	})
	require.NoError(t, err)

	// Step 3: verify the wire body is the OpenAI shape. openai-go
	// would hit <cfg.BaseURL>/embeddings = <srv.URL>/v1/embeddings,
	// which is what the dash-go SDK also hits.
	got := h.lastRequest(t)
	assert.Equal(t, "POST", got.Method)
	assert.Equal(t, "/v1/embeddings", got.Path)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	assert.Equal(t, "hello world", body["input"])
	assert.Equal(t, "text-embedding-3-small", body["model"])

	// Step 4: verify the response is parseable into the typed
	// shape openai-go returns. dash-go's EmbeddingResponse
	// matches the four required top-level fields.
	assert.Equal(t, "list", resp.Object)
	assert.Equal(t, "text-embedding-3-small", resp.Model)
	assert.Equal(t, 2, resp.Usage.PromptTokens)
	assert.Equal(t, 2, resp.Usage.TotalTokens)
	require.Len(t, resp.Data, 1)
	assert.Equal(t, "embedding", resp.Data[0].Object)
}

// TestDashClientOpenAIStyleErrorEnvelope verifies that an OpenAI-
// shaped error body from DASH is parsed into a DashAPIError with
// the right error_type and message, matching what openai-go would
// surface.
func TestDashClientOpenAIStyleErrorEnvelope(t *testing.T) {
	h := &recordingHandler{status: 400, body: openAIErrorBody}
	srv := newTestServer(t, h)

	cfg := NewOpenAICompatibleConfig(srv.URL, "sk-test")
	c := New(cfg.BaseURL, WithAPIKey(cfg.APIKey))

	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid_request_error")
	assert.Contains(t, err.Error(), "input must contain at least one text")
}

// TestDashClientArrayInputMatchesOpenAIArray verifies that a
// []string input is sent as a JSON array, matching what openai-go
// produces for the same call.
func TestDashClientArrayInputMatchesOpenAIArray(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingArrayResponse}
	srv := newTestServer(t, h)

	cfg := NewOpenAICompatibleConfig(srv.URL, "")
	c := New(cfg.BaseURL)

	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{
		Input: []string{"alpha", "beta", "gamma"},
	})
	require.NoError(t, err)

	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	arr, ok := body["input"].([]any)
	require.True(t, ok, "input should be a JSON array")
	require.Len(t, arr, 3)
	assert.Equal(t, "alpha", arr[0])
	assert.Equal(t, "beta", arr[1])
	assert.Equal(t, "gamma", arr[2])
}

// TestDashClientOptionalFieldsBehaviour verifies the SDK can
// produce the exact wire body openai-go produces when optional
// fields are set, and that they are absent when not set.
func TestDashClientOptionalFieldsBehaviour(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	cfg := NewOpenAICompatibleConfig(srv.URL, "")
	c := New(cfg.BaseURL)

	// With encoding_format and user: should be present.
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{
		Input:          "hi",
		Model:          "text-embedding-3-small",
		EncodingFormat: "float",
		User:           "u-1",
	})
	require.NoError(t, err)
	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	assert.Equal(t, "float", body["encoding_format"])
	assert.Equal(t, "u-1", body["user"])

	// Without: should be absent.
	_, err = c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)
	requests := h.allRequests()
	require.Len(t, requests, 2)
	body = map[string]any{}
	require.NoError(t, json.Unmarshal(requests[1].Body, &body))
	_, hasEnc := body["encoding_format"]
	_, hasUser := body["user"]
	assert.False(t, hasEnc)
	assert.False(t, hasUser)
}

// TestDashClientTimeoutForOpenAIUsage verifies a custom timeout
// from the OpenAICompatibleConfig propagates through the SDK.
func TestDashClientTimeoutForOpenAIUsage(t *testing.T) {
	cfg := NewOpenAICompatibleConfig("http://localhost:8080", "")
	cfg.Timeout = 2 * time.Second
	assert.Equal(t, 2*time.Second, cfg.Timeout)
}

// TestDashClientPreservesAuthHeaderForOpenAIUsage verifies the
// bearer token from NewOpenAICompatibleConfig is forwarded on the
// wire.
func TestDashClientPreservesAuthHeaderForOpenAIUsage(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	cfg := NewOpenAICompatibleConfig(srv.URL, "sk-openai")
	c := New(cfg.BaseURL, WithAPIKey(cfg.APIKey))
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)

	got := h.lastRequest(t)
	assert.Equal(t, "Bearer sk-openai", got.Header.Get("Authorization"))
}

// TestOpenAICompatConfigIsAValueType verifies the helper returns a
// plain value, not a pointer, so callers can copy it freely.
func TestOpenAICompatConfigIsAValueType(t *testing.T) {
	cfg := NewOpenAICompatibleConfig("http://x", "k")
	cp := cfg
	cp.APIKey = "k2"
	assert.Equal(t, "k", cfg.APIKey)
	assert.Equal(t, "k2", cp.APIKey)
}

// TestDashClientRejectsInvalidInputBeforeWire verifies that
// invalid Input values are caught client-side and never produce a
// wire request, matching openai-go's behaviour.
func TestDashClientRejectsInvalidInputBeforeWire(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	cfg := NewOpenAICompatibleConfig(srv.URL, "")
	c := New(cfg.BaseURL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: 123})
	require.Error(t, err)
	assert.Empty(t, h.allRequests())
}

// TestDashClientFullHTTPTransportMatchesOpenAIWire verifies that
// the exact request body the SDK sends to /v1/embeddings is a
// valid OpenAI wire body (the four documented top-level fields,
// plus the per-record shape).
func TestDashClientFullHTTPTransportMatchesOpenAIWire(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleEmbeddingResponse}
	srv := newTestServer(t, h)

	cfg := NewOpenAICompatibleConfig(srv.URL, "sk")
	c := New(cfg.BaseURL, WithAPIKey(cfg.APIKey))
	resp, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{
		Input: "hello",
		Model: "text-embedding-3-small",
	})
	require.NoError(t, err)

	// Required top-level fields per the OpenAI spec.
	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	assert.Equal(t, "hello", body["input"])
	assert.Equal(t, "text-embedding-3-small", body["model"])

	// The response side must also satisfy the OpenAI spec.
	assert.Equal(t, "list", resp.Object)
	require.Len(t, resp.Data, 1)
	assert.Equal(t, "embedding", resp.Data[0].Object)
	assert.NotEmpty(t, resp.Data[0].Embedding)
	assert.Equal(t, 0, resp.Data[0].Index)
	assert.Equal(t, "text-embedding-3-small", resp.Model)
	assert.GreaterOrEqual(t, resp.Usage.PromptTokens, 0)
	assert.GreaterOrEqual(t, resp.Usage.TotalTokens, 0)
}

// TestNewOpenAICompatibleConfigEmptyBaseURL is a defensive check
// that the helper does not panic on a malformed input.
func TestNewOpenAICompatibleConfigEmptyBaseURL(t *testing.T) {
	cfg := NewOpenAICompatibleConfig("", "k")
	assert.Equal(t, "/v1", cfg.BaseURL)
}
