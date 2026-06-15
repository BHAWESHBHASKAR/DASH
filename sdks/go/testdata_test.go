package dash

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// sampleEmbeddingResponse is the OpenAI-compatible wire body for a
// single-input embeddings call.
var sampleEmbeddingResponse = map[string]any{
	"object": "list",
	"data": []map[string]any{
		{
			"object":    "embedding",
			"embedding": []float32{0.013, -0.042, 0.077, 0.0, 0.5},
			"index":     0,
		},
	},
	"model":  "text-embedding-3-small",
	"usage":  map[string]any{"prompt_tokens": 2, "total_tokens": 2},
}

// sampleEmbeddingArrayResponse is the OpenAI-compatible wire body
// for a 3-string-array input.
var sampleEmbeddingArrayResponse = map[string]any{
	"object": "list",
	"data": []map[string]any{
		{"object": "embedding", "embedding": []float32{0.1, 0.2, 0.3}, "index": 0},
		{"object": "embedding", "embedding": []float32{0.4, 0.5, 0.6}, "index": 1},
		{"object": "embedding", "embedding": []float32{0.7, 0.8, 0.9}, "index": 2},
	},
	"model": "text-embedding-3-small",
	"usage": map[string]any{"prompt_tokens": 6, "total_tokens": 6},
}

// sampleRetrieveResponse is the native /v1/retrieve wire body for
// a single claim with one citation.
var sampleRetrieveResponse = map[string]any{
	"results": []map[string]any{
		{
			"claim_id":       "claim-1",
			"canonical_text": "Acme Co. was acquired in 2024.",
			"score":          0.93,
			"supports":       4,
			"contradicts":    1,
			"citations": []map[string]any{
				{
					"evidence_id":     "ev-1",
					"source_id":       "source://reuters",
					"stance":          "supports",
					"source_quality":  0.88,
					"chunk_id":        "chunk-7",
					"span_start":      120,
					"span_end":        168,
					"doc_id":          "doc://reuters-acme",
					"extraction_model": "extractor-v5",
					"ingested_at":     1735689700000,
				},
			},
		},
	},
}

// openAIErrorBody is the OpenAI-shaped error envelope emitted by
// /v1/embeddings.
var openAIErrorBody = map[string]any{
	"error": map[string]any{
		"message": "input must contain at least one text",
		"type":    "invalid_request_error",
		"param":   nil,
		"code":    nil,
	},
}

// recordingHandler builds an http.Handler that records the most
// recent request and returns the given status / body for every
// request. It is the workhorse of the test suite.
type recordingHandler struct {
	mu       sync.Mutex
	requests []recordedRequest
	status   int
	body     any
	rawText  string
	headers  http.Header
	// headersHandler overrides the default body+status reply and
	// allows tests to inspect the request headers.
	headersHandler func(w http.ResponseWriter, r *http.Request)
}

type recordedRequest struct {
	Method string
	Path   string
	Body   []byte
	Header http.Header
}

func (h *recordingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	h.mu.Lock()
	h.requests = append(h.requests, recordedRequest{
		Method: r.Method,
		Path:   r.URL.Path,
		Body:   body,
		Header: r.Header.Clone(),
	})
	h.mu.Unlock()

	if h.headersHandler != nil {
		h.headersHandler(w, r)
		return
	}
	writeResponse(w, h.status, h.body, h.rawText)
}

func (h *recordingHandler) lastRequest(t *testing.T) recordedRequest {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	require.NotEmpty(t, h.requests, "no requests recorded")
	return h.requests[len(h.requests)-1]
}

func (h *recordingHandler) allRequests() []recordedRequest {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]recordedRequest, len(h.requests))
	copy(out, h.requests)
	return out
}

func newTestServer(t *testing.T, h *recordingHandler) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

func writeResponse(w http.ResponseWriter, status int, body any, raw string) {
	if raw != "" {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(raw))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if body == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(body)
}
