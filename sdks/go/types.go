package dash

// EmbeddingRequest is the request body for POST /v1/embeddings.
//
// Mirrors services/retrieval/src/openai_embeddings.rs.
// Input accepts either a single string or a list of strings; the
// value is passed through to the server unchanged so the wire body
// stays byte-for-byte compatible with the OpenAI spec.
type EmbeddingRequest struct {
	// Input is the text to embed. Pass a string for a single input
	// or a []string to embed many inputs in one call.
	Input any
	// Model is a hint for the server. DASH uses its configured
	// embedding provider regardless, but the value is echoed back
	// in the response. Defaults to "text-embedding-3-small" to
	// match OpenAI's own default.
	Model string
	// EncodingFormat is "float" only; other values are rejected by
	// the server. Omit for the default behaviour.
	EncodingFormat string
	// User is an opaque OpenAI-style user identifier. Omit for the
	// default behaviour.
	User string
}

// EmbeddingData is one embedding vector in a response.
type EmbeddingData struct {
	Object    string    `json:"object"`
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

// EmbeddingUsage is the token usage block returned alongside the
// embedding vectors.
type EmbeddingUsage struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

// EmbeddingResponse is the response body for POST /v1/embeddings.
type EmbeddingResponse struct {
	Object string          `json:"object"`
	Data   []EmbeddingData `json:"data"`
	Model  string          `json:"model"`
	Usage  EmbeddingUsage  `json:"usage"`
}

// RetrieveRequest is the request body for POST /v1/retrieve.
//
// Mirrors schema::RetrievalRequest and the test JSON in
// services/retrieval/tests/transport_http.rs:
//
//	{"tenant_id": "...", "query": "...",
//	 "top_k": 10, "stance_mode": "balanced",
//	 "return_graph": false}
type RetrieveRequest struct {
	// TenantID is the tenant namespace to search within.
	TenantID string
	// Query is the free-text query.
	Query string
	// TopK is the maximum number of claims to return. Defaults to
	// 10 on the server when zero.
	TopK int
	// StanceMode is "balanced" (default) or "support_only".
	StanceMode string
	// ReturnGraph optionally asks the server to also return the
	// claim graph. Servers that do not implement it silently ignore
	// the flag.
	ReturnGraph bool
}

// Citation is a single evidence citation attached to a retrieval
// result.
//
// Mirrors schema::Citation. The optional fields (ChunkID, SpanStart,
// SpanEnd, DocID, ExtractionModel, IngestedAt) are pointers so the
// JSON "absent" case round-trips as nil rather than the zero value.
type Citation struct {
	EvidenceID      string  `json:"evidence_id"`
	SourceID        string  `json:"source_id"`
	Stance          string  `json:"stance"`
	SourceQuality   float32 `json:"source_quality"`
	ChunkID         *string `json:"chunk_id,omitempty"`
	SpanStart       *uint32 `json:"span_start,omitempty"`
	SpanEnd         *uint32 `json:"span_end,omitempty"`
	DocID           *string `json:"doc_id,omitempty"`
	ExtractionModel *string `json:"extraction_model,omitempty"`
	IngestedAt      *int64  `json:"ingested_at,omitempty"`
}

// RetrieveResult is a single claim returned by /v1/retrieve.
//
// Mirrors schema::RetrievalResult. The Claim + Evidence +
// Contradiction differentiator lives in the Supports and
// Contradicts fields: callers can filter on them without walking
// the Citations list.
type RetrieveResult struct {
	ClaimID       string     `json:"claim_id"`
	CanonicalText string     `json:"canonical_text"`
	Score         float32    `json:"score"`
	Supports      int        `json:"supports"`
	Contradicts   int        `json:"contradicts"`
	Citations     []Citation `json:"citations"`
}

// RetrieveResponse is the response body for POST /v1/retrieve.
// The wire format is {"results": [...]}.
type RetrieveResponse struct {
	Results []RetrieveResult `json:"results"`
}
