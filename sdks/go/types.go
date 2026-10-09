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

// TimeRange is an inclusive unix-second window for RetrieveRequest.
// Either bound may be nil (open-ended).
type TimeRange struct {
	FromUnix *int64 `json:"from_unix,omitempty"`
	ToUnix   *int64 `json:"to_unix,omitempty"`
}

// RetrieveRequest is the request body for POST /v1/retrieve.
//
// Mirrors the JSON accepted by build_retrieve_transport_request_from_json
// in services/retrieval/src/transport/payload.rs. Only TenantID and Query
// are required; zero-valued optional fields are omitted from the wire body.
type RetrieveRequest struct {
	// TenantID is the tenant namespace to search within.
	TenantID string
	// Query is the free-text query.
	Query string
	// TopK is the maximum number of claims to return. Defaults to 5
	// (the server default) when zero.
	TopK int
	// StanceMode is "balanced" (default) or "support_only".
	StanceMode string
	// ReturnGraph optionally asks the server to also return the
	// claim graph.
	ReturnGraph bool
	// QueryEmbedding is an optional pre-computed query vector.
	QueryEmbedding []float32
	// EntityFilters restricts results to claims mentioning these entities.
	EntityFilters []string
	// EmbeddingIDFilters restricts results to these embedding ids.
	EmbeddingIDFilters []string
	// TimeRange optionally restricts results by event time.
	TimeRange *TimeRange
	// ReadConsistency is "one" (server default), "quorum" or "all".
	ReadConsistency string
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

// RetrieveResult is a single claim returned by /v1/retrieve
// (render_evidence_node_json in the retrieval service).
//
// The Claim + Evidence + Contradiction differentiator lives in the
// Supports and Contradicts fields: callers can filter on them without
// walking the Citations list. Fields after Citations are optional and
// nil when the server omits them or sends null.
type RetrieveResult struct {
	ClaimID       string     `json:"claim_id"`
	CanonicalText string     `json:"canonical_text"`
	Score         float32    `json:"score"`
	Supports      int        `json:"supports"`
	Contradicts   int        `json:"contradicts"`
	Citations     []Citation `json:"citations"`

	ClaimConfidence         *float32 `json:"claim_confidence,omitempty"`
	ConfidenceBand          *string  `json:"confidence_band,omitempty"`
	DominantStance          *string  `json:"dominant_stance,omitempty"`
	ContradictionRisk       *float32 `json:"contradiction_risk,omitempty"`
	GraphScore              *float32 `json:"graph_score,omitempty"`
	SupportPathCount        *int     `json:"support_path_count,omitempty"`
	ContradictionChainDepth *int     `json:"contradiction_chain_depth,omitempty"`
	EventTimeUnix           *int64   `json:"event_time_unix,omitempty"`
	TemporalMatchMode       *string  `json:"temporal_match_mode,omitempty"`
	TemporalInRange         *bool    `json:"temporal_in_range,omitempty"`
	ClaimType               *string  `json:"claim_type,omitempty"`
	ValidFrom               *int64   `json:"valid_from,omitempty"`
	ValidTo                 *int64   `json:"valid_to,omitempty"`
	CreatedAt               *int64   `json:"created_at,omitempty"`
	UpdatedAt               *int64   `json:"updated_at,omitempty"`
}

// GraphEdge is an edge of the evidence graph returned when
// RetrieveRequest.ReturnGraph is set.
type GraphEdge struct {
	FromClaimID string  `json:"from_claim_id"`
	ToClaimID   string  `json:"to_claim_id"`
	Relation    string  `json:"relation"`
	Strength    float32 `json:"strength"`
}

// RetrieveGraph is the evidence graph of a retrieve response.
type RetrieveGraph struct {
	Nodes []RetrieveResult `json:"nodes"`
	Edges []GraphEdge      `json:"edges"`
}

// RetrieveResponse is the response body for POST /v1/retrieve.
// The wire format is {"results": [...], "graph": ..., "read_policy": ...,
// "read_quorum_met": ..., "serving_replica": ...}; everything except
// Results is optional.
type RetrieveResponse struct {
	Results        []RetrieveResult `json:"results"`
	Graph          *RetrieveGraph   `json:"graph,omitempty"`
	ReadPolicy     *string          `json:"read_policy,omitempty"`
	ReadQuorumMet  *bool            `json:"read_quorum_met,omitempty"`
	ServingReplica *string          `json:"serving_replica,omitempty"`
}
