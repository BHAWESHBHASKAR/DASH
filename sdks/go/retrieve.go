package dash

import (
	"context"
)

// RetrieveService calls POST /v1/retrieve. The endpoint is the
// differentiator: it returns Claim + Evidence + Contradiction
// results that a RAG pipeline can filter on directly, instead of a
// flat list of vector nearest-neighbours.
type RetrieveService struct {
	client *Client
}

// Query issues a retrieve call. TenantID and Query are required;
// TopK defaults to 10 and StanceMode defaults to "balanced" on the
// server when zero / empty.
//
// The ReturnGraph flag is optional on the wire. The struct's public
// type is bool for ergonomics; the SDK only emits the JSON field
// when the caller has explicitly set it via QueryWithReturnGraph.
// Use Query when you do not need the flag at all.
func (s *RetrieveService) Query(ctx context.Context, req RetrieveRequest) (*RetrieveResponse, error) {
	body := retrieveRequestBody{
		TenantID:   req.TenantID,
		Query:      req.Query,
		TopK:       req.TopK,
		StanceMode: req.StanceMode,
	}
	if body.TopK == 0 {
		body.TopK = 10
	}
	if body.StanceMode == "" {
		body.StanceMode = "balanced"
	}
	if req.ReturnGraph {
		body.ReturnGraph = &req.ReturnGraph
	}
	var out RetrieveResponse
	if err := s.client.post(ctx, "/v1/retrieve", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// retrieveRequestBody is the wire shape of POST /v1/retrieve.
// ReturnGraph is a *bool with omitempty so the field is dropped
// from the wire when the caller did not set it, matching the
// Python SDK's Optional[bool] behaviour.
type retrieveRequestBody struct {
	TenantID    string `json:"tenant_id"`
	Query       string `json:"query"`
	TopK        int    `json:"top_k"`
	StanceMode  string `json:"stance_mode"`
	ReturnGraph *bool  `json:"return_graph,omitempty"`
}
