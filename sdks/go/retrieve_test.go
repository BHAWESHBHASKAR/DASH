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

// TestRetrieveDefaultStanceModeAndTopK verifies that a Query call
// with zero values uses the documented defaults (top_k=10,
// stance_mode="balanced") on the wire and that the typed response is
// populated.
func TestRetrieveDefaultStanceModeAndTopK(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleRetrieveResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	resp, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
		TenantID: "tenant-a",
		Query:    "company x",
	})
	require.NoError(t, err)

	got := h.lastRequest(t)
	assert.Equal(t, "POST", got.Method)
	assert.Equal(t, "/v1/retrieve", got.Path)

	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	assert.Equal(t, "tenant-a", body["tenant_id"])
	assert.Equal(t, "company x", body["query"])
	assert.EqualValues(t, 10, body["top_k"])
	assert.Equal(t, "balanced", body["stance_mode"])

	require.Len(t, resp.Results, 1)
	r := resp.Results[0]
	assert.Equal(t, "claim-1", r.ClaimID)
	assert.Equal(t, "Acme Co. was acquired in 2024.", r.CanonicalText)
	assert.InDelta(t, 0.93, r.Score, 0.0001)
	assert.Equal(t, 4, r.Supports)
	assert.Equal(t, 1, r.Contradicts)
	require.Len(t, r.Citations, 1)

	cit := r.Citations[0]
	assert.Equal(t, "ev-1", cit.EvidenceID)
	assert.Equal(t, "source://reuters", cit.SourceID)
	assert.Equal(t, "supports", cit.Stance)
	assert.InDelta(t, 0.88, cit.SourceQuality, 0.0001)
	require.NotNil(t, cit.ChunkID)
	assert.Equal(t, "chunk-7", *cit.ChunkID)
	require.NotNil(t, cit.SpanStart)
	assert.EqualValues(t, 120, *cit.SpanStart)
	require.NotNil(t, cit.SpanEnd)
	assert.EqualValues(t, 168, *cit.SpanEnd)
	require.NotNil(t, cit.DocID)
	assert.Equal(t, "doc://reuters-acme", *cit.DocID)
	require.NotNil(t, cit.ExtractionModel)
	assert.Equal(t, "extractor-v5", *cit.ExtractionModel)
	require.NotNil(t, cit.IngestedAt)
	assert.EqualValues(t, 1735689700000, *cit.IngestedAt)
}

// TestRetrieveCustomTopKAndStanceMode verifies that explicit values
// are forwarded verbatim.
func TestRetrieveCustomTopKAndStanceMode(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleRetrieveResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	for _, mode := range []string{"balanced", "support_only"} {
		_, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
			TenantID:   "tenant-a",
			Query:      "q",
			TopK:       3,
			StanceMode: mode,
		})
		require.NoError(t, err)
	}

	requests := h.allRequests()
	require.Len(t, requests, 2)

	for i, want := range []string{"balanced", "support_only"} {
		var body map[string]any
		require.NoError(t, json.Unmarshal(requests[i].Body, &body))
		assert.EqualValues(t, 3, body["top_k"])
		assert.Equal(t, want, body["stance_mode"])
	}
}

// TestRetrieveReturnGraphFlagSentWhenTrue verifies that setting
// ReturnGraph=true emits the field on the wire.
func TestRetrieveReturnGraphFlagSentWhenTrue(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleRetrieveResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
		TenantID:    "tenant-a",
		Query:       "q",
		ReturnGraph: true,
	})
	require.NoError(t, err)

	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	assert.Equal(t, true, body["return_graph"])
}

// TestRetrieveReturnGraphOmittedWhenFalse verifies that the
// return_graph field is dropped from the wire when false, matching
// the Python SDK's Optional[bool] behaviour.
func TestRetrieveReturnGraphOmittedWhenFalse(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleRetrieveResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
		TenantID: "tenant-a",
		Query:    "q",
	})
	require.NoError(t, err)

	got := h.lastRequest(t)
	var body map[string]any
	require.NoError(t, json.Unmarshal(got.Body, &body))
	_, present := body["return_graph"]
	assert.False(t, present, "return_graph should be absent when false")
}

// TestRetrieveHandlesEmptyResults verifies that an empty results
// array is returned as a typed empty slice.
func TestRetrieveHandlesEmptyResults(t *testing.T) {
	h := &recordingHandler{status: 200, body: map[string]any{"results": []any{}}}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	resp, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
		TenantID: "tenant-a",
		Query:    "q",
	})
	require.NoError(t, err)
	assert.Empty(t, resp.Results)
}

// TestRetrieveCitationOptionalFieldsNil verifies that a citation
// with no optional fields round-trips as nil pointers (so callers
// can detect absence with a simple nil check).
func TestRetrieveCitationOptionalFieldsNil(t *testing.T) {
	h := &recordingHandler{status: 200, body: map[string]any{
		"results": []map[string]any{
			{
				"claim_id":       "c1",
				"canonical_text": "minimal claim",
				"score":          0.5,
				"supports":       1,
				"contradicts":    0,
				"citations": []map[string]any{
					{
						"evidence_id":    "e1",
						"source_id":      "s1",
						"stance":         "supports",
						"source_quality": 0.9,
					},
				},
			},
		},
	}}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	resp, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
		TenantID: "t",
		Query:    "q",
	})
	require.NoError(t, err)
	require.Len(t, resp.Results, 1)
	cit := resp.Results[0].Citations[0]
	assert.Nil(t, cit.ChunkID)
	assert.Nil(t, cit.SpanStart)
	assert.Nil(t, cit.SpanEnd)
	assert.Nil(t, cit.DocID)
	assert.Nil(t, cit.ExtractionModel)
	assert.Nil(t, cit.IngestedAt)
}

// TestRetrieveRaisesAPIErrorOnAdHocShape verifies that a non-2xx
// response with a /v1/retrieve-style ad-hoc error envelope
// ({"error": "..."}) is parsed correctly.
func TestRetrieveRaisesAPIErrorOnAdHocShape(t *testing.T) {
	h := &recordingHandler{status: 503, body: map[string]any{
		"error": "routing unavailable",
		"code":  "no_healthy_node",
	}}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
		TenantID: "tenant-a",
		Query:    "q",
	})
	require.Error(t, err)

	var apiErr *DashAPIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, 503, apiErr.StatusCode())
	assert.Equal(t, "routing unavailable", apiErr.Message)
}

// TestRetrieveRaisesAPIErrorOn403 verifies that a 403 (e.g. tenant
// not allowed) is surfaced as a typed DashAPIError.
func TestRetrieveRaisesAPIErrorOn403(t *testing.T) {
	h := &recordingHandler{status: 403, body: map[string]any{
		"error": "tenant is not allowed for this API key",
	}}
	srv := newTestServer(t, h)

	c := New(srv.URL, WithAPIKey("sk-test"))
	_, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
		TenantID: "tenant-other",
		Query:    "q",
	})
	require.Error(t, err)

	var apiErr *DashAPIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, 403, apiErr.StatusCode())
}

// TestRetrieveConnectionErrorOnRefusedConnection verifies that a
// refused TCP connection surfaces as a DashConnectionError.
func TestRetrieveConnectionErrorOnRefusedConnection(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	c := New("http://" + addr)
	_, err = c.Retrieve().Query(context.Background(), RetrieveRequest{
		TenantID: "tenant-a",
		Query:    "q",
	})
	require.Error(t, err)

	var connErr *DashConnectionError
	require.True(t, errors.As(err, &connErr))
	assert.Equal(t, "connection_error", connErr.ErrorType())
}

// TestRetrieveSupportsAllStanceModes iterates over the documented
// stance modes and verifies each is sent verbatim.
func TestRetrieveSupportsAllStanceModes(t *testing.T) {
	h := &recordingHandler{status: 200, body: sampleRetrieveResponse}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	modes := []string{"balanced", "support_only"}
	for _, m := range modes {
		_, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
			TenantID:   "t",
			Query:      "q",
			StanceMode: m,
		})
		require.NoError(t, err)
	}
	requests := h.allRequests()
	require.Len(t, requests, len(modes))
	for i, m := range modes {
		var body map[string]any
		require.NoError(t, json.Unmarshal(requests[i].Body, &body))
		assert.Equal(t, m, body["stance_mode"])
	}
}

// TestRetrieveMultipleResults exercises parsing of multi-claim
// responses with mixed support/contradiction tallies — the
// differentiator for the retrieve endpoint.
func TestRetrieveMultipleResults(t *testing.T) {
	h := &recordingHandler{status: 200, body: map[string]any{
		"results": []map[string]any{
			{
				"claim_id":       "c-support",
				"canonical_text": "Acme grew 30% in Q3.",
				"score":          0.91,
				"supports":       5,
				"contradicts":    0,
				"citations":      []map[string]any{},
			},
			{
				"claim_id":       "c-contra",
				"canonical_text": "Acme actually shrank in Q3.",
				"score":          0.82,
				"supports":       1,
				"contradicts":    3,
				"citations":      []map[string]any{},
			},
		},
	}}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	resp, err := c.Retrieve().Query(context.Background(), RetrieveRequest{
		TenantID: "t",
		Query:    "acme q3",
	})
	require.NoError(t, err)
	require.Len(t, resp.Results, 2)
	assert.Equal(t, 0, resp.Results[0].Contradicts)
	assert.Equal(t, 5, resp.Results[0].Supports)
	assert.Equal(t, 3, resp.Results[1].Contradicts)
}
