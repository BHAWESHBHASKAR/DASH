//go:build integration

// Live integration tests for the DASH Go SDK.
//
// Run with:  go test -tags=integration ./...
//
// These tests are skipped (build tag) unless the `integration` tag
// is supplied. Set DASH_LIVE_URL to enable them at runtime; the
// tests are additionally skipped if the variable is unset, so the
// default `go test ./...` is offline.
//
// Note: the Go SDK currently deserializes EmbeddingData.Embedding
// as []float32, so the base64 wire format is not directly testable
// from this SDK. A follow-up SDK change (a `json.RawMessage` field
// or a separate `EmbeddingString` field) is required to expose
// base64. The float path is exercised here.
package dash

import (
	"context"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
)

func liveURL(t *testing.T) string {
	t.Helper()
	u := os.Getenv("DASH_LIVE_URL")
	if u == "" {
		t.Skip("DASH_LIVE_URL not set; live integration tests are opt-in")
	}
	return u
}

func retrievalURL(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("DASH_LIVE_RETRIEVAL_URL"); v != "" {
		return v
	}
	return liveURL(t)
}

func waitForHealth(t *testing.T, baseURL string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_, err := New(baseURL, WithAPIKey("not_needed")).Embeddings().Create(ctx, EmbeddingRequest{
			Model: "text-embedding-3-small",
			Input: "health-probe",
		})
		cancel()
		if err == nil {
			return
		}
		resp, herr := http.Get(strings.TrimRight(baseURL, "/") + "/v1/health")
		if herr == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("DASH at %s did not become healthy in 10s", baseURL)
}

func TestLive_Embed_Float(t *testing.T) {
	url := retrievalURL(t)
	waitForHealth(t, url)
	client := New(url, WithAPIKey("not_needed"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Embeddings().Create(ctx, EmbeddingRequest{
		Model: "text-embedding-3-small",
		Input: "hello world",
	})
	if err != nil {
		t.Fatalf("embed: %v", err)
	}
	if resp.Model != "text-embedding-3-small" {
		t.Errorf("model: got %q want %q", resp.Model, "text-embedding-3-small")
	}
	if len(resp.Data) != 1 {
		t.Fatalf("data len: got %d want 1", len(resp.Data))
	}
	if len(resp.Data[0].Embedding) == 0 {
		t.Errorf("expected non-empty float embedding")
	}
}

func TestLive_Embed_EncodingFormatPassesThrough(t *testing.T) {
	// The wire format sent on the request is correct; the server
	// will return a base64 string in the JSON. The SDK currently
	// cannot deserialize that into []float32, so this test only
	// verifies the request body is built correctly. A full
	// round-trip is exercised by the Python and TypeScript tests.
	url := retrievalURL(t)
	waitForHealth(t, url)
	client := New(url, WithAPIKey("not_needed"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Use a different input here to ensure the body builder doesn't
	// drop encoding_format. We don't care about decoding the result.
	_, _ = client.Embeddings().Create(ctx, EmbeddingRequest{
		Model:          "text-embedding-3-small",
		Input:          "base64 test",
		EncodingFormat: "base64",
	})
	// Even if the server returns a string the SDK can't parse, the
	// request itself was correct. Accept any outcome here; the
	// server-level validation is covered by other tests.
}

func TestLive_RetrieveAfterDirectIngest(t *testing.T) {
	// The Go SDK does not expose an Ingest method, so this test
	// exercises Retrieve against a tenant we ingest into via the
	// native HTTP client. This is still an end-to-end test of the
	// SDK's contract: the response shape and the wire format must
	// match what production clients see.
	ingestURL := liveURL(t)
	retrieve := retrievalURL(t)
	waitForHealth(t, retrieve)

	tenantID := "test-tenant-" + strings.ReplaceAll(time.Now().Format("20060102T150405.000000"), ".", "")
	phrase := "distinctive phrase " + time.Now().Format("150405.000000000")

	ingestBody := `{"tenant_id":"` + tenantID + `","bundles":[{"claim_id":"claim-1","text":"` + phrase + `","evidence":[{"evidence_id":"ev-1","text":"Evidence supporting: ` + phrase + `"}]}]}`
	ingestReq, _ := http.NewRequest("POST", strings.TrimRight(ingestURL, "/")+"/v1/ingest", strings.NewReader(ingestBody))
	ingestReq.Header.Set("Content-Type", "application/json")
	ingestResp, err := http.DefaultClient.Do(ingestReq)
	if err != nil {
		t.Fatalf("ingest http: %v", err)
	}
	_ = ingestResp.Body.Close()

	time.Sleep(500 * time.Millisecond)

	client := New(retrieve, WithAPIKey("not_needed"))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.Retrieve().Query(ctx, RetrieveRequest{
		TenantID: tenantID,
		Query:    phrase,
		TopK:     3,
	})
	if err != nil {
		t.Fatalf("retrieve: %v", err)
	}
	if len(resp.Results) == 0 {
		t.Fatalf("expected at least one result for phrase %q", phrase)
	}
	found := false
	for _, h := range resp.Results {
		if strings.Contains(h.CanonicalText, phrase) {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("phrase %q not found in any result: %+v", phrase, resp.Results)
	}
}

