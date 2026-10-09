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
// Environment: DASH_LIVE_URL (required), DASH_LIVE_RETRIEVAL_URL (defaults
// to DASH_LIVE_URL) and DASH_LIVE_INGESTION_URL (defaults to DASH_LIVE_URL).
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

func ingestionURL(t *testing.T) string {
	t.Helper()
	if v := os.Getenv("DASH_LIVE_INGESTION_URL"); v != "" {
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

func TestLive_Embed_Base64(t *testing.T) {
	url := retrievalURL(t)
	waitForHealth(t, url)
	client := New(url, WithAPIKey("not_needed"))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.Embeddings().Create(ctx, EmbeddingRequest{
		Model:          "text-embedding-3-small",
		Input:          "base64 test",
		EncodingFormat: "base64",
	})
	if err != nil {
		t.Fatalf("embed base64: %v", err)
	}
	if len(resp.Data) != 1 || len(resp.Data[0].Embedding) == 0 {
		t.Fatalf("expected one decoded float embedding, got %+v", resp.Data)
	}
}

func TestLive_RetrieveAfterDirectIngest(t *testing.T) {
	// The Go SDK does not expose an Ingest method, so this test
	// exercises Retrieve against a tenant we ingest into via the
	// native HTTP client. This is still an end-to-end test of the
	// SDK's contract: the response shape and the wire format must
	// match what production clients see.
	ingestURL := ingestionURL(t)
	retrieve := retrievalURL(t)
	waitForHealth(t, retrieve)

	tenantID := "test-tenant-" + strings.ReplaceAll(time.Now().Format("20060102T150405.000000"), ".", "")
	phrase := "distinctive phrase " + time.Now().Format("150405.000000000")

	// Claim ids are global across tenants, so keep them unique per run.
	claimID := "claim-" + tenantID
	ingestBody := `{"claim":{"claim_id":"` + claimID + `","tenant_id":"` + tenantID + `","canonical_text":"` + phrase + `","confidence":0.9},` +
		`"evidence":[{"evidence_id":"ev-` + tenantID + `","claim_id":"` + claimID + `","source_id":"test://integration","stance":"supports","source_quality":0.8}],"edges":[]}`
	ingestReq, _ := http.NewRequest("POST", strings.TrimRight(ingestURL, "/")+"/v1/ingest", strings.NewReader(ingestBody))
	ingestReq.Header.Set("Content-Type", "application/json")
	if key := os.Getenv("DASH_LIVE_INGEST_API_KEY"); key != "" {
		ingestReq.Header.Set("Authorization", "Bearer "+key)
	}
	ingestResp, err := http.DefaultClient.Do(ingestReq)
	if err != nil {
		t.Fatalf("ingest http: %v", err)
	}
	ingestStatus := ingestResp.StatusCode
	_ = ingestResp.Body.Close()
	if ingestStatus != http.StatusOK {
		t.Fatalf("ingest status: got %d want 200", ingestStatus)
	}

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

