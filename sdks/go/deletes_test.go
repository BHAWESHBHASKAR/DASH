package dash

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deleteCall struct {
	Method     string
	RequestURI string
	Body       string
	Header     http.Header
}

func deleteServer(t *testing.T, status int, body string) (*httptest.Server, func() []deleteCall) {
	t.Helper()
	var mu sync.Mutex
	var calls []deleteCall
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		calls = append(calls, deleteCall{r.Method, r.RequestURI, string(raw), r.Header.Clone()})
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []deleteCall {
		mu.Lock()
		defer mu.Unlock()
		return append([]deleteCall(nil), calls...)
	}
}

const claimDeleted = `{"deleted":true,"scope":"claim","tenant_id":"tenant a","claim_id":"c/1","claims_deleted":1,"evidence_deleted":2,"edges_deleted":1,"vectors_deleted":1,"claims_total":41,"checkpoint_triggered":false,"checkpoint_deferred":false}`

func TestDeriveIngestionURL(t *testing.T) {
	assert.Equal(t, "http://localhost:8081", deriveIngestionURL("http://localhost:8080"))
	assert.Equal(t, "https://dash.local:8081/base", deriveIngestionURL("https://dash.local:8080/base"))
	assert.Equal(t, "http://[::1]:8081", deriveIngestionURL("http://[::1]:8080"))
	assert.Equal(t, "", deriveIngestionURL("https://dash.example.com"))
	assert.Equal(t, "", deriveIngestionURL("http://localhost:9000"))
	assert.Equal(t, "http://localhost:8081", New("http://localhost:8080").IngestionBaseURL())
}

func TestDeleteClaimSendsDeleteToTheIngestionService(t *testing.T) {
	srv, calls := deleteServer(t, 200, claimDeleted)
	c := New("http://retrieval.invalid", WithIngestionBaseURL(srv.URL+"/"), WithAPIKey("k"))
	resp, err := c.DeleteClaim(context.Background(), "tenant a", "c/1")
	require.NoError(t, err)
	got := calls()
	require.Len(t, got, 1)
	assert.Equal(t, http.MethodDelete, got[0].Method)
	assert.Equal(t, "/v1/claims/c%2F1?tenant_id=tenant+a", got[0].RequestURI)
	assert.Empty(t, got[0].Body)
	assert.Empty(t, got[0].Header.Get("Content-Type"))
	assert.Equal(t, "Bearer k", got[0].Header.Get("Authorization"))
	assert.True(t, resp.Deleted)
	assert.Equal(t, "claim", resp.Scope)
	assert.Equal(t, 2, resp.EvidenceDeleted)
	assert.Equal(t, "c/1", resp.ClaimID)
	assert.Equal(t, 41, resp.ClaimsTotal)
}

func TestDeleteEvidenceAndTenantRoutes(t *testing.T) {
	srv, calls := deleteServer(t, 200, `{"deleted":false,"scope":"evidence","tenant_id":"t","evidence_id":"e 1"}`)
	c := New("http://retrieval.invalid", WithIngestionBaseURL(srv.URL))
	resp, err := c.DeleteEvidence(context.Background(), "t", "e 1")
	require.NoError(t, err)
	assert.False(t, resp.Deleted)
	assert.Equal(t, 0, resp.ClaimsDeleted)
	_, err = c.DeleteTenant(context.Background(), "t")
	require.NoError(t, err)
	got := calls()
	require.Len(t, got, 2)
	assert.Equal(t, "/v1/evidence/e%201?tenant_id=t", got[0].RequestURI)
	assert.Equal(t, "/v1/tenants/t", got[1].RequestURI)
}

func TestDeleteWithoutIngestionURLFailsBeforeAnyRequest(t *testing.T) {
	c := New("https://dash.example.com")
	_, err := c.DeleteTenant(context.Background(), "t")
	assert.True(t, errors.Is(err, ErrNoIngestionURL), "%v", err)
}

func TestDeleteRejectsEmptyIdentifiers(t *testing.T) {
	srv, calls := deleteServer(t, 200, claimDeleted)
	c := New("http://retrieval.invalid", WithIngestionBaseURL(srv.URL))
	ctx := context.Background()
	_, err := c.DeleteClaim(ctx, "", "c")
	assert.ErrorContains(t, err, "tenantID")
	_, err = c.DeleteClaim(ctx, "t", " ")
	assert.ErrorContains(t, err, "claimID")
	_, err = c.DeleteEvidence(ctx, "t", "")
	assert.ErrorContains(t, err, "evidenceID")
	_, err = c.DeleteTenant(ctx, "")
	assert.ErrorContains(t, err, "tenantID")
	assert.Empty(t, calls())
}

func TestDeleteErrorsAreAPIErrors(t *testing.T) {
	srv, _ := deleteServer(t, 403, `{"error":"missing required role: admin"}`)
	c := New("http://retrieval.invalid", WithIngestionBaseURL(srv.URL))
	_, err := c.DeleteTenant(context.Background(), "t")
	var apiErr *DashAPIError
	require.True(t, errors.As(err, &apiErr), "%v", err)
	assert.Equal(t, 403, apiErr.StatusCode())
}
