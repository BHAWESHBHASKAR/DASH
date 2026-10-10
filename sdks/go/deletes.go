package dash

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// DeleteResponse is the body of DELETE /v1/claims/{id},
// DELETE /v1/evidence/{id} and DELETE /v1/tenants/{id}. Deletes are
// idempotent: Deleted is false (with HTTP 200) when the target did not
// exist, including a claim that belongs to another tenant.
type DeleteResponse struct {
	Deleted             bool   `json:"deleted"`
	Scope               string `json:"scope"`
	TenantID            string `json:"tenant_id"`
	ClaimID             string `json:"claim_id,omitempty"`
	EvidenceID          string `json:"evidence_id,omitempty"`
	ClaimsDeleted       int    `json:"claims_deleted"`
	EvidenceDeleted     int    `json:"evidence_deleted"`
	EdgesDeleted        int    `json:"edges_deleted"`
	VectorsDeleted      int    `json:"vectors_deleted"`
	ClaimsTotal         int    `json:"claims_total"`
	CheckpointTriggered bool   `json:"checkpoint_triggered"`
	CheckpointDeferred  bool   `json:"checkpoint_deferred"`
}

// ErrNoIngestionURL is returned by the delete methods when the client
// has no ingestion service URL (see WithIngestionBaseURL).
var ErrNoIngestionURL = errors.New(
	"dash: the delete methods call the ingestion service: set WithIngestionBaseURL " +
		"(it is only derived automatically when the base URL uses port 8080)")

// DeleteClaim calls DELETE /v1/claims/{claimID}?tenant_id=...: it removes
// the claim with its vector, evidence and every edge from or to it. The
// credential needs the ingest role.
func (c *Client) DeleteClaim(ctx context.Context, tenantID, claimID string) (*DeleteResponse, error) {
	if err := requireIDs("tenantID", tenantID, "claimID", claimID); err != nil {
		return nil, err
	}
	return c.delete(ctx, "/v1/claims/"+url.PathEscape(claimID)+"?tenant_id="+url.QueryEscape(tenantID))
}

// DeleteEvidence calls DELETE /v1/evidence/{evidenceID}?tenant_id=...: it
// removes every evidence row with this id on the tenant's claims. The
// credential needs the ingest role.
func (c *Client) DeleteEvidence(ctx context.Context, tenantID, evidenceID string) (*DeleteResponse, error) {
	if err := requireIDs("tenantID", tenantID, "evidenceID", evidenceID); err != nil {
		return nil, err
	}
	return c.delete(ctx, "/v1/evidence/"+url.PathEscape(evidenceID)+"?tenant_id="+url.QueryEscape(tenantID))
}

// DeleteTenant calls DELETE /v1/tenants/{tenantID}: it erases all of the
// tenant's data. The credential needs the admin role for that tenant.
func (c *Client) DeleteTenant(ctx context.Context, tenantID string) (*DeleteResponse, error) {
	if err := requireIDs("tenantID", tenantID); err != nil {
		return nil, err
	}
	return c.delete(ctx, "/v1/tenants/"+url.PathEscape(tenantID))
}

func requireIDs(pairs ...string) error {
	for i := 0; i+1 < len(pairs); i += 2 {
		if strings.TrimSpace(pairs[i+1]) == "" {
			return errInvalidInput("%s must not be empty", pairs[i])
		}
	}
	return nil
}

func (c *Client) delete(ctx context.Context, path string) (*DeleteResponse, error) {
	if c.ingest == nil {
		return nil, ErrNoIngestionURL
	}
	status, raw, err := c.ingest.Delete(ctx, path)
	if err != nil {
		return nil, &DashConnectionError{Err: err}
	}
	rawText, parsed := decodeBody(raw)
	if status < 200 || status >= 300 {
		return nil, newAPIError(status, rawText, parsed)
	}
	var out DeleteResponse
	if err := decodeInto(parsed, raw, &out); err != nil {
		return nil, fmt.Errorf("decode delete response: %w", err)
	}
	return &out, nil
}

// deriveIngestionURL maps a base URL on port 8080 to the same host on
// port 8081; any other URL yields "".
func deriveIngestionURL(base string) string {
	u, err := url.Parse(base)
	if err != nil || u.Port() != "8080" || u.Hostname() == "" {
		return ""
	}
	host := u.Hostname()
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	u.Host = host + ":8081"
	u.RawQuery = ""
	u.Fragment = ""
	return strings.TrimRight(u.String(), "/")
}
