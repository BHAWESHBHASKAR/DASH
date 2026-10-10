// Package transport is the low-level HTTP client used by the
// top-level dash.Client. It is intentionally small: encode a JSON
// body, send a POST, decode the response, and turn any failure into
// a typed dash.DashError.
package transport

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Config bundles the inputs a Transport needs. The zero value is not
// useful; the public API in client.go builds one from Client + Option.
type Config struct {
	BaseURL string
	APIKey  string
	Timeout time.Duration
	Client  *http.Client

	// Extra headers are merged into every request after the default
	// User-Agent and Accept headers. Useful for tracing IDs, custom
	// auth schemes, or feature flags.
	ExtraHeaders map[string]string
}

// DefaultUserAgent is the value sent in the User-Agent header when
// the caller has not supplied one of their own.
const DefaultUserAgent = "dash-go/0.2.0"

// Transport executes a single HTTP request and returns the raw
// response body. It is safe for concurrent use; the underlying
// http.Client is shared.
type Transport struct {
	cfg    Config
	client *http.Client
}

// New returns a Transport. The http.Client is created lazily from
// cfg.Timeout when cfg.Client is nil so callers can either trust
// the SDK's default timeouts or plug in their own.
func New(cfg Config) *Transport {
	t := &Transport{cfg: cfg}
	if cfg.Client != nil {
		t.client = cfg.Client
	} else {
		timeout := cfg.Timeout
		if timeout <= 0 {
			timeout = 30 * time.Second
		}
		t.client = &http.Client{Timeout: timeout}
	}
	return t
}

// Post issues a POST with a JSON body and returns the raw response.
// The status code and body are returned to the caller as separate
// values so the higher-level package can build a DashAPIError
// without re-reading the body.
//
// If the request fails before a response is received, err is a
// wrapped network error and status is 0.
func (t *Transport) Post(ctx context.Context, path string, body any) (status int, raw []byte, err error) {
	return t.Do(ctx, http.MethodPost, path, body)
}

// Delete issues a DELETE without a body (the delete routes take their
// arguments from the path and query) and returns the raw response.
func (t *Transport) Delete(ctx context.Context, path string) (status int, raw []byte, err error) {
	return t.Do(ctx, http.MethodDelete, path, nil)
}

// Do issues a request with an optional JSON body. A nil body sends no
// body and no Content-Type.
func (t *Transport) Do(ctx context.Context, method, path string, body any) (status int, raw []byte, err error) {
	url := t.cfg.BaseURL + path

	var payload []byte
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return 0, nil, fmt.Errorf("marshal request body: %w", err)
		}
	}

	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return 0, nil, fmt.Errorf("build request: %w", err)
	}
	if payload != nil || method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	if req.Header.Get("User-Agent") == "" {
		req.Header.Set("User-Agent", DefaultUserAgent)
	}
	if t.cfg.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+t.cfg.APIKey)
	}
	for k, v := range t.cfg.ExtraHeaders {
		// Don't let a user-supplied extra header clobber the auth
		// header that was set above; the explicit Authorization
		// value is the source of truth.
		if strings.EqualFold(k, "Authorization") {
			continue
		}
		req.Header.Set(k, v)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()

	raw, err = io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("read response body: %w", err)
	}
	return resp.StatusCode, raw, nil
}
