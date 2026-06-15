package transport

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPostSetsContentTypeAndAccept verifies the request carries
// Content-Type, Accept, and a non-empty User-Agent.
func TestPostSetsContentTypeAndAccept(t *testing.T) {
	var seen http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()

	tr := New(Config{BaseURL: srv.URL})
	status, body, err := tr.Post(context.Background(), "/v1/ping", map[string]any{"x": 1})
	require.NoError(t, err)
	assert.Equal(t, 200, status)
	assert.JSONEq(t, `{"ok":true}`, string(body))
	assert.Equal(t, "application/json", seen.Get("Content-Type"))
	assert.Equal(t, "application/json", seen.Get("Accept"))
	assert.NotEmpty(t, seen.Get("User-Agent"))
}

// TestPostSetsAuthorizationWhenAPIKey verifies the bearer token
// is forwarded.
func TestPostSetsAuthorizationWhenAPIKey(t *testing.T) {
	var seen http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	tr := New(Config{BaseURL: srv.URL, APIKey: "sk-abc"})
	_, _, err := tr.Post(context.Background(), "/v1/ping", nil)
	require.NoError(t, err)
	assert.Equal(t, "Bearer sk-abc", seen.Get("Authorization"))
}

// TestPostMergesExtraHeaders verifies user-supplied headers are
// appended to the request.
func TestPostMergesExtraHeaders(t *testing.T) {
	var seen http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	tr := New(Config{BaseURL: srv.URL, ExtraHeaders: map[string]string{
		"X-Trace-Id": "abc-123",
	}})
	_, _, err := tr.Post(context.Background(), "/v1/ping", nil)
	require.NoError(t, err)
	assert.Equal(t, "abc-123", seen.Get("X-Trace-Id"))
}

// TestPostExtraAuthorizationHeaderIsIgnored verifies that an
// Authorization header in ExtraHeaders cannot clobber the API-key
// value the SDK manages.
func TestPostExtraAuthorizationHeaderIsIgnored(t *testing.T) {
	var seen http.Header
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = r.Header.Clone()
		w.WriteHeader(200)
	}))
	defer srv.Close()

	tr := New(Config{
		BaseURL: srv.URL,
		APIKey:  "real-key",
		ExtraHeaders: map[string]string{
			"Authorization": "Bearer attacker-key",
		},
	})
	_, _, err := tr.Post(context.Background(), "/v1/ping", nil)
	require.NoError(t, err)
	assert.Equal(t, "Bearer real-key", seen.Get("Authorization"))
}

// TestPostTimeoutWhenServerSlow verifies a slow server trips the
// client timeout and surfaces an error.
func TestPostTimeoutWhenServerSlow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	tr := New(Config{BaseURL: srv.URL, Timeout: 50 * time.Millisecond})
	_, _, err := tr.Post(context.Background(), "/v1/ping", nil)
	require.Error(t, err)
}

// TestPostEmptyBody verifies a nil body issues a POST with an
// empty request body and a Content-Length of 0.
func TestPostEmptyBody(t *testing.T) {
	var seenContentLength int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenContentLength = r.ContentLength
		// Drain the body so the connection can be reused.
		_, _ = io.Copy(io.Discard, r.Body)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	tr := New(Config{BaseURL: srv.URL})
	_, _, err := tr.Post(context.Background(), "/v1/ping", nil)
	require.NoError(t, err)
	assert.EqualValues(t, 0, seenContentLength)
}

// TestPostNonObjectBody verifies the body is passed through
// verbatim for non-object payloads.
func TestPostNonObjectBody(t *testing.T) {
	var got string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		buf, _ := io.ReadAll(r.Body)
		got = string(buf)
		w.WriteHeader(200)
	}))
	defer srv.Close()

	tr := New(Config{BaseURL: srv.URL})
	_, _, err := tr.Post(context.Background(), "/v1/ping", "raw-text")
	require.NoError(t, err)
	// The body is JSON-encoded; a raw string becomes a quoted
	// JSON string.
	assert.Equal(t, `"raw-text"`, got)
}

// TestPostCustomHTTPClient verifies a custom http.Client is used
// when supplied.
func TestPostCustomHTTPClient(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	}))
	defer srv.Close()

	custom := &http.Client{Timeout: 2 * time.Second}
	tr := New(Config{BaseURL: srv.URL, Client: custom})
	_, body, err := tr.Post(context.Background(), "/v1/ping", nil)
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(body), "ok"))
}

// TestPostContextCancellation verifies a cancelled context aborts
// the request.
func TestPostContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer srv.Close()

	tr := New(Config{BaseURL: srv.URL})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, _, err := tr.Post(ctx, "/v1/ping", nil)
	require.Error(t, err)
}

// TestPostInvalidJSONReturnsError verifies a body that fails to
// marshal returns a clear error without a wire call.
func TestPostInvalidJSONReturnsError(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.WriteHeader(200)
	}))
	defer srv.Close()

	tr := New(Config{BaseURL: srv.URL})
	// Channels don't marshal to JSON.
	_, _, err := tr.Post(context.Background(), "/v1/ping", make(chan int))
	require.Error(t, err)
	assert.Equal(t, 0, hits)
}
