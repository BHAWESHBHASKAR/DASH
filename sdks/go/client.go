package dash

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/anomalyco/dash-go/internal/transport"
)

// Version is the SDK version. It is included in the default
// User-Agent header on every request.
const Version = "0.1.0"

// DefaultTimeout is applied to the underlying http.Client when the
// caller does not supply one of their own. 30s matches the Python
// SDK default.
const DefaultTimeout = 30 * time.Second

// Client is the top-level DASH client. It is cheap to construct and
// safe for concurrent use; the underlying http.Client pools
// connections.
type Client struct {
	baseURL      string
	apiKey       string
	timeout      time.Duration
	httpClient   *http.Client
	extraHeaders map[string]string

	t *transport.Transport

	embeddings *EmbeddingsService
	retrieve   *RetrieveService
}

// Option mutates a Client during construction. The functional-option
// pattern keeps New's signature stable as we add knobs.
type Option func(*Client)

// WithAPIKey sets the bearer token used for Authorization. When
// non-empty, every request carries
// "Authorization: Bearer <key>".
func WithAPIKey(key string) Option {
	return func(c *Client) { c.apiKey = key }
}

// WithHTTPClient replaces the underlying *http.Client. Use this to
// inject a custom Transport (for example, for mTLS, or a recording
// transport in tests) or to share a single client across many SDK
// instances.
func WithHTTPClient(h *http.Client) Option {
	return func(c *Client) { c.httpClient = h }
}

// WithTimeout sets the per-request timeout used when no custom
// *http.Client is provided. A value <= 0 is ignored.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithHeader adds an extra header to every request. Multiple calls
// for the same key keep the last value. A User-Agent header set via
// WithHeader is forwarded to the wire and not overridden by the
// SDK's default.
func WithHeader(key, value string) Option {
	return func(c *Client) {
		if c.extraHeaders == nil {
			c.extraHeaders = map[string]string{}
		}
		c.extraHeaders[key] = value
	}
}

// New constructs a Client. The baseURL must be a non-empty absolute
// URL with no trailing slash (it is normalised for callers that
// pass one anyway).
func New(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		timeout:  DefaultTimeout,
	}
	for _, opt := range opts {
		opt(c)
	}
	c.t = transport.New(transport.Config{
		BaseURL:      c.baseURL,
		APIKey:       c.apiKey,
		Timeout:      c.timeout,
		Client:       c.httpClient,
		ExtraHeaders: c.extraHeaders,
	})
	c.embeddings = &EmbeddingsService{client: c}
	c.retrieve = &RetrieveService{client: c}
	return c
}

// BaseURL returns the (slash-trimmed) base URL the client was
// constructed with.
func (c *Client) BaseURL() string { return c.baseURL }

// Close releases the underlying http.Client. The standard
// *http.Client has no Close, so this is currently a no-op kept for
// parity with the Python SDK's context manager. It is safe to call
// multiple times.
func (c *Client) Close() error { return nil }

// Embeddings returns the embeddings service, used to call
// /v1/embeddings. The same instance is returned on every call.
func (c *Client) Embeddings() *EmbeddingsService { return c.embeddings }

// Retrieve returns the native retrieval service, used to call
// /v1/retrieve. The same instance is returned on every call.
func (c *Client) Retrieve() *RetrieveService { return c.retrieve }

// post is the single low-level entry point every service uses to
// talk to the wire. It centralises error translation so the
// service methods stay focused on the domain.
func (c *Client) post(ctx context.Context, path string, req any, out any) error {
	status, raw, err := c.t.Post(ctx, path, req)
	if err != nil {
		return &DashConnectionError{Err: err}
	}
	rawText, parsed := decodeBody(raw)
	if status < 200 || status >= 300 {
		return newAPIError(status, rawText, parsed)
	}
	if out == nil {
		return nil
	}
	if err := decodeInto(parsed, raw, out); err != nil {
		return fmt.Errorf("decode %s response: %w", path, err)
	}
	return nil
}

// errInvalidInput is returned by the SDK itself when an argument
// combination is invalid (for example, an Input that is neither a
// string nor a []string). It is not a DashError because it is a
// programming error, not a wire-level failure; callers receive a
// plain error.
func errInvalidInput(format string, args ...any) error {
	return fmt.Errorf("dash: "+format, args...)
}
