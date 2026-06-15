package dash

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDashAPIErrorImplementsDashError verifies that *DashAPIError
// satisfies the DashError interface so callers can rely on a single
// error-typed signature.
func TestDashAPIErrorImplementsDashError(t *testing.T) {
	var e DashError = &DashAPIError{
		StatusCodeValue: 401,
		ErrorTypeValue:  "invalid_request_error",
		BodyValue:       `{"error":{"message":"x"}}`,
		Message:         "x",
	}
	assert.Equal(t, 401, e.StatusCode())
	assert.Equal(t, "invalid_request_error", e.ErrorType())
	assert.Contains(t, e.Body(), "x")
	assert.NotEmpty(t, e.Error())
}

// TestDashConnectionErrorImplementsDashError verifies that
// *DashConnectionError satisfies the DashError interface and
// reports "connection_error" as the error type with status 0.
func TestDashConnectionErrorImplementsDashError(t *testing.T) {
	base := &net.OpError{Op: "dial", Err: errors.New("refused")}
	var e DashError = &DashConnectionError{Err: base}
	assert.Equal(t, 0, e.StatusCode())
	assert.Equal(t, "connection_error", e.ErrorType())
	assert.Contains(t, e.Body(), "refused")
	assert.NotEmpty(t, e.Error())
}

// TestDashConnectionErrorUnwrap verifies Unwrap returns the
// underlying network error so errors.Is/As compose.
func TestDashConnectionErrorUnwrap(t *testing.T) {
	base := errors.New("connection refused")
	connErr := &DashConnectionError{Err: base}
	assert.Same(t, base, connErr.Unwrap())
}

// TestErrorsAsExtractsDashAPIError verifies that errors.As finds a
// *DashAPIError wrapped in a fmt.Errorf("%w", ...) chain.
func TestErrorsAsExtractsDashAPIError(t *testing.T) {
	h := &recordingHandler{status: 401, body: openAIErrorBody}
	srv := newTestServer(t, h)

	c := New(srv.URL)
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var apiErr *DashAPIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, 401, apiErr.StatusCode())
}

// TestErrorsAsFromUnreachableServer verifies that errors.As finds a
// *DashConnectionError from a refused TCP connection.
func TestErrorsAsFromUnreachableServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	c := New("http://" + addr)
	_, err = c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.Error(t, err)

	var connErr *DashConnectionError
	require.True(t, errors.As(err, &connErr))
	assert.NotNil(t, connErr.Unwrap())
}

// TestErrorsAsNilLeavesTargetUntouched verifies that passing a nil
// error to errors.As does not panic and reports false.
func TestErrorsAsNilLeavesTargetUntouched(t *testing.T) {
	var apiErr *DashAPIError
	assert.False(t, errors.As(nil, &apiErr))
	assert.Nil(t, apiErr)
}

// TestErrorsAsFromPlainErrorIsFalse verifies that errors.As returns
// false for errors that do not match.
func TestErrorsAsFromPlainErrorIsFalse(t *testing.T) {
	err := errors.New("not a dash error")
	var apiErr *DashAPIError
	assert.False(t, errors.As(err, &apiErr))
}

// TestDashAPIErrorErrorStringFormat verifies the Error() method
// includes the status code and message so log lines are useful.
func TestDashAPIErrorErrorStringFormat(t *testing.T) {
	e := &DashAPIError{
		StatusCodeValue: 503,
		ErrorTypeValue:  "server_error",
		BodyValue:       `{"error":"boom"}`,
		Message:         "boom",
	}
	got := e.Error()
	assert.Contains(t, got, "503")
	assert.Contains(t, got, "server_error")
	assert.Contains(t, got, "boom")
}

// TestNewAPIErrorFromOpenAIShape verifies the OpenAI error envelope
// (the /v1/embeddings shape) is parsed into a DashAPIError.
func TestNewAPIErrorFromOpenAIShape(t *testing.T) {
	parsed := map[string]any{
		"error": map[string]any{
			"message": "input must contain at least one text",
			"type":    "invalid_request_error",
		},
	}
	e := newAPIError(400, `{"error":{"message":"x","type":"y"}}`, parsed)
	assert.Equal(t, 400, e.StatusCodeValue)
	assert.Equal(t, "invalid_request_error", e.ErrorTypeValue)
	assert.Equal(t, "input must contain at least one text", e.Message)
}

// TestNewAPIErrorFromRetrieveAdHocShape verifies the ad-hoc
// /v1/retrieve error shape is parsed.
func TestNewAPIErrorFromRetrieveAdHocShape(t *testing.T) {
	parsed := map[string]any{
		"error": "routing unavailable",
	}
	e := newAPIError(503, `{"error":"routing unavailable"}`, parsed)
	assert.Equal(t, 503, e.StatusCodeValue)
	assert.Equal(t, "api_error", e.ErrorTypeValue)
	assert.Equal(t, "routing unavailable", e.Message)
}

// TestNewAPIErrorFallbackOnUnparseable verifies that a non-JSON
// 5xx body uses the raw text as the message and "api_error" as the
// type.
func TestNewAPIErrorFallbackOnUnparseable(t *testing.T) {
	e := newAPIError(502, "Bad Gateway", nil)
	assert.Equal(t, "Bad Gateway", e.Message)
	assert.Equal(t, "api_error", e.ErrorTypeValue)
}

// TestNewAPIErrorFallbackOnEmptyBody verifies that an empty 4xx
// body synthesises an "HTTP <status>" message.
func TestNewAPIErrorFallbackOnEmptyBody(t *testing.T) {
	e := newAPIError(418, "", nil)
	assert.Equal(t, "HTTP 418", e.Message)
	assert.Equal(t, "api_error", e.ErrorTypeValue)
}

// TestNewAPIErrorPrefersTypeOverCode verifies that a structured
// error's "type" wins over its "code" when both are present.
func TestNewAPIErrorPrefersTypeOverCode(t *testing.T) {
	parsed := map[string]any{
		"error": map[string]any{
			"message": "x",
			"type":    "invalid_request_error",
			"code":    "should-be-ignored",
		},
	}
	e := newAPIError(400, "", parsed)
	assert.Equal(t, "invalid_request_error", e.ErrorTypeValue)
}

// TestNewAPIErrorFallsBackToCodeWhenTypeMissing verifies that the
// "code" field is used as the error type when "type" is absent.
func TestNewAPIErrorFallsBackToCodeWhenTypeMissing(t *testing.T) {
	parsed := map[string]any{
		"error": map[string]any{
			"message": "x",
			"code":    "no_healthy_node",
		},
	}
	e := newAPIError(503, "", parsed)
	assert.Equal(t, "no_healthy_node", e.ErrorTypeValue)
}

// TestNewAPIErrorTopLevelMessage verifies that a top-level "message"
// on the parsed body is used as a fallback when the "error" object
// is absent.
func TestNewAPIErrorTopLevelMessage(t *testing.T) {
	parsed := map[string]any{
		"message": "top-level message",
	}
	e := newAPIError(500, "", parsed)
	assert.Equal(t, "top-level message", e.Message)
}

// TestDashAPIErrorIsDashError verifies a *DashAPIError is a
// DashError, satisfying the same assertion users would do in their
// own code.
func TestDashAPIErrorIsDashError(t *testing.T) {
	var e error = &DashAPIError{
		StatusCodeValue: 400,
		ErrorTypeValue:  "invalid_request_error",
		Message:         "x",
	}
	var dashErr DashError
	require.True(t, errors.As(e, &dashErr))
	assert.Equal(t, 400, dashErr.StatusCode())
}

// TestDashConnectionErrorUnwrapRoundTrip verifies that errors.Is on
// the wrapped error finds the underlying sentinel.
func TestDashConnectionErrorUnwrapRoundTrip(t *testing.T) {
	sentinel := errors.New("dial tcp: connection refused")
	connErr := &DashConnectionError{Err: sentinel}
	require.True(t, errors.Is(connErr, sentinel))
}

// TestDashErrorInterfaceIsSatisfiedByBothTypes is a compile-time
// guard: if either *DashAPIError or *DashConnectionError stops
// implementing DashError, this test file fails to build.
func TestDashErrorInterfaceIsSatisfiedByBothTypes(t *testing.T) {
	var _ DashError = (*DashAPIError)(nil)
	var _ DashError = (*DashConnectionError)(nil)
}

// TestDashAPIErrorStringContainsStatusAndType is a redundant
// friendly check that the format string is stable enough for
// log-scrubbing regexes.
func TestDashAPIErrorStringContainsStatusAndType(t *testing.T) {
	e := &DashAPIError{StatusCodeValue: 401, ErrorTypeValue: "invalid_request_error", Message: "x"}
	assert.Contains(t, fmt.Sprint(e), "401")
	assert.Contains(t, fmt.Sprint(e), "invalid_request_error")
}

// TestDashConnectionErrorEmptyUnderlying verifies that a
// DashConnectionError with a nil underlying error still produces a
// stable string.
func TestDashConnectionErrorEmptyUnderlying(t *testing.T) {
	e := &DashConnectionError{}
	assert.NotPanics(t, func() { _ = e.Error() })
}

// TestDashErrorStatusCheckOnUnknownStatus verifies the StatusCode
// is preserved as-is even on unusual codes (e.g. 0, 418).
func TestDashErrorStatusCheckOnUnknownStatus(t *testing.T) {
	e := &DashAPIError{StatusCodeValue: 0, ErrorTypeValue: "api_error", Message: "x"}
	assert.Equal(t, 0, e.StatusCode())
	e.StatusCodeValue = 418
	assert.Equal(t, 418, e.StatusCode())
}

// TestDashConnectionErrorIsNotDashAPIError verifies the two error
// types remain distinct.
func TestDashConnectionErrorIsNotDashAPIError(t *testing.T) {
	connErr := &DashConnectionError{Err: errors.New("boom")}
	var apiErr *DashAPIError
	assert.False(t, errors.As(connErr, &apiErr))
}

// TestDashAPIErrorWrappedTwiceIsFound verifies errors.As walks a
// chain of fmt.Errorf("%w", ...) wraps.
func TestDashAPIErrorWrappedTwiceIsFound(t *testing.T) {
	base := &DashAPIError{StatusCodeValue: 500, ErrorTypeValue: "server_error", Message: "boom"}
	wrapped := fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", base))
	var apiErr *DashAPIError
	require.True(t, errors.As(wrapped, &apiErr))
	assert.Equal(t, 500, apiErr.StatusCode())
}

// roundTripIs used to confirm the SDK's net error wrapping
// interacts sensibly with the stdlib's error helpers.
type roundTripIs struct{}

func (roundTripIs) RoundTrip(r *http.Request) (*http.Response, error) {
	_ = r
	return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
}

// TestDashConnectionErrorWrappingNetOpError verifies a net.OpError
// is reachable via errors.As on the returned DashConnectionError.
func TestDashConnectionErrorWrappingNetOpError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("ok"))
	}))
	srv.Close()
	// srv is closed; any request should now fail. Use a transport
	// that returns a net.OpError to make the assertion deterministic.
	c := New("http://127.0.0.1:1", WithHTTPClient(&http.Client{
		Transport: roundTripIs{},
	}))
	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "x"})
	require.Error(t, err)
	var opErr *net.OpError
	assert.True(t, errors.As(err, &opErr))
}
