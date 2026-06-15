// Package dash provides an idiomatic Go client for the DASH retrieval
// engine. DASH exposes an OpenAI-compatible /v1/embeddings endpoint and
// a native /v1/retrieve endpoint that returns structured Claim +
// Evidence + Contradiction results. This package wraps both in a
// small, typed surface that mirrors the official openai-go client
// where the wire format is identical and adds first-class support
// for the retrieval endpoint.
package dash

import (
	"encoding/json"
	"fmt"
)

// DashError is the interface implemented by every error this package
// can return. Callers can rely on errors.As to extract the concrete
// type while still inspecting status code, error type, and body.
type DashError interface {
	error
	StatusCode() int
	ErrorType() string
	Body() string
}

// DashConnectionError indicates the client could not reach DASH at
// all: DNS failure, connection refused, read timeout, TLS error. The
// underlying network error is exposed via Unwrap so callers can use
// errors.Is/As to inspect it (for example to check
// net.Error.Timeout()).
type DashConnectionError struct {
	Err error
}

func (e *DashConnectionError) Error() string {
	if e == nil || e.Err == nil {
		return "dash: connection error"
	}
	return fmt.Sprintf("dash: connection error: %s", e.Err.Error())
}

func (e *DashConnectionError) StatusCode() int { return 0 }

func (e *DashConnectionError) ErrorType() string { return "connection_error" }

func (e *DashConnectionError) Body() string {
	if e == nil || e.Err == nil {
		return ""
	}
	return e.Err.Error()
}

func (e *DashConnectionError) Unwrap() error { return e.Err }

// DashAPIError is returned when DASH replies with a non-2xx HTTP
// status. It carries the wire status code, the OpenAI-style "error
// type" string (for example "invalid_request_error" or
// "server_error"), and the raw response body so callers can log or
// re-parse it.
type DashAPIError struct {
	StatusCodeValue int
	ErrorTypeValue  string
	BodyValue       string
	Message         string
}

func (e *DashAPIError) Error() string {
	if e == nil {
		return "dash: api error"
	}
	return fmt.Sprintf("dash api error (%d %s): %s", e.StatusCodeValue, e.ErrorTypeValue, e.Message)
}

func (e *DashAPIError) StatusCode() int { return e.StatusCodeValue }

func (e *DashAPIError) ErrorType() string { return e.ErrorTypeValue }

func (e *DashAPIError) Body() string { return e.BodyValue }

// newAPIError builds a DashAPIError by inspecting a parsed JSON body
// and the raw text. The body may be nil if the server returned a
// non-JSON payload.
//
// /v1/embeddings emits the OpenAI-shaped envelope
// {"error": {"message": "...", "type": "invalid_request_error", ...}}.
// /v1/retrieve uses an ad-hoc shape ({"error": "...", "code": "..."}
// or {"message": "..."}). We tolerate both forms and fall back to
// "api_error" with the raw body or "HTTP <status>" as the message
// when nothing parseable is present.
func newAPIError(statusCode int, rawBody string, parsed any) *DashAPIError {
	errorType := "api_error"
	var errorMessage string

	if m, ok := parsed.(map[string]any); ok {
		if errObj, ok := m["error"]; ok {
			switch e := errObj.(type) {
			case map[string]any:
				if t, ok := e["type"].(string); ok && t != "" {
					errorType = t
				} else if c, ok := e["code"].(string); ok && c != "" {
					errorType = c
				}
				if msg, ok := e["message"].(string); ok {
					errorMessage = msg
				}
			case string:
				if e != "" {
					errorMessage = e
				}
			}
		}
		if errorMessage == "" {
			if msg, ok := m["message"].(string); ok {
				errorMessage = msg
			}
		}
	}

	if errorMessage == "" {
		if rawBody != "" {
			errorMessage = rawBody
		} else {
			errorMessage = fmt.Sprintf("HTTP %d", statusCode)
		}
	}

	return &DashAPIError{
		StatusCodeValue: statusCode,
		ErrorTypeValue:  errorType,
		BodyValue:       rawBody,
		Message:         errorMessage,
	}
}

// decodeBody attempts to decode the response body as JSON. It returns
// the parsed value and the raw text. Non-JSON bodies are tolerated
// and surfaced to callers as a DashAPIError with the raw text.
func decodeBody(raw []byte) (string, any) {
	if len(raw) == 0 {
		return "", nil
	}
	text := string(raw)
	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return text, nil
	}
	return text, parsed
}

// decodeInto decodes a JSON value into out. When parsed is nil and
// raw is non-empty we re-parse the raw bytes; this handles the case
// where the body is a valid JSON document that decodeBody was unable
// to type for some reason.
func decodeInto(parsed any, raw []byte, out any) error {
	if parsed != nil {
		// Re-marshal + decode to drive the strong typing in out
		// without losing information: encoding/json handles maps
		// with non-string keys, numbers outside float64, etc. by
		// re-emitting a canonical form. For the shapes this SDK
		// touches (string-keyed objects, numeric arrays) the
		// re-emission is lossless.
		buf, err := json.Marshal(parsed)
		if err != nil {
			return err
		}
		return json.Unmarshal(buf, out)
	}
	if len(raw) == 0 {
		return fmt.Errorf("empty response body")
	}
	return json.Unmarshal(raw, out)
}
