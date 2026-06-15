# dash-go

An idiomatic Go client for the [DASH](https://github.com/dash-retrieval/dash)
retrieval engine. DASH serves an OpenAI-compatible `/v1/embeddings`
endpoint and a native `/v1/retrieve` endpoint that returns structured
**Claim + Evidence + Contradiction** results — the differentiator that
makes it a real retrieval engine, not a vector store.

```bash
go get github.com/anomalyco/dash-go
```

## 5-minute quickstart

### 1. Embeddings (OpenAI-compatible)

```go
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/anomalyco/dash-go"
)

func main() {
	client := dash.New("http://localhost:8080")

	resp, err := client.Embeddings().Create(context.Background(), dash.EmbeddingRequest{
		Input: "hello world",
		Model: "text-embedding-3-small",
	})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(resp.Data[0].Embedding[:5])
	// [0.013 -0.042 0.077 0 0.5]
}
```

That's it — the same shape as
`openai.NewClient(...).Embeddings.New(...)`. DASH's `/v1/embeddings`
is byte-for-byte compatible with the OpenAI spec, so any tool that
speaks the OpenAI protocol works.

### 2. Drop-in OpenAI compatibility (zero code change)

If you already use the official
[`openai-go`](https://github.com/openai/openai-go) SDK, you can point
it at DASH with `dash.NewOpenAICompatibleConfig`:

```go
import (
	openai "github.com/openai/openai-go"
	"github.com/anomalyco/dash-go"
)

cfg := dash.NewOpenAICompatibleConfig("http://localhost:8080", "not-required-for-local")
client := openai.NewClient(openai.ClientConfig{
	BaseURL: cfg.BaseURL, // <dash>/v1
	APIKey:  cfg.APIKey,
})
// client.Embeddings.New(ctx, ...) now talks to DASH.
```

`cfg.BaseURL` is `<dash>/v1`; the openai-go SDK appends `embeddings`
to it automatically, so the wire path is the same one dash-go
hits internally.

### 3. Native retrieve — the Claim + Evidence + Contradiction RAG example

Where DASH pulls ahead of a vector store is the retrieval endpoint,
which returns **Claims** with explicit `supports` and `contradicts`
tallies and a structured `Citation` list per claim. That's the
substance you actually need to build a RAG pipeline that doesn't
confidently echo back the wrong thing.

```go
client := dash.New("http://localhost:8080")

// Balanced mode (default) — include all claims.
resp, err := client.Retrieve().Query(ctx, dash.RetrieveRequest{
	TenantID:   "acme-corp",
	Query:      "what was the Q3 2024 revenue?",
	TopK:       5,
	StanceMode: "balanced",
})
if err != nil {
	log.Fatal(err)
}

// A RAG pipeline that wants "claims that are actually supported and
// not contradicted" can filter the typed results directly.
cleanClaims := make([]dash.RetrieveResult, 0)
for _, r := range resp.Results {
	if r.Contradicts == 0 && r.Supports > 0 {
		cleanClaims = append(cleanClaims, r)
	}
}

for _, claim := range resp.Results {
	fmt.Printf("[%.2f] %s\n", claim.Score, claim.CanonicalText)
	fmt.Printf("   supports=%d  contradicts=%d\n", claim.Supports, claim.Contradicts)
	for _, cite := range claim.Citations {
		fmt.Printf("   - %-11s %s (q=%.2f)\n", cite.Stance, cite.SourceID, cite.SourceQuality)
	}
}
```

Use `StanceMode: "support_only"` to ask the server to filter out
claims whose contradiction tally exceeds their support tally before
they reach your prompt.

### 4. Construction options

```go
client := dash.New("http://localhost:8080",
	dash.WithAPIKey("sk-live-..."),     // sets Authorization: Bearer <key>
	dash.WithTimeout(10*time.Second),   // per-request timeout
	dash.WithHeader("X-Trace-Id", id),  // extra header on every request
	dash.WithHTTPClient(customClient),  // share a *http.Client with another library
)
```

| Option            | Purpose                                                     |
| ----------------- | ----------------------------------------------------------- |
| `WithAPIKey`      | Bearer token; sent as `Authorization: Bearer <key>`.         |
| `WithTimeout`     | Per-request timeout when no `WithHTTPClient` is supplied.   |
| `WithHeader`      | Add an extra header to every request.                       |
| `WithHTTPClient`  | Replace the underlying `*http.Client` (mTLS, instrumentation, pooling). |

`dash.New` strips any trailing slash from the base URL. The default
`User-Agent` is `dash-go/<version>`; set your own via `WithHeader` to
override.

### 5. Authentication and error handling

```go
import (
	"errors"
	"github.com/anomalyco/dash-go"
)

client := dash.New("https://dash.example.com",
	dash.WithAPIKey("sk-live-..."),
	dash.WithTimeout(10*time.Second),
)

resp, err := client.Embeddings().Create(ctx, dash.EmbeddingRequest{Input: "hello"})
if err != nil {
	var apiErr *dash.DashAPIError
	if errors.As(err, &apiErr) {
		// HTTP error. StatusCode, ErrorType, Message, and Body are
		// populated. ErrorType is "invalid_request_error" or
		// "server_error" for OpenAI-shaped bodies, and "api_error"
		// for the ad-hoc native /v1/retrieve error envelope.
		log.Printf("dash api error: %d %s: %s", apiErr.StatusCode(), apiErr.ErrorType(), apiErr.Message)
		return
	}
	var connErr *dash.DashConnectionError
	if errors.As(err, &connErr) {
		// Network failure (DNS, refused, timeout, TLS). The
		// underlying error is reachable via errors.Unwrap.
		log.Printf("dash connection error: %v", connErr.Unwrap())
		return
	}
	log.Fatalf("unexpected error: %v", err)
}
```

Both error types satisfy the `DashError` interface:

```go
type DashError interface {
	error
	StatusCode() int   // 0 for DashConnectionError
	ErrorType() string // "invalid_request_error" | "server_error" | "api_error" | "connection_error"
	Body() string
}
```

`errors.Is` / `errors.As` walk wrapped error chains, so you can
also check the underlying network error (for example to test
`net.Error.Timeout()`).

## Why a Go SDK for DASH?

DASH already speaks the OpenAI wire format, so most users can use
the official `openai-go` SDK. `dash-go` exists for the cases that
need a typed, first-class client:

- Typed response objects (so IDEs autocomplete `.CanonicalText`,
  `.Supports`, etc.).
- Native access to `/v1/retrieve` with `StanceMode` and
  `ReturnGraph` flags.
- A consistent error hierarchy (`DashConnectionError` vs
  `DashAPIError`) instead of inspecting `*http.Response` by hand.
- `context.Context` on every call for cancellation and timeouts.

## License

Apache-2.0.
