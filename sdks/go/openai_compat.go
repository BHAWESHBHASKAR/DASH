// OpenAI compatibility helpers.
//
// DASH's /v1/embeddings endpoint is byte-for-byte compatible with the
// OpenAI v1 embeddings API, so callers that already use the
// official openai-go SDK can point it at DASH with no code changes.
// This file is a thin reference / convenience layer: it does not
// import openai-go (that would force a runtime dependency on every
// user of dash-go), but it documents the recipe and offers
// `NewOpenAICompatibleConfig` for callers that want to feed a stock
// openai-go client with DASH settings programmatically.

package dash

import "time"

// OpenAICompatibleConfig is the minimal set of values the official
// openai-go client needs to talk to a DASH server. Pass it to
// openai.NewClient to get a fully-featured OpenAI client whose
// embeddings calls land on DASH.
//
// Example:
//
//	import (
//		openai "github.com/openai/openai-go"
//		"github.com/anomalyco/dash-go"
//	)
//
//	cfg := dash.NewOpenAICompatibleConfig("http://localhost:8080", "sk-live-...")
//	client := openai.NewClient(cfg)
//
//	resp, err := client.Embeddings.New(ctx, openai.EmbeddingNewParams{
//		Input: openai.EmbeddingNewParamsInputUnion{OfString: openai.String("hello world")},
//		Model: openai.EmbeddingModelTextEmbedding3Small,
//	})
type OpenAICompatibleConfig struct {
	// BaseURL is the DASH root with the /v1 path appended (the
	// official SDK appends the resource path itself, so we must
	// point at the /v1 prefix).
	BaseURL string
	// APIKey is forwarded as Authorization: Bearer <key>. DASH
	// ignores it when auth is disabled.
	APIKey string
	// Timeout is the per-request timeout. Zero means "use the
	// openai-go default".
	Timeout time.Duration
}

// NewOpenAICompatibleConfig builds an OpenAICompatibleConfig for the
// given DASH endpoint. The returned BaseURL is "<baseURL>/v1"; the
// openai-go SDK appends "embeddings" to it automatically.
func NewOpenAICompatibleConfig(baseURL, apiKey string) OpenAICompatibleConfig {
	return OpenAICompatibleConfig{
		BaseURL: trimRight(baseURL, "/") + "/v1",
		APIKey:  apiKey,
	}
}

// trimRight is a small wrapper around strings.TrimRight that lives
// here so this file has no external imports.
func trimRight(s, cutset string) string {
	end := len(s)
	for end > 0 && containsByte(cutset, s[end-1]) {
		end--
	}
	return s[:end]
}

func containsByte(s string, b byte) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return true
		}
	}
	return false
}
