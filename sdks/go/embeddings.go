package dash

import (
	"context"
)

// EmbeddingsService calls POST /v1/embeddings. The endpoint is
// byte-for-byte compatible with the OpenAI v1 embeddings API, so any
// client that already speaks that protocol can be pointed at DASH
// without code changes.
type EmbeddingsService struct {
	client *Client
}

// Create embeds one or more strings. The Input field of req may be a
// string or a []string; anything else returns an error.
func (s *EmbeddingsService) Create(ctx context.Context, req EmbeddingRequest) (*EmbeddingResponse, error) {
	body, err := buildEmbeddingBody(req)
	if err != nil {
		return nil, err
	}
	var out EmbeddingResponse
	if err := s.client.post(ctx, "/v1/embeddings", body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// embeddingBody is the wire shape of POST /v1/embeddings. Optional
// fields use the empty string + omitempty convention so the encoder
// drops them when unset, matching the OpenAI spec.
type embeddingBody struct {
	Input          any    `json:"input"`
	Model          string `json:"model"`
	EncodingFormat string `json:"encoding_format,omitempty"`
	User           string `json:"user,omitempty"`
}

// buildEmbeddingBody normalises an EmbeddingRequest into the wire
// shape, picking a default model and rejecting obviously invalid
// Input values before they reach the server.
func buildEmbeddingBody(req EmbeddingRequest) (embeddingBody, error) {
	if req.Input == nil {
		return embeddingBody{}, errInvalidInput("Input is required")
	}
	switch req.Input.(type) {
	case string, []string:
		// valid
	default:
		return embeddingBody{}, errInvalidInput("Input must be a string or []string, got %T", req.Input)
	}
	model := req.Model
	if model == "" {
		model = "text-embedding-3-small"
	}
	return embeddingBody{
		Input:          req.Input,
		Model:          model,
		EncodingFormat: req.EncodingFormat,
		User:           req.User,
	}, nil
}
