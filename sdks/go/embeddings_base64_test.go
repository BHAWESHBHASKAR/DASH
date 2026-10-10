package dash

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// serverBase64 is the exact shape the server emits for
// encoding_format=base64: float32 values packed little-endian, standard
// base64 alphabet with padding. It encodes [1.0, -2.0, 3.5, 0.125].
const serverBase64 = "AACAPwAAAMAAAGBAAAAAPg=="

func embeddingResponseBody(embedding any) map[string]any {
	return map[string]any{
		"object": "list",
		"data": []any{
			map[string]any{"object": "embedding", "embedding": embedding, "index": 0},
		},
		"model": "m",
		"usage": map[string]any{"prompt_tokens": 1, "total_tokens": 1},
	}
}

func TestEmbeddingDataDecodesBase64IntoFloats(t *testing.T) {
	var data EmbeddingData
	require.NoError(t, json.Unmarshal(
		[]byte(`{"object":"embedding","embedding":"`+serverBase64+`","index":2}`), &data))
	assert.Equal(t, []float32{1.0, -2.0, 3.5, 0.125}, data.Embedding)
	assert.Equal(t, 2, data.Index)
	assert.Equal(t, "embedding", data.Object)
}

func TestEmbeddingDataKeepsFloatListsUnchanged(t *testing.T) {
	var data EmbeddingData
	require.NoError(t, json.Unmarshal(
		[]byte(`{"object":"embedding","embedding":[0.5,0.25],"index":1}`), &data))
	assert.Equal(t, []float32{0.5, 0.25}, data.Embedding)
	assert.Equal(t, 1, data.Index)
}

func TestEmbeddingDataRejectsMalformedBase64(t *testing.T) {
	var data EmbeddingData
	assert.Error(t, json.Unmarshal([]byte(`{"embedding":"not base64!!","index":0}`), &data))
	// 5 bytes is not a whole number of float32 values.
	assert.Error(t, json.Unmarshal([]byte(`{"embedding":"MTIzNDU=","index":0}`), &data))
}

func TestEmbeddingsCreateBase64ReturnsFloats(t *testing.T) {
	h := &recordingHandler{status: 200, body: embeddingResponseBody(serverBase64)}
	srv := newTestServer(t, h)
	resp, err := New(srv.URL).Embeddings().Create(context.Background(), EmbeddingRequest{
		Input:          "hi",
		EncodingFormat: "base64",
	})
	require.NoError(t, err)
	require.Len(t, resp.Data, 1)
	assert.Equal(t, []float32{1.0, -2.0, 3.5, 0.125}, resp.Data[0].Embedding)

	var body map[string]any
	require.NoError(t, json.Unmarshal(h.lastRequest(t).Body, &body))
	assert.Equal(t, "base64", body["encoding_format"])
}

func TestEmbeddingsCreateSendsDimensionsOnlyWhenSet(t *testing.T) {
	h := &recordingHandler{status: 200, body: embeddingResponseBody([]float32{0.5, 0.25})}
	srv := newTestServer(t, h)
	c := New(srv.URL)

	_, err := c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi", Dimensions: 2})
	require.NoError(t, err)
	var body map[string]any
	require.NoError(t, json.Unmarshal(h.lastRequest(t).Body, &body))
	assert.EqualValues(t, 2, body["dimensions"])

	_, err = c.Embeddings().Create(context.Background(), EmbeddingRequest{Input: "hi"})
	require.NoError(t, err)
	body = map[string]any{}
	require.NoError(t, json.Unmarshal(h.lastRequest(t).Body, &body))
	assert.NotContains(t, body, "dimensions")
}
