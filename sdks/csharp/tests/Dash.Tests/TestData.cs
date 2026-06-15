using System.Collections.Generic;

namespace Dash.Tests;

/// <summary>
/// Canonical test fixtures: the JSON bodies the Rust DASH server
/// would emit for a given request.
/// </summary>
internal static class TestData
{
    public const string BaseUrl = "http://localhost:8080";

    public const string SampleEmbeddingResponseJson = """
    {
      "object": "list",
      "data": [
        {
          "object": "embedding",
          "embedding": [0.013, -0.042, 0.077, 0.0, 0.5],
          "index": 0
        }
      ],
      "model": "text-embedding-3-small",
      "usage": { "prompt_tokens": 2, "total_tokens": 2 }
    }
    """;

    public const string SampleEmbeddingArrayResponseJson = """
    {
      "object": "list",
      "data": [
        { "object": "embedding", "embedding": [0.1, 0.2, 0.3], "index": 0 },
        { "object": "embedding", "embedding": [0.4, 0.5, 0.6], "index": 1 },
        { "object": "embedding", "embedding": [0.7, 0.8, 0.9], "index": 2 }
      ],
      "model": "text-embedding-3-small",
      "usage": { "prompt_tokens": 6, "total_tokens": 6 }
    }
    """;

    public const string SampleRetrieveResponseJson = """
    {
      "hits": [
        {
          "claim_id": "claim-1",
          "canonical_text": "Acme Co. was acquired in 2024.",
          "score": { "overall": 0.93, "semantic": 0.95, "lexical": 0.80 },
          "supports": 4,
          "contradicts": 1,
          "citations": [
            {
              "evidence_id": "ev-1",
              "source_id": "source://reuters",
              "stance": "supports",
              "source_quality": 0.88,
              "chunk_id": "chunk-7",
              "span_start": 120,
              "span_end": 168,
              "doc_id": "doc://reuters-acme",
              "extraction_model": "extractor-v5",
              "ingested_at": 1735689700000
            }
          ]
        }
      ]
    }
    """;

    public const string SampleIngestResponseJson = """
    {
      "bundles_ingested": 1,
      "claims_ingested": 2,
      "evidence_ingested": 3
    }
    """;

    public const string SampleDeleteResponseJson = """
    {
      "claims_deleted": 5,
      "evidence_deleted": 9
    }
    """;

    public const string SampleHealthResponseJson = """
    {
      "status": "ok",
      "version": "0.4.0",
      "details": { "uptime_seconds": 12345 }
    }
    """;

    public static readonly object OpenAIStyleErrorBody = new
    {
        error = new
        {
            message = "input must contain at least one text",
            type = "invalid_request_error",
            param = (string?)null,
            code = (string?)null,
        },
    };

    public static readonly object ServerErrorBody = new
    {
        error = new
        {
            message = "embedding failed: backend unavailable",
            type = "server_error",
        },
    };

    public static EmbeddingRequest SampleEmbeddingRequest() => new()
    {
        Input = "hello world",
        Model = "text-embedding-3-small",
    };

    public static IReadOnlyList<float> SampleVector() =>
        new[] { 0.013f, -0.042f, 0.077f, 0.0f, 0.5f };
}
