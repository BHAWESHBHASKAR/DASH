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

    // Shape emitted by render_retrieve_response_json in
    // services/retrieval/src/transport/payload.rs (numeric score, nullable
    // optional fields, extra fields the SDK must tolerate).
    public const string SampleRetrieveResponseJson = """
    {
      "results": [
        {
          "claim_id": "claim-1",
          "canonical_text": "Acme Co. was acquired in 2024.",
          "score": 0.930000,
          "claim_confidence": 0.870000,
          "confidence_band": "high",
          "dominant_stance": "supports",
          "contradiction_risk": null,
          "graph_score": 0.410000,
          "support_path_count": 2,
          "contradiction_chain_depth": null,
          "supports": 4,
          "contradicts": 1,
          "citations": [
            {
              "evidence_id": "ev-1",
              "source_id": "source://reuters",
              "stance": "supports",
              "source_quality": 0.880000,
              "chunk_id": "chunk-7",
              "span_start": 120,
              "span_end": 168,
              "doc_id": "doc://reuters-acme",
              "extraction_model": "extractor-v5",
              "ingested_at": 1735689700000
            }
          ],
          "event_time_unix": 1735689600,
          "temporal_match_mode": null,
          "temporal_in_range": null,
          "claim_type": "factual",
          "valid_from": null,
          "valid_to": null,
          "created_at": 1735689601,
          "updated_at": null,
          "some_future_field": { "ignored": true }
        }
      ],
      "graph": {
        "nodes": [],
        "edges": [
          { "from_claim_id": "claim-1", "to_claim_id": "claim-2", "relation": "supports", "strength": 0.500000 }
        ]
      },
      "read_policy": "one",
      "read_quorum_met": true,
      "serving_replica": null
    }
    """;

    public const string MinimalRetrieveResponseJson = """{ "results": [ { "claim_id": "c", "canonical_text": "t", "score": 1 } ] }""";

    // Shape of IngestApiResponse in services/ingestion/src/api.rs.
    public const string SampleIngestResponseJson = """
    {
      "ingested_claim_id": "c-1",
      "claims_total": 7,
      "commit_epoch": 12,
      "ack_count": 1,
      "required_acks": 1,
      "commit_status": "committed",
      "checkpoint_triggered": false
    }
    """;

    public const string SampleHealthResponseJson = """
    {
      "status": "ok",
      "version": "0.4.0",
      "details": { "uptime_seconds": 12345 }
    }
    """;

    // DELETE responses, shaped like docs-site/docs/reference/api.md (Deletes).
    public const string SampleDeleteClaimResponseJson = """
    {
      "deleted": true,
      "scope": "claim",
      "tenant_id": "t1",
      "claim_id": "c1",
      "claims_deleted": 1,
      "evidence_deleted": 2,
      "edges_deleted": 1,
      "vectors_deleted": 1,
      "claims_total": 41,
      "checkpoint_triggered": false,
      "checkpoint_deferred": false
    }
    """;

    public const string SampleDeleteEvidenceResponseJson = """
    {
      "deleted": true,
      "scope": "evidence",
      "tenant_id": "t1",
      "evidence_id": "ev-1",
      "claims_deleted": 0,
      "evidence_deleted": 3,
      "edges_deleted": 0,
      "vectors_deleted": 0,
      "claims_total": 41,
      "checkpoint_triggered": false,
      "checkpoint_deferred": false
    }
    """;

    public const string SampleDeleteTenantResponseJson = """
    {
      "deleted": true,
      "scope": "tenant",
      "tenant_id": "t1",
      "claims_deleted": 5,
      "evidence_deleted": 9,
      "edges_deleted": 4,
      "vectors_deleted": 5,
      "claims_total": 0,
      "checkpoint_triggered": true,
      "checkpoint_deferred": false
    }
    """;

    public const string SampleDeleteNothingResponseJson = """
    {
      "deleted": false,
      "scope": "claim",
      "tenant_id": "t1",
      "claim_id": "missing",
      "claims_deleted": 0,
      "evidence_deleted": 0,
      "edges_deleted": 0,
      "vectors_deleted": 0,
      "claims_total": 41,
      "checkpoint_triggered": false,
      "checkpoint_deferred": false
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
