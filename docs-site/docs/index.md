---
hide:
  - navigation
  - toc
---

# DASH: Vector database with claims, evidence, and contradictions

> **Evidence-first retrieval for citation-grade RAG.**

DASH is a pre-1.0 evidence-first vector database (not yet production-ready; see the README Status section) that stores atomic **claims** with their supporting **evidence** and recorded **contradictions**. Every retrieval result ships with the citations that justify it — the source identifier, the stance (supports / contradicts / neutral), the source quality, and an optional character span — so a downstream model or a downstream auditor can answer the question *"why did the system say that?"*.

- **OpenAI-compatible embeddings** — `POST /v1/embeddings` follows the OpenAI v1 request and response shape. Point an OpenAI client at DASH by setting its base URL and API key (authentication is required from v0.3.0).
- **Semantic-first retrieval** — dense-similarity is the primary ranking signal; lexical/BM25 acts as a small tie-breaker. The result is a defensible ranking for retrieval-augmented generation.
- **On-disk storage** — a write-ahead log with checkpoints, plus a `redb` mirror (on by default when a WAL path is set). Optional SHA-256 hash-chained audit log. Recovery is tested for the single-process case; known gaps are listed in the issue register.

## At a glance

=== "Python (OpenAI client)"

    ```python
    import os
    import openai

    # Point an OpenAI client at DASH's retrieval service
    client = openai.OpenAI(
        base_url="http://localhost:8080/v1",
        api_key=os.environ["DASH_RETRIEVAL_API_KEY"],
    )

    resp = client.embeddings.create(
        input="Company X acquired Company Y",
        model="text-embedding-3-small",
    )
    vec = resp.data[0].embedding
    ```

=== "curl (retrieve)"

    ```bash
    curl -fsS -X POST http://localhost:8080/v1/retrieve \
      -H "Content-Type: application/json" \
      -H "x-api-key: $DASH_RETRIEVAL_API_KEY" \
      -d '{
        "tenant_id": "t1",
        "query": "Company X acquired Company Y",
        "top_k": 5,
        "stance_mode": "support_only"
      }'
    ```

The [Quickstart](quickstart.md) shows how to start the stack and generate the keys. The first-party [SDKs](guides/sdks.md) are installed from a checkout; none is published to a package registry yet.

## What makes DASH different

Naive RAG ranks documents by vector similarity and returns the top *k* chunks. That works for "summarize this article" but fails in three common enterprise cases:

1. **Two sources say opposite things** — and the system has no way to demote the contradicted one. DASH counts `Stance::Contradicts` evidence (and contradicting edges) against a claim and lowers its score, and the retrieval API exposes `stance_mode: support_only` to drop claims with more contradictions than supports.
2. **A fact has a temporal window** — and the version retrieved is stale. Every claim carries `event_time_unix`, `valid_from`, and `valid_to`; the retrieval API takes a `time_range` constraint.
3. **An auditor asks "why did the model say that?"** — and the answer is "because a high-dimensional vector was close to a query." DASH returns `{ claim_id, canonical_text, score, supports, contradicts, citations[] }` — every citation carries its `source_id`, `stance`, `source_quality`, and optional `chunk_id` plus `span_start`/`span_end` for character-level traceability.

## Next steps

<div markdown class="grid cards" markdown>

-   :material-rocket-launch: **Quickstart**

    ---

    The 5-minute path from `git clone` to a first retrieval query.

    [:octicons-arrow-right-24: Get started](quickstart.md)

-   :material-book-open-page-variant: **Concepts**

    ---

    Why DASH exists, the service architecture, the data model, and the
    durability story.

    [:octicons-arrow-right-24: Read the concepts](concepts/index.md)

-   :material-api: **HTTP API**

    ---

    The wire-level reference for `/v1/ingest`, `/v1/retrieve`,
    `/v1/embeddings`, `/v1/health`, and `/metrics`, with auth requirements and status codes.

    [:octicons-arrow-right-24: API reference](reference/api.md)

-   :fontawesome-brands-github: **Source code**

    ---

    Open source under Apache 2.0. 379 Rust tests + 86 Go + 65 TypeScript
    + 59 Python across the workspace.

    [:octicons-arrow-right-24: BHAWESHBHASKAR/DASH](https://github.com/BHAWESHBHASKAR/DASH)

</div>
