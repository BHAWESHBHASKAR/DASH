---
hide:
  - navigation
  - toc
---

# DASH: Vector database with claims, evidence, and contradictions

> **Evidence-first retrieval for citation-grade RAG.**

DASH is a production-grade vector database that stores atomic **claims** with their supporting **evidence** and recorded **contradictions**. Every retrieval result ships with the citations that justify it — the source identifier, the stance (supports / contradicts / neutral), the source quality, and an optional character span — so a downstream model or a downstream auditor can answer the question *"why did the system say that?"*.

- **OpenAI drop-in** — `POST /v1/embeddings` is wire-byte compatible with the OpenAI v1 API. Point any OpenAI client at DASH with one environment variable.
- **Semantic-first retrieval** — dense-similarity is the primary ranking signal; lexical/BM25 acts as a small tie-breaker. The result is a defensible ranking for retrieval-augmented generation.
- **Durable on-disk storage** — `redb`-backed persistence for claims, evidence, edges, vectors, and the tenant→claim set. Hash-chained audit log. Crash-recovery semantics are explicit and tested.

## At a glance

=== "Python"

    ```python
    import openai

    # Point any OpenAI client at DASH
    client = openai.OpenAI(
        base_url="http://localhost:8080/v1",
        api_key="not_needed",
    )

    resp = client.embeddings.create(
        input="Company X acquired Company Y",
        model="text-embedding-3-small",
    )
    vec = resp.data[0].embedding
    ```

=== "Go"

    ```go
    package main

    import (
        "context"
        "fmt"
        dash "github.com/BHAWESHBHASKAR/dash-go"
    )

    func main() {
        client := dash.NewClient("http://localhost:8080")
        emb, err := client.Embeddings.Create(context.Background(), &dash.EmbeddingRequest{
            Model: "text-embedding-3-small",
            Input: "Company X acquired Company Y",
        })
        if err != nil {
            panic(err)
        }
        fmt.Println(emb.Data[0].Embedding[:5])
    }
    ```

=== "curl"

    ```bash
    curl -X POST http://localhost:8080/v1/embeddings \
      -H "Content-Type: application/json" \
      -d '{
        "input": "Company X acquired Company Y",
        "model": "text-embedding-3-small"
      }'
    ```

## What makes DASH different

Naive RAG ranks documents by vector similarity and returns the top *k* chunks. That works for "summarize this article" but fails in three common enterprise cases:

1. **Two sources say opposite things** — and the system has no way to demote the contradicted one. DASH treats a `Stance::Contradicts` evidence record as a hard demotion signal, and the retrieval API exposes `stance_mode: support_only` to filter contradicted claims out entirely.
2. **A fact has a temporal window** — and the version retrieved is stale. Every claim carries `event_time_unix`, `valid_from`, and `valid_to`; the retrieval API takes a `time_range` constraint.
3. **An auditor asks "why did the model say that?"** — and the answer is "because a 768-dimensional number was close to a query." DASH returns `{ claim, score, supports, contradicts, citations[] }` — every citation carries its `source_id`, `stance`, `source_quality`, and optional `chunk_id` plus `span_start`/`span_end` for character-level traceability.

## Next steps

<div markdown class="grid cards" markdown>

-   :material-rocket-launch: **Quickstart**

    ---

    The 5-minute path from `git clone` to a first retrieval query.

    [:octicons-arrow-right-24: Get started](quickstart.md)

-   :material-book-open-page-variant: **Concepts**

    ---

    Why DASH exists, the two-service architecture, the data model, and the
    durability story.

    [:octicons-arrow-right-24: Read the concepts](concepts/index.md)

-   :material-api: **HTTP API**

    ---

    The wire-level reference for `/v1/ingest`, `/v1/retrieve`,
    `/v1/embeddings`, `/v1/health`, and `/metrics`.

    [:octicons-arrow-right-24: API reference](reference/api.md)

-   :fontawesome-brands-github: **Source code**

    ---

    Open source under Apache 2.0. 379 Rust tests + 86 Go + 65 TypeScript
    + 59 Python across the workspace.

    [:octicons-arrow-right-24: BHAWESHBHASKAR/DASH](https://github.com/BHAWESHBHASKAR/DASH)

</div>
