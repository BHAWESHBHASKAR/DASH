# Guides

The guides are task-oriented. They show the request and the response, the option that matters, and the failure mode. They are written to be read in order, but each one stands alone.

## Pages

- [**Ingest**](ingest.md) — `POST /v1/ingest`. The `IngestRequest` shape, idempotency, and bulk ingest.
- [**Retrieve**](retrieve.md) — `POST /v1/retrieve`. The retrieval request, dense vs sparse vs hybrid ranking, and the stance / time filters.
- [**Embeddings**](embeddings.md) — `POST /v1/embeddings`. The OpenAI drop-in, provider selection, and the base64 encoding.
- [**SDKs**](sdks.md) — Python, Go, TypeScript, Java, and C#. The package, the import, and a complete example for each.

## How to read this section

If you are wiring DASH into a new application, start with [Ingest](ingest.md) and [Retrieve](retrieve.md) — those are the two endpoints that move data. If you are migrating from an OpenAI-compatible stack, [Embeddings](embeddings.md) is the only page you need. If you are building a client library, [SDKs](sdks.md) is the reference.
