# Changelog

All notable changes to `dash-csharp` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Retrieval models now match the server: the response field is `results`
  (was `hits`), `score` is a number (was an object), and the extra
  fields (`claim_confidence`, `contradiction_risk`, `graph`,
  `read_policy`, ...) are decoded as optional. Response models tolerate
  unknown and missing optional fields.
- Ingest models now match `POST /v1/ingest` (`claim`, `evidence`,
  `edges`; response `ingested_claim_id`, `claims_total`,
  `commit_status`, ...). Ingest is **experimental**.
- `netstandard2.0` target compiles (polyfills for `init`/`required`,
  `ThrowIfNull`, `HttpStatusCode.TooManyRequests`); the test project no
  longer inherits the multi-target setting.
- `TopK` and `StanceMode` are omitted by default so server defaults
  (`top_k` 5, `balanced`) apply.

### Added

- `DeleteClaimAsync`, `DeleteEvidenceAsync` and `DeleteTenantAsync` (with
  sync variants `DeleteClaim`, `DeleteEvidence`, `DeleteTenant`) for
  `DELETE /v1/claims/{id}?tenant_id=...`, `DELETE /v1/evidence/{id}?tenant_id=...`
  and `DELETE /v1/tenants/{id}` on the ingestion service, and the
  `DeleteResponse` model. Ids are percent-encoded; blank ids throw
  `ArgumentException`. Deletes are idempotent and retried like reads. The
  generic `DeleteAsync` removed in 0.2.0 stays removed.
- `DashClientOptions.IngestionBaseUrl` and `DashClient.IngestionBaseUrl`
  for the separate ingestion service (default port 8081).
- `RequestOptions` (`IdempotencyKey`, `Retry`).
- Optional retrieve request fields: `QueryEmbedding`, `EntityFilters`,
  `EmbeddingIdFilters`, `TimeRange`, `ReadConsistency`.

### Changed

- Retries only happen for idempotent requests (embeddings, retrieve,
  health) or when an idempotency key / `Retry = true` is supplied;
  ingest is never retried by default. Backoff is jittered and capped
  (5 s); `Retry-After` is honoured (up to 30 s); `MaxRetries` is capped
  at 10.

### Removed

- `DeleteAsync` / `Delete` and the `DeleteRequest` / `DeleteResponse`
  models: the server has no `/v1/delete` endpoint.

## [0.2.0] - 2026-06-15

### Added

- Initial public release.
- `DashClient` with both synchronous and asynchronous APIs for
  `/v1/embeddings`, `/v1/ingest`, `/v1/retrieve`, `/v1/delete`,
  and `/health`.
- Typed request / response models under the `Dash` namespace
  (`EmbeddingRequest`, `IngestRequest`, `RetrievalRequest`,
  `DeleteRequest`, `HealthResponse`, etc.).
- `DashException` hierarchy with `DashAuthException`,
  `DashRateLimitException`, and `DashNotFoundException` subclasses
  for typed error handling.
- `DashConnectionException` for transport-level failures.
- Hand-rolled retry with exponential backoff in `HttpTransport`,
  honouring the server's `Retry-After` header on HTTP 429.
- `System.Text.Json`-based JSON serialisation with a
  `SnakeCaseLower` naming policy and `JsonPropertyName`
  attributes on the public models.
- Multi-target build: `net8.0` and `netstandard2.0`.
- 25+ xUnit tests under `tests/Dash.Tests` covering happy paths,
  401/403/404/429/500 error mapping, network failures, retries,
  cancellation, sync variants, dispose, and OpenAI drop-in
  compatibility.
- Console sample under `samples/Dash.Sample` that hits a live
  DASH instance when `DASH_URL` is set; otherwise no-ops.
