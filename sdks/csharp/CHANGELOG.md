# Changelog

All notable changes to `dash-csharp` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
