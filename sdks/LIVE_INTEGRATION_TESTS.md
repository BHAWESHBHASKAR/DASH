# Live Integration Tests

The DASH SDKs ship with comprehensive unit tests that use mocks
(MockWebServer for Java, `httptest.Server` for Go, `vi.mock` for
TypeScript, `responses` for Python). The mocks prove the SDK is
correct in isolation, but the **first-contact risk** — the SDK
working against a real DASH server over a real network — needs
to be exercised before each release.

This directory (and the `tests/test_live_integration.py`,
`live_integration_test.go`, `tests/live-integration.test.ts`
files at the SDK roots) contains opt-in live tests that hit a
running DASH instance.

## Running

### Python

```bash
export DASH_LIVE_URL=http://127.0.0.1:8080
export DASH_LIVE_INGESTION_URL=http://127.0.0.1:8081
export DASH_LIVE_RETRIEVAL_URL=http://127.0.0.1:8080
cd sdks/python && pytest tests/test_live_integration.py -v
```

### Go

```bash
export DASH_LIVE_URL=http://127.0.0.1:8080
cd sdks/go && go test -tags=integration -v ./...
```

### TypeScript

```bash
export DASH_LIVE_URL=http://127.0.0.1:8080
export LIVE=1
cd sdks/typescript && npm test -- live-integration.test.ts
```

### Java / Kotlin

The Java SDK's live tests live in
`sdks/java/src/test/java/dev/dash/LiveIntegrationTest.java`
and use the same `DASH_LIVE_URL` env var. Run with
`mvn -Dlive.tests=true test`.

### C# / .NET

`sdks/csharp/tests/Dash.Tests.Live/LiveIntegrationTests.cs`
is a separate project guarded by `[Trait("Category", "Live")]`
and `DASH_LIVE_URL`. Run with
`dotnet test --filter "Category=Live"`.

## What they cover

For each SDK, the live tests verify:

1. `/v1/health` returns 200 with `{"status": "ok"}`.
2. `/v1/embeddings` returns a float vector for a string input.
3. `/v1/embeddings` with `encoding_format=base64` returns a
   base64 string that round-trips through `base64` decode and
   `f32::from_le_bytes` (or the language equivalent).
4. `POST /v1/ingest` accepts a bundle, and a follow-up
   `POST /v1/retrieve` with the ingested text returns at
   least one hit containing that text.
5. `POST /v1/delete` does not return 5xx for a non-existent
   claim id (404 or 200 are both acceptable).

## Why opt-in?

These tests require a running DASH instance (the easiest way
is `docker compose -f deploy/container/docker-compose.yml up`).
We don't want CI to spin up a database on every PR just to
exercise the SDK. Instead, the live tests run:

- on every push to `main` via `.github/workflows/sdks-live.yml`
  (a workflow that starts a DASH service in a sidecar container)
- before every release via `.github/workflows/release.yml`
  (which is the most important gate: a release cannot ship
  with a broken first-contact path)

## Adding a new live test

The pattern is:

1. Use `process.env.DASH_LIVE_URL` (or language equivalent)
   to discover the URL.
2. Skip with a clear message when the env var is unset.
3. Wait for `/v1/health` to return 200 before running the
   actual assertions.
4. Use a unique tenant id (timestamp or uuid) to avoid
   interference between concurrent runs.
5. Allow up to 500ms after ingest for the WAL flush.
