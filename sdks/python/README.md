# dash-py

A thin, idiomatic Python client for the [DASH](https://github.com/dash-retrieval/dash)
retrieval engine. DASH serves an OpenAI-compatible `/v1/embeddings`
endpoint and a native `/v1/retrieve` endpoint that returns structured
**Claim + Evidence + Contradiction** results — the differentiator
that makes it a real retrieval engine, not a vector store.

```bash
pip install dash-py
```

## 5-minute quickstart

### 1. Embeddings (OpenAI-compatible)

```python
from dash import Client

client = Client(base_url="http://localhost:8080")
response = client.embeddings.create("hello world", model="text-embedding-3-small")
print(response.data[0].embedding[:5])
# [0.013, -0.042, 0.077, 0.0, 0.5]
```

That's it — the same shape as `openai.OpenAI(...).embeddings.create(...)`.
DASH's `/v1/embeddings` is byte-for-byte compatible with the OpenAI
spec, so any tool that speaks the OpenAI protocol just works.

### 2. Drop-in OpenAI compatibility (zero code change)

If you already use the `openai` Python SDK, you don't even need
`dash-py` to talk to DASH — just point the base URL at DASH:

```python
import openai

client = openai.OpenAI(
    base_url="http://localhost:8080/v1",
    api_key="not-required-for-local",
)
resp = client.embeddings.create(
    input="hello world",
    model="text-embedding-3-small",
)
print(resp.data[0].embedding[:5])
```

This is the same path LangChain, LlamaIndex, Semantic Kernel, and the
`openai` CLI use, so they all work too.

### 3. Native retrieve (the differentiator)

Where DASH pulls ahead of a vector store is the retrieval endpoint,
which returns Claims with explicit **support** and **contradiction**
counts and a structured **Citation** list per claim. That's the
substance you actually need to build a RAG pipeline that doesn't
confidently echo back the wrong thing.

```python
from dash import Client

client = Client(base_url="http://localhost:8080")

# Balanced mode (default) — include all claims.
response = client.retrieve(
    tenant_id="acme-corp",
    query="what was the Q3 2024 revenue?",
    top_k=5,
    stance_mode="balanced",
)

# A RAG pipeline that wants "claims that are actually supported and
# not contradicted" can filter the typed results directly:
clean_claims = [
    r for r in response.results
    if r.contradicts == 0 and r.supports > 0
]

for claim in response.results:
    print(f"[{claim.score:.2f}] {claim.canonical_text}")
    print(f"   supports={claim.supports}  contradicts={claim.contradicts}")
    for cite in claim.citations:
        print(f"   - {cite.stance:>11s}  {cite.source_id}  (q={cite.source_quality:.2f})")
```

Use `stance_mode="support_only"` to filter out claims whose
contradiction tally exceeds their support tally before they reach your
prompt.

### 4. Async client

```python
import asyncio
from dash import AsyncClient

async def main() -> None:
    async with AsyncClient(base_url="http://localhost:8080") as client:
        response = await client.embeddings.create("hello world")
        print(response.data[0].embedding[:5])

asyncio.run(main())
```

The async client is a 1:1 mirror of the sync one, built on
`httpx.AsyncClient`. All methods are `async def`.

### 5. Authentication & error handling

```python
from dash import Client, DashAPIError, DashConnectionError

client = Client(
    base_url="https://dash.example.com",
    api_key="sk-live-...",
    timeout=10.0,
)

try:
    response = client.embeddings.create("hello")
except DashConnectionError as exc:
    # Network failure (DNS, refused, timeout). ``__cause__`` chains
    # the underlying requests.RequestException.
    ...
except DashAPIError as exc:
    # Non-2xx response. ``status_code``, ``error_type``,
    # ``error_message`` and ``body`` are populated.
    print(exc.status_code, exc.error_type, exc.error_message)
```

Both clients support `with` / `async with` for safe connection-pool
cleanup.

## Why a Python SDK for DASH?

DASH already speaks the OpenAI wire format, so most users can use the
official `openai` SDK. `dash-py` exists for the cases that need a
typed, first-class client:

- Typed response objects (so IDEs autocomplete `.canonical_text`,
  `.supports`, etc.).
- Native access to `/v1/retrieve` with `stance_mode` and
  `return_graph` flags.
- A consistent exception hierarchy (`DashConnectionError` vs
  `DashAPIError`) instead of inspecting `requests`/`httpx` exceptions
  by hand.
- A coroutine-first async client without the `openai` SDK's
  beta-header gymnastics.

## License

Apache-2.0.
