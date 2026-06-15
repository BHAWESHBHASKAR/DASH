# dash-ts

A thin, idiomatic TypeScript client for the [DASH](https://github.com/dash-retrieval/dash)
retrieval engine. DASH serves an OpenAI-compatible `/v1/embeddings`
endpoint and a native `/v1/retrieve` endpoint that returns structured
**Claim + Evidence + Contradiction** results — the differentiator
that makes it a real retrieval engine, not a vector store.

```bash
npm install dash-ts
```

Requires Node.js 18+ (uses the built-in `fetch`). Zero runtime
dependencies.

## 5-minute quickstart

### 1. Embeddings (OpenAI-compatible)

```typescript
import { createClient } from 'dash-ts';

const client = createClient({ baseUrl: 'http://localhost:8080' });
const response = await client.embeddings.create('hello world');
console.log(response.data[0]!.embedding.slice(0, 5));
// [0.013, -0.042, 0.077, 0.0, 0.5]
```

That's it — the same shape as `new OpenAI(...).embeddings.create(...)`.
DASH's `/v1/embeddings` is byte-for-byte compatible with the OpenAI
spec, so any tool that speaks the OpenAI protocol just works.

### 2. Drop-in OpenAI compatibility (zero code change)

If you already use the `openai` npm package, you don't even need
`dash-ts` to talk to DASH — just point the base URL at DASH:

```typescript
import OpenAI from 'openai';
import { openAIBaseURL } from 'dash-ts';

const client = new OpenAI({
  baseURL: openAIBaseURL('http://localhost:8080'),
  apiKey: 'not-required-for-local',
});
const resp = await client.embeddings.create({
  input: 'hello world',
  model: 'text-embedding-3-small',
});
console.log(resp.data[0]!.embedding.slice(0, 5));
```

`openAIBaseURL('http://localhost:8080')` returns
`'http://localhost:8080/v1'`, the exact shape the OpenAI SDK expects.
This is the same path LangChain, LlamaIndex, Semantic Kernel, and
the `openai` CLI use, so they all work too.

### 3. Native retrieve (the differentiator)

Where DASH pulls ahead of a vector store is the retrieval endpoint,
which returns Claims with explicit **support** and **contradiction**
counts and a structured **Citation** list per claim. That's the
substance you actually need to build a RAG pipeline that doesn't
confidently echo back the wrong thing.

```typescript
import { createClient } from 'dash-ts';

const client = createClient({ baseUrl: 'http://localhost:8080' });

// Balanced mode (default) — include every claim.
const response = await client.retrieve.query({
  tenant_id: 'acme-corp',
  query: 'what was the Q3 2024 revenue?',
  top_k: 5,
  stance_mode: 'balanced',
});

// A RAG pipeline that wants "claims that are actually supported and
// not contradicted" can filter the typed results directly:
const cleanClaims = response.results.filter(
  (r) => r.contradicts === 0 && r.supports > 0,
);

for (const claim of response.results) {
  console.log(`[${claim.score.toFixed(2)}] ${claim.canonical_text}`);
  console.log(`   supports=${claim.supports}  contradicts=${claim.contradicts}`);
  for (const cite of claim.citations) {
    console.log(
      `   - ${cite.stance.padStart(11)}  ${cite.source_id}  (q=${cite.source_quality.toFixed(2)})`,
    );
  }
}
```

Use `stance_mode: 'support_only'` to filter out claims whose
contradiction tally exceeds their support tally before they reach
your prompt.

### 4. The Claim + Evidence + Contradiction RAG pattern

A practical RAG pipeline that doesn't echo back contradicted claims:

```typescript
import {
  createClient,
  DashAPIError,
  DashConnectionError,
  type Citation,
} from 'dash-ts';

const client = createClient({
  baseUrl: 'http://localhost:8080',
  apiKey: process.env.DASH_API_KEY,
  timeoutMs: 10_000,
});

interface CleanClaim {
  text: string;
  score: number;
  citations: Citation[];
}

async function retrieveCleanClaims(query: string): Promise<CleanClaim[]> {
  const response = await client.retrieve.query({
    tenant_id: 'acme-corp',
    query,
    top_k: 10,
    stance_mode: 'support_only', // server-side: drop contradicted claims
  });
  // Client-side: keep only claims with at least one supporting citation.
  return response.results
    .filter((r) => r.supports > 0)
    .map((r) => ({
      text: r.canonical_text,
      score: r.score,
      citations: r.citations,
    }));
}

const clean = await retrieveCleanClaims('Q3 2024 revenue');
const context = clean
  .map(
    (c) => `- ${c.text} [score=${c.score.toFixed(2)}, citations=${c.citations.length}]`,
  )
  .join('\n');
// paste `context` into your LLM prompt.
```

### 5. Authentication & error handling

```typescript
import {
  createClient,
  DashAPIError,
  DashConnectionError,
} from 'dash-ts';

const client = createClient({
  baseUrl: 'https://dash.example.com',
  apiKey: 'sk-live-...',
  timeoutMs: 10_000,
});

try {
  const response = await client.embeddings.create('hello');
} catch (err) {
  if (err instanceof DashConnectionError) {
    // Network failure (DNS, refused, timeout). The underlying
    // fetch / AbortError is on `err.cause`.
    console.error('connectivity:', err.message, 'cause:', err.cause);
  } else if (err instanceof DashAPIError) {
    // Non-2xx response. statusCode, errorType, errorMessage,
    // and the raw body are all populated.
    console.error(
      `DASH ${err.statusCode} ${err.errorType}: ${err.errorMessage}`,
    );
  } else {
    throw err;
  }
}
```

Every error inherits from `DashError`, so a single
`catch (err) { if (err instanceof DashError) { ... } }` is enough
to catch "anything went wrong talking to DASH".

## API surface

| Symbol | Purpose |
| --- | --- |
| `createClient(options)` | Construct a `DashClient`. |
| `DashClient` | Holds `baseUrl`, `apiKey`, `timeoutMs`, `defaultHeaders`, `fetch`. |
| `client.embeddings.create(input, options?)` | `POST /v1/embeddings`, single string or array. |
| `client.embeddings.createRaw(request)` | `POST /v1/embeddings` with the full request body. |
| `client.retrieve.query(request, options?)` | `POST /v1/retrieve` with `{ tenant_id, query, top_k?, stance_mode?, return_graph? }`. |
| `client.retrieve.topResult(request, options?)` | Convenience: the top result, or `null`. |
| `DashError`, `DashAPIError`, `DashConnectionError` | Exception hierarchy. |
| `openAIBaseURL(base)` | Build an OpenAI-style base URL (`http://host/v1`). |
| `request(opts)`, `defaultHeaders()`, `USER_AGENT` | Low-level transport escape hatch. |
| `VERSION` | The package version string. |

## Configuration reference

```typescript
interface ClientOptions {
  baseUrl: string;              // e.g. "http://localhost:8080"
  apiKey?: string;              // → "Authorization: Bearer <apiKey>"
  timeoutMs?: number;           // default 30_000
  fetch?: typeof fetch;         // override for tests / proxies
  defaultHeaders?: Record<string, string>;
}
```

The `timeoutMs` can be overridden per-call:

```typescript
await client.embeddings.create('hi', { timeoutMs: 5_000 });
await client.retrieve.query(req, { timeoutMs: 5_000 });
```

Cancellation uses the standard `AbortSignal` API:

```typescript
const ctl = new AbortController();
setTimeout(() => ctl.abort(), 1_000);
await client.embeddings.create('hi', { signal: ctl.signal });
```

## Why a TypeScript SDK for DASH?

DASH already speaks the OpenAI wire format, so most users can use
the official `openai` SDK. `dash-ts` exists for the cases that
need a typed, first-class client:

- Typed response objects (so IDEs autocomplete `.canonical_text`,
  `.supports`, etc.).
- Native access to `/v1/retrieve` with `stance_mode` and
  `return_graph` flags.
- A consistent exception hierarchy (`DashConnectionError` vs
  `DashAPIError`) instead of inspecting `fetch` rejections by hand.
- Zero runtime dependencies — pure ESM, native `fetch`, Node 18+.

## License

Apache-2.0.
