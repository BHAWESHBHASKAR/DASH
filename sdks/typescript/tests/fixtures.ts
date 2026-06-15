/**
 * Shared test fixtures and helpers for the dash-ts test suite.
 *
 * Tests inject a custom `fetch` via `ClientOptions.fetch` so no
 * live DASH instance is required. Each fixture returns the JSON
 * body the Rust server would emit for a given request.
 */

import type {
  EmbeddingResponse,
  RetrieveResponse,
} from '../src/types.js';

export const BASE_URL = 'http://localhost:8080';

/** A canonical OpenAI-compatible embedding response (single string). */
export const SAMPLE_EMBEDDING_RESPONSE: EmbeddingResponse = {
  object: 'list',
  data: [
    {
      object: 'embedding',
      embedding: [0.013, -0.042, 0.077, 0.0, 0.5],
      index: 0,
    },
  ],
  model: 'text-embedding-3-small',
  usage: { prompt_tokens: 2, total_tokens: 2 },
};

/** A canonical OpenAI-compatible embedding response (3-string array). */
export const SAMPLE_EMBEDDING_ARRAY_RESPONSE: EmbeddingResponse = {
  object: 'list',
  data: [
    { object: 'embedding', embedding: [0.1, 0.2, 0.3], index: 0 },
    { object: 'embedding', embedding: [0.4, 0.5, 0.6], index: 1 },
    { object: 'embedding', embedding: [0.7, 0.8, 0.9], index: 2 },
  ],
  model: 'text-embedding-3-small',
  usage: { prompt_tokens: 6, total_tokens: 6 },
};

/** A canonical native /v1/retrieve response (one claim + one citation). */
export const SAMPLE_RETRIEVE_RESPONSE: RetrieveResponse = {
  results: [
    {
      claim_id: 'claim-1',
      canonical_text: 'Acme Co. was acquired in 2024.',
      score: 0.93,
      supports: 4,
      contradicts: 1,
      citations: [
        {
          evidence_id: 'ev-1',
          source_id: 'source://reuters',
          stance: 'supports',
          source_quality: 0.88,
          chunk_id: 'chunk-7',
          span_start: 120,
          span_end: 168,
          doc_id: 'doc://reuters-acme',
          extraction_model: 'extractor-v5',
          ingested_at: 1_735_689_700_000,
        },
      ],
    },
  ],
};

/** OpenAI-style error body. */
export const OPENAI_STYLE_ERROR_BODY = {
  error: {
    message: 'input must contain at least one text',
    type: 'invalid_request_error',
    param: null,
    code: null,
  },
};

/**
 * A minimal `fetch`-compatible function shape. We only use
 * `url`, `method`, `body`, `headers`, and `signal` from the
 * init bag.
 */
export interface FetchCall {
  url: string;
  method: string;
  body: unknown;
  headers: Record<string, string>;
  signal: AbortSignal | null;
}

/**
 * Build a `fetch` mock that returns a canned response and records
 * the call for later assertions.
 */
export function makeFetchMock(
  status: number,
  body: unknown,
  rawText?: string,
): { fetch: typeof fetch; calls: FetchCall[] } {
  const calls: FetchCall[] = [];
  const text = rawText ?? (body === undefined ? '' : JSON.stringify(body));
  const fetchImpl = (async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const url = typeof input === 'string' ? input : input.toString();
    const method = (init?.method ?? 'GET').toUpperCase();
    const headers: Record<string, string> = {};
    if (init?.headers) {
      const h = init.headers;
      if (h instanceof Headers) {
        h.forEach((v, k) => {
          headers[k] = v;
        });
      } else if (Array.isArray(h)) {
        for (const [k, v] of h) {
          headers[k] = v;
        }
      } else {
        Object.assign(headers, h as Record<string, string>);
      }
    }
    let parsedBody: unknown = null;
    if (init?.body) {
      if (typeof init.body === 'string') {
        try {
          parsedBody = JSON.parse(init.body);
        } catch {
          parsedBody = init.body;
        }
      } else {
        parsedBody = init.body;
      }
    }
    calls.push({ url, method, body: parsedBody, headers, signal: init?.signal ?? null });
    return new Response(text, {
      status,
      headers: { 'Content-Type': 'application/json' },
    });
  }) as unknown as typeof fetch;
  return { fetch: fetchImpl, calls };
}

/**
 * Build a `fetch` mock that throws on every call. Useful for
 * simulating network failures.
 */
export function makeFailingFetchMock(
  err: Error = new Error('refused'),
): { fetch: typeof fetch; calls: FetchCall[] } {
  const calls: FetchCall[] = [];
  const fetchImpl = (async (
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> => {
    const url = typeof input === 'string' ? input : input.toString();
    calls.push({
      url,
      method: (init?.method ?? 'GET').toUpperCase(),
      body: null,
      headers: {},
      signal: init?.signal ?? null,
    });
    throw err;
  }) as unknown as typeof fetch;
  return { fetch: fetchImpl, calls };
}
