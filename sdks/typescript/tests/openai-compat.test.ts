/**
 * OpenAI wire-format compatibility tests.
 *
 * DASH's `/v1/embeddings` is byte-for-byte compatible with the
 * OpenAI spec, so the official `openai` npm package can be pointed
 * at DASH with `baseURL: 'http://localhost:8080/v1'`. These tests
 * pin that contract from the JS side: we mock `fetch` and assert
 * the request body DASH would receive matches what the `openai`
 * SDK is known to send.
 */

import { describe, expect, it } from 'vitest';

import { openAIBaseURL } from '../src/openai-compat.js';
import {
  BASE_URL,
  SAMPLE_EMBEDDING_RESPONSE,
  SAMPLE_RETRIEVE_RESPONSE,
  makeFetchMock,
} from './fixtures.js';
import { createClient } from '../src/client.js';

describe('openAIBaseURL', () => {
  it('appends /v1 to a bare DASH root', () => {
    expect(openAIBaseURL('http://localhost:8080')).toBe(
      'http://localhost:8080/v1',
    );
  });

  it('does not double-append when /v1 is already present', () => {
    expect(openAIBaseURL('http://localhost:8080/v1')).toBe(
      'http://localhost:8080/v1',
    );
  });

  it('strips trailing slashes before appending /v1', () => {
    expect(openAIBaseURL('http://localhost:8080/')).toBe(
      'http://localhost:8080/v1',
    );
    expect(openAIBaseURL('http://localhost:8080///')).toBe(
      'http://localhost:8080/v1',
    );
  });

  it('leaves an already-/v1-suffixed URL alone (modulo trailing /)', () => {
    expect(openAIBaseURL('http://localhost:8080/v1/')).toBe(
      'http://localhost:8080/v1',
    );
  });
});

describe('OpenAI drop-in compatibility', () => {
  it('hits POST /v1/embeddings with a single-string input', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create('hello world', {
      model: 'text-embedding-3-small',
    });
    expect(calls[0]!.method).toBe('POST');
    expect(calls[0]!.url).toBe(`${BASE_URL}/v1/embeddings`);
    expect(calls[0]!.body).toEqual({
      input: 'hello world',
      model: 'text-embedding-3-small',
    });
  });

  it('accepts a baseURL that already has /v1 — the OpenAI SDK pattern', async () => {
    // When a user does `new OpenAI({ baseURL: 'http://localhost:8080/v1' })`
    // and we hand them a DASH client that uses that URL directly, the
    // client must still hit `/v1/embeddings` (not
    // `/v1/v1/embeddings`). We simulate that by configuring a
    // DashClient whose baseUrl already ends in /v1.
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({
      baseUrl: `${BASE_URL}/v1`,
      fetch: f,
    });
    await client.embeddings.create('hi');
    expect(calls[0]!.url).toBe(`${BASE_URL}/v1/embeddings`);
  });

  it('embeds a list of strings as a JSON array', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create(['alpha', 'beta', 'gamma']);
    expect((calls[0]!.body as { input: string[] }).input).toEqual([
      'alpha',
      'beta',
      'gamma',
    ]);
  });

  it('parses the canonical OpenAI response shape', async () => {
    const body = {
      object: 'list',
      data: [
        { object: 'embedding', embedding: [0.1, 0.2, 0.3], index: 0 },
      ],
      model: 'text-embedding-3-small',
      usage: { prompt_tokens: 1, total_tokens: 1 },
    };
    const { fetch: f } = makeFetchMock(200, body);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const response = await client.embeddings.create('hi');
    expect(response.object).toBe('list');
    expect(response.model).toBe('text-embedding-3-small');
    expect(response.usage.prompt_tokens).toBe(1);
    expect(response.usage.total_tokens).toBe(1);
    expect(response.data[0]!.object).toBe('embedding');
    expect(response.data[0]!.index).toBe(0);
  });

  it('parses 400 invalid_request_error envelopes', async () => {
    const { fetch: f } = makeFetchMock(400, {
      error: {
        message: "encoding_format 'base64' is not supported; only 'float' is currently implemented",
        type: 'invalid_request_error',
        param: 'encoding_format',
        code: null,
      },
    });
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    try {
      await client.embeddings.create('hi', { encoding_format: 'base64' });
      throw new Error('expected throw');
    } catch (err) {
      expect((err as { statusCode: number }).statusCode).toBe(400);
      expect((err as { errorType: string }).errorType).toBe(
        'invalid_request_error',
      );
      expect((err as { errorMessage: string }).errorMessage).toContain(
        'encoding_format',
      );
    }
  });

  it('preserves the Claim + Evidence + Contradiction differentiator', async () => {
    const body = {
      results: [
        {
          claim_id: 'claim-support',
          canonical_text: 'Acme grew 30% in Q3.',
          score: 0.91,
          supports: 5,
          contradicts: 0,
          citations: [
            {
              evidence_id: 'ev-a',
              source_id: 's://wsj',
              stance: 'supports',
              source_quality: 0.9,
            },
          ],
        },
        {
          claim_id: 'claim-contra',
          canonical_text: 'Acme actually shrank in Q3.',
          score: 0.82,
          supports: 1,
          contradicts: 3,
          citations: [
            {
              evidence_id: 'ev-b',
              source_id: 's://bloomberg',
              stance: 'contradicts',
              source_quality: 0.85,
            },
          ],
        },
      ],
    };
    const { fetch: f } = makeFetchMock(200, body);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const response = await client.retrieve.query({
      tenant_id: 't',
      query: 'acme q3 revenue',
    });
    const clean = response.results.filter(
      (r) => r.contradicts === 0 && r.supports > 0,
    );
    expect(clean.map((r) => r.claim_id)).toEqual(['claim-support']);
  });

  it('integrates with the standard /v1/retrieve JSON shape', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.retrieve.query({ tenant_id: 't', query: 'q' });
    expect(calls[0]!.url).toBe(`${BASE_URL}/v1/retrieve`);
    expect(calls[0]!.body).toEqual({
      tenant_id: 't',
      query: 'q',
      top_k: 10,
      stance_mode: 'balanced',
    });
  });
});
