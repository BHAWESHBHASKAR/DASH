/**
 * Tests for {@link RetrieveService}.
 */

import { describe, expect, it } from 'vitest';

import { DashAPIError, DashConnectionError } from '../src/errors.js';
import {
  BASE_URL,
  SAMPLE_RETRIEVE_RESPONSE,
  makeFailingFetchMock,
  makeFetchMock,
} from './fixtures.js';
import { createClient } from '../src/client.js';

describe('RetrieveService.query', () => {
  it('uses the default top_k=10 and stance_mode=balanced', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const response = await client.retrieve.query({
      tenant_id: 'tenant-a',
      query: 'company x',
    });

    expect(calls).toHaveLength(1);
    expect(calls[0]!.method).toBe('POST');
    expect(calls[0]!.url).toBe(`${BASE_URL}/v1/retrieve`);
    expect(calls[0]!.body).toEqual({
      tenant_id: 'tenant-a',
      query: 'company x',
      top_k: 10,
      stance_mode: 'balanced',
    });
    expect(response.results).toHaveLength(1);
  });

  it('omits return_graph when not provided', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.retrieve.query({ tenant_id: 't', query: 'q' });
    expect('return_graph' in (calls[0]!.body as Record<string, unknown>)).toBe(
      false,
    );
  });

  it('honours a custom top_k', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.retrieve.query({
      tenant_id: 't',
      query: 'q',
      top_k: 3,
    });
    expect((calls[0]!.body as { top_k: number }).top_k).toBe(3);
  });

  it.each(['balanced', 'support_only'] as const)(
    'sends the %s stance mode on the wire',
    async (mode) => {
      const { fetch: f, calls } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
      const client = createClient({ baseUrl: BASE_URL, fetch: f });
      await client.retrieve.query({
        tenant_id: 't',
        query: 'q',
        stance_mode: mode,
      });
      expect((calls[0]!.body as { stance_mode: string }).stance_mode).toBe(mode);
    },
  );

  it('sends return_graph when explicitly set', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.retrieve.query({
      tenant_id: 't',
      query: 'q',
      return_graph: true,
    });
    expect((calls[0]!.body as { return_graph: boolean }).return_graph).toBe(true);
  });

  it('handles empty results without error', async () => {
    const { fetch: f } = makeFetchMock(200, { results: [] });
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const response = await client.retrieve.query({ tenant_id: 't', query: 'q' });
    expect(response.results).toEqual([]);
  });

  it('parses claim + evidence + contradiction fields verbatim', async () => {
    const { fetch: f } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const response = await client.retrieve.query({ tenant_id: 't', query: 'q' });
    const r = response.results[0]!;
    expect(r.claim_id).toBe('claim-1');
    expect(r.canonical_text).toBe('Acme Co. was acquired in 2024.');
    expect(r.score).toBeCloseTo(0.93);
    expect(r.supports).toBe(4);
    expect(r.contradicts).toBe(1);
    expect(r.citations).toHaveLength(1);
    const c = r.citations[0]!;
    expect(c.evidence_id).toBe('ev-1');
    expect(c.stance).toBe('supports');
    expect(c.source_quality).toBeCloseTo(0.88);
    expect(c.span_start).toBe(120);
    expect(c.span_end).toBe(168);
    expect(c.ingested_at).toBe(1_735_689_700_000);
  });
});

describe('RetrieveService.topResult', () => {
  it('returns the first result on success', async () => {
    const { fetch: f } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const result = await client.retrieve.topResult({
      tenant_id: 't',
      query: 'q',
    });
    expect(result).not.toBeNull();
    expect(result!.claim_id).toBe('claim-1');
  });

  it('returns null when there are no results', async () => {
    const { fetch: f } = makeFetchMock(200, { results: [] });
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const result = await client.retrieve.topResult({
      tenant_id: 't',
      query: 'q',
    });
    expect(result).toBeNull();
  });
});

describe('RetrieveService error mapping', () => {
  it('raises DashAPIError on 503 with ad-hoc error shape', async () => {
    const { fetch: f } = makeFetchMock(503, {
      error: 'routing unavailable',
      code: 'no_healthy_node',
    });
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    try {
      await client.retrieve.query({ tenant_id: 't', query: 'q' });
      throw new Error('expected throw');
    } catch (err) {
      expect(err).toBeInstanceOf(DashAPIError);
      const e = err as DashAPIError;
      expect(e.statusCode).toBe(503);
      expect(e.errorMessage).toBe('routing unavailable');
    }
  });

  it('raises DashAPIError on 403', async () => {
    const { fetch: f } = makeFetchMock(403, {
      error: 'tenant is not allowed for this API key',
    });
    const client = createClient({
      baseUrl: BASE_URL,
      apiKey: 'sk-test',
      fetch: f,
    });
    await expect(
      client.retrieve.query({ tenant_id: 't-other', query: 'q' }),
    ).rejects.toMatchObject({ statusCode: 403 });
  });

  it('raises DashConnectionError on transport failure', async () => {
    const { fetch: f } = makeFailingFetchMock(new Error('socket hang up'));
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await expect(
      client.retrieve.query({ tenant_id: 't', query: 'q' }),
    ).rejects.toBeInstanceOf(DashConnectionError);
  });
});
