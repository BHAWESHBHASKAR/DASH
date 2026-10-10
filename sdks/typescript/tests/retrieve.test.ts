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
  it('uses the default top_k=5 and stance_mode=balanced', async () => {
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
      top_k: 5,
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

describe('RetrieveService.query server-contract fields', () => {
  it('sends optional payload.rs fields when provided', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.retrieve.query({
      tenant_id: 't',
      query: 'q',
      top_k: 3,
      return_graph: true,
      query_embedding: [0.5, 0.25],
      entity_filters: ['acme'],
      embedding_id_filters: ['emb-1'],
      time_range: { from_unix: 10, to_unix: 20 },
      read_consistency: 'quorum',
    });
    expect(calls[0]!.body).toEqual({
      tenant_id: 't',
      query: 'q',
      top_k: 3,
      stance_mode: 'balanced',
      return_graph: true,
      query_embedding: [0.5, 0.25],
      entity_filters: ['acme'],
      embedding_id_filters: ['emb-1'],
      time_range: { from_unix: 10, to_unix: 20 },
      read_consistency: 'quorum',
    });
  });

  it('omits unset optional fields and half-open time ranges', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_RETRIEVE_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.retrieve.query({ tenant_id: 't', query: 'q', time_range: { from_unix: 5 } });
    const body = calls[0]!.body as Record<string, unknown>;
    expect(body.time_range).toEqual({ from_unix: 5 });
    for (const key of ['query_embedding', 'entity_filters', 'embedding_id_filters', 'read_consistency']) {
      expect(key in body).toBe(false);
    }
  });

  it('decodes graph, read policy and extra node fields', async () => {
    const serverBody = {
      results: [
        {
          claim_id: 'c-1',
          canonical_text: 'x',
          score: 0.5,
          claim_confidence: 0.8,
          confidence_band: 'high',
          dominant_stance: 'supports',
          contradiction_risk: null,
          graph_score: 0.25,
          support_path_count: 2,
          contradiction_chain_depth: null,
          supports: 1,
          contradicts: 0,
          citations: [],
          event_time_unix: 1700000000,
          temporal_in_range: true,
          claim_type: 'factual',
          future_field: { ignored: true },
        },
      ],
      graph: {
        nodes: [],
        edges: [{ from_claim_id: 'c-1', to_claim_id: 'c-2', relation: 'supports', strength: 0.5 }],
      },
      read_policy: 'one',
      read_quorum_met: true,
      serving_replica: null,
    };
    const { fetch: f } = makeFetchMock(200, serverBody);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const response = await client.retrieve.query({ tenant_id: 't', query: 'q' });

    const hit = response.results[0]!;
    expect(hit.claim_confidence).toBe(0.8);
    expect(hit.confidence_band).toBe('high');
    expect(hit.contradiction_risk).toBeNull();
    expect(hit.support_path_count).toBe(2);
    expect(hit.temporal_in_range).toBe(true);
    expect(hit.event_time_unix).toBe(1700000000);
    expect(response.graph?.edges[0]?.relation).toBe('supports');
    expect(response.read_policy).toBe('one');
    expect(response.read_quorum_met).toBe(true);
    expect(response.serving_replica).toBeUndefined();
  });
});
