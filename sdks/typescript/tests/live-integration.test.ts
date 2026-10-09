/**
 * Live integration tests for the DASH TypeScript SDK.
 *
 * Skipped by default; runs only when DASH_LIVE_URL is set.
 * Run with:  DASH_LIVE_URL=http://127.0.0.1:8080 npm test
 *
 * Environment: DASH_LIVE_URL (required), DASH_LIVE_RETRIEVAL_URL and
 * DASH_LIVE_INGESTION_URL (both default to DASH_LIVE_URL), and optionally
 * DASH_LIVE_INGEST_API_KEY for an authenticated ingestion service.
 */
import { describe, test, expect, beforeAll } from 'vitest';
import { DashClient } from '../src/client';
import type { EmbeddingResponse, RetrieveResponse } from '../src/types';

const LIVE = !!process.env.DASH_LIVE_URL;

const liveURL = (): string => {
  const v = process.env.DASH_LIVE_URL;
  if (!v) throw new Error('DASH_LIVE_URL not set');
  return v;
};
const retrievalURL = (): string =>
  process.env.DASH_LIVE_RETRIEVAL_URL || liveURL();
const ingestionURL = (): string =>
  process.env.DASH_LIVE_INGESTION_URL || liveURL();

const trimSlash = (url: string): string => url.replace(/\/$/, '');

async function waitForHealth(baseURL: string, timeoutMs = 10000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`${trimSlash(baseURL)}/v1/health`);
      if (res.status === 200) return;
      lastErr = new Error(`status ${res.status}`);
    } catch (e) {
      lastErr = e;
    }
    await new Promise((r) => setTimeout(r, 200));
  }
  throw new Error(
    `DASH at ${baseURL} did not become healthy in ${timeoutMs}ms: ${String(lastErr)}`,
  );
}

const describeIfLive = LIVE ? describe : describe.skip;

describeIfLive('live integration', () => {
  let client: DashClient;

  beforeAll(async () => {
    client = new DashClient({ baseUrl: retrievalURL(), apiKey: 'not_needed' });
    await waitForHealth(retrievalURL());
  });

  test('health endpoint returns ok', async () => {
    const res = await fetch(`${trimSlash(retrievalURL())}/v1/health`);
    expect(res.status).toBe(200);
    expect(((await res.json()) as { status: string }).status).toBe('ok');
  });

  test('embed returns float vector', async () => {
    const resp: EmbeddingResponse = await client.embeddings.create('hello world', {
      model: 'text-embedding-3-small',
    });
    expect(resp.model).toBe('text-embedding-3-small');
    expect(resp.data).toHaveLength(1);
    expect(resp.data[0]!.embedding.length).toBeGreaterThan(0);
    expect(typeof resp.data[0]!.embedding[0]).toBe('number');
  });

  test('embeddings with encoding_format base64 decode to floats', async () => {
    const resp: EmbeddingResponse = await client.embeddings.create('base64 test', {
      encoding_format: 'base64',
    });
    expect(resp.data).toHaveLength(1);
    expect(resp.data[0]!.embedding.length).toBeGreaterThan(0);
    expect(resp.data[0]!.embedding.every((v) => Number.isFinite(v))).toBe(true);
  });

  test('ingest then retrieve returns the ingested phrase', async () => {
    const tenantId = `test-tenant-${Date.now()}`;
    const phrase = `distinctive phrase ${Date.now()}`;
    // Claim ids are global across tenants, so keep them unique per run.
    const claimId = `claim-${tenantId}`;
    const headers: Record<string, string> = { 'Content-Type': 'application/json' };
    const ingestKey = process.env.DASH_LIVE_INGEST_API_KEY;
    if (ingestKey) headers.Authorization = `Bearer ${ingestKey}`;

    // The SDK has no ingest method; post the real ingestion wire shape.
    const ingest = await fetch(`${trimSlash(ingestionURL())}/v1/ingest`, {
      method: 'POST',
      headers,
      body: JSON.stringify({
        claim: {
          claim_id: claimId,
          tenant_id: tenantId,
          canonical_text: phrase,
          confidence: 0.9,
        },
        evidence: [
          {
            evidence_id: `ev-${tenantId}`,
            claim_id: claimId,
            source_id: 'test://integration',
            stance: 'supports',
            source_quality: 0.8,
          },
        ],
        edges: [],
      }),
    });
    expect(ingest.status).toBe(200);

    // Allow the WAL flush / replication to settle.
    await new Promise((r) => setTimeout(r, 500));

    const resp: RetrieveResponse = await client.retrieve.query({
      tenant_id: tenantId,
      query: phrase,
      top_k: 3,
    });
    expect(resp.results.length).toBeGreaterThan(0);
    const found = resp.results.some((h) =>
      (h.canonical_text ?? '').includes(phrase),
    );
    expect(found).toBe(true);
  });

  test('delete of an unknown claim never returns 5xx', async () => {
    const res = await fetch(`${trimSlash(ingestionURL())}/v1/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ tenant_id: 'test-tenant', claim_ids: ['nonexistent-id'] }),
    });
    expect(res.status).toBeLessThan(500);
  });
});
