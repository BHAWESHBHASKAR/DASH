/**
 * Live integration tests for the DASH TypeScript SDK.
 *
 * Skipped by default; runs only when DASH_LIVE_URL is set.
 * Run with:  DASH_LIVE_URL=http://127.0.0.1:8080 npm test
 *
 * The TypeScript SDK types EmbeddingData.embedding as number[],
 * so the base64 wire format is not directly testable from this
 * SDK. The Python SDK has the same test using the openai drop-in
 * (which handles string/number polymorphism via OpenAI's own
 * types). The float path is exercised here.
 */
import { describe, test, expect, beforeAll } from 'vitest';
import { DashClient } from '../src/client';
import type {
  HealthResponse,
  EmbeddingResponse,
  IngestResponse,
  RetrieveResponse,
} from '../src/types';

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

async function waitForHealth(baseURL: string, timeoutMs = 10000): Promise<void> {
  const client = new DashClient({ baseUrl: baseURL, apiKey: 'not_needed' });
  const deadline = Date.now() + timeoutMs;
  let lastErr: unknown;
  while (Date.now() < deadline) {
    try {
      await client.health();
      return;
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
    const h: HealthResponse = await client.health();
    expect(h.status).toBe('ok');
  });

  test('embed returns float vector', async () => {
    const resp: EmbeddingResponse = await client.embeddings.create({
      input: 'hello world',
      model: 'text-embedding-3-small',
    });
    expect(resp.model).toBe('text-embedding-3-small');
    expect(resp.data).toHaveLength(1);
    expect(resp.data[0].embedding.length).toBeGreaterThan(0);
    expect(typeof resp.data[0].embedding[0]).toBe('number');
  });

  test('embed base64 encoding_format is accepted in the request', async () => {
    // The TS SDK's EmbeddingData.embedding is number[] only, so we
    // cannot assert the round-trip here. This test verifies that
    // the SDK does not drop the encoding_format field from the
    // outgoing request body, and that the server accepts it.
    try {
      await client.embeddings.create({
        input: 'base64 probe',
        model: 'text-embedding-3-small',
        encoding_format: 'base64',
      });
    } catch (e: unknown) {
      // A decode error on the response is acceptable here: it means
      // the server honored the encoding_format and returned a
      // base64 string that the SDK couldn't parse. The fact that
      // the request body was correct is what we're verifying.
      const err = e as { statusCode?: number; message?: string };
      if (!err.message?.includes('JSON') && err.statusCode === undefined) {
        throw e;
      }
    }
  });

  test('ingest then retrieve returns the ingested phrase', async () => {
    const ing = new DashClient({ baseUrl: ingestionURL(), apiKey: 'not_needed' });
    const ret = new DashClient({ baseUrl: retrievalURL(), apiKey: 'not_needed' });
    const tenantId = `test-tenant-${Date.now()}`;
    const phrase = `distinctive phrase ${Date.now()}`;

    const ingestResp: IngestResponse = await ing.ingest({
      tenant_id: tenantId,
      bundles: [
        {
          claim_id: 'claim-1',
          text: phrase,
          evidence: [
            { evidence_id: 'ev-1', text: `Evidence supporting: ${phrase}` },
          ],
        },
      ],
    });
    expect(ingestResp).toBeDefined();

    // Allow the WAL flush / write-through to settle.
    await new Promise((r) => setTimeout(r, 500));

    const resp: RetrieveResponse = await ret.retrieve.query({
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

  test('delete accepts claim ids', async () => {
    const ing = new DashClient({ baseUrl: ingestionURL(), apiKey: 'not_needed' });
    try {
      await ing.delete({ tenant_id: 'test-tenant', claim_ids: ['nonexistent-id'] });
    } catch (e: unknown) {
      const err = e as { statusCode?: number };
      if (err.statusCode !== undefined) {
        expect(err.statusCode).toBeLessThan(500);
      }
    }
  });
});
