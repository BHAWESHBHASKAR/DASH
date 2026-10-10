/**
 * Tests for the delete methods (ingestion service).
 */

import { describe, expect, it } from 'vitest';

import { createClient } from '../src/client.js';
import { deriveIngestionUrl } from '../src/deletes.js';
import { DashAPIError, DashError } from '../src/errors.js';
import { BASE_URL, makeFetchMock } from './fixtures.js';

const CLAIM_DELETED = {
  deleted: true,
  scope: 'claim',
  tenant_id: 'tenant a',
  claim_id: 'c/1',
  claims_deleted: 1,
  evidence_deleted: 2,
  edges_deleted: 1,
  vectors_deleted: 1,
  claims_total: 41,
  checkpoint_triggered: false,
  checkpoint_deferred: false,
};

describe('deriveIngestionUrl', () => {
  it('maps port 8080 to 8081 and nothing else', () => {
    expect(deriveIngestionUrl('http://localhost:8080')).toBe('http://localhost:8081');
    expect(deriveIngestionUrl('https://dash.local:8080/base/')).toBe('https://dash.local:8081/base');
    expect(deriveIngestionUrl('https://dash.example.com')).toBeUndefined();
    expect(deriveIngestionUrl('http://localhost:9000')).toBeUndefined();
    expect(deriveIngestionUrl('not a url')).toBeUndefined();
  });
});

describe('DashClient deletes', () => {
  it('sends DELETE /v1/claims/{id} to the ingestion service', async () => {
    const { fetch: f, calls } = makeFetchMock(200, CLAIM_DELETED);
    const client = createClient({ baseUrl: BASE_URL, apiKey: 'k', fetch: f });
    const response = await client.deleteClaim('tenant a', 'c/1');
    expect(calls).toHaveLength(1);
    expect(calls[0]!.method).toBe('DELETE');
    expect(calls[0]!.url).toBe('http://localhost:8081/v1/claims/c%2F1?tenant_id=tenant%20a');
    expect(calls[0]!.body).toBeNull();
    expect(calls[0]!.headers['Authorization']).toBe('Bearer k');
    expect(response.deleted).toBe(true);
    expect(response.evidence_deleted).toBe(2);
    expect(response.claim_id).toBe('c/1');
  });

  it('uses the evidence and tenant routes on an explicit ingestion URL', async () => {
    const { fetch: f, calls } = makeFetchMock(200, {
      deleted: false,
      scope: 'evidence',
      tenant_id: 't',
      evidence_id: 'e 1',
    });
    const client = createClient({
      baseUrl: 'https://dash.example.com',
      ingestionBaseUrl: 'https://ingest.example.com/',
      fetch: f,
    });
    const evidence = await client.deleteEvidence('t', 'e 1');
    await client.deleteTenant('t');
    expect(calls.map((c) => c.url)).toEqual([
      'https://ingest.example.com/v1/evidence/e%201?tenant_id=t',
      'https://ingest.example.com/v1/tenants/t',
    ]);
    expect(evidence.deleted).toBe(false);
    expect(evidence.claims_deleted).toBe(0);
  });

  it('fails before any request without an ingestion URL', async () => {
    const { fetch: f, calls } = makeFetchMock(200, CLAIM_DELETED);
    const client = createClient({ baseUrl: 'https://dash.example.com', fetch: f });
    await expect(client.deleteTenant('t')).rejects.toBeInstanceOf(DashError);
    await expect(client.deleteTenant('t')).rejects.toThrow(/ingestionBaseUrl/);
    expect(calls).toHaveLength(0);
  });

  it('rejects empty identifiers', async () => {
    const { fetch: f, calls } = makeFetchMock(200, CLAIM_DELETED);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await expect(client.deleteClaim('', 'c')).rejects.toThrow(/tenantId/);
    await expect(client.deleteClaim('t', ' ')).rejects.toThrow(/claimId/);
    await expect(client.deleteEvidence('t', '')).rejects.toThrow(/evidenceId/);
    await expect(client.deleteTenant('')).rejects.toThrow(/tenantId/);
    expect(calls).toHaveLength(0);
  });

  it('surfaces a 403 as DashAPIError', async () => {
    const { fetch: f } = makeFetchMock(403, { error: 'missing required role: admin' });
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const err = await client.deleteTenant('t').catch((e: unknown) => e);
    expect(err).toBeInstanceOf(DashAPIError);
    expect((err as DashAPIError).statusCode).toBe(403);
  });
});
