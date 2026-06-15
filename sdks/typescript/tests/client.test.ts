/**
 * Tests for {@link DashClient} construction and configuration.
 *
 * The HTTP transport is mocked by injecting a custom `fetch`
 * function via `ClientOptions.fetch`, so no live DASH instance is
 * required.
 */

import { describe, expect, it } from 'vitest';

import { DashClient, createClient } from '../src/client.js';
import { DashError, DashAPIError, DashConnectionError } from '../src/errors.js';
import {
  BASE_URL,
  SAMPLE_EMBEDDING_RESPONSE,
  makeFetchMock,
} from './fixtures.js';

describe('DashClient construction', () => {
  it('strips a trailing slash from baseUrl', () => {
    const client = new DashClient({ baseUrl: `${BASE_URL}/` });
    expect(client.baseUrl).toBe(BASE_URL);
  });

  it('strips multiple trailing slashes from baseUrl', () => {
    const client = new DashClient({ baseUrl: `${BASE_URL}///` });
    expect(client.baseUrl).toBe(BASE_URL);
  });

  it('rejects an empty baseUrl', () => {
    expect(() => new DashClient({ baseUrl: '' })).toThrow(/baseUrl/);
  });

  it('rejects a non-positive timeoutMs', () => {
    expect(() => new DashClient({ baseUrl: BASE_URL, timeoutMs: 0 })).toThrow(
      /timeoutMs/,
    );
    expect(() => new DashClient({ baseUrl: BASE_URL, timeoutMs: -1 })).toThrow(
      /timeoutMs/,
    );
  });

  it('exposes an embeddings and retrieve namespace', () => {
    const client = new DashClient({ baseUrl: BASE_URL });
    expect(client.embeddings).toBeDefined();
    expect(typeof client.embeddings.create).toBe('function');
    expect(client.retrieve).toBeDefined();
    expect(typeof client.retrieve.query).toBe('function');
  });

  it('createClient is equivalent to the constructor', () => {
    const a = createClient({ baseUrl: BASE_URL });
    const b = new DashClient({ baseUrl: BASE_URL });
    expect(a).toBeInstanceOf(DashClient);
    expect(b).toBeInstanceOf(DashClient);
  });

  it('defaults timeoutMs to 30000', () => {
    const client = new DashClient({ baseUrl: BASE_URL });
    expect(client.timeoutMs).toBe(30_000);
  });

  it('honours a custom timeoutMs', () => {
    const client = new DashClient({ baseUrl: BASE_URL, timeoutMs: 5_000 });
    expect(client.timeoutMs).toBe(5_000);
  });
});

describe('DashClient auth + headers', () => {
  it('sends an Authorization header when apiKey is set', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = new DashClient({
      baseUrl: BASE_URL,
      apiKey: 'sk-test-123',
      fetch: f,
    });
    await client.embeddings.create('hi');
    expect(calls).toHaveLength(1);
    expect(calls[0]!.headers['Authorization']).toBe('Bearer sk-test-123');
  });

  it('omits Authorization when apiKey is unset', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = new DashClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create('hi');
    expect('Authorization' in calls[0]!.headers).toBe(false);
  });

  it('merges defaultHeaders on top of the built-in headers', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = new DashClient({
      baseUrl: BASE_URL,
      fetch: f,
      defaultHeaders: { 'X-Tenant': 'acme' },
    });
    await client.embeddings.create('hi');
    expect(calls[0]!.headers['X-Tenant']).toBe('acme');
    expect(calls[0]!.headers['Accept']).toBe('application/json');
  });

  it('sends the dash-ts User-Agent on every request', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = new DashClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create('hi');
    expect(calls[0]!.headers['User-Agent']).toBe('dash-ts/0.1.0');
  });
});

describe('DashClient error-class exports', () => {
  it('exposes DashError, DashAPIError, DashConnectionError', () => {
    expect(DashError).toBeDefined();
    expect(DashAPIError).toBeDefined();
    expect(DashConnectionError).toBeDefined();
  });
});
