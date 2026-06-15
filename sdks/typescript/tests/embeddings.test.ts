/**
 * Tests for {@link EmbeddingsService}.
 *
 * The HTTP transport is mocked at the global `fetch` boundary, so
 * no live DASH instance is required. The fixtures return the exact
 * JSON the Rust server emits.
 */

import { describe, expect, it } from 'vitest';

import { DashAPIError, DashConnectionError, DashError } from '../src/errors.js';
import {
  BASE_URL,
  OPENAI_STYLE_ERROR_BODY,
  SAMPLE_EMBEDDING_ARRAY_RESPONSE,
  SAMPLE_EMBEDDING_RESPONSE,
  makeFailingFetchMock,
  makeFetchMock,
} from './fixtures.js';
import { createClient } from '../src/client.js';

describe('EmbeddingsService.create', () => {
  it('embeds a single string and returns the typed response', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const response = await client.embeddings.create('hello world');

    expect(response.object).toBe('list');
    expect(response.model).toBe('text-embedding-3-small');
    expect(response.usage.prompt_tokens).toBe(2);
    expect(response.usage.total_tokens).toBe(2);
    expect(response.data).toHaveLength(1);
    expect(response.data[0]!.object).toBe('embedding');
    expect(response.data[0]!.index).toBe(0);
    expect(response.data[0]!.embedding[0]).toBeCloseTo(0.013);

    // Wire body: single string, no list wrapping, model preserved.
    expect(calls).toHaveLength(1);
    expect(calls[0]!.method).toBe('POST');
    expect(calls[0]!.url).toBe(`${BASE_URL}/v1/embeddings`);
    expect(calls[0]!.body).toEqual({
      input: 'hello world',
      model: 'text-embedding-3-small',
    });
  });

  it('embeds a list of strings and preserves indices', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_ARRAY_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const response = await client.embeddings.create(['a', 'b', 'c']);

    expect(response.data).toHaveLength(3);
    expect(response.data.map((d) => d.index)).toEqual([0, 1, 2]);
    expect(calls[0]!.body).toEqual({
      input: ['a', 'b', 'c'],
      model: 'text-embedding-3-small',
    });
  });

  it('defaults to the text-embedding-3-small model', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create('hi');
    expect((calls[0]!.body as { model: string }).model).toBe(
      'text-embedding-3-small',
    );
  });

  it('sends a custom model when provided', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create('hi', { model: 'text-embedding-3-large' });
    expect((calls[0]!.body as { model: string }).model).toBe(
      'text-embedding-3-large',
    );
  });

  it('sends encoding_format and user when provided', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create('hi', {
      encoding_format: 'float',
      user: 'user-42',
    });
    const body = calls[0]!.body as Record<string, unknown>;
    expect(body.encoding_format).toBe('float');
    expect(body.user).toBe('user-42');
  });

  it('omits encoding_format and user by default', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create('hi');
    const body = calls[0]!.body as Record<string, unknown>;
    expect('encoding_format' in body).toBe(false);
    expect('user' in body).toBe(false);
  });

  it('always sets Content-Type: application/json', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create('hi');
    expect(calls[0]!.headers['Content-Type']).toBe('application/json');
  });
});

describe('EmbeddingsService.createRaw', () => {
  it('sends the full request body unchanged', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.createRaw({
      input: 'hi',
      model: 'text-embedding-3-large',
      encoding_format: 'float',
      user: 'u-1',
    });
    expect(calls[0]!.body).toEqual({
      input: 'hi',
      model: 'text-embedding-3-large',
      encoding_format: 'float',
      user: 'u-1',
    });
  });

  it('falls back to the default model when not provided', async () => {
    const { fetch: f, calls } = makeFetchMock(200, SAMPLE_EMBEDDING_RESPONSE);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.createRaw({ input: 'hi' });
    expect((calls[0]!.body as { model: string }).model).toBe(
      'text-embedding-3-small',
    );
  });
});

describe('EmbeddingsService error mapping', () => {
  it('raises DashAPIError on 4xx with OpenAI-style envelope', async () => {
    const { fetch: f } = makeFetchMock(400, OPENAI_STYLE_ERROR_BODY);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await expect(client.embeddings.create('hi')).rejects.toBeInstanceOf(
      DashAPIError,
    );
    await expect(client.embeddings.create('hi')).rejects.toMatchObject({
      statusCode: 400,
      errorType: 'invalid_request_error',
      errorMessage: 'input must contain at least one text',
    });
  });

  it('raises DashAPIError on 5xx server_error', async () => {
    const { fetch: f } = makeFetchMock(500, {
      error: {
        message: 'embedding failed: backend unavailable',
        type: 'server_error',
        param: null,
        code: null,
      },
    });
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await expect(client.embeddings.create('hi')).rejects.toBeInstanceOf(
      DashAPIError,
    );
    await expect(client.embeddings.create('hi')).rejects.toMatchObject({
      statusCode: 500,
      errorType: 'server_error',
    });
  });

  it('falls back to raw body for non-JSON 5xx', async () => {
    const { fetch: f } = makeFetchMock(502, undefined, 'Bad Gateway');
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await expect(client.embeddings.create('hi')).rejects.toMatchObject({
      statusCode: 502,
      errorType: 'api_error',
    });
    await expect(client.embeddings.create('hi')).rejects.toMatchObject({
      body: 'Bad Gateway',
    });
  });

  it('raises DashConnectionError when fetch throws', async () => {
    const { fetch: f } = makeFailingFetchMock(new Error('ECONNREFUSED'));
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    try {
      await client.embeddings.create('hi');
      throw new Error('should not reach');
    } catch (err) {
      expect(err).toBeInstanceOf(DashError);
      expect(err).toBeInstanceOf(DashConnectionError);
      expect((err as DashConnectionError).cause?.message).toBe('ECONNREFUSED');
      expect((err as DashConnectionError).statusCode).toBe(0);
      expect((err as DashConnectionError).errorType).toBe('connection_error');
    }
  });

  it('DashAPIError is a subclass of DashError', async () => {
    const { fetch: f } = makeFetchMock(400, OPENAI_STYLE_ERROR_BODY);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await expect(client.embeddings.create('hi')).rejects.toBeInstanceOf(DashError);
  });

  it('error messages include the status and type', async () => {
    const { fetch: f } = makeFetchMock(400, OPENAI_STYLE_ERROR_BODY);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    try {
      await client.embeddings.create('hi');
      throw new Error('expected throw');
    } catch (err) {
      const msg = (err as Error).message;
      expect(msg).toContain('400');
      expect(msg).toContain('invalid_request_error');
      expect(msg).toContain('input must contain at least one text');
    }
  });
});
