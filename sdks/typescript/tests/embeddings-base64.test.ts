/**
 * `encoding_format: "base64"` and `dimensions` support.
 *
 * The base64 fixture is the exact shape the server emits: float32 values
 * packed little-endian, standard base64 alphabet with padding.
 * `AACAPwAAAMAAAGBAAAAAPg==` is `[1.0, -2.0, 3.5, 0.125]`.
 */

import { describe, expect, it } from 'vitest';

import { createClient } from '../src/client.js';
import { embeddingRequestToBody, parseEmbeddingResponse } from '../src/types.js';
import { BASE_URL, makeFetchMock } from './fixtures.js';

const SERVER_BASE64 = 'AACAPwAAAMAAAGBAAAAAPg==';
const FLOATS = [1.0, -2.0, 3.5, 0.125];

function response(embedding: unknown): Record<string, unknown> {
  return {
    object: 'list',
    data: [{ object: 'embedding', embedding, index: 0 }],
    model: 'm',
    usage: { prompt_tokens: 1, total_tokens: 1 },
  };
}

describe('base64 embeddings', () => {
  it('decodes the server base64 string into a float array', () => {
    const parsed = parseEmbeddingResponse(response(SERVER_BASE64));
    expect(parsed.data[0]!.embedding).toEqual(FLOATS);
  });

  it('keeps float arrays unchanged', () => {
    const parsed = parseEmbeddingResponse(response([0.5, 0.25]));
    expect(parsed.data[0]!.embedding).toEqual([0.5, 0.25]);
  });

  it('rejects malformed base64 and partial float32 values', () => {
    expect(() => parseEmbeddingResponse(response('not base64!!'))).toThrow(TypeError);
    // 5 bytes is not a whole number of float32 values.
    expect(() => parseEmbeddingResponse(response(btoa('12345')))).toThrow(TypeError);
  });

  it('create() with encoding_format base64 returns floats', async () => {
    const { fetch: f, calls } = makeFetchMock(200, response(SERVER_BASE64));
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    const result = await client.embeddings.create('hi', { encoding_format: 'base64' });
    expect(result.data[0]!.embedding).toEqual(FLOATS);
    expect(calls[0]!.body).toMatchObject({ encoding_format: 'base64' });
  });
});

describe('dimensions request parameter', () => {
  it('is sent only when provided', async () => {
    const { fetch: f, calls } = makeFetchMock(200, response([0.5, 0.25]));
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    await client.embeddings.create('hi', { dimensions: 2 });
    await client.embeddings.create('hi');
    expect(calls[0]!.body).toMatchObject({ dimensions: 2 });
    expect(calls[1]!.body).not.toHaveProperty('dimensions');
    expect(embeddingRequestToBody({ input: 'x', dimensions: 8 }).dimensions).toBe(8);
  });
});
