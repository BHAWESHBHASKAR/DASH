/**
 * Tests for the error hierarchy: {@link DashError},
 * {@link DashAPIError}, {@link DashConnectionError}.
 */

import { describe, expect, it } from 'vitest';

import { DashAPIError, DashConnectionError, DashError } from '../src/errors.js';
import {
  BASE_URL,
  OPENAI_STYLE_ERROR_BODY,
  makeFailingFetchMock,
  makeFetchMock,
} from './fixtures.js';
import { createClient } from '../src/client.js';

describe('DashError', () => {
  it('is an Error subclass', () => {
    const e = new DashError('boom', 500, 'server_error', 'body');
    expect(e).toBeInstanceOf(Error);
    expect(e).toBeInstanceOf(DashError);
    expect(e.name).toBe('DashError');
    expect(e.message).toBe('boom');
    expect(e.statusCode).toBe(500);
    expect(e.errorType).toBe('server_error');
    expect(e.body).toBe('body');
  });

  it('preserves the prototype chain so `instanceof` works across realms', () => {
    // This test guards the Object.setPrototypeOf dance: without it,
    // `e instanceof DashError` would silently start returning
    // `false` when targeting ES5.
    const e = new DashError('x', 400, 'invalid_request_error', '');
    expect(Object.getPrototypeOf(e)).toBe(DashError.prototype);
  });
});

describe('DashConnectionError', () => {
  it('is a DashError subclass and chains the cause', () => {
    const cause = new Error('ECONNREFUSED');
    const e = new DashConnectionError('failed to connect', { cause });
    expect(e).toBeInstanceOf(Error);
    expect(e).toBeInstanceOf(DashError);
    expect(e).toBeInstanceOf(DashConnectionError);
    expect(e.name).toBe('DashConnectionError');
    expect(e.cause).toBe(cause);
    expect(e.statusCode).toBe(0);
    expect(e.errorType).toBe('connection_error');
  });

  it('omits cause when not provided', () => {
    const e = new DashConnectionError('failed');
    expect(e.cause).toBeUndefined();
  });
});

describe('DashAPIError', () => {
  it('captures message, status, type, and body', () => {
    const e = new DashAPIError(
      'invalid input',
      400,
      'invalid_request_error',
      '{"error":{...}}',
    );
    expect(e).toBeInstanceOf(Error);
    expect(e).toBeInstanceOf(DashError);
    expect(e).toBeInstanceOf(DashAPIError);
    expect(e.name).toBe('DashAPIError');
    expect(e.statusCode).toBe(400);
    expect(e.errorType).toBe('invalid_request_error');
    expect(e.body).toBe('{"error":{...}}');
  });

  describe('fromResponse', () => {
    it('parses an OpenAI-shaped error envelope', () => {
      const e = DashAPIError.fromResponse(
        400,
        JSON.stringify(OPENAI_STYLE_ERROR_BODY),
        OPENAI_STYLE_ERROR_BODY,
      );
      expect(e.statusCode).toBe(400);
      expect(e.errorType).toBe('invalid_request_error');
      expect(e.message).toContain('input must contain at least one text');
    });

    it('parses a string error', () => {
      const e = DashAPIError.fromResponse(
        500,
        '"routing unavailable"',
        { error: 'routing unavailable' },
      );
      expect(e.errorType).toBe('api_error');
      expect(e.errorMessage).toBe('routing unavailable');
    });

    it('parses a top-level message', () => {
      const e = DashAPIError.fromResponse(
        500,
        '{"message":"oops"}',
        { message: 'oops' },
      );
      expect(e.errorType).toBe('api_error');
      expect(e.errorMessage).toBe('oops');
    });

    it('falls back to the raw body for non-JSON responses', () => {
      const e = DashAPIError.fromResponse(502, 'Bad Gateway', undefined);
      expect(e.errorType).toBe('api_error');
      expect(e.errorMessage).toBe('Bad Gateway');
    });

    it('falls back to "HTTP <code>" for an empty body', () => {
      const e = DashAPIError.fromResponse(500, '', undefined);
      expect(e.errorMessage).toBe('HTTP 500');
    });
  });
});

describe('end-to-end error mapping', () => {
  it('a 4xx response becomes a DashAPIError on embeddings', async () => {
    const { fetch: f } = makeFetchMock(400, OPENAI_STYLE_ERROR_BODY);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    try {
      await client.embeddings.create('hi');
      throw new Error('expected throw');
    } catch (err) {
      expect(err).toBeInstanceOf(DashError);
      expect(err).toBeInstanceOf(DashAPIError);
      expect((err as DashAPIError).errorMessage).toBe(
        'input must contain at least one text',
      );
    }
  });

  it('a fetch rejection becomes a DashConnectionError', async () => {
    const cause = new TypeError('fetch failed');
    const { fetch: f } = makeFailingFetchMock(cause);
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    try {
      await client.embeddings.create('hi');
      throw new Error('expected throw');
    } catch (err) {
      expect(err).toBeInstanceOf(DashError);
      expect(err).toBeInstanceOf(DashConnectionError);
      expect((err as DashConnectionError).cause).toBe(cause);
    }
  });

  it('the .message of a DashConnectionError mentions the URL', async () => {
    const { fetch: f } = makeFailingFetchMock(new Error('refused'));
    const client = createClient({ baseUrl: BASE_URL, fetch: f });
    try {
      await client.embeddings.create('hi');
      throw new Error('expected throw');
    } catch (err) {
      const msg = (err as Error).message;
      expect(msg).toContain(BASE_URL);
    }
  });
});
