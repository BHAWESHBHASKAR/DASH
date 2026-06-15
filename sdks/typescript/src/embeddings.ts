/**
 * Embeddings service — wraps `POST /v1/embeddings`.
 *
 * The wire format is byte-for-byte compatible with OpenAI's
 * `/v1/embeddings` endpoint, so any client that speaks the OpenAI
 * protocol (langchain, llama-index, the official `openai` SDK) can
 * point at DASH with no code changes.
 *
 * Example:
 *
 *     const client = createClient({ baseUrl: 'http://localhost:8080' });
 *     const response = await client.embeddings.create('hello world');
 *     console.log(response.data[0].embedding.slice(0, 3));
 */

import {
  embeddingRequestToBody,
  parseEmbeddingResponse,
  type EmbeddingRequest,
  type EmbeddingResponse,
} from './types.js';
import type { RequestOptions } from './transport.js';
import { request } from './transport.js';
import type { DashClient } from './client.js';

export interface CreateEmbeddingsOptions {
  /** Override the model. Default is `"text-embedding-3-small"`. */
  model?: string;
  /** Encoding format — DASH currently only supports `"float"`. */
  encoding_format?: EmbeddingRequest['encoding_format'];
  /** OpenAI-style opaque user identifier. */
  user?: string;
  /** Per-request timeout in milliseconds. Overrides the client default. */
  timeoutMs?: number;
  /** AbortSignal to chain for cancellation. */
  signal?: AbortSignal;
}

/**
 * Namespace object exposed as `DashClient.embeddings`.
 *
 * The shape matches `openai.resources.embeddings.Embeddings` so
 * users porting from OpenAI find the same method names.
 */
export class EmbeddingsService {
  private readonly _client: DashClient;

  constructor(client: DashClient) {
    this._client = client;
  }

  /**
   * Call `POST /v1/embeddings` and return a typed response.
   *
   * @param input - A single string or a list of strings to embed.
   * @param options - Optional model / encoding / user overrides.
   */
  async create(
    input: string | string[],
    options: CreateEmbeddingsOptions = {},
  ): Promise<EmbeddingResponse> {
    const requestBody: EmbeddingRequest = {
      input,
    };
    if (options.model !== undefined) requestBody.model = options.model;
    if (options.encoding_format !== undefined)
      requestBody.encoding_format = options.encoding_format;
    if (options.user !== undefined) requestBody.user = options.user;
    return this.createRaw(requestBody, {
      timeoutMs: options.timeoutMs,
      signal: options.signal,
    });
  }

  /**
   * Call `POST /v1/embeddings` with a fully-formed request body.
   *
   * Use this when you need fine-grained control over
   * `encoding_format` or `user` that the convenience method hides.
   */
  async createRaw(
    requestBody: EmbeddingRequest,
    options: { timeoutMs?: number; signal?: AbortSignal } = {},
  ): Promise<EmbeddingResponse> {
    const body = embeddingRequestToBody(requestBody);
    const opts: RequestOptions = {
      method: 'POST',
      url: this._client._resolvePath('/v1/embeddings'),
      jsonBody: body,
      headers: this._client._authHeaders(),
      fetchImpl: this._client._fetchImpl,
    };
    if (options.timeoutMs !== undefined) opts.timeoutMs = options.timeoutMs;
    if (options.signal !== undefined) opts.signal = options.signal;

    const raw = await request<unknown>(opts);
    return parseEmbeddingResponse(raw);
  }
}
