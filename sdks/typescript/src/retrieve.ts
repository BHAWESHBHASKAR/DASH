/**
 * Retrieve service — wraps `POST /v1/retrieve`.
 *
 * Where DASH pulls ahead of a vector store is this endpoint: it
 * returns Claims with explicit `supports` and `contradicts` counts
 * and a structured `Citation[]` list per claim. That's the
 * substance you need to build a RAG pipeline that doesn't
 * confidently echo back the wrong thing.
 *
 * Example:
 *
 *     const response = await client.retrieve.query({
 *       tenant_id: 'acme-corp',
 *       query: 'what was the Q3 2024 revenue?',
 *       top_k: 5,
 *     });
 *     for (const r of response.results) {
 *       console.log(`[${r.score.toFixed(2)}] ${r.canonical_text}`);
 *     }
 */

import {
  parseRetrieveResponse,
  retrieveRequestToBody,
  type RetrieveRequest,
  type RetrieveResponse,
  type RetrieveResult,
} from './types.js';
import type { RequestOptions } from './transport.js';
import { request as sendRequest } from './transport.js';
import type { DashClient } from './client.js';

export interface QueryOptions {
  /** Per-request timeout in milliseconds. Overrides the client default. */
  timeoutMs?: number;
  /** AbortSignal to chain for cancellation. */
  signal?: AbortSignal;
}

/**
 * Namespace object exposed as `DashClient.retrieve`.
 */
export class RetrieveService {
  private readonly _client: DashClient;

  constructor(client: DashClient) {
    this._client = client;
  }

  /**
   * Call `POST /v1/retrieve` and return a typed response.
   *
   * @param req - The retrieve request body.
   * @param options - Optional per-request timeout / signal.
   */
  async query(
    req: RetrieveRequest,
    options: QueryOptions = {},
  ): Promise<RetrieveResponse> {
    const body = retrieveRequestToBody(req);
    const opts: RequestOptions = {
      method: 'POST',
      url: this._client._resolvePath('/v1/retrieve'),
      jsonBody: body,
      headers: this._client._authHeaders(),
      fetchImpl: this._client._fetchImpl,
    };
    if (options.timeoutMs !== undefined) opts.timeoutMs = options.timeoutMs;
    if (options.signal !== undefined) opts.signal = options.signal;

    const raw = await sendRequest<unknown>(opts);
    return parseRetrieveResponse(raw);
  }

  /**
   * Convenience: return just the top result, or `null` if no
   * results were returned.
   */
  async topResult(
    req: RetrieveRequest,
    options: QueryOptions = {},
  ): Promise<RetrieveResult | null> {
    const response = await this.query(req, options);
    if (response.results.length === 0) {
      return null;
    }
    return response.results[0]!;
  }
}
