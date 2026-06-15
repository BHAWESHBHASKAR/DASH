/**
 * The DASH TypeScript client.
 *
 * Mirrors the layout of the official `openai` SDK
 * (`client.embeddings.create(...)`) so that switching from OpenAI
 * to DASH is a one-line import change. The native `/v1/retrieve`
 * endpoint lives at `client.retrieve.query(...)`.
 *
 * Example:
 *
 *     import { createClient } from 'dash-ts';
 *
 *     const client = createClient({ baseUrl: 'http://localhost:8080' });
 *     const response = await client.embeddings.create('hello world');
 *     console.log(response.data[0].embedding.slice(0, 3));
 */

import { EmbeddingsService } from './embeddings.js';
import { RetrieveService } from './retrieve.js';

/** Default per-request timeout, in milliseconds. */
export const DEFAULT_TIMEOUT_MS = 30_000;

/** Options for constructing a {@link DashClient}. */
export interface ClientOptions {
  /**
   * Root URL of the DASH service, e.g. `"http://localhost:8080"`.
   * The trailing slash is optional; the client normalises it.
   */
  baseUrl: string;
  /**
   * Optional bearer token. When set, sent as
   * `Authorization: Bearer <api_key>`. When omitted, no auth
   * header is added. Convenient for local DASH instances with
   * auth disabled.
   */
  apiKey?: string;
  /**
   * Default per-request timeout in milliseconds. Individual calls
   * can override it with their own `timeoutMs` option. Defaults
   * to {@link DEFAULT_TIMEOUT_MS}.
   */
  timeoutMs?: number;
  /**
   * Override `fetch` (e.g. for tests or a custom proxy). Defaults
   * to the global `fetch` available in Node 18+ and modern browsers.
   */
  fetch?: typeof fetch;
  /**
   * Extra headers to send on every request. Useful for
   * `X-Tenant-Id`, tracing IDs, etc. They merge on top of the
   * auth and default headers.
   */
  defaultHeaders?: Record<string, string>;
}

/**
 * The DASH client. Holds configuration and exposes the
 * `embeddings` and `retrieve` namespaces.
 */
export class DashClient {
  /** Root URL of the DASH service, with any trailing `/` stripped. */
  readonly baseUrl: string;
  /** Bearer token, if configured. */
  readonly apiKey?: string;
  /** Default per-request timeout, in milliseconds. */
  readonly timeoutMs: number;
  /** The `embeddings` namespace. */
  readonly embeddings: EmbeddingsService;
  /** The `retrieve` namespace. */
  readonly retrieve: RetrieveService;

  /** The custom `fetch` implementation, if provided. */
  readonly _fetchImpl?: typeof fetch;
  /** User-supplied default headers. */
  private readonly _userHeaders?: Record<string, string>;

  constructor(options: ClientOptions) {
    if (!options.baseUrl || options.baseUrl.length === 0) {
      throw new Error('baseUrl is required');
    }
    if (options.timeoutMs !== undefined && options.timeoutMs <= 0) {
      throw new Error('timeoutMs must be positive');
    }

    this.baseUrl = options.baseUrl.replace(/\/+$/, '');
    this.apiKey = options.apiKey;
    this.timeoutMs = options.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    this._fetchImpl = options.fetch;
    this._userHeaders = options.defaultHeaders
      ? { ...options.defaultHeaders }
      : undefined;
    this.embeddings = new EmbeddingsService(this);
    this.retrieve = new RetrieveService(this);
  }

  /**
   * Build the auth + user-headers map used on every request.
   *
   * Internal so service classes can pick it up; not part of the
   * public surface.
   */
  _authHeaders(): Record<string, string> {
    const headers: Record<string, string> = {};
    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }
    if (this._userHeaders) {
      Object.assign(headers, this._userHeaders);
    }
    return headers;
  }

  /**
   * Resolve a service path against {@link baseUrl}, tolerating
   * either a bare DASH root (`http://localhost:8080`) or an
   * OpenAI-style base URL that already ends in `/v1`
   * (`http://localhost:8080/v1`).
   *
   * Internal — service classes call this to compute the absolute
   * request URL.
   */
  _resolvePath(path: string): string {
    const trimmedBase = this.baseUrl.replace(/\/+$/, '');
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;
    if (trimmedBase.endsWith('/v1') && normalizedPath.startsWith('/v1/')) {
      return `${trimmedBase}${normalizedPath.slice(3)}`;
    }
    return `${trimmedBase}${normalizedPath}`;
  }
}

/**
 * Convenience factory: equivalent to `new DashClient(options)`.
 */
export function createClient(options: ClientOptions): DashClient {
  return new DashClient(options);
}
