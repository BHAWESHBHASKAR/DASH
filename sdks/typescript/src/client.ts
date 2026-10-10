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

import {
  MISSING_INGESTION_URL,
  claimDeletePath,
  deriveIngestionUrl,
  evidenceDeletePath,
  parseDeleteResponse,
  tenantDeletePath,
  type DeleteOptions,
  type DeleteResponse,
} from './deletes.js';
import { EmbeddingsService } from './embeddings.js';
import { DashError } from './errors.js';
import { RetrieveService } from './retrieve.js';
import { request as sendRequest, type RequestOptions } from './transport.js';
import { trimTrailingSlashes } from './url.js';

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
   * Root URL of the ingestion service, used by the delete methods. When
   * omitted it is derived only for the conventional local layout
   * (`baseUrl` on port 8080 maps to the same host on port 8081).
   */
  ingestionBaseUrl?: string;
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
  /** Root URL of the ingestion service (deletes), if known. */
  readonly ingestionBaseUrl?: string;
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

    this.baseUrl = trimTrailingSlashes(options.baseUrl);
    this.ingestionBaseUrl = options.ingestionBaseUrl
      ? trimTrailingSlashes(options.ingestionBaseUrl)
      : deriveIngestionUrl(this.baseUrl);
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
   * Call `DELETE /v1/claims/{claimId}?tenant_id=...`: remove the claim
   * with its vector, evidence and every edge from or to it. Needs the
   * `ingest` role. Idempotent (`deleted: false` when absent).
   */
  async deleteClaim(
    tenantId: string,
    claimId: string,
    options: DeleteOptions = {},
  ): Promise<DeleteResponse> {
    return this._delete(claimDeletePath(tenantId, claimId), options);
  }

  /**
   * Call `DELETE /v1/evidence/{evidenceId}?tenant_id=...`: remove every
   * evidence row with this id on the tenant's claims. Needs `ingest`.
   */
  async deleteEvidence(
    tenantId: string,
    evidenceId: string,
    options: DeleteOptions = {},
  ): Promise<DeleteResponse> {
    return this._delete(evidenceDeletePath(tenantId, evidenceId), options);
  }

  /**
   * Call `DELETE /v1/tenants/{tenantId}`: erase all of the tenant's
   * data. Needs the `admin` role for that tenant.
   */
  async deleteTenant(tenantId: string, options: DeleteOptions = {}): Promise<DeleteResponse> {
    return this._delete(tenantDeletePath(tenantId), options);
  }

  private async _delete(path: string, options: DeleteOptions): Promise<DeleteResponse> {
    if (!this.ingestionBaseUrl) {
      throw new DashError(MISSING_INGESTION_URL, 0, 'configuration_error', '');
    }
    const opts: RequestOptions = {
      method: 'DELETE',
      url: `${this.ingestionBaseUrl}${path}`,
      headers: this._authHeaders(),
      fetchImpl: this._fetchImpl,
      timeoutMs: options.timeoutMs ?? this.timeoutMs,
    };
    if (options.signal !== undefined) opts.signal = options.signal;
    return parseDeleteResponse(await sendRequest<unknown>(opts));
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
    const trimmedBase = trimTrailingSlashes(this.baseUrl);
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
