/**
 * Low-level HTTP transport for the DASH client.
 *
 * Wraps the global `fetch` (Node 18+ or the browser) and turns
 * non-2xx responses and transport-level failures into the
 * {@link DashError} / {@link DashAPIError} / {@link DashConnectionError}
 * hierarchy. All higher-level service code (`EmbeddingsService`,
 * `RetrieveService`) goes through this module.
 *
 * The transport is intentionally a thin layer of pure functions;
 * the `DashClient` holds the configuration (base URL, headers,
 * timeout) and passes them in.
 */

import { DashAPIError, DashConnectionError } from './errors.js';

/** The default per-request timeout, in milliseconds. */
export const DEFAULT_TIMEOUT_MS = 30_000;

/** User agent reported to the server. */
export const USER_AGENT = 'dash-ts/0.1.0';

/** Default headers sent on every request. */
export function defaultHeaders(): Record<string, string> {
  return {
    'User-Agent': USER_AGENT,
    Accept: 'application/json',
  };
}

/** Configuration for a single {@link request} call. */
export interface RequestOptions {
  /** HTTP method (uppercase). */
  method: string;
  /** Absolute URL. */
  url: string;
  /** Optional JSON body. Sent as a UTF-8 string. */
  jsonBody?: unknown;
  /** Per-request timeout in milliseconds. Overrides the client default. */
  timeoutMs?: number;
  /** Headers to merge on top of the defaults and auth header. */
  headers?: Record<string, string>;
  /** `fetch` implementation to use. Defaults to the global one. */
  fetchImpl?: typeof fetch;
  /** AbortSignal to chain to the request (e.g. for cancellation). */
  signal?: AbortSignal;
}

/**
 * Issue an HTTP request and return the parsed JSON body.
 *
 * @throws {@link DashConnectionError} on transport-level failure
 *         (DNS, refused, timeout, abort).
 * @throws {@link DashAPIError} on a non-2xx response.
 */
export async function request<T = unknown>(options: RequestOptions): Promise<T> {
  const fetchImpl = options.fetchImpl ?? globalThis.fetch;
  if (typeof fetchImpl !== 'function') {
    throw new DashConnectionError(
      'no fetch implementation is available; pass one via ClientOptions.fetch or use Node 18+',
    );
  }

  const headers: Record<string, string> = {
    ...defaultHeaders(),
    ...(options.headers ?? {}),
  };
  if (options.jsonBody !== undefined) {
    headers['Content-Type'] = 'application/json';
  }

  const init: RequestInit = {
    method: options.method,
    headers,
  };

  if (options.jsonBody !== undefined) {
    init.body =
      typeof options.jsonBody === 'string'
        ? (options.jsonBody as string)
        : JSON.stringify(options.jsonBody);
  }

  // Compose the abort signals: the caller's signal plus our own
  // timeout-derived signal, so callers can cancel and we still
  // honour the configured timeout.
  const { signal, clearTimer } = buildAbortSignal(options.signal, options.timeoutMs);
  if (signal) {
    init.signal = signal;
  }

  let response: Response;
  try {
    response = await fetchImpl(options.url, init);
  } catch (err) {
    const e = err as Error;
    const isAbort =
      (typeof DOMException !== 'undefined' && e instanceof DOMException && e.name === 'AbortError') ||
      e.name === 'AbortError' ||
      e.name === 'TimeoutError';
    const message = isAbort
      ? `request to DASH timed out after ${options.timeoutMs ?? 0}ms: ${e.message}`
      : `failed to connect to DASH at ${options.url}: ${e.message}`;
    throw new DashConnectionError(message, { cause: e });
  } finally {
    clearTimer();
  }

  return parseResponse<T>(response);
}

/**
 * Build a combined AbortSignal + a cleanup callback for any
 * timeout timer. Returns `undefined` for the signal when no
 * timeout and no caller signal were provided.
 */
function buildAbortSignal(
  callerSignal: AbortSignal | undefined,
  timeoutMs: number | undefined,
): { signal: AbortSignal | undefined; clearTimer: () => void } {
  const signals: AbortSignal[] = [];
  let timer: ReturnType<typeof setTimeout> | undefined;

  if (callerSignal) {
    signals.push(callerSignal);
  }
  if (typeof timeoutMs === 'number' && timeoutMs > 0) {
    const ctl = new AbortController();
    timer = setTimeout(() => ctl.abort(), timeoutMs);
    signals.push(ctl.signal);
  }

  let signal: AbortSignal | undefined;
  if (signals.length === 1) {
    signal = signals[0];
  } else if (signals.length > 1) {
    signal = combineSignals(signals);
  }

  return {
    signal,
    clearTimer: () => {
      if (timer !== undefined) {
        clearTimeout(timer);
      }
    },
  };
}

/**
 * Parse a `Response` into a JSON value, raising a
 * {@link DashAPIError} on non-2xx.
 */
async function parseResponse<T>(response: Response): Promise<T> {
  // We always need the raw text for the error path, so read once
  // and JSON-parse opportunistically.
  const rawText = await response.text();
  let parsed: unknown;
  if (rawText.length > 0) {
    try {
      parsed = JSON.parse(rawText);
    } catch {
      parsed = undefined;
    }
  }

  if (response.status >= 200 && response.status < 300) {
    return parsed as T;
  }

  throw DashAPIError.fromResponse(response.status, rawText, parsed);
}

/**
 * Combine multiple `AbortSignal`s into one. Any abort fires the
 * resulting signal.
 *
 * Uses `AbortSignal.any` when available (Node 20.3+), falling back
 * to a manual implementation that subscribes to each input signal.
 */
function combineSignals(signals: AbortSignal[]): AbortSignal {
  type AbortSignalAnyCtor = new (sigs: AbortSignal[]) => AbortSignal;
  const ctor = (AbortSignal as unknown as { any?: AbortSignalAnyCtor }).any;
  if (typeof ctor === 'function') {
    return new ctor(signals);
  }
  const ctl = new AbortController();
  for (const s of signals) {
    if (s.aborted) {
      ctl.abort();
      break;
    }
    s.addEventListener(
      'abort',
      () => ctl.abort(),
      { once: true },
    );
  }
  return ctl.signal;
}
