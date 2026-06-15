/**
 * Exception hierarchy for the DASH TypeScript client.
 *
 * All public errors inherit from {@link DashError}, so callers can
 * catch the whole family with a single `catch (err)` clause that
 * narrows on `err instanceof DashError`, while still being able to
 * distinguish network failures from HTTP errors.
 */

/**
 * Error type strings returned in the OpenAI-shaped `error.type` field.
 *
 * DASH emits these for `/v1/embeddings` and uses `"api_error"` for
 * ad-hoc native endpoint errors.
 */
export type DashAPIErrorType =
  | 'invalid_request_error'
  | 'server_error'
  | 'api_error'
  | (string & {});

/**
 * Base class for every error raised by the DASH client.
 *
 * Catch this if you want to handle "anything went wrong talking to
 * DASH" without caring about the specific failure mode.
 */
export class DashError extends Error {
  readonly statusCode: number;
  readonly errorType: string;
  readonly body: string;

  constructor(
    message: string,
    statusCode: number,
    errorType: string,
    body: string,
  ) {
    super(message);
    this.name = 'DashError';
    this.statusCode = statusCode;
    this.errorType = errorType;
    this.body = body;
    // Preserve a clean prototype chain when targeting ES5
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Raised when the client cannot reach DASH at all.
 *
 * Examples: DNS resolution failure, connection refused, read timeout.
 * The underlying `TypeError`/`Error` from the global `fetch` (or
 * AbortController signal) is attached to `.cause` so it is still
 * inspectable from the caller.
 */
export class DashConnectionError extends DashError {
  readonly cause?: Error;

  constructor(message: string, options: { cause?: Error } = {}) {
    super(message, 0, 'connection_error', '');
    this.name = 'DashConnectionError';
    if (options.cause !== undefined) {
      this.cause = options.cause;
    }
    Object.setPrototypeOf(this, new.target.prototype);
  }
}

/**
 * Raised when DASH returns a non-2xx HTTP response.
 *
 * DASH's `/v1/embeddings` endpoint returns OpenAI-shaped errors:
 *
 *     {"error": {"message": "...", "type": "invalid_request_error",
 *                "param": null, "code": null}}
 *
 * The native `/v1/retrieve` endpoint uses a more ad-hoc shape, so
 * {@link DashAPIError.fromResponse} tolerates both forms (and falls
 * back to the raw body when no parseable `error` object is present).
 */
export class DashAPIError extends DashError {
  /** The user-facing message extracted from the response body. */
  readonly errorMessage: string;

  constructor(
    message: string,
    statusCode: number,
    errorType: DashAPIErrorType,
    body: string,
    errorMessage: string = message,
  ) {
    super(message, statusCode, errorType, body);
    this.name = 'DashAPIError';
    this.errorMessage = errorMessage;
    Object.setPrototypeOf(this, new.target.prototype);
  }

  /**
   * Build a {@link DashAPIError} from a raw HTTP response.
   *
   * `parsedBody` is the JSON-decoded body when available. When the
   * server returned non-JSON we fall back to the raw `rawBody`
   * string. The returned error always has a sensible message and
   * `errorType`, defaulting to `"api_error"` and a synthesized
   * message when the body cannot be interpreted.
   */
  static fromResponse(
    statusCode: number,
    rawBody: string,
    parsedBody?: unknown,
  ): DashAPIError {
    let errorType: DashAPIErrorType = 'api_error';
    let errorMessage: string | undefined;

    if (parsedBody && typeof parsedBody === 'object' && !Array.isArray(parsedBody)) {
      const err = (parsedBody as Record<string, unknown>).error;
      if (err && typeof err === 'object' && !Array.isArray(err)) {
        const e = err as Record<string, unknown>;
        const t = e.type ?? e.code;
        if (typeof t === 'string' && t.length > 0) {
          errorType = t as DashAPIErrorType;
        }
        if (typeof e.message === 'string') {
          errorMessage = e.message;
        }
      } else if (typeof err === 'string' && err.length > 0) {
        errorMessage = err;
        errorType = 'api_error';
      } else {
        const m = (parsedBody as Record<string, unknown>).message;
        if (typeof m === 'string' && m.length > 0) {
          // Some endpoints (e.g. /v1/retrieve) put the message
          // directly on the top-level object.
          errorMessage = m;
          errorType = 'api_error';
        }
      }
    }

    if (errorMessage === undefined) {
      errorMessage = rawBody && rawBody.length > 0 ? rawBody : `HTTP ${statusCode}`;
    }
    if (!errorType) {
      errorType = 'api_error';
    }

    const fullMessage = `DASH API error (${statusCode} ${errorType}): ${errorMessage}`;
    return new DashAPIError(
      fullMessage,
      statusCode,
      errorType,
      rawBody ?? '',
      errorMessage,
    );
  }
}
