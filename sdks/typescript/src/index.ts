/**
 * Public surface of the `dash-ts` package.
 *
 * Quickstart:
 *
 *     import { createClient, DashAPIError, DashConnectionError } from 'dash-ts';
 *
 *     const client = createClient({ baseUrl: 'http://localhost:8080' });
 *     try {
 *       const response = await client.embeddings.create('hello world');
 *       console.log(response.data[0].embedding.slice(0, 3));
 *     } catch (err) {
 *       if (err instanceof DashAPIError) {
 *         // Non-2xx response; statusCode / errorType / body available.
 *       } else if (err instanceof DashConnectionError) {
 *         // Network failure; .cause has the underlying fetch error.
 *       } else {
 *         throw err;
 *       }
 *     }
 */

export { DashClient, createClient, DEFAULT_TIMEOUT_MS } from './client.js';
export type { ClientOptions } from './client.js';

export { EmbeddingsService } from './embeddings.js';
export type { CreateEmbeddingsOptions } from './embeddings.js';

export { RetrieveService } from './retrieve.js';
export type { QueryOptions } from './retrieve.js';

export { DashError, DashAPIError, DashConnectionError } from './errors.js';
export type { DashAPIErrorType } from './errors.js';

export {
  embeddingRequestToBody,
  parseEmbeddingResponse,
  parseRetrieveResponse,
  retrieveRequestToBody,
} from './types.js';
export type {
  Citation,
  EmbeddingData,
  EmbeddingRequest,
  EmbeddingResponse,
  EmbeddingUsage,
  EncodingFormat,
  RetrieveRequest,
  RetrieveResponse,
  RetrieveResult,
  Stance,
  StanceMode,
} from './types.js';

export { USER_AGENT, request, defaultHeaders } from './transport.js';
export type { RequestOptions } from './transport.js';

export { openAIBaseURL, DASH_TS_VERSION } from './openai-compat.js';

/** The package version. */
export const VERSION = '0.1.0';
