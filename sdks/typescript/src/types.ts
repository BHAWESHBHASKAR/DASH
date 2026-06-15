/**
 * Typed request and response models for the DASH retrieval engine.
 *
 * Mirrors the wire shapes documented in:
 *
 * - `services/retrieval/src/openai_embeddings.rs` for the
 *   OpenAI-compatible `/v1/embeddings` endpoint.
 * - `services/retrieval/tests/transport_http.rs` for the native
 *   `/v1/retrieve` endpoint.
 */

// ---------------------------------------------------------------------------
// OpenAI-compatible /v1/embeddings
// ---------------------------------------------------------------------------

/**
 * Encoding format for the returned embedding vectors.
 *
 * DASH currently only supports `"float"`. `"base64"` is accepted on
 * the wire by the OpenAI spec but rejected by DASH; the error is
 * surfaced as a {@link DashAPIError}.
 */
export type EncodingFormat = 'float' | 'base64';

/**
 * Request body for `POST /v1/embeddings`.
 *
 * Mirrors `OpenAIEmbeddingsRequest` in
 * `services/retrieval/src/openai_embeddings.rs`.
 */
export interface EmbeddingRequest {
  /** A single string or a list of strings to embed. */
  input: string | string[];
  /** Model name. Defaults to `"text-embedding-3-small"`. */
  model?: string;
  encoding_format?: EncodingFormat;
  user?: string;
}

/**
 * Single embedding record from a response.
 *
 * Mirrors `OpenAIEmbeddingData`.
 */
export interface EmbeddingData {
  object: 'embedding';
  embedding: number[];
  index: number;
}

/**
 * Token usage block from a response.
 *
 * Mirrors `OpenAIUsage`.
 */
export interface EmbeddingUsage {
  prompt_tokens: number;
  total_tokens: number;
}

/**
 * Response body for `POST /v1/embeddings`.
 *
 * Mirrors `OpenAIEmbeddingsResponse`.
 */
export interface EmbeddingResponse {
  object: 'list';
  data: EmbeddingData[];
  model: string;
  usage: EmbeddingUsage;
}

// ---------------------------------------------------------------------------
// Native /v1/retrieve
// ---------------------------------------------------------------------------

/**
 * Stance mode for the retrieve endpoint.
 *
 * - `"balanced"` (default): include every claim regardless of
 *   support/contradiction tally.
 * - `"support_only"`: server-side filter that drops claims whose
 *   contradiction tally exceeds their support tally.
 */
export type StanceMode = 'balanced' | 'support_only';

/** Stance recorded for a single citation. */
export type Stance = 'supports' | 'contradicts' | 'neutral';

/**
 * Request body for `POST /v1/retrieve`.
 *
 * Mirrors `schema::RetrievalRequest` and the test JSON in
 * `services/retrieval/tests/transport_http.rs`:
 *
 *     {"tenant_id": "...", "query": "...",
 *      "top_k": 10, "stance_mode": "balanced",
 *      "return_graph": false}
 */
export interface RetrieveRequest {
  /** Tenant namespace to search within. */
  tenant_id: string;
  /** Free-text query. */
  query: string;
  /** Maximum number of claims to return. Defaults to `10`. */
  top_k?: number;
  /** Defaults to `"balanced"`. */
  stance_mode?: StanceMode;
  /** Optional flag to also return the claim graph. */
  return_graph?: boolean;
}

/**
 * A citation attached to a retrieval result.
 *
 * Mirrors `schema::Citation`.
 */
export interface Citation {
  evidence_id: string;
  source_id: string;
  stance: Stance;
  source_quality: number;
  chunk_id?: string | null;
  span_start?: number | null;
  span_end?: number | null;
  doc_id?: string | null;
  extraction_model?: string | null;
  ingested_at?: number | null;
}

/**
 * A single claim returned by `/v1/retrieve`.
 *
 * Mirrors `schema::RetrievalResult`. The **Claim + Evidence +
 * Contradiction** differentiator lives here: `supports` and
 * `contradicts` give the caller the stance tally for the claim
 * without having to walk citations manually.
 */
export interface RetrieveResult {
  claim_id: string;
  canonical_text: string;
  score: number;
  supports: number;
  contradicts: number;
  citations: Citation[];
}

/**
 * Response body for `POST /v1/retrieve`.
 *
 * Wire format is `{"results": [...]}`.
 */
export interface RetrieveResponse {
  results: RetrieveResult[];
}

// ---------------------------------------------------------------------------
// Wire-format helpers
// ---------------------------------------------------------------------------

/**
 * Build the JSON body DASH expects for `POST /v1/embeddings`.
 *
 * Optional fields are omitted when not provided so the wire body
 * stays byte-for-byte compatible with the OpenAI spec.
 */
export function embeddingRequestToBody(req: EmbeddingRequest): Record<string, unknown> {
  const body: Record<string, unknown> = {
    input: req.input,
    model: req.model ?? 'text-embedding-3-small',
  };
  if (req.encoding_format !== undefined) {
    body.encoding_format = req.encoding_format;
  }
  if (req.user !== undefined) {
    body.user = req.user;
  }
  return body;
}

/**
 * Build the JSON body DASH expects for `POST /v1/retrieve`.
 */
export function retrieveRequestToBody(req: RetrieveRequest): Record<string, unknown> {
  const body: Record<string, unknown> = {
    tenant_id: req.tenant_id,
    query: req.query,
    top_k: req.top_k ?? 10,
    stance_mode: req.stance_mode ?? 'balanced',
  };
  if (req.return_graph !== undefined) {
    body.return_graph = req.return_graph;
  }
  return body;
}

/**
 * Parse the JSON body of a `/v1/embeddings` response.
 *
 * Tolerates missing `data`/`usage` (returns empty defaults) so the
 * client can produce a useful object even if DASH adds a new field
 * in a backwards-compatible way.
 */
export function parseEmbeddingResponse(raw: unknown): EmbeddingResponse {
  if (typeof raw !== 'object' || raw === null) {
    throw new TypeError('expected an object for /v1/embeddings response');
  }
  const body = raw as Record<string, unknown>;

  const dataRaw = Array.isArray(body.data) ? body.data : [];
  const data: EmbeddingData[] = dataRaw.map((d, fallbackIndex) => {
    const item = d as Record<string, unknown>;
    const embeddingRaw = Array.isArray(item.embedding) ? item.embedding : [];
    const embedding: number[] = embeddingRaw.map((v) =>
      typeof v === 'number' ? v : Number(v),
    );
    return {
      object: 'embedding',
      embedding,
      index: typeof item.index === 'number' ? item.index : fallbackIndex,
    };
  });

  const usageRaw =
    typeof body.usage === 'object' && body.usage !== null
      ? (body.usage as Record<string, unknown>)
      : {};
  const usage: EmbeddingUsage = {
    prompt_tokens: Number(usageRaw.prompt_tokens ?? 0),
    total_tokens: Number(usageRaw.total_tokens ?? 0),
  };

  return {
    object: 'list',
    data,
    model: typeof body.model === 'string' ? body.model : '',
    usage,
  };
}

/**
 * Parse the JSON body of a `/v1/retrieve` response.
 */
export function parseRetrieveResponse(raw: unknown): RetrieveResponse {
  if (typeof raw !== 'object' || raw === null) {
    throw new TypeError('expected an object for /v1/retrieve response');
  }
  const body = raw as Record<string, unknown>;
  const resultsRaw = Array.isArray(body.results) ? body.results : [];

  const results: RetrieveResult[] = resultsRaw.map((r) => {
    const item = r as Record<string, unknown>;
    const citationsRaw = Array.isArray(item.citations) ? item.citations : [];
    const citations: Citation[] = citationsRaw.map((c) => {
      const ci = c as Record<string, unknown>;
      return {
        evidence_id: String(ci.evidence_id ?? ''),
        source_id: String(ci.source_id ?? ''),
        stance: (ci.stance as Stance) ?? 'neutral',
        source_quality: Number(ci.source_quality ?? 0),
        chunk_id: (ci.chunk_id as string | null | undefined) ?? null,
        span_start:
          ci.span_start === null || ci.span_start === undefined
            ? null
            : Number(ci.span_start),
        span_end:
          ci.span_end === null || ci.span_end === undefined
            ? null
            : Number(ci.span_end),
        doc_id: (ci.doc_id as string | null | undefined) ?? null,
        extraction_model: (ci.extraction_model as string | null | undefined) ?? null,
        ingested_at:
          ci.ingested_at === null || ci.ingested_at === undefined
            ? null
            : Number(ci.ingested_at),
      };
    });

    return {
      claim_id: String(item.claim_id ?? ''),
      canonical_text: String(item.canonical_text ?? ''),
      score: Number(item.score ?? 0),
      supports: Number(item.supports ?? 0),
      contradicts: Number(item.contradicts ?? 0),
      citations,
    };
  });

  return { results };
}
