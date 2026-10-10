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
  /**
   * Expected embedding size. The server rejects values that differ from
   * its provider's dimensionality.
   */
  dimensions?: number;
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

/** How many replicas must answer a read. Server default is `"one"`. */
export type ReadConsistency = 'one' | 'quorum' | 'all';

/** Inclusive unix-second window; either bound may be omitted. */
export interface TimeRange {
  from_unix?: number | null;
  to_unix?: number | null;
}

/**
 * Request body for `POST /v1/retrieve`.
 *
 * Mirrors the JSON accepted by `build_retrieve_transport_request_from_json`
 * in `services/retrieval/src/transport/payload.rs`. Only `tenant_id` and
 * `query` are required.
 */
export interface RetrieveRequest {
  /** Tenant namespace to search within. */
  tenant_id: string;
  /** Free-text query. */
  query: string;
  /** Maximum number of claims to return. Defaults to `5` (the server default). */
  top_k?: number;
  /** Defaults to `"balanced"`. */
  stance_mode?: StanceMode;
  /** Optional flag to also return the claim graph. */
  return_graph?: boolean;
  /** Pre-computed query vector (skips server-side embedding). */
  query_embedding?: number[];
  /** Restrict results to claims mentioning these entities. */
  entity_filters?: string[];
  /** Restrict results to these embedding ids. */
  embedding_id_filters?: string[];
  /** Restrict results by event time. */
  time_range?: TimeRange;
  /** Read consistency policy (server default `"one"`). */
  read_consistency?: ReadConsistency;
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
 * A single claim returned by `/v1/retrieve`
 * (`render_evidence_node_json` in the retrieval service).
 *
 * The **Claim + Evidence + Contradiction** differentiator lives here:
 * `supports` and `contradicts` give the caller the stance tally for the
 * claim without having to walk citations manually. Fields after
 * `citations` are optional and `null` when the server omits them.
 */
export interface RetrieveResult {
  claim_id: string;
  canonical_text: string;
  score: number;
  supports: number;
  contradicts: number;
  citations: Citation[];
  claim_confidence?: number | null;
  confidence_band?: string | null;
  dominant_stance?: string | null;
  contradiction_risk?: number | null;
  graph_score?: number | null;
  support_path_count?: number | null;
  contradiction_chain_depth?: number | null;
  event_time_unix?: number | null;
  temporal_match_mode?: string | null;
  temporal_in_range?: boolean | null;
  claim_type?: string | null;
  valid_from?: number | null;
  valid_to?: number | null;
  created_at?: number | null;
  updated_at?: number | null;
}

/** An edge of the evidence graph (`return_graph: true`). */
export interface GraphEdge {
  from_claim_id: string;
  to_claim_id: string;
  relation: string;
  strength: number;
}

/** Evidence graph returned when `return_graph` is true. */
export interface RetrieveGraph {
  nodes: RetrieveResult[];
  edges: GraphEdge[];
}

/**
 * Response body for `POST /v1/retrieve`.
 *
 * Wire format is `{"results": [...], "graph": ..., "read_policy": ...,
 * "read_quorum_met": ..., "serving_replica": ...}`; everything except
 * `results` is optional.
 */
export interface RetrieveResponse {
  results: RetrieveResult[];
  graph?: RetrieveGraph | null;
  read_policy?: string | null;
  read_quorum_met?: boolean | null;
  serving_replica?: string | null;
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
  if (req.dimensions !== undefined) {
    body.dimensions = req.dimensions;
  }
  return body;
}

/**
 * Decode the server's `encoding_format: "base64"` embedding: float32
 * components packed little-endian, standard base64 alphabet.
 *
 * @throws TypeError when the input is not valid base64 or not a whole
 *   number of float32 values.
 */
export function decodeBase64Embedding(encoded: string): number[] {
  let binary: string;
  try {
    binary = atob(encoded);
  } catch {
    throw new TypeError('embedding is not valid base64');
  }
  if (binary.length % 4 !== 0) {
    throw new TypeError(
      `base64 embedding has ${binary.length} bytes, not a multiple of 4`,
    );
  }
  const view = new DataView(new ArrayBuffer(binary.length));
  for (let i = 0; i < binary.length; i++) {
    view.setUint8(i, binary.charCodeAt(i));
  }
  const out: number[] = [];
  for (let offset = 0; offset < binary.length; offset += 4) {
    out.push(view.getFloat32(offset, true));
  }
  return out;
}

/**
 * Build the JSON body DASH expects for `POST /v1/retrieve`.
 */
export function retrieveRequestToBody(req: RetrieveRequest): Record<string, unknown> {
  const body: Record<string, unknown> = {
    tenant_id: req.tenant_id,
    query: req.query,
    top_k: req.top_k ?? 5,
    stance_mode: req.stance_mode ?? 'balanced',
  };
  if (req.return_graph !== undefined) {
    body.return_graph = req.return_graph;
  }
  if (req.query_embedding !== undefined) {
    body.query_embedding = req.query_embedding;
  }
  if (req.entity_filters !== undefined) {
    body.entity_filters = req.entity_filters;
  }
  if (req.embedding_id_filters !== undefined) {
    body.embedding_id_filters = req.embedding_id_filters;
  }
  if (req.time_range !== undefined) {
    const tr: Record<string, number> = {};
    if (req.time_range.from_unix != null) tr.from_unix = req.time_range.from_unix;
    if (req.time_range.to_unix != null) tr.to_unix = req.time_range.to_unix;
    body.time_range = tr;
  }
  if (req.read_consistency !== undefined) {
    body.read_consistency = req.read_consistency;
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
    // `encoding_format: "base64"` makes the server return a string.
    const embedding: number[] =
      typeof item.embedding === 'string'
        ? decodeBase64Embedding(item.embedding)
        : (Array.isArray(item.embedding) ? item.embedding : []).map((v) =>
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

  const results: RetrieveResult[] = resultsRaw.map((r) => parseRetrieveResult(r));

  const out: RetrieveResponse = { results };
  if (typeof body.graph === 'object' && body.graph !== null) {
    const g = body.graph as Record<string, unknown>;
    const nodes = Array.isArray(g.nodes) ? g.nodes.map((n) => parseRetrieveResult(n)) : [];
    const edges: GraphEdge[] = (Array.isArray(g.edges) ? g.edges : []).map((e) => {
      const ei = e as Record<string, unknown>;
      return {
        from_claim_id: String(ei.from_claim_id ?? ''),
        to_claim_id: String(ei.to_claim_id ?? ''),
        relation: String(ei.relation ?? ''),
        strength: Number(ei.strength ?? 0),
      };
    });
    out.graph = { nodes, edges };
  }
  if (typeof body.read_policy === 'string') out.read_policy = body.read_policy;
  if (typeof body.read_quorum_met === 'boolean') out.read_quorum_met = body.read_quorum_met;
  if (typeof body.serving_replica === 'string') out.serving_replica = body.serving_replica;
  return out;
}

function optNumber(v: unknown): number | null {
  return v === null || v === undefined ? null : Number(v);
}

function optString(v: unknown): string | null {
  return typeof v === 'string' ? v : null;
}

function parseRetrieveResult(r: unknown): RetrieveResult {
  const item = (r ?? {}) as Record<string, unknown>;
  const citationsRaw = Array.isArray(item.citations) ? item.citations : [];
  const citations: Citation[] = citationsRaw.map((c) => {
    const ci = c as Record<string, unknown>;
    return {
      evidence_id: String(ci.evidence_id ?? ''),
      source_id: String(ci.source_id ?? ''),
      stance: (ci.stance as Stance) ?? 'neutral',
      source_quality: Number(ci.source_quality ?? 0),
      chunk_id: (ci.chunk_id as string | null | undefined) ?? null,
      span_start: optNumber(ci.span_start),
      span_end: optNumber(ci.span_end),
      doc_id: (ci.doc_id as string | null | undefined) ?? null,
      extraction_model: (ci.extraction_model as string | null | undefined) ?? null,
      ingested_at: optNumber(ci.ingested_at),
    };
  });

  return {
    claim_id: String(item.claim_id ?? ''),
    canonical_text: String(item.canonical_text ?? ''),
    score: Number(item.score ?? 0),
    supports: Number(item.supports ?? 0),
    contradicts: Number(item.contradicts ?? 0),
    citations,
    claim_confidence: optNumber(item.claim_confidence),
    confidence_band: optString(item.confidence_band),
    dominant_stance: optString(item.dominant_stance),
    contradiction_risk: optNumber(item.contradiction_risk),
    graph_score: optNumber(item.graph_score),
    support_path_count: optNumber(item.support_path_count),
    contradiction_chain_depth: optNumber(item.contradiction_chain_depth),
    event_time_unix: optNumber(item.event_time_unix),
    temporal_match_mode: optString(item.temporal_match_mode),
    temporal_in_range: typeof item.temporal_in_range === 'boolean' ? item.temporal_in_range : null,
    claim_type: optString(item.claim_type),
    valid_from: optNumber(item.valid_from),
    valid_to: optNumber(item.valid_to),
    created_at: optNumber(item.created_at),
    updated_at: optNumber(item.updated_at),
  };
}
