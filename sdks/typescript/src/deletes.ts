/**
 * Deletes — `DELETE /v1/claims/{id}`, `DELETE /v1/evidence/{id}` and
 * `DELETE /v1/tenants/{id}` on the ingestion service.
 *
 * DASH runs two HTTP services: retrieval (embeddings, retrieve; default
 * port 8080) and ingestion (writes and deletes; default port 8081). Every
 * delete is idempotent: the server answers 200 with `deleted: false` when
 * the target does not exist (including a claim of another tenant).
 */

/** Response body of every delete route. */
export interface DeleteResponse {
  /** `true` when something was removed. */
  deleted: boolean;
  /** `claim`, `evidence` or `tenant`. */
  scope: 'claim' | 'evidence' | 'tenant';
  tenant_id: string;
  claim_id?: string;
  evidence_id?: string;
  claims_deleted: number;
  evidence_deleted: number;
  edges_deleted: number;
  vectors_deleted: number;
  /** Claims left on the ingestion node after the delete. */
  claims_total: number;
  checkpoint_triggered: boolean;
  checkpoint_deferred: boolean;
}

/** Options accepted by every delete method. */
export interface DeleteOptions {
  /** Per-request timeout in milliseconds. Overrides the client default. */
  timeoutMs?: number;
  /** AbortSignal to chain for cancellation. */
  signal?: AbortSignal;
}

/**
 * Map a retrieval URL on port 8080 to the same host on port 8081 (the
 * conventional local layout). Any other URL yields `undefined`.
 */
export function deriveIngestionUrl(baseUrl: string): string | undefined {
  let url: URL;
  try {
    url = new URL(baseUrl);
  } catch {
    return undefined;
  }
  if (url.port !== '8080') {
    return undefined;
  }
  url.port = '8081';
  url.search = '';
  url.hash = '';
  return url.toString().replace(/\/$/, '');
}

function segment(name: string, value: string): string {
  if (typeof value !== 'string' || value.trim().length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
  return encodeURIComponent(value);
}

/** `/v1/claims/{claim_id}?tenant_id=...` with both ids escaped. */
export function claimDeletePath(tenantId: string, claimId: string): string {
  return `/v1/claims/${segment('claimId', claimId)}?tenant_id=${segment('tenantId', tenantId)}`;
}

/** `/v1/evidence/{evidence_id}?tenant_id=...` with both ids escaped. */
export function evidenceDeletePath(tenantId: string, evidenceId: string): string {
  return `/v1/evidence/${segment('evidenceId', evidenceId)}?tenant_id=${segment('tenantId', tenantId)}`;
}

/** `/v1/tenants/{tenant_id}` with the id escaped. */
export function tenantDeletePath(tenantId: string): string {
  return `/v1/tenants/${segment('tenantId', tenantId)}`;
}

/** Validate the wire shape of a delete response. */
export function parseDeleteResponse(raw: unknown): DeleteResponse {
  if (raw === null || typeof raw !== 'object' || Array.isArray(raw)) {
    throw new Error('DASH returned a non-object body for a delete');
  }
  const o = raw as Record<string, unknown>;
  if (typeof o.deleted !== 'boolean' || typeof o.scope !== 'string' || typeof o.tenant_id !== 'string') {
    throw new Error('DASH delete response is missing deleted, scope or tenant_id');
  }
  const num = (v: unknown): number => (typeof v === 'number' ? v : 0);
  const out: DeleteResponse = {
    deleted: o.deleted,
    scope: o.scope as DeleteResponse['scope'],
    tenant_id: o.tenant_id,
    claims_deleted: num(o.claims_deleted),
    evidence_deleted: num(o.evidence_deleted),
    edges_deleted: num(o.edges_deleted),
    vectors_deleted: num(o.vectors_deleted),
    claims_total: num(o.claims_total),
    checkpoint_triggered: o.checkpoint_triggered === true,
    checkpoint_deferred: o.checkpoint_deferred === true,
  };
  if (typeof o.claim_id === 'string') out.claim_id = o.claim_id;
  if (typeof o.evidence_id === 'string') out.evidence_id = o.evidence_id;
  return out;
}

export const MISSING_INGESTION_URL =
  'the delete methods call the ingestion service: set ingestionBaseUrl ' +
  '(it is only derived automatically when baseUrl uses port 8080)';
