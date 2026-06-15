# DASH Threat Model

This document describes DASH's threat model using the STRIDE framework. It is
a living artifact: it is updated whenever the system architecture changes
materially (new trust boundary, new persistent store, new network egress), and
it is reviewed as part of the security sign-off gate before each release.

## 1. System Overview

DASH is a multi-tenant vector search service. At the version this model was
authored against, the runtime consists of two user-facing services and one
embedded data store:

| Component        | Role                                                              | Persistence                          |
|------------------|-------------------------------------------------------------------|--------------------------------------|
| `ingestion-svc`  | gRPC + HTTP/JSON API. Accepts upsert/delete, persists vectors.    | `redb` (vectors), `redb` (audit log) |
| `retrieval-svc`  | gRPC + HTTP/JSON API. Serves k-NN and metadata queries.           | read-only views into `redb`          |
| `redb` (embedded)| Embedded key-value store backing both services, on local disk.    | local file (`dash.redb`)             |

A single binary may run both services in-process for local development, but in
production the deployment topology is:

```
                ┌────────────────────┐
   client ───▶  │   ingestion-svc    │  ──▶  redb (writers)
                └────────────────────┘
                ┌────────────────────┐
   client ───▶  │   retrieval-svc    │  ──▶  redb (readers)
                └────────────────────┘
                          │
                          ▼
                     local disk
```

A single multi-tenant `redb` file is shared by both services; tenant isolation
is enforced by the application layer (tenant-keyed tables, request-scoped
tenant_id checks), not by separate files.

## 2. Trust Boundaries

The following boundaries exist. Each is annotated with the trust level on each
side.

| # | Boundary                                                    | Untrusted side        | Trusted side          |
|---|-------------------------------------------------------------|-----------------------|-----------------------|
| 1 | Internet → ingestion-svc / retrieval-svc                   | external client       | service process       |
| 2 | Service process → `redb`                                   | service code          | embedded DB           |
| 3 | Service process → local disk                               | file system driver    | service process       |
| 4 | Service A → Service B (in-process share)                   | service A's request    | service B's data      |
| 5 | Operator → service process (admin API, SSH, redeploy)      | operator workstation  | service process       |
| 6 | CI/CD pipeline → registry (container images)               | CI runner             | release artifact      |

Crossing a trust boundary is the only place we explicitly validate input,
authenticate the caller, or assert authorization. All other code is treated as
trusted-but-defensive.

## 3. STRIDE Analysis

For each STRIDE category we list the threats we have considered, our
mitigations, and the residual risk we accept. "Mitigations" references the
specific code or process; the references are deliberately short so the model
stays readable.

### 3.1 Spoofing

| Threat                                                 | Mitigation                                                                                          | Residual |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------|----------|
| **JWT forgery.** Attacker forges a JWT signed with a guessed or stolen key and reads another tenant's vectors. | HS256 signing with a server-side secret loaded from the operator's secret store. Secret is rotated on a documented cadence; rotation is exercised by the security sign-off gate. Per-tenant signing keys are supported behind a feature flag and used for any tenant that requests them. | If the secret leaks (operator compromise, debug log), forgery becomes possible until rotation. We accept this and rely on rotation + alerting. |
| **Tenant impersonation.** Authenticated client for `tenant=A` presents a request body referencing `tenant=B`. | `tenant_id` is taken **only** from the verified JWT claim, never from the request body or query string. A request-body `tenant_id` is ignored and a warning is emitted. | If a future regression removes the assertion, impersonation is silent. We mitigate via integration tests (see §3.5). |

### 3.2 Tampering

| Threat                                                 | Mitigation                                                                                          | Residual |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------|----------|
| **Audit log modification.** Attacker with disk access rewrites audit entries to hide their actions. | The audit log is a **hash-chained append-only** structure: each entry contains `prev_hash` and `entry_hash = SHA-256(canonical(entry) ‖ prev_hash)`. Any modification breaks the chain, detected by `scripts/verify_audit_chain.sh` and the security sign-off gate. The chain is also embedded in the `redb` WAL; WAL entries are HMAC-signed with a separate `audit-hmac` key, so an attacker must forge both the chain and the WAL signature. | An attacker with root on the host can rewrite both. We accept this and rely on host hardening + off-host log shipping for forensic recovery. |
| **Vector payload tampering on disk.** | `redb` is opened with its built-in crash-safety, but not encrypted at rest (see ADR-006 — a deliberate trade-off, see Residual Risks §4). The deployment guide requires the underlying volume to be on an encrypted block device (LUKS / cloud-provider managed encryption). | Operator configuration drift. Detected by the security checklist and the deployment audit. |
| **WAL entry forgery on restart.** | WAL entries are HMAC-signed; replay is rejected on startup if the signature does not match. | If the HMAC key is lost, the WAL cannot be replayed. This is intentional and is documented in the runbook. |

### 3.3 Repudiation

| Threat                                                 | Mitigation                                                                                          | Residual |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------|----------|
| **User denies performing an action.** Operator cannot prove which tenant issued a write. | Every state-changing request produces an audit entry containing the verified `tenant_id`, the principal's JWT `jti`, the request hash, the response hash, and the server-side timestamp. The entry is hash-chained (see §3.2), so any retroactive edit is detectable. | A user can still deny a request that is not in scope of the audit log (e.g. health checks). We log only meaningful events and document the scope. |
| **Per-tenant audit retention.** A tenant disputes an action taken 18 months ago. | Per-tenant retention is configurable; the default is 365 days. The retention is enforced by a periodic compaction job and recorded as an audit event of its own. | If a tenant's retention is reduced without operator awareness, evidence is lost. The compaction job requires an explicit operator approval and emits its own audit event. |

### 3.4 Information Disclosure

| Threat                                                 | Mitigation                                                                                          | Residual |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------|----------|
| **Cross-tenant data leak via shared `redb`.** A bug allows a request for `tenant=A` to read `tenant=B`'s vectors. | All tables are **tenant-keyed**: the primary key is a composite of `(tenant_id, vector_id)`. Every read path is required to call `assert_tenant(request, record)` and returns 404 (not 403) on mismatch to avoid tenant existence enumeration. Tenant-keyed tables are exercised by a dedicated integration test suite (`tests/tenant_isolation.rs`) that runs on every PR. | A new code path that bypasses the assertion is the dominant residual risk. The test suite is the primary defense; we plan to add a property-based test in Future Work §5. |
| **Verbose error messages leaking internals.** A panic in a query path returns a stack trace to the client. | Errors are mapped to a stable, opaque set of public error codes; the raw cause is logged server-side with the request id. A regression test asserts that no error response contains the substring `"redb"`, the on-disk path, or a hex-encoded key. | None accepted; this is enforced in CI. |
| **Side channel via timing.** Different tenant responses take different amounts of time, allowing existence probes. | Query path is constant-time with respect to tenant identity for the cases we have measured. We have not done a full side-channel audit; see Future Work §5. | Accepted pending a side-channel audit. |

### 3.5 Denial of Service

| Threat                                                 | Mitigation                                                                                          | Residual |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------|----------|
| **Memory exhaustion via huge vectors or huge batches.** A client uploads a 1 GB vector or a 100k batch. | Per-request payload limits (`--max-vector-bytes`, `--max-batch-vectors`) are enforced at the gRPC and HTTP layers. The limits default to conservative values and are documented. Limits are validated as part of the security sign-off gate. | A malicious operator can raise the limits. This is accepted; limits are an operator decision, not a security control. |
| **Slow-loris / connection starvation.** A client opens many idle connections. | All ingress listeners enforce a `request_timeout` and a `keep-alive` budget. Idle connections past the budget are closed. Per-IP connection caps are configurable and default on. | Network-layer DoS is **out of scope** (see `SECURITY.md`). |
| **Expensive query that blocks the worker pool.** A client issues a brute-force k-NN scan. | k-NN is index-bound; the request rate is rate-limited per tenant (`--rate-limit-rps`); the worker pool is bounded and queues have explicit depth limits. | A tenant with a legitimate high-RPS workload can be rate-limited. The default is generous and operators can tune. |

### 3.6 Elevation of Privilege

| Threat                                                 | Mitigation                                                                                          | Residual |
|--------------------------------------------------------|-----------------------------------------------------------------------------------------------------|----------|
| **Tenant A reads Tenant B's vectors.** (A specific instance of §3.4, called out here because it is a privilege boundary, not a confidentiality boundary.) | The composite primary key makes a foreign-tenant read impossible at the data layer. **In addition**, every code path that returns a record is required to call `assert_tenant(request, record)` and the call is the subject of an integration test (`tests/tenant_isolation.rs::cross_tenant_read_is_rejected`). The test fails the build on regression. | See §3.4 — we treat this as a single threat with two independent mitigations. |
| **Local privilege escalation to bypass tenant check.** An attacker who can write to the service binary modifies it to skip the tenant check. | Service binary is deployed from a signed container image (see `docs/supply-chain.md`); the host's package manager verifies the signature on update. Modifications between deploys are detected by the host's file-integrity monitoring (operator responsibility, documented in the runbook). | The host itself must be hardened; this is out of scope for the service code. |

## 4. Residual Risks

The following risks remain after the mitigations above. Each is accepted
**with** a justification and an owner; the security sign-off gate refuses to
pass if a residual-risk row is missing an owner.

1. **No encryption at rest inside `redb`.** Decided in ADR-006. The mitigation
   is "encrypt the volume underneath the file", not "encrypt the file
   itself". Owner: platform team. Tracked in the security checklist.
2. **HS256 JWTs.** Switching to RS256/EdDSA is planned but not yet shipped
   (tracked in the roadmap). HS256 with a rotated secret is acceptable for
   the current threat model because the verifier and signer are the same
   process; there is no federated identity yet.
3. **No full side-channel audit.** Timing differences across tenant
   boundaries have not been measured by a third party. Owner: security
   working group.
4. **In-process trust between ingestion and retrieval.** When run in the
   same process, the two services share memory. A memory-corruption bug
   in one can read the other's data. Owner: runtime team — process
   separation is the long-term plan.
5. **Operator compromise.** We assume the operator's secret store is
   competent. A compromised operator is the dominant residual risk for
   any self-hosted service; we mitigate with audit, alerting, and
   documented runbooks, not with code.

## 5. Future Work

- **Property-based tenant isolation tests.** `quickcheck`/`proptest`
  harness that asserts no path in the read or write API can return a
  record whose `tenant_id` differs from the caller's.
- **Side-channel audit.** Engage an external reviewer to measure
  per-tenant response-time distributions.
- **Process separation by default.** Run ingestion and retrieval as
  separate processes with separate `redb` files (one writer, one reader)
  and IPC for control-plane events.
- **Migrate JWTs to RS256/EdDSA.** Tracked in the roadmap. Eliminates the
  shared-secret class of attack once we federate identity.
- **Audit log external anchoring.** Periodically publish the head of the
  hash chain to a transparency log (Sigstore Rekor or similar) so that
  off-host tampering of the WAL becomes detectable.
- **Threat model for the control plane.** Once we have a control plane
  (admin API, RBAC, multi-region replication), this document is split and
  a separate model is authored for the control plane.
