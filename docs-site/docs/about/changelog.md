# Changelog

The canonical changelog is [`CHANGELOG.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/CHANGELOG.md) in the repository root. This page summarizes it and does not duplicate the detail; an earlier copy here had drifted from the source and contained claims that were not true (for example that the ANN index was replaced by `usearch`).

DASH has no tagged release yet. The sections below follow the root changelog.

## 0.3.0 (unreleased): P0 production-readiness hardening

All of the code below is merged in the tree; nothing is tagged or published. Scope and exit criteria are in the [master plan](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-production-readiness-master-plan.md); what each area covers, with evidence and open items, is in the [P0 status register](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-p0-status.md). Moving from 0.2.x requires configuration changes: see the [upgrade guide](../operations/upgrading.md).

**Security (breaking)**

- Deny by default: services refuse to start without credentials unless `DASH_INSECURE_DEV_MODE=1` (loopback only). Strict secret validation is on by default (16 characters for keys and tokens, 32 for JWT secrets, placeholders rejected).
- Role hierarchy; a JWT without a roles claim gets no roles (403) unless `DASH_*_JWT_DEFAULT_ROLES` is set; unscoped API keys get `DASH_*_API_KEY_DEFAULT_ROLES`.
- `/v1/embeddings`, `/debug/*` and `/metrics` require authentication, and authentication runs before any embedding provider call.
- Replication requires `DASH_INGEST_REPLICATION_TOKEN`; the control plane requires `DASH_CONTROL_PLANE_TOKEN`.
- Per-tenant rate limiting is enforced (HTTP 429 with `Retry-After`).
- JWT and OIDC hardening (mandatory `exp`, lifetime cap, `jti` denylist, JWKS cache), SIGHUP reload, HTTPS for the OpenAI provider, one canonical audit-chain encoding with a shared verifier.
- Optional native TLS on every listener (TLS 1.2/1.3, ALPN `http/1.1`), client-certificate verification, replication and placement over mutual TLS with per-follower certificate pinning, certificate reload without restart; TLS variants of the Helm chart, kustomize manifests and Compose file. Off by default.

**Data integrity**

- Idempotent evidence and edge upserts; checksummed WAL v2 records; torn-tail truncation; WAL generations with generation-aware replication; atomic single-ingest commit groups; quarantine of unreadable legacy records; content-aware batch idempotency.

**Behavior changes (breaking)**

- Edge direction: `from supports to` supports the target claim. Ranking contributions saturate.
- `/v1/embeddings` rejects token-id inputs by default, reports an estimated `prompt_tokens`, uses OpenAI-shaped errors; provider failures are 502/503.
- New status codes and bounds: 408, 413, 429, 431, 501, `top_k` at most 1000.

**Operations and SDKs**

- New tools `wal-inspect` and `audit-verify`; corrected Compose, Kubernetes, Helm and systemd packaging; Java, Kotlin and C# SDKs fixed and unified at 0.2.0 with `delete()` removed; the Go module path is now `github.com/BHAWESHBHASKAR/DASH/sdks/go`.

**Not in 0.3.0:** HMAC-keyed audit chain, encryption at rest, TLS on by default, consensus replication and failover, delete and tenant APIs, a real GPU backend, signed images.

## M11 (in tree, untagged)

OIDC/JWKS validation and role-based access control in ingestion and retrieval; the `pkg/encryption` AES-256-GCM library (not wired into storage); a SOC 2 readiness mapping, policy templates and an evidence-collector script. See the root changelog for the list of known gaps.

## 0.2.x modernization line (in tree, untagged)

Claim, evidence and edge model; WAL with checkpoints; semantic-first retrieval; OpenAI-compatible embeddings endpoint; six SDKs; Docker, Helm and systemd packaging; redb persistence (default on when a WAL path is set); fuzz targets; benchmark suite. Test counts quoted in older entries were not re-verified; the current static counts are in the [README](https://github.com/BHAWESHBHASKAR/DASH#tests).

## Known limitations

See the "Status" section of the README and the [issue register](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-issue-register.md).
