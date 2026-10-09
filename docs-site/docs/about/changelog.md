# Changelog

The canonical changelog is [`CHANGELOG.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/CHANGELOG.md) in the repository root. This page summarizes it and does not duplicate the detail; an earlier copy here had drifted from the source and contained claims that were not true (for example that the ANN index was replaced by `usearch`).

DASH has no tagged release yet. The sections below follow the root changelog.

## 0.3.0 (planned): P0 production-readiness hardening

Nothing here has shipped. Scope and exit criteria are in the [master plan](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-production-readiness-master-plan.md).

- Services refuse to start without credentials unless `DASH_INSECURE_DEV_MODE=1` (dev mode binds localhost only).
- Strict secret validation is on by default (at least 32 characters, placeholders rejected).
- `/v1/embeddings`, `/debug/*` and `/metrics` require authentication.
- Replication requires `DASH_INGEST_REPLICATION_TOKEN`; the control plane requires `DASH_CONTROL_PLANE_TOKEN`.
- Rate limiting is enforced and returns HTTP 429.
- Evidence and edges become idempotent upserts.
- A torn WAL tail is truncated on recovery; WAL generation ids force follower resync after compaction.
- The Ollama endpoint variable is `DASH_OLLAMA_ENDPOINT`; the OpenAI provider speaks TLS.
- Java, Kotlin and C# SDKs are fixed and unified at 0.2.0.
- Documentation rewritten to match the code; a claims ledger tracks each README claim to a test.

## M11 (in tree, untagged)

OIDC/JWKS validation and role-based access control in ingestion and retrieval; the `pkg/encryption` AES-256-GCM library (not wired into storage); a SOC 2 readiness mapping, policy templates and an evidence-collector script. See the root changelog for the list of known gaps.

## 0.2.x modernization line (in tree, untagged)

Claim, evidence and edge model; WAL with checkpoints; semantic-first retrieval; OpenAI-compatible embeddings endpoint; six SDKs; Docker, Helm and systemd packaging; redb persistence (default on when a WAL path is set); fuzz targets; benchmark suite. Test counts quoted in older entries were not re-verified; the current static counts are in the [README](https://github.com/BHAWESHBHASKAR/DASH#tests).

## Known limitations

See the "Status" section of the README and the [issue register](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/plans/2026-10-09-issue-register.md).
