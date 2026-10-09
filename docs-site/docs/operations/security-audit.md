# Security and audit

This page describes the security checks and audit tooling that exist in the repository, and says what they do and do not prove. It replaces an earlier version that reported "0 open advisories" and "0 HIGH, 0 CRITICAL" without a source; no such result is recorded in this repository, so none is claimed here.

## CI checks (`.github/workflows/security.yml`)

| Job | What it runs | Notes |
|---|---|---|
| `cargo-audit` | `cargo install cargo-audit --locked` then `cargo audit` | Checks `Cargo.lock` against the RustSec database. It uses default behavior, not `--deny warnings`. There is no `deny.toml` in the repository. |
| Trivy filesystem scan | `trivy fs` with `ignore-unfixed`, severity HIGH/CRITICAL, SARIF upload | A scan of the source tree, not of a built container image. The action is referenced as `@master` (register SEC-21). |
| CodeQL | `github/codeql-action` with `security-and-quality`, per language matrix | Findings appear in the repository's code-scanning tab. |
| Gitleaks | `gitleaks/gitleaks-action@v2` | Secret scanning on the repository. |

Run the dependency audit locally with `scripts/cargo-audit.sh` and the secret scan with `scripts/secret-scan.sh`. To see current results, open the latest workflow runs in GitHub Actions; do not rely on any number written in documentation.

## Release pipeline (`.github/workflows/release.yml`)

On a `v*` tag it cross-compiles binaries, builds and pushes images to `ghcr.io/<owner>/dash-<service>`, and generates an SBOM with `anchore/sbom-action`. Images are not signed and there is no provenance attestation (register SEC-21, planned P7). No release has been tagged yet, so no published images or SBOMs exist.

## Audit log tooling

`scripts/verify_audit_chain.sh --path <file> [--service ingestion|retrieval]` checks `seq`, `prev_hash` linkage and the SHA-256 of each record. Limitations, in short: the chain is unkeyed (rewritable), ingestion-written logs fail verification today (SEC-17), and auditing is off unless `DASH_INGEST_AUDIT_LOG_PATH` / `DASH_RETRIEVAL_AUDIT_LOG_PATH` is set. See [Security](../concepts/security.md#audit-log) and the [data model](../concepts/data-model.md#audit-record).

## Release sign-off scripts

`scripts/security_signoff_gate.sh`, `scripts/auth_revocation_drill.sh` and `scripts/release_candidate_gate.sh` automate parts of a release check. They are drills run by a person; their output is not published anywhere and nothing here states that they have passed.

## Threat model

The repository threat model is [`docs/threat-model.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/threat-model.md). It lists, per threat, what is mitigated today, what is not, and which release or phase addresses it.

## How to report a vulnerability

See [About → Security](../about/security.md).
