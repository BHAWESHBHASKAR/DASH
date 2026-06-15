# Security & audit

DASH is built to be deployed in audit-subjected environments. The security story is layered: a small attack surface, a tight set of dependencies, a hash-chained audit log, and a CI pipeline that catches regressions before they ship. This page describes the audit tools, the CI checks, and the threat model.

## `cargo audit`

`cargo audit` checks the Rust dependency tree against the [RustSec Advisory Database](https://rustsec.org/). The check is in CI on every PR:

```bash
cargo install --locked cargo-audit
cargo audit --deny warnings
```

A high-severity advisory against a dependency **fails the build**. The `deny.toml` in the repo pins the policy: any advisory with a `[[advisory]]` entry under `[advisories]` is treated as a hard error regardless of severity. To ignore a specific advisory (e.g. for a transitive that the team has reviewed and accepted), add an entry to `deny.toml` with a `reason`.

The current advisory count is **0** open advisories.

## `trivy`

`trivy` scans the container image for OS-level CVEs and misconfigurations. The check is in CI on every image push:

```bash
trivy image --severity HIGH,CRITICAL \
  --exit-code 1 \
  ghcr.io/bhaweshbhaskar/dash:0.1.0
```

The image is built on `debian:bookworm-slim`, pinned at build time. The `Dockerfile` runs `apt-get update && apt-get install -y --no-install-recommends ...` and then `rm -rf /var/lib/apt/lists/*` to keep the layer small. A multi-stage build discards the build-time dependencies from the final image.

The current `trivy` result is **0 HIGH, 0 CRITICAL**.

## `codeql`

`codeql` does semantic analysis of the Rust source for security-relevant patterns. The check is in CI on every PR, against the `security-and-quality` query pack:

```yaml
- name: Initialize CodeQL
  uses: github/codeql-action/init@v3
  with:
    languages: rust
    queries: security-and-quality

- name: Perform CodeQL Analysis
  uses: github/codeql-action/analyze@v3
  with:
    category: "/language:rust"
```

A new finding of `security` or higher severity **fails the PR check**. The findings dashboard is on the [GitHub Security tab](https://github.com/BHAWESHBHASKAR/DASH/security/code-scanning).

## Threat model

The full threat model is at [Security → Threat model](../about/security.md#threat-model). The condensed view:

| Threat                                            | Mitigation                                                          | Residual risk                                                                 |
| ------------------------------------------------- | ------------------------------------------------------------------- | ----------------------------------------------------------------------------- |
| Stolen JWT                                         | Short lifetime, `aud = dash`, scope checks, per-tenant rate limit. | A stolen JWT is valid until `exp`. Use short lifetimes; rotate keys.          |
| Stolen API key                                     | Per-tenant scoping, rotation overlap, constant-time compare.        | A stolen key is valid until `expires_at_unix`. Use short expiry; rotate.     |
| Cross-tenant data leak                             | Hard isolation in the store + the API layer.                        | A bug in the isolation boundary. Mitigated by per-tenant integration tests.    |
| Tampering with audit log                           | SHA-256 hash chain; verify script in CI.                            | A motivated insider with write access to the log file.                         |
| Replay of ingest                                   | Idempotency keys; redb dedup.                                        | None observed.                                                                |
| DoS via large payload                              | `Content-Length` cap; per-tenant rate limit.                         | A flood from many distinct tenants. Mitigated by upstream WAF.                |
| Slow-loris DoS                                     | TCP read timeout in the transport.                                   | None observed.                                                                |
| SQL injection                                      | N/A — DASH is not SQL.                                              | N/A.                                                                          |
| Dependency CVE                                     | `cargo audit` in CI; `trivy` on the image.                          | A zero-day. Mitigated by rapid patch policy.                                  |
| Compromised redb file                              | `redb` is ACID; replay from WAL reconstructs.                        | A maliciously crafted redb file could exploit a redb parser bug.              |
| Compromised process memory                        | JWT public-key only; private key never enters DASH.                 | A memory-disclosure vulnerability in a transitive dep.                       |
| Compromised operator workstation                  | Out of scope.                                                       | See the operator-host-hardening guide (forthcoming).                          |

### How to report a vulnerability

See [Security → Reporting](../about/security.md#reporting). The PGP fingerprint and the supported-versions table are also in the security policy.
