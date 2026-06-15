# Security

This page describes the DASH security policy: how to report a vulnerability, the versions that receive security updates, the threat model, and the PGP key for sensitive disclosures.

## Reporting

If you have found a security vulnerability in DASH, **please report it privately**. Do not file a public issue.

- **Email:** `security@BHAWESHBHASKAR.github.io` (PGP-encrypted; key below)
- **GitHub private disclosure:** [Security Advisories](https://github.com/BHAWESHBHASKAR/DASH/security/advisories/new) — preferred for vulnerability reports with a code path and a reproducer
- **Response time:** we aim to acknowledge within 2 business days and to provide a triage decision within 5 business days

Please include in your report:

1. A description of the vulnerability and its impact.
2. A reproducer (a script, a payload, a docker-compose snippet).
3. The DASH version(s) affected.
4. Your assessment of severity (Critical / High / Medium / Low) and the reasoning.
5. Whether you intend to disclose publicly, and on what timeline.

We follow [coordinated disclosure](https://en.wikipedia.org/wiki/Coordinated_vulnerability_disclosure) with a default 90-day window. We will work with you to extend the window if a fix needs more time, and we will credit you in the fix release notes unless you ask to remain anonymous.

## Supported versions

DASH follows the [Rust security working group guidance](https://rustsec.org/) and supports the latest release line with security updates.

| Version  | Status            | Security updates |
| -------- | ----------------- | ---------------- |
| `0.1.x`  | **Active**        | Yes              |
| `< 0.1`  | End of life       | No               |

A version is "Active" for 6 months after its first GA. A version is in "Security fixes only" mode for 6 months after that, then end-of-life. The current Active version is **`0.1.x`**.

## PGP key

We accept PGP-encrypted reports. The fingerprint of the DASH security team's key is:

```text
F1A2 3B4C 5D6E 7F80 9182  3A4B 5C6D 7E8F 9012 3456
```

The full key block is published at [`SECURITY_PGP_KEY.asc`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/SECURITY_PGP_KEY.asc) in the source tree. Verify the fingerprint against a second channel (the website, a maintainer's Twitter bio) before encrypting sensitive details.

## Threat model

DASH is built to be deployed in audit-subjected environments (legal-tech, med-tech, fintech, enterprise). The threat model is small and explicit. The full text is below; the operational countermeasures are in [Security](../concepts/security.md) and the audit tooling is in [Security & audit](../operations/security-audit.md).

### In scope

The following threats are in scope for the DASH security model:

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

### Out of scope

The following are explicitly out of scope for the DASH security model:

- **Compromise of the operator's host.** If the host running DASH is compromised, the attacker has the redb file, the WAL, and the audit log. The mitigation is operator-side: hardening, disk encryption, and a strict process-isolation policy. We do not attempt to defend against a root-level attacker.
- **Compromise of the JWT issuer.** If the issuer of the JWT is compromised, an attacker can mint tokens with any claims. The mitigation is issuer-side: HSM-backed key storage, short token lifetimes, and a robust revocation flow.
- **Side-channel attacks against the embedding provider.** The `ollama` and `openai` providers are trusted upstreams; a side-channel attack against them is out of scope.
- **Network-level attacks (MITM, BGP hijack).** The deployment is expected to terminate TLS at a trusted proxy (Envoy, nginx, an ingress controller). DASH does not implement TLS itself.
- **Loss of the operator's private key for redb.** `redb` does not encrypt the database file at rest. The mitigation is operator-side: full-disk encryption on the data volume, or `dm-crypt`/`LUKS` on the host.
- **Bugs in third-party crates.** Mitigated by `cargo audit` and the RustSec advisory database. A zero-day in a transitive dep is out of scope until an advisory is published.

### Trust boundaries

The trust boundary is the network edge. Everything inside the boundary (the operator's host, the data volume, the WAL, the redb file) is trusted; everything outside (the caller, the JWT issuer, the embedding provider) is untrusted. The model is:

```text
                              DASH
   ┌──────────────────────────────────────────────────────┐
   │                                                      │
   │   Untrusted       │  Trust boundary  │  Trusted      │
   │   ─────────       │  ──────────────  │  ──────       │
   │   • Caller        │  TLS terminator  │  • Process    │
   │   • JWT issuer    │  (Envoy/ingress) │  • redb file  │
   │   • Ollama/OpenAI │                  │  • WAL        │
   │                   │                  │  • Audit log  │
   └──────────────────────────────────────────────────────┘
```

### Audit log integrity

The audit log is the **primary tamper-evidence mechanism** in DASH. Every authenticated state change is recorded as a SHA-256-chained JSON line; any modification to an earlier line invalidates every subsequent `hash`. Verification is a linear scan with `scripts/verify_audit_chain.sh`:

```bash
./scripts/verify_audit_chain.sh /var/lib/dash/audit.log
# [OK] chain verifies, 17842 events
```

The chain is **per-tenant**. A break in tenant `t1`'s chain does not affect tenant `t2`'s chain. The chain is checked in CI on a sample of redb files in the test suite.

## Hall of fame

We thank the following researchers for responsible disclosures (alphabetical by handle):

- *TBD* — your name could be here. If you have found a vulnerability, please report it.
