# Security

This page summarizes the DASH security policy. The authoritative policy is [`SECURITY.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/SECURITY.md) in the repository root; the threat model is [`docs/threat-model.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/threat-model.md).

## Reporting

If you have found a security vulnerability, please report it privately and do not file a public issue.

- **Preferred:** GitHub private vulnerability reporting: [Security advisories](https://github.com/BHAWESHBHASKAR/DASH/security/advisories/new).
- **Email:** the address in `SECURITY.md`. Note that `SECURITY.md` currently carries a placeholder PGP fingerprint that the maintainers must replace before a public release. Do not trust any fingerprint printed in documentation until it is published through a second channel by the maintainers. Earlier versions of this page printed a fingerprint and an `SECURITY_PGP_KEY.asc` file that do not exist; both were removed.

Please include a description and impact, a reproducer, the affected version or commit, and your severity assessment. The response targets and the disclosure timeline are in `SECURITY.md`.

## Supported versions

See the table in `SECURITY.md`. DASH is pre-1.0; the current development tree is 0.3.0 (unreleased, the hardening release) and the last line before it is 0.2.x.

## Threat model

The current threat model, including the attack surface that earlier text omitted (control plane, replication endpoints, embeddings proxy), is in [`docs/threat-model.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/docs/threat-model.md). Each mitigation there is marked as implemented, partial, planned or not implemented, with a link to code or a test. A control is only described as present if it can be found in the repository.

Out of scope for the DASH code, as before: compromise of the operator's host, compromise of the identity provider, TLS termination (DASH speaks plain HTTP and expects a proxy), and encryption of volumes (DASH can encrypt its data files when `DASH_ENCRYPTION_KEY_FILE` is set; it is off by default, and file names, sizes and audit logs stay plaintext).

For the controls in more detail see [Security](../concepts/security.md) and [Security and audit](../operations/security-audit.md).
