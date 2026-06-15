# Security Policy

The DASH team takes the security of the project seriously. We appreciate responsible
disclosure of vulnerabilities and work with reporters to triage, fix, and disclose
issues in a coordinated manner.

## Supported Versions

The following table lists the DASH release lines that currently receive security
support. Each row is evaluated independently when a CVE or internal advisory is
filed.

| Version  | Supported                  | Notes                                              |
|----------|----------------------------|----------------------------------------------------|
| 0.2.x    | :white_check_mark: Active  | Current development line. Receives all patches.    |
| 0.1.x    | :white_check_mark: Patches  | Receives security patches only; no new features.   |
| < 0.1.0  | :x: End of life            | Not supported. Please upgrade.                     |

Security patches for 0.1.x are produced on a best-effort basis and may be bundled
with the next 0.2.x release. Critical issues that affect both lines will be
backported to 0.1.x; low-severity issues will not.

## Reporting a Vulnerability

Please report suspected vulnerabilities to **security@dash-project.dev**. All
other channels (GitHub issues, discussions, pull requests) are **not** monitored
for security reports.

When you email, please include:

- A clear description of the issue and the impact you believe it has.
- Reproduction steps, proof-of-concept code, or a minimal failing test.
- The exact commit SHA, tag, or release artifact you observed the issue against.
- Your assessment of severity (we will independently triage).
- Whether you intend to disclose publicly and on what timeline.

### What to Expect

- **Acknowledgement:** within **48 hours** of receipt, a maintainer will reply
  to confirm we received the report and that a triage is in progress.
- **Triage:** within **5 business days** we will provide an initial severity
  classification (Critical/High/Medium/Low) and a tentative fix timeline.
- **Status updates:** we will send a status update at least once every **14
  days** until the issue is resolved or disclosed.
- **Resolution:** we will either release a fix in a patched version, document a
  workaround, or — for issues we determine to be out of scope — explain our
  decision in writing.

### PGP Key

If you need to send sensitive details (for example, a working exploit chain),
please encrypt your report with the following maintainer key. The fingerprint is
**`0000 0000 0000 0000 0000 0000 0000 0000 0000 0000`** (placeholder — replace
before first public release). A copy of the public key is published at
<https://dash-project.dev/.well-known/pgp-key.asc> and in the repository at
`docs/security/pgp-key.asc`.

## Disclosure Timeline

We follow a **90-day** coordinated disclosure timeline, mirroring industry
practice:

1. **Day 0** — Report received and acknowledged (within 48 hours).
2. **Day 0–30** — Triage, fix development, and internal verification.
3. **Day 30–60** — Pre-disclosure coordination with the reporter; CVE assigned
   via GitHub Security Advisories or MITRE.
4. **Day 60–90** — Patch release and public advisory.
5. **Day 90** — Coordinated public disclosure; embargo lifts.

Extensions of up to **30 days** are granted on request when a fix requires more
time, a downstream consumer needs lead time, or a multi-party coordination is
in flight. We will not grant extensions that exceed 120 days from the original
report date.

## Out of Scope

The following classes of issues are **not** considered security vulnerabilities
in DASH and will be closed as such (with a brief explanation). They are listed
here so that reporters do not invest time in reports we cannot action.

- **Denial of service at the network layer.** Volume-based DoS, link flooding,
  BGP hijacks, DNS poisoning, and similar attacks are handled by the
  operator's edge infrastructure (CDN, WAF, upstream provider). DASH
  deployments are expected to sit behind a rate-limiting reverse proxy.
- **Theoretical issues without a proof of concept.** A vulnerability that
  cannot be reproduced against the latest release will be treated as a
  hardening suggestion, not a security report.
- **Vulnerabilities in software we do not own.** Please report these to the
  upstream maintainer and link the advisory; we will track the impact in
  `docs/supply-chain.md`.
- **Self-XSS / social engineering.** Issues that require the operator to paste
  attacker-controlled content into their own environment are out of scope.
- **Scanner-only findings.** Reports consisting solely of automated scanner
  output without manual validation are not accepted as security reports.

For concerns not covered above, default to **reporting it** — we would rather
triage a benign report than miss a real issue.

## Security Hall of Fame

We are grateful to the security community. Reporters who follow this policy
and submit a valid vulnerability will be listed here (with their permission).

| Reporter | Advisory | Date |
|----------|----------|------|
| _no entries yet_ | | |

If you would prefer to remain anonymous, say so in your report and we will
record the advisory without a name.
