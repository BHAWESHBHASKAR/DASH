# DASH Supply-Chain Security

This document describes the supply-chain controls applied to DASH artifacts
(source, dependencies, container images, and SDKs). It is referenced from
`SECURITY.md` and from the pre-release checklist in
`docs/security-checklist.md`.

## Goals

1. **Provenance.** A consumer of any DASH artifact can mechanically answer
   "where did this come from?".
2. **Integrity.** A consumer can mechanically answer "has this been
   tampered with since it was built?".
3. **Inventory.** A consumer can mechanically answer "what is in this?".
4. **Pace.** Known vulnerabilities are surfaced and fixed on a predictable
   cadence.

## 1. SBOM — Software Bill of Materials

DASH ships a **CycloneDX / SPDX 2.3** SBOM with every release.

- Generated with **`cargo-cyclonedx`**:

  ```bash
  cargo install cargo-cyclonedx --locked
  cargo cyclonedx --format spdxjson --override-filename target/sbom/dash
  ```

- Attached to the GitHub release as `dash-<version>-sbom.spdx.json`.
- Also published to the internal artifact registry under
  `sbom/dash/<version>/`.
- Re-generated on every dependency change in the `nightly` workflow; the
  result is diffed against the previous SBOM and a PR is opened on any
  unexpected change.

The SBOM is consumed by:

- The vulnerability scanner (Trivy / Grype) for matching `RUSTSEC` and
  GitHub Advisory IDs.
- The license review step of the security checklist.
- Customer compliance teams via the public release page.

## 2. SLSA L3 Provenance

Release artifacts are built with **`slsa-github-generator`** and the
resulting provenance attestation satisfies **SLSA Level 3**:

- Builds run in **isolated, ephemeral GitHub Actions runners** with
  network egress restricted by an OPA policy.
- The workflow identity is pinned to a specific commit SHA via
  `github.com/slsa-framework/slsa-github-generator/.github/workflows/
  generator_container_slsa3.yml@<pinned-tag>`.
- The provenance predicate is signed with Sigstore `cosign` and attached
  to the release.
- Consumers verify with:

  ```bash
  slsa-verifier verify-image \
    --image-uri ghcr.io/dash-project/ingestion-svc:<version> \
    --provenance-path dash-<version>.intoto.jsonl \
    --source-uri github.com/dash-project/dash
  ```

The same pipeline produces provenance for:

- The container image (per service, per architecture).
- The release tarball attached to the GitHub release.
- Each SDK tarball under `sdks/<lang>/dist/`.

## 3. Container Image Signing (Sigstore / cosign)

Every container image is signed with **Sigstore cosign** using **keyless
signing** (OIDC identity bound to the release workflow):

```bash
cosign sign --yes \
  ghcr.io/dash-project/ingestion-svc@<digest>
cosign sign --yes \
  ghcr.io/dash-project/retrieval-svc@<digest>
```

- Signature transparency log entries are anchored in **Rekor**.
- Verification policy (a `cosign verify` invocation) is published at
  `docs/security/cosign-policy.yaml` and is run in CI on a daily
  schedule.
- The signing identity is pinned to the `release.yml` workflow path; a
  signature made by any other identity is rejected by the verifier.

The release checklist requires that the signed digest referenced by the
release page **matches** the digest produced by the workflow — a
mismatch fails the gate.

## 4. Reproducible Builds

We aim for **bit-for-bit reproducible builds** of the release tarballs
and container images. This is enforced by the
**`cargo-deterministic`** plugin and a set of source-determinism rules
recorded in `docs/adr/ADR-008-reproducible-builds.md`.

Rules:

- `SOURCE_DATE_EPOCH` is set to the timestamp of the release tag.
- `CARGO_PROFILE_RELEASE_INCREMENTAL=false` and
  `CARGO_PROFILE_RELEASE_DEBUG=false`.
- The build container's `LC_ALL=C.UTF-8`, `LANG=C.UTF-8`, and `TZ=UTC`.
- No network access during the build (the `redb` build is fully
  in-tree).
- `gzip -n` (no name/timestamp) for tarballs.

A nightly job builds the release twice — once on a GitHub-hosted runner
and once on an internal reproducible-builds runner — and diffs the
artifacts. Any non-deterministic difference is a release-blocker.

## 5. Dependency Update Cadence

Dependencies are updated on a **weekly** cadence via **Dependabot**,
configured in `.github/dependabot.yml`:

- **Cargo** (the workspace at the repo root and the per-crate
  workspaces under `services/` and `tools/`) — grouped by minor version,
  weekly.
- **GitHub Actions** — pinned to commit SHA; Dependabot opens PRs that
  update the SHA and the integrity comment together.
- **Container base images** (in `deploy/`) — weekly, with manual review
  for major version bumps.

Dependabot PRs are auto-merged **only** when:

- `scripts/cargo-audit.sh` is green.
- The CI matrix is green.
- The PR label is `dependencies` and the change is a patch-level bump.

Minor and major bumps require a reviewer. The resulting SBOM diff is
attached to the Dependabot PR for human inspection.

## 6. Vulnerability Disclosure & Patch Timeline

Vulnerabilities affecting DASH or its dependencies are handled in three
buckets, each with its own SLA:

| Bucket                                  | Source                                            | Internal SLA | Public disclosure |
|-----------------------------------------|---------------------------------------------------|--------------|-------------------|
| **Critical** in a direct dependency     | `RUSTSEC`, GitHub Advisory, vendor advisory       | 7 days       | 30 days           |
| **High** in a direct dependency         | same                                              | 30 days      | 60 days           |
| **Medium / Low** in any dependency      | same                                              | 90 days      | 90 days           |
| **Critical** in a transitive dependency | Grype/Trivy reachability graph                    | 30 days      | 60 days           |
| **Any** in a base container image       | Trivy scan of `ghcr.io/dash-project/*`            | 7 days       | 30 days           |

These SLAs bound the **patch release**; the **public advisory** follows
the coordinated disclosure timeline in `SECURITY.md` (90 days from report
by default).

A patch release is cut as soon as the fix is verified, even if it is
ahead of the SLA, except during embargo periods required by upstream
coordinated disclosure.

## 7. Auditing & Verification

The following are run on a continuous schedule and on every release:

| Cadence   | Tool / Script                            | Purpose                                              |
|-----------|------------------------------------------|------------------------------------------------------|
| Per-PR    | `scripts/secret-scan.sh`                 | detect committed secrets                             |
| Per-PR    | `scripts/cargo-audit.sh`                 | deny warnings + license check                        |
| Per-PR    | CodeQL (`rust`, `actions`)               | static analysis                                      |
| Daily     | Trivy image scan                         | container CVE scan                                   |
| Daily     | `slsa-verifier verify-image`             | provenance & signature verification                  |
| Weekly    | `cargo outdated` report                  | stale-dependency surface area                        |
| Per-tag   | `scripts/security_signoff_gate.sh`       | end-to-end release sign-off                          |
| Per-tag   | SBOM diff                                | flag unexpected new components/licenses              |

Failures of any "Per-PR" step block the PR. Failures of "Daily" or
"Weekly" steps open an issue tagged `security/supply-chain` and
assigned to the on-call engineer.
