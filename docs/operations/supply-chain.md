# Supply chain: verifying releases, SBOMs and pins

This page says what a DASH release publishes besides the binaries and
images, how to verify it before you deploy, and which controls keep the
build inputs fixed. Register items: SEC-20 (image digest pinning) and
SEC-21 (CI supply chain).

Status: the release workflow (`.github/workflows/release.yml`) produces
everything below, but no tag has been pushed since it was added, so no
published release carries these files yet. The first release that does
is the first one tagged after 0.3.0 work merges.

## What a release contains

For each service (`ingestion`, `retrieval`, `control-plane`) and target
(`x86_64-unknown-linux-gnu`, `aarch64-unknown-linux-gnu`,
`x86_64-apple-darwin`, `aarch64-apple-darwin`), the GitHub Release has:

| File | What it is |
|---|---|
| `<service>-<target>` | The binary. |
| `<service>-<target>.cdx.json` | CycloneDX 1.5 SBOM of the binary's crate graph for that target, from `Cargo.lock` (`cargo-cyclonedx` 0.5.9). |
| `dash-<service>-image.spdx.json` | SPDX SBOM of the container image (syft, via `anchore/sbom-action`). |
| `dash-<service>-image.digest` | The pushed image reference, `ghcr.io/<owner>/dash-<service>@sha256:...`. |
| `SHA256SUMS` | SHA-256 of every file above. |
| `<file>.cosign.bundle` | Sigstore bundle for each file above and for `SHA256SUMS`: signature, signing certificate and Rekor inclusion proof. |

Each image `ghcr.io/<owner>/dash-<service>` is pushed for `linux/amd64`
and `linux/arm64`, and its digest carries:

- a cosign signature,
- a cosign attestation of type `spdxjson` holding the image SBOM,
- a GitHub build-provenance (SLSA v1) attestation, also pushed to the
  registry.

All files are also covered by a GitHub build-provenance attestation.

### How signing works

Signing is keyless. The workflow run's GitHub OIDC token is exchanged for a
short-lived Fulcio certificate whose identity is the workflow file and the
tag ref, for example

```
https://github.com/BHAWESHBHASKAR/DASH/.github/workflows/release.yml@refs/tags/v0.3.0
```

issued by `https://token.actions.githubusercontent.com`. There is no
long-lived key to steal or rotate; what you check is that the artifact was
signed by this repository's release workflow for the tag you expect.

Only two jobs can request an OIDC token (`id-token: write`): `docker`
(signs and attests images) and `sign-artifacts` (signs and attests files).
Neither can write repository contents. The `github-release` job, which
creates the release, has `contents: write` and no OIDC token.

## Verifying

Tools: [cosign](https://github.com/sigstore/cosign) 2.x and the
[GitHub CLI](https://cli.github.com/) (`gh attestation`, 2.49 or later).
Replace `v0.3.0` with the tag you are installing.

```bash
TAG=v0.3.0
IDENTITY="https://github.com/BHAWESHBHASKAR/DASH/.github/workflows/release.yml@refs/tags/${TAG}"
ISSUER="https://token.actions.githubusercontent.com"
```

### A binary (or any release file)

```bash
f=retrieval-x86_64-unknown-linux-gnu
cosign verify-blob --bundle "${f}.cosign.bundle" \
  --certificate-identity "${IDENTITY}" --certificate-oidc-issuer "${ISSUER}" \
  "${f}"
# Verified OK
```

Or verify the checksum file once and check files against it:

```bash
cosign verify-blob --bundle SHA256SUMS.cosign.bundle \
  --certificate-identity "${IDENTITY}" --certificate-oidc-issuer "${ISSUER}" \
  SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
```

Build provenance (which workflow run, commit and runner built the file):

```bash
gh attestation verify retrieval-x86_64-unknown-linux-gnu --repo BHAWESHBHASKAR/DASH
```

### A container image

Always deploy and verify by digest; a tag can be moved. The digest is in
`dash-<service>-image.digest`, or resolve it:

```bash
docker buildx imagetools inspect ghcr.io/bhaweshbhaskar/dash-retrieval:0.3.0 \
  --format '{{json .Manifest.Digest}}'
IMAGE=ghcr.io/bhaweshbhaskar/dash-retrieval@sha256:<digest>
```

Signature:

```bash
cosign verify "${IMAGE}" \
  --certificate-identity "${IDENTITY}" --certificate-oidc-issuer "${ISSUER}"
```

SBOM attestation (verifies the signature, then prints the SPDX document):

```bash
cosign verify-attestation --type spdxjson "${IMAGE}" \
  --certificate-identity "${IDENTITY}" --certificate-oidc-issuer "${ISSUER}" \
  | jq -r '.payload' | base64 -d | jq '.predicate'
```

Build provenance:

```bash
gh attestation verify "oci://${IMAGE}" --repo BHAWESHBHASKAR/DASH
```

To enforce this at admission time in Kubernetes, use a policy controller
(for example Sigstore policy-controller or Kyverno `verifyImages`) with the
same identity and issuer.

### Using the SBOMs

The CycloneDX files list every crate (name, version, `pkg:cargo` purl,
license, crates.io checksum) compiled into that binary for that target.
Feed them to a scanner to check a deployed version against new advisories
without rebuilding, for example:

```bash
trivy sbom retrieval-x86_64-unknown-linux-gnu.cdx.json
grype sbom:retrieval-x86_64-unknown-linux-gnu.cdx.json
```

The SPDX image SBOM adds the Debian packages of the runtime image.

## Controls on the build inputs

| Control | Where | What it prevents |
|---|---|---|
| Every third-party action pinned to a 40-character commit SHA, release tag in a trailing comment | `.github/workflows/*.yml` | A moved or compromised tag changing what CI runs. |
| `dtolnay/rust-toolchain` pinned to a `master` commit (the project publishes no release tags), toolchain passed as an input | same | As above. |
| `cargo install --locked --version <exact>` for cargo-audit, cargo-deny, cargo-fuzz, cross, cargo-cyclonedx | same | A new tool release or a drifting tool dependency entering CI unreviewed. |
| Base images pinned by multi-arch index digest: `rust:1.99-bookworm@sha256:...`, `debian:bookworm-slim@sha256:...`, `docker/dockerfile:1.7@sha256:...` | `deploy/container/Dockerfile` | A re-pushed tag changing the compiler or runtime image of a rebuild. |
| Dependabot for cargo, pip, gomod, npm, maven, gradle, nuget, github-actions and docker | `.github/dependabot.yml` | Pins going stale. Dependabot updates a SHA pin together with its version comment and a digest pin together with its tag. |
| `cargo deny check` | `deny.toml`, `security.yml` job `cargo-deny` | Vulnerable, unsound, unmaintained or yanked crates; licenses outside the allow-list; crates from anywhere but crates.io; wildcard version requirements. Duplicate versions are reported as warnings. |
| `cargo audit` | `security.yml` job `cargo-audit` | Same RustSec database, second tool. |
| Trivy filesystem scan, CodeQL, Gitleaks | `security.yml` | Vulnerable dependencies in all ecosystems, code issues, committed secrets. |

### Re-resolving a pin by hand

Never type a SHA or digest from memory; read it from the source.

Action SHA for a tag (for an annotated tag use the `^{}` line, which is
the commit):

```bash
git ls-remote https://github.com/actions/checkout refs/tags/v4.4.0 'refs/tags/v4.4.0^{}'
```

Image digest (the index digest covers every platform):

```bash
docker buildx imagetools inspect debian:bookworm-slim   # "Digest:" line
```

Without Docker, ask the registry directly:

```bash
repo=library/debian tag=bookworm-slim
token=$(curl -fsS "https://auth.docker.io/token?service=registry.docker.io&scope=repository:${repo}:pull" | jq -r .token)
curl -fsSI -H "Authorization: Bearer ${token}" \
  -H 'Accept: application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json' \
  "https://registry-1.docker.io/v2/${repo}/manifests/${tag}" | grep -i docker-content-digest
```

When the Rust toolchain moves, change `channel` in `rust-toolchain.toml` and
the `rust:` tag and digest in the Dockerfile in the same change.

### cargo-deny policy in short

- Advisories: vulnerabilities and unsound crates fail; `unmaintained =
  "all"` and `yanked = "deny"` fail. The ignore list is empty; an entry
  needs an advisory id, a reason and a tracking issue.
- Licenses: 0BSD, Apache-2.0 (and with LLVM-exception), BSD-2-Clause,
  BSD-3-Clause, BSL-1.0, CC0-1.0, CDLA-Permissive-2.0, ISC, MIT, MIT-0,
  Unicode-3.0, Unlicense, Zlib. Copyleft licenses are not allowed. The
  workspace crates declare Apache-2.0 (the `LICENSE` file) and
  `publish = false`.
- Sources: crates.io only; no git dependencies.

## Known gaps

- Not yet exercised end to end: the signing, attestation and SBOM steps run
  only on a tag push, and none has happened since they were added.
  `actionlint` (with shellcheck) passes on all workflows.
- The macOS targets are built with `cross` on an Ubuntu runner, which does
  not provide Apple SDKs; those matrix legs are expected to fail until they
  move to macOS runners. This predates this page.
- The Helm chart and the raw manifests reference images by tag
  (`ghcr.io/bhaweshbhaskar/dash-<service>:0.2.0`). Until the chart accepts
  a digest, pin by digest in your own overlay (`image@sha256:...`) after
  verifying it as above.
- The SDK packages (PyPI, npm, Maven, NuGet) are not published by this
  workflow and are therefore not signed here.
