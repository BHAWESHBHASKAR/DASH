# Security Checklist (Pre-Release)

This checklist is the gate that the release captain works through before
cutting a DASH release. Every box must be checked (or explicitly waived with a
linked rationale comment) for the release to be tagged.

The checklist is intentionally a flat list — it is meant to be ticked off
during the release meeting, not searched. If you find yourself adding a new
item, also add a corresponding test, script, or workflow that prevents
regression.

## How to Use

1. Open this file at the start of the release captain's runbook.
2. For each item, run the listed command (or open the linked UI).
3. Tick the box (`[x]`) and paste a one-line evidence note (commit SHA, CI
   run URL, screenshot path, or "waived: <link>").
4. The release is not "ready" until the list is fully ticked.

## Checklist

- [ ] **cargo audit clean** — `scripts/cargo-audit.sh` exits 0. No unfixed
      `RUSTSEC` advisories; no denied licenses; no banned crates.
      Evidence: <paste CI run URL>
- [ ] **gitleaks clean** — `scripts/secret-scan.sh` exits 0. No credentials,
      tokens, or private keys in the working tree or in any committed
      artifact since the previous tag.
      Evidence: <paste CI run URL>
- [ ] **trivy scan clean on latest image** — `trivy image --severity
      HIGH,CRITICAL ghcr.io/dash-project/ingestion-svc:<tag>` reports 0
      findings. (Or: findings are triaged and linked to a tracking issue.)
      Evidence: <paste scan output or issue link>
- [ ] **CodeQL workflow green** — the `CodeQL` GitHub Actions workflow for
      `main` is green on the release commit. No new alerts introduced by
      the diff against the previous tag.
      Evidence: <paste workflow run URL>
- [ ] **JWT secret rotation tested** — the `security_signoff_gate.sh` drill
      that rotates the HS256 secret and re-issues tokens runs green in
      staging. Confirm rotation is logged as an audit event.
      Evidence: <paste drill run URL>
- [ ] **Rate limit thresholds validated** — load test with
      `bench_transport_concurrency.sh` shows that the default
      `--rate-limit-rps` and per-IP connection cap hold steady-state at
      the documented RPS without dropping legitimate traffic. Worst-case
      per-tenant RPS is recorded in the runbook.
      Evidence: <paste bench summary>
- [ ] **redb encryption-at-rest decision recorded** — currently **no** in-app
      encryption; see `docs/adr/ADR-006-redb-encryption.md`. This decision
      has been re-confirmed for this release, and the operator-facing
      runbook still requires an encrypted block device underneath the
      `dash.redb` file.
      Evidence: <link to re-confirmation comment>
- [ ] **Dependency licenses reviewed** — `cargo deny check licenses` is
      green and the resulting license list has been diffed against the
      previous release. Any new license appears in `THIRD_PARTY_LICENSES.md`.
      Evidence: <paste diff or "no changes">
- [ ] **SBOM generated and attached to release** — `cargo cyclonedx --format
      spdxjson` produced a CycloneDX/SPDX SBOM, which is attached to the
      GitHub release as `dash-<version>-sbom.spdx.json` and signed with
      `cosign sign --predicate`.
      Evidence: <link to release asset>
- [ ] **All SDK versions match the released server version** — the
      `sdks/` workspace has been tagged at `<version>`, the cross-SDK
      version check in `sdks.yml` is green, and the server's
      `/version` endpoint reports `<version>` for both `ingestion-svc`
      and `retrieval-svc`.
      Evidence: <paste workflow run URL + `/version` output>

## Waivers

If a box cannot be ticked, the release captain must:

1. Open a tracking issue titled `release-<version>-waiver: <item>`.
2. Link the issue in the box above as `waived: <link>`.
3. Get sign-off from the security lead in the issue.

A release with three or more open waivers is **blocked** until the security
lead either closes the waivers or escalates to the steering committee.
