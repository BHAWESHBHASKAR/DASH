#!/usr/bin/env bash
# Validate the monitoring artifacts in deploy/observability/:
#
#   1. `promtool check rules` on the recording and alert rules;
#   2. `promtool test rules` on the unit tests in prometheus/tests/;
#   3. every alert has a runbook_url naming an existing runbook under
#      docs/operations/runbooks/, and every runbook belongs to an alert;
#   4. every metric the rules and dashboards query is emitted by the code
#      (its name appears in the Rust sources under services/ or pkg/);
#   5. the Grafana dashboards are valid JSON with unique uids, use the
#      ${datasource} variable, and every panel query parses as PromQL
#      (checked by promtool through a generated rules file);
#   6. the copies bundled in the Helm chart (deploy/helm/dash/files/) are
#      identical to deploy/observability/;
#   7. when `helm` is on PATH, the chart's PrometheusRule (rendered with
#      metrics.prometheusRule.enabled=true) passes `promtool check rules`.
#
# promtool comes from a pinned Prometheus release whose tarball is verified
# against the release's sha256sums.txt and against the checksum pinned here.
# Set PROMTOOL=/path/to/promtool to use an existing binary instead.
#
# Usage: scripts/check_observability.sh
# Needs: bash, curl, tar, sha256sum, jq, grep.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${ROOT_DIR}"

PROMETHEUS_VERSION="3.5.0"
PROMETHEUS_SHA256="e811827af26d822afb09a4f28314f61b618b12cff5369835a67f674d8b46f39a"
OBS="deploy/observability"
RULES=("${OBS}/prometheus/dash-recording.rules.yml" "${OBS}/prometheus/dash-alerts.rules.yml")
RUNBOOKS="docs/operations/runbooks"

WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}"' EXIT

fail() {
  echo "check_observability: $*" >&2
  exit 1
}

for tool in jq grep sha256sum; do
  command -v "${tool}" >/dev/null || fail "${tool} is required"
done

# --- promtool ---------------------------------------------------------------
if [[ -n "${PROMTOOL:-}" ]]; then
  promtool="${PROMTOOL}"
else
  case "$(uname -s)-$(uname -m)" in
    Linux-x86_64) platform="linux-amd64" ;;
    Linux-aarch64) platform="linux-arm64" ;;
    *) fail "no pinned promtool for $(uname -s)-$(uname -m); set PROMTOOL" ;;
  esac
  tarball="prometheus-${PROMETHEUS_VERSION}.${platform}.tar.gz"
  base="https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}"
  curl -fsSL --retry 3 -o "${WORK}/sha256sums.txt" "${base}/sha256sums.txt"
  curl -fsSL --retry 3 -o "${WORK}/${tarball}" "${base}/${tarball}"
  (cd "${WORK}" && grep " ${tarball}\$" sha256sums.txt | sha256sum -c --quiet -) \
    || fail "${tarball} does not match the release sha256sums.txt"
  if [[ "${platform}" == "linux-amd64" ]]; then
    echo "${PROMETHEUS_SHA256}  ${WORK}/${tarball}" | sha256sum -c --quiet - \
      || fail "${tarball} does not match the pinned checksum"
  fi
  tar -xzf "${WORK}/${tarball}" -C "${WORK}" "prometheus-${PROMETHEUS_VERSION}.${platform}/promtool"
  promtool="${WORK}/prometheus-${PROMETHEUS_VERSION}.${platform}/promtool"
fi
"${promtool}" --version | head -1

# --- 1, 2: rules ------------------------------------------------------------
"${promtool}" check rules "${RULES[@]}"
"${promtool}" test rules "${OBS}"/prometheus/tests/*.yml

# --- 3: runbooks ------------------------------------------------------------
mapfile -t alerts < <(grep -hoE '^\s*- alert: [A-Za-z0-9]+' "${RULES[@]}" | awk '{print $3}' | sort -u)
[[ "${#alerts[@]}" -gt 0 ]] || fail "no alerts found"
alert_rules=$(grep -c '^\s*- alert: ' "${OBS}/prometheus/dash-alerts.rules.yml")
runbook_urls=$(grep -c '^\s*runbook_url: ' "${OBS}/prometheus/dash-alerts.rules.yml")
[[ "${alert_rules}" == "${runbook_urls}" ]] \
  || fail "${alert_rules} alert rules but ${runbook_urls} runbook_url annotations"
mapfile -t urls < <(grep -hoE 'runbook_url: \S+' "${RULES[@]}" | awk '{print $2}' | sort -u)
for url in "${urls[@]}"; do
  [[ -f "${url}" ]] || fail "runbook ${url} does not exist"
done
for runbook in "${RUNBOOKS}"/*.md; do
  [[ "$(basename "${runbook}")" == "README.md" ]] && continue
  printf '%s\n' "${urls[@]}" | grep -qxF "${runbook}" \
    || fail "runbook ${runbook} is not referenced by any alert"
done
for alert in "${alerts[@]}"; do
  grep -rqF "${alert}" "${RUNBOOKS}" || fail "alert ${alert} is not named in any runbook"
done
echo "runbooks: ${#alerts[@]} alerts, ${#urls[@]} runbooks"

# --- 5: dashboards ----------------------------------------------------------
dashboards=("${OBS}"/grafana/*.json)
[[ "${#dashboards[@]}" -gt 0 ]] || fail "no dashboards found"
for d in "${dashboards[@]}"; do
  jq -e . "${d}" >/dev/null || fail "${d} is not valid JSON"
  jq -e '(.uid | type == "string" and length > 0)
    and (.title | type == "string" and length > 0)
    and (.schemaVersion | type == "number")
    and (.panels | type == "array" and length > 0)
    and ([.templating.list[] | select(.name == "datasource" and .type == "datasource")] | length == 1)' \
    "${d}" >/dev/null || fail "${d}: missing uid, title, schemaVersion, panels or the datasource variable"
  bad=$(jq -r '[.panels[] | select(.type != "row") | select(.datasource.uid != "${datasource}") | .title] | join(", ")' "${d}")
  [[ -z "${bad}" ]] || fail "${d}: panels without the \${datasource} variable: ${bad}"
  empty=$(jq -r '[.panels[] | select(.type != "row") | select((.targets // []) | length == 0 or any(.[]; (.expr // "") == "")) | .title] | join(", ")' "${d}")
  [[ -z "${empty}" ]] || fail "${d}: panels without a query: ${empty}"
  ids=$(jq -r '[.panels[].id] | (length == (unique | length))' "${d}")
  [[ "${ids}" == "true" ]] || fail "${d}: duplicate panel ids"
done
uids=$(jq -r '.uid' "${dashboards[@]}" | sort)
[[ "$(echo "${uids}" | uniq -d)" == "" ]] || fail "duplicate dashboard uids: $(echo "${uids}" | uniq -d)"

# Every dashboard query must parse: wrap them as recording rules (Grafana
# variables replaced by concrete values) and let promtool parse the file.
{
  echo "groups:"
  echo "  - name: dashboard-queries"
  echo "    rules:"
  jq -r '.panels[] | select(.type != "row") | .targets[].expr' "${dashboards[@]}" |
    sed -e 's/\$__rate_interval/5m/g' -e 's/\$job/.*/g' |
    while IFS= read -r expr; do jq -cRn --arg e "${expr}" '$e'; done |
    awk '{ printf "      - record: dashboard:query_%d\n        expr: %s\n", NR, $0 }'
} >"${WORK}/dashboard-queries.yml"
"${promtool}" check rules "${WORK}/dashboard-queries.yml" >"${WORK}/dashboard-queries.log" 2>&1 \
  || { cat "${WORK}/dashboard-queries.log"; fail "a dashboard query does not parse"; }
echo "dashboards: ${#dashboards[@]} valid, $(grep -c 'record: dashboard:query_' "${WORK}/dashboard-queries.yml") queries parse"

# --- 4: metric names exist in the code --------------------------------------
# Names queried by the rules and dashboards that the DASH code emits
# (dash_*, process_*); histogram suffixes are stripped. Recording-rule names
# (dash:...) and metrics from other exporters (up, kubelet_*, node_*, ALERTS)
# are not checked.
mapfile -t metrics < <(
  cat "${RULES[@]}" "${dashboards[@]}" |
    grep -oE '\b(dash|process)_[a-z0-9_]+' |
    sed -E 's/_(bucket|sum|count)$//' | sort -u
)
sources="$(find services pkg -type f -name '*.rs' -not -path 'pkg/config/*')"
missing=()
for metric in "${metrics[@]}"; do
  # shellcheck disable=SC2086
  grep -qF "${metric}" ${sources} || missing+=("${metric}")
done
[[ "${#missing[@]}" -eq 0 ]] || fail "metrics not emitted by the code: ${missing[*]}"
echo "metrics: ${#metrics[@]} referenced names are emitted by the code"

# --- 6: Helm chart copies ---------------------------------------------------
for f in "${RULES[@]}" "${dashboards[@]}"; do
  sub="prometheus"
  [[ "${f}" == *.json ]] && sub="grafana"
  copy="deploy/helm/dash/files/${sub}/$(basename "${f}")"
  cmp -s "${f}" "${copy}" || fail "${copy} differs from ${f} (copy it: cp ${f} ${copy})"
done
extra=$(comm -13 <(for f in "${RULES[@]}" "${dashboards[@]}"; do basename "${f}"; done | sort) \
  <(find deploy/helm/dash/files -type f -exec basename {} \; | sort))
[[ -z "${extra}" ]] || fail "files in deploy/helm/dash/files without a source in ${OBS}: ${extra}"
echo "helm chart copies: identical"

# --- 7: rendered PrometheusRule ---------------------------------------------
if command -v helm >/dev/null; then
  gen() { head -c 32 /dev/urandom | sha256sum | cut -d' ' -f1; }
  helm template dash deploy/helm/dash --namespace dash-system \
    --set "secret.retrieval.apiKey=$(gen)" --set "secret.retrieval.hs256Secret=$(gen)" \
    --set "secret.ingestion.apiKey=$(gen)" --set "secret.ingestion.hs256Secret=$(gen)" \
    --set "secret.replicationToken=$(gen)" --set "secret.controlPlane.token=$(gen)" \
    --set metrics.prometheusRule.enabled=true \
    --show-only templates/monitoring.yaml >"${WORK}/prometheusrule.yaml"
  # spec.groups of the single PrometheusRule document, de-indented to a
  # standalone rules file.
  awk '/^spec:$/ {in_spec=1; next} in_spec && /^---/ {exit} in_spec {print}' \
    "${WORK}/prometheusrule.yaml" | sed 's/^  //' >"${WORK}/rendered.rules.yml"
  "${promtool}" check rules "${WORK}/rendered.rules.yml"
  rendered=$(grep -cE '^\s*(- )?(alert|record): ' "${WORK}/rendered.rules.yml")
  expected=$(cat "${RULES[@]}" | grep -cE '^\s*(- )?(alert|record): ')
  [[ "${rendered}" == "${expected}" ]] \
    || fail "rendered PrometheusRule has ${rendered} rules, expected ${expected}"
  echo "helm PrometheusRule: ${rendered} rules render and parse"
else
  echo "helm not found: skipping the rendered PrometheusRule check"
fi

echo "check_observability: OK"
