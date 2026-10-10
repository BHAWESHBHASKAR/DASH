#!/usr/bin/env bash
# End-to-end test of the DASH Helm chart on a local kind cluster.
#
# Usage:
#   scripts/kind_e2e.sh
#
# What it does (each step fails loudly with a message and a timeout):
#   1. installs pinned kind, helm and kubectl into a tools directory
#      (sha256-verified against the official checksum files);
#   2. creates a kind cluster from a node image pinned by digest;
#   3. builds the DASH image from deploy/container/Dockerfile (or uses a
#      prebuilt one) and loads it into the cluster with `kind load`;
#   4. generates secrets with scripts/generate-secrets.sh and installs the
#      chart (deploy/helm/dash, values deploy/helm/dash/ci/kind-values.yaml)
#      into a namespace that enforces Pod Security "restricted", with
#      NetworkPolicy on and per-pod PVCs on kind's default StorageClass;
#   5. core scenario, through kubectl port-forward:
#        ingest claims with the API key (and check 401 without it),
#        retrieve them from every retrieval pod after replication,
#        delete one and check it disappears everywhere,
#        kill the ingestion pod, check acknowledged data survived on its PVC,
#        kill a retrieval pod (keeps its PVC) and one whose PVC is deleted
#        (resyncs from scratch), and check both recover,
#        back up with scripts/k8s_backup_restore.sh, write more, restore,
#        check the post-backup write is gone and new writes work,
#        `helm upgrade` with changed values (poll interval, 3 retrieval
#        replicas) and check every pod rolled and no data was lost;
#   6. optional: install the chart from an older git ref and `helm upgrade`
#      it to the working-tree chart (DASH_E2E_UPGRADE_FROM_REF);
#   7. raw manifests: `kubectl apply -k deploy/k8s` (image swapped for the
#      e2e image), then ingest -> retrieve on every retrieval pod;
#   8. TLS: installs with tls.enabled=true and a self-signed CA generated
#      here (no cert-manager), then ingest -> retrieve over HTTPS, which
#      also proves mutually authenticated replication.
#   On failure, pod logs, `kubectl describe` output and events are written
#   to the artifacts directory.
#
# Requirements: docker, curl, jq, openssl, tar, sha256sum, git.
#
# Environment:
#   DASH_E2E_CLUSTER          kind cluster name (default: dash-e2e)
#   DASH_E2E_WORK_DIR         scratch directory (default: mktemp -d)
#   DASH_E2E_TOOLS_DIR        where the pinned tools go (default: <work>/bin)
#   DASH_E2E_ARTIFACT_DIR     diagnostics on failure (default: <work>/artifacts)
#   DASH_E2E_SKIP_BUILD=1     do not build; the image must already exist
#   DASH_E2E_IMAGE            image to load (default: dash.local/dash-e2e:e2e);
#                             built from the Dockerfile unless SKIP_BUILD=1
#   DASH_E2E_BUILD_ARGS       extra arguments for `docker buildx build`
#                             (whitespace-separated)
#   DASH_E2E_PHASES           space-separated subset of:
#                             core upgrade-from kustomize tls
#                             (default: "core kustomize tls", plus upgrade-from when
#                             DASH_E2E_UPGRADE_FROM_REF is set)
#   DASH_E2E_UPGRADE_FROM_REF git ref whose chart is installed first and then
#                             upgraded to the working-tree chart
#   DASH_E2E_UPGRADE_FROM_OPTIONAL=1  skip (with a warning) instead of failing
#                             when the chart at that ref does not install
#   DASH_E2E_REUSE_CLUSTER=1  reuse an existing cluster with the same name
#   DASH_E2E_KEEP_CLUSTER=1   do not delete the cluster at the end
#   DASH_E2E_TIMEOUT          seconds for each rollout / replication wait
#                             (default: 300)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHART_DIR="${ROOT_DIR}/deploy/helm/dash"
CI_VALUES="${CHART_DIR}/ci/kind-values.yaml"

# --- pinned tools -------------------------------------------------------------
# kind v0.31.0 and its default node image (pkg/apis/config/defaults/image.go
# at the v0.31.0 tag). Checksums are the published *.sha256sum / *.sha256
# files of each release.
KIND_VERSION="v0.31.0"
KIND_NODE_IMAGE="kindest/node:v1.35.0@sha256:452d707d4862f52530247495d180205e029056831160e22870e37e3f6c1ac31f"
HELM_VERSION="v3.19.2"
KUBECTL_VERSION="v1.35.0"
declare -A TOOL_SHA256=(
  [kind-amd64]="eb244cbafcc157dff60cf68693c14c9a75c4e6e6fedaf9cd71c58117cb93e3fa"
  [kind-arm64]="8e1014e87c34901cc422a1445866835d1e666f2a61301c27e722bdeab5a1f7e4"
  [helm-amd64]="2114c9dea2844dce6d0ee2d792a9aae846be8cf53d5b19dc2988b5a0e8fec26e"
  [helm-arm64]="566e9f3a5a83a81e4b03503ae37e368edd52d699619e8a9bb1fdf21561ae0e88"
  [kubectl-amd64]="a2e984a18a0c063279d692533031c1eff93a262afcc0afdc517375432d060989"
  [kubectl-arm64]="58f82f9fe796c375c5c4b8439850b0f3f4d401a52434052f2df46035a8789e25"
)

# --- configuration ------------------------------------------------------------
CLUSTER="${DASH_E2E_CLUSTER:-dash-e2e}"
WORK_DIR="${DASH_E2E_WORK_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/dash-kind-e2e-XXXXXX")}"
TOOLS_DIR="${DASH_E2E_TOOLS_DIR:-${WORK_DIR}/bin}"
ARTIFACT_DIR="${DASH_E2E_ARTIFACT_DIR:-${WORK_DIR}/artifacts}"
TIMEOUT="${DASH_E2E_TIMEOUT:-300}"
IMAGE_REGISTRY="dash.local"
IMAGE_REPOSITORY="dash"
IMAGE_TAG="e2e"
SOURCE_IMAGE="${DASH_E2E_IMAGE:-${IMAGE_REGISTRY}/${IMAGE_REPOSITORY}-e2e:${IMAGE_TAG}}"
UPGRADE_FROM_REF="${DASH_E2E_UPGRADE_FROM_REF:-}"
DEFAULT_PHASES="core kustomize tls"
if [[ -n "${UPGRADE_FROM_REF}" ]]; then
  DEFAULT_PHASES="core upgrade-from kustomize tls"
fi
PHASES="${DASH_E2E_PHASES:-${DEFAULT_PHASES}}"
TENANT="e2e-tenant"
QUERY="orbital station maintenance"

mkdir -p "${WORK_DIR}" "${TOOLS_DIR}" "${ARTIFACT_DIR}"
export PATH="${TOOLS_DIR}:${PATH}"
export KUBECONFIG="${WORK_DIR}/kubeconfig"

START_TS="$(date +%s)"
CURRENT_STEP="setup"
CLUSTER_CREATED=0
SKIPPED=""
declare -A PF_PID=()
declare -A PF_PORT=()

# --- output helpers -----------------------------------------------------------
log() { printf '[kind-e2e %4ss] %s\n' "$(( $(date +%s) - START_TS ))" "$*"; }
step() { CURRENT_STEP="$*"; log "=== ${CURRENT_STEP}"; }
fail() {
  echo "[kind-e2e] FAILED in step '${CURRENT_STEP}': $*" >&2
  exit 1
}

need() {
  command -v "$1" >/dev/null 2>&1 || fail "$1 is required but not installed"
}

# --- tools --------------------------------------------------------------------
arch() {
  case "$(uname -m)" in
    x86_64|amd64) echo amd64 ;;
    aarch64|arm64) echo arm64 ;;
    *) fail "unsupported architecture $(uname -m) (need amd64 or arm64)" ;;
  esac
}

# fetch URL DEST SHA256: download unless DEST already has that checksum.
fetch_verified() {
  local url="$1" dest="$2" sum="$3"
  if [[ -f "${dest}" ]] && echo "${sum}  ${dest}" | sha256sum -c --quiet - >/dev/null 2>&1; then
    return 0
  fi
  curl -fsSL --retry 3 --max-time 300 -o "${dest}.part" "${url}" || fail "download failed: ${url}"
  if ! echo "${sum}  ${dest}.part" | sha256sum -c --quiet -; then
    rm -f "${dest}.part"
    fail "checksum mismatch for ${url} (expected ${sum})"
  fi
  mv "${dest}.part" "${dest}"
}

install_tools() {
  step "install pinned kind ${KIND_VERSION}, helm ${HELM_VERSION}, kubectl ${KUBECTL_VERSION}"
  local a
  a="$(arch)"
  fetch_verified "https://github.com/kubernetes-sigs/kind/releases/download/${KIND_VERSION}/kind-linux-${a}" \
    "${TOOLS_DIR}/kind" "${TOOL_SHA256[kind-${a}]}"
  chmod 0755 "${TOOLS_DIR}/kind"
  fetch_verified "https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/${a}/kubectl" \
    "${TOOLS_DIR}/kubectl" "${TOOL_SHA256[kubectl-${a}]}"
  chmod 0755 "${TOOLS_DIR}/kubectl"
  fetch_verified "https://get.helm.sh/helm-${HELM_VERSION}-linux-${a}.tar.gz" \
    "${TOOLS_DIR}/helm.tar.gz" "${TOOL_SHA256[helm-${a}]}"
  tar -xzf "${TOOLS_DIR}/helm.tar.gz" -C "${TOOLS_DIR}" --strip-components=1 "linux-${a}/helm"
  chmod 0755 "${TOOLS_DIR}/helm"
  kind version
  helm version --short
  kubectl version --client
}

# --- diagnostics and cleanup --------------------------------------------------
collect_diagnostics() {
  log "collecting diagnostics into ${ARTIFACT_DIR}"
  local d="${ARTIFACT_DIR}/cluster"
  mkdir -p "${d}"
  kubectl get nodes -o wide > "${d}/nodes.txt" 2>&1 || true
  kubectl get all,pvc,pv,networkpolicy,pdb,configmap,secret -A -o wide > "${d}/resources.txt" 2>&1 || true
  kubectl get events -A --sort-by=.lastTimestamp > "${d}/events.txt" 2>&1 || true
  local ns pod
  for ns in $(kubectl get ns -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    case "${ns}" in dash-*|local-path-storage|kube-system) ;; *) continue ;; esac
    mkdir -p "${d}/${ns}"
    kubectl -n "${ns}" describe pods > "${d}/${ns}/describe-pods.txt" 2>&1 || true
    kubectl -n "${ns}" describe statefulsets,pvc,services,networkpolicies > "${d}/${ns}/describe-other.txt" 2>&1 || true
    for pod in $(kubectl -n "${ns}" get pods -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
      kubectl -n "${ns}" logs "${pod}" --all-containers --timestamps > "${d}/${ns}/${pod}.log" 2>&1 || true
      kubectl -n "${ns}" logs "${pod}" --all-containers --timestamps --previous \
        > "${d}/${ns}/${pod}.previous.log" 2>/dev/null || rm -f "${d}/${ns}/${pod}.previous.log"
    done
  done
  cp "${WORK_DIR}"/pf-*.log "${d}/" 2>/dev/null || true
  helm list -A > "${d}/helm-list.txt" 2>&1 || true
  kind export logs "${ARTIFACT_DIR}/kind" --name "${CLUSTER}" >/dev/null 2>&1 || true
}

pf_stop_all() {
  local key
  for key in "${!PF_PID[@]}"; do
    kill "${PF_PID[${key}]}" 2>/dev/null || true
  done
  PF_PID=()
  PF_PORT=()
}

on_exit() {
  local rc=$?
  set +e
  pf_stop_all
  if [[ "${rc}" -ne 0 ]]; then
    echo "[kind-e2e] exit ${rc} during step '${CURRENT_STEP}'" >&2
    if command -v kubectl >/dev/null 2>&1 && [[ -f "${KUBECONFIG}" ]]; then
      collect_diagnostics
    fi
  fi
  if [[ "${CLUSTER_CREATED}" -eq 1 && "${DASH_E2E_KEEP_CLUSTER:-0}" != "1" ]]; then
    log "deleting kind cluster ${CLUSTER}"
    kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1
  fi
  # Generated secrets and keys never outlive the run.
  rm -rf "${WORK_DIR}/secrets" "${WORK_DIR}/tls" "${WORK_DIR}/backup"
  if [[ "${rc}" -eq 0 ]]; then
    log "PASSED (phases: ${PHASES}${SKIPPED:+; skipped:${SKIPPED}}) in $(( $(date +%s) - START_TS ))s"
  else
    echo "[kind-e2e] FAILED; diagnostics in ${ARTIFACT_DIR}" >&2
  fi
  exit "${rc}"
}
trap on_exit EXIT

# --- cluster and image --------------------------------------------------------
create_cluster() {
  step "create kind cluster ${CLUSTER} (${KIND_NODE_IMAGE})"
  if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
    if [[ "${DASH_E2E_REUSE_CLUSTER:-0}" == "1" ]]; then
      kind export kubeconfig --name "${CLUSTER}" --kubeconfig "${KUBECONFIG}" >/dev/null
      return 0
    fi
    fail "kind cluster ${CLUSTER} already exists (delete it, or set DASH_E2E_REUSE_CLUSTER=1)"
  fi
  CLUSTER_CREATED=1
  local config="${WORK_DIR}/kind-config.yaml"
  cat > "${config}" <<'EOF'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
EOF
  # Kubernetes 1.35 refuses to start the kubelet on cgroup v1 hosts unless
  # told otherwise. CI runners use cgroup v2; this keeps older hosts usable.
  if [[ "$(stat -fc %T /sys/fs/cgroup 2>/dev/null || true)" != "cgroup2fs" ]]; then
    log "host uses cgroup v1: setting failCgroupV1=false on the kubelet"
    cat >> "${config}" <<'EOF'
kubeadmConfigPatches:
  - |
    kind: KubeletConfiguration
    failCgroupV1: false
EOF
  fi
  # Some sandboxed VMs forbid lowering oom_score_adj even for root; runc then
  # fails every pod sandbox ("can't get final child's PID from pipe").
  # restrict_oom_score_adj makes containerd clamp instead of failing.
  local restrict_oom="${DASH_E2E_RESTRICT_OOM_SCORE_ADJ:-0}"
  if [[ "$(id -u)" -eq 0 ]] && ! (echo -1 > /proc/self/oom_score_adj) 2>/dev/null; then
    restrict_oom=1
  fi
  if [[ "${restrict_oom}" == "1" ]]; then
    log "oom_score_adj cannot be lowered here: setting containerd restrict_oom_score_adj=true"
    cat >> "${config}" <<'EOF'
containerdConfigPatches:
  - |-
    [plugins."io.containerd.grpc.v1.cri"]
      restrict_oom_score_adj = true
EOF
  fi
  kind create cluster --name "${CLUSTER}" --image "${KIND_NODE_IMAGE}" --config "${config}" \
    --kubeconfig "${KUBECONFIG}" --wait "${TIMEOUT}s" \
    || fail "kind create cluster failed"
  kubectl wait --for=condition=Ready nodes --all --timeout="${TIMEOUT}s" >/dev/null \
    || fail "nodes not ready within ${TIMEOUT}s"
  kubectl get storageclass
}

build_and_load_image() {
  if [[ "${IMAGE_BUILT:-0}" == "1" ]]; then
    step "reuse image ${SOURCE_IMAGE} built earlier in this run"
  elif [[ "${DASH_E2E_SKIP_BUILD:-0}" == "1" ]]; then
    step "use prebuilt image ${SOURCE_IMAGE}"
    docker image inspect "${SOURCE_IMAGE}" >/dev/null 2>&1 \
      || fail "DASH_E2E_SKIP_BUILD=1 but image ${SOURCE_IMAGE} does not exist locally"
  else
    step "build ${SOURCE_IMAGE} from deploy/container/Dockerfile"
    local extra=()
    if [[ -n "${DASH_E2E_BUILD_ARGS:-}" ]]; then
      read -r -a extra <<<"${DASH_E2E_BUILD_ARGS}"
    fi
    docker buildx build --load \
      -f "${ROOT_DIR}/deploy/container/Dockerfile" \
      --target runtime \
      --build-arg SERVICE=retrieval \
      -t "${SOURCE_IMAGE}" \
      "${extra[@]}" \
      "${ROOT_DIR}" || fail "docker build failed"
  fi
  IMAGE_BUILT=1
  # One image carries every binary; the chart sets DASH_BIN per workload, so
  # the per-service names the chart expects are tags of the same image.
  step "load the image into kind as ${IMAGE_REGISTRY}/${IMAGE_REPOSITORY}-{ingestion,retrieval,control-plane}:${IMAGE_TAG}"
  local svc names=()
  for svc in ingestion retrieval control-plane; do
    docker tag "${SOURCE_IMAGE}" "${IMAGE_REGISTRY}/${IMAGE_REPOSITORY}-${svc}:${IMAGE_TAG}"
    names+=("${IMAGE_REGISTRY}/${IMAGE_REPOSITORY}-${svc}:${IMAGE_TAG}")
  done
  kind load docker-image --name "${CLUSTER}" "${names[@]}" || fail "kind load docker-image failed"
}

# --- secrets ------------------------------------------------------------------
SECRET_VALUES=""
generate_secrets() {
  step "generate secrets (scripts/generate-secrets.sh)"
  mkdir -p "${WORK_DIR}/secrets"
  chmod 0700 "${WORK_DIR}/secrets"
  local env_file="${WORK_DIR}/secrets/dash.env"
  bash "${ROOT_DIR}/scripts/generate-secrets.sh" --force --output "${env_file}" >/dev/null
  # shellcheck disable=SC1090
  source "${env_file}"
  [[ "${DASH_INGEST_REPLICATION_TOKEN}" == "${DASH_RETRIEVAL_REPLICATION_TOKEN}" ]] \
    || fail "generate-secrets.sh wrote different replication tokens"
  INGEST_KEY="${DASH_INGEST_API_KEY}"
  RETRIEVAL_KEY="${DASH_RETRIEVAL_API_KEY}"
  SECRET_VALUES="${WORK_DIR}/secrets/values.yaml"
  (
    umask 077
    cat > "${SECRET_VALUES}" <<EOF
secret:
  replicationToken: "${DASH_INGEST_REPLICATION_TOKEN}"
  retrieval:
    apiKey: "${DASH_RETRIEVAL_API_KEY}"
    hs256Secret: "${DASH_RETRIEVAL_JWT_HS256_SECRET}"
  ingestion:
    apiKey: "${DASH_INGEST_API_KEY}"
    hs256Secret: "${DASH_INGEST_JWT_HS256_SECRET}"
  controlPlane:
    token: "${DASH_CONTROL_PLANE_TOKEN}"
EOF
  )
}

# --- release helpers ----------------------------------------------------------
# Release names below all contain "dash", so the chart's fullname is the
# release name and components are <release>-<component>.
create_namespace() {
  local ns="$1"
  kubectl apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: ${ns}
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/enforce-version: latest
EOF
}

wait_release() {
  local ns="$1" rel="$2" sts
  for sts in $(kubectl -n "${ns}" get statefulset -l "app.kubernetes.io/instance=${rel}" \
      -o jsonpath='{.items[*].metadata.name}'); do
    kubectl -n "${ns}" rollout status "statefulset/${sts}" --timeout="${TIMEOUT}s" \
      || fail "statefulset ${ns}/${sts} not ready within ${TIMEOUT}s"
  done
}

# helm_deploy install|upgrade NS RELEASE CHART [helm args...]
helm_deploy() {
  local verb="$1" ns="$2" rel="$3" chart="$4"
  shift 4
  helm "${verb}" "${rel}" "${chart}" --namespace "${ns}" \
    -f "${CI_VALUES}" -f "${SECRET_VALUES}" \
    --set "namespace.name=${ns}" \
    --wait --timeout "${TIMEOUT}s" "$@" \
    || fail "helm ${verb} ${rel} failed"
  wait_release "${ns}" "${rel}"
  kubectl -n "${ns}" get pods,pvc -o wide
}

delete_namespace() {
  local ns="$1"
  kubectl delete namespace "${ns}" --wait=true --timeout="${TIMEOUT}s" >/dev/null 2>&1 \
    || log "warning: namespace ${ns} did not finish deleting within ${TIMEOUT}s"
}

pod_uid() { kubectl -n "$1" get pod "$2" -o jsonpath='{.metadata.uid}'; }

# --- port-forward -------------------------------------------------------------
# pf_start KEY NS TARGET REMOTE_PORT: forward a free local port to TARGET
# (pod/<name> or svc/<name>) and record it in PF_PORT[KEY].
pf_start() {
  local key="$1" ns="$2" target="$3" remote="$4" logf port="" i
  pf_stop "${key}"
  logf="${WORK_DIR}/pf-${key}.log"
  kubectl -n "${ns}" port-forward "${target}" ":${remote}" --address 127.0.0.1 >"${logf}" 2>&1 &
  PF_PID[${key}]=$!
  for ((i = 0; i < 60; i++)); do
    port="$(sed -n 's/^Forwarding from 127\.0\.0\.1:\([0-9]*\) .*/\1/p' "${logf}" | head -n 1)"
    [[ -n "${port}" ]] && break
    kill -0 "${PF_PID[${key}]}" 2>/dev/null || break
    sleep 0.5
  done
  [[ -n "${port}" ]] || { cat "${logf}" >&2; fail "port-forward to ${ns}/${target} did not start"; }
  PF_PORT[${key}]="${port}"
}

pf_stop() {
  local key="$1"
  if [[ -n "${PF_PID[${key}]:-}" ]]; then
    kill "${PF_PID[${key}]}" 2>/dev/null || true
    wait "${PF_PID[${key}]}" 2>/dev/null || true
    unset "PF_PID[${key}]" "PF_PORT[${key}]"
  fi
}

# --- HTTP helpers -------------------------------------------------------------
# Per-target TLS settings: TLS_HOST[KEY] is the certificate name to request
# (resolved to 127.0.0.1) and TLS_CA the CA file; plain http otherwise.
declare -A TLS_HOST=()
TLS_CA=""
HTTP_STATUS=""
HTTP_BODY=""

# http KEY METHOD PATH API_KEY [JSON]
http() {
  local key="$1" method="$2" path="$3" api_key="$4" body="${5:-}"
  local port="${PF_PORT[${key}]:-}" url args=() out="${WORK_DIR}/http-body"
  [[ -n "${port}" ]] || fail "no port-forward for ${key}"
  if [[ -n "${TLS_HOST[${key}]:-}" ]]; then
    url="https://${TLS_HOST[${key}]}:${port}${path}"
    args+=(--cacert "${TLS_CA}" --resolve "${TLS_HOST[${key}]}:${port}:127.0.0.1")
  else
    url="http://127.0.0.1:${port}${path}"
  fi
  if [[ -n "${api_key}" ]]; then
    args+=(-H "x-api-key: ${api_key}")
  fi
  if [[ -n "${body}" ]]; then
    args+=(-H 'content-type: application/json' --data-binary "${body}")
  fi
  HTTP_STATUS="$(curl -sS --max-time 15 -o "${out}" -w '%{http_code}' -X "${method}" "${args[@]}" "${url}" 2>"${WORK_DIR}/http-err")" \
    || HTTP_STATUS="000"
  HTTP_BODY="$(cat "${out}" 2>/dev/null || true)"
  if [[ "${HTTP_STATUS}" == "000" ]]; then
    HTTP_BODY="$(cat "${WORK_DIR}/http-err")"
  fi
}

expect_status() {
  local want="$1" what="$2"
  [[ "${HTTP_STATUS}" == "${want}" ]] \
    || fail "${what}: expected HTTP ${want}, got ${HTTP_STATUS}: ${HTTP_BODY:0:500}"
}

wait_http_ok() {
  local key="$1" path="$2" deadline=$(( $(date +%s) + TIMEOUT ))
  while :; do
    http "${key}" GET "${path}" ""
    [[ "${HTTP_STATUS}" == "200" ]] && return 0
    (( $(date +%s) < deadline )) || fail "${key}${path} not 200 within ${TIMEOUT}s (last: ${HTTP_STATUS} ${HTTP_BODY:0:300})"
    sleep 1
  done
}

ingest_claim() {
  local key="$1" id="$2" n="$3"
  local body
  body="$(jq -cn --arg id "${id}" --arg t "${TENANT}" --arg text "Claim ${n}: the orbital station maintenance log entry ${id}" '
    {claim: {claim_id: $id, tenant_id: $t, canonical_text: $text, confidence: 0.9},
     evidence: [{evidence_id: ("ev-" + $id), claim_id: $id, source_id: ("src-" + $id),
                 stance: "supports", source_quality: 0.8}]}')"
  http "${key}" POST /v1/ingest "${INGEST_KEY}" "${body}"
  expect_status 200 "ingest ${id}"
}

delete_claim() {
  local key="$1" id="$2"
  http "${key}" DELETE "/v1/claims/${id}?tenant_id=${TENANT}" "${INGEST_KEY}"
  expect_status 200 "delete ${id}"
  [[ "$(jq -r '.deleted' <<<"${HTTP_BODY}")" == "true" ]] \
    || fail "delete ${id}: response does not say deleted=true: ${HTTP_BODY}"
}

# Sorted, comma-separated claim ids a retrieval target returns for QUERY.
retrieved_ids() {
  local key="$1" body
  body="$(jq -cn --arg t "${TENANT}" --arg q "${QUERY}" '{tenant_id: $t, query: $q, top_k: 100}')"
  http "${key}" POST /v1/retrieve "${RETRIEVAL_KEY}" "${body}"
  if [[ "${HTTP_STATUS}" != "200" ]]; then
    echo "<http ${HTTP_STATUS}>"
    return 0
  fi
  jq -r '[.results[].claim_id] | sort | join(",")' <<<"${HTTP_BODY}"
}

# wait_ids KEY EXPECTED: poll until the target returns exactly EXPECTED.
wait_ids() {
  local key="$1" want="$2" got="" deadline=$(( $(date +%s) + TIMEOUT ))
  while :; do
    got="$(retrieved_ids "${key}")"
    if [[ "${got}" == "${want}" ]]; then
      log "${key}: claims [${got}]"
      return 0
    fi
    (( $(date +%s) < deadline )) \
      || fail "${key}: expected claims [${want}], still [${got}] after ${TIMEOUT}s"
    sleep 1
  done
}

csv() { local IFS=,; printf '%s' "$*"; }

# --- core scenario ------------------------------------------------------------
NS_CORE="dash-system"
REL_CORE="dash"

forward_core() {
  # Ingestion through its Service; every retrieval pod individually.
  pf_start ing "${NS_CORE}" "svc/${REL_CORE}-ingestion" 80
  wait_http_ok ing /v1/ready
  local i n
  n="$(kubectl -n "${NS_CORE}" get statefulset "${REL_CORE}-retrieval" -o jsonpath='{.spec.replicas}')"
  for ((i = 0; i < n; i++)); do
    pf_start "ret${i}" "${NS_CORE}" "pod/${REL_CORE}-retrieval-${i}" 8080
    wait_http_ok "ret${i}" /v1/ready
  done
  RETRIEVAL_REPLICAS="${n}"
}

wait_all_followers() {
  local want="$1" i
  for ((i = 0; i < RETRIEVAL_REPLICAS; i++)); do
    wait_ids "ret${i}" "${want}"
  done
}

phase_core() {
  step "core: helm install ${REL_CORE} into ${NS_CORE}"
  create_namespace "${NS_CORE}"
  helm_deploy install "${NS_CORE}" "${REL_CORE}" "${CHART_DIR}"
  forward_core

  step "core: authentication is enforced"
  http ing POST /v1/ingest "" '{"claim":{"claim_id":"x","tenant_id":"x","canonical_text":"x","confidence":0.5}}'
  expect_status 401 "ingest without an API key"
  http ret0 POST /v1/retrieve "" "{\"tenant_id\":\"${TENANT}\",\"query\":\"x\"}"
  expect_status 401 "retrieve without an API key"
  http ret0 POST /v1/retrieve "${INGEST_KEY}" "{\"tenant_id\":\"${TENANT}\",\"query\":\"x\"}"
  [[ "${HTTP_STATUS}" == "401" || "${HTTP_STATUS}" == "403" ]] \
    || fail "retrieve with the ingestion key: expected 401/403, got ${HTTP_STATUS}"

  step "core: ingest 5 claims, retrieve them from every retrieval pod"
  local i
  for i in 1 2 3 4 5; do
    ingest_claim ing "claim-${i}" "${i}"
  done
  wait_all_followers "$(csv claim-1 claim-2 claim-3 claim-4 claim-5)"

  step "core: delete claim-3, check it is gone everywhere"
  delete_claim ing claim-3
  wait_all_followers "$(csv claim-1 claim-2 claim-4 claim-5)"

  step "core: kill the ingestion pod; acknowledged data must survive on its PVC"
  local old_uid
  old_uid="$(pod_uid "${NS_CORE}" "${REL_CORE}-ingestion-0")"
  pf_stop ing
  kubectl -n "${NS_CORE}" delete pod "${REL_CORE}-ingestion-0" --grace-period=1 --wait=true \
    --timeout="${TIMEOUT}s" >/dev/null || fail "could not delete the ingestion pod"
  wait_release "${NS_CORE}" "${REL_CORE}"
  [[ "$(pod_uid "${NS_CORE}" "${REL_CORE}-ingestion-0")" != "${old_uid}" ]] \
    || fail "ingestion pod was not replaced"
  pf_start ing "${NS_CORE}" "svc/${REL_CORE}-ingestion" 80
  wait_http_ok ing /v1/ready
  ingest_claim ing claim-6 6
  wait_all_followers "$(csv claim-1 claim-2 claim-4 claim-5 claim-6)"

  step "core: kill retrieval-0 (keeps its PVC); it must recover and keep following"
  pf_stop ret0
  kubectl -n "${NS_CORE}" delete pod "${REL_CORE}-retrieval-0" --wait=true --timeout="${TIMEOUT}s" >/dev/null \
    || fail "could not delete retrieval-0"
  wait_release "${NS_CORE}" "${REL_CORE}"
  pf_start ret0 "${NS_CORE}" "pod/${REL_CORE}-retrieval-0" 8080
  wait_http_ok ret0 /v1/ready
  wait_ids ret0 "$(csv claim-1 claim-2 claim-4 claim-5 claim-6)"

  step "core: delete retrieval-1 and its PVC; it must resync everything from ingestion"
  local old_pvc
  old_pvc="$(kubectl -n "${NS_CORE}" get pvc "data-${REL_CORE}-retrieval-1" -o jsonpath='{.metadata.uid}')"
  pf_stop ret1
  kubectl -n "${NS_CORE}" delete pvc "data-${REL_CORE}-retrieval-1" --wait=false >/dev/null
  kubectl -n "${NS_CORE}" delete pod "${REL_CORE}-retrieval-1" --wait=true --timeout="${TIMEOUT}s" >/dev/null \
    || fail "could not delete retrieval-1"
  # The StatefulSet controller recreates the pod and, once the old claim is
  # gone, a fresh PVC from the template.
  local deadline=$(( $(date +%s) + TIMEOUT )) new_pvc=""
  while :; do
    new_pvc="$(kubectl -n "${NS_CORE}" get pvc "data-${REL_CORE}-retrieval-1" -o jsonpath='{.metadata.uid}' 2>/dev/null || true)"
    [[ -n "${new_pvc}" && "${new_pvc}" != "${old_pvc}" ]] && break
    (( $(date +%s) < deadline )) || fail "retrieval-1 did not get a fresh PVC within ${TIMEOUT}s"
    sleep 2
  done
  wait_release "${NS_CORE}" "${REL_CORE}"
  pf_start ret1 "${NS_CORE}" "pod/${REL_CORE}-retrieval-1" 8080
  wait_http_ok ret1 /v1/ready
  wait_ids ret1 "$(csv claim-1 claim-2 claim-4 claim-5 claim-6)"

  step "core: back up ingestion (scripts/k8s_backup_restore.sh backup)"
  pf_stop ing
  bash "${ROOT_DIR}/scripts/k8s_backup_restore.sh" backup --namespace "${NS_CORE}" --release "${REL_CORE}" \
    --out-dir "${WORK_DIR}/backup" --label e2e --timeout "${TIMEOUT}" || fail "backup failed"
  [[ -s "${WORK_DIR}/backup/dash-backup-e2e.tar.gz" ]] || fail "backup bundle missing"
  tar -tzf "${WORK_DIR}/backup/dash-backup-e2e.tar.gz"
  pf_start ing "${NS_CORE}" "svc/${REL_CORE}-ingestion" 80
  wait_http_ok ing /v1/ready
  wait_all_followers "$(csv claim-1 claim-2 claim-4 claim-5 claim-6)"

  step "core: write claim-7 after the backup"
  ingest_claim ing claim-7 7
  wait_all_followers "$(csv claim-1 claim-2 claim-4 claim-5 claim-6 claim-7)"

  step "core: restore the backup (scripts/k8s_backup_restore.sh restore)"
  pf_stop ing
  bash "${ROOT_DIR}/scripts/k8s_backup_restore.sh" restore --namespace "${NS_CORE}" --release "${REL_CORE}" \
    --bundle "${WORK_DIR}/backup/dash-backup-e2e.tar.gz" --timeout "${TIMEOUT}" || fail "restore failed"
  pf_start ing "${NS_CORE}" "svc/${REL_CORE}-ingestion" 80
  wait_http_ok ing /v1/ready
  # Followers see a new WAL lineage and resync: claim-7 must disappear.
  wait_all_followers "$(csv claim-1 claim-2 claim-4 claim-5 claim-6)"
  ingest_claim ing claim-8 8
  wait_all_followers "$(csv claim-1 claim-2 claim-4 claim-5 claim-6 claim-8)"

  step "core: helm upgrade with changed values (poll interval, 3 retrieval replicas)"
  local before_uids=() p
  for p in "${REL_CORE}-ingestion-0" "${REL_CORE}-retrieval-0" "${REL_CORE}-retrieval-1" "${REL_CORE}-control-plane-0"; do
    before_uids+=("${p}=$(pod_uid "${NS_CORE}" "${p}")")
  done
  pf_stop_all
  helm_deploy upgrade "${NS_CORE}" "${REL_CORE}" "${CHART_DIR}" \
    --set config.replication.pollIntervalMs=300 --set replicas.retrieval=3
  local entry
  for entry in "${before_uids[@]}"; do
    p="${entry%%=*}"
    [[ "$(pod_uid "${NS_CORE}" "${p}")" != "${entry#*=}" ]] \
      || fail "${p} was not restarted by a helm upgrade that changed the ConfigMap"
  done
  [[ "$(kubectl -n "${NS_CORE}" exec "${REL_CORE}-retrieval-0" -c retrieval -- printenv DASH_RETRIEVAL_REPLICATION_POLL_INTERVAL_MS)" == "300" ]] \
    || fail "retrieval-0 does not run with the upgraded configuration"
  forward_core
  [[ "${RETRIEVAL_REPLICAS}" == "3" ]] || fail "expected 3 retrieval replicas after the upgrade"
  wait_all_followers "$(csv claim-1 claim-2 claim-4 claim-5 claim-6 claim-8)"
  ingest_claim ing claim-9 9
  wait_all_followers "$(csv claim-1 claim-2 claim-4 claim-5 claim-6 claim-8 claim-9)"
  helm history "${REL_CORE}" --namespace "${NS_CORE}"

  pf_stop_all
  step "core: uninstall"
  helm uninstall "${REL_CORE}" --namespace "${NS_CORE}" --wait --timeout "${TIMEOUT}s" >/dev/null || true
  delete_namespace "${NS_CORE}"
}

# --- upgrade from an older chart ----------------------------------------------
phase_upgrade_from() {
  [[ -n "${UPGRADE_FROM_REF}" ]] || fail "phase upgrade-from needs DASH_E2E_UPGRADE_FROM_REF"
  local ns="dash-upgrade" rel="dash-upgrade" old_chart="${WORK_DIR}/chart-${UPGRADE_FROM_REF//\//_}"
  step "upgrade-from: extract the chart at ${UPGRADE_FROM_REF}"
  git -C "${ROOT_DIR}" rev-parse --verify --quiet "${UPGRADE_FROM_REF}^{commit}" >/dev/null \
    || fail "git ref ${UPGRADE_FROM_REF} not found (fetch it first)"
  rm -rf "${old_chart}"
  mkdir -p "${old_chart}"
  git -C "${ROOT_DIR}" archive "${UPGRADE_FROM_REF}" deploy/helm/dash | tar -x -C "${old_chart}"
  [[ -f "${old_chart}/deploy/helm/dash/Chart.yaml" ]] || fail "no chart at ${UPGRADE_FROM_REF}"

  step "upgrade-from: install the ${UPGRADE_FROM_REF} chart"
  create_namespace "${ns}"
  # A base chart that cannot install on its own is not something the
  # upgrade under test can fix; with DASH_E2E_UPGRADE_FROM_OPTIONAL=1 that
  # case is reported and the phase is skipped instead of failing.
  if ! helm install "${rel}" "${old_chart}/deploy/helm/dash" --namespace "${ns}" \
      -f "${CI_VALUES}" -f "${SECRET_VALUES}" --set "namespace.name=${ns}" \
      --set replicas.retrieval=1 --wait --timeout "${TIMEOUT}s"; then
    if [[ "${DASH_E2E_UPGRADE_FROM_OPTIONAL:-0}" == "1" ]]; then
      log "WARNING: the chart at ${UPGRADE_FROM_REF} does not install on its own; skipping the upgrade-from phase"
      SKIPPED+=" upgrade-from"
      if [[ -n "${GITHUB_ACTIONS:-}" ]]; then
        echo "::warning::kind e2e: the chart at ${UPGRADE_FROM_REF} does not install; upgrade-from phase skipped"
      fi
      kubectl -n "${ns}" get pods -o wide || true
      helm uninstall "${rel}" --namespace "${ns}" --wait --timeout "${TIMEOUT}s" >/dev/null 2>&1 || true
      delete_namespace "${ns}"
      return 0
    fi
    fail "helm install of the ${UPGRADE_FROM_REF} chart failed"
  fi
  wait_release "${ns}" "${rel}"
  pf_start ing "${ns}" "svc/${rel}-ingestion" 80
  pf_start ret0 "${ns}" "pod/${rel}-retrieval-0" 8080
  wait_http_ok ing /v1/ready
  wait_http_ok ret0 /v1/ready
  ingest_claim ing claim-1 1
  ingest_claim ing claim-2 2
  wait_ids ret0 "$(csv claim-1 claim-2)"

  step "upgrade-from: helm upgrade to the working-tree chart"
  pf_stop_all
  helm_deploy upgrade "${ns}" "${rel}" "${CHART_DIR}" --set replicas.retrieval=1
  pf_start ing "${ns}" "svc/${rel}-ingestion" 80
  pf_start ret0 "${ns}" "pod/${rel}-retrieval-0" 8080
  wait_http_ok ing /v1/ready
  wait_http_ok ret0 /v1/ready
  wait_ids ret0 "$(csv claim-1 claim-2)"
  ingest_claim ing claim-3 3
  wait_ids ret0 "$(csv claim-1 claim-2 claim-3)"
  pf_stop_all
  helm uninstall "${rel}" --namespace "${ns}" --wait --timeout "${TIMEOUT}s" >/dev/null || true
  delete_namespace "${ns}"
}

# --- raw manifests (kustomize) ------------------------------------------------
phase_kustomize() {
  local ns="dash-system" overlay="${WORK_DIR}/kustomize-overlay" svc
  step "kustomize: apply deploy/k8s with the e2e image"
  rm -rf "${overlay}"
  mkdir -p "${overlay}"
  {
    echo "apiVersion: kustomize.config.k8s.io/v1beta1"
    echo "kind: Kustomization"
    echo "resources:"
    # kustomize only accepts a relative path to a base.
    echo "  - $(realpath --relative-to="${overlay}" "${ROOT_DIR}/deploy/k8s")"
    echo "images:"
    for svc in ingestion retrieval control-plane; do
      echo "  - name: ghcr.io/bhaweshbhaskar/dash-${svc}"
      echo "    newName: ${IMAGE_REGISTRY}/${IMAGE_REPOSITORY}-${svc}"
      echo "    newTag: \"${IMAGE_TAG}\""
    done
  } > "${overlay}/kustomization.yaml"
  kubectl apply -f "${ROOT_DIR}/deploy/k8s/00-namespace.yaml" >/dev/null
  # The Secrets the manifests expect (deploy/k8s/11-secrets.yaml).
  kubectl -n "${ns}" create secret generic dash-retrieval-secrets \
    --from-literal=DASH_RETRIEVAL_API_KEY="${DASH_RETRIEVAL_API_KEY}" \
    --from-literal=DASH_RETRIEVAL_JWT_HS256_SECRET="${DASH_RETRIEVAL_JWT_HS256_SECRET}" \
    --from-literal=DASH_RETRIEVAL_REPLICATION_TOKEN="${DASH_RETRIEVAL_REPLICATION_TOKEN}" >/dev/null
  kubectl -n "${ns}" create secret generic dash-ingestion-secrets \
    --from-literal=DASH_INGEST_API_KEY="${DASH_INGEST_API_KEY}" \
    --from-literal=DASH_INGEST_JWT_HS256_SECRET="${DASH_INGEST_JWT_HS256_SECRET}" \
    --from-literal=DASH_INGEST_REPLICATION_TOKEN="${DASH_INGEST_REPLICATION_TOKEN}" >/dev/null
  kubectl -n "${ns}" create secret generic dash-control-plane-secrets \
    --from-literal=DASH_CONTROL_PLANE_TOKEN="${DASH_CONTROL_PLANE_TOKEN}" >/dev/null
  kubectl apply -k "${overlay}" >/dev/null || fail "kubectl apply -k deploy/k8s failed"
  local sts
  for sts in dash-ingestion dash-retrieval dash-control-plane; do
    kubectl -n "${ns}" rollout status "statefulset/${sts}" --timeout="${TIMEOUT}s" \
      || fail "statefulset ${ns}/${sts} (raw manifests) not ready within ${TIMEOUT}s"
  done
  kubectl -n "${ns}" get pods,pvc -o wide

  step "kustomize: ingest -> retrieve on every retrieval pod"
  pf_start ing "${ns}" svc/dash-ingestion 80
  pf_start ret0 "${ns}" pod/dash-retrieval-0 8080
  pf_start ret1 "${ns}" pod/dash-retrieval-1 8080
  wait_http_ok ing /v1/ready
  ingest_claim ing k-claim-1 1
  ingest_claim ing k-claim-2 2
  wait_ids ret0 "$(csv k-claim-1 k-claim-2)"
  wait_ids ret1 "$(csv k-claim-1 k-claim-2)"
  pf_stop_all
  delete_namespace "${ns}"
}

# --- TLS ----------------------------------------------------------------------
generate_tls() {
  local ns="$1" rel="$2" dir="${WORK_DIR}/tls" san="" c
  mkdir -p "${dir}"
  chmod 0700 "${dir}"
  for c in ingestion retrieval control-plane; do
    san+="DNS:${rel}-${c},DNS:${rel}-${c}.${ns},DNS:${rel}-${c}.${ns}.svc,DNS:${rel}-${c}.${ns}.svc.cluster.local,"
  done
  san+="DNS:localhost,IP:127.0.0.1"
  openssl ecparam -name prime256v1 -genkey -noout -out "${dir}/ca.key" 2>/dev/null
  openssl req -x509 -new -key "${dir}/ca.key" -sha256 -days 2 -subj "/CN=dash-kind-e2e-ca" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "${dir}/ca.crt"
  openssl ecparam -name prime256v1 -genkey -noout -out "${dir}/tls.ec.key" 2>/dev/null
  openssl pkcs8 -topk8 -nocrypt -in "${dir}/tls.ec.key" -out "${dir}/tls.key"
  openssl req -new -key "${dir}/tls.key" -subj "/CN=${rel}" -out "${dir}/tls.csr"
  cat > "${dir}/ext.cnf" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=${san}
EOF
  openssl x509 -req -in "${dir}/tls.csr" -CA "${dir}/ca.crt" -CAkey "${dir}/ca.key" -CAcreateserial \
    -days 2 -sha256 -extfile "${dir}/ext.cnf" -out "${dir}/tls.crt" 2>/dev/null
  kubectl -n "${ns}" create secret generic "${rel}-internal-tls" \
    --from-file=tls.crt="${dir}/tls.crt" --from-file=tls.key="${dir}/tls.key" \
    --from-file=ca.crt="${dir}/ca.crt" >/dev/null
  TLS_CA="${dir}/ca.crt"
}

phase_tls() {
  local ns="dash-tls" rel="dash-tls"
  step "tls: install with tls.enabled=true and a self-signed CA"
  create_namespace "${ns}"
  generate_tls "${ns}" "${rel}"
  helm_deploy install "${ns}" "${rel}" "${CHART_DIR}" \
    --set tls.enabled=true --set "tls.secretName=${rel}-internal-tls" --set replicas.retrieval=1
  TLS_HOST[ing]="${rel}-ingestion.${ns}.svc.cluster.local"
  TLS_HOST[ret0]="${rel}-retrieval.${ns}.svc.cluster.local"
  pf_start ing "${ns}" "svc/${rel}-ingestion" 80
  pf_start ret0 "${ns}" "pod/${rel}-retrieval-0" 8080
  wait_http_ok ing /v1/ready
  wait_http_ok ret0 /v1/ready

  step "tls: plain http is refused on the TLS listener"
  if curl -sS --max-time 5 -o /dev/null "http://127.0.0.1:${PF_PORT[ing]}/v1/live" 2>/dev/null; then
    http_code="$(curl -sS --max-time 5 -o /dev/null -w '%{http_code}' "http://127.0.0.1:${PF_PORT[ing]}/v1/live" 2>/dev/null || true)"
    [[ "${http_code}" != "200" ]] || fail "ingestion answered plain http with 200 while tls.enabled=true"
  fi

  step "tls: ingest over HTTPS, retrieve after mTLS replication"
  ingest_claim ing tls-claim-1 1
  ingest_claim ing tls-claim-2 2
  wait_ids ret0 "$(csv tls-claim-1 tls-claim-2)"
  delete_claim ing tls-claim-1
  wait_ids ret0 "tls-claim-2"
  pf_stop_all
  TLS_HOST=()
  helm uninstall "${rel}" --namespace "${ns}" --wait --timeout "${TIMEOUT}s" >/dev/null || true
  delete_namespace "${ns}"
}

# --- main ---------------------------------------------------------------------
main() {
  local p
  for p in ${PHASES}; do
    case "${p}" in
      core|upgrade-from|kustomize|tls) ;;
      *) fail "unknown phase '${p}' in DASH_E2E_PHASES (expected: core upgrade-from kustomize tls)" ;;
    esac
  done
  for tool in docker curl jq openssl tar sha256sum git; do
    need "${tool}"
  done
  docker info >/dev/null 2>&1 || fail "docker is installed but the daemon is not reachable"
  log "work dir ${WORK_DIR}; phases: ${PHASES}"

  install_tools
  create_cluster
  build_and_load_image
  generate_secrets
  local first=1
  for p in ${PHASES}; do
    # The raw manifests hard-code the dash-system namespace that the core
    # phase also uses. Reusing it right after an uninstall (same namespace
    # name, recycled pod IPs) left kindnet's network-policy state stale on CI
    # runners and the new pods could not resolve DNS, so this phase gets a
    # fresh cluster unless it runs first.
    if [[ "${p}" == "kustomize" && "${first}" -eq 0 && "${DASH_E2E_REUSE_CLUSTER:-0}" != "1" ]]; then
      pf_stop_all
      step "recreate the kind cluster for the kustomize phase"
      kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
      CLUSTER_CREATED=0
      create_cluster
      build_and_load_image
    fi
    first=0
    case "${p}" in
      core) phase_core ;;
      upgrade-from) phase_upgrade_from ;;
      kustomize) phase_kustomize ;;
      tls) phase_tls ;;
    esac
  done
  CURRENT_STEP="done"
}

main "$@"
