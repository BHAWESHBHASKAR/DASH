#!/usr/bin/env bash
# Back up or restore the ingestion state of a DASH Helm release on Kubernetes.
#
# Usage:
#   scripts/k8s_backup_restore.sh backup  --out-dir DIR [--label LABEL] [options]
#   scripts/k8s_backup_restore.sh restore --bundle FILE [options]
#
# Options:
#   --namespace NS     namespace of the release (default: dash-system)
#   --release NAME     Helm release name (default: dash)
#   --timeout SECONDS  limit for every wait (default: 300)
#
# Both commands take a COLD copy: the ingestion StatefulSet is scaled to 0,
# a short-lived helper pod (same image, same security context) mounts the
# ingestion PVC, the files are streamed through `kubectl exec ... tar`, and
# the StatefulSet is scaled back. Writes are unavailable for that window
# (typically well under a minute); retrieval pods keep serving what they
# have. See docs/operations/kubernetes.md.
#
# backup writes <out-dir>/dash-backup-<label>.tar.gz in the format of
# scripts/backup_state_bundle.sh (WAL, WAL snapshot, segment directory, with
# SHA-256 checksums), plus <out-dir>/dash-audit-<label>.tar.gz with the
# ingestion audit log when one exists. Derived files (redb mirror, saved
# vector index, WAL lineage id) are not copied: they are rebuilt from the WAL.
#
# restore verifies the bundle, then replaces the WAL, snapshot and segment
# directory on the ingestion PVC and deletes the derived files, so ingestion
# replays the restored WAL and starts a new WAL lineage. Retrieval followers
# detect the new lineage and resync from a full export of the restored
# state; data written after the backup is gone from every pod. An audit
# tarball next to the bundle is restored only when the PVC has no audit log.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CMD="${1:-}"
[[ $# -gt 0 ]] && shift
NAMESPACE="dash-system"
RELEASE="dash"
OUT_DIR=""
LABEL="$(date -u +%Y%m%d-%H%M%S)"
BUNDLE=""
TIMEOUT=300

usage() {
  sed -n '2,31p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
}

die() {
  echo "k8s_backup_restore: $*" >&2
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --namespace) NAMESPACE="${2:?--namespace needs a value}"; shift 2 ;;
    --release) RELEASE="${2:?--release needs a value}"; shift 2 ;;
    --out-dir) OUT_DIR="${2:?--out-dir needs a value}"; shift 2 ;;
    --label) LABEL="${2:?--label needs a value}"; shift 2 ;;
    --bundle) BUNDLE="${2:?--bundle needs a value}"; shift 2 ;;
    --timeout) TIMEOUT="${2:?--timeout needs a value}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
done

case "${CMD}" in
  backup) [[ -n "${OUT_DIR}" ]] || die "backup needs --out-dir" ;;
  restore)
    [[ -n "${BUNDLE}" ]] || die "restore needs --bundle"
    [[ -f "${BUNDLE}" ]] || die "bundle not found: ${BUNDLE}"
    BUNDLE="$(cd "$(dirname "${BUNDLE}")" && pwd)/$(basename "${BUNDLE}")"
    ;;
  -h|--help|"") usage; [[ -n "${CMD}" ]] && exit 0; exit 2 ;;
  *) usage >&2; die "unknown command: ${CMD} (expected backup or restore)" ;;
esac
[[ "${LABEL}" =~ ^[A-Za-z0-9._-]+$ ]] || die "--label may only contain letters, digits, '.', '_' and '-'"
[[ "${TIMEOUT}" =~ ^[0-9]+$ ]] || die "--timeout must be a number of seconds"

for tool in kubectl jq tar; do
  command -v "${tool}" >/dev/null 2>&1 || die "${tool} is required"
done

k() { kubectl -n "${NAMESPACE}" "$@"; }

log() { echo "[k8s-$(printf '%s' "${CMD}")] $*"; }

# --- locate the ingestion StatefulSet, its PVC and its configuration --------
selector="app.kubernetes.io/instance=${RELEASE},app.kubernetes.io/component=ingestion"
STS="$(k get statefulset -l "${selector}" -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)"
[[ -n "${STS}" ]] || die "no ingestion StatefulSet with labels ${selector} in namespace ${NAMESPACE}"
STS_JSON="$(k get statefulset "${STS}" -o json)"
POD="${STS}-0"
PVC="data-${POD}"
k get pvc "${PVC}" >/dev/null 2>&1 || die "PVC ${PVC} not found (is persistence.enabled=true?)"
REPLICAS="$(jq -r '.spec.replicas // 1' <<<"${STS_JSON}")"
CONFIGMAP="$(jq -r '[.spec.template.spec.containers[0].envFrom[]? | .configMapRef.name // empty][0] // empty' <<<"${STS_JSON}")"
[[ -n "${CONFIGMAP}" ]] || die "StatefulSet ${STS} has no configMapRef in envFrom"
CM_JSON="$(k get configmap "${CONFIGMAP}" -o json)"
cm() { jq -r --arg k "$1" '.data[$k] // empty' <<<"${CM_JSON}"; }
WAL_PATH="$(cm DASH_INGEST_WAL_PATH)"
SEGMENT_DIR="$(cm DASH_INGEST_SEGMENT_DIR)"
REDB_PATH="$(cm DASH_INGEST_PERSISTENCE_PATH)"
AUDIT_PATH="$(cm DASH_INGEST_AUDIT_LOG_PATH)"
[[ "${WAL_PATH}" == /* ]] || die "DASH_INGEST_WAL_PATH missing or relative in ConfigMap ${CONFIGMAP}"
WAL_DIR="$(dirname "${WAL_PATH}")"
DATA_MOUNT="$(jq -r '.spec.template.spec.containers[0].volumeMounts[] | select(.name=="data") | .mountPath' <<<"${STS_JSON}")"
[[ -n "${DATA_MOUNT}" ]] || die "StatefulSet ${STS} does not mount a volume named data"

HELPER="${STS}-${CMD}-helper"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/dash-k8s-${CMD}-XXXXXX")"
scaled_down=0

cleanup() {
  local rc=$?
  k delete pod "${HELPER}" --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [[ "${scaled_down}" -eq 1 ]]; then
    log "scaling ${STS} back to ${REPLICAS} replica(s)"
    k scale statefulset "${STS}" --replicas="${REPLICAS}" >/dev/null 2>&1 || true
  fi
  rm -rf "${WORK}"
  if [[ "${rc}" -ne 0 ]]; then
    echo "k8s_backup_restore: ${CMD} FAILED (exit ${rc})" >&2
  fi
}
trap cleanup EXIT

scale_down() {
  log "scaling ${STS} to 0 (writes are unavailable until it is scaled back)"
  scaled_down=1
  k scale statefulset "${STS}" --replicas=0 >/dev/null
  if k get pod "${POD}" >/dev/null 2>&1; then
    k wait --for=delete "pod/${POD}" --timeout="${TIMEOUT}s" >/dev/null \
      || die "pod ${POD} did not terminate within ${TIMEOUT}s"
  fi
}

scale_up() {
  log "scaling ${STS} back to ${REPLICAS} replica(s)"
  k scale statefulset "${STS}" --replicas="${REPLICAS}" >/dev/null
  scaled_down=0
  k rollout status "statefulset/${STS}" --timeout="${TIMEOUT}s" \
    || die "${STS} did not become ready within ${TIMEOUT}s"
}

# The helper pod copies the image, pull policy, pod and container security
# contexts, node placement and the PVC from the StatefulSet, so it passes
# the same admission (Pod Security "restricted") and file ownership.
start_helper() {
  local spec
  spec="$(jq --arg name "${HELPER}" --arg pvc "${PVC}" --arg mount "${DATA_MOUNT}" '
    .spec.template.spec as $p
    | ($p.containers[0]) as $c
    | {
        apiVersion: "v1",
        kind: "Pod",
        metadata: {
          name: $name,
          labels: {
            "app.kubernetes.io/part-of": "dash",
            "app.kubernetes.io/component": "maintenance"
          }
        },
        spec: {
          restartPolicy: "Never",
          automountServiceAccountToken: false,
          terminationGracePeriodSeconds: 1,
          securityContext: ($p.securityContext // {}),
          nodeSelector: ($p.nodeSelector // {}),
          tolerations: ($p.tolerations // []),
          imagePullSecrets: ($p.imagePullSecrets // []),
          containers: [{
            name: "helper",
            image: $c.image,
            imagePullPolicy: ($c.imagePullPolicy // "IfNotPresent"),
            command: ["sleep", "3600"],
            securityContext: ($c.securityContext // {}),
            resources: {
              requests: {cpu: "10m", memory: "16Mi"},
              limits: {cpu: "500m", memory: "128Mi"}
            },
            volumeMounts: [
              {name: "data", mountPath: $mount},
              {name: "tmp", mountPath: "/tmp"}
            ]
          }],
          volumes: [
            {name: "data", persistentVolumeClaim: {claimName: $pvc}},
            {name: "tmp", emptyDir: {sizeLimit: "64Mi"}}
          ]
        }
      }' <<<"${STS_JSON}")"
  k delete pod "${HELPER}" --ignore-not-found --wait=true >/dev/null
  printf '%s\n' "${spec}" | k apply -f - >/dev/null
  k wait --for=condition=Ready "pod/${HELPER}" --timeout="${TIMEOUT}s" >/dev/null \
    || die "helper pod ${HELPER} not ready within ${TIMEOUT}s"
}

hexec() { k exec -i "${HELPER}" -- "$@"; }

do_backup() {
  mkdir -p "${OUT_DIR}"
  OUT_DIR="$(cd "${OUT_DIR}" && pwd)"
  scale_down
  start_helper

  hexec test -f "${WAL_PATH}" || die "no WAL at ${WAL_PATH} on PVC ${PVC} (nothing ingested yet?)"
  local stage="${WORK}/stage" seg_args=()
  mkdir -p "${stage}/wal"
  # Only the WAL and its snapshot; lineage, index and replication files in
  # the WAL directory are derived.
  local wal_files=("$(basename "${WAL_PATH}")")
  if hexec test -f "${WAL_PATH}.snapshot"; then
    wal_files+=("$(basename "${WAL_PATH}").snapshot")
  fi
  hexec tar -C "${WAL_DIR}" -cf - "${wal_files[@]}" | tar -C "${stage}/wal" -xf -
  if [[ -n "${SEGMENT_DIR}" ]] && hexec test -d "${SEGMENT_DIR}"; then
    mkdir -p "${stage}/segments"
    hexec tar -C "${SEGMENT_DIR}" -cf - . | tar -C "${stage}/segments" -xf -
    seg_args=(--segment-dir "${stage}/segments")
  fi
  if [[ -n "${AUDIT_PATH}" ]] && hexec test -f "${AUDIT_PATH}"; then
    hexec tar -C "$(dirname "${AUDIT_PATH}")" -czf - "$(basename "${AUDIT_PATH}")" \
      > "${OUT_DIR}/dash-audit-${LABEL}.tar.gz"
    log "audit log: ${OUT_DIR}/dash-audit-${LABEL}.tar.gz"
  fi

  k delete pod "${HELPER}" --wait=true >/dev/null
  scale_up

  bash "${ROOT_DIR}/scripts/backup_state_bundle.sh" \
    --wal-path "${stage}/wal/$(basename "${WAL_PATH}")" \
    "${seg_args[@]}" \
    --output-dir "${OUT_DIR}" \
    --bundle-label "${LABEL}"
  log "done: ${OUT_DIR}/dash-backup-${LABEL}.tar.gz"
}

do_restore() {
  # Verify and unpack locally before touching the cluster.
  bash "${ROOT_DIR}/scripts/restore_state_bundle.sh" --bundle "${BUNDLE}" --verify-only true \
    --wal-path "${WORK}/unused.wal"
  local stage="${WORK}/stage"
  mkdir -p "${stage}/wal"
  bash "${ROOT_DIR}/scripts/restore_state_bundle.sh" --bundle "${BUNDLE}" --force true \
    --wal-path "${stage}/wal/$(basename "${WAL_PATH}")" \
    --segment-dir "${stage}/segments"
  local audit_tar
  audit_tar="$(dirname "${BUNDLE}")/$(basename "${BUNDLE}" | sed 's/^dash-backup-/dash-audit-/')"

  scale_down
  start_helper

  log "replacing ${WAL_DIR} and ${SEGMENT_DIR:-<no segment dir>}; removing ${REDB_PATH:-<no redb>}"
  # Remove the WAL directory contents (WAL, snapshot and every derived file:
  # .gen, .vindex, replication bookkeeping), the segment directory and the
  # redb mirror, then unpack the restored files.
  # shellcheck disable=SC2016  # expanded by the helper's shell
  hexec sh -c '
    set -eu
    wal_dir="$1"; seg_dir="$2"; redb="$3"
    mkdir -p "$wal_dir"
    find "$wal_dir" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
    if [ -n "$seg_dir" ]; then rm -rf "$seg_dir"; mkdir -p "$seg_dir"; fi
    if [ -n "$redb" ]; then rm -f "$redb" "$redb.lock"; fi
  ' sh "${WAL_DIR}" "${SEGMENT_DIR}" "${REDB_PATH}"
  tar -C "${stage}/wal" -cf - . | hexec tar -C "${WAL_DIR}" -xf - --no-same-owner
  if [[ -n "${SEGMENT_DIR}" && -d "${stage}/segments" ]]; then
    tar -C "${stage}/segments" -cf - . | hexec tar -C "${SEGMENT_DIR}" -xf - --no-same-owner
  fi
  if [[ -f "${audit_tar}" && -n "${AUDIT_PATH}" ]]; then
    if hexec test -s "${AUDIT_PATH}"; then
      log "keeping the existing audit log ${AUDIT_PATH} (not overwritten by ${audit_tar})"
    else
      hexec mkdir -p "$(dirname "${AUDIT_PATH}")"
      hexec tar -C "$(dirname "${AUDIT_PATH}")" -xzf - --no-same-owner < "${audit_tar}"
      log "audit log restored from ${audit_tar}"
    fi
  fi
  # shellcheck disable=SC2016
  hexec sh -c 'ls -la "$1"' sh "${WAL_DIR}"

  k delete pod "${HELPER}" --wait=true >/dev/null
  scale_up
  log "done: ingestion replays the restored WAL; followers resync to the new lineage"
}

case "${CMD}" in
  backup) do_backup ;;
  restore) do_restore ;;
esac
