#!/usr/bin/env bash
set -euo pipefail

# SOC 2 evidence collector for DASH self-hosted deployments.
# Usage: ./scripts/soc2_evidence_collector.sh <output-directory>

OUTPUT_DIR="${1:-./soc2-evidence}"
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COLLECTION_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
EVIDENCE_DIR="$OUTPUT_DIR/$COLLECTION_DATE"
mkdir -p "$EVIDENCE_DIR"

echo "[soc2] collecting evidence in $EVIDENCE_DIR"

# 1. Repository metadata
echo "[soc2] repository metadata"
{
    echo "collection_date=$COLLECTION_DATE"
    echo "repo_root=$REPO_ROOT"
    (cd "$REPO_ROOT" && git log --oneline -20 2>/dev/null || echo "git_log_unavailable")
} > "$EVIDENCE_DIR/repo-metadata.txt"

# 2. CI workflow definitions (not run logs; those live in GitHub Actions)
echo "[soc2] CI workflow definitions"
mkdir -p "$EVIDENCE_DIR/workflows"
cp "$REPO_ROOT/.github/workflows/"*.yml "$EVIDENCE_DIR/workflows/" 2>/dev/null || true

# 3. Run CI health check (fmt, clippy, tests) and capture result
echo "[soc2] running CI health check"
if (cd "$REPO_ROOT" && ./scripts/ci.sh) > "$EVIDENCE_DIR/ci-health.log" 2>&1; then
    echo "ci_health=pass" >> "$EVIDENCE_DIR/repo-metadata.txt"
else
    echo "ci_health=fail" >> "$EVIDENCE_DIR/repo-metadata.txt"
fi

# 4. Backup/restore drill
echo "[soc2] backup/restore drill"
if (cd "$REPO_ROOT" && ./scripts/backup_restore_drill.sh) > "$EVIDENCE_DIR/backup-drill.log" 2>&1; then
    echo "backup_drill=pass" >> "$EVIDENCE_DIR/repo-metadata.txt"
else
    echo "backup_drill=fail" >> "$EVIDENCE_DIR/repo-metadata.txt"
fi

# 5. Prometheus alert rules
echo "[soc2] alert rules"
mkdir -p "$EVIDENCE_DIR/monitoring"
cp "$REPO_ROOT/deploy/container/monitoring/prometheus-alert-rules.yml" "$EVIDENCE_DIR/monitoring/" 2>/dev/null || true

# 6. Encryption provider name (non-sensitive)
echo "[soc2] encryption provider config"
{
    echo "DASH_ENCRYPTION_PROVIDER=${DASH_ENCRYPTION_PROVIDER:-unset}"
    echo "EME_ENCRYPTION_PROVIDER=${EME_ENCRYPTION_PROVIDER:-unset}"
} > "$EVIDENCE_DIR/encryption-provider.txt"

# 7. Dependency audit (if cargo-audit installed)
echo "[soc2] dependency audit"
if command -v cargo-audit >/dev/null 2>&1; then
    (cd "$REPO_ROOT" && cargo audit --json 2>/dev/null) > "$EVIDENCE_DIR/cargo-audit.json" || true
else
    echo "cargo-audit not installed" > "$EVIDENCE_DIR/cargo-audit.txt"
fi

# 8. Compliance document checksums
echo "[soc2] compliance document checksums"
find "$REPO_ROOT/docs/compliance" -type f -print0 2>/dev/null | xargs -0 sha256sum > "$EVIDENCE_DIR/compliance-sha256.txt" || true

echo "[soc2] evidence collection complete: $EVIDENCE_DIR"
