# Runbooks

One runbook per alert in [`deploy/observability/prometheus/dash-alerts.rules.yml`](../../../deploy/observability/prometheus/dash-alerts.rules.yml); each alert links its runbook through `annotations.runbook_url`. SLOs and error budgets: [slos.md](../slos.md). Metrics and logs: [observability](../../../docs-site/docs/operations/observability.md).

Every runbook has the same sections: meaning, impact, diagnosis (queries and commands), mitigation, escalation. Every DASH response carries an `X-Request-Id` header (and error bodies a `request_id` field); search the logs and the audit log for it when a client reports a failing request.

| Alert | Severity | Runbook |
|---|---|---|
| `DashAuditDenialsDropped` | warning | [dash-audit-denials-dropped.md](dash-audit-denials-dropped.md) |
| `DashAuditWriteFailures` | critical | [dash-audit-write-failures.md](dash-audit-write-failures.md) |
| `DashCheckpointFailing` | warning | [dash-checkpoint-failing.md](dash-checkpoint-failing.md) |
| `DashControlPlaneNoLeader` | warning | [dash-control-plane-no-leader.md](dash-control-plane-no-leader.md) |
| `DashDiskNearlyFull` | warning | [dash-disk-nearly-full.md](dash-disk-nearly-full.md) |
| `DashDiskUnavailable` | critical | [dash-disk-unavailable.md](dash-disk-unavailable.md) |
| `DashEmbeddingBreakerOpen` | warning | [dash-embedding-breaker-open.md](dash-embedding-breaker-open.md) |
| `DashErrorBudgetBurn` | critical (window=fast), warning (window=slow) | [dash-error-budget-burn.md](dash-error-budget-burn.md) |
| `DashFileDescriptorsExhausted` | warning | [dash-file-descriptors-exhausted.md](dash-file-descriptors-exhausted.md) |
| `DashHighErrorRate` | critical | [dash-high-error-rate.md](dash-high-error-rate.md) |
| `DashHighLatencyP99` | warning | [dash-high-latency-p99.md](dash-high-latency-p99.md) |
| `DashIngestFailoverBlocked` | critical | [dash-ingest-failover-blocked.md](dash-ingest-failover-blocked.md) |
| `DashIngestFailoverHappened` | info | [dash-ingest-failover-happened.md](dash-ingest-failover-happened.md) |
| `DashIngestNoLeader` | critical | [dash-ingest-no-leader.md](dash-ingest-no-leader.md) |
| `DashIngestSyncReplicationTimeouts` | warning | [dash-ingest-sync-replication-timeouts.md](dash-ingest-sync-replication-timeouts.md) |
| `DashIngestToVisibleLagHigh` | warning | [dash-ingest-to-visible-lag-high.md](dash-ingest-to-visible-lag-high.md) |
| `DashReplicationFollowerLagging` | warning | [dash-replication-follower-lagging.md](dash-replication-follower-lagging.md) |
| `DashReplicationFollowerNotReady` | critical | [dash-replication-follower-not-ready.md](dash-replication-follower-not-ready.md) |
| `DashReplicationResyncStorm` | warning | [dash-replication-resync-storm.md](dash-replication-resync-storm.md) |
| `DashRequestsShed` | warning | [dash-requests-shed.md](dash-requests-shed.md) |
| `DashStorageDivergence` | warning | [dash-storage-divergence.md](dash-storage-divergence.md) |
| `DashTargetDown` | critical | [dash-target-down.md](dash-target-down.md) |
| `DashWalFsyncSlow` | warning | [dash-wal-fsync-slow.md](dash-wal-fsync-slow.md) |
| `DashWalPoisoned` | critical | [dash-wal-poisoned.md](dash-wal-poisoned.md) |
| `DashWalWriteFailing` | critical | [dash-wal-write-failing.md](dash-wal-write-failing.md) |
