# Operations

The operations pages describe how to run DASH in production. They assume the reader is comfortable with Docker, Kubernetes, and Prometheus, and they focus on the *DASH-specific* parts: the deployment topology, the metrics surface, the scaling strategy, the backup procedure, and the security-audit workflow.

## Pages

- [**Deploy**](deploy.md) — Docker, docker-compose, Kubernetes (raw YAML), and Helm.
- [**Observability**](observability.md) — the `/metrics` endpoint, Prometheus scrape config, OpenTelemetry tracing (future), and structured logging.
- [**Scaling**](scaling.md) — horizontal scaling, `redb` PR 3 replication, and ANN index sharding.
- [**Backup**](backup.md) — `redb` snapshots, WAL archiving, and cross-region replication.
- [**Security & audit**](security-audit.md) — `cargo audit`, `trivy`, `codeql`, and the threat model link.

## How to read this section

If you are doing the first deployment, start with [Deploy](deploy.md) and [Observability](observability.md). If you are running a production deployment, the [Scaling](scaling.md), [Backup](backup.md), and [Security & audit](security-audit.md) pages are the ones you will return to.
