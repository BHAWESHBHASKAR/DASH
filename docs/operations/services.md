# DASH Service Operations

Both the `retrieval` and `ingestion` services are production-grade
HTTP servers. This document covers runtime behavior, health
probes, graceful shutdown, and the CLI conventions.

## Running

Both services default to serve mode. To run the retrieval service
on the default port:

```bash
./target/release/retrieval
# → retrieval: serving on http://127.0.0.1:8080 (--cli to run without a port)
```

To run the ingestion service:

```bash
./target/release/ingestion
# → ingestion: serving on http://127.0.0.1:8081 (--cli to run without a port)
```

The previous behavior required an explicit `--serve` flag. The
default was flipped because the previous footgun (services that
silently exit after printing the startup banner when run without
`--serve`) was confusing for first-time users. To run the legacy
one-shot path explicitly, pass `--cli` or `--no-serve`.

## Environment variables

Both services accept the same set of env-var conventions. The
primary variables:

| Variable | Default | Purpose |
|---|---|---|
| `DASH_RETRIEVAL_BIND` | `127.0.0.1:8080` | bind address for retrieval |
| `DASH_INGEST_BIND` | `127.0.0.1:8081` | bind address for ingestion |
| `DASH_RETRIEVAL_HTTP_WORKERS` | `10` | number of HTTP worker threads |
| `DASH_INGEST_HTTP_WORKERS` | `10` | number of HTTP worker threads |
| `DASH_RETRIEVAL_PERSISTENCE_PATH` | `./data/dash-retrieval.redb` | redb path for the disk-backed store |
| `DASH_INGEST_PERSISTENCE_PATH` | `./data/dash-ingestion.redb` | redb path for the disk-backed store |
| `DASH_*_PERSISTENCE_DISABLE=1` | (unset = enabled) | set to skip disk persistence entirely |
| `DASH_RETRIEVAL_API_KEY` | (no auth) | bearer token required for /v1/* endpoints |
| `DASH_INGEST_API_KEY` | (no auth) | bearer token required for /v1/ingest* endpoints |
| `DASH_*_WAL_PATH` | `./data/dash-{service}.wal` | WAL file for crash recovery |

Every variable has a legacy `EME_*` alias for backward compat with
deployments that predate the rename.

## Endpoints

### Health probes (all return 200 + JSON)

| Path | Probe type | Body | Use |
|---|---|---|---|
| `GET /health` (legacy) or `GET /v1/health` | basic | `{"status":"ok"}` | unspecified — prefer `/live` or `/v1/live` |
| `GET /live` (legacy) or `GET /v1/live` | liveness | `{"status":"alive"}` | k8s `livenessProbe` — pod is alive and not deadlocked |
| `GET /ready` (legacy) or `GET /v1/ready` | readiness | `{"status":"ready"}` | k8s `readinessProbe` — pod can serve traffic |
| `GET /metrics` | observability | Prometheus text format | scrape every 15-30s |

The k8s manifest should look like:

```yaml
livenessProbe:
  httpGet: { path: /v1/live, port: 8080 }
  periodSeconds: 30
readinessProbe:
  httpGet: { path: /v1/ready, port: 8080 }
  periodSeconds: 5
```

### API endpoints

| Service | Endpoint | Method | Body |
|---|---|---|---|
| retrieval | `/v1/embeddings` | POST | OpenAI-shaped `{input, model, encoding_format?}` |
| retrieval | `/v1/retrieve` | GET, POST | `RetrievalRequest { tenant_id, query, top_k, stance_mode }` |
| ingestion | `/v1/ingest` | POST | `{ claim, claim_embedding?, evidence, edges }` |
| ingestion | `/v1/ingest/batch` | POST | `{ commit_id?, items: [...] }` |
| ingestion | `/v1/ingest/raw` | POST | raw document extraction path |
| ingestion | `/v1/ingest/document` | POST | document-level path |
| ingestion | `/v1/delete` | POST | `{ tenant_id, claim_ids }` |
| ingestion | `/internal/replication/ack` | POST | inter-replica WAL ack |

## Graceful shutdown

Both services install SIGTERM and SIGINT handlers via the
`signal-hook` crate. On signal:

1. The accept loop polls a shared `Arc<AtomicBool>` every 50ms.
2. When the flag is set, the loop exits the accept phase.
3. In-flight requests finish on the worker threads (the
   `std::thread::scope` blocks until all workers return).
4. The service exits with status 0.

The 50ms poll interval caps shutdown latency at ~50ms p99 — well
below the typical k8s `terminationGracePeriodSeconds` of 30s.

```bash
# Graceful shutdown
kill -TERM $(pgrep -f release/retrieval)
# → retrieval: shutdown signal received, draining in-flight requests
# → (process exits)
```

## Disabling disk persistence for tests

By default, both services attach a `redb`-backed disk store at
`./data/dash-{service}.redb`. To skip persistence (e.g. for unit
tests or benchmarks):

```bash
DASH_RETRIEVAL_PERSISTENCE_DISABLE=1 ./target/release/retrieval
```

The services still maintain their in-memory state; the disk
attachment is the only thing that's disabled.

## Metrics

`GET /metrics` returns a Prometheus text-format dump. The most
useful counters and gauges for the retrieval service:

- `dash_http_requests_total` — total requests by path
- `dash_retrieve_requests_total` — /v1/retrieve invocations
- `dash_retrieve_success_total` / `dash_retrieve_client_error_total` / `dash_retrieve_server_error_total` — split by status code class
- `dash_retrieve_latency_ms_p50` / `_p95` / `_p99` — retrieve latency percentiles
- `dash_transport_auth_success_total` / `dash_transport_auth_failure_total` — auth outcomes
- `dash_retrieve_transport_queue_depth` / `_full_reject_total` — backpressure
- `dash_retrieve_placement_*` — shard placement state

The ingestion service adds:

- `dash_ingest_to_visible_lag_ms_p50` / `_p95` — end-to-end
  ingest-to-readable latency (WAL flush + visibility)
- `dash_ingest_wal_durability_*` — current WAL policy
- `dash_ingest_checkpoint_*` — checkpoint frequency and trigger

A Grafana dashboard with these panels is on the roadmap.

## Common operational tasks

### Force a checkpoint

The ingestion service auto-checkpoints on `WalWritePolicy` triggers
(WAL size, record count, or time interval). To force a manual
checkpoint, send SIGUSR1:

```bash
kill -USR1 $(pgrep -f release/ingestion)
```

(Implementation pending — currently SIGUSR1 is not bound.)

### Drain a node for maintenance

```bash
# 1. Cordon the node (k8s): not applicable to the service, but
#    if running on a host: drop the service from the load
#    balancer.
# 2. Send SIGTERM for graceful shutdown
kill -TERM $(pgrep -f release/retrieval)
# 3. Wait for the process to exit (up to terminationGracePeriodSeconds)
wait $(pgrep -f release/retrieval)
```

### Read the in-memory store contents (forensics)

The retrieval service exposes debug endpoints:

- `GET /debug/placement` — current shard placement state
- `GET /debug/planner` — last retrieve planner snapshot (auth-gated)
- `GET /debug/storage-visibility` — current storage visibility state

These are intended for the platform team, not external clients.
Gate them at the load balancer or via `DASH_*_API_KEY_SCOPES`.

## Failure modes

| Symptom | Likely cause | Mitigation |
|---|---|---|
| Process exits with `Address already in use` | another instance is bound to the same port | find and kill the old process (`pgrep -f release/<service>`) |
| `DASH_*_PERSISTENCE_PATH` open fails | read-only filesystem or missing parent | check the path; the service falls back to in-memory mode and logs an error |
| High `dash_*_full_reject_total` | backpressure — workers can't keep up | increase `DASH_*_HTTP_WORKERS` or scale horizontally |
| `/v1/ready` returns 503 | runtime mutex poisoned (a panic happened earlier) | restart the pod; inspect logs for the panic |
