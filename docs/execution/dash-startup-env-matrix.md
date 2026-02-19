# DASH Startup Env Matrix

Date: 2026-02-17  
Status: active

## Ingestion Service

| Variable | Required | Default | Description | Legacy Fallback |
| --- | --- | --- | --- | --- |
| `DASH_INGEST_BIND` | no | `127.0.0.1:8081` | bind address for HTTP ingestion transport (`/v1/ingest`) | `EME_INGEST_BIND` |
| `DASH_INGEST_HTTP_WORKERS` | no | auto (`min(available_parallelism, 32)`, fallback `4`) | ingestion HTTP worker pool size | `EME_INGEST_HTTP_WORKERS` |
| `DASH_INGEST_HTTP_QUEUE_CAPACITY` | no | `workers * 64` | bounded ingestion worker-queue capacity; when full, requests are rejected with `503` and backpressure metrics increment | `EME_INGEST_HTTP_QUEUE_CAPACITY` |
| `DASH_INGEST_TRANSPORT_RUNTIME` | no | `std` | transport runtime selector (`std` or `axum`) | `EME_INGEST_TRANSPORT_RUNTIME` |
| `DASH_INGEST_API_KEY` | no | unset | optional API key for `POST /v1/ingest` (`X-API-Key` or `Authorization: Bearer`) | `EME_INGEST_API_KEY` |
| `DASH_INGEST_API_KEYS` | no | unset | optional comma-separated API key set (rotation overlap) accepted in addition to `DASH_INGEST_API_KEY` | `EME_INGEST_API_KEYS` |
| `DASH_INGEST_REVOKED_API_KEYS` | no | unset | optional comma-separated revoked API keys denied even if configured in scopes or key set | `EME_INGEST_REVOKED_API_KEYS` |
| `DASH_INGEST_JWT_HS256_SECRET` | no | unset | optional HS256 JWT secret for bearer-token auth (`Authorization: Bearer <jwt>`) | `EME_INGEST_JWT_HS256_SECRET` |
| `DASH_INGEST_JWT_HS256_SECRETS` | no | unset | optional comma-separated fallback HS256 JWT secrets for rotation overlap (used when JWT header has no `kid`) | `EME_INGEST_JWT_HS256_SECRETS` |
| `DASH_INGEST_JWT_HS256_SECRETS_BY_KID` | no | unset | optional JWT `kid` secret map (`kid-a:secret-a;kid-b:secret-b`) for deterministic key selection | `EME_INGEST_JWT_HS256_SECRETS_BY_KID` |
| `DASH_INGEST_JWT_ISSUER` | no | unset | optional required JWT `iss` claim value | `EME_INGEST_JWT_ISSUER` |
| `DASH_INGEST_JWT_AUDIENCE` | no | unset | optional required JWT `aud` claim value | `EME_INGEST_JWT_AUDIENCE` |
| `DASH_INGEST_JWT_LEEWAY_SECS` | no | `0` | optional JWT time-claim leeway seconds (`exp`, `nbf`) | `EME_INGEST_JWT_LEEWAY_SECS` |
| `DASH_INGEST_JWT_REQUIRE_EXP` | no | `true` | when `true`, JWT auth requires `exp` claim | `EME_INGEST_JWT_REQUIRE_EXP` |
| `DASH_INGEST_ALLOWED_TENANTS` | no | unset (`*`) | optional tenant allowlist (comma-separated tenant IDs or `*`) for ingest writes | `EME_INGEST_ALLOWED_TENANTS` |
| `DASH_INGEST_API_KEY_SCOPES` | no | unset | optional per-key tenant scopes (`key-a:tenant-a,tenant-b;key-b:*`) | `EME_INGEST_API_KEY_SCOPES` |
| `DASH_INGEST_AUDIT_LOG_PATH` | no | unset | optional JSONL audit log path for ingest events (success/denied/error) | `EME_INGEST_AUDIT_LOG_PATH` |
| `DASH_INGEST_SEGMENT_DIR` | no | unset | optional segment publish root directory (tenant-scoped immutable segment snapshots) | `EME_INGEST_SEGMENT_DIR` |
| `DASH_INGEST_SEGMENT_MAX_SEGMENT_SIZE` | no | `10000` | max claim IDs per segment before per-tier chunking | `EME_INGEST_SEGMENT_MAX_SEGMENT_SIZE` |
| `DASH_INGEST_SEGMENT_MAX_SEGMENTS_PER_TIER` | no | `8` | compaction planning threshold per tier | `EME_INGEST_SEGMENT_MAX_SEGMENTS_PER_TIER` |
| `DASH_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS` | no | `4` | max input segments consumed per compaction plan | `EME_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS` |
| `DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS` | no | `30000` | in-process segment lifecycle maintenance tick interval; `0` disables scheduled maintenance worker | `EME_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS` |
| `DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS` | no | `60000` | minimum age for unreferenced `.seg` files before maintenance GC can delete them | `EME_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS` |
| `DASH_ROUTER_PLACEMENT_FILE` | no | unset | optional shard placement CSV file path (enables placement-aware write routing) | `EME_ROUTER_PLACEMENT_FILE` |
| `DASH_ROUTER_LOCAL_NODE_ID` | conditional (required when `DASH_ROUTER_PLACEMENT_FILE` is set) | unset | local node identity used to verify this ingestion instance is the routed write leader | `EME_ROUTER_LOCAL_NODE_ID` |
| `DASH_NODE_ID` | conditional alias | unset | fallback alias for local node identity if `DASH_ROUTER_LOCAL_NODE_ID` is unset | `EME_NODE_ID` |
| `DASH_ROUTER_SHARD_IDS` | no | inferred from placement file | optional shard ring override (comma-separated u32 IDs) | `EME_ROUTER_SHARD_IDS` |
| `DASH_ROUTER_VIRTUAL_NODES_PER_SHARD` | no | `64` | optional virtual-node count for consistent-hash shard ring | `EME_ROUTER_VIRTUAL_NODES_PER_SHARD` |
| `DASH_ROUTER_REPLICA_COUNT` | no | inferred from placement file | optional replica count override for routing plan | `EME_ROUTER_REPLICA_COUNT` |
| `DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS` | no | unset (`0` / disabled) | optional live placement reload interval for in-process route re-resolution (no-restart failover) | `EME_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS` |
| `DASH_INGEST_WAL_PATH` | yes (for persistence) | none | WAL path for durable claim/evidence/edge writes | `EME_INGEST_WAL_PATH` |
| `DASH_INGEST_WAL_SYNC_EVERY_RECORDS` | no | `1` | WAL durability batch interval (`1` = fsync every append; `N>1` = group-commit style sync every N records) | `EME_INGEST_WAL_SYNC_EVERY_RECORDS` |
| `DASH_INGEST_WAL_APPEND_BUFFER_RECORDS` | no | `1` | in-process WAL append buffer threshold before flushing batched lines to disk | `EME_INGEST_WAL_APPEND_BUFFER_RECORDS` |
| `DASH_INGEST_WAL_SYNC_INTERVAL_MS` | no | unset | optional max interval before pending WAL records are synced | `EME_INGEST_WAL_SYNC_INTERVAL_MS` |
| `DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS` | no | auto when WAL batching is enabled | optional async WAL flush worker interval (`off`/`0` disables); worker bounds unsynced window during idle traffic | `EME_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS` |
| `DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY` | no | `false` | when `true`, request threads do not flush/sync WAL; async worker is required for durability window bounding | `EME_INGEST_WAL_BACKGROUND_FLUSH_ONLY` |
| `DASH_INGEST_ALLOW_UNSAFE_WAL_DURABILITY` | no | `false` | when `true`, bypasses ingestion startup WAL durability guardrails; use only for controlled stress benchmarks | `EME_INGEST_ALLOW_UNSAFE_WAL_DURABILITY` |
| `DASH_CHECKPOINT_MAX_WAL_RECORDS` | no | unset | checkpoint trigger by WAL record count | `EME_CHECKPOINT_MAX_WAL_RECORDS` |
| `DASH_CHECKPOINT_MAX_WAL_BYTES` | no | unset | checkpoint trigger by WAL file bytes | `EME_CHECKPOINT_MAX_WAL_BYTES` |
| `DASH_INGEST_ANN_MAX_NEIGHBORS_BASE` | no | `12` | ANN base-layer max neighbors for ingestion-side index build | `EME_INGEST_ANN_MAX_NEIGHBORS_BASE` |
| `DASH_INGEST_ANN_MAX_NEIGHBORS_UPPER` | no | `6` | ANN upper-layer max neighbors for ingestion-side index build | `EME_INGEST_ANN_MAX_NEIGHBORS_UPPER` |
| `DASH_INGEST_ANN_SEARCH_EXPANSION_FACTOR` | no | `12` | ANN search expansion multiplier (used at retrieval-time candidate expansion budget) | `EME_INGEST_ANN_SEARCH_EXPANSION_FACTOR` |
| `DASH_INGEST_ANN_SEARCH_EXPANSION_MIN` | no | `64` | ANN minimum expansion budget clamp | `EME_INGEST_ANN_SEARCH_EXPANSION_MIN` |
| `DASH_INGEST_ANN_SEARCH_EXPANSION_MAX` | no | `4096` | ANN maximum expansion budget clamp | `EME_INGEST_ANN_SEARCH_EXPANSION_MAX` |

Ingestion segment lifecycle daemon note:

- `target/release/segment-maintenance-daemon` reads the same segment env keys (`DASH_INGEST_SEGMENT_DIR`, `DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS`, `DASH_INGEST_SEGMENT_GC_MIN_STALE_AGE_MS`) for out-of-process maintenance loops.
- when daemon mode is active, set ingestion `DASH_INGEST_SEGMENT_MAINTENANCE_INTERVAL_MS=0` to avoid duplicate in-process maintenance loops.

## Retrieval Service

| Variable | Required | Default | Description | Legacy Fallback |
| --- | --- | --- | --- | --- |
| `DASH_RETRIEVAL_BIND` | no | `127.0.0.1:8080` | bind address for HTTP transport | `EME_RETRIEVAL_BIND` |
| `DASH_RETRIEVAL_HTTP_WORKERS` | no | auto (`min(available_parallelism, 32)`, fallback `4`) | retrieval HTTP worker pool size | `EME_RETRIEVAL_HTTP_WORKERS` |
| `DASH_RETRIEVAL_HTTP_QUEUE_CAPACITY` | no | `workers * 64` | bounded retrieval worker-queue capacity; when full, requests are rejected with `503` and backpressure metrics increment | `EME_RETRIEVAL_HTTP_QUEUE_CAPACITY` |
| `DASH_RETRIEVAL_TRANSPORT_RUNTIME` | no | `std` | transport runtime selector (`std` or `axum`) | `EME_RETRIEVAL_TRANSPORT_RUNTIME` |
| `DASH_RETRIEVAL_API_KEY` | no | unset | optional API key for `GET/POST /v1/retrieve` (`X-API-Key` or `Authorization: Bearer`) | `EME_RETRIEVAL_API_KEY` |
| `DASH_RETRIEVAL_API_KEYS` | no | unset | optional comma-separated API key set (rotation overlap) accepted in addition to `DASH_RETRIEVAL_API_KEY` | `EME_RETRIEVAL_API_KEYS` |
| `DASH_RETRIEVAL_REVOKED_API_KEYS` | no | unset | optional comma-separated revoked API keys denied even if configured in scopes or key set | `EME_RETRIEVAL_REVOKED_API_KEYS` |
| `DASH_RETRIEVAL_JWT_HS256_SECRET` | no | unset | optional HS256 JWT secret for bearer-token auth (`Authorization: Bearer <jwt>`) | `EME_RETRIEVAL_JWT_HS256_SECRET` |
| `DASH_RETRIEVAL_JWT_HS256_SECRETS` | no | unset | optional comma-separated fallback HS256 JWT secrets for rotation overlap (used when JWT header has no `kid`) | `EME_RETRIEVAL_JWT_HS256_SECRETS` |
| `DASH_RETRIEVAL_JWT_HS256_SECRETS_BY_KID` | no | unset | optional JWT `kid` secret map (`kid-a:secret-a;kid-b:secret-b`) for deterministic key selection | `EME_RETRIEVAL_JWT_HS256_SECRETS_BY_KID` |
| `DASH_RETRIEVAL_JWT_ISSUER` | no | unset | optional required JWT `iss` claim value | `EME_RETRIEVAL_JWT_ISSUER` |
| `DASH_RETRIEVAL_JWT_AUDIENCE` | no | unset | optional required JWT `aud` claim value | `EME_RETRIEVAL_JWT_AUDIENCE` |
| `DASH_RETRIEVAL_JWT_LEEWAY_SECS` | no | `0` | optional JWT time-claim leeway seconds (`exp`, `nbf`) | `EME_RETRIEVAL_JWT_LEEWAY_SECS` |
| `DASH_RETRIEVAL_JWT_REQUIRE_EXP` | no | `true` | when `true`, JWT auth requires `exp` claim | `EME_RETRIEVAL_JWT_REQUIRE_EXP` |
| `DASH_RETRIEVAL_ALLOWED_TENANTS` | no | unset (`*`) | optional tenant allowlist (comma-separated tenant IDs or `*`) for retrieval requests | `EME_RETRIEVAL_ALLOWED_TENANTS` |
| `DASH_RETRIEVAL_API_KEY_SCOPES` | no | unset | optional per-key tenant scopes (`key-a:tenant-a,tenant-b;key-b:*`) | `EME_RETRIEVAL_API_KEY_SCOPES` |
| `DASH_RETRIEVAL_AUDIT_LOG_PATH` | no | unset | optional JSONL audit log path for retrieval events (success/denied/error) | `EME_RETRIEVAL_AUDIT_LOG_PATH` |
| `DASH_RETRIEVAL_SEGMENT_DIR` | no | unset | optional segment read root directory used as additional retrieval allow-list prefilter | `EME_RETRIEVAL_SEGMENT_DIR` |
| `DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT` | no | `1000` | warning threshold for `/debug/storage-visibility` when WAL-delta claim count exceeds this value | `EME_RETRIEVAL_STORAGE_DIVERGENCE_WARN_DELTA_COUNT` |
| `DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO` | no | `0.25` | warning threshold for `/debug/storage-visibility` when `wal_delta_count / storage_visible_count` exceeds this value | `EME_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO` |
| `DASH_ROUTER_PLACEMENT_FILE` | no | unset | optional shard placement CSV file path (enables placement-aware read routing) | `EME_ROUTER_PLACEMENT_FILE` |
| `DASH_ROUTER_LOCAL_NODE_ID` | conditional (required when `DASH_ROUTER_PLACEMENT_FILE` is set) | unset | local node identity used to verify this retrieval instance is the routed read replica | `EME_ROUTER_LOCAL_NODE_ID` |
| `DASH_NODE_ID` | conditional alias | unset | fallback alias for local node identity if `DASH_ROUTER_LOCAL_NODE_ID` is unset | `EME_NODE_ID` |
| `DASH_ROUTER_READ_PREFERENCE` | no | `any_healthy` | read replica selection policy (`any_healthy`, `leader_only`, `prefer_follower`) | `EME_ROUTER_READ_PREFERENCE` |
| `DASH_ROUTER_SHARD_IDS` | no | inferred from placement file | optional shard ring override (comma-separated u32 IDs) | `EME_ROUTER_SHARD_IDS` |
| `DASH_ROUTER_VIRTUAL_NODES_PER_SHARD` | no | `64` | optional virtual-node count for consistent-hash shard ring | `EME_ROUTER_VIRTUAL_NODES_PER_SHARD` |
| `DASH_ROUTER_REPLICA_COUNT` | no | inferred from placement file | optional replica count override for routing plan | `EME_ROUTER_REPLICA_COUNT` |
| `DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS` | no | unset (`0` / disabled) | optional live placement reload interval for in-process route re-resolution (no-restart failover) | `EME_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS` |
| `DASH_RETRIEVAL_WAL_PATH` | no | unset | WAL path for startup replay mode | `EME_RETRIEVAL_WAL_PATH` |
| `DASH_RETRIEVAL_ANN_MAX_NEIGHBORS_BASE` | no | `12` | ANN base-layer max neighbors used after replay/build | `EME_RETRIEVAL_ANN_MAX_NEIGHBORS_BASE` |
| `DASH_RETRIEVAL_ANN_MAX_NEIGHBORS_UPPER` | no | `6` | ANN upper-layer max neighbors used after replay/build | `EME_RETRIEVAL_ANN_MAX_NEIGHBORS_UPPER` |
| `DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_FACTOR` | no | `12` | ANN search expansion multiplier | `EME_RETRIEVAL_ANN_SEARCH_EXPANSION_FACTOR` |
| `DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_MIN` | no | `64` | ANN minimum expansion budget clamp | `EME_RETRIEVAL_ANN_SEARCH_EXPANSION_MIN` |
| `DASH_RETRIEVAL_ANN_SEARCH_EXPANSION_MAX` | no | `4096` | ANN maximum expansion budget clamp | `EME_RETRIEVAL_ANN_SEARCH_EXPANSION_MAX` |

Runtime note:

- `axum` runtime selection requires build feature `async-transport` on each service crate.
- backup/restore tooling (`scripts/backup_state_bundle.sh`, `scripts/restore_state_bundle.sh`) reads these same `DASH_*` paths by default, so keep runtime envs and restore targets aligned.

## CI / Benchmark Guard

| Variable | Required | Default | Description | Legacy Fallback |
| --- | --- | --- | --- | --- |
| `DASH_BENCH_GUARD_MAX_REGRESSION_PCT` | no | `50` | max allowed DASH avg latency increase for history guard | `EME_BENCH_GUARD_MAX_REGRESSION_PCT` |
| `DASH_BENCH_ANN_MAX_NEIGHBORS_BASE` | no | `12` | benchmark run-time ANN base neighbor cap override | none |
| `DASH_BENCH_ANN_MAX_NEIGHBORS_UPPER` | no | `6` | benchmark run-time ANN upper neighbor cap override | none |
| `DASH_BENCH_ANN_SEARCH_EXPANSION_FACTOR` | no | `12` | benchmark run-time ANN search expansion multiplier | none |
| `DASH_BENCH_ANN_SEARCH_EXPANSION_MIN` | no | `64` | benchmark run-time ANN search minimum expansion clamp | none |
| `DASH_BENCH_ANN_SEARCH_EXPANSION_MAX` | no | `4096` | benchmark run-time ANN search maximum expansion clamp | none |
| `DASH_BENCH_LARGE_MIN_CANDIDATE_REDUCTION_PCT` | no | `95` | large profile minimum candidate reduction gate (%) | none |
| `DASH_BENCH_LARGE_MAX_DASH_LATENCY_MS` | no | `120` | large profile max DASH avg latency gate (ms) | none |
| `DASH_CONCURRENCY_INGEST_WAL_SYNC_EVERY_RECORDS` | no | `1` | ingestion transport concurrency benchmark WAL sync threshold override | none |
| `DASH_CONCURRENCY_INGEST_WAL_APPEND_BUFFER_RECORDS` | no | `1` | ingestion transport concurrency benchmark WAL append-buffer threshold override | none |
| `DASH_CONCURRENCY_INGEST_WAL_SYNC_INTERVAL_MS` | no | unset | ingestion transport concurrency benchmark WAL sync-interval override | none |
| `DASH_CONCURRENCY_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS` | no | unset (`auto`) | ingestion transport concurrency benchmark async WAL flush worker interval override | none |
| `DASH_CONCURRENCY_INGEST_WAL_BACKGROUND_FLUSH_ONLY` | no | `false` | ingestion transport concurrency benchmark background-only WAL flush toggle | none |
| `DASH_CONCURRENCY_INGEST_ALLOW_UNSAFE_WAL_DURABILITY` | no | `false` | ingestion transport concurrency benchmark unsafe WAL durability override (`true` for stress modes) | none |
| `DASH_CI_INCLUDE_LARGE_GUARD` | no | `false` | when `true`, runs large profile history guard in CI | `EME_CI_INCLUDE_LARGE_GUARD` |
| `DASH_CI_LARGE_GUARD_ITERATIONS` | no | unset (benchmark default) | override iterations for large CI guard run | none |
| `DASH_CI_LARGE_ANN_MAX_NEIGHBORS_BASE` | no | benchmark/default fallback (`12`) | large CI guard ANN base neighbor override | none |
| `DASH_CI_LARGE_ANN_MAX_NEIGHBORS_UPPER` | no | benchmark/default fallback (`6`) | large CI guard ANN upper neighbor override | none |
| `DASH_CI_LARGE_ANN_SEARCH_EXPANSION_FACTOR` | no | benchmark/default fallback (`12`) | large CI guard ANN search factor override | none |
| `DASH_CI_LARGE_ANN_SEARCH_EXPANSION_MIN` | no | benchmark/default fallback (`64`) | large CI guard ANN search min override | none |
| `DASH_CI_LARGE_ANN_SEARCH_EXPANSION_MAX` | no | benchmark/default fallback (`4096`) | large CI guard ANN search max override | none |
| `DASH_CI_LARGE_MIN_CANDIDATE_REDUCTION_PCT` | no | benchmark/default fallback (`95`) | large CI guard min candidate reduction gate (%) | none |
| `DASH_CI_LARGE_MAX_DASH_LATENCY_MS` | no | benchmark/default fallback (`120`) | large CI guard max DASH avg latency gate (ms) | none |
| `DASH_CI_INCLUDE_HYBRID_GUARD` | no | `false` | when `true`, runs hybrid profile history guard in CI | `EME_CI_INCLUDE_HYBRID_GUARD` |
| `DASH_CI_RUN_BENCH_TREND` | no | `false` | when `true`, runs `scripts/benchmark_trend.sh` inside CI | `EME_CI_RUN_BENCH_TREND` |
| `DASH_CI_BENCH_TREND_INCLUDE_LARGE` | no | `true` | include large profile in CI trend automation | `EME_CI_BENCH_TREND_INCLUDE_LARGE` |
| `DASH_CI_BENCH_TREND_INCLUDE_HYBRID` | no | `false` | include hybrid profile in CI trend automation | `EME_CI_BENCH_TREND_INCLUDE_HYBRID` |
| `DASH_CI_BENCH_TREND_TAG` | no | `ci-trend` | run-tag suffix for CI trend artifacts | `EME_CI_BENCH_TREND_TAG` |
| `DASH_CI_CHECK_ASYNC_TRANSPORT` | no | `false` | when `true`, runs async transport feature checks and tests (`cargo check` + `cargo test` for ingestion/retrieval with `--features async-transport`) | `EME_CI_CHECK_ASYNC_TRANSPORT` |
| `DASH_CI_INCLUDE_SLO_GUARD` | no | `false` | when `true`, runs `scripts/slo_guard.sh` as an SLO/error-budget CI gate | `EME_CI_INCLUDE_SLO_GUARD` |
| `DASH_CI_SLO_INCLUDE_RECOVERY_DRILL` | no | `false` | controls whether CI SLO guard includes recovery drill execution | none |

SLO guard runtime envs (`scripts/slo_guard.sh`):

- `DASH_SLO_HISTORY_PATH` (default `docs/benchmarks/history/runs/slo-history.csv`)
- `DASH_SLO_SUMMARY_DIR` (default `docs/benchmarks/history/runs`)
- `DASH_SLO_RUN_TAG` (default `manual`)
- `DASH_SLO_PROFILE` (default `smoke`)
- `DASH_SLO_ITERATIONS` (optional)
- `DASH_SLO_MAX_DASH_LATENCY_MS` (default `120`)
- `DASH_SLO_MIN_CONTRADICTION_F1` (default `0.80`)
- `DASH_SLO_MIN_ANN_RECALL_AT_10` (default `0.95`)
- `DASH_SLO_MIN_ANN_RECALL_AT_100` (default `0.99`)
- `DASH_SLO_MIN_QUALITY_PROBES_PASSED` (default `5`)
- `DASH_SLO_REQUIRE_DASH_HIT` (default `true`)
- `DASH_SLO_WINDOW_RUNS` (default `20`)
- `DASH_SLO_MAX_FAILED_RUN_PCT` (default `10`)
- `DASH_SLO_REQUIRE_CURRENT_PASS` (default `true`)
- `DASH_SLO_INCLUDE_RECOVERY_DRILL` (default `false`)
- `DASH_SLO_RECOVERY_MAX_RTO_SECONDS` (default `60`)
- `DASH_SLO_RECOVERY_MAX_RPO_CLAIM_GAP` (default `0`)
