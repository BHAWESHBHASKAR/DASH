//! Generator of the configuration reference page
//! (`docs-site/docs/reference/configuration.md`).
//!
//! The tables come from the registry; the hand-written prose (conventions,
//! notes, fixed limits, examples) lives here as static sections so nothing is
//! lost when the page is regenerated. The output is deterministic.

use crate::model::{CONTROL, DATA, Entry, INGEST, RETRIEVAL, Scope};
use crate::registry::{
    REGISTRY, T_ANN, T_AUDIT, T_AUTH, T_BENCH, T_CONTAINER, T_EMBEDDING, T_ENCRYPTION,
    T_EXTRACTION, T_JWT, T_PLACEMENT, T_REPLICATION, T_RETRIEVAL, T_SEGMENTS, T_SERVER,
    T_TRANSPORT,
};

/// Path of the generated page, relative to the repository root.
pub const DOC_PATH: &str = "docs-site/docs/reference/configuration.md";

const HEADER: &str = r#"# Configuration

<!-- This page is generated from the settings registry in pkg/config (`dash-config docs`). Do not edit it by hand: change pkg/config/src/registry.rs (or the prose in pkg/config/src/docs.rs) and regenerate. -->

DASH is configured through environment variables. Any setting can also be given in an optional TOML file named by `DASH_CONFIG_FILE`; the environment always wins over the file (see [Configuration file](../operations/configuration-file.md)). Another file input is the [auth overlay](#config-reload-and-sighup) used for SIGHUP reload. This page lists **only variables that the Rust code reads**. It is generated from the settings registry (`pkg/config`), which a test keeps in step with the sources in `services/`, `pkg/` and `tools/`. If this page and the code disagree, the code wins.

Two checks keep the list complete: the test `registry_covers_every_env_var_read_by_code` fails when the code reads a variable the registry does not know (or the registry lists one no code reads), and `scripts/check_config_docs.sh` fails when this page differs from what the registry generates. To change this page, edit the registry and run `cargo run -p dash-config -- docs`. See [CONTRIBUTING](../about/contributing.md).

## Conventions

- **Legacy `EME_` aliases.** Most `DASH_*` service variables are also read under the legacy name `EME_*` (for example `EME_INGEST_BIND`) when the `DASH_*` name is unset. The `EME_` prefix is deprecated: a service logs a warning at startup for every `EME_*` variable that is set, and the fallbacks will be dropped after one deprecation release. New deployments should use `DASH_*`. Variables with **no** `EME_` alias are marked "DASH only" in the tables below; in general these are the ones added in 0.3.0 plus the embedding, Ollama, OpenAI, vector-backend, strict-mode and logging variables.
- **Booleans** accept `1`, `true`, `yes`, `on` (true) and `0`, `false`, `no`, `off` (false), case-insensitive, unless a row says otherwise. A few legacy switches enable only on the literal `1` (or `1`/`true`/`yes`); those rows say so, and startup validation warns when another truthy spelling is used for them.
- **Startup validation.** Every service validates its environment before it starts (see [Configuration file](../operations/configuration-file.md)). A malformed value of a typed setting (a non-number where a number is expected, a value out of range, an unknown word for an enumerated setting, an unparsable boolean, a blank value where blank is meaningless) is an error: the service prints every error and exits with code 2. `DASH_CONFIG_VALIDATION=warn` downgrades errors to warnings. Unknown `DASH_*` / `EME_*` variables are reported as warnings with a did-you-mean suggestion. The readers themselves still fall back to the default silently on an unparseable number; validation is what makes that visible.
- **Bind addresses** are full `host:port` strings, not a bare port. There is no `DASH_INGEST_PORT` or `DASH_RETRIEVAL_PORT`.
- **Exit codes.** A service that refuses to start because of invalid configuration exits with code 2 (auth, secrets, WAL durability guardrails, placement routing in ingestion, startup validation) or 1 (failed to open or replay the WAL, retrieval placement configuration, listener failure).
- Variables are read at process start, with these exceptions: the authentication settings can be reloaded with SIGHUP (see below); the revoked-key and revoked-`jti` files are re-read when their modification time or size changes (checked at most once per second); the embedding provider settings (`DASH_EMBEDDING_PROVIDER`, `DASH_OLLAMA_*`, `DASH_OPENAI_*`) and the audit-log path variables are looked up per request, but treat a restart as the supported way to change them.

## Defaults at a glance

| Service | Default bind | Role |
|---|---|---|
| ingestion | `127.0.0.1:8081` | write API, WAL owner, replication source |
| retrieval | `127.0.0.1:8080` | read API, embeddings endpoint, replication follower |
| control-plane | `127.0.0.1:8090` | placement and leader state |

The container image and compose file override the bind addresses to `0.0.0.0:<port>` (and the compose file publishes the host ports on `127.0.0.1`).

"#;

const TAIL: &str = r#"## Config reload and SIGHUP

On unix, `kill -HUP <pid>` makes `ingestion` and `retrieval` rebuild their whole authentication policy from the process environment, overlaid with the file named by `DASH_CONFIG_RELOAD_FILE`. The overlay holds `KEY=VALUE` lines (blank lines and `#` comments ignored, optional quotes stripped), only `DASH_*` / `EME_*` keys are read, and an overlay value wins over the process environment. The file is also read at startup. A new policy that fails validation is rejected and the previous one stays in force. Only authentication settings are reloaded. Details: `docs/operations/auth.md`.

Values that come from `DASH_CONFIG_FILE` (the TOML file) are applied to the process environment once at startup, so a SIGHUP reload sees them as ordinary environment variables; the file is not re-read.

## Variables that do not exist

Earlier versions of this page documented the following. No code reads them; setting them does nothing (startup validation reports them as unknown variables).

`DASH_LOG_LEVEL` (use `RUST_LOG`), `DASH_EMBEDDING_MODEL`, `DASH_EMBEDDING_DIM`, `DASH_OPENAI_BASE_URL`, `DASH_OPENAI_TIMEOUT_MS`, `DASH_OLLAMA_TIMEOUT_MS`, `DASH_INGEST_WAL_SEGMENT_BYTES`, `DASH_INGEST_WAL_FSYNC_POLICY`, `DASH_INGEST_CHECKPOINT_ON_SIGHUP`, `DASH_INGEST_JWT_PUBLIC_KEY`, `DASH_INGEST_JWT_PUBLIC_KEY_PATH`, `DASH_INGEST_JWT_ALGORITHM`, `DASH_RETRIEVAL_JWT_PUBLIC_KEY`, `DASH_RETRIEVAL_JWT_PUBLIC_KEY_PATH`, `DASH_RETRIEVAL_JWT_ALGORITHM`, `DASH_API_KEY_OVERLAP_SECONDS`, `DASH_INGEST_PORT`, `DASH_RETRIEVAL_PORT`, `DASH_INGEST_WORKERS`, `DASH_RETRIEVAL_WORKERS`, `DASH_TCP_READ_TIMEOUT_MS`, `DASH_MAX_BODY_BYTES`, `DASH_RETRIEVAL_RATE_LIMIT_RPS`, `DASH_INGEST_RATE_LIMIT_RPS` (the real names are `*_RATE_LIMIT_PER_TENANT_RPS`), `DASH_ANN_M`, `DASH_ANN_EF_CONSTRUCTION`, `DASH_ANN_EF_SEARCH`, `DASH_ANN_REBUILD_THRESHOLD`, `DASH_AUDIT_LOG_PATH` (use `DASH_INGEST_AUDIT_LOG_PATH` / `DASH_RETRIEVAL_AUDIT_LOG_PATH`), `DASH_AUDIT_RETENTION_DAYS`, `DASH_AUDIT_COMPACT_AT_RATIO`, `DASH_IDEMPOTENCY_RETENTION_DAYS`, `DASH_METRICS_ENABLED`, `DASH_METRICS_NAMESPACE`.

## Example

A local development retrieval + ingestion pair, using real variable names (generate real secrets with `scripts/generate-secrets.sh`):

```bash
export DASH_INGEST_API_KEY=$(openssl rand -hex 32)       # 64 hex chars
export DASH_RETRIEVAL_API_KEY=$(openssl rand -hex 32)
export DASH_INGEST_REPLICATION_TOKEN=$(openssl rand -hex 32)
export DASH_RETRIEVAL_REPLICATION_TOKEN=$DASH_INGEST_REPLICATION_TOKEN

DASH_INGEST_BIND=127.0.0.1:8081 \
DASH_INGEST_WAL_PATH=./data/ingest.wal \
  cargo run --release -p ingestion

DASH_RETRIEVAL_BIND=127.0.0.1:8080 \
DASH_RETRIEVAL_REPLICATION_SOURCE_URL=http://127.0.0.1:8081 \
DASH_RETRIEVAL_REPLICATION_OFFSET_PATH=./data/retrieval.offset \
  cargo run --release -p retrieval
```

(Run each command in its own terminal. The services serve by default; `--cli` runs a one-shot smoke path instead.)

The same settings can live in a TOML file instead; see [Configuration file](../operations/configuration-file.md). To check a configuration without starting a service, run `dash-config validate --service ingestion`.

For the full deployment story, see [Deploy](../operations/deploy.md).
"#;

/// Scopes in page order, with their section heading and intro.
const SECTIONS: &[(Scope, &str, &str)] = &[
    (
        Scope::Common,
        "Common settings",
        "Settings shared by the services (or built per service from the `DASH_INGEST_` / `DASH_RETRIEVAL_` prefixes). The **Notes** column names the services that read a setting when that is not both ingestion and retrieval.",
    ),
    (
        Scope::Ingestion,
        "Ingestion settings",
        "Read by the `ingestion` service.",
    ),
    (
        Scope::Retrieval,
        "Retrieval settings",
        "Read by the `retrieval` service.",
    ),
    (
        Scope::ControlPlane,
        "Control-plane settings",
        "Read by the `control-plane` service.",
    ),
    (
        Scope::Tools,
        "Benchmarks and load tests",
        "`DASH_BENCH_*` (benchmark thresholds and fixtures in `tests/benchmarks`), `DASH_LIVE_URL` and `DASH_API_KEY` (load-test binary) are read only by the benchmark binaries. See the comments at the top of `tests/benchmarks/src/main.rs` and `tests/benchmarks/src/bin/load_test.rs`.",
    ),
];

/// Prose placed before and after the table of a topic.
fn topic_prose(scope: Scope, topic: &str) -> (&'static str, &'static str) {
    match (scope, topic) {
        (Scope::Common, t) if t == T_TRANSPORT => (
            "",
            "`/health`, `/live`, `/ready`, their `/v1/` forms and `/metrics` are served by two reserved workers, so slow requests cannot starve probes. Read-stage failures (400/408/413/417/431/501/505) are counted in `dash_*_transport_read_error_total{status_class}`.\n\nFixed limits (compiled in, **not** configurable): request body cap 16 MiB (413), request line and each header line 8 KiB, header block 32 KiB, at most 100 headers (431), per-connection socket timeout 5 seconds, `Transfer-Encoding` is not supported (501). The control plane has its own limits (see the control-plane settings below).",
        ),
        (Scope::Common, t) if t == T_AUTH => (
            "Ingestion variables use the `DASH_INGEST_` prefix; retrieval variables use `DASH_RETRIEVAL_`. Both services support the same set, listed once below with both full names. Credentials are separate per service: an ingestion key does not work on retrieval. Clients send an API key in the `x-api-key` header or as `Authorization: Bearer <key>`. The full decision order, role table and JWT/OIDC behavior are in the operations guide `docs/operations/auth.md`.\n\nThe service refuses to start (exit 2) when no authentication method is configured and `DASH_INSECURE_DEV_MODE` is not set. A configured but invalid setting (unknown role name, malformed scoped key, OIDC without issuer/audience, weak secret under strict mode) also stops startup.",
            "Rate-limit state is in memory per process and starts fresh after a restart or a SIGHUP reload.",
        ),
        (Scope::Common, t) if t == T_JWT => (
            "",
            "HS256 tokens carry tenants in `tenant_id` (string) or `tenants` / `tenant_ids` (array). OIDC mode accepts only asymmetric algorithms (RS256/384/512, PS256/384/512, ES256/384, EdDSA) and requires a `kid`. The automated tests exercise RS256 signature verification, RS256-vs-RS384 key restriction and the JWKS cache behavior; the other listed algorithms are accepted by the allow-list but have no dedicated test. There are no RS256/ES256 PEM-key variables (`..._JWT_PUBLIC_KEY`, `..._JWT_ALGORITHM` do not exist).",
        ),
        (Scope::Common, t) if t == T_AUDIT => (
            "",
            "Verify a log with `tools/audit-verify` or `scripts/verify_audit_chain.sh`.",
        ),
        (Scope::Common, t) if t == T_ANN => (
            "Each variable can be set per service (`DASH_INGEST_ANN_*`, `DASH_RETRIEVAL_ANN_*`) or shared (`DASH_ANN_*`); the per-service name wins, then the shared name, then the `EME_` forms. Values must be positive integers. The index is an in-repo HNSW-style graph, not `usearch`.",
            "There is no `DASH_ANN_M`, `DASH_ANN_EF_CONSTRUCTION`, `DASH_ANN_EF_SEARCH` or `DASH_ANN_REBUILD_THRESHOLD`.",
        ),
        (Scope::Common, t) if t == T_PLACEMENT => (
            "Placement routing is enabled when `DASH_ROUTER_PLACEMENT_FILE` or `DASH_ROUTER_CONTROL_PLANE_URL` is set. Both services then require a local node id: ingestion exits (code 2) and retrieval fails to start (exit 1) if it is missing or if the placement source cannot be loaded.",
            "",
        ),
        (Scope::Common, t) if t == T_EMBEDDING => (
            "",
            "The provider clients use HTTPS (rustls) where the URL is `https://`, do not follow redirects, cap response bodies, retry with jittered backoff and sit behind a circuit breaker whose half-open state admits a single probe. The hash provider's default dimension is 384. Timeouts are compiled in (Ollama 5 s, OpenAI 30 s). There is no `DASH_EMBEDDING_MODEL`, `DASH_EMBEDDING_DIM`, `DASH_OPENAI_BASE_URL`, `DASH_OPENAI_TIMEOUT_MS` or `DASH_OLLAMA_TIMEOUT_MS`.\n\nNetwork providers (`ollama`, `openai`) are wrapped in a circuit breaker and a concurrency cap (`DASH_EMBEDDING_MAX_CONCURRENCY`, `DASH_EMBEDDING_QUEUE_WAIT_MS`, `DASH_EMBEDDING_BREAKER_THRESHOLD`, `DASH_EMBEDDING_BREAKER_RESET_MS`). Provider outages (breaker open, timeout, connection error, 429, 5xx, concurrency cap) answer 503 `embedding_unavailable` with `Retry-After` (the upstream's value when present, otherwise 1). Unusable provider output (non-finite values, wrong dimensions, malformed payload, other 4xx) answers 502 `embedding_provider_error` (retrieval) or `embedding_upstream_error` (ingestion).",
        ),
        (Scope::Common, t) if t == T_ENCRYPTION => (
            "`DASH_ENCRYPTION_PROVIDER` (`none` or `env`) and `DASH_ENCRYPTION_MASTER_KEY` (64 hex characters or base64 of 32 bytes) are read by `pkg/encryption::provider_from_env` (also under the `EME_` names). **No service calls it**, so setting them has no effect on stored data. Encryption at rest is planned (P4).",
            "",
        ),
        (Scope::Common, t) if t == T_CONTAINER => (
            "`DASH_BIN` (which binary the image entrypoint runs: `ingestion`, `retrieval`, `control-plane` or `segment-maintenance-daemon`; default `retrieval`), `DASH_HOME` (default `/opt/dash`) and `DASH_HEALTHCHECK_URL` are read by the shell scripts in `deploy/container/scripts/`, not by the Rust services. `DASH_PUBLISH_ADDR` (default `127.0.0.1`) is read by the compose file for published ports. The compose file may also set `DASH_INGEST_TRANSPORT_RUNTIME` and `DASH_RETRIEVAL_TRANSPORT_RUNTIME`; no code reads them. These are known to the validator so that they do not produce unknown-variable warnings.",
            "",
        ),
        (Scope::Ingestion, t) if t == T_REPLICATION => (
            "An ingestion node can itself follow another ingestion node; it then pulls WAL frames from the source, persists `(generation, offset)` together and resyncs from a full export when the leader's WAL generation changes.",
            "",
        ),
        (Scope::Ingestion, t) if t == T_SEGMENTS => (
            "Read by the ingestion service (segment publishing) and the `segment-maintenance-daemon` binary.",
            "Tenant directories under the segment root are named by an injective escaping of the tenant id (bytes outside `a-z0-9-` become `_xx`; ids over 96 characters get a hash suffix). Existing directories created by the older lossy sanitizer are renamed automatically the first time a tenant is touched.",
        ),
        (Scope::Ingestion, t) if t == T_EXTRACTION => (
            "These control `POST /v1/ingest/raw` and `/v1/ingest/document` (see `GET /debug/document-parser`).",
            "Adapter commands are operator-controlled shell command lines; anyone who can set the service environment can run code as the service user.",
        ),
        (Scope::Retrieval, t) if t == T_REPLICATION => (
            "Both followers pull WAL frames from an ingestion node, persist `(generation, offset)` together and resync from a full export when the leader's WAL generation changes. Retrieval's offset is saved next to its WAL as `<DASH_RETRIEVAL_WAL_PATH>.replication` when a retrieval WAL is configured; without a retrieval WAL nothing replicated survives a restart and the follower always starts with a full resync.",
            "",
        ),
        (Scope::Retrieval, t) if t == T_RETRIEVAL => (
            "",
            "Fixed retrieve bounds (not configurable): `query` at most 8 KiB, at most 256 values in each of `entity_filters` and `embedding_id_filters`, `query_embedding` at most 8192 values.",
        ),
        (Scope::ControlPlane, t) if t == T_SERVER => (
            "",
            "All six server settings are DASH only and the server ignores values of 0 or unparseable values; startup validation reports them as errors instead.",
        ),
        (Scope::Tools, t) if t == T_BENCH => ("", ""),
        _ => ("", ""),
    }
}

fn scope_default_readers(scope: Scope) -> u8 {
    match scope {
        Scope::Ingestion => INGEST,
        Scope::Retrieval => RETRIEVAL,
        Scope::ControlPlane => CONTROL,
        Scope::Common => DATA,
        Scope::Tools => crate::model::TOOLS,
    }
}

fn escape_cell(text: &str) -> String {
    text.replace('|', "\\|").replace('\n', " ")
}

fn code(name: &str) -> String {
    format!("`{name}`")
}

fn default_cell(entry: &Entry) -> String {
    fn one(text: &str) -> String {
        if text.is_empty() {
            "unset".to_string()
        } else if text.contains(' ') || text.contains('*') {
            text.to_string()
        } else {
            format!("`{text}`")
        }
    }
    match entry.svc_default {
        Some((i, r)) if i != r => format!("{} (ingestion), {} (retrieval)", one(i), one(r)),
        Some((i, _)) => one(i),
        None => one(entry.default),
    }
}

fn variable_cell(entry: &Entry) -> String {
    let mut names: Vec<String> = Vec::new();
    if entry.is_pattern() {
        names.push(
            ["INGEST", "RETRIEVAL"]
                .iter()
                .map(|svc| code(&entry.name.replace("{SVC}", svc)))
                .collect::<Vec<_>>()
                .join(" / "),
        );
    } else {
        names.push(code(entry.name));
    }
    let mut cell = names.join("");
    let shared: Vec<String> = entry
        .aliases
        .iter()
        .filter(|a| !a.deprecated)
        .map(|a| code(a.name))
        .collect();
    if !shared.is_empty() {
        cell.push_str(&format!(" (shared: {})", shared.join(", ")));
    }
    cell
}

fn reader_names(readers: u8) -> String {
    let mut v = Vec::new();
    if readers & INGEST != 0 {
        v.push("ingestion");
    }
    if readers & RETRIEVAL != 0 {
        v.push("retrieval");
    }
    if readers & CONTROL != 0 {
        v.push("control-plane");
    }
    if readers & crate::model::TOOLS != 0 {
        v.push("tools");
    }
    v.join(", ")
}

fn notes_cell(entry: &Entry) -> String {
    let mut notes: Vec<String> = Vec::new();
    if !entry.notes.is_empty() {
        notes.push(entry.notes.replace("{SVC}", "<SVC>"));
    }
    let deprecated: Vec<String> = entry
        .aliases
        .iter()
        .filter(|a| a.deprecated)
        .map(|a| code(&a.name.replace("{SVC}", "<SVC>")))
        .collect();
    if !deprecated.is_empty() && entry.notes.is_empty() {
        notes.push(format!("Deprecated alias: {}.", deprecated.join(", ")));
    }
    if !entry.eme && entry.scope != Scope::Tools && !entry.external {
        notes.push("DASH only.".to_string());
    }
    if entry.readers == 0 {
        notes.push("Not read by any service.".to_string());
    } else if entry.readers != scope_default_readers(entry.scope) && !entry.external {
        notes.push(format!("Read by {}.", reader_names(entry.readers)));
    }
    if entry.planned {
        notes.push("Planned, not implemented.".to_string());
    }
    if let crate::model::Kind::Bool(honors) = entry.kind
        && honors != crate::model::Honors::Lenient
    {
        notes.push(format!("Enabled by {}.", honors.describe()));
    }
    notes.join(" ")
}

fn render_table(entries: &[&Entry], out: &mut String) {
    out.push_str("| Variable | Default | Type | Description | Notes |\n");
    out.push_str("|---|---|---|---|---|\n");
    for entry in entries {
        out.push_str(&format!(
            "| {} | {} | {} | {} | {} |\n",
            variable_cell(entry),
            escape_cell(&default_cell(entry)),
            escape_cell(&entry.kind.describe()),
            escape_cell(entry.description),
            escape_cell(&notes_cell(entry)),
        ));
    }
    out.push('\n');
}

/// Render the whole page.
pub fn render() -> String {
    let mut out = String::from(HEADER);

    for (scope, title, intro) in SECTIONS {
        let entries: Vec<&Entry> = REGISTRY
            .iter()
            .filter(|e| e.scope == *scope && !e.planned)
            .collect();
        if entries.is_empty() {
            continue;
        }
        out.push_str(&format!("## {title}\n\n{intro}\n\n"));
        // Topics in order of first appearance.
        let mut topics: Vec<&str> = Vec::new();
        for e in &entries {
            if !topics.contains(&e.topic) {
                topics.push(e.topic);
            }
        }
        for topic in topics {
            let rows: Vec<&Entry> = entries
                .iter()
                .copied()
                .filter(|e| e.topic == topic)
                .collect();
            let (before, after) = topic_prose(*scope, topic);
            out.push_str(&format!("### {topic}\n\n"));
            if !before.is_empty() {
                out.push_str(before);
                out.push_str("\n\n");
            }
            render_table(&rows, &mut out);
            if !after.is_empty() {
                out.push_str(after);
                out.push_str("\n\n");
            }
        }
    }

    out.push_str(TAIL);
    out
}

/// Result of comparing the committed page with the generated one.
#[derive(Debug, PartialEq, Eq)]
pub enum CheckOutcome {
    UpToDate,
    Stale { first_difference_line: usize },
    Missing,
}

/// Compare `committed` (file content, `None` if missing) with [`render`].
pub fn check(committed: Option<&str>) -> CheckOutcome {
    let Some(committed) = committed else {
        return CheckOutcome::Missing;
    };
    let expected = render();
    if committed == expected {
        return CheckOutcome::UpToDate;
    }
    let line = committed
        .lines()
        .zip(expected.lines())
        .position(|(a, b)| a != b)
        .unwrap_or_else(|| committed.lines().count().min(expected.lines().count()))
        + 1;
    CheckOutcome::Stale {
        first_difference_line: line,
    }
}
