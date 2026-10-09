# Configuration file and startup validation

Every DASH setting is an environment variable (the full list is in the
[configuration reference](../reference/configuration.md)). Two features make
that list easier to operate:

- an **optional TOML file** that fills settings which are not set in the
  environment, and
- **startup validation** that stops a service with a clear message when a value
  is malformed or a variable name is misspelled, instead of silently using a
  default.

Both are derived from one typed registry of settings (`pkg/config`), so the
reference page, the validation rules and the file keys cannot drift apart.

## The configuration file

Point `DASH_CONFIG_FILE` at a TOML file. Every service reads it at startup:

```toml
# /etc/dash/dash.toml
[common]
embedding_provider = "ollama"
ollama_endpoint    = "http://ollama:11434"
http_request_timeout_ms = 10000

[ingestion]
bind        = "0.0.0.0:8081"
wal_path    = "/var/lib/dash/ingest.wal"
http_workers = 8
audit_log_path = "/var/lib/dash/audit/ingestion.jsonl"
audit_fail_closed = true

[retrieval]
bind     = "0.0.0.0:8080"
max_top_k = 200
replication_source_url = "http://ingestion:8081"

[control_plane]
bind    = "0.0.0.0:8090"
node_id = "control-plane-1"
```

```bash
DASH_CONFIG_FILE=/etc/dash/dash.toml ingestion
```

### Tables and keys

The tables are `[ingestion]`, `[retrieval]`, `[control_plane]` and `[common]`.
A key is the lowercase suffix of the environment variable name:

| Table | Prefix removed from the name | Example key | Variable |
|---|---|---|---|
| `[ingestion]` | `DASH_INGEST_` (or `DASH_`) | `wal_path` | `DASH_INGEST_WAL_PATH` |
| `[ingestion]` | `DASH_` | `checkpoint_max_wal_records` | `DASH_CHECKPOINT_MAX_WAL_RECORDS` |
| `[retrieval]` | `DASH_RETRIEVAL_` | `max_top_k` | `DASH_RETRIEVAL_MAX_TOP_K` |
| `[control_plane]` | `DASH_CONTROL_PLANE_` | `lease_path` | `DASH_CONTROL_PLANE_LEASE_PATH` |
| `[common]` | `DASH_` | `embedding_provider` | `DASH_EMBEDDING_PROVIDER` |

`dash-config list` prints every setting and `dash-config list --service
retrieval` only those a service reads. A service applies the keys of the
settings it reads, whichever table they are in; keys for other services are
checked for typos but otherwise ignored, so one shared file can configure the
whole deployment. A service-specific table also accepts the keys of `[common]`.

Values can be strings, integers, floats, booleans or arrays:

- a boolean becomes `1` or `0`, which every reader understands;
- an array is joined with the separator the reader expects (`,` for most lists,
  `;` for scoped keys and `kid:secret` maps), so
  `api_keys = ["key-one-...", "key-two-..."]` works;
- an array for a setting that is not a list is an error.

Settings that are read outside the Rust services (for example `DASH_BIN` for the
container entrypoint) cannot be set from the file.

### Precedence: the environment wins

The file only fills variables that are **not set** in the environment. A
variable counts as set if it is present under any of its names: the canonical
`DASH_*` name, a shared name such as `DASH_ANN_*`, a deprecated alias, or the
legacy `EME_*` spelling. A variable that is set to an empty string is also set,
so it still overrides the file. This keeps the usual deployment workflow
working: a Kubernetes `env:` entry or a `docker run -e` always wins over the file
baked into the image.

The values from the file are written into the process environment once, at
startup, before the service reads any setting. They are not re-read on `SIGHUP`
(see [Authentication](auth.md) for what `SIGHUP` reloads).

### Secrets in the file

Settings of type *secret* (API keys, JWT secrets, replication and control-plane
tokens, the audit fingerprint key, the OpenAI key) may be put in the file, but
then the file must not be readable by others: its mode must be `0600` or `0640`
(`0400` and `0440` are accepted as well). Otherwise the service refuses to start
and applies nothing from the file. The check only applies to files that actually
contain a secret key.

Secret values are never printed: not in validation messages, not in TOML syntax
errors (those report only the line and column), not by `dash-config print`.

## Startup validation

At the top of `main`, after logging is initialized, each service

1. reads the file named by `DASH_CONFIG_FILE` (if any) and fills unset variables,
2. validates the resulting environment against the registry,
3. prints every problem it found, then either starts (only warnings) or exits.

### Errors (exit code 2)

- a non-numeric value where a number is expected, or a number out of range
  (for example `DASH_INGEST_HTTP_WORKERS=abc` or `=0`);
- a word that is not one of the allowed values
  (`DASH_EMBEDDING_PROVIDER=olama`);
- a boolean that is not one of `1 true yes on 0 false no off`;
- a malformed `host:port` or URL;
- a blank value where blank has no meaning (`DASH_INGEST_BIND=`);
- any problem with the configuration file: unreadable, invalid TOML, an unknown
  table or key (with a *did you mean* suggestion), a key set twice, an array for
  a single-valued setting, or a world-readable file that holds secrets.

All errors are listed, not just the first:

```text
ingestion refusing to start: 2 configuration error(s):
  - DASH_INGEST_HTTP_WORKERS: out of range (got '0'); expected >= 1
  - [ingestion] wal_paht: unknown key; did you mean 'wal_path'?
fix the settings above, or set DASH_CONFIG_VALIDATION=warn to start anyway
```

The validator accepts every value the existing readers accept. Where readers
differ it accepts the union: for example `DASH_INSECURE_DEV_MODE` accepts `true`
for the data services even though the control plane only honors `1`. Where a
reader accepts a word but does not enable the setting (a few legacy switches
honor only the literal `1`), the value is not an error but a warning.

### Warnings (logged, the service starts)

- an unknown `DASH_*` or `EME_*` variable, with the closest known name when one
  is near (`DASH_INGEST_WAL_PAHT: unknown variable, ignored; did you mean
  DASH_INGEST_WAL_PATH?`). Variables that container platforms inject, such as
  Kubernetes service links (`DASH_INGESTION_SERVICE_HOST`,
  `DASH_INGESTION_PORT_8081_TCP`), are ignored;
- one deprecation warning per `EME_*` variable and per deprecated alias in use
  (`DASH_OLLAMA_BASE_URL` for `DASH_OLLAMA_ENDPOINT`,
  `DASH_*_JWT_ROLE_CLAIM` for `DASH_*_JWT_ROLES_CLAIM`). The `EME_*` fallbacks
  will be removed after one deprecation release;
- a truthy word for a switch whose reader would not enable it.

### `DASH_CONFIG_VALIDATION`

| Value | Behavior |
|---|---|
| `error` (default) | Errors stop the service with exit code 2. |
| `warn` | Errors are downgraded to warnings and the service starts. A file with errors still applies nothing. Use it for a staged rollout, then remove it. |

Any other value is itself an error.

## The `dash-config` tool

```bash
# Check a configuration without starting anything (exit 0, or 2 on errors).
# Reads the current environment and DASH_CONFIG_FILE; always strict.
DASH_CONFIG_FILE=/etc/dash/dash.toml dash-config validate --service ingestion

# Effective values and where they come from: env, file or default.
# Secrets are always shown as <redacted>; --redacted hides every configured value.
dash-config print --service retrieval --redacted

# List settings (optionally only those a service reads).
dash-config list --service control-plane

# Regenerate / check the reference page.
dash-config docs
dash-config docs --check
```

`dash-config` is built from `pkg/config` (`cargo run -p dash-config -- ...`).
`validate` is the way to check a configuration in CI or before a deploy.

## Adding a setting

Add a row to `pkg/config/src/registry.rs` (name, type, default, description) and
run `cargo run -p dash-config -- docs`. The test
`registry_covers_every_env_var_read_by_code` fails when the code reads a variable
the registry does not know, and when the registry lists one the code does not
read, so the registry cannot silently go stale.
