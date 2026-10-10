//! The TOML configuration file overlay (`DASH_CONFIG_FILE`).
//!
//! ```toml
//! [ingestion]
//! wal_path = "/var/lib/dash/ingest.wal"
//!
//! [retrieval]
//! max_top_k = 200
//!
//! [common]
//! embedding_provider = "ollama"
//! ```
//!
//! Tables are `[ingestion]`, `[retrieval]`, `[control_plane]` and `[common]`.
//! A key is the lowercase suffix of the environment name (`wal_path` is
//! `DASH_INGEST_WAL_PATH`); the registry resolves it. The overlay only fills
//! variables that are not set in the environment under any of their names:
//! the environment always wins. Secret-typed keys require a private file mode.
//! Values are never printed.

use std::collections::{HashMap, HashSet};

use crate::model::{Kind, Scope, Service, Setting, file_keys, lookup_file_key};
use crate::validate::{Issue, did_you_mean, did_you_mean_within};

/// A key of the file resolved to a setting.
#[derive(Debug, Clone)]
pub struct FileEntry {
    pub table: Scope,
    pub key: String,
    pub setting: &'static Setting,
    /// Environment-style value (arrays joined with the list separator).
    pub value: String,
}

/// Tables that may appear in the file.
pub const TABLES: [Scope; 4] = [
    Scope::Ingestion,
    Scope::Retrieval,
    Scope::ControlPlane,
    Scope::Common,
];

fn issue(name: &str, message: impl Into<String>) -> Issue {
    Issue {
        name: name.to_string(),
        message: message.into(),
    }
}

/// Parse and resolve the file. Never includes a value in an error.
pub fn parse_file(content: &str) -> Result<Vec<FileEntry>, Vec<Issue>> {
    let doc: toml::Table = match content.parse() {
        Ok(doc) => doc,
        Err(err) => {
            // The default Display of a TOML error quotes the offending source
            // line, which may be a secret: report only line and column.
            let (line, col) = err
                .span()
                .map(|span| line_col(content, span.start))
                .unwrap_or((0, 0));
            return Err(vec![issue(
                "DASH_CONFIG_FILE",
                format!(
                    "invalid TOML at line {line}, column {col}: {}",
                    err.message()
                ),
            )]);
        }
    };

    let mut errors = Vec::new();
    let mut entries: Vec<FileEntry> = Vec::new();
    let mut seen: HashMap<String, String> = HashMap::new();

    for (table_name, table_value) in &doc {
        let Some(scope) = TABLES
            .iter()
            .copied()
            .find(|s| s.table() == table_name.as_str())
        else {
            let names: Vec<&str> = TABLES.iter().map(|s| s.table()).collect();
            let hint = did_you_mean_within(table_name, names.iter().copied(), 3)
                .map(|s| format!("; did you mean [{s}]?"))
                .unwrap_or_default();
            errors.push(issue(
                &format!("[{table_name}]"),
                format!(
                    "unknown table (expected one of: {}){hint}",
                    names
                        .iter()
                        .map(|n| format!("[{n}]"))
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
            continue;
        };
        let Some(table) = table_value.as_table() else {
            errors.push(issue(
                table_name,
                format!("must be a table; write [{table_name}] and put keys under it"),
            ));
            continue;
        };
        for (key, value) in table {
            let label = format!("[{table_name}] {key}");
            let Some(setting) = lookup_file_key(scope, key) else {
                errors.push(issue(&label, unknown_key_message(scope, key)));
                continue;
            };
            if setting.entry.external || setting.entry.planned {
                errors.push(issue(
                    &label,
                    format!(
                        "{} is not read by the services and cannot be set from the file",
                        setting.name
                    ),
                ));
                continue;
            }
            match to_env_value(setting, value) {
                Ok(text) => {
                    if let Some(previous) = seen.insert(setting.name.clone(), label.clone()) {
                        errors.push(issue(
                            &label,
                            format!("sets {} again (already set by {previous})", setting.name),
                        ));
                        continue;
                    }
                    entries.push(FileEntry {
                        table: scope,
                        key: key.clone(),
                        setting,
                        value: text,
                    });
                }
                Err(why) => errors.push(issue(&label, why)),
            }
        }
    }
    if errors.is_empty() {
        Ok(entries)
    } else {
        Err(errors)
    }
}

fn unknown_key_message(scope: Scope, key: &str) -> String {
    let keys = file_keys(scope);
    if let Some(hit) = did_you_mean(key, keys.iter().map(String::as_str)) {
        return format!("unknown key; did you mean '{hit}'?");
    }
    // The key may be valid in another table.
    for other in TABLES {
        if other != scope && lookup_file_key(other, key).is_some() {
            return format!(
                "unknown key in [{}]; it belongs in [{}]",
                scope.table(),
                other.table()
            );
        }
    }
    format!(
        "unknown key (see `dash-config list` for the settings of [{}])",
        scope.table()
    )
}

fn line_col(content: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(content.len());
    let before = &content[..offset];
    let line = before.matches('\n').count() + 1;
    let col = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
    (line, col)
}

fn scalar_text(value: &toml::Value, bool_as_digit: bool) -> Option<String> {
    match value {
        toml::Value::String(s) => Some(s.clone()),
        toml::Value::Integer(i) => Some(i.to_string()),
        toml::Value::Float(f) => Some(f.to_string()),
        toml::Value::Boolean(b) => Some(if bool_as_digit {
            if *b { "1" } else { "0" }.to_string()
        } else {
            b.to_string()
        }),
        _ => None,
    }
}

/// Convert a TOML value to the text a reader expects in the environment.
fn to_env_value(setting: &Setting, value: &toml::Value) -> Result<String, String> {
    let kind = setting.kind();
    let bool_as_digit = matches!(kind, Kind::Bool(_));
    match value {
        toml::Value::Array(items) => {
            let Some(sep) = kind.list_separator() else {
                return Err(format!(
                    "{} is not a list; give a single value",
                    setting.name
                ));
            };
            let mut parts = Vec::with_capacity(items.len());
            for item in items {
                match scalar_text(item, false) {
                    Some(text) => parts.push(text),
                    None => {
                        return Err("list items must be strings, numbers or booleans".to_string());
                    }
                }
            }
            Ok(parts.join(&sep.to_string()))
        }
        toml::Value::Table(_) => Err("a nested table is not a valid value".to_string()),
        toml::Value::Datetime(_) => Err("a date/time is not a valid value".to_string()),
        scalar => {
            scalar_text(scalar, bool_as_digit).ok_or_else(|| "unsupported value type".to_string())
        }
    }
}

/// Check the file mode when the file holds secret-typed keys. `mode` is the
/// unix permission bits (`None` where unknown, which skips the check).
pub fn check_permissions(entries: &[FileEntry], mode: Option<u32>, path: &str) -> Vec<Issue> {
    let secrets: Vec<&str> = entries
        .iter()
        .filter(|e| e.setting.kind().is_secret())
        .map(|e| e.key.as_str())
        .collect();
    let Some(mode) = mode else {
        return Vec::new();
    };
    if secrets.is_empty() {
        return Vec::new();
    }
    let bits = mode & 0o777;
    if matches!(bits, 0o600 | 0o640 | 0o400 | 0o440) {
        return Vec::new();
    }
    vec![issue(
        "DASH_CONFIG_FILE",
        format!(
            "{path} holds secret settings ({}) but has mode {bits:04o}; restrict it to 0600 or 0640 (chmod 600 {path})",
            secrets.join(", ")
        ),
    )]
}

/// Variables of `service` the file sets that are still unset in `env`, as
/// `(canonical name, value)` pairs. Does not modify `env`.
pub fn pending(
    service: Service,
    entries: &[FileEntry],
    env: &dyn crate::validate::EnvSource,
) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut taken: HashSet<&str> = HashSet::new();
    for entry in entries {
        let setting = entry.setting;
        if !setting.read_by(service) || !taken.insert(setting.name.as_str()) {
            continue;
        }
        if setting.all_names().iter().any(|n| env.get(n).is_some()) {
            continue;
        }
        out.push((setting.name.clone(), entry.value.clone()));
    }
    out
}

/// Fill `env` from the file for `service` (environment wins). Returns the
/// canonical names that were filled.
pub fn overlay_into(
    service: Service,
    entries: &[FileEntry],
    env: &mut HashMap<String, String>,
) -> Vec<String> {
    let todo = pending(service, entries, env);
    let mut applied = Vec::with_capacity(todo.len());
    for (name, value) in todo {
        env.insert(name.clone(), value);
        applied.push(name);
    }
    applied
}
