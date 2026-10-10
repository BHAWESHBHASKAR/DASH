//! Data model of the settings registry: scopes, value kinds and entries.
//!
//! The table itself lives in [`crate::registry`]. Everything here is `const`
//! friendly so the table can be a plain `static` slice.

use std::collections::HashMap;
use std::sync::OnceLock;

/// A DASH service (process) that reads settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Service {
    Ingestion,
    Retrieval,
    ControlPlane,
}

impl Service {
    pub const ALL: [Service; 3] = [
        Service::Ingestion,
        Service::Retrieval,
        Service::ControlPlane,
    ];

    /// Name used on the command line and in messages.
    pub fn name(self) -> &'static str {
        match self {
            Service::Ingestion => "ingestion",
            Service::Retrieval => "retrieval",
            Service::ControlPlane => "control-plane",
        }
    }

    /// Parse a service name (`ingestion`, `retrieval`, `control-plane`, with a
    /// few spellings).
    pub fn parse(raw: &str) -> Option<Service> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "ingestion" | "ingest" => Some(Service::Ingestion),
            "retrieval" => Some(Service::Retrieval),
            "control-plane" | "control_plane" | "controlplane" => Some(Service::ControlPlane),
            _ => None,
        }
    }

    /// The scope whose settings belong to this service alone.
    pub fn scope(self) -> Scope {
        match self {
            Service::Ingestion => Scope::Ingestion,
            Service::Retrieval => Scope::Retrieval,
            Service::ControlPlane => Scope::ControlPlane,
        }
    }

    pub(crate) fn reader_bit(self) -> u8 {
        match self {
            Service::Ingestion => INGEST,
            Service::Retrieval => RETRIEVAL,
            Service::ControlPlane => CONTROL,
        }
    }
}

/// Reader bit flags: which processes read a setting.
pub const INGEST: u8 = 1;
pub const RETRIEVAL: u8 = 2;
pub const CONTROL: u8 = 4;
pub const TOOLS: u8 = 8;
/// Ingestion and retrieval.
pub const DATA: u8 = INGEST | RETRIEVAL;
/// All three services.
pub const ALL_SERVICES: u8 = INGEST | RETRIEVAL | CONTROL;

/// Which part of the system owns a setting. It also decides which table of
/// the TOML file overlay the setting lives in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Scope {
    Ingestion,
    Retrieval,
    ControlPlane,
    /// Shared by several services (or by every service).
    Common,
    /// Benchmarks, load tests and other developer tooling.
    Tools,
}

impl Scope {
    pub const ALL: [Scope; 5] = [
        Scope::Common,
        Scope::Ingestion,
        Scope::Retrieval,
        Scope::ControlPlane,
        Scope::Tools,
    ];

    /// Table name in the TOML overlay (`[ingestion]`), also used in docs.
    pub fn table(self) -> &'static str {
        match self {
            Scope::Ingestion => "ingestion",
            Scope::Retrieval => "retrieval",
            Scope::ControlPlane => "control_plane",
            Scope::Common => "common",
            Scope::Tools => "tools",
        }
    }

    pub fn from_table(raw: &str) -> Option<Scope> {
        Scope::ALL.into_iter().find(|s| s.table() == raw)
    }

    pub fn title(self) -> &'static str {
        match self {
            Scope::Common => "Common",
            Scope::Ingestion => "Ingestion",
            Scope::Retrieval => "Retrieval",
            Scope::ControlPlane => "Control plane",
            Scope::Tools => "Tools and benchmarks",
        }
    }

    /// The env-name prefix stripped to form the file key.
    fn key_prefix(self) -> &'static str {
        match self {
            Scope::Ingestion => "DASH_INGEST_",
            Scope::Retrieval => "DASH_RETRIEVAL_",
            Scope::ControlPlane => "DASH_CONTROL_PLANE_",
            Scope::Common | Scope::Tools => "DASH_",
        }
    }
}

/// Which spellings a reader treats as "true". Every flavor treats the usual
/// false words (`0 false no off`) as false; they differ in what enables the
/// setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Honors {
    /// `1 true yes on`, case-insensitive.
    Lenient,
    /// Only the literal `1`.
    One,
    /// `1 true yes`, case-insensitive (no `on`).
    OneTrueYes,
    /// Exactly `1`, `true` or `TRUE`.
    OneTrueCaps,
    /// Exactly `1`, `true`, `TRUE` or `yes`.
    OneTrueCapsYes,
}

impl Honors {
    /// Does a reader of this flavor enable the setting for `raw`?
    pub fn enables(self, raw: &str) -> bool {
        let t = raw.trim();
        match self {
            Honors::Lenient => {
                matches!(t.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
            }
            Honors::One => t == "1",
            Honors::OneTrueYes => {
                matches!(t.to_ascii_lowercase().as_str(), "1" | "true" | "yes")
            }
            Honors::OneTrueCaps => matches!(t, "1" | "true" | "TRUE"),
            Honors::OneTrueCapsYes => matches!(t, "1" | "true" | "TRUE" | "yes"),
        }
    }

    /// Spellings shown in docs.
    pub fn describe(self) -> &'static str {
        match self {
            Honors::Lenient => "`1`, `true`, `yes`, `on`",
            Honors::One => "only the literal `1`",
            Honors::OneTrueYes => "`1`, `true`, `yes`",
            Honors::OneTrueCaps => "`1`, `true`, `TRUE`",
            Honors::OneTrueCapsYes => "`1`, `true`, `TRUE`, `yes`",
        }
    }
}

/// The value type of a setting.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Kind {
    /// Boolean switch. Valid values are the union of every reader's spellings
    /// (`1 true yes on 0 false no off`); `Honors` says which of them this
    /// setting's reader actually enables.
    Bool(Honors),
    /// Unsigned integer in `min..=max`.
    Int { min: u64, max: u64 },
    /// Duration in milliseconds, `min..=max`.
    Millis { min: u64, max: u64 },
    /// Finite floating point number in `min..=max`.
    Float { min: f64, max: f64 },
    /// Free text.
    Str,
    /// Filesystem path.
    Path,
    /// Credential. Never printed, file overlay requires a private file mode.
    Secret,
    /// Separated list of credentials (a TOML array is joined with `sep`).
    SecretList { sep: char },
    /// `host:port` listen/bind address.
    Addr,
    /// `http://` or `https://` URL.
    Url,
    /// Separated list. With `u32_items` every item must be an unsigned integer.
    List { sep: char, u32_items: bool },
    /// One of a fixed set of words (case-insensitive, surrounding blanks ignored).
    Enum { values: &'static [&'static str] },
    /// Either one of `words` (case-insensitive) or an integer `>= min`.
    IntOrWords {
        words: &'static [&'static str],
        min: u64,
    },
}

impl Kind {
    pub const BOOL: Kind = Kind::Bool(Honors::Lenient);
    pub const UINT: Kind = Kind::Int {
        min: 0,
        max: u64::MAX,
    };
    pub const POSITIVE: Kind = Kind::Int {
        min: 1,
        max: u64::MAX,
    };
    pub const MILLIS: Kind = Kind::Millis {
        min: 0,
        max: u64::MAX,
    };
    pub const POSITIVE_MILLIS: Kind = Kind::Millis {
        min: 1,
        max: u64::MAX,
    };
    pub const CSV: Kind = Kind::List {
        sep: ',',
        u32_items: false,
    };
    pub const SEMI: Kind = Kind::List {
        sep: ';',
        u32_items: false,
    };
    pub const ANY_FLOAT: Kind = Kind::Float {
        min: f64::MIN,
        max: f64::MAX,
    };
    pub const NON_NEGATIVE_FLOAT: Kind = Kind::Float {
        min: 0.0,
        max: f64::MAX,
    };

    pub fn is_secret(self) -> bool {
        matches!(self, Kind::Secret | Kind::SecretList { .. })
    }

    /// Separator used when a TOML array is joined into an environment value.
    pub fn list_separator(self) -> Option<char> {
        match self {
            Kind::List { sep, .. } | Kind::SecretList { sep } => Some(sep),
            _ => None,
        }
    }

    /// Whether a blank value is acceptable unless the entry overrides it.
    const fn blank_default(self) -> bool {
        matches!(
            self,
            Kind::Str | Kind::Secret | Kind::SecretList { .. } | Kind::List { .. }
        )
    }

    /// Short human description used in the docs `Type` column.
    pub fn describe(self) -> String {
        match self {
            Kind::Bool(_) => "bool".to_string(),
            Kind::Int { min, max } => match (min, max) {
                (0, u64::MAX) => "integer".to_string(),
                (m, u64::MAX) => format!("integer >= {m}"),
                (m, x) => format!("integer {m}..{x}"),
            },
            Kind::Millis { min, max } => match (min, max) {
                (0, u64::MAX) => "milliseconds".to_string(),
                (m, u64::MAX) => format!("milliseconds >= {m}"),
                (m, x) => format!("milliseconds {m}..{x}"),
            },
            Kind::Float { min, max } => {
                if min == f64::MIN && max == f64::MAX {
                    "number".to_string()
                } else if max == f64::MAX {
                    format!("number >= {min}")
                } else {
                    format!("number {min}..{max}")
                }
            }
            Kind::Str => "string".to_string(),
            Kind::Path => "path".to_string(),
            Kind::Secret => "secret".to_string(),
            Kind::SecretList { sep } => format!("secret list (`{sep}`)"),
            Kind::Addr => "host:port".to_string(),
            Kind::Url => "URL".to_string(),
            Kind::List { sep, u32_items } => {
                if u32_items {
                    format!("list of integers (`{sep}`)")
                } else {
                    format!("list (`{sep}`)")
                }
            }
            Kind::Enum { values } => values
                .iter()
                .map(|v| format!("`{v}`"))
                .collect::<Vec<_>>()
                .join(" | "),
            Kind::IntOrWords { words, min } => {
                let w = words
                    .iter()
                    .map(|v| format!("`{v}`"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("integer >= {min} or {w}")
            }
        }
    }
}

/// An alternative spelling of a setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Alias {
    pub name: &'static str,
    /// Deprecated aliases produce a startup warning when set.
    pub deprecated: bool,
    /// The code also reads the `EME_` twin of this alias.
    pub eme: bool,
}

impl Alias {
    /// A deprecated alias (warns when used).
    pub const fn deprecated(name: &'static str) -> Alias {
        Alias {
            name,
            deprecated: true,
            eme: false,
        }
    }

    /// A supported shared spelling (for example `DASH_ANN_*` shared by both
    /// services), also read under its `EME_` twin.
    pub const fn shared(name: &'static str) -> Alias {
        Alias {
            name,
            deprecated: false,
            eme: true,
        }
    }
}

/// One row of the registry. `name` may contain `{SVC}`, which expands to
/// `INGEST` and `RETRIEVAL` (a per-service pattern).
#[derive(Debug, Clone, Copy)]
pub struct Entry {
    pub name: &'static str,
    pub scope: Scope,
    /// Docs sub-heading the entry is listed under.
    pub topic: &'static str,
    pub kind: Kind,
    /// Default as text; empty means "unset".
    pub default: &'static str,
    /// Per-service defaults for `{SVC}` patterns: (ingestion, retrieval).
    pub svc_default: Option<(&'static str, &'static str)>,
    pub description: &'static str,
    pub notes: &'static str,
    /// The code also reads the legacy `EME_` twin of `name`.
    pub eme: bool,
    pub aliases: &'static [Alias],
    /// Documented but not implemented yet: exempt from "read by code".
    pub planned: bool,
    /// Read outside the Rust code (shell scripts, compose) or only set by DASH
    /// for child processes: exempt from "read by code".
    pub external: bool,
    /// Which processes read the setting (reader bit flags).
    pub readers: u8,
    pub blank_ok: bool,
}

impl Entry {
    pub const fn new(
        name: &'static str,
        scope: Scope,
        topic: &'static str,
        kind: Kind,
        default: &'static str,
        description: &'static str,
    ) -> Entry {
        let readers = match scope {
            Scope::Ingestion => INGEST,
            Scope::Retrieval => RETRIEVAL,
            Scope::ControlPlane => CONTROL,
            Scope::Common => DATA,
            Scope::Tools => TOOLS,
        };
        Entry {
            name,
            scope,
            topic,
            kind,
            default,
            svc_default: None,
            description,
            notes: "",
            eme: false,
            aliases: &[],
            planned: false,
            external: false,
            readers,
            blank_ok: kind.blank_default(),
        }
    }

    /// The code also reads the `EME_` twin.
    pub const fn eme(self) -> Entry {
        Entry { eme: true, ..self }
    }

    pub const fn notes(self, notes: &'static str) -> Entry {
        Entry { notes, ..self }
    }

    pub const fn aliases(self, aliases: &'static [Alias]) -> Entry {
        Entry { aliases, ..self }
    }

    pub const fn readers(self, readers: u8) -> Entry {
        Entry { readers, ..self }
    }

    pub const fn svc_default(self, ingestion: &'static str, retrieval: &'static str) -> Entry {
        Entry {
            svc_default: Some((ingestion, retrieval)),
            ..self
        }
    }

    pub const fn blank_ok(self) -> Entry {
        Entry {
            blank_ok: true,
            ..self
        }
    }

    pub const fn reject_blank(self) -> Entry {
        Entry {
            blank_ok: false,
            ..self
        }
    }

    pub const fn planned(self) -> Entry {
        Entry {
            planned: true,
            ..self
        }
    }

    pub const fn external(self) -> Entry {
        Entry {
            external: true,
            ..self
        }
    }

    /// True for `{SVC}` patterns.
    pub fn is_pattern(&self) -> bool {
        self.name.contains("{SVC}")
    }
}

/// `DASH_X` to `EME_X`.
pub fn eme_twin(name: &str) -> Option<String> {
    name.strip_prefix("DASH_").map(|rest| format!("EME_{rest}"))
}

/// A concrete setting: a registry entry with `{SVC}` expanded.
#[derive(Debug, Clone)]
pub struct Setting {
    pub entry: &'static Entry,
    /// Canonical `DASH_*` name.
    pub name: String,
    pub scope: Scope,
    pub readers: u8,
    pub default: &'static str,
    /// Supported shared spellings (`DASH_ANN_*`), not deprecated.
    pub shared: Vec<String>,
    /// Deprecated spellings other than `EME_` (for example
    /// `DASH_OLLAMA_BASE_URL`).
    pub deprecated: Vec<String>,
    /// Every `EME_` spelling the code reads for this setting.
    pub eme: Vec<String>,
}

impl Setting {
    pub fn kind(&self) -> Kind {
        self.entry.kind
    }

    /// Canonical name, shared names, deprecated aliases and `EME_` names in
    /// reader precedence order (DASH names first).
    pub fn all_names(&self) -> Vec<&str> {
        let mut out: Vec<&str> = vec![self.name.as_str()];
        out.extend(self.shared.iter().map(String::as_str));
        out.extend(self.deprecated.iter().map(String::as_str));
        out.extend(self.eme.iter().map(String::as_str));
        out
    }

    /// Is `name` a deprecated spelling (an `EME_` name or a deprecated alias)?
    pub fn is_deprecated_name(&self, name: &str) -> bool {
        self.eme.iter().any(|n| n == name) || self.deprecated.iter().any(|n| n == name)
    }

    /// Key of this setting in the TOML file overlay table of its scope.
    pub fn file_key(&self) -> String {
        let prefix = self.scope.key_prefix();
        let rest = self
            .name
            .strip_prefix(prefix)
            .or_else(|| self.name.strip_prefix("DASH_"))
            .unwrap_or(&self.name);
        rest.to_ascii_lowercase()
    }

    /// Does the service read this setting?
    pub fn read_by(&self, service: Service) -> bool {
        self.readers & service.reader_bit() != 0
    }
}

fn expand(entry: &'static Entry, infix: Option<&'static str>, default: &'static str) -> Setting {
    let sub = |s: &str| match infix {
        Some(i) => s.replace("{SVC}", i),
        None => s.to_string(),
    };
    let name = sub(entry.name);
    let mut eme = Vec::new();
    if entry.eme
        && let Some(t) = eme_twin(&name)
    {
        eme.push(t);
    }
    let mut shared = Vec::new();
    let mut deprecated = Vec::new();
    for alias in entry.aliases {
        let alias_name = sub(alias.name);
        if alias.eme
            && let Some(t) = eme_twin(&alias_name)
        {
            eme.push(t);
        }
        if alias.deprecated {
            deprecated.push(alias_name);
        } else {
            shared.push(alias_name);
        }
    }
    let (scope, readers) = match infix {
        Some("INGEST") => (Scope::Ingestion, entry.readers & INGEST),
        Some("RETRIEVAL") => (Scope::Retrieval, entry.readers & RETRIEVAL),
        _ => (entry.scope, entry.readers),
    };
    Setting {
        entry,
        name,
        scope,
        readers,
        default,
        shared,
        deprecated,
        eme,
    }
}

/// Every concrete setting (patterns expanded), in registry order.
pub fn settings() -> &'static [Setting] {
    static CELL: OnceLock<Vec<Setting>> = OnceLock::new();
    CELL.get_or_init(|| {
        let mut out = Vec::new();
        for entry in crate::registry::REGISTRY {
            if entry.is_pattern() {
                for (infix, svc) in [("INGEST", 0usize), ("RETRIEVAL", 1)] {
                    let default = match entry.svc_default {
                        Some((i, r)) => {
                            if svc == 0 {
                                i
                            } else {
                                r
                            }
                        }
                        None => entry.default,
                    };
                    if entry.readers & (if svc == 0 { INGEST } else { RETRIEVAL }) != 0 {
                        out.push(expand(entry, Some(infix), default));
                    }
                }
            } else {
                out.push(expand(entry, None, entry.default));
            }
        }
        out
    })
}

/// Index from every known spelling (canonical, shared, deprecated, `EME_`)
/// to the position in [`settings`].
fn name_index() -> &'static HashMap<String, usize> {
    static CELL: OnceLock<HashMap<String, usize>> = OnceLock::new();
    CELL.get_or_init(|| {
        let mut map = HashMap::new();
        for (i, s) in settings().iter().enumerate() {
            for n in s.all_names() {
                map.insert(n.to_string(), i);
            }
        }
        map
    })
}

/// Look a setting up by any of its spellings.
pub fn lookup(name: &str) -> Option<&'static Setting> {
    name_index().get(name).map(|&i| &settings()[i])
}

/// Every known spelling of every setting.
pub fn all_names() -> Vec<&'static str> {
    let mut v: Vec<&'static str> = settings().iter().flat_map(|s| s.all_names()).collect();
    v.sort_unstable();
    v.dedup();
    v
}

/// Canonical names only.
pub fn canonical_names() -> Vec<&'static str> {
    settings().iter().map(|s| s.name.as_str()).collect()
}

/// Resolve a TOML overlay key within a table to a setting.
///
/// A key is looked up among the settings of the table's own scope; the
/// `[ingestion]`, `[retrieval]` and `[control_plane]` tables also accept the
/// keys of `[common]`, so a service-specific table can set a shared variable.
pub fn lookup_file_key(scope: Scope, key: &str) -> Option<&'static Setting> {
    let find = |sc: Scope| {
        settings()
            .iter()
            .find(|s| s.scope == sc && s.file_key() == key)
    };
    find(scope).or_else(|| match scope {
        Scope::Ingestion | Scope::Retrieval | Scope::ControlPlane => find(Scope::Common),
        _ => None,
    })
}

/// File keys that are valid in a table (for did-you-mean suggestions).
pub fn file_keys(scope: Scope) -> Vec<String> {
    let mut keys: Vec<String> = settings()
        .iter()
        .filter(|s| {
            s.scope == scope
                || (matches!(
                    scope,
                    Scope::Ingestion | Scope::Retrieval | Scope::ControlPlane
                ) && s.scope == Scope::Common)
        })
        .filter(|s| !s.entry.external && !s.entry.planned)
        .map(|s| s.file_key())
        .collect();
    keys.sort();
    keys.dedup();
    keys
}
