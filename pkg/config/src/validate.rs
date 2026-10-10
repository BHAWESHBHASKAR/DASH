//! Startup validation of environment settings against the registry.
//!
//! The validator is deliberately conservative: it only reports an *error*
//! for a value that no reader of the setting could interpret as intended
//! (non-numeric where a number is expected, out of range, unknown enum word,
//! unparsable boolean, a blank value where blank is meaningless). Values a
//! reader accepts but does not honor (for example `true` for a switch whose
//! reader only enables on the literal `1`) are *warnings*.
//!
//! Secret values are never echoed in messages.

use std::collections::{BTreeMap, HashMap};

use crate::model::{Honors, Kind, Service, Setting, all_names, lookup, settings};

/// Where the validator reads values from. Implemented for maps (tests) and
/// for the process environment.
pub trait EnvSource {
    /// Value of `name`, if set.
    fn get(&self, name: &str) -> Option<String>;
    /// Names of every variable that is set.
    fn names(&self) -> Vec<String>;
}

impl EnvSource for HashMap<String, String> {
    fn get(&self, name: &str) -> Option<String> {
        HashMap::get(self, name).cloned()
    }

    fn names(&self) -> Vec<String> {
        self.keys().cloned().collect()
    }
}

impl EnvSource for BTreeMap<String, String> {
    fn get(&self, name: &str) -> Option<String> {
        BTreeMap::get(self, name).cloned()
    }

    fn names(&self) -> Vec<String> {
        self.keys().cloned().collect()
    }
}

/// The real process environment (non-unicode values read as unset, exactly
/// like the service readers).
pub struct ProcessEnv;

impl EnvSource for ProcessEnv {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }

    fn names(&self) -> Vec<String> {
        std::env::vars_os()
            .filter_map(|(k, _)| k.into_string().ok())
            .collect()
    }
}

/// One finding: the variable (or file key) and a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub name: String,
    pub message: String,
}

impl std::fmt::Display for Issue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.message)
    }
}

/// Result of validating an environment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    pub errors: Vec<Issue>,
    pub warnings: Vec<Issue>,
}

impl Report {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
    }

    pub fn error(&mut self, name: &str, message: impl Into<String>) {
        self.errors.push(Issue {
            name: name.to_string(),
            message: message.into(),
        });
    }

    pub fn warn(&mut self, name: &str, message: impl Into<String>) {
        self.warnings.push(Issue {
            name: name.to_string(),
            message: message.into(),
        });
    }

    pub fn merge(&mut self, other: Report) {
        self.errors.extend(other.errors);
        self.warnings.extend(other.warnings);
    }

    /// Turn every error into a warning (`DASH_CONFIG_VALIDATION=warn`).
    pub fn downgrade_errors(&mut self) {
        let errors = std::mem::take(&mut self.errors);
        for mut issue in errors {
            issue.message = format!(
                "{} (would be an error; downgraded by DASH_CONFIG_VALIDATION=warn)",
                issue.message
            );
            self.warnings.push(issue);
        }
    }
}

const TRUE_WORDS: [&str; 4] = ["1", "true", "yes", "on"];
const FALSE_WORDS: [&str; 4] = ["0", "false", "no", "off"];

/// Validate the environment as seen by `service`.
pub fn validate_env(service: Service, env: &dyn EnvSource) -> Report {
    let mut report = Report::default();

    for setting in settings().iter().filter(|s| s.read_by(service)) {
        for name in setting.all_names() {
            let Some(value) = env.get(name) else {
                continue;
            };
            if setting.is_deprecated_name(name) {
                report.warn(name, deprecation_message(setting, name));
            }
            // Settings read outside the Rust services (container scripts,
            // compose) are known names, but their values are not ours to judge.
            if !setting.entry.external {
                check_value(setting, name, &value, &mut report);
            }
        }
    }

    for name in sorted(env.names()) {
        if !(name.starts_with("DASH_") || name.starts_with("EME_")) {
            continue;
        }
        if lookup(&name).is_some() || is_platform_noise(&name, env) {
            continue;
        }
        let hint = match suggest(&name) {
            Some(s) => format!("unknown variable, ignored; did you mean {s}?"),
            None => "unknown variable, ignored".to_string(),
        };
        report.warn(&name, hint);
    }
    report
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

fn deprecation_message(setting: &Setting, used: &str) -> String {
    if used.starts_with("EME_") {
        format!(
            "the EME_ prefix is deprecated and will be removed; use {}",
            setting.name
        )
    } else {
        format!("deprecated alias; use {} instead", setting.name)
    }
}

/// Environment variables injected by container platforms that happen to
/// start with `DASH_` (Kubernetes service links such as
/// `DASH_INGESTION_SERVICE_HOST` or `DASH_INGESTION_PORT_8081_TCP`).
pub fn is_platform_noise(name: &str, env: &dyn EnvSource) -> bool {
    if name.ends_with("_SERVICE_HOST")
        || name.ends_with("_SERVICE_PORT")
        || name.contains("_SERVICE_PORT_")
    {
        return true;
    }
    if let Some(idx) = name.find("_PORT_") {
        let rest = &name[idx + "_PORT_".len()..];
        let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
        if !digits.is_empty() {
            let tail = &rest[digits.len()..];
            if tail.starts_with("_TCP") || tail.starts_with("_UDP") {
                return true;
            }
        }
    }
    if name.ends_with("_PORT") {
        return env
            .get(name)
            .is_some_and(|v| v.starts_with("tcp://") || v.starts_with("udp://"));
    }
    false
}

fn check_value(setting: &Setting, name: &str, raw: &str, report: &mut Report) {
    let trimmed = raw.trim();
    let kind = setting.kind();
    let echo = if kind.is_secret() {
        String::new()
    } else {
        format!(" (got '{}')", shorten(trimmed))
    };

    if trimmed.is_empty() {
        if !setting.entry.blank_ok {
            report.error(
                name,
                "is set but empty; unset it or give it a value (a blank value is not meaningful here)",
            );
        }
        return;
    }

    match kind {
        Kind::Bool(honors) => check_bool(name, trimmed, honors, &echo, report),
        Kind::Int { min, max } => check_int(name, trimmed, min, max, "an integer", &echo, report),
        Kind::Millis { min, max } => check_int(
            name,
            trimmed,
            min,
            max,
            "an integer number of milliseconds",
            &echo,
            report,
        ),
        Kind::Float { min, max } => match trimmed.parse::<f64>() {
            Ok(v) if v.is_finite() => {
                if v < min || v > max {
                    report.error(
                        name,
                        format!("out of range{echo}; expected {}", range_text_f64(min, max)),
                    );
                }
            }
            _ => report.error(name, format!("must be a finite number{echo}")),
        },
        Kind::Str | Kind::Path | Kind::Secret | Kind::SecretList { .. } => {}
        Kind::Addr => {
            if let Err(why) = check_addr(trimmed) {
                report.error(name, format!("{why}{echo}"));
            }
        }
        Kind::Url => {
            if let Err(why) = check_url(trimmed) {
                report.error(name, format!("{why}{echo}"));
            }
        }
        Kind::List { sep, u32_items } => {
            if u32_items {
                for item in trimmed.split(sep).map(str::trim).filter(|i| !i.is_empty()) {
                    if item.parse::<u32>().is_err() {
                        report.error(
                            name,
                            format!(
                                "every item must be an unsigned integer separated by '{sep}'{echo}"
                            ),
                        );
                        break;
                    }
                }
            }
        }
        Kind::Enum { values } => {
            let lower = trimmed.to_ascii_lowercase();
            if !values.iter().any(|v| *v == lower) {
                report.error(
                    name,
                    format!("expected one of: {}{echo}", values.join(", ")),
                );
            }
        }
        Kind::IntOrWords { words, min } => {
            let lower = trimmed.to_ascii_lowercase();
            if words.iter().any(|w| *w == lower) {
                return;
            }
            match trimmed.parse::<u64>() {
                Ok(v) if v >= min => {}
                _ => report.error(
                    name,
                    format!(
                        "expected an integer >= {min} or one of: {}{echo}",
                        words.join(", ")
                    ),
                ),
            }
        }
    }
}

fn check_bool(name: &str, trimmed: &str, honors: Honors, echo: &str, report: &mut Report) {
    let lower = trimmed.to_ascii_lowercase();
    let truthy = TRUE_WORDS.contains(&lower.as_str());
    let falsy = FALSE_WORDS.contains(&lower.as_str());
    if !truthy && !falsy {
        report.error(
            name,
            format!("expected a boolean (1, true, yes, on, 0, false, no, off){echo}"),
        );
        return;
    }
    if truthy && !honors.enables(trimmed) {
        report.warn(
            name,
            format!(
                "'{trimmed}' does not enable this setting: its reader honors {}; it will be treated as off",
                honors.describe()
            ),
        );
    }
}

fn check_int(
    name: &str,
    trimmed: &str,
    min: u64,
    max: u64,
    what: &str,
    echo: &str,
    report: &mut Report,
) {
    match trimmed.parse::<u64>() {
        Ok(v) => {
            if v < min || v > max {
                report.error(
                    name,
                    format!("out of range{echo}; expected {}", range_text_u64(min, max)),
                );
            }
        }
        Err(_) => report.error(name, format!("must be {what}{echo}")),
    }
}

fn range_text_u64(min: u64, max: u64) -> String {
    match (min > 0, max < u64::MAX) {
        (true, true) => format!("{min}..={max}"),
        (true, false) => format!(">= {min}"),
        (false, true) => format!("<= {max}"),
        (false, false) => "a non-negative integer".to_string(),
    }
}

fn range_text_f64(min: f64, max: f64) -> String {
    match (min > f64::MIN, max < f64::MAX) {
        (true, true) => format!("{min}..={max}"),
        (true, false) => format!(">= {min}"),
        (false, true) => format!("<= {max}"),
        (false, false) => "a finite number".to_string(),
    }
}

fn check_addr(value: &str) -> Result<(), String> {
    if value.chars().any(char::is_whitespace) {
        return Err("must be host:port without spaces".to_string());
    }
    let Some((host, port)) = value.rsplit_once(':') else {
        return Err("must be host:port (a bare port is not accepted)".to_string());
    };
    let host = host.trim_start_matches('[').trim_end_matches(']');
    if host.is_empty() {
        return Err("must be host:port (the host is empty)".to_string());
    }
    if port.parse::<u16>().is_err() {
        return Err("must be host:port with a port in 0..=65535".to_string());
    }
    Ok(())
}

fn check_url(value: &str) -> Result<(), String> {
    let lower = value.to_ascii_lowercase();
    let rest = lower
        .strip_prefix("http://")
        .or_else(|| lower.strip_prefix("https://"));
    let Some(rest) = rest else {
        return Err("must be an http:// or https:// URL".to_string());
    };
    if value.chars().any(char::is_whitespace) {
        return Err("must not contain whitespace".to_string());
    }
    let host = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host.is_empty() {
        return Err("must name a host".to_string());
    }
    Ok(())
}

fn shorten(value: &str) -> String {
    let cleaned: String = value
        .chars()
        .map(|c| if c.is_control() { '?' } else { c })
        .collect();
    if cleaned.chars().count() > 48 {
        let head: String = cleaned.chars().take(45).collect();
        format!("{head}...")
    } else {
        cleaned
    }
}

/// Optimal-string-alignment edit distance (insert, delete, substitute and
/// adjacent transposition each cost one).
pub fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<char> = a.chars().collect();
    let b: Vec<char> = b.chars().collect();
    let mut d = vec![vec![0usize; b.len() + 1]; a.len() + 1];
    for (i, row) in d.iter_mut().enumerate() {
        row[0] = i;
    }
    for (j, cell) in d[0].iter_mut().enumerate() {
        *cell = j;
    }
    for i in 1..=a.len() {
        for j in 1..=b.len() {
            let cost = usize::from(a[i - 1] != b[j - 1]);
            let mut best = (d[i - 1][j] + 1)
                .min(d[i][j - 1] + 1)
                .min(d[i - 1][j - 1] + cost);
            if i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1] {
                best = best.min(d[i - 2][j - 2] + 1);
            }
            d[i][j] = best;
        }
    }
    d[a.len()][b.len()]
}

/// The closest candidate within a sensible distance, if any.
pub fn did_you_mean<'a>(
    input: &str,
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    did_you_mean_within(input, candidates, (input.chars().count() / 8).clamp(1, 3))
}

/// Like [`did_you_mean`] with an explicit edit-distance limit.
pub fn did_you_mean_within<'a>(
    input: &str,
    candidates: impl IntoIterator<Item = &'a str>,
    limit: usize,
) -> Option<&'a str> {
    candidates
        .into_iter()
        .map(|c| (edit_distance(input, c), c))
        .filter(|(d, _)| *d <= limit)
        .min_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(b.1)))
        .map(|(_, c)| c)
}

/// Suggest a known variable name for an unknown `DASH_*` / `EME_*` name.
fn suggest(name: &str) -> Option<String> {
    let names = all_names();
    if let Some(hit) = did_you_mean(name, names.iter().copied()) {
        return Some(hit.to_string());
    }
    // A misspelled EME_ name: compare its DASH_ form and suggest the DASH_ name.
    if let Some(rest) = name.strip_prefix("EME_") {
        let as_dash = format!("DASH_{rest}");
        if let Some(hit) = did_you_mean(&as_dash, names.iter().copied()) {
            return Some(hit.to_string());
        }
    }
    None
}
