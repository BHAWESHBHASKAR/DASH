//! Keeps the settings registry in step with the code.
//!
//! The test walks the workspace sources, extracts every `"DASH_*"` / `"EME_*"`
//! string literal (ignoring comments, `#[cfg(test)]` items, test modules and
//! test files) plus the per-service names that are built at runtime, and
//! compares them with the registry in both directions.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use dash_config::{all_names, lookup, settings};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

// ---------------------------------------------------------------------------
// A small Rust lexer: string literals outside comments and cfg(test) items.
// ---------------------------------------------------------------------------

/// A string literal and the (whitespace-free) text right before it.
#[derive(Debug, PartialEq, Eq)]
struct Literal {
    text: String,
    before: String,
}

fn is_ident(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Skip a `// ...` comment, a `/* ... */` comment, a string, a raw string or a
/// char literal starting at `i`; returns the index after it, if one starts here.
fn skip_trivia(chars: &[char], i: usize) -> Option<usize> {
    let n = chars.len();
    match chars[i] {
        '/' if i + 1 < n && chars[i + 1] == '/' => {
            let mut j = i;
            while j < n && chars[j] != '\n' {
                j += 1;
            }
            Some(j)
        }
        '/' if i + 1 < n && chars[i + 1] == '*' => {
            let mut j = i + 2;
            let mut depth = 1;
            while j < n && depth > 0 {
                if chars[j] == '/' && j + 1 < n && chars[j + 1] == '*' {
                    depth += 1;
                    j += 2;
                } else if chars[j] == '*' && j + 1 < n && chars[j + 1] == '/' {
                    depth -= 1;
                    j += 2;
                } else {
                    j += 1;
                }
            }
            Some(j)
        }
        _ => None,
    }
}

/// If a (raw) string literal starts at `i`, return (content, index after).
fn read_string(chars: &[char], i: usize) -> Option<(String, usize)> {
    let n = chars.len();
    let prev_is_ident = i > 0 && is_ident(chars[i - 1]);
    // Raw string: r"..." or r#"..."#, optionally with a b prefix.
    if chars[i] == 'r' && !prev_is_ident {
        let mut j = i + 1;
        let mut hashes = 0;
        while j < n && chars[j] == '#' {
            hashes += 1;
            j += 1;
        }
        if j < n && chars[j] == '"' {
            j += 1;
            let start = j;
            while j < n {
                if chars[j] == '"' && (0..hashes).all(|k| j + 1 + k < n && chars[j + 1 + k] == '#')
                {
                    let content: String = chars[start..j].iter().collect();
                    return Some((content, j + 1 + hashes));
                }
                j += 1;
            }
            return Some((chars[start..].iter().collect(), n));
        }
        return None;
    }
    if chars[i] == '"' {
        let mut j = i + 1;
        let mut content = String::new();
        while j < n && chars[j] != '"' {
            if chars[j] == '\\' && j + 1 < n {
                content.push(chars[j]);
                content.push(chars[j + 1]);
                j += 2;
            } else {
                content.push(chars[j]);
                j += 1;
            }
        }
        return Some((content, (j + 1).min(n)));
    }
    None
}

fn skip_char_literal(chars: &[char], i: usize) -> Option<usize> {
    // 'x' or '\n' or '\u{..}' (but not a lifetime like 'a).
    let n = chars.len();
    if chars[i] != '\'' {
        return None;
    }
    if i + 2 < n && chars[i + 1] != '\\' && chars[i + 2] == '\'' {
        return Some(i + 3);
    }
    if i + 1 < n && chars[i + 1] == '\\' {
        let mut j = i + 2;
        while j < n && chars[j] != '\'' && j < i + 12 {
            j += 1;
        }
        if j < n && chars[j] == '\'' {
            return Some(j + 1);
        }
    }
    None
}

/// Skip the item that follows a `#[cfg(test)]` attribute: everything up to
/// the end of its braces (or its terminating `;`).
fn skip_item(chars: &[char], mut i: usize) -> usize {
    let n = chars.len();
    let mut depth = 0usize;
    let mut started = false;
    while i < n {
        if let Some(next) = skip_trivia(chars, i) {
            i = next;
            continue;
        }
        if let Some((_, next)) = read_string(chars, i) {
            i = next;
            continue;
        }
        if let Some(next) = skip_char_literal(chars, i) {
            i = next;
            continue;
        }
        match chars[i] {
            '{' => {
                depth += 1;
                started = true;
            }
            '}' => {
                depth = depth.saturating_sub(1);
                if started && depth == 0 {
                    return i + 1;
                }
            }
            ';' if depth == 0 && !started => return i + 1,
            _ => {}
        }
        i += 1;
    }
    n
}

fn string_literals(src: &str) -> Vec<Literal> {
    let chars: Vec<char> = src.chars().collect();
    let n = chars.len();
    let marker: Vec<char> = "#[cfg(test)]".chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i < n {
        if let Some(next) = skip_trivia(&chars, i) {
            i = next;
            continue;
        }
        if chars[i] == '#' && chars[i..].starts_with(&marker) {
            i = skip_item(&chars, i + marker.len());
            continue;
        }
        if let Some((text, next)) = read_string(&chars, i) {
            let from = i.saturating_sub(80);
            let before: String = chars[from..i]
                .iter()
                .filter(|c| !c.is_whitespace())
                .collect();
            out.push(Literal { text, before });
            i = next;
            continue;
        }
        if let Some(next) = skip_char_literal(&chars, i) {
            i = next;
            continue;
        }
        i += 1;
    }
    out
}

// ---------------------------------------------------------------------------
// Walking the workspace
// ---------------------------------------------------------------------------

fn source_files(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    for top in ["services", "pkg", "tools", "tests/benchmarks"] {
        collect(&root.join(top), &root.join(top), &mut files);
    }
    files.sort();
    files
}

fn collect(base: &Path, dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(read) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in read.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if path.is_dir() {
            if name == "target" || name == "tests" || name == ".git" {
                continue;
            }
            collect(base, &path, out);
        } else if is_scanned_file(base, &path) {
            out.push(path);
        }
    }
}

fn is_scanned_file(base: &Path, path: &Path) -> bool {
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
    if !name.ends_with(".rs") || name.ends_with("tests.rs") {
        return false;
    }
    let rel = path.strip_prefix(base).unwrap_or(path);
    // Only crate sources.
    let mut comps = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned());
    if !comps.any(|c| c == "src") {
        return false;
    }
    // The registry crate defines names rather than reading them; its one
    // reading file is `startup.rs` (DASH_CONFIG_FILE, DASH_CONFIG_VALIDATION).
    if path.to_string_lossy().contains("/pkg/config/") {
        return name == "startup.rs";
    }
    true
}

fn is_env_name(text: &str) -> bool {
    (text.starts_with("DASH_") || text.starts_with("EME_"))
        && text.len() > 5
        && text
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// Names read by the code: literal names plus per-service names built at
/// runtime (the shared auth policy and the audit options).
fn names_read_by_code() -> BTreeMap<String, BTreeSet<String>> {
    let root = workspace_root();
    let mut names: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut add = |name: String, file: &str| {
        names.entry(name).or_default().insert(file.to_string());
    };
    for file in source_files(&root) {
        let rel = file
            .strip_prefix(&root)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let src = std::fs::read_to_string(&file).expect("read source");
        for lit in string_literals(&src) {
            if is_env_name(&lit.text) {
                add(lit.text.clone(), &rel);
                continue;
            }
            // Suffix passed to the per-service helpers of the auth policy:
            // `svc.var(lookup, "SUFFIX")` reads DASH_ and EME_, `svc.dash("SUFFIX")` DASH_.
            let suffix_ok = !lit.text.is_empty()
                && lit
                    .text
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_');
            if suffix_ok && rel.ends_with("services/common/src/policy.rs") {
                let reads_both = lit.before.ends_with(".var(lookup,");
                let reads_dash = lit.before.ends_with(".dash(");
                if reads_both || reads_dash {
                    for infix in ["INGEST", "RETRIEVAL"] {
                        add(format!("DASH_{infix}_{}", lit.text), &rel);
                        if reads_both {
                            add(format!("EME_{infix}_{}", lit.text), &rel);
                        }
                    }
                }
            }
            // Names formatted with the service prefix: "DASH_{prefix}_AUDIT_FSYNC".
            for lead in ["DASH_", "EME_"] {
                if let Some(rest) = lit.text.strip_prefix(lead)
                    && let Some(suffix) = rest.strip_prefix("{prefix}_")
                    && !suffix.is_empty()
                    && suffix
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                {
                    for infix in ["INGEST", "RETRIEVAL"] {
                        add(format!("{lead}{infix}_{suffix}"), &rel);
                    }
                }
            }
        }
    }
    names
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn registry_covers_every_env_var_read_by_code() {
    let read = names_read_by_code();
    assert!(
        read.len() > 200,
        "scanner found only {} names; the source walk is broken",
        read.len()
    );

    // Direction 1: every name the code reads must be in the registry.
    let known: BTreeSet<&str> = all_names().into_iter().collect();
    let missing: Vec<String> = read
        .iter()
        .filter(|(name, _)| !known.contains(name.as_str()))
        .map(|(name, files)| {
            format!(
                "  {name} (read in {})",
                files.iter().cloned().collect::<Vec<_>>().join(", ")
            )
        })
        .collect();

    // Direction 2: every registry entry (not planned, not external) must be
    // read by the code, including the aliases it claims.
    let mut unread: Vec<String> = Vec::new();
    for setting in settings() {
        if setting.entry.planned || setting.entry.external {
            continue;
        }
        for name in setting.all_names() {
            if !read.contains_key(name) {
                unread.push(format!(
                    "  {name} (listed for {} but no code reads it)",
                    setting.name
                ));
            }
        }
    }

    let mut report = String::new();
    if !missing.is_empty() {
        report.push_str(&format!(
            "{} name(s) read by the code but missing from pkg/config/src/registry.rs:\n{}\n",
            missing.len(),
            missing.join("\n")
        ));
    }
    if !unread.is_empty() {
        report.push_str(&format!(
            "{} registry name(s) that no code reads (remove the row, mark it planned/external, or fix its eme/alias flags):\n{}\n",
            unread.len(),
            unread.join("\n")
        ));
    }
    assert!(report.is_empty(), "\n{report}");
}

#[test]
fn registry_has_no_duplicate_names_or_file_keys() {
    let mut names = BTreeSet::new();
    // Shared spellings (`DASH_ANN_*`) intentionally belong to both services
    // of a per-service pattern.
    let shared: BTreeSet<String> = settings()
        .iter()
        .flat_map(|s| {
            s.shared
                .iter()
                .flat_map(|n| [n.clone(), dash_config::eme_twin(n).unwrap()])
        })
        .collect();
    for setting in settings() {
        for name in setting.all_names() {
            if shared.contains(name) {
                continue;
            }
            assert!(
                names.insert(name.to_string()),
                "{name} is listed twice in the registry"
            );
        }
    }
    let mut keys = BTreeSet::new();
    for setting in settings() {
        if setting.entry.planned || setting.entry.external {
            continue;
        }
        let key = (setting.scope, setting.file_key());
        assert!(
            keys.insert(key.clone()),
            "file key {:?} is ambiguous within [{}]",
            key.1,
            key.0.table()
        );
    }
}

#[test]
fn every_registry_name_is_canonical_dash_form() {
    for setting in settings() {
        assert!(
            setting.name.starts_with("DASH_") || setting.entry.external,
            "{} must use the DASH_ prefix",
            setting.name
        );
        assert!(lookup(&setting.name).is_some());
    }
}

#[test]
fn scanner_ignores_comments_test_modules_and_non_env_strings() {
    let src = r##"
        // "DASH_IN_COMMENT"
        /* "DASH_IN_BLOCK" */
        const A: &str = "DASH_REAL_ONE";
        const B: &str = r#"DASH_RAW"#;
        fn f() { let _c = '"'; let _d = "DASH_AFTER_CHAR"; }
        #[cfg(test)]
        mod tests {
            const T: &str = "DASH_ONLY_IN_TESTS";
            fn g() { let _ = "}"; }
        }
        #[cfg(test)]
        use std::fmt;
        const C: &str = "DASH_AFTER_TESTS";
    "##;
    let found: Vec<String> = string_literals(src)
        .into_iter()
        .map(|l| l.text)
        .filter(|t| is_env_name(t))
        .collect();
    assert_eq!(
        found,
        vec![
            "DASH_REAL_ONE",
            "DASH_RAW",
            "DASH_AFTER_CHAR",
            "DASH_AFTER_TESTS"
        ]
    );
}

#[test]
fn scanner_finds_the_runtime_built_auth_and_audit_names() {
    let read = names_read_by_code();
    for name in [
        "DASH_INGEST_API_KEY",
        "EME_RETRIEVAL_API_KEY",
        "DASH_RETRIEVAL_JWT_ROLE_CLAIM",
        "DASH_INGEST_AUDIT_FSYNC",
        "DASH_RETRIEVAL_AUDIT_FAIL_CLOSED",
        "DASH_CONFIG_FILE",
        "DASH_CONFIG_VALIDATION",
    ] {
        assert!(
            read.contains_key(name),
            "{name} should be found by the scanner"
        );
    }
}
