// Minimal JSON helpers shared across the transport module. The structured
// request/response paths in `payload.rs` use `serde_json` directly; these
// helpers cover the small set of cases that still emit JSON by hand
// (`document_parser_debug.rs`, `http.rs`, `audit.rs`).
//
// `JsonValue` and `parse_json` are intentionally `pub(super)` so the test
// suite in `transport/tests.rs` can use them to assert on audit log
// records, but they're only invoked from `#[cfg(test)]` code paths, so
// the `dead_code` lint fires under clippy when the lib test cfg is
// disabled. The `#[allow(dead_code)]` attribute on each item silences
// that without changing the public API.

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(super) enum JsonValue {
    Object(HashMap<String, JsonValue>),
    Array(Vec<JsonValue>),
    String(String),
    Number(String),
    Bool(bool),
    Null,
}

#[allow(dead_code)]
pub(super) fn parse_json(input: &str) -> Result<JsonValue, String> {
    let value: serde_json::Value = serde_json::from_str(input).map_err(|err| err.to_string())?;
    Ok(convert(value))
}

#[allow(dead_code)]
fn convert(value: serde_json::Value) -> JsonValue {
    match value {
        serde_json::Value::Null => JsonValue::Null,
        serde_json::Value::Bool(b) => JsonValue::Bool(b),
        serde_json::Value::Number(n) => JsonValue::Number(n.to_string()),
        serde_json::Value::String(s) => JsonValue::String(s),
        serde_json::Value::Array(arr) => JsonValue::Array(arr.into_iter().map(convert).collect()),
        serde_json::Value::Object(obj) => {
            JsonValue::Object(obj.into_iter().map(|(k, v)| (k, convert(v))).collect())
        }
    }
}

pub(super) fn json_escape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            _ => out.push(ch),
        }
    }
    out
}
