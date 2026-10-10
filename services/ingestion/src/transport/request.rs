//! Request-target helpers. Parsing and percent-decoding live in `dash-http`.

use std::collections::HashMap;

pub(super) use dash_http::{query_encoding_is_invalid, split_target};

pub(super) fn parse_query_usize(
    query: &HashMap<String, String>,
    key: &str,
) -> Result<Option<usize>, String> {
    match query.get(key) {
        None => Ok(None),
        Some(value) => value
            .parse::<usize>()
            .map(Some)
            .map_err(|_| format!("query parameter '{key}' must be a positive integer")),
    }
}
