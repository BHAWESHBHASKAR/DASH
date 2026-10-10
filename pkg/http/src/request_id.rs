//! Request correlation ids.
//!
//! Every request handled by [`crate::serve`] carries an `X-Request-Id`. A
//! well-formed id sent by the client (or a proxy in front of the service) is
//! kept; a missing or malformed one is replaced by a freshly generated id.
//! The id is visible to the handler as the `x-request-id` request header,
//! echoed in the `X-Request-Id` response header, and added as a
//! `"request_id"` field to JSON error bodies, so a client report, a log line
//! and an audit record can be joined.

use std::collections::HashMap;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::response::Response;

/// Lower-case request header name (the parser lower-cases header names).
pub const REQUEST_ID_HEADER: &str = "x-request-id";
/// Response header name.
pub const REQUEST_ID_RESPONSE_HEADER: &str = "X-Request-Id";
/// Longest accepted client-supplied id.
pub const MAX_REQUEST_ID_LEN: usize = 128;

/// A client-supplied id is accepted when it is 1..=128 bytes of ASCII
/// letters, digits and `-_.:/+=@`. Anything else (spaces, quotes, control
/// characters, non-ASCII) is replaced, so the id is always safe to place in
/// a log line, a JSON string and a header without escaping.
pub fn is_valid_request_id(raw: &str) -> bool {
    !raw.is_empty()
        && raw.len() <= MAX_REQUEST_ID_LEN
        && raw
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:/+=@".contains(&b))
}

/// A new 128-bit id as 32 lower-case hex characters. Ids are unique per
/// process (a counter is mixed in) and unpredictable across processes (the
/// hash keys come from the operating system's randomness via
/// [`RandomState`]); they are correlation handles, not secrets.
pub fn generate_request_id() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let mut parts = [0u64; 2];
    for (i, part) in parts.iter_mut().enumerate() {
        let mut hasher = RandomState::new().build_hasher();
        hasher.write_u64(n);
        hasher.write_u128(nanos);
        hasher.write_usize(i);
        *part = hasher.finish();
    }
    format!("{:016x}{:016x}", parts[0], parts[1])
}

/// The id for a request with these (lower-cased) headers: the client's
/// `x-request-id` when well formed, otherwise a generated one.
pub fn resolve_request_id(headers: &HashMap<String, String>) -> String {
    headers
        .get(REQUEST_ID_HEADER)
        .map(|v| v.trim())
        .filter(|v| is_valid_request_id(v))
        .map(str::to_string)
        .unwrap_or_else(generate_request_id)
}

/// Echo `id` in the response header and, for a JSON error body that is an
/// object without a `request_id` field, add `"request_id":"<id>"` as its
/// first field.
pub fn attach_request_id(response: &mut Response, id: &str) {
    if !response
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case(REQUEST_ID_RESPONSE_HEADER))
    {
        response.headers.push((
            std::borrow::Cow::Borrowed(REQUEST_ID_RESPONSE_HEADER),
            id.to_string(),
        ));
    }
    if response.status < 400
        || !response.content_type.starts_with("application/json")
        || !is_valid_request_id(id)
    {
        return;
    }
    let body = response.body.trim_start();
    let Some(inner) = body.strip_prefix('{') else {
        return;
    };
    if response.body.contains("\"request_id\"") {
        return;
    }
    let (separator, inner) = if inner.trim_start().starts_with('}') {
        ("", inner.trim_start())
    } else {
        (",", inner)
    };
    response.body = format!("{{\"request_id\":\"{id}\"{separator}{inner}");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_ids_are_kept_and_bad_ones_replaced() {
        for good in ["abc", "0f8e-11:22/33+44=@x_y.z", &"a".repeat(128)] {
            let headers = HashMap::from([(REQUEST_ID_HEADER.to_string(), good.to_string())]);
            assert_eq!(resolve_request_id(&headers), good);
        }
        for bad in [
            "",
            " ",
            "has space",
            "quote\"",
            "back\\slash",
            "new\nline",
            "caf\u{e9}",
            &"a".repeat(129),
        ] {
            let headers = HashMap::from([(REQUEST_ID_HEADER.to_string(), bad.to_string())]);
            let id = resolve_request_id(&headers);
            assert_ne!(id, bad);
            assert_eq!(id.len(), 32);
            assert!(id.bytes().all(|b| b.is_ascii_hexdigit()));
        }
        assert_eq!(resolve_request_id(&HashMap::new()).len(), 32);
    }

    #[test]
    fn generated_ids_are_unique() {
        let ids: std::collections::HashSet<String> =
            (0..10_000).map(|_| generate_request_id()).collect();
        assert_eq!(ids.len(), 10_000);
    }

    #[test]
    fn json_error_bodies_gain_the_id_and_other_bodies_are_untouched() {
        let mut err = Response::error(404, "unknown path");
        attach_request_id(&mut err, "rid-1");
        assert_eq!(
            err.body,
            "{\"request_id\":\"rid-1\",\"error\":\"unknown path\"}"
        );
        assert!(
            err.headers
                .iter()
                .any(|(n, v)| n == REQUEST_ID_RESPONSE_HEADER && v == "rid-1")
        );

        let mut empty = Response::json(500, "{ }".into());
        attach_request_id(&mut empty, "rid-2");
        assert_eq!(empty.body, "{\"request_id\":\"rid-2\"}");

        let mut ok = Response::json(200, "{\"status\":\"ok\"}".into());
        attach_request_id(&mut ok, "rid-3");
        assert_eq!(ok.body, "{\"status\":\"ok\"}");

        let mut text = Response::new(503, "text/plain", "busy".into());
        attach_request_id(&mut text, "rid-4");
        assert_eq!(text.body, "busy");

        let mut array = Response::json(400, "[1]".into());
        attach_request_id(&mut array, "rid-5");
        assert_eq!(array.body, "[1]");

        let mut already = Response::json(400, "{\"request_id\":\"x\"}".into());
        attach_request_id(&mut already, "rid-6");
        assert_eq!(already.body, "{\"request_id\":\"x\"}");

        // A handler-set header is not duplicated.
        let mut preset = Response::json(200, "{}".into()).with_header("X-Request-Id", "own");
        attach_request_id(&mut preset, "rid-7");
        assert_eq!(preset.headers.len(), 1);
    }
}
