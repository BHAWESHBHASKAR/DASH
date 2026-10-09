use std::collections::HashMap;
use std::net::SocketAddr;

/// A fully read, validated request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Request {
    pub method: String,
    /// Raw request target (path plus optional query).
    pub target: String,
    /// Header fields keyed by lowercase name. Repeated credential headers and
    /// conflicting `Content-Length` values never reach this map (the parser
    /// rejects them); other repeated headers keep the last value.
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
    /// Remote address when the request came from a socket.
    pub peer: Option<SocketAddr>,
}

impl Request {
    /// Case-insensitive header lookup.
    pub fn header(&self, name: &str) -> Option<&str> {
        if name.bytes().any(|b| b.is_ascii_uppercase()) {
            self.headers
                .get(&name.to_ascii_lowercase())
                .map(String::as_str)
        } else {
            self.headers.get(name).map(String::as_str)
        }
    }

    /// Path component of the target (before `?`).
    pub fn path(&self) -> &str {
        self.target
            .split_once('?')
            .map_or(self.target.as_str(), |(path, _)| path)
    }

    /// Decoded query parameters; parameters with invalid percent-encoding are
    /// skipped. Use [`Request::try_query`] to reject them instead.
    pub fn query(&self) -> HashMap<String, String> {
        split_target(&self.target).1
    }

    /// Decoded query parameters, or an error when any key or value has an
    /// invalid percent-encoding.
    pub fn try_query(&self) -> Result<HashMap<String, String>, String> {
        let Some((_, query)) = self.target.split_once('?') else {
            return Ok(HashMap::new());
        };
        let mut out = HashMap::new();
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
            out.insert(percent_decode(raw_key)?, percent_decode(raw_value)?);
        }
        Ok(out)
    }

    /// True when the query string contains an invalid percent-encoding (bad
    /// hex digits, truncated escape, or non-UTF-8 bytes).
    pub fn query_encoding_is_invalid(&self) -> bool {
        query_encoding_is_invalid(&self.target)
    }
}

/// Split a request target into its path and decoded query parameters.
/// Parameters with invalid percent-encoding are skipped; callers that must
/// reject them check [`Request::query_encoding_is_invalid`] first.
pub fn split_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, query_str) = target
        .split_once('?')
        .map(|(path, query)| (path, Some(query)))
        .unwrap_or((target, None));

    let mut query = HashMap::new();
    if let Some(query_str) = query_str {
        for pair in query_str.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
            let (Ok(key), Ok(value)) = (percent_decode(raw_key), percent_decode(raw_value)) else {
                continue;
            };
            query.insert(key, value);
        }
    }
    (path.to_string(), query)
}

/// True when the query string of `target` contains an invalid
/// percent-encoding.
pub(crate) fn query_encoding_is_invalid(target: &str) -> bool {
    let Some((_, query)) = target.split_once('?') else {
        return false;
    };
    query.split('&').any(|pair| {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        percent_decode(key).is_err() || percent_decode(value).is_err()
    })
}

/// Decode `%XX` escapes and `+` (as space). Fails on a truncated or non-hex
/// escape and when the result is not valid UTF-8.
pub fn percent_decode(raw: &str) -> Result<String, String> {
    let bytes = raw.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' => {
                if i + 2 >= bytes.len() {
                    return Err("incomplete percent escape".to_string());
                }
                let hi = decode_hex(bytes[i + 1])?;
                let lo = decode_hex(bytes[i + 2])?;
                out.push((hi << 4) | lo);
                i += 3;
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8(out).map_err(|_| "invalid UTF-8 in URL field".to_string())
}

fn decode_hex(byte: u8) -> Result<u8, String> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err("invalid hex digit".to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_target_percent_decodes_query_values() {
        let (path, query) =
            split_target("/internal/replication/ack?commit_id=a%3Ab%2Fc&replica_id=r+1&flag");
        assert_eq!(path, "/internal/replication/ack");
        assert_eq!(query.get("commit_id").map(String::as_str), Some("a:b/c"));
        assert_eq!(query.get("replica_id").map(String::as_str), Some("r 1"));
        assert_eq!(query.get("flag").map(String::as_str), Some(""));
    }

    #[test]
    fn malformed_escapes_are_detected_so_the_request_can_be_rejected() {
        assert!(query_encoding_is_invalid("/x?bad=%zz&ok=1"));
        assert!(query_encoding_is_invalid("/x?a=%FF"));
        assert!(query_encoding_is_invalid("/x?a=%4"));
        assert!(!query_encoding_is_invalid("/x?a=b+c&d=%41&e"));
        assert!(!query_encoding_is_invalid("/x"));
    }

    #[test]
    fn try_query_errors_on_bad_encoding() {
        let request = |target: &str| Request {
            method: "GET".into(),
            target: target.into(),
            headers: HashMap::new(),
            body: Vec::new(),
            peer: None,
        };
        assert!(request("/x?a=%zz").try_query().is_err());
        assert_eq!(request("/x?a=%41").try_query().unwrap()["a"], "A");
        assert!(request("/x").try_query().unwrap().is_empty());
        assert_eq!(request("/x?a=1").path(), "/x");
    }
}
