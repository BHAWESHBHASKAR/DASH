//! AWS Signature Version 4 (header form), enough for the KMS JSON API.
//!
//! Implemented from the published specification
//! (<https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html>)
//! and checked against the AWS SigV4 test suite vectors (see the tests).

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

type HmacSha256 = Hmac<Sha256>;

pub(crate) const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// One request to sign. Header names are matched case-insensitively; every
/// header given is signed.
pub(crate) struct SigningRequest<'a> {
    pub method: &'a str,
    /// Absolute path, not yet URI-encoded (`/`).
    pub path: &'a str,
    /// Query parameters, not yet URI-encoded.
    pub query: &'a [(&'a str, &'a str)],
    pub headers: &'a [(&'a str, &'a str)],
    pub payload: &'a [u8],
}

/// Where and when the signature is valid.
pub(crate) struct SigningScope<'a> {
    /// `YYYYMMDDTHHMMSSZ`.
    pub amz_date: &'a str,
    pub region: &'a str,
    pub service: &'a str,
}

fn hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC takes any key length");
    mac.update(msg);
    mac.finalize().into_bytes().into()
}

fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

/// RFC 3986 percent-encoding as SigV4 requires: unreserved characters kept,
/// everything else `%XX` (upper case). `/` is kept when `keep_slash`.
fn uri_encode(input: &str, keep_slash: bool) -> String {
    let mut out = String::with_capacity(input.len());
    for b in input.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'/' if keep_slash => out.push('/'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn canonical_query(query: &[(&str, &str)]) -> String {
    let mut pairs: Vec<(String, String)> = query
        .iter()
        .map(|(k, v)| (uri_encode(k, false), uri_encode(v, false)))
        .collect();
    pairs.sort();
    pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

/// Lower-cased, sorted header names and their trimmed values (inner runs of
/// spaces collapsed; repeated names joined with `,`).
fn canonical_headers(headers: &[(&str, &str)]) -> (String, String) {
    let mut list: Vec<(String, String)> = headers
        .iter()
        .map(|(name, value)| {
            let collapsed = value.split_whitespace().collect::<Vec<_>>().join(" ");
            (name.to_ascii_lowercase(), collapsed)
        })
        .collect();
    list.sort_by(|a, b| a.0.cmp(&b.0));
    let mut merged: Vec<(String, String)> = Vec::new();
    for (name, value) in list {
        match merged.last_mut() {
            Some((last, joined)) if *last == name => {
                joined.push(',');
                joined.push_str(&value);
            }
            _ => merged.push((name, value)),
        }
    }
    let canonical: String = merged.iter().map(|(n, v)| format!("{n}:{v}\n")).collect();
    let signed = merged
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    (canonical, signed)
}

/// The canonical request and the signed header list.
pub(crate) fn canonical_request(req: &SigningRequest<'_>) -> (String, String) {
    let (headers, signed) = canonical_headers(req.headers);
    let path = if req.path.is_empty() { "/" } else { req.path };
    let text = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method,
        uri_encode(path, true),
        canonical_query(req.query),
        headers,
        signed,
        sha256_hex(req.payload)
    );
    (text, signed)
}

/// The derived signing key for a date (`YYYYMMDD`), region and service.
pub(crate) fn signing_key(
    secret: &str,
    date: &str,
    region: &str,
    service: &str,
) -> Zeroizing<[u8; 32]> {
    let k_secret = Zeroizing::new(format!("AWS4{secret}"));
    let k_date = Zeroizing::new(hmac(k_secret.as_bytes(), date.as_bytes()));
    let k_region = Zeroizing::new(hmac(&*k_date, region.as_bytes()));
    let k_service = Zeroizing::new(hmac(&*k_region, service.as_bytes()));
    Zeroizing::new(hmac(&*k_service, b"aws4_request"))
}

/// The `Authorization` header value for `req`.
pub(crate) fn authorization(
    access_key_id: &str,
    secret: &str,
    scope: &SigningScope<'_>,
    req: &SigningRequest<'_>,
) -> String {
    let (creq, signed) = canonical_request(req);
    let date = &scope.amz_date[..8.min(scope.amz_date.len())];
    let credential_scope = format!("{date}/{}/{}/aws4_request", scope.region, scope.service);
    let string_to_sign = format!(
        "{ALGORITHM}\n{}\n{credential_scope}\n{}",
        scope.amz_date,
        sha256_hex(creq.as_bytes())
    );
    let key = signing_key(secret, date, scope.region, scope.service);
    let signature = hex::encode(hmac(&*key, string_to_sign.as_bytes()));
    format!(
        "{ALGORITHM} Credential={access_key_id}/{credential_scope}, SignedHeaders={signed}, Signature={signature}"
    )
}

/// `YYYYMMDDTHHMMSSZ` for Unix seconds.
pub(crate) fn amz_date(unix_secs: u64) -> String {
    let days = (unix_secs / 86_400) as i64;
    let rem = unix_secs % 86_400;
    let (y, m, d) = super::civil_from_days(days);
    format!(
        "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
        rem / 3600,
        (rem / 60) % 60,
        rem % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // Credentials and scope of the AWS SigV4 test suite.
    const AKID: &str = "AKIDEXAMPLE";
    const SECRET: &str = "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY";
    const DATE: &str = "20150830T123600Z";

    fn scope(service: &str) -> SigningScope<'_> {
        SigningScope {
            amz_date: DATE,
            region: "us-east-1",
            service,
        }
    }

    #[test]
    fn signing_key_matches_the_published_example() {
        // IAM user guide, "Derive a signing key" example.
        let key = signing_key(SECRET, "20150830", "us-east-1", "iam");
        assert_eq!(
            hex::encode(*key),
            "c4afb1cc5771d871763a393e44b703571b55cc28424d1a5e86da6ed3c154a4b9"
        );
    }

    #[test]
    fn test_suite_get_vanilla() {
        let req = SigningRequest {
            method: "GET",
            path: "/",
            query: &[],
            headers: &[("Host", "example.amazonaws.com"), ("X-Amz-Date", DATE)],
            payload: b"",
        };
        let (creq, _) = canonical_request(&req);
        assert_eq!(
            creq,
            "GET\n/\n\nhost:example.amazonaws.com\nx-amz-date:20150830T123600Z\n\nhost;x-amz-date\ne3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            authorization(AKID, SECRET, &scope("service"), &req),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=host;x-amz-date, Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31"
        );
    }

    #[test]
    fn test_suite_post_vanilla() {
        let req = SigningRequest {
            method: "POST",
            path: "/",
            query: &[],
            headers: &[("Host", "example.amazonaws.com"), ("X-Amz-Date", DATE)],
            payload: b"",
        };
        assert!(
            authorization(AKID, SECRET, &scope("service"), &req).ends_with(
                "Signature=5da7c1a2acd57cee7505fc6676e4e544621c30862966e37dddb68e92efbe5d6b"
            )
        );
    }

    #[test]
    fn test_suite_get_vanilla_query_order_key_case() {
        let req = SigningRequest {
            method: "GET",
            path: "/",
            query: &[("Param2", "value2"), ("Param1", "value1")],
            headers: &[("Host", "example.amazonaws.com"), ("X-Amz-Date", DATE)],
            payload: b"",
        };
        assert!(
            authorization(AKID, SECRET, &scope("service"), &req).ends_with(
                "Signature=b97d918cfa904a5beff61c982a1b6f458b799221646efd99d3219ec94cdf2500"
            )
        );
    }

    #[test]
    fn test_suite_post_x_www_form_urlencoded() {
        let req = SigningRequest {
            method: "POST",
            path: "/",
            query: &[],
            headers: &[
                ("Content-Type", "application/x-www-form-urlencoded"),
                ("Host", "example.amazonaws.com"),
                ("X-Amz-Date", DATE),
            ],
            payload: b"Param1=value1",
        };
        assert_eq!(
            authorization(AKID, SECRET, &scope("service"), &req),
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, SignedHeaders=content-type;host;x-amz-date, Signature=ff11897932ad3f4e8b18135d722051e5ac45fc38421b1da7b9d196a0fe09473a"
        );
    }

    #[test]
    fn iam_list_users_example() {
        // IAM user guide, complete signing example (GET ListUsers).
        let req = SigningRequest {
            method: "GET",
            path: "/",
            query: &[("Action", "ListUsers"), ("Version", "2010-05-08")],
            headers: &[
                (
                    "Content-Type",
                    "application/x-www-form-urlencoded; charset=utf-8",
                ),
                ("Host", "iam.amazonaws.com"),
                ("X-Amz-Date", DATE),
            ],
            payload: b"",
        };
        assert!(authorization(AKID, SECRET, &scope("iam"), &req).ends_with(
            "Signature=5d672d79c15b13162d9279b0855cfba6789a8edb4c82c400e06b5924a6f2b5d7"
        ));
    }

    #[test]
    fn header_values_are_trimmed_and_repeated_names_joined() {
        let (canonical, signed) = canonical_headers(&[
            ("My-Header1", "  a   b   c "),
            ("my-header1", "d"),
            ("Host", "h"),
        ]);
        assert_eq!(canonical, "host:h\nmy-header1:a b c,d\n");
        assert_eq!(signed, "host;my-header1");
    }

    #[test]
    fn encoding_and_dates() {
        assert_eq!(uri_encode("a b/c~d*", true), "a%20b/c~d%2A");
        assert_eq!(uri_encode("a/b", false), "a%2Fb");
        assert_eq!(amz_date(1_440_938_160), DATE);
    }
}
