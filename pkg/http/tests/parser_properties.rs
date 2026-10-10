//! Seeded, deterministic property tests for the request parser: mutated
//! requests never panic or hang, accepted requests are consistent, and
//! conflicting framing is never accepted.

use std::{
    io::Write,
    net::{Shutdown, TcpListener, TcpStream},
    time::{Duration, Instant},
};

use dash_http::{ReadError, Request, ServerConfig, parse_request_bytes, read_request};

/// xorshift64*: small, fast and reproducible.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n.max(1) as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }
}

const SEEDS: [&[u8]; 5] = [
    b"GET /health HTTP/1.1\r\nHost: t\r\n\r\n",
    b"GET /v1/retrieve?tenant_id=t&query=a%20b HTTP/1.1\r\nHost: t\r\nAuthorization: Bearer abc\r\nAccept: */*\r\n\r\n",
    b"POST /v1/ingest HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 13\r\n\r\n{\"claim\":\"x\"}",
    b"POST /x HTTP/1.0\r\nX-Api-Key: k\r\nContent-Length: 0\r\n\r\n",
    b"PUT /a/b HTTP/1.1\nHost: t\nContent-Length: 3\n\nabc",
];

const INTERESTING: [&[u8]; 12] = [
    b"\r\n",
    b"\n",
    b"\r",
    b": ",
    b" ",
    b"\t",
    b"Content-Length: 7\r\n",
    b"Transfer-Encoding: chunked\r\n",
    b"transfer-encoding : chunked\r\n",
    b"Expect: 100-continue\r\n",
    b"%zz",
    b"\0",
];

fn mutate(rng: &mut Rng, seed: &[u8]) -> Vec<u8> {
    let mut data = seed.to_vec();
    for _ in 0..=rng.below(4) {
        if data.is_empty() {
            data.push(b'G');
        }
        match rng.below(7) {
            0 => {
                let i = rng.below(data.len());
                data[i] ^= 1 << rng.below(8);
            }
            1 => {
                let start = rng.below(data.len());
                let end = (start + 1 + rng.below(8)).min(data.len());
                data.drain(start..end);
            }
            2 => {
                let at = rng.below(data.len() + 1);
                let junk: Vec<u8> = (0..1 + rng.below(6)).map(|_| rng.next() as u8).collect();
                data.splice(at..at, junk);
            }
            3 => {
                let at = rng.below(data.len() + 1);
                let piece = *rng.pick(&INTERESTING);
                data.splice(at..at, piece.iter().copied());
            }
            4 => {
                let keep = rng.below(data.len() + 1);
                data.truncate(keep);
            }
            5 => {
                let a = rng.below(data.len());
                let b = rng.below(data.len());
                data.swap(a, b);
            }
            _ => {
                // Duplicate a random slice in place.
                let start = rng.below(data.len());
                let end = (start + 1 + rng.below(24)).min(data.len());
                let slice = data[start..end].to_vec();
                data.splice(end..end, slice);
            }
        }
    }
    data
}

fn cfg() -> ServerConfig {
    ServerConfig::new("fuzz", 1, 1)
}

/// Invariants every accepted request must satisfy.
fn check_accepted(request: &Request, raw: &[u8]) {
    let config = cfg();
    assert!(
        !request.headers.contains_key("transfer-encoding"),
        "chunked framing accepted: {raw:?}"
    );
    assert!(!request.headers.contains_key("expect"), "{raw:?}");
    match request.headers.get("content-length") {
        Some(value) => {
            let declared: usize = value.parse().unwrap_or_else(|_| {
                panic!("accepted non-numeric content-length {value:?}: {raw:?}")
            });
            assert_eq!(declared, request.body.len(), "{raw:?}");
        }
        None => assert!(request.body.is_empty(), "{raw:?}"),
    }
    assert!(request.headers.len() <= config.max_header_count, "{raw:?}");
    assert!(request.body.len() <= config.max_body_bytes);
    assert!(!request.method.is_empty() && !request.method.contains(char::is_whitespace));
    assert!(!request.target.is_empty() && !request.target.contains(char::is_whitespace));
    for (name, value) in &request.headers {
        assert_eq!(name, &name.to_ascii_lowercase());
        assert!(
            !name.is_empty() && !name.contains([' ', '\t', ':']),
            "{name:?}"
        );
        assert_eq!(value, value.trim());
    }
    // Derived accessors never panic.
    let _ = request.path();
    let _ = request.query();
    let _ = request.try_query();
}

#[test]
fn mutated_requests_never_panic_and_accepted_ones_are_consistent() {
    let mut accepted = 0usize;
    let mut rejected = 0usize;
    for seed_index in 0..64u64 {
        let mut rng = Rng::new(0xD45A_0000 + seed_index);
        for _ in 0..400 {
            let seed = *rng.pick(&SEEDS);
            let raw = mutate(&mut rng, seed);
            match parse_request_bytes(&raw, &cfg()) {
                Ok(request) => {
                    accepted += 1;
                    check_accepted(&request, &raw);
                }
                Err(ReadError { status, message }) => {
                    rejected += 1;
                    assert!(
                        matches!(status, 400 | 408 | 413 | 417 | 431 | 501 | 505),
                        "unexpected status {status} ({message}) for {raw:?}"
                    );
                    assert!(!message.is_empty());
                }
            }
        }
    }
    // The mutator must exercise both outcomes, or the test proves nothing.
    assert!(accepted > 1000, "only {accepted} accepted");
    assert!(rejected > 1000, "only {rejected} rejected");
}

#[test]
fn arbitrary_byte_soup_never_panics() {
    let mut rng = Rng::new(7);
    for _ in 0..5000 {
        let len = rng.below(600);
        let raw: Vec<u8> = (0..len)
            .map(|_| match rng.below(4) {
                0 => *rng.pick(b"\r\n: "),
                1 => rng.next() as u8,
                _ => b'a' + rng.below(26) as u8,
            })
            .collect();
        let _ = parse_request_bytes(&raw, &cfg());
    }
}

fn case_variant(rng: &mut Rng, name: &str) -> String {
    name.chars()
        .map(|c| {
            if rng.below(2) == 0 {
                c.to_ascii_uppercase()
            } else {
                c.to_ascii_lowercase()
            }
        })
        .collect()
}

fn benign_headers(rng: &mut Rng) -> Vec<String> {
    let names = [
        "Host",
        "Accept",
        "User-Agent",
        "X-Request-Id",
        "Cookie",
        "Accept-Language",
    ];
    (0..rng.below(5))
        .map(|i| format!("{}: v{i}\r\n", rng.pick(&names)))
        .collect()
}

#[test]
fn conflicting_or_ambiguous_framing_is_never_accepted() {
    let mut rng = Rng::new(99);
    for _ in 0..2000 {
        let a = rng.below(40);
        let mut b = rng.below(40);
        if a == b {
            b += 1;
        }
        let mut lines = benign_headers(&mut rng);
        let cl_name = case_variant(&mut rng, "Content-Length");
        lines.push(format!("{cl_name}: {a}\r\n"));
        let other_name = case_variant(&mut rng, "content-length");
        lines.push(format!("{other_name}: {b}\r\n"));
        // Shuffle by rotating.
        let rotate = rng.below(lines.len());
        lines.rotate_left(rotate);
        let body = "x".repeat(a.max(b));
        let raw = format!("POST /x HTTP/1.1\r\n{}\r\n{body}", lines.concat());
        let err = parse_request_bytes(raw.as_bytes(), &cfg()).expect_err("conflicting lengths");
        assert_eq!(err.status, 400, "{raw:?}");

        // Any Transfer-Encoding, with or without a Content-Length, is refused.
        let te_name = case_variant(&mut rng, "Transfer-Encoding");
        let value = *rng.pick(&["chunked", "CHUNKED", "identity", "gzip, chunked", ""]);
        let mut lines = benign_headers(&mut rng);
        if rng.below(2) == 0 {
            lines.push(format!("Content-Length: {a}\r\n"));
        }
        lines.push(format!("{te_name}: {value}\r\n"));
        let rotate = rng.below(lines.len());
        lines.rotate_left(rotate);
        let raw = format!(
            "POST /x HTTP/1.1\r\n{}\r\n{}",
            lines.concat(),
            "x".repeat(a)
        );
        let err = parse_request_bytes(raw.as_bytes(), &cfg()).expect_err("transfer-encoding");
        assert_eq!(err.status, 501, "{raw:?}");
    }
}

fn pair() -> (TcpStream, TcpStream, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    (client, server, listener)
}

#[test]
fn streaming_reader_never_hangs_and_agrees_with_the_buffered_parser() {
    let mut rng = Rng::new(2024);
    let config = cfg();
    for _ in 0..300 {
        let seed = *rng.pick(&SEEDS);
        let raw = mutate(&mut rng, seed);
        let (mut client, mut server, _listener) = pair();
        // Write in random chunks, then close: the reader must finish (the
        // deadline is only a backstop) with a request or an error.
        let mut sent = 0;
        while sent < raw.len() {
            let end = (sent + 1 + rng.below(40)).min(raw.len());
            if client.write_all(&raw[sent..end]).is_err() {
                break;
            }
            sent = end;
        }
        let _ = client.shutdown(Shutdown::Write);
        let started = Instant::now();
        let streamed = read_request(&mut server, &config, started + Duration::from_secs(2));
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "reader hung on {raw:?}"
        );
        if let Ok(request) = parse_request_bytes(&raw, &config) {
            let streamed = streamed
                .unwrap_or_else(|e| {
                    panic!("buffered parser accepted but stream failed ({e}): {raw:?}")
                })
                .expect("a request");
            assert_eq!(streamed.method, request.method);
            assert_eq!(streamed.target, request.target);
            assert_eq!(streamed.headers, request.headers);
            assert_eq!(streamed.body, request.body);
        }
    }
}
