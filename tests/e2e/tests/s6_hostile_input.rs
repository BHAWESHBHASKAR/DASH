//! Scenario 6: hostile input resilience against the real HTTP listeners of
//! retrieval and ingestion. After every abusive exchange the process must
//! still be alive and a normal authenticated request must still succeed.

use std::io::{Read, Write};
use std::thread;
use std::time::{Duration, Instant};

use dash_e2e::http;
use dash_e2e::*;
use serde_json::{Value, json};

const WORKERS: usize = 4;

fn opts() -> StackOpts {
    StackOpts {
        extra_ingest_env: vec![("DASH_INGEST_HTTP_WORKERS".into(), WORKERS.to_string())],
        extra_retrieval_env: vec![("DASH_RETRIEVAL_HTTP_WORKERS".into(), WORKERS.to_string())],
        ..Default::default()
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Svc {
    Retrieval,
    Ingestion,
}

struct Target<'a> {
    svc: Svc,
    stack: &'a mut Stack,
}

impl Target<'_> {
    fn client(&self) -> Client {
        match self.svc {
            Svc::Retrieval => self.stack.rc(),
            Svc::Ingestion => self.stack.ic(),
        }
    }
    fn path(&self) -> &'static str {
        match self.svc {
            Svc::Retrieval => "/v1/retrieve",
            Svc::Ingestion => "/v1/ingest",
        }
    }
    fn key(&self) -> String {
        match self.svc {
            Svc::Retrieval => self.stack.rk("tenant-a").1,
            Svc::Ingestion => self.stack.ik("tenant-a").1,
        }
    }
    fn good_body(&self, tag: &str) -> Value {
        match self.svc {
            Svc::Retrieval => json!({"tenant_id": "tenant-a", "query": "alpha", "top_k": 3}),
            Svc::Ingestion => bundle("tenant-a", &format!("h-{tag}"), "hostile input survivor", 1),
        }
    }

    /// A syntactically valid request head for this target.
    fn head(&self, extra: &str, content_length: Option<usize>) -> String {
        let mut h = format!(
            "POST {} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Type: application/json\r\nx-api-key: {}\r\n{extra}",
            self.path(),
            self.key()
        );
        if let Some(n) = content_length {
            h.push_str(&format!("Content-Length: {n}\r\n"));
        }
        h.push_str("\r\n");
        h
    }

    fn alive(&mut self) -> bool {
        let p = match self.svc {
            Svc::Retrieval => self.stack.retrieval.as_mut(),
            Svc::Ingestion => self.stack.ingest.as_mut(),
        };
        p.map(|p| p.is_alive()).unwrap_or(false)
    }

    /// The process is up and a normal authenticated request works.
    fn assert_healthy(&mut self, after: &str) {
        self.assert_healthy_within(after, Duration::from_secs(10));
    }

    fn assert_healthy_within(&mut self, after: &str, limit: Duration) {
        assert!(self.alive(), "process died after: {after}\n{}", self.log());
        let mut c = self.client();
        c.timeout = Duration::from_secs(30);
        let key = self.key();
        let started = Instant::now();
        let r = c.post_json(
            self.path(),
            &[("x-api-key", &key)],
            &self.good_body(&after.replace(' ', "-")),
        );
        assert_eq!(
            r.status,
            200,
            "normal request failed after: {after}: {}\n{}",
            r.body,
            self.log()
        );
        println!(
            "[{}] healthy after '{after}' in {:?}",
            if self.svc == Svc::Retrieval {
                "retrieval"
            } else {
                "ingestion"
            },
            started.elapsed()
        );
        assert!(
            started.elapsed() < limit,
            "normal request took {:?} (limit {limit:?}) after: {after}",
            started.elapsed()
        );
        assert_eq!(
            c.get("/live", &[]).status,
            200,
            "liveness failed after: {after}"
        );
    }

    fn log(&self) -> String {
        match self.svc {
            Svc::Retrieval => self.stack.retrieval_log(),
            Svc::Ingestion => self.stack.ingest_log(),
        }
    }
}

/// Acceptable outcomes for abusive input: a 4xx answer or a closed
/// connection. Never a 2xx and never a hang (an io timeout is an `Err`).
fn assert_rejected(what: &str, r: std::io::Result<Option<Resp>>) {
    match r {
        Ok(Some(resp)) => assert!(
            (400..500).contains(&resp.status) || resp.status == 501,
            "{what}: expected a 4xx (or 501) rejection, got {} {}",
            resp.status,
            resp.body.chars().take(200).collect::<String>()
        ),
        Ok(None) => {}
        Err(e) => panic!("{what}: server neither answered nor closed the connection: {e}"),
    }
}

fn run_cases(t: &mut Target<'_>) {
    let c = t.client();
    t.assert_healthy("baseline");

    // 1 MB of '[' as the JSON body.
    let body = vec![b'['; 1024 * 1024];
    let mut raw = t.head("", Some(body.len())).into_bytes();
    raw.extend_from_slice(&body);
    assert_rejected("1 MB of '['", c.raw(&raw));
    t.assert_healthy("1 MB of [");

    // 100k nested arrays, well formed.
    let mut nested = "[".repeat(100_000);
    nested.push_str(&"]".repeat(100_000));
    let mut raw = t.head("", Some(nested.len())).into_bytes();
    raw.extend_from_slice(nested.as_bytes());
    assert_rejected("100k nested arrays", c.raw(&raw));
    t.assert_healthy("100k nested arrays");

    // Deeply nested objects too.
    let mut nested = "{\"a\":".repeat(50_000);
    nested.push('1');
    nested.push_str(&"}".repeat(50_000));
    let mut raw = t.head("", Some(nested.len())).into_bytes();
    raw.extend_from_slice(nested.as_bytes());
    assert_rejected("50k nested objects", c.raw(&raw));
    t.assert_healthy("50k nested objects");

    // Header flood: more than 100 headers.
    let flood: String = (0..300).map(|i| format!("X-Flood-{i}: v{i}\r\n")).collect();
    let good = serde_json::to_vec(&t.good_body("flood")).unwrap();
    let mut raw = t.head(&flood, Some(good.len())).into_bytes();
    raw.extend_from_slice(&good);
    assert_rejected("300 headers", c.raw(&raw));
    t.assert_healthy("header flood");

    // A single 64 KiB header line.
    let big = format!("X-Big: {}\r\n", "A".repeat(64 * 1024));
    let mut raw = t.head(&big, Some(good.len())).into_bytes();
    raw.extend_from_slice(&good);
    assert_rejected("64 KiB header line", c.raw(&raw));
    t.assert_healthy("64 KiB header line");

    // Just over the documented per-line cap (8 KiB).
    let mid = format!("X-Mid: {}\r\n", "B".repeat(9 * 1024));
    let mut raw = t.head(&mid, Some(good.len())).into_bytes();
    raw.extend_from_slice(&good);
    assert_rejected("9 KiB header line", c.raw(&raw));
    t.assert_healthy("9 KiB header line");

    // An enormous request line.
    let line = format!(
        "GET /{} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n",
        "p".repeat(128 * 1024)
    );
    assert_rejected("128 KiB request line", c.raw(line.as_bytes()));
    t.assert_healthy("huge request line");

    // Content-Length larger than what is sent, connection left open: the
    // server must give up (deadline) rather than wait forever.
    let started = Instant::now();
    let mut s = c.connect().unwrap();
    s.set_read_timeout(Some(Duration::from_secs(25))).unwrap();
    s.write_all(t.head("", Some(1000)).as_bytes()).unwrap();
    s.write_all(b"{\"tenant_id\":").unwrap();
    let outcome = http::read_optional(&mut s);
    assert_rejected("short body with Content-Length 1000", outcome);
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "server held a stalled body for {:?}",
        started.elapsed()
    );
    t.assert_healthy("stalled body");

    // Content-Length smaller than the body actually sent.
    let good_s = serde_json::to_string(&t.good_body("cl-short")).unwrap();
    let mut raw = t.head("", Some(5)).into_bytes();
    raw.extend_from_slice(good_s.as_bytes());
    assert_rejected("Content-Length 5 with a longer body", c.raw(&raw));
    t.assert_healthy("Content-Length too small");

    // Negative, non-numeric and conflicting Content-Length values.
    for (label, hdr) in [
        ("negative CL", "Content-Length: -1\r\n"),
        ("non numeric CL", "Content-Length: abc\r\n"),
        ("huge CL", "Content-Length: 99999999999999999999\r\n"),
    ] {
        let raw = format!(
            "POST {} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nx-api-key: {}\r\n{hdr}\r\n{good_s}",
            t.path(),
            t.key()
        );
        assert_rejected(label, c.raw(raw.as_bytes()));
        t.assert_healthy(label);
    }
    let raw = format!(
        "POST {} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nx-api-key: {}\r\nContent-Length: {}\r\nContent-Length: {}\r\n\r\n{good_s}",
        t.path(),
        t.key(),
        good_s.len(),
        good_s.len() + 7
    );
    assert_rejected("conflicting Content-Length headers", c.raw(raw.as_bytes()));
    t.assert_healthy("conflicting Content-Length");

    // Chunked transfer encoding: either decoded properly or refused, never a hang.
    let good_c = serde_json::to_string(&t.good_body("chunked")).unwrap();
    let (a, b) = good_c.split_at(good_c.len() / 2);
    let chunked = format!(
        "POST {} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nContent-Type: application/json\r\nx-api-key: {}\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{a}\r\n{:x}\r\n{b}\r\n0\r\n\r\n",
        t.path(),
        t.key(),
        a.len(),
        b.len()
    );
    match c.raw(chunked.as_bytes()) {
        Ok(Some(r)) => assert!(
            r.status == 200 || (400..500).contains(&r.status) || r.status == 501,
            "chunked request answered {} {}",
            r.status,
            r.body
        ),
        Ok(None) => {}
        Err(e) => panic!("chunked request hung or failed: {e}"),
    }
    t.assert_healthy("chunked encoding");
    // Malformed chunk framing and chunked + Content-Length together.
    let bad = format!(
        "POST {} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nx-api-key: {}\r\nTransfer-Encoding: chunked\r\n\r\nZZZZ\r\nnot a chunk\r\n",
        t.path(),
        t.key()
    );
    assert_rejected("malformed chunk size", c.raw(bad.as_bytes()));
    t.assert_healthy("malformed chunk");
    let both = format!(
        "POST {} HTTP/1.1\r\nHost: x\r\nConnection: close\r\nx-api-key: {}\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
        t.path(),
        t.key()
    );
    match c.raw(both.as_bytes()) {
        Ok(Some(r)) => assert!(
            r.status < 500 || r.status == 501,
            "CL+TE answered {}: {}",
            r.status,
            r.body
        ),
        Ok(None) => {}
        Err(e) => panic!("CL+TE request hung: {e}"),
    }
    t.assert_healthy("CL + TE");

    // Binary garbage, bad HTTP versions and bare newlines.
    for (label, bytes) in [
        (
            "binary garbage",
            vec![0u8, 255, 1, 254, 13, 10, 13, 10, 7, 7, 7],
        ),
        ("not http", b"HELLO WORLD\r\n\r\n".to_vec()),
        (
            "bad version",
            b"GET /live HTTP/9.9\r\nHost: x\r\n\r\n".to_vec(),
        ),
        (
            "lone LF headers",
            b"GET /live HTTP/1.1\nHost: x\n\n".to_vec(),
        ),
        (
            "NUL in target",
            b"GET /li\0ve HTTP/1.1\r\nHost: x\r\n\r\n".to_vec(),
        ),
    ] {
        match c.raw(&bytes) {
            Ok(_) => {}
            Err(e) => panic!("{label}: server hung: {e}"),
        }
        t.assert_healthy(label);
    }

    // Many connections opened and dropped without sending anything.
    for _ in 0..200 {
        let _ = c.connect();
    }
    t.assert_healthy("200 idle connects dropped");

    // Slowloris: far more drip-feeding connections than worker threads.
    let addr = c.addr;
    let head = t.head("", Some(10_000));
    let stop_at = Instant::now() + Duration::from_secs(14);
    let mut drippers = vec![];
    for _ in 0..16 {
        let head = head.clone();
        drippers.push(thread::spawn(move || {
            let Ok(mut s) = std::net::TcpStream::connect(addr) else {
                return None;
            };
            s.set_read_timeout(Some(Duration::from_millis(50))).ok();
            let started = Instant::now();
            let mut closed_after = None;
            for b in head.bytes() {
                if Instant::now() > stop_at {
                    break;
                }
                if s.write_all(&[b]).is_err() {
                    closed_after = Some(started.elapsed());
                    break;
                }
                let mut buf = [0u8; 512];
                match s.read(&mut buf) {
                    Ok(0) => {
                        closed_after = Some(started.elapsed());
                        break;
                    }
                    Ok(_) => {
                        // Server answered (e.g. 408); fine.
                        closed_after = Some(started.elapsed());
                        break;
                    }
                    Err(_) => {}
                }
                thread::sleep(Duration::from_millis(250));
            }
            closed_after
        }));
    }
    thread::sleep(Duration::from_millis(500));
    // While they drip, a normal request must be served (it may queue behind
    // the deadline of slow peers but must complete).
    // Known weakness (worker pool exhaustion): 16 drip connections hold every
    // worker until their request deadline, so the legitimate request queues
    // for roughly the deadline. It must still complete, within 30 s.
    t.assert_healthy_within("slowloris in progress", Duration::from_secs(30));
    let closed: Vec<Option<Duration>> = drippers.into_iter().map(|h| h.join().unwrap()).collect();
    // Every connection a worker picked up must have been cut off at the
    // request deadline (10 s), not held open for as long as the client drips.
    // The rest were still queued behind busy workers when the clients quit.
    let cut: Vec<Duration> = closed.iter().flatten().copied().collect();
    assert!(
        cut.len() >= WORKERS,
        "expected the {WORKERS} served slow connections to be cut off at the deadline, got {} ({closed:?})",
        cut.len()
    );
    assert!(
        cut.iter().all(|d| *d < Duration::from_secs(13)),
        "a slow connection outlived the request deadline: {cut:?}"
    );
    t.assert_healthy("slowloris finished");
}

#[test]
fn retrieval_survives_hostile_input() {
    let mut stack = Stack::new(opts());
    stack.start_all();
    let mut t = Target {
        svc: Svc::Retrieval,
        stack: &mut stack,
    };
    run_cases(&mut t);
}

#[test]
fn ingestion_survives_hostile_input() {
    let mut stack = Stack::new(opts());
    stack.start_all();
    let mut t = Target {
        svc: Svc::Ingestion,
        stack: &mut stack,
    };
    run_cases(&mut t);
    // Nothing the abuse sent may have been ingested.
    let leader = stack.leader_state();
    let stray: Vec<&String> = leader
        .claims
        .keys()
        .filter(|c| !c.starts_with("h-"))
        .collect();
    assert!(
        stray.is_empty(),
        "hostile requests created claims: {stray:?}"
    );
}
