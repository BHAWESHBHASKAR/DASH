#![no_main]
use libfuzzer_sys::fuzz_target;

use dash_http::{ExpectPolicy, ServerConfig, parse_request_bytes};

fuzz_target!(|data: &[u8]| {
    for expect in [ExpectPolicy::Reject, ExpectPolicy::Continue100] {
        let mut config = ServerConfig::new("fuzz", 1, 1);
        config.expect = expect;
        let Ok(request) = parse_request_bytes(data, &config) else {
            continue;
        };
        // Framing must be unambiguous: no Transfer-Encoding, and the body is
        // exactly the declared Content-Length.
        assert!(!request.headers.contains_key("transfer-encoding"));
        match request.headers.get("content-length") {
            Some(value) => assert_eq!(value.parse::<usize>().ok(), Some(request.body.len())),
            None => assert!(request.body.is_empty()),
        }
        assert!(request.headers.len() <= config.max_header_count);
        let _ = request.path();
        let _ = request.query();
        let _ = request.try_query();
    }
});
