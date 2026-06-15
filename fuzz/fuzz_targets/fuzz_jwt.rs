#![no_main]
use libfuzzer_sys::fuzz_target;

use std::collections::HashMap;

use arbitrary::Arbitrary;
use auth::{verify_hs256_token_for_tenant, JwtValidationConfig};

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    token: String,
    secret: String,
    iss: Option<String>,
    aud: Option<String>,
    now_unix_secs: u64,
}

fuzz_target!(|input: FuzzInput| {
    let config = JwtValidationConfig {
        hs256_secret: input.secret,
        hs256_fallback_secrets: Vec::new(),
        hs256_secrets_by_kid: HashMap::new(),
        issuer: input.iss,
        audience: input.aud,
        leeway_secs: 0,
        require_exp: false,
    };
    let _ = verify_hs256_token_for_tenant(&input.token, "t1", &config, input.now_unix_secs);
});
