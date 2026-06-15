#![no_main]
use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;
use retrieval::openai_embeddings::handle_openai_embeddings;

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    body: String,
}

fuzz_target!(|input: FuzzInput| {
    let _ = handle_openai_embeddings(&input.body);
});
