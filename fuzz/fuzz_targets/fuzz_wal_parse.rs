#![no_main]
use libfuzzer_sys::fuzz_target;

use arbitrary::Arbitrary;
use store::InMemoryStore;

#[derive(Arbitrary, Debug)]
struct FuzzInput {
    line: String,
}

fuzz_target!(|input: FuzzInput| {
    // The hand-rolled WAL parser (`line_to_record`) is private. Its nearest
    // public entry point is `InMemoryStore::apply_persisted_record_line`,
    // which calls the parser and then dispatches the resulting record into
    // the in-memory indexes. Either step must not panic on weird input.
    let mut store = InMemoryStore::new();
    let _ = store.apply_persisted_record_line(&input.line);
});
