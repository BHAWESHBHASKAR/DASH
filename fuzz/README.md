# DASH fuzz harnesses

`cargo-fuzz` / libFuzzer harnesses for the DASH retrieval engine. Each target
feeds a fuzzer-derived `Arbitrary` input to a focused, parser- or scoring-shaped
public entry point of the workspace crates and asserts that the call terminates
without panicking (and, where relevant, returns bounded outputs).

The `fuzz/` crate is its own workspace, so the main `cargo build --workspace`
and `cargo test --workspace` invocations are not affected.

## Targets

| File | Target name | Public surface exercised |
| --- | --- | --- |
| `fuzz_targets/fuzz_jwt.rs` | `fuzz_jwt` | `auth::verify_hs256_token_for_tenant` |
| `fuzz_targets/fuzz_openai_embeddings.rs` | `fuzz_openai_embeddings` | `retrieval::openai_embeddings::handle_openai_embeddings` |
| `fuzz_targets/fuzz_ranking.rs` | `fuzz_ranking` | `ranking::score_claim`, `ranking::bm25_score` |
| `fuzz_targets/fuzz_wal_parse.rs` | `fuzz_wal_parse` | `store::InMemoryStore::apply_persisted_record_line` (the public wrapper around the hand-rolled `line_to_record` WAL parser) |

Each target is a thin `fuzz_target!` block. The `FuzzInput` structs derive
`Arbitrary`, so libFuzzer generates structured inputs (UTF-8 strings, `Option`s,
`u64`/`usize`/`f32`s) and the harness just calls the public API.

The ranking target also asserts that `score_claim` and `bm25_score` never
produce `NaN` or `±∞` scores — the only way the fuzzer finds a regression
in the bounded-score contract.

## Prerequisites

- A recent nightly toolchain:
  ```sh
  rustup install nightly
  ```
- `cargo-fuzz`:
  ```sh
  cargo install cargo-fuzz
  ```

## Running a target

From the repo root:

```sh
cargo +nightly fuzz run fuzz_jwt                       # one process, infinite
cargo +nightly fuzz run fuzz_openai_embeddings -- -max_total_time=60
cargo +nightly fuzz run fuzz_ranking -- -max_total_time=60
cargo +nightly fuzz run fuzz_wal_parse -- -max_total_time=60
```

libFuzzer flags worth knowing:

- `-max_total_time=N` — stop after N seconds.
- `-max_len=N` — cap the generated input size.
- `-jobs=N` / `-workers=N` — parallel fuzzing.
- `-print_final_stats=1` — print a coverage summary at the end.

Crash artefacts land in `fuzz/artifacts/<target>/`. The "shrunk" minimal
reproducer is in that directory when libFuzzer reports `==<pid>>` a
deadly signal. A regression test should be added next to the existing
unit tests in the affected crate for any new finding.

## Building / checking without nightly

The `fuzz` crate compiles on stable — `libfuzzer-sys` is built with
`link_libfuzzer` off, and the `fuzz_target!` macro only expands to
`extern "C" fn LLVMFuzzerTestOneInput`. So:

```sh
cd fuzz
cargo check          # stable is fine
cargo +nightly check # also fine
```

`cargo +nightly fuzz run …` is the only step that requires nightly
(it needs `-Z sanitizer=address` and friends).

## Layout

```
fuzz/
├── Cargo.toml                  # separate workspace, path-deps on the main crates
├── README.md                   # this file
├── src/lib.rs                  # empty lib target so `[[bin]]` paths can live in fuzz_targets/
└── fuzz_targets/
    ├── fuzz_jwt.rs
    ├── fuzz_openai_embeddings.rs
    ├── fuzz_ranking.rs
    └── fuzz_wal_parse.rs
```

## Design notes

- The WAL parser is `line_to_record` in `pkg/store/src/lib.rs`, which is
  `fn`-private. Its nearest public entry point is
  `InMemoryStore::apply_persisted_record_line`, which calls the parser
  and then dispatches the resulting record into the in-memory indexes.
  Fuzzing the public wrapper covers both the parser and the dispatch
  path; either step panicking on weird input is a regression.
- `JwtValidationConfig` is fully populated from the fuzz input so the
  harness exercises the issuer/audience/tenant/kid branches as well as
  the signature path.
- The `ranking` harness clamps `confidence` to `[0, 1]` and forces
  `source_quality` to a finite value before calling the rankers, then
  asserts that the returned scores are finite. The clamp is needed
  because `f32::clamp` propagates `NaN`; the assertion then catches
  any non-finite score that the rankers produce.
