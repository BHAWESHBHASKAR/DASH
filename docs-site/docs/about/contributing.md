# Contributing to DASH

Thank you for your interest in contributing to DASH! This guide will help you get started. It is the **public-docs version** of the in-tree `CONTRIBUTING.md`; the canonical version lives at the repository root.

## Table of contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting started](#getting-started)
3. [Development workflow](#development-workflow)
4. [Project structure](#project-structure)
5. [Good first issues](#good-first-issues)
6. [Feature proposals](#feature-proposals)
7. [Code standards](#code-standards)
8. [Testing](#testing)
9. [Documentation](#documentation)
10. [Review process](#review-process)

## Code of Conduct

Be respectful, constructive, and collaborative. We're building this in the open — help create an environment where everyone can contribute their best work. The full text is at [`CODE_OF_CONDUCT.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/CODE_OF_CONDUCT.md).

## Getting started

### Prerequisites

- **Rust 1.83+** — `rustup install stable`
- **Git** — for version control
- Familiarity with Rust fundamentals (ownership, traits, error handling)

### Fork and clone

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/<your-username>/DASH.git
cd DASH

# Add upstream remote
git remote add upstream https://github.com/BHAWESHBHASKAR/DASH.git
```

### Build and test

```bash
# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Run linter (clippy)
cargo clippy --workspace -- -D warnings

# Run the full CI pipeline
./scripts/ci.sh
```

The **CI pipeline** runs (in this order):

1. `cargo fmt --check` — formatting gate.
2. `cargo clippy --workspace --all-targets -- -D warnings` — lints.
3. `cargo test --workspace` — unit + integration tests.
4. `cargo audit --deny warnings` — RustSec advisory check.
5. `cargo run -p benchmark-smoke --bin perf_bench --release -- --all` — regression guard against the published baselines.
6. `trivy image` — image scan (on tagged releases only).
7. `mkdocs build --strict` — docs site builds cleanly.

A failure on any step blocks the PR.

## Development workflow

### 1. Create a feature branch

```bash
git checkout -b feature/add-hnsw-index
```

### 2. Make changes

- Follow the [Code standards](#code-standards).
- Write tests for new functionality.
- Run `cargo clippy` and `cargo test` locally.

### 3. Commit

Use clear, imperative commit messages:

```text
Add HNSW vector index integration

- Integrate usearch crate for ANN search
- Add IndexBuilder trait for pluggable indexes
- Implement IndexedStore with vector retrieval path
- Add benchmark comparing linear scan vs HNSW
```

### 4. Push and open a PR

```bash
git push origin feature/add-hnsw-index
```

Open a pull request on GitHub. Reference any related issues (e.g. `Closes #42`).

### 5. Code review

- Respond to feedback constructively.
- Make requested changes in new commits (don't force-push during review).
- Once approved, a maintainer will merge your PR.

## Project structure

```text
DASH/
├── pkg/                   # Core library crates
│   ├── schema/            # Domain types: Claim, Evidence, Edge
│   ├── ranking/           # Scoring and ranking logic
│   ├── graph/             # Edge summarization and traversal
│   ├── embeddings/        # EmbeddingProvider trait + impls
│   ├── auth/              # JWT verification
│   └── store/             # In-memory + redb-backed store, WAL, ANN
├── services/              # Service binaries
│   ├── ingestion/         # Ingest API and pipeline
│   ├── retrieval/         # Retrieval API and query execution
│   ├── indexer/           # Data tiering (hot/warm/cold)
│   ├── metadata-router/   # Tenant/entity sharding
│   └── control-plane/     # Tenant + key management
├── tests/                 # Integration + benchmark tests
│   ├── store/             # Store-level integration tests
│   ├── retrieval/         # Retrieval HTTP + OpenAI compatibility tests
│   ├── auth/              # Auth integration tests
│   └── benchmarks/        # perf_bench binary
├── tools/                 # CLI utilities (dash-admin, dash-wal-replay, …)
├── fuzz/                  # cargo-fuzz harnesses
├── sdks/                  # First-party SDKs
│   ├── python/            # dash-py
│   ├── go/                # dash-go
│   ├── typescript/        # dash-ts
│   ├── java/              # dash-java
│   └── csharp/            # Dash.NET
├── deploy/                # Docker, docker-compose, k8s, helm
├── docs/                  # Architecture and execution docs
└── docs-site/             # mkdocs-material public docs
```

**Crate isolation.** Each crate is independently testable. You can work on `pkg/ranking` without deep knowledge of `pkg/store`.

## Good first issues

Looking for a place to start? Check issues labeled [`good-first-issue`](https://github.com/BHAWESHBHASKAR/DASH/issues?q=is%3Aopen+label%3A%22good+first+issue%22).

### Low complexity

- Replace hand-rolled JSON with `serde` ([migration guide](https://serde.rs/))
- Add `Content-Length` limit to HTTP transport (prevent OOM)
- Add TCP read timeout to prevent slow-loris DoS
- Pin Dockerfile rust version for reproducible builds
- Add `fsync` to WAL append for durability

### Medium complexity

- Migrate HTTP transport from blocking TCP to `tokio` + `axum`
- Implement BM25 lexical scoring
- Add evidence deduplication to `InMemoryStore`
- Create Python SDK bindings
- Write a tutorial: "Ingest and retrieve your first claims"

### High complexity (but high impact!)

- Integrate ANN vector index (HNSW via `usearch` or `hora`)
- Build pluggable embedding model API
- Implement multi-hop graph traversal
- Design segment storage with object store backend (S3)

## Feature proposals

### Before writing code

1. **Check existing issues** — maybe it's already planned or in progress.
2. **Open a discussion** — describe the problem, proposed solution, and alternatives.
3. **Get feedback** — maintainers will help refine scope and approach.

### RFC process (for large features)

For significant changes (new indexes, distributed protocols, API changes):

1. Create an RFC document in `docs/rfcs/NNNN-feature-name.md`.
2. Open a PR with `[RFC]` prefix.
3. Community discussion and iteration.
4. Once approved, implement in follow-up PRs.

## Code standards

### Rust style

- Follow [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/).
- Use `cargo fmt` (enforced in CI).
- Pass `cargo clippy --workspace -- -D warnings` (enforced in CI).
- Prefer explicit error types over `unwrap()` in library code.
- Document public APIs with `///` doc comments.

### Naming conventions

- **Types:** `PascalCase` (e.g., `Claim`, `InMemoryStore`).
- **Functions/variables:** `snake_case` (e.g., `ingest_bundle`, `claim_id`).
- **Constants:** `UPPER_SNAKE_CASE` (e.g., `MAX_BODY_SIZE`, `WAL_HEADER`).

### Error handling

- Use `Result<T, E>` with specific error types.
- Implement `From<SourceError>` for error conversions.
- Avoid `panic!` in library code (services may panic on invariant violations).

### Comments

- Explain **why**, not **what** (code shows what).
- Flag TODOs with context: `// TODO(username): reason`.
- Use `SAFETY:` comments for `unsafe` blocks.

## Testing

### Unit tests

Place tests in the same file as the code under test:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ingest_bundle_writes_wal_entries() {
        let mut store = InMemoryStore::new();
        let claim = claim("c1", "test claim");
        store.ingest_bundle(claim, vec![], vec![]).unwrap();
        assert_eq!(store.claims_len(), 1);
    }
}
```

### Integration tests

- Place in `tests/` directory (e.g. `tests/store/`, `tests/retrieval/`).
- Use `cargo test --workspace` to run all tests.
- Integration tests that need a running service should be marked with `#[ignore]` and run in CI's `integration` job.

### Benchmark tests

- Add profiles to `tests/benchmarks/profiles/`.
- Run with `cargo run --release -p benchmark-smoke --bin perf_bench --release -- --all`.
- CI enforces regression guards: a > 10% regression on p95 latency for any scenario blocks the PR.

## Documentation

### Code documentation

```rust
/// Ingests a claim bundle (claim + evidence + edges) into the store.
///
/// # Arguments
/// * `claim` — The claim to ingest
/// * `evidence` — Supporting evidence for the claim
/// * `edges` — Graph relationships to other claims
///
/// # Errors
/// Returns `StoreError::Validation` if claim/evidence/edges fail validation.
pub fn ingest_bundle(
    &mut self,
    claim: Claim,
    evidence: Vec<Evidence>,
    edges: Vec<ClaimEdge>,
) -> Result<(), StoreError> {
    // ...
}
```

### Architecture docs

- For architectural decisions, add an ADR under `docs/rfcs/NNNN-title.md`. The five published ADRs (`ADR-001` through `ADR-005`) are the template.
- For execution details, add to `docs/execution/`.
- For public docs (this site), edit under `docs-site/docs/`. The site rebuilds on every push to `main`.

## Review process

### What reviewers look for

- **Correctness:** Does it work? Are edge cases handled?
- **Tests:** Are there tests? Do they cover error paths?
- **Clarity:** Is the code readable? Are complex parts commented?
- **Performance:** Any obvious performance footguns?
- **API design:** Does it fit the existing architecture?

### Response time

- Initial review: within 48 hours.
- Follow-up reviews: within 24 hours.
- **Note:** All maintainers are volunteers — please be patient!

### Merge criteria

- [ ] CI passes (clippy + tests + benchmarks).
- [ ] At least one approving review.
- [ ] All review feedback addressed.
- [ ] Documentation updated (if API changed).
- [ ] Changelog entry added (if user-facing).

## Communication

- **GitHub Issues:** Bug reports, feature requests.
- **GitHub Discussions:** Design questions, general questions.
- **Pull Requests:** Code review and technical discussion.
- **Discord:** (Coming soon) Real-time discussion.

## Recognition

Contributors are recognized in:

- `AUTHORS` file (auto-generated from git log).
- Release notes for significant contributions.
- Special thanks section for major features.

## Questions?

- Check the [Concepts](../concepts/index.md) section of the docs for system design.
- Check [`EME_ARCHITECTURE.md`](https://github.com/BHAWESHBHASKAR/DASH/blob/main/EME_ARCHITECTURE.md) for the original EME architecture doc.
- Open a [GitHub Discussion](https://github.com/BHAWESHBHASKAR/DASH/discussions) for questions.
- Tag maintainers in issues if blocked.

**Thank you for contributing to DASH!**
