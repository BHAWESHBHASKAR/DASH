# Contributing to Dash

Thank you for your interest in contributing to Dash! This guide will help you get started.

---

## Table of Contents

1. [Code of Conduct](#code-of-conduct)
2. [Getting Started](#getting-started)
3. [Development Workflow](#development-workflow)
4. [Project Structure](#project-structure)
5. [Good First Issues](#good-first-issues)
6. [Feature Proposals](#feature-proposals)
7. [Code Standards](#code-standards)
8. [Testing](#testing)
9. [Documentation](#documentation)
10. [Review Process](#review-process)

---

## Code of Conduct

Be respectful, constructive, and collaborative. We're building this in the open — help create an environment where everyone can contribute their best work.

---

## Getting Started

### Prerequisites

- **Rust 1.83+** — `rustup install stable`
- **Git** — for version control
- Familiarity with Rust fundamentals (ownership, traits, error handling)

### Fork and Clone

```bash
# Fork the repository on GitHub, then clone your fork
git clone https://github.com/<your-username>/dash.git
cd dash

# Add upstream remote
git remote add upstream https://github.com/<org>/dash.git
```

### Build and Test

```bash
# Build all crates
cargo build --workspace

# Run tests
cargo test --workspace

# Run linter (clippy)
cargo clippy --workspace -- -D warnings

# Run full CI pipeline
./scripts/ci.sh
```

---

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/add-hnsw-index
```

### 2. Make Changes

- Follow the [Code Standards](#code-standards)
- Write tests for new functionality
- Run `cargo clippy` and `cargo test` locally

### 3. Commit

Use clear, imperative commit messages:

```
Add HNSW vector index integration

- Integrate usearch crate for ANN search
- Add IndexBuilder trait for pluggable indexes
- Implement IndexedStore with vector retrieval path
- Add benchmark comparing linear scan vs HNSW
```

### 4. Push and Open PR

```bash
git push origin feature/add-hnsw-index
```

Open a pull request on GitHub. Reference any related issues (e.g., `Closes #42`).

### 5. Code Review

- Respond to feedback constructively
- Make requested changes in new commits (don't force-push during review)
- Once approved, a maintainer will merge your PR

### 6. Closing Issues and Status Claims

**An issue is closed only when the fix is merged with a regression test that fails on the old code; plan status lines must link to evidence.**

In practice:

- A pull request that closes an issue (including a row in `docs/plans/2026-10-09-issue-register.md`) must add or change a test that fails without the fix, and the PR description names that test and the CI run that executed it.
- A plan, roadmap or changelog line may say "done" or "fixed" only if it links to evidence: the test (`path::name`), a CI job, or a drill record. No link means not done.
- Capability claims in `README.md` are listed in `docs/claims-ledger.md` with the test that proves them. Run `scripts/check_claims_ledger.sh` after changing either file; it fails if a referenced test or file does not exist, or if the README test count is stale.
- Changes to security, storage format or replication need a second reviewer and an update to `docs/threat-model.md` (or an ADR line) when behavior changes.
- Configuration and API reference pages must list only what the code reads and serves. When you add an environment variable or a route, update `docs-site/docs/reference/configuration.md` or `api.md` in the same PR.

---

## Project Structure

```
dash/
├── pkg/                   # Core library crates
│   ├── schema/            # Domain types: Claim, Evidence, Edge
│   ├── ranking/           # Scoring and ranking logic
│   ├── graph/             # Edge summarization and traversal
│   └── store/             # In-memory store + WAL persistence
├── services/              # Service binaries
│   ├── ingestion/         # Ingest API and pipeline
│   ├── retrieval/         # Retrieval API and query execution
│   ├── indexer/           # Data tiering (hot/warm/cold)
│   └── metadata-router/   # Tenant/entity sharding
├── tests/benchmarks/      # Performance benchmarks
├── tools/buddy-chat/      # Agent coordination CLI
├── docs/                  # Architecture and execution docs
├── deploy/                # Docker, systemd, packaging
└── scripts/               # CI and release scripts
```

**Crate Isolation:**  
Each crate is independently testable. You can work on `pkg/ranking` without deep knowledge of `pkg/store`.

---

## Good First Issues

Looking for a place to start? Check out issues labeled `good-first-issue`:

### Low Complexity
- Replace hand-rolled JSON with `serde` ([migration guide](https://serde.rs/))
- Add Content-Length limit to HTTP transport (prevent OOM)
- Add TCP read timeout to prevent slow-loris DoS
- Pin Dockerfile rust version for reproducible builds
- Add `fsync` to WAL append for durability

### Medium Complexity
- Migrate HTTP transport from blocking TCP to `tokio` + `axum`
- Implement BM25 lexical scoring
- Add evidence deduplication to `InMemoryStore`
- Create Python SDK bindings
- Write tutorial: \"Ingest and retrieve your first claims\"

### High Complexity (but high impact!)
- Integrate ANN vector index (HNSW via `usearch` or `hora`)
- Build pluggable embedding model API
- Implement multi-hop graph traversal
- Design segment storage with object store backend (S3)

---

## Feature Proposals

### Before Writing Code

1. **Check existing issues** — maybe it's already planned or in progress
2. **Open a discussion** — describe the problem, proposed solution, and alternatives
3. **Get feedback** — maintainers will help refine scope and approach

### RFC Process (for large features)

For significant changes (new indexes, distributed protocols, API changes):

1. Create an RFC document in `docs/rfcs/NNNN-feature-name.md`
2. Open a PR with `[RFC]` prefix
3. Community discussion and iteration
4. Once approved, implement in follow-up PRs

---

## Code Standards

### Rust Style

- Follow [Rust API Guidelines](https://rust-lang.github.io/api-guidelines/)
- Use `cargo fmt` (enforced in CI)
- Pass `cargo clippy --workspace -- -D warnings` (enforced in CI)
- Prefer explicit error types over `unwrap()` in library code
- Document public APIs with `///` doc comments

### Naming Conventions

- **Types:** `PascalCase` (e.g., `Claim`, `InMemoryStore`)
- **Functions/variables:** `snake_case` (e.g., `ingest_bundle`, `claim_id`)
- **Constants:** `UPPER_SNAKE_CASE` (e.g., `MAX_BODY_SIZE`, `WAL_HEADER`)

### Error Handling

- Use `Result<T, E>` with specific error types
- Implement `From<SourceError>` for error conversions
- Avoid `panic!` in library code (services may panic on invariant violations)

### Comments

- Explain **why**, not **what** (code shows what)
- Flag TODOs with context: `// TODO(username): reason`
- Use `SAFETY:` comments for `unsafe` blocks

---

## Testing

### Unit Tests

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

### Integration Tests

- Place in `tests/` directory (e.g., `tests/benchmarks/`)
- Use `cargo test --workspace` to run all tests

### Benchmark Tests

- Add profiles to `tests/benchmarks/profiles/`
- Run with `cargo run --release -p benchmark-smoke -- --profile your-profile`
- CI enforces regression guards

---

## Documentation

### Code Documentation

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

### Architecture Docs

- For architectural decisions, update `docs/architecture/eme-architecture.md`
- For execution details, add to `docs/execution/`

---

## Review Process

### What Reviewers Look For

- **Correctness:** Does it work? Are edge cases handled?
- **Tests:** Are there tests? Do they cover error paths?
- **Clarity:** Is the code readable? Are complex parts commented?
- **Performance:** Any obvious performance footguns?
- **API design:** Does it fit the existing architecture?

### Response Time

- Initial review: within 48 hours
- Follow-up reviews: within 24 hours
- **Note:** All maintainers are volunteers — please be patient!

### Merge Criteria

- [ ] CI passes (clippy + tests + benchmarks)
- [ ] At least one approving review
- [ ] All review feedback addressed
- [ ] Documentation updated (if API changed)
- [ ] Changelog entry added (if user-facing)

---

## Communication

- **GitHub Issues:** Bug reports, feature requests
- **GitHub Discussions:** Design questions, general questions
- **Pull Requests:** Code review and technical discussion
- **Discord:** (Coming soon) Real-time discussion

---

## Recognition

Contributors are recognized in:
- `AUTHORS` file (auto-generated from git log)
- Release notes for significant contributions
- Special thanks section for major features

---

## Questions?

- Check [`docs/architecture/eme-architecture.md`](docs/architecture/eme-architecture.md) for system design
- Check [`feasibility.md`](feasibility.md) for roadmap and priorities
- Open a [GitHub Discussion](https://github.com/your-org/dash/discussions) for questions
- Tag maintainers in issues if blocked

**Thank you for contributing to Dash!** 🚀
