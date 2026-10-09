//! Harness for the black-box end-to-end tests.
//!
//! The services are started from their compiled binaries (built on demand
//! with `cargo build`, or taken from `DASH_E2E_BIN_DIR`), on ephemeral
//! loopback ports, with temp directories and freshly generated secrets.
//! Every child process is owned by a [`Proc`] whose `Drop` kills and reaps it.

pub mod http;
pub mod jwt;
pub mod leader;
pub mod proc;
pub mod stack;

pub use http::{Client, Resp};
pub use leader::LeaderState;
pub use proc::{Proc, bin_path, free_port, random_secret};
pub use stack::{Stack, StackOpts};

use serde_json::{Value, json};

/// Build an ingest body: one claim and `n_evidence` supporting evidence
/// items (`<claim>-e0` ...), no edges.
pub fn bundle(tenant: &str, claim_id: &str, text: &str, n_evidence: usize) -> Value {
    let evidence: Vec<Value> = (0..n_evidence)
        .map(|i| {
            json!({
                "evidence_id": format!("{claim_id}-e{i}"),
                "claim_id": claim_id,
                "source_id": format!("src://{claim_id}/{i}"),
                "stance": "supports",
                "source_quality": 0.9,
            })
        })
        .collect();
    json!({
        "claim": {
            "claim_id": claim_id,
            "tenant_id": tenant,
            "canonical_text": text,
            "confidence": 0.9,
        },
        "evidence": evidence,
        "edges": [],
    })
}

/// Evidence item with an explicit stance.
pub fn evidence(claim_id: &str, id: &str, stance: &str) -> Value {
    json!({
        "evidence_id": id,
        "claim_id": claim_id,
        "source_id": format!("src://{id}"),
        "stance": stance,
        "source_quality": 0.9,
    })
}
