//! Parsing of the leader's replication export frame into plain id sets.

use std::collections::{BTreeMap, BTreeSet};

/// Ids found in a replication export frame.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct LeaderState {
    pub generation: String,
    /// claim_id -> (tenant, text)
    pub claims: BTreeMap<String, (String, String)>,
    /// evidence_id -> claim_id
    pub evidence: BTreeMap<String, String>,
    /// evidence_id -> number of E records carrying it (duplicates show here).
    pub evidence_lines: BTreeMap<String, usize>,
    /// The same count per section: `[snapshot, wal]`. While a checkpoint
    /// is pending, the snapshot section is the base snapshot followed by
    /// the closed WAL files it replays.
    pub evidence_lines_by_section: BTreeMap<String, [usize; 2]>,
    /// (from, to, relation)
    pub edges: BTreeSet<(String, String, String)>,
    /// Number of snapshot + WAL record lines.
    pub records: usize,
}

/// The comparable content of a [`LeaderState`] (no record count, no
/// generation: those legitimately change across checkpoints).
pub type Content = (
    BTreeMap<String, (String, String)>,
    BTreeMap<String, String>,
    BTreeSet<(String, String, String)>,
);

impl LeaderState {
    pub fn parse(body: &str) -> LeaderState {
        let mut st = LeaderState::default();
        let mut in_records = false;
        let mut section = 0usize;
        for line in body.lines() {
            match line {
                "SNAPSHOT" | "WAL" => {
                    in_records = true;
                    section = usize::from(line == "WAL");
                    continue;
                }
                _ => {}
            }
            if let Some(g) = line.strip_prefix("generation=") {
                st.generation = g.to_string();
            }
            if !in_records || line.is_empty() {
                continue;
            }
            st.records += 1;
            let f: Vec<&str> = line.split('\t').collect();
            match f[0].chars().next() {
                Some('C') if f.len() > 3 => {
                    st.claims
                        .insert(f[1].to_string(), (f[2].to_string(), f[3].to_string()));
                }
                Some('E') if f.len() > 2 => {
                    st.evidence.insert(f[1].to_string(), f[2].to_string());
                    *st.evidence_lines.entry(f[1].to_string()).or_default() += 1;
                    st.evidence_lines_by_section
                        .entry(f[1].to_string())
                        .or_default()[section] += 1;
                }
                Some('G') if f.len() > 4 => {
                    st.edges
                        .insert((f[2].to_string(), f[3].to_string(), f[4].to_string()));
                }
                _ => {}
            }
        }
        st
    }

    pub fn content(&self) -> Content {
        (
            self.claims.clone(),
            self.evidence.clone(),
            self.edges.clone(),
        )
    }
}
