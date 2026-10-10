//! Scenario 3: durability without duplicates across clean restarts,
//! idempotent re-ingest and edit-as-update.

use std::collections::BTreeMap;
use std::time::Duration;

use dash_e2e::*;
use serde_json::{Value, json};

const T: &str = "tenant-a";
/// One word from every text this file ingests (claim bundles, the batch,
/// both documents), so a retrieve with it lists every claim through lexical
/// matching. A query that matches nothing returns nothing.
const ALL: &str = "claim turbines blades bridge tolls";
const N: usize = 25;

fn edge(id: &str, from: &str, to: &str) -> Value {
    json!({"edge_id": id, "from_claim_id": from, "to_claim_id": to, "relation": "supports", "strength": 0.7})
}

fn bundles() -> Vec<Value> {
    (0..N)
        .map(|i| {
            let mut b = bundle(
                T,
                &format!("d{i}"),
                &format!("durable claim number {i} about turbines"),
                2,
            );
            if i > 0 {
                b["edges"] = json!([edge(
                    &format!("g{i}"),
                    &format!("d{i}"),
                    &format!("d{}", i - 1)
                )]);
            }
            b
        })
        .collect()
}

fn api_state(s: &Stack) -> BTreeMap<String, Vec<String>> {
    s.claim_evidence_map(T, ALL, 1000)
}

fn assert_no_duplicate_evidence(state: &BTreeMap<String, Vec<String>>) {
    for (claim, ev) in state {
        let mut uniq = ev.clone();
        uniq.dedup();
        assert_eq!(&uniq, ev, "claim {claim} has duplicated evidence: {ev:?}");
    }
}

#[test]
fn restarts_never_change_state_and_reingest_is_a_noop() {
    let mut s = Stack::new(StackOpts::default());
    s.start_all();
    let bundles = bundles();
    for b in &bundles {
        let r = s.ingest_as(T, b);
        assert_eq!(r.status, 200, "{}", r.body);
    }
    // A batch (with commit id) and a document on top.
    let batch = json!({
        "commit_id": "batch-1",
        "items": [bundle(T, "bt0", "batch claim zero", 2), bundle(T, "bt1", "batch claim one", 2)],
    });
    let k = s.ik(T).1;
    let r = s
        .ic()
        .post_json("/v1/ingest/batch", &[("x-api-key", &k)], &batch);
    assert_eq!(r.status, 200, "batch: {}", r.body);
    let doc = json!({"tenant_id":T,"document_id":"docA","source_id":"src://docA","mime_type":"text/plain",
        "text":"Turbines spin quickly under load. Blades are inspected every week."});
    let r = s
        .ic()
        .post_json("/v1/ingest/document", &[("x-api-key", &k)], &doc);
    assert_eq!(r.status, 200, "document: {}", r.body);

    s.wait_caught_up(Duration::from_secs(30));
    let base_api = api_state(&s);
    let base_leader = s.leader_state();
    let (gen0, records0) = s.leader_position();
    assert_eq!(
        base_api.len(),
        N + 2 + 2,
        "claims visible through retrieval"
    );
    for i in 0..N {
        assert_eq!(base_api[&format!("d{i}")].len(), 2, "d{i} evidence count");
    }
    assert_no_duplicate_evidence(&base_api);
    assert_eq!(base_leader.claims.len(), base_api.len());
    assert_eq!(base_leader.evidence.len(), N * 2 + 4 + 2);

    // Identical re-ingest of everything: accepted and a no-op for the WAL.
    for b in &bundles {
        assert_eq!(s.ingest_as(T, b).status, 200);
    }
    let r = s
        .ic()
        .post_json("/v1/ingest/batch", &[("x-api-key", &k)], &batch);
    assert_eq!(r.status, 200, "batch replay: {}", r.body);
    let r = s
        .ic()
        .post_json("/v1/ingest/document", &[("x-api-key", &k)], &doc);
    assert_eq!(r.status, 200, "document replay: {}", r.body);
    assert_eq!(
        r.json()["idempotent_replay"],
        true,
        "document replay must be flagged idempotent: {}",
        r.body
    );
    assert_eq!(
        s.leader_position(),
        (gen0, records0),
        "identical re-ingest grew the WAL"
    );

    // Five clean restarts of ingestion.
    for round in 1..=5 {
        s.restart_ingest(true);
        assert_eq!(
            s.leader_position(),
            (gen0, records0),
            "round {round}: WAL changed over an ingestion restart"
        );
        assert_eq!(
            s.leader_state().content(),
            base_leader.content(),
            "round {round}: leader content changed"
        );
        // The restarted leader must still recognise every bundle as already stored.
        for b in &bundles {
            assert_eq!(s.ingest_as(T, b).status, 200, "round {round}");
        }
        assert_eq!(
            s.leader_position(),
            (gen0, records0),
            "round {round}: re-ingest after restart wrote to the WAL (leader forgot its state)"
        );
        s.wait_caught_up(Duration::from_secs(30));
        assert_eq!(
            api_state(&s),
            base_api,
            "round {round}: retrieval view changed after ingestion restart"
        );
    }

    // Five clean restarts of retrieval.
    for round in 1..=5 {
        s.restart_retrieval(true);
        s.wait_caught_up(Duration::from_secs(30));
        let now = api_state(&s);
        assert_no_duplicate_evidence(&now);
        assert_eq!(
            now, base_api,
            "round {round}: retrieval state changed after its restart"
        );
    }
    let rep = s.retrieval_replication();
    assert!(
        rep["resyncs_total"].as_u64().unwrap_or(0) <= 1,
        "unexpected resyncs: {rep}"
    );
}

#[test]
fn edits_update_instead_of_dropping() {
    let mut s = Stack::new(StackOpts::default());
    s.start_all();
    let k = s.ik(T).1;

    // Claim edit through /v1/ingest: new text, one more evidence item.
    let v1 = bundle(T, "edit-me", "original wording of the claim", 2);
    assert_eq!(s.ingest_as(T, &v1).status, 200);
    let mut v2 = bundle(T, "edit-me", "revised wording of the claim", 3);
    v2["claim"]["confidence"] = json!(0.5);
    let r = s.ingest_as(T, &v2);
    assert_eq!(
        r.status, 200,
        "edit must be accepted as an update: {}",
        r.body
    );
    s.wait_caught_up(Duration::from_secs(20));
    let got = s.retrieve(T, ALL, 100);
    let c = got
        .iter()
        .find(|c| c["claim_id"] == "edit-me")
        .expect("edited claim vanished");
    assert_eq!(
        c["canonical_text"], "revised wording of the claim",
        "new text was dropped"
    );
    assert!(
        (c["claim_confidence"].as_f64().unwrap() - 0.5).abs() < 1e-6,
        "new confidence was dropped: {c}"
    );
    let mut ev: Vec<&str> = c["citations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["evidence_id"].as_str().unwrap())
        .collect();
    ev.sort();
    assert_eq!(
        ev,
        ["edit-me-e0", "edit-me-e1", "edit-me-e2"],
        "evidence must be upserted by id, not appended"
    );
    // Replaying the edit is a no-op.
    let (g, n) = s.leader_position();
    assert_eq!(s.ingest_as(T, &v2).status, 200);
    assert_eq!(
        s.leader_position(),
        (g, n),
        "replaying the edit wrote to the WAL"
    );

    // Document edit with the same number of sentences: the new text must land.
    let doc = |text: &str| json!({"tenant_id":T,"document_id":"docE","source_id":"src://docE","mime_type":"text/plain","text":text});
    let post = |text: &str| {
        s.ic()
            .post_json("/v1/ingest/document", &[("x-api-key", &k)], &doc(text))
    };
    let r1 = post("The bridge opened in May. The tolls are collected electronically.");
    assert_eq!(r1.status, 200, "{}", r1.body);
    let r2 = post("The bridge opened in June. The tolls are collected electronically.");
    assert_eq!(r2.status, 200, "{}", r2.body);
    assert_ne!(
        r2.json()["idempotent_replay"],
        true,
        "an edited document must not be treated as a replay: {}",
        r2.body
    );
    s.wait_caught_up(Duration::from_secs(20));
    let docs: Vec<Value> = s
        .retrieve(T, ALL, 100)
        .into_iter()
        .filter(|c| c["claim_id"].as_str().unwrap().contains(":docE:"))
        .collect();
    let texts: Vec<&str> = docs
        .iter()
        .map(|c| c["canonical_text"].as_str().unwrap())
        .collect();
    assert!(
        texts.contains(&"The bridge opened in June"),
        "edited sentence dropped: {texts:?}"
    );
    assert!(
        !texts.contains(&"The bridge opened in May"),
        "stale sentence still served: {texts:?}"
    );
    assert_eq!(
        docs.len(),
        2,
        "claim count changed on a same-size edit: {texts:?}"
    );
    for c in &docs {
        assert_eq!(
            c["citations"].as_array().unwrap().len(),
            1,
            "document claim evidence duplicated: {c}"
        );
    }
    // Survives restarts of both processes.
    s.restart_ingest(false);
    s.restart_retrieval(false);
    s.wait_caught_up(Duration::from_secs(20));
    let after = s.retrieve(T, ALL, 100);
    let june = after
        .iter()
        .filter(|c| c["canonical_text"] == "The bridge opened in June")
        .count();
    assert_eq!(june, 1);
    let c = after.iter().find(|c| c["claim_id"] == "edit-me").unwrap();
    assert_eq!(c["canonical_text"], "revised wording of the claim");
    assert_eq!(c["citations"].as_array().unwrap().len(), 3);
}
