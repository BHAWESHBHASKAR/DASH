//! Scenario 2: ingest on the leader, replicate, retrieve on the follower.

use std::collections::BTreeMap;
use std::time::Duration;

use dash_e2e::*;
use serde_json::{Value, json};

fn edge(id: &str, from: &str, to: &str, rel: &str) -> Value {
    json!({"edge_id": id, "from_claim_id": from, "to_claim_id": to, "relation": rel, "strength": 0.8})
}

struct Results(BTreeMap<String, Value>);

impl std::ops::Index<&str> for Results {
    type Output = Value;
    fn index(&self, id: &str) -> &Value {
        self.0
            .get(id)
            .unwrap_or_else(|| panic!("claim {id} missing from results; have {:?}", self.0.keys().collect::<Vec<_>>()))
    }
}

impl Results {
    fn contains_key(&self, id: &str) -> bool {
        self.0.contains_key(id)
    }
}

fn by_id(results: Vec<Value>) -> Results {
    Results(
        results
            .into_iter()
            .map(|r| (r["claim_id"].as_str().unwrap().to_string(), r))
            .collect(),
    )
}

fn score(v: &Value) -> f64 {
    v["score"].as_f64().expect("score")
}

fn ingest_ok(s: &Stack, tenant: &str, body: &Value) {
    let r = s.ingest_as(tenant, body);
    assert_eq!(r.status, 200, "ingest failed: {}", r.body);
}

#[test]
fn citations_edges_and_stance_semantics_survive_replication() {
    let mut s = Stack::new(StackOpts::default());
    s.start_all();

    // Every claim with an edge has an edge-free twin (same text, same tenant,
    // so identical corpus statistics); only the edges can make scores differ.
    ingest_ok(&s, "tenant-a", &bundle("tenant-a", "x", "reactor safe", 1));
    ingest_ok(&s, "tenant-a", &bundle("tenant-a", "x2", "reactor safe", 1));
    let mut y = bundle("tenant-a", "y", "reactor unsafe", 1);
    y["edges"] = json!([edge("e-y-x", "y", "x", "contradicts")]);
    ingest_ok(&s, "tenant-a", &y);
    ingest_ok(&s, "tenant-a", &bundle("tenant-a", "y2", "reactor unsafe", 1));

    // Supports edge: q --supports--> p.
    ingest_ok(&s, "tenant-a", &bundle("tenant-a", "p", "pump operational", 1));
    ingest_ok(&s, "tenant-a", &bundle("tenant-a", "p2", "pump operational", 1));
    let mut q = bundle("tenant-a", "q", "pump verified", 1);
    q["edges"] = json!([edge("e-q-p", "q", "p", "supports")]);
    ingest_ok(&s, "tenant-a", &q);
    ingest_ok(&s, "tenant-a", &bundle("tenant-a", "q2", "pump verified", 1));

    // Evidence of every stance on one claim: exactly one citation per evidence.
    let mut m = bundle("tenant-a", "multi", "valve status report", 0);
    m["evidence"] = json!([
        evidence("multi", "m-e1", "supports"),
        evidence("multi", "m-e2", "supports"),
        evidence("multi", "m-e3", "contradicts"),
        evidence("multi", "m-e4", "neutral"),
    ]);
    ingest_ok(&s, "tenant-a", &m);

    s.wait_caught_up(Duration::from_secs(20));
    let a = by_id(s.retrieve("tenant-a", "reactor pump valve", 100));

    // Contradiction edge y -> x lowers the TARGET (x), never the author (y).
    assert_eq!(a["x"]["contradicts"], 1, "target x must count the incoming contradiction: {}", a["x"]);
    assert_eq!(a["y"]["contradicts"], 0, "author y must not be contradicted: {}", a["y"]);
    assert!(
        score(&a["x"]) < score(&a["x2"]),
        "contradicted target should score lower than its edge-free twin: {} vs {}",
        score(&a["x"]),
        score(&a["x2"])
    );
    assert!(
        (score(&a["y"]) - score(&a["y2"])).abs() < 1e-6,
        "edge author must be unaffected: {} vs {}",
        score(&a["y"]),
        score(&a["y2"])
    );
    // Supports edge q -> p benefits the target p, not the author q.
    assert!(
        score(&a["p"]) > score(&a["p2"]),
        "supported target should outscore its edge-free twin: {} vs {}",
        score(&a["p"]),
        score(&a["p2"])
    );
    assert_eq!(a["p"]["supports"], 2, "p: own evidence plus the incoming supports edge");
    assert!(
        (score(&a["q"]) - score(&a["q2"])).abs() < 1e-6,
        "supports-edge author must be unaffected: {} vs {}",
        score(&a["q"]),
        score(&a["q2"])
    );
    assert_eq!(a["p"]["contradicts"], 0);

    // Exactly one citation per evidence item, with the right stance tallies.
    let cites = a["multi"]["citations"].as_array().unwrap();
    let mut ids: Vec<&str> = cites.iter().map(|c| c["evidence_id"].as_str().unwrap()).collect();
    ids.sort();
    assert_eq!(ids, ["m-e1", "m-e2", "m-e3", "m-e4"], "citations: {cites:?}");
    assert_eq!(a["multi"]["supports"], 2);
    assert_eq!(a["multi"]["contradicts"], 1);
    for id in ["x", "y", "p", "q"] {
        assert_eq!(a[id]["citations"].as_array().unwrap().len(), 1, "claim {id}");
    }

    // Replicated edge is visible in the returned graph, direction preserved.
    let r = s.retrieve_as(
        "tenant-a",
        &json!({"tenant_id":"tenant-a","query":"reactor","top_k":10,"return_graph":true}),
    );
    let g = r.json()["graph"].clone();
    let edges = g["edges"].as_array().unwrap();
    assert!(
        edges.iter().any(|e| e["from_claim_id"] == "y" && e["to_claim_id"] == "x" && e["relation"] == "contradicts"),
        "graph edges: {edges:?}"
    );
}

#[test]
fn stance_mode_filters_contradicted_claims() {
    let mut s = Stack::new(StackOpts::default());
    s.start_all();
    let mut bad = bundle("tenant-a", "bad", "orbit stable", 0);
    bad["evidence"] = json!([
        evidence("bad", "bad-e1", "contradicts"),
        evidence("bad", "bad-e2", "contradicts"),
        evidence("bad", "bad-e3", "supports"),
    ]);
    ingest_ok(&s, "tenant-a", &bad);
    ingest_ok(&s, "tenant-a", &bundle("tenant-a", "good", "orbit stable", 2));
    s.wait_caught_up(Duration::from_secs(20));

    let ask = |mode: &str| {
        let r = s.retrieve_as(
            "tenant-a",
            &json!({"tenant_id":"tenant-a","query":"orbit stable","top_k":10,"stance_mode":mode}),
        );
        assert_eq!(r.status, 200, "{}", r.body);
        by_id(r.json()["results"].as_array().cloned().unwrap())
    };
    let balanced = ask("balanced");
    assert!(balanced.contains_key("bad") && balanced.contains_key("good"));
    assert!(
        score(&balanced["bad"]) < score(&balanced["good"]),
        "contradicted claim should score lower in balanced mode"
    );
    let support_only = ask("support_only");
    assert!(support_only.contains_key("good"));
    assert!(
        !support_only.contains_key("bad"),
        "support_only must drop claims with more contradicting than supporting evidence"
    );
    let r = s.retrieve_as(
        "tenant-a",
        &json!({"tenant_id":"tenant-a","query":"orbit","stance_mode":"bogus"}),
    );
    assert_eq!(r.status, 400, "unknown stance_mode must be rejected");
}

#[test]
fn time_range_filters_by_event_time() {
    let mut s = Stack::new(StackOpts::default());
    s.start_all();
    for (id, t) in [("t1", 1_000_000), ("t2", 2_000_000), ("t3", 3_000_000)] {
        let mut b = bundle("tenant-a", id, "temporal claim", 1);
        b["claim"]["event_time_unix"] = json!(t);
        ingest_ok(&s, "tenant-a", &b);
    }
    s.wait_caught_up(Duration::from_secs(20));
    let window = |from: Option<i64>, to: Option<i64>| -> Vec<String> {
        let mut tr = serde_json::Map::new();
        if let Some(f) = from {
            tr.insert("from_unix".into(), json!(f));
        }
        if let Some(t) = to {
            tr.insert("to_unix".into(), json!(t));
        }
        let r = s.retrieve_as(
            "tenant-a",
            &json!({"tenant_id":"tenant-a","query":"temporal","top_k":10,"time_range":tr}),
        );
        assert_eq!(r.status, 200, "{}", r.body);
        let mut v: Vec<String> = r.json()["results"]
            .as_array()
            .unwrap()
            .iter()
            .map(|x| x["claim_id"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };
    assert_eq!(window(Some(1_500_000), Some(2_500_000)), ["t2"]);
    assert_eq!(window(Some(1_000_000), Some(2_000_000)), ["t1", "t2"], "bounds are inclusive");
    assert_eq!(window(Some(2_500_000), None), ["t3"]);
    assert_eq!(window(None, Some(1_500_000)), ["t1"]);
    assert!(window(Some(5_000_000), Some(6_000_000)).is_empty());
    let r = s.retrieve_as(
        "tenant-a",
        &json!({"tenant_id":"tenant-a","query":"temporal","time_range":{"from_unix":9,"to_unix":1}}),
    );
    assert_eq!(r.status, 400, "inverted range is a client error");
}

#[test]
fn non_ascii_text_round_trips_through_post_and_get() {
    let mut s = Stack::new(StackOpts::default());
    s.start_all();
    let texts = [
        ("u1", "café 日本語 résumé"),
        ("u2", "emoji 😀 naïve Ωmega"),
        ("u3", "quotes \"and\" back\\slash and\ttab"),
    ];
    for (id, text) in texts {
        let mut b = bundle("tenant-a", id, text, 1);
        b["evidence"][0]["source_id"] = json!(format!("src://日本語/é/{id}"));
        ingest_ok(&s, "tenant-a", &b);
    }
    s.wait_caught_up(Duration::from_secs(20));

    let post = by_id(s.retrieve("tenant-a", "café 日本語", 10));
    for (id, text) in texts {
        assert_eq!(post[id]["canonical_text"], text, "POST round trip of {id}");
        assert_eq!(
            post[id]["citations"][0]["source_id"],
            format!("src://日本語/é/{id}"),
            "citation source_id of {id}"
        );
    }

    let key = s.rk("tenant-a").1;
    let r = s.rc().get(
        "/v1/retrieve?tenant_id=tenant-a&query=caf%C3%A9+%E6%97%A5%E6%9C%AC%E8%AA%9E&top_k=10",
        &[("x-api-key", &key)],
    );
    assert_eq!(r.status, 200, "GET with percent-encoded UTF-8: {}", r.body);
    let get = by_id(r.json()["results"].as_array().cloned().unwrap());
    for (id, text) in texts {
        assert_eq!(get[id]["canonical_text"], text, "GET round trip of {id}");
    }
    // Raw (unencoded) UTF-8 bytes in the query string must not crash anything.
    let r = s
        .rc()
        .request(
            "GET",
            "/v1/retrieve?tenant_id=tenant-a&query=café日本語&top_k=3",
            &[("x-api-key", &key)],
            None,
        )
        .expect("raw UTF-8 in target");
    assert!(r.status == 200 || r.status == 400, "raw UTF-8 query -> {} {}", r.status, r.body);
    // Retrieval stays healthy and the ingestion side agrees after a restart
    // replays the WAL.
    s.restart_ingest(false);
    s.restart_retrieval(false);
    s.wait_caught_up(Duration::from_secs(20));
    let again = by_id(s.retrieve("tenant-a", "café", 10));
    for (id, text) in texts {
        assert_eq!(again[id]["canonical_text"], text, "after restart: {id}");
    }
}
