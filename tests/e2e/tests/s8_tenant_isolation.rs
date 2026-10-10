//! Scenario 8: tenant isolation across ingestion, replication and retrieval.

use std::time::Duration;

use dash_e2e::*;
use serde_json::{Value, json};

/// A word every claim text in this file contains, so a retrieve with it lists
/// every claim through lexical matching (a query that matches nothing returns
/// nothing).
const ALL: &str = "statement";

fn edge(id: &str, from: &str, to: &str, rel: &str) -> Value {
    json!({"edge_id": id, "from_claim_id": from, "to_claim_id": to, "relation": rel, "strength": 0.9})
}

fn stack(tenants: &[&str]) -> Stack {
    let mut s = Stack::new(StackOpts {
        tenants: tenants.iter().map(|t| t.to_string()).collect(),
        ..Default::default()
    });
    s.start_all();
    s
}

#[test]
fn lookalike_tenant_ids_do_not_share_data() {
    let s = stack(&["a.b", "a_b", "a-b"]);
    let tenants = ["a.b", "a_b", "a-b"];
    for (i, t) in tenants.iter().enumerate() {
        let mut b = bundle(
            t,
            &format!("claim-{i}"),
            &format!("SECRET-{i} private statement of tenant {i}"),
            2,
        );
        // Evidence ids are namespaced by the claim id already; add an edge too.
        if i > 0 {
            b["edges"] = json!([edge(
                &format!("edge-{i}"),
                &format!("claim-{i}"),
                &format!("claim-{i}"),
                "refines"
            )]);
        }
        let r = s.ingest_as(t, &b);
        assert_eq!(r.status, 200, "{t}: {}", r.body);
    }
    s.wait_caught_up(Duration::from_secs(20));
    for (i, t) in tenants.iter().enumerate() {
        let results = s.retrieve(t, ALL, 100);
        let ids: Vec<&str> = results
            .iter()
            .map(|r| r["claim_id"].as_str().unwrap())
            .collect();
        assert_eq!(
            ids,
            [format!("claim-{i}")],
            "tenant {t} must see exactly its own claim"
        );
        let r = s.retrieve_as(t, &json!({"tenant_id": t, "query": "private statement", "top_k": 10, "return_graph": true}));
        assert_eq!(r.status, 200);
        for (j, other) in tenants.iter().enumerate() {
            if j != i {
                assert!(
                    !r.body.contains(&format!("SECRET-{j}"))
                        && !r.body.contains(&format!("claim-{j}")),
                    "tenant {t} response leaks tenant {}'s data: {}",
                    other,
                    r.body
                );
            }
        }
        // Each tenant's key is useless against its look-alikes.
        for (j, other) in tenants.iter().enumerate() {
            if j != i {
                let r = s.retrieve_as(
                    t,
                    &json!({"tenant_id": other, "query": "private", "top_k": 5}),
                );
                assert_eq!(r.status, 403, "key of {t} reading {other}: {}", r.body);
                let w = s.ingest_as(t, &bundle(other, "intruder", "written across tenants", 1));
                assert_eq!(w.status, 403, "key of {t} writing {other}: {}", w.body);
            }
        }
    }
    // Nothing from the failed cross-tenant writes landed anywhere.
    assert!(!s.leader_state().claims.contains_key("intruder"));
}

#[test]
fn id_conflicts_do_not_reveal_the_other_tenant() {
    let s = stack(&["tenant-a", "tenant-b"]);
    let r = s.ingest_as(
        "tenant-a",
        &bundle("tenant-a", "shared-id", "SECRET-A original text", 1),
    );
    assert_eq!(r.status, 200, "{}", r.body);

    // Same claim_id from another tenant.
    let r = s.ingest_as(
        "tenant-b",
        &bundle("tenant-b", "shared-id", "tenant b text", 1),
    );
    assert_eq!(
        r.status, 409,
        "claim_id reuse across tenants must conflict: {}",
        r.body
    );
    for leak in ["tenant-a", "SECRET-A", "original text"] {
        assert!(
            !r.body.contains(leak),
            "conflict message names the other tenant ({leak}): {}",
            r.body
        );
    }
    // Same evidence_id from another tenant on its own claim.
    let mut b = bundle("tenant-b", "b-own", "tenant b own claim", 0);
    b["evidence"] = json!([evidence("b-own", "shared-id-e0", "supports")]);
    let r = s.ingest_as("tenant-b", &b);
    if r.status != 200 {
        for leak in ["tenant-a", "SECRET-A", "shared-id"] {
            assert!(
                !r.body.contains(leak),
                "evidence conflict leaks ({leak}): {}",
                r.body
            );
        }
    }
    // Batch path.
    let k = s.ik("tenant-b").1;
    let r = s.ic().post_json(
        "/v1/ingest/batch",
        &[("x-api-key", &k)],
        &json!({"commit_id": "bb", "items": [bundle("tenant-b", "shared-id", "tenant b text", 1)]}),
    );
    assert_ne!(
        r.status, 200,
        "batch overwrote another tenant's claim: {}",
        r.body
    );
    for leak in ["tenant-a", "SECRET-A"] {
        assert!(
            !r.body.contains(leak),
            "batch conflict leaks ({leak}): {}",
            r.body
        );
    }

    // The original claim is untouched.
    s.wait_caught_up(Duration::from_secs(20));
    let a = s.retrieve("tenant-a", ALL, 10);
    let c = a
        .iter()
        .find(|c| c["claim_id"] == "shared-id")
        .expect("tenant-a lost its claim");
    assert_eq!(c["canonical_text"], "SECRET-A original text");
    assert!(
        s.retrieve("tenant-b", ALL, 10)
            .iter()
            .all(|c| c["claim_id"] != "shared-id"),
        "tenant-b sees tenant-a's claim"
    );
}

#[test]
fn edges_cannot_reach_across_tenants() {
    let s = stack(&["tenant-a", "tenant-b"]);
    let r = s.ingest_as(
        "tenant-a",
        &bundle("tenant-a", "a-victim", "SECRET-A the vault is secure", 1),
    );
    assert_eq!(r.status, 200);
    s.wait_caught_up(Duration::from_secs(20));
    let a_before = s.retrieve("tenant-a", ALL, 10);
    let score_before = a_before[0]["score"].as_f64().unwrap();

    // tenant-b tries to attack and to read a-victim through edges.
    let mut atk = bundle("tenant-b", "b-attacker", "the vault is not secure", 1);
    atk["edges"] = json!([
        edge("b-to-a-contra", "b-attacker", "a-victim", "contradicts"),
        edge("b-to-a-support", "b-attacker", "a-victim", "supports"),
    ]);
    let r = s.ingest_as("tenant-b", &atk);
    println!("cross-tenant edge ingest: {} {}", r.status, r.body);
    for leak in ["tenant-a", "SECRET-A"] {
        assert!(
            !r.body.contains(leak),
            "edge response leaks {leak}: {}",
            r.body
        );
    }
    s.wait_caught_up(Duration::from_secs(20));

    let a_after = s.retrieve("tenant-a", ALL, 10);
    let victim = a_after
        .iter()
        .find(|c| c["claim_id"] == "a-victim")
        .unwrap();
    assert_eq!(
        victim["contradicts"], 0,
        "foreign contradiction edge affected tenant-a: {victim}"
    );
    assert_eq!(
        victim["supports"], 1,
        "foreign supports edge affected tenant-a: {victim}"
    );
    assert!(
        (victim["score"].as_f64().unwrap() - score_before).abs() < 1e-9,
        "tenant-a's score moved"
    );
    let r = s.retrieve_as(
        "tenant-b",
        &json!({"tenant_id":"tenant-b","query":"vault","top_k":10,"return_graph":true}),
    );
    assert_eq!(r.status, 200);
    assert!(
        !r.body.contains("SECRET-A") && !r.body.contains("the vault is secure"),
        "tenant-b's graph exposes tenant-a's claim text: {}",
        r.body
    );
    let a = s.retrieve_as(
        "tenant-a",
        &json!({"tenant_id":"tenant-a","query":"vault","top_k":10,"return_graph":true}),
    );
    assert!(
        !a.body.contains("b-attacker") && !a.body.contains("the vault is not secure"),
        "tenant-a's graph shows foreign claim: {}",
        a.body
    );
}

#[test]
fn a_tenant_key_never_reads_foreign_data_by_any_route() {
    let s = stack(&["tenant-a", "tenant-b"]);
    let b_ing = s.ingest_as(
        "tenant-b",
        &bundle("tenant-b", "b-1", "SECRET-B launch codes are 0000", 1),
    );
    assert_eq!(b_ing.status, 200);
    s.ingest_as(
        "tenant-a",
        &bundle("tenant-a", "a-1", "tenant a harmless note", 1),
    );
    s.wait_caught_up(Duration::from_secs(20));
    let (_, ka) = s.rk("tenant-a");
    let h = [("x-api-key", ka.as_str())];
    let rc = s.rc();

    // Direct reads of the foreign tenant.
    let r = rc.get("/v1/retrieve?tenant_id=tenant-b&query=launch", &h);
    assert_eq!(r.status, 403, "{}", r.body);
    for path in ["/debug/planner", "/debug/storage-visibility"] {
        let r = rc.get(&format!("{path}?tenant_id=tenant-b&query=launch"), &h);
        assert_eq!(r.status, 403, "{path}: {}", r.body);
        assert!(
            !r.body.contains("SECRET-B") && !r.body.contains("b-1"),
            "{path} leaks: {}",
            r.body
        );
        let r = rc.get(&format!("{path}?tenant_id=tenant-a&query=launch"), &h);
        assert!(
            r.status == 200 || r.status == 404,
            "{path} for own tenant: {}",
            r.status
        );
        assert!(
            !r.body.contains("SECRET-B") && !r.body.contains("b-1"),
            "{path} (own tenant) leaks foreign data: {}",
            r.body
        );
    }

    // Tenant-id spoofing variants must not widen access.
    let spoofs = [
        "tenant-b ",
        " tenant-b",
        "TENANT-B",
        "tenant-b\t",
        "tenant-b%00",
        "tenant-b%0a",
        "tenant-b,tenant-a",
        "tenant-a,tenant-b",
        "%74enant-b",
        "tenant-b/",
        "tenant-b%2e",
        "*",
        "",
        "tenant-%62",
    ];
    for sp in spoofs {
        let r = rc.get(
            &format!("/v1/retrieve?tenant_id={sp}&query=launch&top_k=50"),
            &h,
        );
        assert!(
            !r.body.contains("SECRET-B") && !r.body.contains("b-1"),
            "tenant_id={sp:?} exposed tenant-b data: {} {}",
            r.status,
            r.body
        );
        let body = format!(
            "{{\"tenant_id\":\"{}\",\"query\":\"launch\",\"top_k\":50}}",
            sp
        );
        let r = rc
            .request(
                "POST",
                "/v1/retrieve",
                &[
                    ("x-api-key", ka.as_str()),
                    ("Content-Type", "application/json"),
                ],
                Some(body.as_bytes()),
            )
            .unwrap();
        assert!(
            !r.body.contains("SECRET-B"),
            "POST tenant_id={sp:?} exposed tenant-b data: {} {}",
            r.status,
            r.body
        );
    }
    // Duplicate and mixed keys in the JSON body.
    for body in [
        r#"{"tenant_id":"tenant-a","tenant_id":"tenant-b","query":"launch","top_k":50}"#,
        r#"{"tenant_id":"tenant-b","tenant_id":"tenant-a","query":"launch","top_k":50}"#,
        r#"{"tenant_id":["tenant-a","tenant-b"],"query":"launch"}"#,
        r#"{"tenant_id":"tenant-a","tenantId":"tenant-b","query":"launch"}"#,
        r#"{"tenant_id":"tenant-a","query":"launch","entity_filters":["tenant-b"],"top_k":50}"#,
    ] {
        let r = rc
            .request(
                "POST",
                "/v1/retrieve",
                &[
                    ("x-api-key", ka.as_str()),
                    ("Content-Type", "application/json"),
                ],
                Some(body.as_bytes()),
            )
            .unwrap();
        assert!(
            !r.body.contains("SECRET-B"),
            "body {body} exposed tenant-b data: {} {}",
            r.status,
            r.body
        );
    }
    // Wide-open queries as tenant-a: only tenant-a's claims.
    let results = s.retrieve("tenant-a", ALL, 1000);
    assert!(
        results.iter().all(|c| c["claim_id"] == "a-1"),
        "tenant-a sees foreign claims: {results:?}"
    );
    let results = s.retrieve("tenant-a", "SECRET-B launch codes", 1000);
    assert!(
        results.iter().all(|c| c["claim_id"] == "a-1"),
        "lexical match crossed tenants: {results:?}"
    );

    // Operational routes readable with a tenant key must not name foreign data.
    for path in ["/metrics", "/ready", "/debug/placement", "/v1/ready"] {
        let r = rc.get(path, &h);
        for leak in ["SECRET-B", "b-1", "tenant-b"] {
            assert!(
                !r.body.contains(leak),
                "retrieval {path} leaks {leak:?} to tenant-a's key:\n{}",
                r.body
            );
        }
    }
    let (_, ia) = s.ik("tenant-a");
    for path in ["/metrics", "/ready", "/debug/placement"] {
        let r = s.ic().get(path, &[("x-api-key", ia.as_str())]);
        for leak in ["SECRET-B", "b-1", "tenant-b"] {
            assert!(
                !r.body.contains(leak),
                "ingestion {path} leaks {leak:?} to tenant-a's key:\n{}",
                r.body
            );
        }
    }
    // The replication-derived surfaces need the replication token, which a
    // tenant API key is not.
    for path in [
        "/internal/replication/export",
        "/internal/replication/wal?from_offset=0",
    ] {
        let r = s.ic().get(path, &[("x-api-key", ia.as_str())]);
        assert_eq!(
            r.status, 403,
            "{path} readable with a tenant key: {}",
            r.body
        );
        assert!(!r.body.contains("SECRET-B"));
        let r = s
            .ic()
            .get(path, &[("Authorization", &format!("Bearer {ia}"))]);
        assert_eq!(
            r.status, 403,
            "{path} readable with a tenant bearer: {}",
            r.body
        );
    }
    // And the retrieval node does not serve replication endpoints at all.
    for path in [
        "/internal/replication/export",
        "/internal/replication/wal?from_offset=0",
    ] {
        let r = rc.get(
            path,
            &[("x-replication-token", s.replication_token.as_str())],
        );
        assert!(
            r.status == 404 || r.status == 403,
            "retrieval exposes {path}: {} {}",
            r.status,
            r.body
        );
        assert!(
            !r.body.contains("SECRET-B"),
            "retrieval serves foreign data on {path}"
        );
    }
}
