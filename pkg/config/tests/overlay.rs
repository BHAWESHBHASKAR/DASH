//! The TOML file overlay: key resolution, precedence, value conversion,
//! permissions and error hygiene. All pure (no process environment).

use std::collections::HashMap;

use dash_config::overlay::{check_permissions, overlay_into, parse_file};
use dash_config::{Scope, Service, lookup_file_key};

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn apply(
    service: Service,
    toml: &str,
    env_pairs: &[(&str, &str)],
) -> (HashMap<String, String>, Vec<String>) {
    let entries = parse_file(toml).unwrap_or_else(|e| panic!("parse failed: {e:?}"));
    let mut map = env(env_pairs);
    let applied = overlay_into(service, &entries, &mut map);
    (map, applied)
}

fn parse_errors(toml: &str) -> Vec<String> {
    parse_file(toml)
        .expect_err("expected errors")
        .into_iter()
        .map(|i| i.to_string())
        .collect()
}

#[test]
fn keys_map_to_the_suffix_of_the_environment_name() {
    let (map, applied) = apply(
        Service::Ingestion,
        r#"
        [ingestion]
        wal_path = "/var/lib/dash/ingest.wal"
        http_workers = 8
        checkpoint_max_wal_records = 50000
        audit_log_path = "/var/log/dash/audit.jsonl"
        api_key_scopes = "k:tenant-a:ingest"

        [common]
        embedding_provider = "ollama"
        strict_secrets = true
        "#,
        &[],
    );
    assert_eq!(map["DASH_INGEST_WAL_PATH"], "/var/lib/dash/ingest.wal");
    assert_eq!(map["DASH_INGEST_HTTP_WORKERS"], "8");
    assert_eq!(map["DASH_CHECKPOINT_MAX_WAL_RECORDS"], "50000");
    assert_eq!(
        map["DASH_INGEST_AUDIT_LOG_PATH"],
        "/var/log/dash/audit.jsonl"
    );
    assert_eq!(map["DASH_INGEST_API_KEY_SCOPES"], "k:tenant-a:ingest");
    assert_eq!(map["DASH_EMBEDDING_PROVIDER"], "ollama");
    assert_eq!(map["DASH_STRICT_SECRETS"], "1");
    assert_eq!(applied.len(), 7);
}

#[test]
fn each_service_table_resolves_against_its_own_names() {
    assert_eq!(
        lookup_file_key(Scope::Retrieval, "wal_path").unwrap().name,
        "DASH_RETRIEVAL_WAL_PATH"
    );
    assert_eq!(
        lookup_file_key(Scope::Ingestion, "wal_path").unwrap().name,
        "DASH_INGEST_WAL_PATH"
    );
    assert_eq!(
        lookup_file_key(Scope::ControlPlane, "lease_path")
            .unwrap()
            .name,
        "DASH_CONTROL_PLANE_LEASE_PATH"
    );
    assert_eq!(
        lookup_file_key(Scope::Common, "router_placement_file")
            .unwrap()
            .name,
        "DASH_ROUTER_PLACEMENT_FILE"
    );
}

#[test]
fn the_environment_wins_over_the_file() {
    let toml = r#"
        [ingestion]
        wal_path = "/from/file"
        http_workers = 3
    "#;
    let (map, applied) = apply(
        Service::Ingestion,
        toml,
        &[("DASH_INGEST_WAL_PATH", "/from/env")],
    );
    assert_eq!(map["DASH_INGEST_WAL_PATH"], "/from/env");
    assert_eq!(map["DASH_INGEST_HTTP_WORKERS"], "3");
    assert_eq!(applied, ["DASH_INGEST_HTTP_WORKERS"]);
}

#[test]
fn a_variable_set_under_any_of_its_names_counts_as_set() {
    let toml = r#"
        [ingestion]
        wal_path = "/from/file"
        ann_max_neighbors_base = 20
        [common]
        ollama_endpoint = "http://file:11434"
    "#;
    let (map, applied) = apply(
        Service::Ingestion,
        toml,
        &[
            // legacy spelling
            ("EME_INGEST_WAL_PATH", "/legacy"),
            // shared spelling
            ("DASH_ANN_MAX_NEIGHBORS_BASE", "9"),
            // deprecated alias
            ("DASH_OLLAMA_BASE_URL", "http://alias:11434"),
        ],
    );
    assert!(applied.is_empty(), "{applied:?}");
    assert!(!map.contains_key("DASH_INGEST_WAL_PATH"));
    assert!(!map.contains_key("DASH_INGEST_ANN_MAX_NEIGHBORS_BASE"));
    assert!(!map.contains_key("DASH_OLLAMA_ENDPOINT"));
}

#[test]
fn a_set_but_empty_variable_still_wins_over_the_file() {
    let (map, applied) = apply(
        Service::Ingestion,
        "[ingestion]\nwal_path = \"/from/file\"\n",
        &[("DASH_INGEST_WAL_PATH", "")],
    );
    assert!(applied.is_empty());
    assert_eq!(map["DASH_INGEST_WAL_PATH"], "");
}

#[test]
fn only_settings_the_service_reads_are_applied() {
    let toml = r#"
        [ingestion]
        wal_path = "/ingest.wal"
        [retrieval]
        wal_path = "/retrieval.wal"
        max_top_k = 50
        [control_plane]
        node_id = "cp-1"
        token = "a-control-plane-token-0123456789abcdef"
        [common]
        log_format = "json"
    "#;
    let (map, _) = apply(Service::Retrieval, toml, &[]);
    assert_eq!(map["DASH_RETRIEVAL_WAL_PATH"], "/retrieval.wal");
    assert_eq!(map["DASH_RETRIEVAL_MAX_TOP_K"], "50");
    assert_eq!(map["DASH_LOG_FORMAT"], "json");
    assert!(!map.contains_key("DASH_INGEST_WAL_PATH"));
    assert!(!map.contains_key("DASH_CONTROL_PLANE_NODE_ID"));
    // The control-plane token is also read by the router client in retrieval.
    assert!(map.contains_key("DASH_CONTROL_PLANE_TOKEN"));

    let (map, _) = apply(Service::ControlPlane, toml, &[]);
    assert_eq!(map["DASH_CONTROL_PLANE_NODE_ID"], "cp-1");
    assert!(!map.contains_key("DASH_RETRIEVAL_WAL_PATH"));
}

#[test]
fn arrays_join_with_the_separator_the_reader_expects() {
    let (map, _) = apply(
        Service::Ingestion,
        r#"
        [ingestion]
        api_keys = ["key-one-0123456789abcdef", "key-two-0123456789abcdef"]
        api_key_scopes = ["k1:tenant-a:ingest", "k2:*:read_only"]
        jwt_tenant_claims = ["tenant_id", "tenants"]
        [common]
        router_shard_ids = [0, 1, 2]
        "#,
        &[],
    );
    assert_eq!(
        map["DASH_INGEST_API_KEYS"],
        "key-one-0123456789abcdef,key-two-0123456789abcdef"
    );
    // Scoped keys are separated by `;` (the commas belong to the tenant list).
    assert_eq!(
        map["DASH_INGEST_API_KEY_SCOPES"],
        "k1:tenant-a:ingest;k2:*:read_only"
    );
    assert_eq!(map["DASH_INGEST_JWT_TENANT_CLAIMS"], "tenant_id,tenants");
    assert_eq!(map["DASH_ROUTER_SHARD_IDS"], "0,1,2");
}

#[test]
fn scalars_convert_to_what_readers_parse() {
    let (map, _) = apply(
        Service::Retrieval,
        r#"
        [retrieval]
        graph_edge_depth_decay = 0.5
        disk_native_segment_execution = false
        persistence_disable = true
        max_top_k = 25
        "#,
        &[],
    );
    assert_eq!(map["DASH_RETRIEVAL_GRAPH_EDGE_DEPTH_DECAY"], "0.5");
    // Booleans become `1`/`0`, which every reader flavor understands.
    assert_eq!(map["DASH_RETRIEVAL_DISK_NATIVE_SEGMENT_EXECUTION"], "0");
    assert_eq!(map["DASH_RETRIEVAL_PERSISTENCE_DISABLE"], "1");
    assert_eq!(map["DASH_RETRIEVAL_MAX_TOP_K"], "25");
}

#[test]
fn an_array_for_a_single_valued_setting_is_an_error() {
    let errors = parse_errors("[ingestion]\nwal_path = [\"/a\", \"/b\"]\n");
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("not a list"), "{errors:?}");
}

#[test]
fn unknown_keys_are_errors_with_a_suggestion() {
    let errors = parse_errors("[ingestion]\nwal_paht = \"/x\"\n");
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("[ingestion] wal_paht"), "{errors:?}");
    assert!(errors[0].contains("did you mean 'wal_path'?"), "{errors:?}");

    let errors = parse_errors("[retrieval]\ntotally_made_up = 1\n");
    assert!(errors[0].contains("unknown key"));
    assert!(!errors[0].contains("did you mean"));
}

#[test]
fn a_key_in_the_wrong_table_says_where_it_belongs() {
    let errors = parse_errors("[common]\nwal_path = \"/x\"\n");
    // `wal_path` is not a common setting, but it is valid in a service table.
    assert!(errors[0].contains("unknown key"), "{errors:?}");
    let errors = parse_errors("[common]\nmax_top_k = 5\n");
    assert!(errors[0].contains("belongs in [retrieval]"), "{errors:?}");
}

#[test]
fn unknown_tables_are_errors_with_a_suggestion() {
    let errors = parse_errors("[ingest]\nwal_path = \"/x\"\n");
    assert!(errors[0].contains("unknown table"), "{errors:?}");
    assert!(
        errors[0].contains("did you mean [ingestion]?"),
        "{errors:?}"
    );
    let errors = parse_errors("top_level = 1\n");
    assert!(errors[0].contains("top_level"));
}

#[test]
fn every_error_is_reported_not_just_the_first() {
    let errors = parse_errors("[ingestion]\nwal_paht = 1\nhttp_workrs = 2\n[nope]\nx = 1\n");
    assert_eq!(errors.len(), 3, "{errors:?}");
}

#[test]
fn settings_not_read_by_the_services_cannot_be_set_from_the_file() {
    // DASH_BIN is read by the container entrypoint, not by a service.
    let errors = parse_errors("[common]\nbin = \"ingestion\"\n");
    assert!(
        errors[0].contains("cannot be set from the file"),
        "{errors:?}"
    );
}

#[test]
fn the_same_setting_in_two_tables_is_an_error() {
    let errors =
        parse_errors("[ingestion]\nstrict_secrets = true\n[common]\nstrict_secrets = false\n");
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert!(errors[0].contains("again"));
}

#[test]
fn toml_syntax_errors_never_quote_the_offending_line() {
    let secret = "super-secret-value-0123456789";
    let errors = parse_errors(&format!(
        "[ingestion]\napi_key = \"{secret}\" trailing garbage\n"
    ));
    assert_eq!(errors.len(), 1);
    assert!(errors[0].contains("invalid TOML at line 2"), "{errors:?}");
    assert!(!errors[0].contains(secret), "{errors:?}");
}

#[test]
fn invalid_value_types_do_not_echo_the_value() {
    let secret = "super-secret-value-0123456789";
    let errors = parse_errors(&format!(
        "[ingestion]\napi_key = {{ nested = \"{secret}\" }}\n"
    ));
    assert_eq!(errors.len(), 1);
    assert!(!errors[0].contains(secret));
}

#[test]
fn secret_keys_require_a_private_file_mode() {
    let entries = parse_file(
        "[ingestion]\napi_key = \"0123456789abcdef0123456789abcdef\"\nwal_path = \"/x\"\n",
    )
    .unwrap();
    for ok in [0o600, 0o640, 0o400, 0o100600, 0o100640] {
        assert!(
            check_permissions(&entries, Some(ok), "/etc/dash.toml").is_empty(),
            "{ok:o}"
        );
    }
    for bad in [0o644, 0o664, 0o666, 0o604, 0o660, 0o755] {
        let issues = check_permissions(&entries, Some(bad), "/etc/dash.toml");
        assert_eq!(issues.len(), 1, "{bad:o}");
        let text = issues[0].to_string();
        assert!(text.contains("api_key"), "{text}");
        assert!(text.contains("0600 or 0640"), "{text}");
        assert!(!text.contains("0123456789abcdef"), "{text}");
    }
    // Unknown mode (non-unix) skips the check.
    assert!(check_permissions(&entries, None, "/etc/dash.toml").is_empty());
}

#[test]
fn files_without_secrets_may_be_world_readable() {
    let entries = parse_file("[ingestion]\nwal_path = \"/x\"\nhttp_workers = 4\n").unwrap();
    assert!(check_permissions(&entries, Some(0o644), "/etc/dash.toml").is_empty());
}

#[test]
fn secret_list_keys_count_as_secrets_too() {
    let entries = parse_file("[retrieval]\napi_keys = [\"0123456789abcdef0123\"]\n").unwrap();
    assert_eq!(check_permissions(&entries, Some(0o644), "/f").len(), 1);
}
