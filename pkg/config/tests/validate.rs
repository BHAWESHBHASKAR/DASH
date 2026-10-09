//! Behavior of `validate_env`. Every test passes the environment as a map, so
//! none of them touches (or depends on) the process environment.

use std::collections::HashMap;

use dash_config::{Kind, Report, Service, did_you_mean, edit_distance, settings, validate_env};

fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn check(service: Service, pairs: &[(&str, &str)]) -> Report {
    validate_env(service, &env(pairs))
}

fn error_names(report: &Report) -> Vec<&str> {
    report.errors.iter().map(|i| i.name.as_str()).collect()
}

fn warning_text(report: &Report) -> String {
    report
        .warnings
        .iter()
        .map(|w| w.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn clean_environment_has_no_findings() {
    let report = check(
        Service::Ingestion,
        &[
            ("DASH_INGEST_BIND", "127.0.0.1:8081"),
            ("DASH_INGEST_WAL_PATH", "/var/lib/dash/ingest.wal"),
            ("DASH_INGEST_HTTP_WORKERS", "8"),
            ("DASH_STRICT_SECRETS", "1"),
            ("DASH_EMBEDDING_PROVIDER", "hash"),
            ("DASH_INGEST_API_KEY", "0123456789abcdef0123456789abcdef"),
        ],
    );
    assert_eq!(report, Report::default());
}

#[test]
fn non_numeric_value_for_a_number_is_an_error() {
    let report = check(Service::Ingestion, &[("DASH_INGEST_HTTP_WORKERS", "abc")]);
    assert_eq!(error_names(&report), ["DASH_INGEST_HTTP_WORKERS"]);
    assert!(report.errors[0].message.contains("integer"));
    assert!(report.errors[0].message.contains("abc"));
}

#[test]
fn out_of_range_number_is_an_error() {
    for (service, name) in [
        (Service::Ingestion, "DASH_INGEST_HTTP_WORKERS"),
        (Service::Retrieval, "DASH_RETRIEVAL_MAX_TOP_K"),
        (
            Service::Ingestion,
            "DASH_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS",
        ),
    ] {
        let report = check(service, &[(name, "0")]);
        assert_eq!(error_names(&report), [name], "{name}=0");
        assert!(report.errors[0].message.contains("out of range"));
    }
    // 1 is below the minimum of 2 for compaction input, 2 is fine.
    assert!(
        check(
            Service::Ingestion,
            &[("DASH_INGEST_SEGMENT_MAX_COMPACTION_INPUT_SEGMENTS", "2")]
        )
        .is_ok()
    );
    // A negative number is not an unsigned integer.
    assert!(!check(Service::Ingestion, &[("DASH_INGEST_HTTP_WORKERS", "-4")]).is_ok());
}

#[test]
fn zero_is_valid_where_it_means_disabled() {
    let report = check(
        Service::Retrieval,
        &[
            ("DASH_HTTP_MAX_CONNS_PER_IP", "0"),
            ("DASH_EMBEDDING_MAX_CONCURRENCY", "0"),
            ("DASH_RETRIEVAL_RATE_LIMIT_PER_TENANT_RPS", "0"),
            ("DASH_ROUTER_PLACEMENT_RELOAD_INTERVAL_MS", "0"),
        ],
    );
    assert!(report.is_ok(), "{:?}", report.errors);
}

#[test]
fn unknown_enum_word_is_an_error_and_matching_is_case_insensitive() {
    let report = check(Service::Retrieval, &[("DASH_EMBEDDING_PROVIDER", "olama")]);
    assert_eq!(error_names(&report), ["DASH_EMBEDDING_PROVIDER"]);
    assert!(report.errors[0].message.contains("hash, ollama, openai"));

    for ok in ["hash", "OLLAMA", " OpenAI "] {
        assert!(
            check(Service::Retrieval, &[("DASH_EMBEDDING_PROVIDER", ok)]).is_ok(),
            "{ok:?}"
        );
    }
    let report = check(
        Service::Retrieval,
        &[("DASH_ROUTER_READ_PREFERENCE", "nearest")],
    );
    assert_eq!(error_names(&report), ["DASH_ROUTER_READ_PREFERENCE"]);
}

#[test]
fn enum_accepts_every_spelling_the_readers_accept() {
    // Aliases that the ingestion extraction readers map to a provider.
    for word in ["rule", "rule_sentence", "model_adapter", "adapter_command"] {
        assert!(
            check(
                Service::Ingestion,
                &[("DASH_INGEST_RAW_EXTRACTION_PROVIDER", word)]
            )
            .is_ok(),
            "{word}"
        );
    }
    for word in ["builtin_hash", "hash", "none", "off", "disabled"] {
        assert!(
            check(
                Service::Ingestion,
                &[("DASH_INGEST_EMBEDDING_PROVIDER", word)]
            )
            .is_ok(),
            "{word}"
        );
    }
    // Blank means the default for these readers.
    assert!(
        check(
            Service::Ingestion,
            &[("DASH_INGEST_RAW_EXTRACTION_PROVIDER", "")]
        )
        .is_ok()
    );
    assert!(check(Service::Retrieval, &[("DASH_ROUTER_READ_PREFERENCE", "")]).is_ok());
}

#[test]
fn unparsable_bool_is_an_error() {
    let report = check(Service::Ingestion, &[("DASH_STRICT_SECRETS", "maybe")]);
    assert_eq!(error_names(&report), ["DASH_STRICT_SECRETS"]);
    let report = check(
        Service::Ingestion,
        &[("DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY", "2")],
    );
    assert_eq!(
        error_names(&report),
        ["DASH_INGEST_WAL_BACKGROUND_FLUSH_ONLY"]
    );
}

#[test]
fn every_boolean_spelling_any_reader_accepts_is_never_an_error() {
    let words = [
        "1", "true", "yes", "on", "0", "false", "no", "off", "TRUE", "True", "YES", "Off", " 1 ",
    ];
    let mut checked = 0;
    for setting in settings() {
        if !matches!(setting.kind(), Kind::Bool(_)) {
            continue;
        }
        for service in Service::ALL {
            if !setting.read_by(service) {
                continue;
            }
            for word in words {
                let report = check(service, &[(setting.name.as_str(), word)]);
                assert!(
                    report.errors.is_empty(),
                    "{}={word:?} must not be an error for {service:?}: {:?}",
                    setting.name,
                    report.errors
                );
                checked += 1;
            }
        }
    }
    assert!(checked > 100, "only {checked} boolean checks ran");
}

#[test]
fn narrow_readers_warn_when_a_truthy_spelling_would_not_enable_them() {
    // Only the literal `1` enables these.
    let name = "DASH_EMBEDDING_ALLOW_INSECURE_HTTP";
    let report = check(Service::Retrieval, &[(name, "true")]);
    assert!(report.errors.is_empty(), "{name}");
    assert!(warning_text(&report).contains(name), "{name}: {report:?}");
    let report = check(Service::Retrieval, &[(name, "1")]);
    assert_eq!(report, Report::default(), "{name}=1");
    let report = check(
        Service::ControlPlane,
        &[("DASH_CONTROL_PLANE_LEASE_RESET", "yes")],
    );
    assert!(report.errors.is_empty());
    assert!(warning_text(&report).contains("DASH_CONTROL_PLANE_LEASE_RESET"));

    // `1`, `true`, `TRUE` enable the stale-placement fallback; `yes` does not.
    assert_eq!(
        check(
            Service::Ingestion,
            &[("DASH_ROUTER_ALLOW_STALE_PLACEMENT", "TRUE")]
        ),
        Report::default()
    );
    assert!(
        !check(
            Service::Ingestion,
            &[("DASH_ROUTER_ALLOW_STALE_PLACEMENT", "yes")]
        )
        .warnings
        .is_empty()
    );

    // Persistence disable: the startup path honors `1`, the readiness probe
    // `1/true/yes`; the union is accepted without a warning for those three.
    for word in ["1", "true", "yes", "0", "off"] {
        assert_eq!(
            check(
                Service::Ingestion,
                &[("DASH_INGEST_PERSISTENCE_DISABLE", word)]
            ),
            Report::default(),
            "{word}"
        );
    }
    assert!(
        !check(
            Service::Ingestion,
            &[("DASH_INGEST_PERSISTENCE_DISABLE", "on")]
        )
        .warnings
        .is_empty()
    );

    // The maintenance daemon honors 1/true/TRUE/yes.
    for word in ["1", "true", "TRUE", "yes"] {
        assert_eq!(
            check(
                Service::Ingestion,
                &[("DASH_INGEST_SEGMENT_MAINTENANCE_STRICT", word)]
            ),
            Report::default(),
            "{word}"
        );
    }
    assert!(
        !check(
            Service::Ingestion,
            &[("DASH_INGEST_SEGMENT_MAINTENANCE_STRICT", "on")]
        )
        .warnings
        .is_empty()
    );
}

#[test]
fn the_control_plane_dev_mode_flag_accepts_the_union_of_reader_spellings() {
    // Data services accept `true`, the control plane only `1`; the shared
    // setting accepts the union (and never errors on it).
    for service in Service::ALL {
        for word in ["1", "true", "yes", "on"] {
            assert!(
                check(service, &[("DASH_INSECURE_DEV_MODE", word)]).is_ok(),
                "{service:?} {word}"
            );
        }
    }
}

#[test]
fn blank_values_are_errors_only_where_blank_is_meaningless() {
    for (service, name) in [
        (Service::Ingestion, "DASH_INGEST_BIND"),
        (Service::Ingestion, "DASH_INGEST_HTTP_WORKERS"),
        (Service::Ingestion, "DASH_INGEST_WAL_PATH"),
        (Service::Retrieval, "DASH_RETRIEVAL_REPLICATION_SOURCE_URL"),
        (Service::Retrieval, "DASH_EMBEDDING_PROVIDER"),
        (Service::Ingestion, "DASH_STRICT_SECRETS"),
        (Service::Ingestion, "DASH_INGEST_ALLOWED_TENANTS"),
    ] {
        let report = check(service, &[(name, "  ")]);
        assert_eq!(error_names(&report), [name], "{name}");
        assert!(report.errors[0].message.contains("empty"), "{name}");
    }
    // Blank is how readers say "unset" for these.
    for (service, name) in [
        (Service::Ingestion, "DASH_INGEST_REPLICATION_OFFSET_PATH"),
        (Service::Retrieval, "DASH_RETRIEVAL_REPLICATION_OFFSET_PATH"),
        (
            Service::Ingestion,
            "DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS",
        ),
        (Service::Ingestion, "DASH_INGEST_API_KEY"),
        (Service::Ingestion, "DASH_ROUTER_CONTROL_PLANE_TOKEN"),
    ] {
        assert!(check(service, &[(name, "")]).is_ok(), "{name}");
    }
}

#[test]
fn wal_async_flush_accepts_integers_and_the_documented_words() {
    for ok in [
        "250", "auto", "AUTO", "off", "none", "false", "disabled", "0", "",
    ] {
        assert!(
            check(
                Service::Ingestion,
                &[("DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS", ok)]
            )
            .is_ok(),
            "{ok:?}"
        );
    }
    for bad in ["fast", "-5", "1.5"] {
        assert!(
            !check(
                Service::Ingestion,
                &[("DASH_INGEST_WAL_ASYNC_FLUSH_INTERVAL_MS", bad)]
            )
            .is_ok(),
            "{bad:?}"
        );
    }
}

#[test]
fn bind_address_must_be_host_and_port() {
    for bad in [
        "8081",
        "0.0.0.0",
        ":8081",
        "host:notaport",
        "a b:80",
        "host:70000",
    ] {
        let report = check(Service::Ingestion, &[("DASH_INGEST_BIND", bad)]);
        assert_eq!(error_names(&report), ["DASH_INGEST_BIND"], "{bad}");
    }
    for ok in [
        "0.0.0.0:8081",
        "127.0.0.1:0",
        "localhost:8080",
        "[::1]:8081",
    ] {
        assert!(
            check(Service::Ingestion, &[("DASH_INGEST_BIND", ok)]).is_ok(),
            "{ok}"
        );
    }
}

#[test]
fn urls_need_an_http_scheme_and_a_host() {
    for bad in ["ingestion:8081", "ftp://x", "http://", "http://a b"] {
        let report = check(
            Service::Retrieval,
            &[("DASH_RETRIEVAL_REPLICATION_SOURCE_URL", bad)],
        );
        assert_eq!(
            error_names(&report),
            ["DASH_RETRIEVAL_REPLICATION_SOURCE_URL"],
            "{bad}"
        );
    }
    for ok in ["http://ingestion:8081", "https://x.example/api", "HTTP://X"] {
        assert!(
            check(
                Service::Retrieval,
                &[("DASH_RETRIEVAL_REPLICATION_SOURCE_URL", ok)]
            )
            .is_ok(),
            "{ok}"
        );
    }
}

#[test]
fn shard_id_lists_must_contain_integers() {
    assert!(check(Service::Ingestion, &[("DASH_ROUTER_SHARD_IDS", "0, 1,2")]).is_ok());
    let report = check(Service::Ingestion, &[("DASH_ROUTER_SHARD_IDS", "0,one")]);
    assert_eq!(error_names(&report), ["DASH_ROUTER_SHARD_IDS"]);
}

#[test]
fn floats_must_be_finite_and_in_range() {
    assert!(
        check(
            Service::Retrieval,
            &[("DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO", "0.5")]
        )
        .is_ok()
    );
    for bad in ["-0.1", "nan", "inf", "half"] {
        assert!(
            !check(
                Service::Retrieval,
                &[("DASH_RETRIEVAL_STORAGE_DIVERGENCE_WARN_RATIO", bad)]
            )
            .is_ok(),
            "{bad}"
        );
    }
}

#[test]
fn a_misspelled_variable_warns_with_a_suggestion_and_is_not_an_error() {
    let report = check(Service::Ingestion, &[("DASH_INGEST_WAL_PAHT", "/x")]);
    assert!(report.errors.is_empty());
    assert_eq!(report.warnings.len(), 1);
    let text = warning_text(&report);
    assert!(text.contains("DASH_INGEST_WAL_PAHT"));
    assert!(
        text.contains("did you mean DASH_INGEST_WAL_PATH?"),
        "{text}"
    );

    let report = check(Service::Retrieval, &[("DASH_RETRIEVAL_HTTP_WORKRES", "4")]);
    assert!(warning_text(&report).contains("did you mean DASH_RETRIEVAL_HTTP_WORKERS?"));
}

#[test]
fn an_unrelated_unknown_variable_warns_without_a_suggestion() {
    let report = check(Service::Ingestion, &[("DASH_TOTALLY_UNRELATED_THING", "x")]);
    assert_eq!(report.warnings.len(), 1);
    assert!(!warning_text(&report).contains("did you mean"));
    assert!(warning_text(&report).contains("unknown variable"));
}

#[test]
fn a_misspelled_legacy_variable_also_gets_a_suggestion() {
    let report = check(Service::Ingestion, &[("EME_INGEST_WAL_PAHT", "/x")]);
    let text = warning_text(&report);
    assert!(text.contains("did you mean"), "{text}");
    assert!(text.contains("WAL_PATH"), "{text}");
}

#[test]
fn variables_outside_the_dash_and_eme_namespaces_are_ignored() {
    let report = check(
        Service::Ingestion,
        &[
            ("PATH", "/bin"),
            ("RUST_LOG", "info"),
            ("DASHBOARD_URL", "x"),
            ("HOME", "/root"),
        ],
    );
    assert_eq!(report, Report::default());
}

#[test]
fn settings_of_other_services_are_known_but_not_validated() {
    // The retrieval WAL path in an ingestion environment: known name, no warning.
    assert_eq!(
        check(Service::Ingestion, &[("DASH_RETRIEVAL_WAL_PATH", "/x")]),
        Report::default()
    );
    // A malformed retrieval-only value does not stop ingestion, but does stop retrieval.
    let pairs = [("DASH_RETRIEVAL_HTTP_WORKERS", "abc")];
    assert!(check(Service::Ingestion, &pairs).is_ok());
    assert!(!check(Service::Retrieval, &pairs).is_ok());
}

#[test]
fn each_legacy_prefix_variable_gets_exactly_one_deprecation_warning() {
    let report = check(
        Service::Ingestion,
        &[
            ("EME_INGEST_BIND", "127.0.0.1:8081"),
            ("EME_INGEST_WAL_PATH", "/x"),
        ],
    );
    assert!(report.errors.is_empty());
    assert_eq!(report.warnings.len(), 2, "{:?}", report.warnings);
    let bind: Vec<_> = report
        .warnings
        .iter()
        .filter(|w| w.name == "EME_INGEST_BIND")
        .collect();
    assert_eq!(bind.len(), 1);
    assert!(bind[0].message.contains("DASH_INGEST_BIND"));
    assert!(bind[0].message.contains("deprecated"));
}

#[test]
fn legacy_prefix_values_are_validated_too() {
    let report = check(Service::Ingestion, &[("EME_INGEST_HTTP_WORKERS", "many")]);
    assert_eq!(error_names(&report), ["EME_INGEST_HTTP_WORKERS"]);
    assert_eq!(report.warnings.len(), 1);
}

#[test]
fn deprecated_aliases_warn_and_name_the_replacement() {
    let report = check(
        Service::Retrieval,
        &[("DASH_OLLAMA_BASE_URL", "http://localhost:11434")],
    );
    assert!(report.errors.is_empty());
    assert_eq!(report.warnings.len(), 1);
    assert!(report.warnings[0].message.contains("DASH_OLLAMA_ENDPOINT"));

    let report = check(
        Service::Ingestion,
        &[("DASH_INGEST_JWT_ROLE_CLAIM", "roles")],
    );
    assert_eq!(report.warnings.len(), 1);
    assert!(
        report.warnings[0]
            .message
            .contains("DASH_INGEST_JWT_ROLES_CLAIM")
    );
}

#[test]
fn shared_ann_names_are_valid_spellings_and_are_validated() {
    assert_eq!(
        check(Service::Ingestion, &[("DASH_ANN_MAX_NEIGHBORS_BASE", "16")]),
        Report::default()
    );
    assert!(
        !check(
            Service::Retrieval,
            &[("DASH_ANN_SEARCH_EXPANSION_MIN", "x")]
        )
        .is_ok()
    );
    assert!(
        !check(
            Service::Ingestion,
            &[("DASH_INGEST_ANN_MAX_NEIGHBORS_UPPER", "0")]
        )
        .is_ok()
    );
}

#[test]
fn runtime_built_per_service_names_are_known_and_typed() {
    // Built at runtime by the shared auth policy.
    let report = check(
        Service::Retrieval,
        &[
            ("DASH_RETRIEVAL_JWT_LEEWAY_SECS", "30"),
            ("DASH_RETRIEVAL_JWT_PROVIDER", "oidc"),
            ("DASH_RETRIEVAL_AUDIT_FSYNC", "0"),
        ],
    );
    assert_eq!(report, Report::default());
    let report = check(
        Service::Retrieval,
        &[("DASH_RETRIEVAL_JWT_PROVIDER", "saml")],
    );
    assert_eq!(error_names(&report), ["DASH_RETRIEVAL_JWT_PROVIDER"]);
    let report = check(
        Service::Ingestion,
        &[("DASH_INGEST_AUDIT_FAIL_CLOSED", "sometimes")],
    );
    assert_eq!(error_names(&report), ["DASH_INGEST_AUDIT_FAIL_CLOSED"]);
}

#[test]
fn platform_injected_service_link_variables_are_not_reported() {
    let report = check(
        Service::Ingestion,
        &[
            ("DASH_INGESTION_SERVICE_HOST", "10.0.0.1"),
            ("DASH_INGESTION_SERVICE_PORT", "8081"),
            ("DASH_INGESTION_SERVICE_PORT_HTTP", "8081"),
            ("DASH_INGESTION_PORT", "tcp://10.0.0.1:8081"),
            ("DASH_INGESTION_PORT_8081_TCP", "tcp://10.0.0.1:8081"),
            ("DASH_INGESTION_PORT_8081_TCP_ADDR", "10.0.0.1"),
        ],
    );
    assert_eq!(report, Report::default());
    // A real mistake with a similar shape still warns.
    let report = check(Service::Ingestion, &[("DASH_INGEST_PORT", "8081")]);
    assert_eq!(report.warnings.len(), 1);
}

#[test]
fn secret_values_are_never_echoed_in_findings() {
    // Kinds that can produce findings never print a secret; a secret-typed
    // setting set to an unsupported value still yields no message with it.
    let secret = "s3cr3t-do-not-print-0123456789";
    for setting in settings().iter().filter(|s| s.kind().is_secret()) {
        for service in Service::ALL {
            if !setting.read_by(service) {
                continue;
            }
            let report = check(service, &[(setting.name.as_str(), secret)]);
            let all = format!("{:?}{:?}", report.errors, report.warnings);
            assert!(!all.contains(secret), "{} leaked its value", setting.name);
        }
    }
}

#[test]
fn every_default_in_the_registry_is_accepted_by_its_own_kind() {
    let mut checked = 0;
    for setting in settings() {
        let default = setting.default.trim();
        let usable = match setting.kind() {
            Kind::Int { .. } | Kind::Millis { .. } => default.parse::<u64>().is_ok(),
            Kind::Float { .. } => default.parse::<f64>().is_ok(),
            Kind::Enum { values } => values.contains(&default),
            Kind::Bool(_) => {
                ["1", "0", "true", "false", "yes", "no", "on", "off"].contains(&default)
            }
            _ => false,
        };
        if !usable {
            continue;
        }
        for service in Service::ALL {
            if !setting.read_by(service) {
                continue;
            }
            let report = check(service, &[(setting.name.as_str(), default)]);
            assert!(
                report.errors.is_empty(),
                "default {default:?} of {} is rejected: {:?}",
                setting.name,
                report.errors
            );
            checked += 1;
        }
    }
    assert!(checked > 40, "only {checked} defaults were checked");
}

#[test]
fn container_variables_are_known_and_their_values_are_not_judged() {
    // Read by the container scripts and compose, not by the services.
    let report = check(
        Service::Ingestion,
        &[
            ("DASH_BIN", "anything-the-entrypoint-understands"),
            ("DASH_HEALTHCHECK_URL", "not a url"),
            ("DASH_DEV_UID", "$(id -u)"),
            ("DASH_PUBLISH_ADDR", "0.0.0.0"),
            ("DASH_INGEST_TRANSPORT_RUNTIME", "legacy"),
            ("RUST_LOG", "debug"),
        ],
    );
    assert_eq!(report, Report::default());
}

#[test]
fn downgrading_turns_errors_into_warnings() {
    let mut report = check(Service::Ingestion, &[("DASH_INGEST_HTTP_WORKERS", "abc")]);
    assert!(!report.is_ok());
    report.downgrade_errors();
    assert!(report.is_ok());
    assert!(warning_text(&report).contains("DASH_INGEST_HTTP_WORKERS"));
    assert!(warning_text(&report).contains("DASH_CONFIG_VALIDATION=warn"));
}

#[test]
fn edit_distance_counts_transpositions_as_one() {
    assert_eq!(edit_distance("PATH", "PAHT"), 1);
    assert_eq!(edit_distance("kitten", "sitting"), 3);
    assert_eq!(edit_distance("", "abc"), 3);
    assert_eq!(edit_distance("same", "same"), 0);
}

#[test]
fn did_you_mean_picks_the_closest_within_a_limit() {
    let names = [
        "DASH_INGEST_BIND",
        "DASH_INGEST_WAL_PATH",
        "DASH_RETRIEVAL_BIND",
    ];
    assert_eq!(
        did_you_mean("DASH_INGEST_WAL_PAHT", names),
        Some("DASH_INGEST_WAL_PATH")
    );
    assert_eq!(did_you_mean("COMPLETELY_DIFFERENT", names), None);
}
