//! The generated configuration reference page.

use std::path::Path;

use dash_config::docs::{CheckOutcome, check, render};
use dash_config::{Service, settings};

fn committed_page() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(dash_config::docs::DOC_PATH);
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

#[test]
fn the_committed_page_matches_the_registry() {
    // Regenerate with: cargo run -p dash-config -- docs
    assert_eq!(
        check(Some(&committed_page())),
        CheckOutcome::UpToDate,
        "docs-site/docs/reference/configuration.md is stale; run `cargo run -p dash-config -- docs`"
    );
}

#[test]
fn rendering_is_deterministic() {
    assert_eq!(render(), render());
}

#[test]
fn check_reports_missing_and_stale_pages() {
    assert_eq!(check(None), CheckOutcome::Missing);
    let page = render();
    assert_eq!(check(Some(&page)), CheckOutcome::UpToDate);
    let edited = page.replacen("# Configuration", "# Configuration (edited)", 1);
    assert_eq!(
        check(Some(&edited)),
        CheckOutcome::Stale {
            first_difference_line: 1
        }
    );
}

#[test]
fn every_setting_is_documented_under_every_one_of_its_names() {
    let page = render();
    for setting in settings() {
        if setting.entry.planned {
            continue;
        }
        assert!(
            page.contains(&format!("`{}`", setting.name)),
            "{} is missing from the generated page",
            setting.name
        );
        for shared in &setting.shared {
            assert!(page.contains(&format!("`{shared}`")), "{shared}");
        }
    }
}

#[test]
fn defaults_and_descriptions_come_from_the_registry() {
    let page = render();
    // Spot checks against the code that reads them.
    assert!(page.contains("| `DASH_INGEST_BIND` | `127.0.0.1:8081` | host:port |"));
    assert!(page.contains("| `DASH_RETRIEVAL_BIND` | `127.0.0.1:8080` | host:port |"));
    assert!(page.contains("`100` (ingestion), `500` (retrieval)"));
    assert!(page.contains("| `DASH_RETRIEVAL_MAX_TOP_K` | `1000` |"));
    assert!(page.contains("| `DASH_CONFIG_VALIDATION` | `error` |"));
}

#[test]
fn hand_written_prose_sections_survive_generation() {
    let page = render();
    for needle in [
        "## Conventions",
        "## Defaults at a glance",
        "Fixed limits (compiled in, **not** configurable)",
        "Fixed retrieve bounds (not configurable)",
        "## Config reload and SIGHUP",
        "## Variables that do not exist",
        "`DASH_INGEST_WAL_FSYNC_POLICY`",
        "## Example",
        "Placement routing is enabled when",
        "Both followers pull WAL frames",
        "There is no `DASH_ANN_M`",
        "Adapter commands are operator-controlled",
        "Tenant directories under the segment root",
        "Provider outages (breaker open",
        "**No service calls it**",
        "scripts/generate-secrets.sh",
    ] {
        assert!(page.contains(needle), "lost prose: {needle}");
    }
}

#[test]
fn the_page_has_a_table_row_per_setting_and_valid_table_shape() {
    let page = render();
    for line in page.lines().filter(|l| l.starts_with("| `")) {
        // Five columns: split on unescaped pipes.
        let cells = line.replace("\\|", "").matches('|').count();
        assert_eq!(cells, 6, "bad table row: {line}");
    }
}

#[test]
fn deprecated_aliases_and_dash_only_markers_are_derived() {
    let page = render();
    assert!(page.contains("DASH_OLLAMA_BASE_URL"));
    // A setting without an EME_ twin is marked DASH only; one with a twin is not.
    let row = |name: &str| {
        page.lines()
            .find(|l| l.starts_with(&format!("| `{name}`")))
            .unwrap_or_else(|| panic!("no row for {name}"))
            .to_string()
    };
    assert!(row("DASH_RETRIEVAL_MAX_TOP_K").contains("DASH only."));
    assert!(!row("DASH_INGEST_BIND").contains("DASH only."));
}

#[test]
fn the_toml_example_in_the_operations_page_is_valid_for_every_service() {
    use std::collections::HashMap;

    use dash_config::plan_startup;
    use dash_config::startup::FileData;

    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("docs-site/docs/operations/configuration-file.md");
    let page = std::fs::read_to_string(&path).expect("read the configuration file page");
    let start = page.find("```toml\n").expect("a toml example") + "```toml\n".len();
    let example = &page[start..start + page[start..].find("\n```").expect("end of example")];

    for service in Service::ALL {
        let env: HashMap<String, String> = [(
            "DASH_CONFIG_FILE".to_string(),
            "/etc/dash/dash.toml".to_string(),
        )]
        .into();
        let plan = plan_startup(service, &env, &|_| {
            Ok(FileData {
                content: example.to_string(),
                mode: Some(0o644),
            })
        });
        assert!(
            plan.report.errors.is_empty(),
            "{service:?}: {:?}",
            plan.report.errors
        );
    }
}
