//! Scenario 9: typed configuration seen from outside. The real binaries run
//! the startup check: malformed settings stop them with exit code 2 and a
//! message naming every problem, `DASH_CONFIG_VALIDATION=warn` lets them start,
//! and a TOML file named by `DASH_CONFIG_FILE` fills settings the environment
//! leaves unset.

use std::net::SocketAddr;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::time::Duration;

use dash_e2e::*;

fn dev_env(bind_var: &str, addr: SocketAddr) -> Vec<(String, String)> {
    vec![
        ("DASH_INSECURE_DEV_MODE".into(), "1".into()),
        (bind_var.into(), addr.to_string()),
    ]
}

fn addr() -> SocketAddr {
    format!("127.0.0.1:{}", free_port()).parse().unwrap()
}

fn exit_code(proc: &mut Proc) -> Option<i32> {
    proc.wait_exit(Duration::from_secs(20))
        .and_then(|s| s.code())
}

#[test]
fn ingestion_refuses_to_start_on_malformed_settings_and_lists_every_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut env = dev_env("DASH_INGEST_BIND", addr());
    env.push(("DASH_INGEST_HTTP_WORKERS".into(), "abc".into()));
    env.push(("DASH_EMBEDDING_PROVIDER".into(), "olama".into()));
    let mut p = Proc::spawn(
        "ingestion",
        "ingestion",
        &[],
        &env,
        &dir.path().join("i.log"),
    );
    assert_eq!(exit_code(&mut p), Some(2), "{}", p.log());
    let log = p.log();
    assert!(log.contains("refusing to start"), "{log}");
    assert!(log.contains("DASH_INGEST_HTTP_WORKERS"), "{log}");
    assert!(log.contains("DASH_EMBEDDING_PROVIDER"), "{log}");
}

#[test]
fn retrieval_refuses_to_start_on_a_bare_port_bind() {
    let dir = tempfile::tempdir().unwrap();
    let env = vec![
        ("DASH_INSECURE_DEV_MODE".to_string(), "1".to_string()),
        ("DASH_RETRIEVAL_BIND".to_string(), "8080".to_string()),
    ];
    let mut p = Proc::spawn(
        "retrieval",
        "retrieval",
        &[],
        &env,
        &dir.path().join("r.log"),
    );
    assert_eq!(exit_code(&mut p), Some(2), "{}", p.log());
    assert!(p.log().contains("DASH_RETRIEVAL_BIND"), "{}", p.log());
}

#[test]
fn control_plane_refuses_to_start_on_a_malformed_setting() {
    let dir = tempfile::tempdir().unwrap();
    let mut env = dev_env("DASH_CONTROL_PLANE_BIND", addr());
    env.push(("DASH_CONTROL_PLANE_QUEUE_DEPTH".into(), "lots".into()));
    let mut p = Proc::spawn(
        "control-plane",
        "control-plane",
        &[],
        &env,
        &dir.path().join("c.log"),
    );
    assert_eq!(exit_code(&mut p), Some(2), "{}", p.log());
    assert!(
        p.log().contains("DASH_CONTROL_PLANE_QUEUE_DEPTH"),
        "{}",
        p.log()
    );
}

#[test]
fn warn_mode_starts_the_service_and_logs_the_problem() {
    let dir = tempfile::tempdir().unwrap();
    let a = addr();
    let mut env = dev_env("DASH_INGEST_BIND", a);
    env.push(("DASH_CONFIG_VALIDATION".into(), "warn".into()));
    env.push(("DASH_INGEST_HTTP_QUEUE_CAPACITY".into(), "many".into()));
    let mut p = Proc::spawn(
        "ingestion",
        "ingestion",
        &[],
        &env,
        &dir.path().join("i.log"),
    );
    p.wait_live(a, "/live", Duration::from_secs(30));
    assert!(
        p.log().contains("DASH_INGEST_HTTP_QUEUE_CAPACITY"),
        "{}",
        p.log()
    );
}

#[test]
fn a_misspelled_variable_is_reported_with_a_suggestion_but_does_not_stop_startup() {
    let dir = tempfile::tempdir().unwrap();
    let a = addr();
    let mut env = dev_env("DASH_INGEST_BIND", a);
    env.push((
        "DASH_INGEST_WAL_PAHT".into(),
        dir.path().join("x.wal").display().to_string(),
    ));
    let mut p = Proc::spawn(
        "ingestion",
        "ingestion",
        &[],
        &env,
        &dir.path().join("i.log"),
    );
    p.wait_live(a, "/live", Duration::from_secs(30));
    let log = p.log();
    assert!(log.contains("DASH_INGEST_WAL_PAHT"), "{log}");
    assert!(log.contains("did you mean DASH_INGEST_WAL_PATH"), "{log}");
}

fn write_private(path: &Path, content: &str, mode: u32) {
    std::fs::write(path, content).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

#[test]
fn a_toml_file_configures_the_control_plane_end_to_end() {
    let dir = tempfile::tempdir().unwrap();
    let a = addr();
    let token = random_secret();
    let file = dir.path().join("dash.toml");
    write_private(
        &file,
        &format!(
            "[control_plane]\nbind = \"{a}\"\nnode_id = \"cp-from-file\"\ntoken = \"{token}\"\n"
        ),
        0o600,
    );
    // The environment only names the file.
    let env = vec![("DASH_CONFIG_FILE".to_string(), file.display().to_string())];
    let mut p = Proc::spawn(
        "control-plane",
        "control-plane",
        &[],
        &env,
        &dir.path().join("c.log"),
    );
    p.wait_live(a, "/v1/control-plane/health", Duration::from_secs(20));
    // The token from the file is enforced by the real server.
    let client = Client::new(a);
    let denied = client.get("/v1/control-plane/leader", &[]);
    assert_eq!(denied.status, 401, "{}", p.log());
    let ok = client.get(
        "/v1/control-plane/leader",
        &[("Authorization", &format!("Bearer {token}"))],
    );
    assert_eq!(ok.status, 200);
    assert!(!p.log().contains(&token), "token leaked into the log");
}

#[test]
fn the_environment_wins_over_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let from_env = addr();
    let from_file = addr();
    let file = dir.path().join("dash.toml");
    write_private(
        &file,
        &format!("[ingestion]\nbind = \"{from_file}\"\n"),
        0o644,
    );
    let mut env = dev_env("DASH_INGEST_BIND", from_env);
    env.push(("DASH_CONFIG_FILE".into(), file.display().to_string()));
    let mut p = Proc::spawn(
        "ingestion",
        "ingestion",
        &[],
        &env,
        &dir.path().join("i.log"),
    );
    p.wait_live(from_env, "/live", Duration::from_secs(30));
}

#[test]
fn a_world_readable_file_with_secrets_stops_the_service() {
    let dir = tempfile::tempdir().unwrap();
    let a = addr();
    let secret = random_secret();
    let file = dir.path().join("dash.toml");
    write_private(
        &file,
        &format!("[ingestion]\nbind = \"{a}\"\napi_key = \"{secret}\"\n"),
        0o644,
    );
    let env = vec![("DASH_CONFIG_FILE".to_string(), file.display().to_string())];
    let mut p = Proc::spawn(
        "ingestion",
        "ingestion",
        &[],
        &env,
        &dir.path().join("i.log"),
    );
    assert_eq!(exit_code(&mut p), Some(2), "{}", p.log());
    let log = p.log();
    assert!(log.contains("0600"), "{log}");
    assert!(!log.contains(&secret), "secret leaked into the log");
}

#[test]
fn an_unknown_key_in_the_file_stops_the_service_with_a_suggestion() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("dash.toml");
    write_private(&file, "[retrieval]\nmax_topk = 5\n", 0o644);
    let mut env = dev_env("DASH_RETRIEVAL_BIND", addr());
    env.push(("DASH_CONFIG_FILE".into(), file.display().to_string()));
    let mut p = Proc::spawn(
        "retrieval",
        "retrieval",
        &[],
        &env,
        &dir.path().join("r.log"),
    );
    assert_eq!(exit_code(&mut p), Some(2), "{}", p.log());
    assert!(p.log().contains("did you mean 'max_top_k'"), "{}", p.log());
}
