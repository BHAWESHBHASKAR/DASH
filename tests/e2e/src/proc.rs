//! Child-process management for the real service binaries.

use std::fs;
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use rand::Rng;

use crate::http::Client;

/// A random lowercase-hex secret (48 chars, passes strict-secret checks).
pub fn random_secret() -> String {
    let mut rng = rand::thread_rng();
    (0..48)
        .map(|_| char::from_digit(rng.gen_range(0..16), 16).unwrap())
        .collect()
}

/// Reserve an ephemeral loopback port (bind to :0, read it, release it).
pub fn free_port() -> u16 {
    let l = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    l.local_addr().unwrap().port()
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("workspace root")
}

fn target_dir() -> PathBuf {
    std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| workspace_root().join("target"))
}

fn profile() -> &'static str {
    match std::env::var("DASH_E2E_PROFILE").as_deref() {
        Ok("release") => "release",
        _ => "debug",
    }
}

/// Directory holding private copies of the three service binaries.
///
/// The shared cargo target directory can be rebuilt by someone else at any
/// time, so the binaries are copied once per content version and the tests
/// run the copy. Set `DASH_E2E_BIN_DIR` to use pre-built binaries as is.
fn bin_dir() -> &'static PathBuf {
    static DIR: OnceLock<PathBuf> = OnceLock::new();
    DIR.get_or_init(|| {
        if let Some(dir) = std::env::var_os("DASH_E2E_BIN_DIR") {
            return PathBuf::from(dir);
        }
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
        let mut cmd = Command::new(cargo);
        cmd.current_dir(workspace_root())
            .args(["build", "-p", "ingestion", "-p", "retrieval", "-p", "control-plane"]);
        if profile() == "release" {
            cmd.arg("--release");
        }
        let status = cmd.status().expect("run cargo build for service binaries");
        assert!(status.success(), "cargo build of the service binaries failed");
        let src = target_dir().join(profile());
        let mut key = String::new();
        for name in ["ingestion", "retrieval", "control-plane"] {
            let meta = fs::metadata(src.join(name)).unwrap_or_else(|e| panic!("{name} binary missing: {e}"));
            let mtime = meta
                .modified()
                .ok()
                .and_then(|m| m.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_nanos())
                .unwrap_or(0);
            key.push_str(&format!("{mtime:x}-{:x}-", meta.len()));
        }
        let dst = std::env::temp_dir()
            .join("dash-e2e-bins")
            .join(profile())
            .join(key.trim_end_matches('-'));
        let marker = dst.join(".complete");
        if !marker.exists() {
            let tmp = dst.with_extension(format!("tmp{}", std::process::id()));
            fs::create_dir_all(&tmp).unwrap();
            for name in ["ingestion", "retrieval", "control-plane"] {
                fs::copy(src.join(name), tmp.join(name)).unwrap_or_else(|e| panic!("copy {name}: {e}"));
            }
            fs::write(tmp.join(".complete"), b"ok").unwrap();
            fs::create_dir_all(dst.parent().unwrap()).unwrap();
            if fs::rename(&tmp, &dst).is_err() {
                // Another process installed the same version first.
                let _ = fs::remove_dir_all(&tmp);
            }
        }
        dst
    })
}

pub fn bin_path(name: &str) -> PathBuf {
    bin_dir().join(name)
}

/// A running child process. Dropping it kills (SIGKILL) and reaps it.
pub struct Proc {
    pub name: String,
    child: Option<Child>,
    pub log_path: PathBuf,
}

impl Proc {
    /// Spawn `bin` with a cleared environment plus `envs`. stdout and stderr
    /// are appended to `log_path`.
    pub fn spawn(name: &str, bin: &str, args: &[&str], envs: &[(String, String)], log_path: &Path) -> Proc {
        let log = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(log_path)
            .expect("open log");
        let log2 = log.try_clone().unwrap();
        let mut cmd = Command::new(bin_path(bin));
        cmd.args(args)
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap_or_default())
            .envs(envs.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(Stdio::null())
            .stdout(Stdio::from(log))
            .stderr(Stdio::from(log2));
        let child = cmd.spawn().unwrap_or_else(|e| panic!("spawn {bin}: {e}"));
        Proc {
            name: name.to_string(),
            child: Some(child),
            log_path: log_path.to_path_buf(),
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.as_ref().map(|c| c.id()).unwrap_or(0)
    }

    pub fn log(&self) -> String {
        fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// `Some(status)` when the process has exited.
    pub fn try_exit(&mut self) -> Option<ExitStatus> {
        self.child.as_mut().and_then(|c| c.try_wait().ok().flatten())
    }

    pub fn is_alive(&mut self) -> bool {
        self.child.is_some() && self.try_exit().is_none()
    }

    /// Wait for exit up to `timeout`.
    pub fn wait_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let end = Instant::now() + timeout;
        while Instant::now() < end {
            if let Some(s) = self.try_exit() {
                return Some(s);
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        None
    }

    /// SIGKILL and reap.
    pub fn kill9(&mut self) {
        if let Some(mut c) = self.child.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }

    /// SIGTERM (graceful shutdown) and wait up to `timeout`; falls back to
    /// SIGKILL. Returns true when the process exited on its own.
    pub fn terminate(&mut self, timeout: Duration) -> bool {
        let Some(mut c) = self.child.take() else { return true };
        let _ = Command::new("kill")
            .args(["-TERM", &c.id().to_string()])
            .status();
        let end = Instant::now() + timeout;
        while Instant::now() < end {
            if matches!(c.try_wait(), Ok(Some(_))) {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        let _ = c.kill();
        let _ = c.wait();
        false
    }

    /// Wait until `GET /live` answers 200 on `addr`, or panic (with the
    /// process log) if it exits or the deadline passes.
    pub fn wait_live(&mut self, addr: SocketAddr, path: &str, timeout: Duration) {
        let end = Instant::now() + timeout;
        let mut client = Client::new(addr);
        client.timeout = Duration::from_secs(2);
        loop {
            if let Some(status) = self.try_exit() {
                panic!(
                    "{} exited during startup with {status}\n--- log ---\n{}",
                    self.name,
                    self.log()
                );
            }
            if let Ok(r) = client.request("GET", path, &[], None)
                && r.status == 200
            {
                return;
            }
            if Instant::now() > end {
                panic!(
                    "{} did not become live on {addr} within {timeout:?}\n--- log ---\n{}",
                    self.name,
                    self.log()
                );
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.kill9();
    }
}
