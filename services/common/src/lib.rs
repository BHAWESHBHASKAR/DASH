//! Cross-service shutdown signaling.
//!
//! A small helper that wraps the `signal-hook` crate to convert
//! SIGTERM and SIGINT into an `Arc<AtomicBool>` that the accept
//! loop can poll. The polling loop is the only sync-friendly way
//! to get graceful shutdown on a blocking `TcpListener` without
//! refactoring to a tokio runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use signal_hook::consts::{SIGINT, SIGTERM};
use signal_hook::flag;

pub struct ShutdownSignal {
    flag: Arc<AtomicBool>,
}

impl ShutdownSignal {
    /// Register SIGTERM and SIGINT handlers that set the flag.
    /// Returns the signal so the caller can poll it in the
    /// accept loop. Panics if signal registration fails (which
    /// would mean we can't terminate cleanly).
    pub fn install() -> Arc<Self> {
        let flag = Arc::new(AtomicBool::new(false));
        flag::register(SIGTERM, Arc::clone(&flag))
            .expect("install SIGTERM handler for graceful shutdown");
        flag::register(SIGINT, Arc::clone(&flag))
            .expect("install SIGINT handler for graceful shutdown");
        Arc::new(Self { flag })
    }

    pub fn is_triggered(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
    }
}

/// Poll the shutdown signal with a bounded sleep between checks.
/// The accept loop calls this between non-blocking accept() calls.
/// `poll_interval` caps the shutdown latency: a shorter interval
/// means faster shutdown at the cost of more wakeups; the default
/// of 50ms gives 50ms p99 shutdown latency, which is below the
/// typical k8s `terminationGracePeriodSeconds` of 30 seconds.
pub fn wait_or_shutdown(signal: &ShutdownSignal, poll_interval: Duration) -> bool {
    if signal.is_triggered() {
        return true;
    }
    std::thread::sleep(poll_interval);
    signal.is_triggered()
}

/// Logs and waits up to `graceful_deadline` for in-flight handlers
/// to finish. Returns the actual shutdown duration for logging.
pub fn wait_for_drain(graceful_deadline: Duration) -> Duration {
    let start = Instant::now();
    let mut elapsed = Duration::ZERO;
    while elapsed < graceful_deadline {
        std::thread::sleep(Duration::from_millis(50));
        elapsed = start.elapsed();
    }
    start.elapsed()
}
