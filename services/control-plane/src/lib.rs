use std::{
    collections::HashMap,
    fs,
    io::Write,
    net::{IpAddr, TcpListener},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use auth::sha256_hex;
use metadata_router::{
    PlacementRouteError, ReplicaRole, ShardPlacement, parse_shard_placements_csv,
    promote_replica_to_leader, render_shard_placements_csv,
};

pub mod leader;
pub mod server;

pub use server::ServerConfig;
#[cfg(test)]
use server::find_header_end;

use dash_http::json_escape;
type HttpRequest = dash_http::Request;

/// Authentication policy for the control-plane HTTP API.
///
/// The default is [`AuthMode::Deny`]: a state object that was never given a
/// token rejects every protected route instead of serving it openly.
#[derive(Clone, Default, PartialEq, Eq)]
pub enum AuthMode {
    /// No credentials configured: every protected route is refused.
    #[default]
    Deny,
    /// Require `Authorization: Bearer <token>` on protected routes.
    Token(String),
    /// Explicit local-development escape hatch (`DASH_INSECURE_DEV_MODE=1`).
    InsecureDev,
}

impl std::fmt::Debug for AuthMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AuthMode::Deny => f.write_str("Deny"),
            AuthMode::Token(_) => f.write_str("Token(<redacted>)"),
            AuthMode::InsecureDev => f.write_str("InsecureDev"),
        }
    }
}

impl AuthMode {
    fn authorize(&self, authorization_header: Option<&str>) -> Result<(), HttpResponse> {
        match self {
            AuthMode::InsecureDev => Ok(()),
            AuthMode::Deny => Err(HttpResponse::error(
                403,
                "control-plane authentication is not configured; set DASH_CONTROL_PLANE_TOKEN",
            )),
            AuthMode::Token(expected) => {
                let presented = authorization_header.and_then(|value| {
                    let value = value.trim();
                    let (scheme, token) = value.split_once(' ')?;
                    scheme
                        .eq_ignore_ascii_case("bearer")
                        .then_some(token.trim())
                });
                match presented {
                    Some(token) if constant_time_eq(token.as_bytes(), expected.as_bytes()) => {
                        Ok(())
                    }
                    _ => Err(HttpResponse::error(401, "missing or invalid bearer token")
                        .with_header("WWW-Authenticate", "Bearer")),
                }
            }
        }
    }
}

/// Compare secrets without an early exit on the first differing byte. Both
/// inputs are hashed first so the comparison time does not depend on the
/// length of the expected token either.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let a = sha256_hex(a);
    let b = sha256_hex(b);
    let mut diff = 0u8;
    for (x, y) in a.bytes().zip(b.bytes()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Result of validating startup security configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecurityConfig {
    pub auth: AuthMode,
    /// Address the server must bind (may differ from the requested one in
    /// insecure dev mode, where only loopback is permitted).
    pub bind_addr: String,
    /// Warnings to log loudly at startup.
    pub warnings: Vec<String>,
}

/// Decide the auth mode and bind address from the environment-provided
/// token and dev-mode flag. Refuses to run without a token unless
/// `insecure_dev` is set, and then restricts the listener to loopback.
pub fn resolve_security(
    token: Option<&str>,
    insecure_dev: bool,
    bind_addr: &str,
) -> Result<SecurityConfig, String> {
    // Strict secret validation is on by default and can only be relaxed in
    // explicit dev mode (`DASH_STRICT_SECRETS=0` together with
    // `DASH_INSECURE_DEV_MODE=1`), exactly like the data-plane services.
    let relaxed = insecure_dev
        && matches!(
            std::env::var("DASH_STRICT_SECRETS")
                .ok()
                .map(|v| v.trim().to_ascii_lowercase())
                .as_deref(),
            Some("0" | "false" | "no" | "off")
        );
    resolve_security_with(token, insecure_dev, !relaxed, bind_addr)
}

/// Minimum length of the control-plane bearer token under strict secrets.
pub const TOKEN_MIN_LENGTH: usize = 32;

/// [`resolve_security`] with the strict-secrets decision made by the caller.
pub fn resolve_security_with(
    token: Option<&str>,
    insecure_dev: bool,
    strict_secrets: bool,
    bind_addr: &str,
) -> Result<SecurityConfig, String> {
    let token = token.map(str::trim).filter(|value| !value.is_empty());
    if let Some(token) = token {
        if strict_secrets {
            dash_common::validate_secret_min_len(
                token,
                "DASH_CONTROL_PLANE_TOKEN",
                TOKEN_MIN_LENGTH,
            )?;
        }
        return Ok(SecurityConfig {
            auth: AuthMode::Token(token.to_string()),
            bind_addr: bind_addr.to_string(),
            warnings: Vec::new(),
        });
    }
    if !insecure_dev {
        return Err(
            "DASH_CONTROL_PLANE_TOKEN is required; set it, or set DASH_INSECURE_DEV_MODE=1 for local development only"
                .to_string(),
        );
    }
    let (host, port) = bind_addr
        .rsplit_once(':')
        .ok_or_else(|| format!("control-plane bind address '{bind_addr}' must be host:port"))?;
    let host = host.trim_start_matches('[').trim_end_matches(']');
    let is_loopback = host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    let mut warnings = vec![
        "DASH_INSECURE_DEV_MODE=1: control-plane authentication is DISABLED. Never use this outside local development."
            .to_string(),
    ];
    let bind = if is_loopback {
        bind_addr.to_string()
    } else {
        warnings.push(format!(
            "insecure dev mode only binds to 127.0.0.1; overriding requested bind host '{host}'"
        ));
        format!("127.0.0.1:{port}")
    };
    Ok(SecurityConfig {
        auth: AuthMode::InsecureDev,
        bind_addr: bind,
        warnings,
    })
}

/// Resolve the leader-election node id. Outside dev mode an explicit
/// `DASH_CONTROL_PLANE_NODE_ID` is required: the old `control-plane-<pid>`
/// default collides between containers (every container is pid 1) and would
/// let two nodes act as the same lease holder.
pub fn resolve_node_id(configured: Option<&str>, insecure_dev: bool) -> Result<String, String> {
    match configured.map(str::trim).filter(|value| !value.is_empty()) {
        Some(node_id) => Ok(node_id.to_string()),
        None if insecure_dev => Ok(format!("control-plane-{}", std::process::id())),
        None => Err(
            "DASH_CONTROL_PLANE_NODE_ID is required (a unique, stable id per control-plane node); \
             set it, or set DASH_INSECURE_DEV_MODE=1 for local development only"
                .to_string(),
        ),
    }
}

/// Failed bearer-token attempts allowed per peer address and window.
const AUTH_FAILURE_LIMIT: u32 = 10;
const AUTH_FAILURE_WINDOW: Duration = Duration::from_secs(60);
const AUTH_FAILURE_MAX_PEERS: usize = 10_000;

/// Throttles credential guessing: after [`AUTH_FAILURE_LIMIT`] failed
/// attempts within a minute a peer address gets 429 with `Retry-After` until
/// its window ends.
#[derive(Default)]
pub struct AuthFailureThrottle {
    peers: Mutex<HashMap<IpAddr, (Instant, u32)>>,
}

impl AuthFailureThrottle {
    /// Seconds the peer must wait, or `None` when it may attempt.
    pub fn blocked_for(&self, peer: IpAddr, now: Instant) -> Option<u64> {
        let peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        let (started, failures) = peers.get(&peer)?;
        let elapsed = now.saturating_duration_since(*started);
        if *failures >= AUTH_FAILURE_LIMIT && elapsed < AUTH_FAILURE_WINDOW {
            Some((AUTH_FAILURE_WINDOW - elapsed).as_secs().max(1))
        } else {
            None
        }
    }

    pub fn record_failure(&self, peer: IpAddr, now: Instant) {
        let mut peers = self.peers.lock().unwrap_or_else(|p| p.into_inner());
        if peers.len() >= AUTH_FAILURE_MAX_PEERS && !peers.contains_key(&peer) {
            peers.retain(|_, (started, _)| {
                now.saturating_duration_since(*started) < AUTH_FAILURE_WINDOW
            });
            if peers.len() >= AUTH_FAILURE_MAX_PEERS {
                // Still full of live windows: drop the oldest entry.
                if let Some(oldest) = peers
                    .iter()
                    .min_by_key(|(_, (started, _))| *started)
                    .map(|(ip, _)| *ip)
                {
                    peers.remove(&oldest);
                }
            }
        }
        let entry = peers.entry(peer).or_insert((now, 0));
        if now.saturating_duration_since(entry.0) >= AUTH_FAILURE_WINDOW {
            *entry = (now, 0);
        }
        entry.1 = entry.1.saturating_add(1);
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlanePersistence {
    state_path: PathBuf,
    checksum_path: Option<PathBuf>,
}

impl ControlPlanePersistence {
    pub fn new(state_path: PathBuf, checksum_path: Option<PathBuf>) -> Result<Self, String> {
        if state_path.as_os_str().is_empty() {
            return Err("control-plane persistence state path must not be empty".to_string());
        }
        Ok(Self {
            state_path,
            checksum_path,
        })
    }

    pub fn state_path(&self) -> &Path {
        &self.state_path
    }

    pub fn checksum_path(&self) -> Option<&Path> {
        self.checksum_path.as_deref()
    }
}

/// Whether this node may currently act as leader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaderStatus {
    /// Leader. `fencing_token` is the lease epoch (`None` when standalone).
    Leader {
        fencing_token: Option<u64>,
    },
    Follower,
}

/// Outcome of one lease maintenance step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LeaseTick {
    Leader {
        fencing_token: Option<u64>,
        newly_acquired: bool,
    },
    Follower,
}

type LagKey = (String, u32, String);

#[derive(Default)]
pub struct ControlPlanePlacementState {
    placements: Vec<ShardPlacement>,
    persistence: Option<ControlPlanePersistence>,
    lease: Option<Arc<leader::LeaderLease>>,
    auth: AuthMode,
    /// Last reported replication lag (in records) per replica. In-memory
    /// only: after a restart or leader change every lag is "unknown" and a
    /// promotion needs `force=true`.
    replica_lag: HashMap<LagKey, u64>,
    /// Fencing token for which in-memory placements were last synced from
    /// disk. A mismatch with the live lease epoch forces a reload before the
    /// node serves or persists anything.
    synced_lease_epoch: Option<u64>,
    /// Failed-token throttle shared by every request served from this state.
    auth_throttle: Arc<AuthFailureThrottle>,
}

impl std::fmt::Debug for ControlPlanePlacementState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlanePlacementState")
            .field("placements", &self.placements)
            .field("persistence", &self.persistence)
            .field("lease", &self.lease.as_ref().map(|_| "..."))
            .field("auth", &self.auth)
            .finish()
    }
}

impl PartialEq for ControlPlanePlacementState {
    fn eq(&self, other: &Self) -> bool {
        self.placements == other.placements && self.persistence == other.persistence
    }
}

impl Eq for ControlPlanePlacementState {}

impl ControlPlanePlacementState {
    pub fn new(placements: Vec<ShardPlacement>) -> Self {
        Self {
            placements,
            ..Self::default()
        }
    }

    pub fn with_persistence(mut self, persistence: ControlPlanePersistence) -> Self {
        self.persistence = Some(persistence);
        self
    }

    pub fn with_lease(mut self, lease: Arc<leader::LeaderLease>) -> Self {
        self.lease = Some(lease);
        self
    }

    pub fn with_auth(mut self, auth: AuthMode) -> Self {
        self.auth = auth;
        self
    }

    pub fn with_auth_token(self, token: impl Into<String>) -> Self {
        self.with_auth(AuthMode::Token(token.into()))
    }

    pub fn auth_mode(&self) -> &AuthMode {
        &self.auth
    }

    pub fn lease(&self) -> Option<Arc<leader::LeaderLease>> {
        self.lease.clone()
    }

    pub fn load_persisted_csv(path: &Path) -> Result<Vec<ShardPlacement>, String> {
        let csv = fs::read_to_string(path).map_err(|err| {
            format!(
                "failed reading control-plane state '{}': {err}",
                path.display()
            )
        })?;
        parse_shard_placements_csv(&csv)
    }

    pub fn verify_checksum_if_configured(&self) -> Result<(), String> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        let Some(checksum_path) = persistence.checksum_path() else {
            return Ok(());
        };
        if !persistence.state_path().exists() {
            return Ok(());
        }
        if !checksum_path.exists() {
            return Err(format!(
                "control-plane checksum file '{}' is missing",
                checksum_path.display()
            ));
        }
        let state_bytes = fs::read(persistence.state_path()).map_err(|err| {
            format!(
                "failed reading control-plane state '{}' for checksum verification: {err}",
                persistence.state_path().display()
            )
        })?;
        let expected = fs::read_to_string(checksum_path).map_err(|err| {
            format!(
                "failed reading control-plane checksum '{}': {err}",
                checksum_path.display()
            )
        })?;
        let actual = sha256_hex(&state_bytes);
        if actual != expected.trim() {
            return Err(format!(
                "control-plane checksum mismatch for '{}' (expected {}, actual {})",
                persistence.state_path().display(),
                expected.trim(),
                actual
            ));
        }
        Ok(())
    }

    pub fn placements(&self) -> &[ShardPlacement] {
        &self.placements
    }

    pub fn highest_epoch(&self) -> u64 {
        self.placements
            .iter()
            .map(|placement| placement.epoch)
            .max()
            .unwrap_or(0)
    }

    pub fn persistence_state_path(&self) -> Option<&Path> {
        self.persistence.as_ref().map(|cfg| cfg.state_path())
    }

    pub fn is_leader(&self) -> Result<bool, String> {
        match &self.lease {
            Some(lease) => lease.is_leader(),
            None => Ok(true),
        }
    }

    pub fn current_leader_info(&self) -> Result<Option<leader::LeaseRecord>, String> {
        match &self.lease {
            Some(lease) => lease.current_leader(),
            None => Ok(Some(leader::LeaseRecord {
                node_id: self.local_node_id_or_unknown(),
                epoch: self.highest_epoch(),
                expires_at_ms: u64::MAX,
                instance_id: String::new(),
            })),
        }
    }

    /// The fencing token this node currently holds, if it is the leader.
    pub fn fencing_token(&self) -> Result<Option<u64>, String> {
        match &self.lease {
            Some(lease) => lease.fencing_token(),
            None => Ok(None),
        }
    }

    pub fn try_acquire_leader(&self, epoch: u64) -> Result<bool, String> {
        match &self.lease {
            Some(lease) => lease.try_acquire(epoch),
            None => Ok(true),
        }
    }

    pub fn renew_leader(&self, epoch: u64) -> Result<bool, String> {
        match &self.lease {
            Some(lease) => lease.renew(epoch),
            None => Ok(true),
        }
    }

    pub fn local_node_id_or_unknown(&self) -> String {
        match &self.lease {
            Some(lease) => lease.node_id().to_string(),
            None => "standalone".to_string(),
        }
    }

    /// Reload the persisted placements from disk (the shared source of truth
    /// when another node was leader) after verifying the checksum. Epoch
    /// regressions are refused. A missing state file is not an error.
    pub fn reload_from_disk(&mut self) -> Result<(), String> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        if !persistence.state_path().exists() {
            return Ok(());
        }
        self.verify_checksum_if_configured()?;
        let loaded = Self::load_persisted_csv(
            self.persistence
                .as_ref()
                .map_or_else(|| Path::new(""), ControlPlanePersistence::state_path),
        )?;
        self.replace_placements_monotonic(loaded)
    }

    /// Verify leadership and make sure in-memory placements reflect the
    /// latest persisted state for the current fencing token. Call before
    /// serving placement data or persisting anything.
    pub fn ensure_leader_synced(&mut self) -> Result<LeaderStatus, String> {
        let Some(lease) = self.lease.clone() else {
            return Ok(LeaderStatus::Leader {
                fencing_token: None,
            });
        };
        match lease.fencing_token()? {
            None => {
                self.synced_lease_epoch = None;
                Ok(LeaderStatus::Follower)
            }
            Some(token) => {
                self.sync_for_token(token)?;
                Ok(LeaderStatus::Leader {
                    fencing_token: Some(token),
                })
            }
        }
    }

    fn sync_for_token(&mut self, token: u64) -> Result<(), String> {
        if self.synced_lease_epoch != Some(token) {
            self.synced_lease_epoch = None;
            self.reload_from_disk()
                .map_err(|err| format!("failed syncing placements after acquiring lease: {err}"))?;
            self.synced_lease_epoch = Some(token);
        }
        Ok(())
    }

    /// Try to acquire (or extend) the lease and, when leadership is newly
    /// obtained, reload persisted placements before returning.
    pub fn try_acquire_and_sync(&mut self) -> Result<LeaderStatus, String> {
        let Some(lease) = self.lease.clone() else {
            return Ok(LeaderStatus::Leader {
                fencing_token: None,
            });
        };
        match lease.acquire()? {
            Some(acquisition) => {
                if acquisition.newly_acquired {
                    self.synced_lease_epoch = None;
                }
                self.sync_for_token(acquisition.record.epoch)?;
                Ok(LeaderStatus::Leader {
                    fencing_token: Some(acquisition.record.epoch),
                })
            }
            None => {
                self.synced_lease_epoch = None;
                Ok(LeaderStatus::Follower)
            }
        }
    }

    /// One maintenance step: extend the lease if held, otherwise try to
    /// acquire it; reload persisted state whenever leadership is new.
    pub fn maintain_lease(&mut self) -> Result<LeaseTick, String> {
        let Some(lease) = self.lease.clone() else {
            return Ok(LeaseTick::Leader {
                fencing_token: None,
                newly_acquired: false,
            });
        };
        match lease.acquire()? {
            Some(acquisition) => {
                let was_synced = self.synced_lease_epoch == Some(acquisition.record.epoch);
                if acquisition.newly_acquired {
                    self.synced_lease_epoch = None;
                }
                self.sync_for_token(acquisition.record.epoch)?;
                Ok(LeaseTick::Leader {
                    fencing_token: Some(acquisition.record.epoch),
                    newly_acquired: acquisition.newly_acquired || !was_synced,
                })
            }
            None => {
                self.synced_lease_epoch = None;
                Ok(LeaseTick::Follower)
            }
        }
    }

    pub fn replace_placements_monotonic(
        &mut self,
        candidate: Vec<ShardPlacement>,
    ) -> Result<(), String> {
        metadata_router::ensure_no_epoch_regression(&self.placements, &candidate)?;
        self.placements = candidate;
        Ok(())
    }

    pub fn replace_placements_from_csv_monotonic(&mut self, csv: &str) -> Result<(), String> {
        let candidate = parse_shard_placements_csv(csv)?;
        self.replace_placements_monotonic(candidate)
    }

    pub fn cas_matches(&self, expected_epoch: Option<u64>) -> Result<(), String> {
        if let Some(expected_epoch) = expected_epoch {
            let current_epoch = self.highest_epoch();
            if expected_epoch != current_epoch {
                return Err(format!(
                    "stale placement epoch: expected={}, current={}",
                    expected_epoch, current_epoch
                ));
            }
        }
        Ok(())
    }

    /// Record the replication lag (records behind the shard leader) a
    /// replica reported. Lag `0` means fully caught up.
    pub fn report_replica_lag(
        &mut self,
        tenant_id: &str,
        shard_id: u32,
        node_id: &str,
        lag: u64,
    ) -> Result<(), String> {
        let placement = self
            .placements
            .iter()
            .find(|placement| placement.tenant_id == tenant_id && placement.shard_id == shard_id)
            .ok_or_else(|| {
                format!(
                    "placement not found for tenant '{}' shard {}",
                    tenant_id, shard_id
                )
            })?;
        if !placement
            .replicas
            .iter()
            .any(|replica| replica.node_id == node_id)
        {
            return Err(format!(
                "replica '{}' not found in shard {}",
                node_id, shard_id
            ));
        }
        self.replica_lag
            .insert((tenant_id.to_string(), shard_id, node_id.to_string()), lag);
        Ok(())
    }

    pub fn replica_lag(&self, tenant_id: &str, shard_id: u32, node_id: &str) -> Option<u64> {
        self.replica_lag
            .get(&(tenant_id.to_string(), shard_id, node_id.to_string()))
            .copied()
    }

    /// Promote `node_id` to shard leader. Unless `force` is set, the replica
    /// must have reported a lag of exactly zero: promoting a replica whose
    /// lag is unknown or non-zero can discard acknowledged writes.
    pub fn promote_replica(
        &mut self,
        tenant_id: &str,
        shard_id: u32,
        node_id: &str,
        force: bool,
    ) -> Result<u64, String> {
        let lag = self.replica_lag(tenant_id, shard_id, node_id);
        let placement = self
            .placements
            .iter_mut()
            .find(|placement| placement.tenant_id == tenant_id && placement.shard_id == shard_id)
            .ok_or_else(|| {
                format!(
                    "placement not found for tenant '{}' shard {}",
                    tenant_id, shard_id
                )
            })?;

        let already_leader = placement
            .replicas
            .iter()
            .any(|replica| replica.node_id == node_id && replica.role == ReplicaRole::Leader);
        if !force && !already_leader && placement.replicas.iter().any(|r| r.node_id == node_id) {
            match lag {
                Some(0) => {}
                Some(behind) => {
                    return Err(format!(
                        "replica '{}' is {} records behind; refusing promotion (pass force=true to override)",
                        node_id, behind
                    ));
                }
                None => {
                    return Err(format!(
                        "replica '{}' has no reported replication lag; refusing promotion (report lag or pass force=true)",
                        node_id
                    ));
                }
            }
        }

        let epoch = promote_replica_to_leader(placement, node_id).map_err(|err| match err {
            PlacementRouteError::ReplicaNotFound { .. } => {
                format!("replica '{}' not found in shard {}", node_id, shard_id)
            }
            PlacementRouteError::ReplicaUnhealthy { .. } => {
                format!("replica '{}' is not promotable due to health", node_id)
            }
            other => format!("promotion failed: {other:?}"),
        })?;
        if !already_leader {
            // Lag figures were relative to the old leader and are now moot.
            self.replica_lag
                .retain(|(tenant, shard, _), _| !(tenant == tenant_id && *shard == shard_id));
        }
        Ok(epoch)
    }

    pub fn persist_if_configured(&self) -> Result<(), String> {
        let Some(persistence) = self.persistence.as_ref() else {
            return Ok(());
        };
        let csv = render_shard_placements_csv(&self.placements);
        persist_atomically(persistence.state_path(), csv.as_bytes())?;
        if let Some(checksum_path) = persistence.checksum_path() {
            let digest = sha256_hex(csv.as_bytes());
            persist_atomically(checksum_path, format!("{digest}\n").as_bytes())?;
        }
        Ok(())
    }

    fn render_placement_json(&self) -> String {
        let placements_json = self
            .placements
            .iter()
            .map(|placement| {
                let replicas_json = placement
                    .replicas
                    .iter()
                    .map(|replica| {
                        let lag_json = self
                            .replica_lag(&placement.tenant_id, placement.shard_id, &replica.node_id)
                            .map(|lag| format!(",\"replica_lag\":{lag}"))
                            .unwrap_or_default();
                        format!(
                            "{{\"node_id\":\"{}\",\"role\":\"{}\",\"health\":\"{}\"{}}}",
                            json_escape(&replica.node_id),
                            replica_role_str(replica.role),
                            replica_health_str(replica.health),
                            lag_json,
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",");
                format!(
                    "{{\"tenant_id\":\"{}\",\"shard_id\":{},\"epoch\":{},\"replicas\":[{}]}}",
                    json_escape(&placement.tenant_id),
                    placement.shard_id,
                    placement.epoch,
                    replicas_json,
                )
            })
            .collect::<Vec<_>>()
            .join(",");

        format!(
            "{{\"placements\":[{}],\"placement_count\":{}}}",
            placements_json,
            self.placements.len()
        )
    }
}

/// Drives lease renewal and re-acquisition for a node. Unlike the previous
/// renewal thread it never exits: after losing leadership it keeps trying to
/// re-acquire with exponential backoff, reloading persisted placements each
/// time it becomes leader again.
pub struct LeaseMaintainer {
    state: Arc<Mutex<ControlPlanePlacementState>>,
    backoff: leader::Backoff,
    renewal_interval: Duration,
    was_leader: Option<bool>,
}

impl LeaseMaintainer {
    pub fn new(
        state: Arc<Mutex<ControlPlanePlacementState>>,
        renewal_interval: Duration,
        lease_duration: Duration,
    ) -> Self {
        let max_backoff = (lease_duration / 2).max(renewal_interval);
        Self {
            state,
            backoff: leader::Backoff::new(renewal_interval, max_backoff),
            renewal_interval,
            was_leader: None,
        }
    }

    /// Run one maintenance step and return how long to wait before the next.
    pub fn step(&mut self) -> Duration {
        let result = match self.state.lock() {
            Ok(mut guard) => guard.maintain_lease(),
            Err(_) => Err("control-plane state lock poisoned".to_string()),
        };
        match result {
            Ok(LeaseTick::Leader {
                fencing_token: lease_epoch,
                newly_acquired,
            }) => {
                if newly_acquired || self.was_leader != Some(true) {
                    // The fencing value is a lease epoch counter, not a credential.
                    eprintln!(
                        "control-plane acquired leader lease (epoch {})",
                        lease_epoch.map_or_else(|| "none".to_string(), |epoch| epoch.to_string())
                    );
                }
                self.was_leader = Some(true);
                self.backoff.reset();
                self.renewal_interval
            }
            Ok(LeaseTick::Follower) => {
                if self.was_leader != Some(false) {
                    eprintln!(
                        "control-plane is not the leader; will keep retrying acquisition with backoff"
                    );
                }
                self.was_leader = Some(false);
                self.backoff.next_delay()
            }
            Err(err) => {
                eprintln!("control-plane lease maintenance failed: {err}");
                self.backoff.next_delay()
            }
        }
    }

    /// Loop forever.
    pub fn run(mut self) -> ! {
        loop {
            let delay = self.step();
            std::thread::sleep(delay);
        }
    }
}

/// Serve on `bind_addr` with configuration from the environment.
pub fn serve_http(
    bind_addr: &str,
    state: Arc<Mutex<ControlPlanePlacementState>>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(bind_addr)?;
    serve_listener(listener, state, ServerConfig::from_env())
}

/// Serve connections from `listener` using a bounded worker pool. When all
/// workers are busy and the queue is full, new connections receive an
/// immediate 503 instead of spawning unbounded threads.
pub fn serve_listener(
    listener: TcpListener,
    state: Arc<Mutex<ControlPlanePlacementState>>,
    config: ServerConfig,
) -> std::io::Result<()> {
    let handler: dash_http::Handler = Arc::new(move |request| {
        let peer = request.peer.map(|addr| addr.ip());
        handle_request(&state, request, peer).into()
    });
    let mut http = config.to_http();
    http.tls = dash_common::tls::listener_tls_from_env(&dash_common::tls::CONTROL_PLANE_TLS_ENV)
        .map_err(|reason| std::io::Error::new(std::io::ErrorKind::InvalidInput, reason))?;
    dash_http::serve(
        listener,
        http,
        handler,
        |_, _| false,
        &dash_http::NeverShutdown,
        Arc::new(dash_http::NoHooks),
    )
}

pub fn handle_http_request_bytes(
    state: &Arc<Mutex<ControlPlanePlacementState>>,
    raw_request: &[u8],
) -> Result<Vec<u8>, String> {
    let request = dash_http::parse_request_bytes(raw_request, &ServerConfig::default().to_http())
        .map_err(|err| err.message)?;
    let response = handle_request(state, request, None);
    Ok(render_response_text(&response).into_bytes())
}

/// Like [`handle_http_request_bytes`] for a request received from `peer`
/// (used by the authentication-failure throttle).
pub fn handle_http_request_bytes_from_peer(
    state: &Arc<Mutex<ControlPlanePlacementState>>,
    raw_request: &[u8],
    peer: IpAddr,
) -> Result<Vec<u8>, String> {
    let request = dash_http::parse_request_bytes(raw_request, &ServerConfig::default().to_http())
        .map_err(|err| err.message)?;
    let response = handle_request(state, request, Some(peer));
    Ok(render_response_text(&response).into_bytes())
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct HttpResponse {
    status: u16,
    content_type: &'static str,
    body: String,
    headers: Vec<(&'static str, String)>,
}

impl HttpResponse {
    fn ok_json(body: String) -> Self {
        Self {
            status: 200,
            content_type: "application/json",
            body,
            headers: Vec::new(),
        }
    }

    fn ok_text(body: String) -> Self {
        Self {
            status: 200,
            content_type: "text/plain; charset=utf-8",
            body,
            headers: Vec::new(),
        }
    }

    fn error(status: u16, reason: &str) -> Self {
        Self {
            status,
            content_type: "application/json",
            body: format!("{{\"error\":\"{}\"}}", json_escape(reason)),
            headers: Vec::new(),
        }
    }

    fn bad_request(reason: &str) -> Self {
        Self::error(400, reason)
    }

    fn not_found(reason: &str) -> Self {
        Self::error(404, reason)
    }

    fn method_not_allowed(reason: &str) -> Self {
        Self::error(405, reason)
    }

    fn with_header(mut self, name: &'static str, value: impl Into<String>) -> Self {
        self.headers.push((name, value.into()));
        self
    }

    /// Mark the response as coming from the leader, with its fencing token.
    fn marked_leader(self, fencing_token: Option<u64>) -> Self {
        let response = self.with_header("X-Dash-Leader", "true");
        match fencing_token {
            Some(token) => response.with_header("X-Dash-Fencing-Token", token.to_string()),
            None => response,
        }
    }
}

fn fencing_json(token: Option<u64>) -> String {
    token.map_or_else(|| "null".to_string(), |token| token.to_string())
}

fn lock_state(
    state: &Arc<Mutex<ControlPlanePlacementState>>,
) -> Result<MutexGuard<'_, ControlPlanePlacementState>, HttpResponse> {
    state
        .lock()
        .map_err(|_| HttpResponse::error(500, "control-plane state lock unavailable"))
}

/// Gate a request on this node being the leader with up-to-date state.
/// Followers answer 503 with `X-Dash-Leader: false` instead of serving
/// possibly stale data.
fn require_leader(
    guard: &mut ControlPlanePlacementState,
    follower_message: &str,
) -> Result<Option<u64>, HttpResponse> {
    match guard.ensure_leader_synced() {
        Ok(LeaderStatus::Leader { fencing_token }) => Ok(fencing_token),
        Ok(LeaderStatus::Follower) => {
            let leader_hint = guard
                .current_leader_info()
                .ok()
                .flatten()
                .map(|record| record.node_id);
            let mut response =
                HttpResponse::error(503, follower_message).with_header("X-Dash-Leader", "false");
            if let Some(leader) = leader_hint
                && !leader.contains(['\r', '\n'])
            {
                response = response.with_header("X-Dash-Leader-Node", leader);
            }
            Err(response)
        }
        Err(reason) => Err(HttpResponse::error(500, &reason)),
    }
}

fn requires_auth(path: &str) -> bool {
    path.starts_with("/v1/control-plane/")
        && !matches!(path, "/v1/control-plane/health" | "/v1/control-plane/ready")
}

fn handle_request(
    state: &Arc<Mutex<ControlPlanePlacementState>>,
    request: HttpRequest,
    peer: Option<IpAddr>,
) -> HttpResponse {
    let (path, query) = split_target(&request.target);

    if requires_auth(&path) {
        let (auth, throttle) = match lock_state(state) {
            Ok(guard) => (guard.auth_mode().clone(), Arc::clone(&guard.auth_throttle)),
            Err(response) => return response,
        };
        // Requests without a known peer share one bucket.
        let peer = peer.unwrap_or(IpAddr::from([0, 0, 0, 0]));
        let now = Instant::now();
        if matches!(auth, AuthMode::Token(_))
            && let Some(retry_after) = throttle.blocked_for(peer, now)
        {
            return HttpResponse::error(429, "too many failed authentication attempts")
                .with_header("Retry-After", retry_after.to_string());
        }
        if let Err(response) = auth.authorize(request.header("authorization")) {
            // Only a presented-but-wrong credential is a guess; a request with no
            // credential at all is not counted.
            if response.status == 401 && request.header("authorization").is_some() {
                throttle.record_failure(peer, now);
            }
            return response;
        }
    }

    match (request.method.as_str(), path.as_str()) {
        ("GET", "/health") | ("GET", "/v1/control-plane/health") => {
            HttpResponse::ok_json("{\"status\":\"ok\"}".to_string())
        }
        ("GET", "/ready") | ("GET", "/v1/control-plane/ready") => {
            let guard = match lock_state(state) {
                Ok(guard) => guard,
                Err(response) => return response,
            };
            match guard.is_leader() {
                Ok(true) => {
                    HttpResponse::ok_json("{\"status\":\"ready\",\"is_leader\":true}".to_string())
                }
                Ok(false) => HttpResponse::error(503, "control-plane is not the leader"),
                Err(reason) => HttpResponse::error(500, &reason),
            }
        }
        ("GET", "/v1/control-plane/leader") => {
            let guard = match lock_state(state) {
                Ok(guard) => guard,
                Err(response) => return response,
            };
            let info = match guard.current_leader_info() {
                Ok(Some(record)) => record,
                Ok(None) => return HttpResponse::error(503, "no leader elected"),
                Err(reason) => return HttpResponse::error(500, &reason),
            };
            let local = guard.local_node_id_or_unknown();
            let is_leader = info.node_id == local;
            HttpResponse::ok_json(format!(
                "{{\"is_leader\":{},\"leader_node_id\":\"{}\",\"epoch\":{},\"fencing_token\":{},\"expires_at_ms\":{},\"local_node_id\":\"{}\"}}",
                is_leader,
                json_escape(&info.node_id),
                info.epoch,
                info.epoch,
                info.expires_at_ms,
                json_escape(&local)
            ))
        }
        ("POST", "/v1/control-plane/leader/acquire") => {
            let mut guard = match lock_state(state) {
                Ok(guard) => guard,
                Err(response) => return response,
            };
            match guard.try_acquire_and_sync() {
                Ok(LeaderStatus::Leader { fencing_token }) => HttpResponse::ok_json(format!(
                    "{{\"status\":\"ok\",\"is_leader\":true,\"node_id\":\"{}\",\"epoch\":{},\"fencing_token\":{},\"placement_epoch\":{}}}",
                    json_escape(&guard.local_node_id_or_unknown()),
                    fencing_token.unwrap_or_else(|| guard.highest_epoch()),
                    fencing_json(fencing_token),
                    guard.highest_epoch(),
                ))
                .marked_leader(fencing_token),
                Ok(LeaderStatus::Follower) => {
                    HttpResponse::error(503, "leader lease is held by another node")
                }
                Err(reason) => HttpResponse::error(500, &reason),
            }
        }
        ("GET", "/v1/control-plane/placement") => {
            let mut guard = match lock_state(state) {
                Ok(guard) => guard,
                Err(response) => return response,
            };
            let token = match require_leader(
                &mut guard,
                "this node is a follower and may hold stale placements; query the leader",
            ) {
                Ok(token) => token,
                Err(response) => return response,
            };
            let response = if query
                .get("format")
                .is_some_and(|value| value.eq_ignore_ascii_case("csv"))
            {
                HttpResponse::ok_text(render_shard_placements_csv(guard.placements()))
            } else {
                HttpResponse::ok_json(guard.render_placement_json())
            };
            response.marked_leader(token)
        }
        ("PUT", "/v1/control-plane/placement") => {
            let expected_epoch = match parse_optional_u64(query.get("expected_epoch")) {
                Ok(value) => value,
                Err(reason) => return HttpResponse::bad_request(&reason),
            };
            let csv = match std::str::from_utf8(&request.body) {
                Ok(value) => value,
                Err(_) => return HttpResponse::bad_request("placement body must be UTF-8 CSV"),
            };
            let mut guard = match lock_state(state) {
                Ok(guard) => guard,
                Err(response) => return response,
            };
            let token = match require_leader(&mut guard, "only the leader may update placements") {
                Ok(token) => token,
                Err(response) => return response,
            };
            if let Err(reason) = guard.cas_matches(expected_epoch) {
                return HttpResponse::error(409, &reason);
            }
            let previous = guard.placements.clone();
            match guard.replace_placements_from_csv_monotonic(csv) {
                Ok(()) => match guard.persist_if_configured() {
                    Ok(()) => HttpResponse::ok_json(format!(
                        "{{\"status\":\"ok\",\"placement_count\":{},\"commit_epoch\":{},\"fencing_token\":{}}}",
                        guard.placements().len(),
                        guard.highest_epoch(),
                        fencing_json(token),
                    ))
                    .marked_leader(token),
                    Err(reason) => {
                        guard.placements = previous;
                        let _ = guard.persist_if_configured();
                        HttpResponse::error(500, &reason)
                    }
                },
                Err(reason) => HttpResponse::error(409, &reason),
            }
        }
        ("POST", "/v1/control-plane/failover/promote") => {
            let expected_epoch = match parse_optional_u64(query.get("expected_epoch")) {
                Ok(value) => value,
                Err(reason) => return HttpResponse::bad_request(&reason),
            };
            let tenant_id = match query.get("tenant_id") {
                Some(value) if !value.trim().is_empty() => value.trim(),
                _ => {
                    return HttpResponse::bad_request(
                        "tenant_id query parameter is required for promotion",
                    );
                }
            };
            let shard_id = match query
                .get("shard_id")
                .and_then(|value| value.trim().parse::<u32>().ok())
            {
                Some(value) => value,
                None => {
                    return HttpResponse::bad_request(
                        "shard_id query parameter must be a valid u32",
                    );
                }
            };
            let node_id = match query.get("node_id") {
                Some(value) if !value.trim().is_empty() => value.trim(),
                _ => {
                    return HttpResponse::bad_request(
                        "node_id query parameter is required for promotion",
                    );
                }
            };
            let force = query
                .get("force")
                .is_some_and(|value| matches!(value.trim(), "1" | "true" | "TRUE" | "True"));
            let mut guard = match lock_state(state) {
                Ok(guard) => guard,
                Err(response) => return response,
            };
            let token = match require_leader(&mut guard, "only the leader may promote a replica") {
                Ok(token) => token,
                Err(response) => return response,
            };
            if let Err(reason) = guard.cas_matches(expected_epoch) {
                return HttpResponse::error(409, &reason);
            }
            let previous = guard.placements.clone();
            let previous_lag = guard.replica_lag.clone();
            match guard.promote_replica(tenant_id, shard_id, node_id, force) {
                Ok(epoch) => match guard.persist_if_configured() {
                    Ok(()) => HttpResponse::ok_json(format!(
                        "{{\"status\":\"ok\",\"tenant_id\":\"{}\",\"shard_id\":{},\"leader_node_id\":\"{}\",\"epoch\":{},\"fencing_token\":{},\"forced\":{}}}",
                        json_escape(tenant_id),
                        shard_id,
                        json_escape(node_id),
                        epoch,
                        fencing_json(token),
                        force,
                    ))
                    .marked_leader(token),
                    Err(reason) => {
                        guard.placements = previous;
                        guard.replica_lag = previous_lag;
                        let _ = guard.persist_if_configured();
                        HttpResponse::error(500, &reason)
                    }
                },
                Err(reason) => HttpResponse::error(409, &reason),
            }
        }
        ("POST", "/v1/control-plane/replica-lag") => {
            let tenant_id = match query.get("tenant_id") {
                Some(value) if !value.trim().is_empty() => value.trim(),
                _ => return HttpResponse::bad_request("tenant_id query parameter is required"),
            };
            let shard_id = match query
                .get("shard_id")
                .and_then(|value| value.trim().parse::<u32>().ok())
            {
                Some(value) => value,
                None => {
                    return HttpResponse::bad_request(
                        "shard_id query parameter must be a valid u32",
                    );
                }
            };
            let node_id = match query.get("node_id") {
                Some(value) if !value.trim().is_empty() => value.trim(),
                _ => return HttpResponse::bad_request("node_id query parameter is required"),
            };
            let lag = match query
                .get("lag")
                .and_then(|value| value.trim().parse::<u64>().ok())
            {
                Some(value) => value,
                None => return HttpResponse::bad_request("lag query parameter must be a u64"),
            };
            let mut guard = match lock_state(state) {
                Ok(guard) => guard,
                Err(response) => return response,
            };
            let token = match require_leader(&mut guard, "only the leader accepts lag reports") {
                Ok(token) => token,
                Err(response) => return response,
            };
            match guard.report_replica_lag(tenant_id, shard_id, node_id, lag) {
                Ok(()) => {
                    HttpResponse::ok_json("{\"status\":\"ok\"}".to_string()).marked_leader(token)
                }
                Err(reason) => HttpResponse::error(404, &reason),
            }
        }
        (_, "/v1/control-plane/placement") => {
            HttpResponse::method_not_allowed("only GET and PUT are supported")
        }
        (_, "/v1/control-plane/failover/promote") | (_, "/v1/control-plane/replica-lag") => {
            HttpResponse::method_not_allowed("only POST is supported")
        }
        _ => HttpResponse::not_found("unknown path"),
    }
}

/// Control-plane query parameters are used verbatim (no percent-decoding), as
/// before the shared server; only the surrounding whitespace is trimmed.
fn split_target(target: &str) -> (String, HashMap<String, String>) {
    let (path, query) = target
        .split_once('?')
        .map(|(path, query)| (path.to_string(), query))
        .unwrap_or_else(|| (target.to_string(), ""));
    let mut map = HashMap::new();
    for item in query.split('&') {
        if item.trim().is_empty() {
            continue;
        }
        let (key, value) = item
            .split_once('=')
            .map(|(key, value)| (key.trim(), value.trim()))
            .unwrap_or_else(|| (item.trim(), ""));
        if key.is_empty() {
            continue;
        }
        map.insert(key.to_string(), value.to_string());
    }
    (path, map)
}

fn parse_optional_u64(value: Option<&String>) -> Result<Option<u64>, String> {
    match value {
        Some(raw) if raw.trim().is_empty() => Ok(None),
        Some(raw) => raw
            .trim()
            .parse::<u64>()
            .map(Some)
            .map_err(|_| format!("invalid u64 value '{}'", raw.trim())),
        None => Ok(None),
    }
}

static PERSIST_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Durable atomic replace: temp file, fsync, rename, fsync of the directory.
fn persist_atomically(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let parent = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
        _ => PathBuf::from("."),
    };
    fs::create_dir_all(&parent).map_err(|err| {
        format!(
            "failed creating parent directory '{}': {err}",
            parent.display()
        )
    })?;
    let tmp_path = path.with_extension(format!(
        "tmp-{}-{}",
        std::process::id(),
        PERSIST_COUNTER.fetch_add(1, Ordering::Relaxed)
    ));
    let write_result = (|| -> Result<(), String> {
        let mut file = fs::File::create(&tmp_path).map_err(|err| {
            format!(
                "failed creating temp persistence file '{}': {err}",
                tmp_path.display()
            )
        })?;
        file.write_all(bytes).map_err(|err| {
            format!(
                "failed writing temp persistence file '{}': {err}",
                tmp_path.display()
            )
        })?;
        file.sync_all().map_err(|err| {
            format!(
                "failed syncing temp persistence file '{}': {err}",
                tmp_path.display()
            )
        })
    })();
    if let Err(err) = write_result {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }
    fs::rename(&tmp_path, path).map_err(|err| {
        let _ = fs::remove_file(&tmp_path);
        format!(
            "failed renaming '{}' to '{}': {err}",
            tmp_path.display(),
            path.display()
        )
    })?;
    fs::File::open(&parent)
        .and_then(|dir| dir.sync_all())
        .map_err(|err| {
            format!(
                "failed syncing parent directory '{}': {err}",
                parent.display()
            )
        })
}

impl From<HttpResponse> for dash_http::Response {
    fn from(response: HttpResponse) -> Self {
        let mut out =
            dash_http::Response::new(response.status, response.content_type, response.body);
        for (name, value) in response.headers {
            out = out.with_header(name, value);
        }
        out
    }
}

fn render_response_text(response: &HttpResponse) -> String {
    dash_http::render_response(&response.clone().into())
}

fn replica_role_str(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Leader => "leader",
        ReplicaRole::Follower => "follower",
    }
}

fn replica_health_str(health: metadata_router::ReplicaHealth) -> &'static str {
    match health {
        metadata_router::ReplicaHealth::Healthy => "healthy",
        metadata_router::ReplicaHealth::Degraded => "degraded",
        metadata_router::ReplicaHealth::Unavailable => "unavailable",
    }
}

#[cfg(test)]
mod tests;
