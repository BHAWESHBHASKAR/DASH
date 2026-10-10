use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs,
    io::{Read, Write},
    net::{TcpStream, ToSocketAddrs},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardAssignment {
    pub tenant_id: String,
    pub entity_key: String,
    pub shard_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingPlan {
    pub primary: ShardAssignment,
    pub replicas: Vec<ShardAssignment>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouterConfig {
    pub shard_ids: Vec<u32>,
    pub virtual_nodes_per_shard: u32,
    pub replica_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaRole {
    Leader,
    Follower,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplicaHealth {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaPlacement {
    pub node_id: String,
    pub role: ReplicaRole,
    pub health: ReplicaHealth,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardPlacement {
    pub tenant_id: String,
    pub shard_id: u32,
    pub epoch: u64,
    pub replicas: Vec<ReplicaPlacement>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPreference {
    LeaderOnly,
    PreferFollower,
    AnyHealthy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutedReplica {
    pub tenant_id: String,
    pub entity_key: String,
    pub shard_id: u32,
    pub epoch: u64,
    pub node_id: String,
    pub role: ReplicaRole,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementRouteError {
    PlacementNotFound { tenant_id: String, shard_id: u32 },
    NoWritableLeader { tenant_id: String, shard_id: u32 },
    NoReadableReplica { tenant_id: String, shard_id: u32 },
    ReplicaNotFound { node_id: String },
    ReplicaUnhealthy { node_id: String },
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            shard_ids: vec![0],
            virtual_nodes_per_shard: 64,
            replica_count: 1,
        }
    }
}

pub fn route_to_shard(tenant_id: &str, entity_key: &str, shard_count: u32) -> ShardAssignment {
    let shard_count = shard_count.max(1);
    let mut hash: u64 = 1469598103934665603;
    // 0xFF never occurs in UTF-8, so ("ab","c") and ("a","bc") cannot collide
    // by concatenation.
    for b in tenant_id
        .as_bytes()
        .iter()
        .chain(std::iter::once(&0xFFu8))
        .chain(entity_key.as_bytes())
    {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    ShardAssignment {
        tenant_id: tenant_id.to_string(),
        entity_key: entity_key.to_string(),
        shard_id: (hash % shard_count as u64) as u32,
    }
}

pub fn route_with_replicas(
    tenant_id: &str,
    entity_key: &str,
    config: &RouterConfig,
) -> RoutingPlan {
    let ring = cached_ring(config);
    if ring.is_empty() {
        return RoutingPlan {
            primary: route_to_shard(tenant_id, entity_key, 1),
            replicas: Vec::new(),
        };
    }

    let target = hash_key(&format!("{tenant_id}|{entity_key}"));
    // Walk the ring clockwise and keep the first occurrence of each shard.
    // `Vec::dedup` only removes adjacent duplicates, which leaves repeats when
    // virtual nodes of different shards interleave.
    let mut seen: HashSet<u32> = HashSet::new();
    let ordered: Vec<u32> = ring
        .range(target..)
        .chain(ring.range(..target))
        .map(|(_, shard_id)| *shard_id)
        .filter(|shard_id| seen.insert(*shard_id))
        .collect();

    let primary_shard = *ordered.first().unwrap_or(&0);
    let primary = ShardAssignment {
        tenant_id: tenant_id.to_string(),
        entity_key: entity_key.to_string(),
        shard_id: primary_shard,
    };

    let replicas = ordered
        .into_iter()
        .filter(|shard_id| *shard_id != primary_shard)
        .take(config.replica_count.saturating_sub(1))
        .map(|shard_id| ShardAssignment {
            tenant_id: tenant_id.to_string(),
            entity_key: entity_key.to_string(),
            shard_id,
        })
        .collect();

    RoutingPlan { primary, replicas }
}

pub fn route_write_with_placement(
    tenant_id: &str,
    entity_key: &str,
    config: &RouterConfig,
    placements: &[ShardPlacement],
) -> Result<RoutedReplica, PlacementRouteError> {
    let shard_id = route_with_replicas(tenant_id, entity_key, config)
        .primary
        .shard_id;
    let placement = find_placement(placements, tenant_id, shard_id).ok_or_else(|| {
        PlacementRouteError::PlacementNotFound {
            tenant_id: tenant_id.to_string(),
            shard_id,
        }
    })?;
    let leader = placement
        .replicas
        .iter()
        .find(|replica| {
            replica.role == ReplicaRole::Leader && replica.health == ReplicaHealth::Healthy
        })
        .ok_or_else(|| PlacementRouteError::NoWritableLeader {
            tenant_id: tenant_id.to_string(),
            shard_id,
        })?;
    Ok(RoutedReplica {
        tenant_id: tenant_id.to_string(),
        entity_key: entity_key.to_string(),
        shard_id,
        epoch: placement.epoch,
        node_id: leader.node_id.clone(),
        role: ReplicaRole::Leader,
    })
}

pub fn route_read_with_placement(
    tenant_id: &str,
    entity_key: &str,
    config: &RouterConfig,
    placements: &[ShardPlacement],
    preference: ReadPreference,
) -> Result<RoutedReplica, PlacementRouteError> {
    let shard_id = route_with_replicas(tenant_id, entity_key, config)
        .primary
        .shard_id;
    let placement = find_placement(placements, tenant_id, shard_id).ok_or_else(|| {
        PlacementRouteError::PlacementNotFound {
            tenant_id: tenant_id.to_string(),
            shard_id,
        }
    })?;
    let chosen = match preference {
        ReadPreference::LeaderOnly => placement.replicas.iter().find(|replica| {
            replica.role == ReplicaRole::Leader && is_readable_replica_health(replica.health)
        }),
        ReadPreference::PreferFollower => placement
            .replicas
            .iter()
            .find(|replica| {
                replica.role == ReplicaRole::Follower && is_readable_replica_health(replica.health)
            })
            .or_else(|| {
                placement.replicas.iter().find(|replica| {
                    replica.role == ReplicaRole::Leader
                        && is_readable_replica_health(replica.health)
                })
            }),
        ReadPreference::AnyHealthy => placement
            .replicas
            .iter()
            .find(|replica| is_readable_replica_health(replica.health)),
    }
    .ok_or_else(|| PlacementRouteError::NoReadableReplica {
        tenant_id: tenant_id.to_string(),
        shard_id,
    })?;
    Ok(RoutedReplica {
        tenant_id: tenant_id.to_string(),
        entity_key: entity_key.to_string(),
        shard_id,
        epoch: placement.epoch,
        node_id: chosen.node_id.clone(),
        role: chosen.role,
    })
}

pub fn set_replica_health(
    placement: &mut ShardPlacement,
    node_id: &str,
    health: ReplicaHealth,
) -> Result<(), PlacementRouteError> {
    let replica = placement
        .replicas
        .iter_mut()
        .find(|replica| replica.node_id == node_id)
        .ok_or_else(|| PlacementRouteError::ReplicaNotFound {
            node_id: node_id.to_string(),
        })?;
    replica.health = health;
    Ok(())
}

pub fn promote_replica_to_leader(
    placement: &mut ShardPlacement,
    node_id: &str,
) -> Result<u64, PlacementRouteError> {
    let promoted_index = placement
        .replicas
        .iter()
        .position(|replica| replica.node_id == node_id)
        .ok_or_else(|| PlacementRouteError::ReplicaNotFound {
            node_id: node_id.to_string(),
        })?;
    if !is_readable_replica_health(placement.replicas[promoted_index].health) {
        return Err(PlacementRouteError::ReplicaUnhealthy {
            node_id: node_id.to_string(),
        });
    }
    if placement.replicas[promoted_index].role == ReplicaRole::Leader {
        return Ok(placement.epoch);
    }
    for replica in &mut placement.replicas {
        if replica.role == ReplicaRole::Leader {
            replica.role = ReplicaRole::Follower;
        }
    }
    placement.replicas[promoted_index].role = ReplicaRole::Leader;
    placement.epoch = placement.epoch.saturating_add(1);
    Ok(placement.epoch)
}

pub fn load_shard_placements_csv(path: &Path) -> Result<Vec<ShardPlacement>, String> {
    let raw = fs::read_to_string(path)
        .map_err(|err| format!("failed to read placement file '{}': {err}", path.display()))?;
    parse_shard_placements_csv(&raw)
}

/// Options controlling how placements are fetched from the control plane.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementSourceOptions {
    /// When the control plane is configured but unreachable, fall back to the
    /// local placement file only if this is set. Off by default: a stale file
    /// can name a deposed leader and cause split-brain writes.
    pub allow_stale_placement: bool,
    pub connect_timeout: Duration,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    /// Upper bound on the response size accepted from the control plane.
    pub max_response_bytes: usize,
    /// Bearer token presented to the control plane.
    pub bearer_token: Option<String>,
    /// Allow sending the bearer token over plain http to a non-loopback
    /// control plane. Off by default: the token would cross the network in
    /// clear text.
    pub allow_insecure_http: bool,
    /// Extra CA bundle trusted for an `https://` control-plane URL (the
    /// public web roots are always trusted; verification is never off).
    pub ca_file: Option<PathBuf>,
    /// Client certificate chain and key presented to an `https://` control
    /// plane that requires client certificates.
    pub client_cert_file: Option<PathBuf>,
    pub client_key_file: Option<PathBuf>,
}

impl Default for PlacementSourceOptions {
    fn default() -> Self {
        Self {
            allow_stale_placement: false,
            connect_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(5),
            write_timeout: Duration::from_secs(5),
            max_response_bytes: 8 * 1024 * 1024,
            bearer_token: None,
            allow_insecure_http: false,
            ca_file: None,
            client_cert_file: None,
            client_key_file: None,
        }
    }
}

impl PlacementSourceOptions {
    /// Defaults plus environment overrides:
    /// `DASH_ROUTER_CONTROL_PLANE_TOKEN` (fallback `DASH_CONTROL_PLANE_TOKEN`),
    /// `DASH_ROUTER_ALLOW_STALE_PLACEMENT=1`, `DASH_ROUTER_ALLOW_INSECURE_HTTP=1`,
    /// `DASH_ROUTER_CONTROL_PLANE_{CONNECT,READ,WRITE}_TIMEOUT_MS` and, for an
    /// `https://` URL, `DASH_ROUTER_CONTROL_PLANE_CA_FILE` and
    /// `DASH_ROUTER_CONTROL_PLANE_CLIENT_{CERT,KEY}_FILE`.
    pub fn from_env() -> Self {
        let mut options = Self {
            bearer_token: env_non_empty("DASH_ROUTER_CONTROL_PLANE_TOKEN")
                .or_else(|| env_non_empty("DASH_CONTROL_PLANE_TOKEN")),
            allow_stale_placement: matches!(
                env_non_empty("DASH_ROUTER_ALLOW_STALE_PLACEMENT").as_deref(),
                Some("1") | Some("true") | Some("TRUE")
            ),
            allow_insecure_http: matches!(
                env_non_empty("DASH_ROUTER_ALLOW_INSECURE_HTTP").as_deref(),
                Some("1") | Some("true") | Some("TRUE")
            ),
            ca_file: env_non_empty("DASH_ROUTER_CONTROL_PLANE_CA_FILE").map(PathBuf::from),
            client_cert_file: env_non_empty("DASH_ROUTER_CONTROL_PLANE_CLIENT_CERT_FILE")
                .map(PathBuf::from),
            client_key_file: env_non_empty("DASH_ROUTER_CONTROL_PLANE_CLIENT_KEY_FILE")
                .map(PathBuf::from),
            ..Self::default()
        };
        if let Some(value) = env_millis("DASH_ROUTER_CONTROL_PLANE_CONNECT_TIMEOUT_MS") {
            options.connect_timeout = value;
        }
        if let Some(value) = env_millis("DASH_ROUTER_CONTROL_PLANE_READ_TIMEOUT_MS") {
            options.read_timeout = value;
        }
        if let Some(value) = env_millis("DASH_ROUTER_CONTROL_PLANE_WRITE_TIMEOUT_MS") {
            options.write_timeout = value;
        }
        options
    }
}

fn env_non_empty(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn env_millis(name: &str) -> Option<Duration> {
    env_non_empty(name)
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis)
}

pub fn load_shard_placements_from_source(
    placement_file: Option<&Path>,
    control_plane_base_url: Option<&str>,
) -> Result<Vec<ShardPlacement>, String> {
    load_shard_placements_from_source_with_options(
        placement_file,
        control_plane_base_url,
        &PlacementSourceOptions::from_env(),
    )
}

pub fn load_shard_placements_from_source_with_options(
    placement_file: Option<&Path>,
    control_plane_base_url: Option<&str>,
    options: &PlacementSourceOptions,
) -> Result<Vec<ShardPlacement>, String> {
    let mut control_plane_error: Option<String> = None;
    if let Some(base_url) = control_plane_base_url {
        let trimmed = base_url.trim();
        if !trimmed.is_empty() {
            match load_shard_placements_from_control_plane_with_options(trimmed, options) {
                Ok(placements) => return Ok(placements),
                Err(err) => control_plane_error = Some(err),
            }
        }
    }
    if let Some(err) = control_plane_error {
        // The control plane is configured but did not answer. Serving from a
        // possibly stale file is only acceptable when explicitly requested.
        if options.allow_stale_placement
            && let Some(path) = placement_file
        {
            return load_shard_placements_csv(path).map_err(|file_err| {
                format!("{err}; stale placement fallback also failed: {file_err}")
            });
        }
        return Err(format!(
            "{err} (refusing to fall back to the placement file; set DASH_ROUTER_ALLOW_STALE_PLACEMENT=1 to allow stale placements)"
        ));
    }
    if let Some(path) = placement_file {
        return load_shard_placements_csv(path);
    }
    Err(
        "placement source is unconfigured: set DASH_ROUTER_CONTROL_PLANE_URL or DASH_ROUTER_PLACEMENT_FILE"
            .to_string(),
    )
}

/// Reject a candidate placement set that moves any shard's epoch backwards
/// relative to `current`. Callers that hold placements in memory (ingestion,
/// retrieval) should apply this before swapping in a reloaded set.
pub fn ensure_no_epoch_regression(
    current: &[ShardPlacement],
    candidate: &[ShardPlacement],
) -> Result<(), String> {
    let current_epochs: HashMap<(&str, u32), u64> = current
        .iter()
        .map(|placement| {
            (
                (placement.tenant_id.as_str(), placement.shard_id),
                placement.epoch,
            )
        })
        .collect();
    for placement in candidate {
        if let Some(current_epoch) =
            current_epochs.get(&(placement.tenant_id.as_str(), placement.shard_id))
            && placement.epoch < *current_epoch
        {
            return Err(format!(
                "epoch regression for tenant '{}' shard {}: current={}, candidate={}",
                placement.tenant_id, placement.shard_id, current_epoch, placement.epoch
            ));
        }
    }
    Ok(())
}

pub fn load_shard_placements_from_control_plane(
    base_url: &str,
) -> Result<Vec<ShardPlacement>, String> {
    load_shard_placements_from_control_plane_with_options(
        base_url,
        &PlacementSourceOptions::from_env(),
    )
}

pub fn load_shard_placements_from_control_plane_with_options(
    base_url: &str,
    options: &PlacementSourceOptions,
) -> Result<Vec<ShardPlacement>, String> {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        return Err("control-plane base URL must not be empty".to_string());
    }
    let url = format!("{base_url}/v1/control-plane/placement?format=csv");
    let target = parse_http_url(&url)?;
    let response = http_get(&target, options)?;
    if response.status != 200 {
        return Err(format!(
            "control-plane placement request failed with HTTP status {}",
            response.status
        ));
    }
    if response
        .header("x-dash-leader")
        .is_some_and(|value| value.eq_ignore_ascii_case("false"))
    {
        return Err(
            "control-plane node is not the leader; refusing its placement data".to_string(),
        );
    }
    let body = String::from_utf8(response.body)
        .map_err(|_| "control-plane response is not valid UTF-8".to_string())?;
    parse_shard_placements_csv(&body)
}

#[derive(Debug)]
struct HttpClientResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpClientResponse {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }
}

const MAX_RESPONSE_HEADER_BYTES: usize = 16 * 1024;

/// True when `authority` (`host[:port]`) names the local machine.
fn is_loopback_authority(authority: &str) -> bool {
    let host = if let Some(rest) = authority.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host)
    };
    host.eq_ignore_ascii_case("localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// A plaintext or TLS connection to the control plane.
enum ClientStream {
    Plain(TcpStream),
    Tls(Box<dash_http::rustls::StreamOwned<dash_http::rustls::ClientConnection, TcpStream>>),
}

impl ClientStream {
    fn socket(&self) -> &TcpStream {
        match self {
            Self::Plain(stream) => stream,
            Self::Tls(stream) => &stream.sock,
        }
    }
}

impl Read for ClientStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.read(buf),
            Self::Tls(stream) => stream.read(buf),
        }
    }
}

impl Write for ClientStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(stream) => stream.write(buf),
            Self::Tls(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(stream) => stream.flush(),
            Self::Tls(stream) => stream.flush(),
        }
    }
}

/// Host part of `host:port` / `[v6]:port`, for TLS server-name checks.
fn authority_host(authority: &str) -> &str {
    if let Some(rest) = authority.strip_prefix('[') {
        return rest.split(']').next().unwrap_or(rest);
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if port.bytes().all(|b| b.is_ascii_digit()) => host,
        _ => authority,
    }
}

fn wrap_tls(
    socket: TcpStream,
    authority: &str,
    options: &PlacementSourceOptions,
) -> Result<ClientStream, String> {
    let identity = match (
        options.client_cert_file.as_deref(),
        options.client_key_file.as_deref(),
    ) {
        (None, None) => None,
        (Some(cert), Some(key)) => Some((cert, key)),
        _ => {
            return Err("DASH_ROUTER_CONTROL_PLANE_CLIENT_CERT_FILE and \
                 DASH_ROUTER_CONTROL_PLANE_CLIENT_KEY_FILE must be set together"
                .to_string());
        }
    };
    let config = dash_http::client_config(options.ca_file.as_deref(), identity)
        .map_err(|err| format!("control-plane TLS configuration rejected: {err}"))?;
    let host = authority_host(authority).to_string();
    let name = dash_http::rustls::pki_types::ServerName::try_from(host)
        .map_err(|_| format!("control-plane URL host '{authority}' is not a valid TLS name"))?;
    let conn = dash_http::rustls::ClientConnection::new(config, name)
        .map_err(|err| format!("control-plane TLS setup failed: {err}"))?;
    Ok(ClientStream::Tls(Box::new(
        dash_http::rustls::StreamOwned::new(conn, socket),
    )))
}

fn http_get(
    target: &ControlPlaneUrl,
    options: &PlacementSourceOptions,
) -> Result<HttpClientResponse, String> {
    http_request("GET", target, options)
}

/// Answer of [`control_plane_post`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ControlPlaneResponse {
    pub status: u16,
    /// `X-Dash-Leader` was `false`: the node is a control-plane follower.
    pub from_follower: bool,
    pub body: String,
}

/// `POST <base_url><path_and_query>` (empty body) to the control plane with
/// the same token, TLS, timeout and size rules as the placement fetch.
pub fn control_plane_post(
    base_url: &str,
    path_and_query: &str,
    options: &PlacementSourceOptions,
) -> Result<ControlPlaneResponse, String> {
    let base_url = base_url.trim().trim_end_matches('/');
    if base_url.is_empty() {
        return Err("control-plane base URL must not be empty".to_string());
    }
    let target = parse_http_url(&format!("{base_url}{path_and_query}"))?;
    let response = http_request("POST", &target, options)?;
    let from_follower = response
        .header("x-dash-leader")
        .is_some_and(|value| value.eq_ignore_ascii_case("false"));
    let body = String::from_utf8(response.body)
        .map_err(|_| "control-plane response is not valid UTF-8".to_string())?;
    Ok(ControlPlaneResponse {
        status: response.status,
        from_follower,
        body,
    })
}

fn http_request(
    method: &str,
    target: &ControlPlaneUrl,
    options: &PlacementSourceOptions,
) -> Result<HttpClientResponse, String> {
    let authority = target.authority.as_str();
    let path = target.path.as_str();
    if options.bearer_token.is_some()
        && !target.tls
        && !options.allow_insecure_http
        && !is_loopback_authority(authority)
    {
        return Err(format!(
            "refusing to send the control-plane token over plain http to non-loopback host \
             '{authority}'; use an https:// control-plane URL, keep the control plane on a \
             trusted local link, or set DASH_ROUTER_ALLOW_INSECURE_HTTP=1 to accept the exposure"
        ));
    }
    let addrs: Vec<_> = authority
        .to_socket_addrs()
        .map_err(|err| format!("failed resolving control-plane '{authority}': {err}"))?
        .collect();
    let mut last_err = format!("control-plane '{authority}' resolved to no addresses");
    let mut stream = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, options.connect_timeout) {
            Ok(connected) => {
                stream = Some(connected);
                break;
            }
            Err(err) => last_err = format!("failed connecting control-plane '{authority}': {err}"),
        }
    }
    let socket = stream.ok_or(last_err)?;
    socket
        .set_read_timeout(Some(options.read_timeout))
        .and_then(|_| socket.set_write_timeout(Some(options.write_timeout)))
        .map_err(|err| format!("failed configuring control-plane socket: {err}"))?;
    let mut stream = if target.tls {
        wrap_tls(socket, authority, options)?
    } else {
        ClientStream::Plain(socket)
    };

    let mut request =
        format!("{method} {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n");
    if method != "GET" {
        request.push_str("Content-Length: 0\r\n");
    }
    if let Some(token) = options.bearer_token.as_deref() {
        request.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    request.push_str("\r\n");
    stream
        .write_all(request.as_bytes())
        .and_then(|_| stream.flush())
        .map_err(|err| format!("failed sending control-plane request: {err}"))?;

    // Overall budget so a server that trickles bytes cannot hold us forever.
    let deadline = Instant::now() + options.read_timeout.saturating_mul(2);
    let mut buf: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_header_end(&buf) {
            break pos;
        }
        if buf.len() > MAX_RESPONSE_HEADER_BYTES {
            return Err("control-plane response headers too large".to_string());
        }
        let n = read_with_deadline(&mut stream, &mut chunk, deadline)?;
        if n == 0 {
            return Err("control-plane response missing HTTP header terminator".to_string());
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let header_text = std::str::from_utf8(&buf[..header_end])
        .map_err(|_| "control-plane response headers are not valid UTF-8".to_string())?
        .to_string();
    let mut lines = header_text.lines();
    let status_line = lines
        .next()
        .ok_or_else(|| "control-plane response missing status line".to_string())?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| "control-plane response status missing code".to_string())
        .and_then(|value| {
            value
                .parse::<u16>()
                .map_err(|_| "control-plane response has invalid status code".to_string())
        })?;
    let mut headers = Vec::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_string(), value.trim().to_string()));
        }
    }
    let mut response = HttpClientResponse {
        status,
        headers,
        body: Vec::new(),
    };
    if response
        .header("transfer-encoding")
        .is_some_and(|value| value.to_ascii_lowercase().contains("chunked"))
    {
        return Err("control-plane sent a chunked response, which is not supported".to_string());
    }
    let content_length = match response.header("content-length") {
        Some(raw) => Some(
            raw.parse::<usize>()
                .map_err(|_| "control-plane response has invalid content-length".to_string())?,
        ),
        None => None,
    };
    if let Some(len) = content_length
        && len > options.max_response_bytes
    {
        return Err(format!(
            "control-plane response of {len} bytes exceeds limit of {}",
            options.max_response_bytes
        ));
    }

    let mut body = buf.split_off(header_end + 4);
    loop {
        if let Some(len) = content_length
            && body.len() >= len
        {
            body.truncate(len);
            break;
        }
        if body.len() > options.max_response_bytes {
            return Err(format!(
                "control-plane response exceeds limit of {} bytes",
                options.max_response_bytes
            ));
        }
        let n = read_with_deadline(&mut stream, &mut chunk, deadline)?;
        if n == 0 {
            if content_length.is_some() {
                return Err("control-plane response body truncated".to_string());
            }
            break;
        }
        body.extend_from_slice(&chunk[..n]);
    }
    response.body = body;
    Ok(response)
}

fn read_with_deadline(
    stream: &mut ClientStream,
    buf: &mut [u8],
    deadline: Instant,
) -> Result<usize, String> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err("control-plane response timed out".to_string());
    }
    stream
        .socket()
        .set_read_timeout(Some(remaining))
        .map_err(|err| format!("failed configuring control-plane socket: {err}"))?;
    loop {
        match stream.read(buf) {
            Ok(n) => return Ok(n),
            Err(err) if err.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(format!("failed reading control-plane response: {err}")),
        }
    }
}

fn find_header_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

pub fn render_shard_placements_csv(placements: &[ShardPlacement]) -> String {
    let mut out = String::new();
    for placement in placements {
        for replica in &placement.replicas {
            out.push_str(&format!(
                "{},{},{},{},{},{}\n",
                placement.tenant_id,
                placement.shard_id,
                placement.epoch,
                replica.node_id,
                replica_role_str(replica.role),
                replica_health_str(replica.health),
            ));
        }
    }
    out
}

pub fn parse_shard_placements_csv(input: &str) -> Result<Vec<ShardPlacement>, String> {
    let mut grouped: BTreeMap<(String, u32), ShardPlacement> = BTreeMap::new();
    for (line_index, line) in input.lines().enumerate() {
        let line_no = line_index + 1;
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let columns: Vec<&str> = line.split(',').map(str::trim).collect();
        if columns.len() != 6 {
            return Err(format!(
                "invalid placement CSV line {line_no}: expected 6 columns (tenant_id,shard_id,epoch,node_id,role,health)"
            ));
        }
        let tenant_id = columns[0];
        if tenant_id.is_empty() {
            return Err(format!(
                "invalid placement CSV line {line_no}: tenant_id must not be empty"
            ));
        }
        let shard_id = columns[1]
            .parse::<u32>()
            .map_err(|_| format!("invalid placement CSV line {line_no}: shard_id must be a u32"))?;
        let epoch = columns[2]
            .parse::<u64>()
            .map_err(|_| format!("invalid placement CSV line {line_no}: epoch must be a u64"))?;
        let node_id = columns[3];
        if node_id.is_empty() {
            return Err(format!(
                "invalid placement CSV line {line_no}: node_id must not be empty"
            ));
        }
        let role = parse_replica_role(columns[4])
            .map_err(|reason| format!("invalid placement CSV line {line_no}: role {reason}"))?;
        let health = parse_replica_health(columns[5])
            .map_err(|reason| format!("invalid placement CSV line {line_no}: health {reason}"))?;
        let key = (tenant_id.to_string(), shard_id);
        let entry = grouped.entry(key).or_insert_with(|| ShardPlacement {
            tenant_id: tenant_id.to_string(),
            shard_id,
            epoch,
            replicas: Vec::new(),
        });
        if entry.epoch != epoch {
            return Err(format!(
                "invalid placement CSV line {line_no}: epoch mismatch for tenant '{tenant_id}' shard {shard_id}"
            ));
        }
        if entry
            .replicas
            .iter()
            .any(|replica| replica.node_id == node_id)
        {
            return Err(format!(
                "invalid placement CSV line {line_no}: duplicate node_id '{node_id}' for tenant '{tenant_id}' shard {shard_id}"
            ));
        }
        entry.replicas.push(ReplicaPlacement {
            node_id: node_id.to_string(),
            role,
            health,
        });
    }

    let mut placements: Vec<ShardPlacement> = grouped.into_values().collect();
    placements.sort_by(|a, b| {
        a.tenant_id
            .cmp(&b.tenant_id)
            .then(a.shard_id.cmp(&b.shard_id))
    });
    Ok(placements)
}

pub fn shard_ids_from_placements(placements: &[ShardPlacement]) -> Vec<u32> {
    let mut shard_ids = BTreeSet::new();
    for placement in placements {
        shard_ids.insert(placement.shard_id);
    }
    shard_ids.into_iter().collect()
}

fn parse_replica_role(raw: &str) -> Result<ReplicaRole, &'static str> {
    match raw.to_ascii_lowercase().as_str() {
        "leader" => Ok(ReplicaRole::Leader),
        "follower" => Ok(ReplicaRole::Follower),
        _ => Err("must be one of: leader, follower"),
    }
}

fn replica_role_str(role: ReplicaRole) -> &'static str {
    match role {
        ReplicaRole::Leader => "leader",
        ReplicaRole::Follower => "follower",
    }
}

fn replica_health_str(health: ReplicaHealth) -> &'static str {
    match health {
        ReplicaHealth::Healthy => "healthy",
        ReplicaHealth::Degraded => "degraded",
        ReplicaHealth::Unavailable => "unavailable",
    }
}

fn parse_replica_health(raw: &str) -> Result<ReplicaHealth, &'static str> {
    match raw.to_ascii_lowercase().as_str() {
        "healthy" => Ok(ReplicaHealth::Healthy),
        "degraded" => Ok(ReplicaHealth::Degraded),
        "unavailable" => Ok(ReplicaHealth::Unavailable),
        _ => Err("must be one of: healthy, degraded, unavailable"),
    }
}

fn find_placement<'a>(
    placements: &'a [ShardPlacement],
    tenant_id: &str,
    shard_id: u32,
) -> Option<&'a ShardPlacement> {
    placements
        .iter()
        .find(|placement| placement.tenant_id == tenant_id && placement.shard_id == shard_id)
}

fn is_readable_replica_health(health: ReplicaHealth) -> bool {
    matches!(health, ReplicaHealth::Healthy | ReplicaHealth::Degraded)
}

fn build_ring(config: &RouterConfig) -> BTreeMap<u64, u32> {
    let mut ring = BTreeMap::new();
    let vnodes = config.virtual_nodes_per_shard.max(1);
    let shard_ids: Vec<u32> = if config.shard_ids.is_empty() {
        vec![0]
    } else {
        config.shard_ids.clone()
    };
    for shard_id in shard_ids {
        for vnode in 0..vnodes {
            let key = format!("shard:{shard_id}:vn:{vnode}");
            ring.insert(hash_key(&key), shard_id);
        }
    }
    ring
}

type RingKey = (Vec<u32>, u32);
type RingCache = Mutex<HashMap<RingKey, Arc<BTreeMap<u64, u32>>>>;

const RING_CACHE_MAX_ENTRIES: usize = 32;

/// Return the hash ring for `config`, building it once per distinct shard set
/// and virtual-node count instead of once per routed claim.
fn cached_ring(config: &RouterConfig) -> Arc<BTreeMap<u64, u32>> {
    static CACHE: OnceLock<RingCache> = OnceLock::new();
    let mut shard_ids = config.shard_ids.clone();
    shard_ids.sort_unstable();
    shard_ids.dedup();
    let key: RingKey = (shard_ids, config.virtual_nodes_per_shard.max(1));
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    let Ok(mut guard) = cache.lock() else {
        // A poisoned cache only costs us the optimisation.
        return Arc::new(build_ring(config));
    };
    if let Some(ring) = guard.get(&key) {
        return Arc::clone(ring);
    }
    let ring = Arc::new(build_ring(config));
    if guard.len() >= RING_CACHE_MAX_ENTRIES {
        guard.clear();
    }
    guard.insert(key, Arc::clone(&ring));
    ring
}

fn hash_key(value: &str) -> u64 {
    let mut hash: u64 = 1469598103934665603;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash
}

/// A parsed control-plane URL.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ControlPlaneUrl {
    tls: bool,
    authority: String,
    path: String,
}

fn parse_http_url(url: &str) -> Result<ControlPlaneUrl, String> {
    let (tls, without_scheme) = if let Some(rest) = url.strip_prefix("https://") {
        (true, rest)
    } else if let Some(rest) = url.strip_prefix("http://") {
        (false, rest)
    } else {
        return Err("control-plane URL must start with http:// or https://".to_string());
    };
    let (authority, path_and_query) = match without_scheme.split_once('/') {
        Some((authority, suffix)) => (authority, format!("/{}", suffix)),
        None => (without_scheme, "/".to_string()),
    };
    if authority.trim().is_empty() {
        return Err("control-plane URL missing host:port authority".to_string());
    }
    Ok(ControlPlaneUrl {
        tls,
        authority: authority.to_string(),
        path: path_and_query,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routing_is_deterministic_for_same_input() {
        let a = route_to_shard("tenant-a", "entity-x", 16);
        let b = route_to_shard("tenant-a", "entity-x", 16);
        assert_eq!(a, b);
    }

    #[test]
    fn routing_changes_with_entity_key() {
        let a = route_to_shard("tenant-a", "entity-x", 16);
        let b = route_to_shard("tenant-a", "entity-y", 16);
        assert_ne!(a.shard_id, b.shard_id);
    }

    #[test]
    fn routing_with_replicas_is_stable() {
        let config = RouterConfig {
            shard_ids: vec![0, 1, 2, 3],
            virtual_nodes_per_shard: 32,
            replica_count: 2,
        };
        let a = route_with_replicas("tenant-a", "entity-x", &config);
        let b = route_with_replicas("tenant-a", "entity-x", &config);
        assert_eq!(a, b);
        assert!(a.replicas.len() <= 1);
    }

    fn single_shard_config() -> RouterConfig {
        RouterConfig {
            shard_ids: vec![5],
            virtual_nodes_per_shard: 16,
            replica_count: 3,
        }
    }

    fn sample_placement() -> ShardPlacement {
        ShardPlacement {
            tenant_id: "tenant-a".to_string(),
            shard_id: 5,
            epoch: 7,
            replicas: vec![
                ReplicaPlacement {
                    node_id: "node-a".to_string(),
                    role: ReplicaRole::Leader,
                    health: ReplicaHealth::Healthy,
                },
                ReplicaPlacement {
                    node_id: "node-b".to_string(),
                    role: ReplicaRole::Follower,
                    health: ReplicaHealth::Healthy,
                },
                ReplicaPlacement {
                    node_id: "node-c".to_string(),
                    role: ReplicaRole::Follower,
                    health: ReplicaHealth::Degraded,
                },
            ],
        }
    }

    #[test]
    fn route_write_with_placement_returns_healthy_leader() {
        let config = single_shard_config();
        let placement = sample_placement();
        let routed = route_write_with_placement("tenant-a", "entity-x", &config, &[placement])
            .expect("write route should resolve");
        assert_eq!(routed.node_id, "node-a");
        assert_eq!(routed.role, ReplicaRole::Leader);
        assert_eq!(routed.epoch, 7);
    }

    #[test]
    fn route_read_with_placement_prefers_followers_when_requested() {
        let config = single_shard_config();
        let placement = sample_placement();
        let routed = route_read_with_placement(
            "tenant-a",
            "entity-x",
            &config,
            &[placement],
            ReadPreference::PreferFollower,
        )
        .expect("read route should resolve");
        assert_eq!(routed.node_id, "node-b");
        assert_eq!(routed.role, ReplicaRole::Follower);
    }

    #[test]
    fn route_write_with_placement_fails_without_healthy_leader() {
        let config = single_shard_config();
        let mut placement = sample_placement();
        set_replica_health(&mut placement, "node-a", ReplicaHealth::Unavailable)
            .expect("leader health update should succeed");
        let err = route_write_with_placement("tenant-a", "entity-x", &config, &[placement])
            .expect_err("write route should fail");
        assert!(matches!(err, PlacementRouteError::NoWritableLeader { .. }));
    }

    #[test]
    fn promote_replica_to_leader_increments_epoch_and_flips_roles() {
        let mut placement = sample_placement();
        let new_epoch =
            promote_replica_to_leader(&mut placement, "node-b").expect("promotion should succeed");
        assert_eq!(new_epoch, 8);
        let leader = placement
            .replicas
            .iter()
            .find(|replica| replica.role == ReplicaRole::Leader)
            .expect("leader should exist");
        assert_eq!(leader.node_id, "node-b");
    }

    #[test]
    fn route_read_with_placement_fails_without_readable_replicas() {
        let config = single_shard_config();
        let mut placement = sample_placement();
        for node in ["node-a", "node-b", "node-c"] {
            set_replica_health(&mut placement, node, ReplicaHealth::Unavailable)
                .expect("health update should succeed");
        }
        let err = route_read_with_placement(
            "tenant-a",
            "entity-x",
            &config,
            &[placement],
            ReadPreference::AnyHealthy,
        )
        .expect_err("read route should fail");
        assert!(matches!(err, PlacementRouteError::NoReadableReplica { .. }));
    }

    #[test]
    fn parse_shard_placements_csv_loads_replicas_per_shard() {
        let csv = r#"
            # tenant_id,shard_id,epoch,node_id,role,health
            tenant-a,0,7,node-a,leader,healthy
            tenant-a,0,7,node-b,follower,degraded
            tenant-a,1,2,node-c,leader,healthy
        "#;
        let placements = parse_shard_placements_csv(csv).expect("csv should parse");
        assert_eq!(placements.len(), 2);
        assert_eq!(placements[0].tenant_id, "tenant-a");
        assert_eq!(placements[0].shard_id, 0);
        assert_eq!(placements[0].epoch, 7);
        assert_eq!(placements[0].replicas.len(), 2);
        assert_eq!(placements[1].shard_id, 1);
        assert_eq!(placements[1].replicas.len(), 1);
    }

    #[test]
    fn parse_shard_placements_csv_rejects_epoch_conflicts() {
        let csv = r#"
            tenant-a,0,7,node-a,leader,healthy
            tenant-a,0,8,node-b,follower,healthy
        "#;
        let err = parse_shard_placements_csv(csv).expect_err("csv should reject epoch conflicts");
        assert!(err.contains("epoch mismatch"));
    }

    #[test]
    fn shard_ids_from_placements_deduplicates_and_sorts() {
        let placements = vec![
            ShardPlacement {
                tenant_id: "tenant-a".to_string(),
                shard_id: 5,
                epoch: 1,
                replicas: vec![],
            },
            ShardPlacement {
                tenant_id: "tenant-b".to_string(),
                shard_id: 2,
                epoch: 1,
                replicas: vec![],
            },
            ShardPlacement {
                tenant_id: "tenant-c".to_string(),
                shard_id: 5,
                epoch: 1,
                replicas: vec![],
            },
        ];
        assert_eq!(shard_ids_from_placements(&placements), vec![2, 5]);
    }

    #[test]
    fn render_shard_placements_csv_round_trips_with_parser() {
        let placements = vec![sample_placement()];
        let csv = render_shard_placements_csv(&placements);
        let reparsed = parse_shard_placements_csv(&csv).expect("csv should parse");
        assert_eq!(reparsed, placements);
    }

    #[test]
    fn parse_http_url_requires_http_or_https_scheme() {
        for bad in ["ftp://127.0.0.1:8090/path", "127.0.0.1:8090", "https://"] {
            let err = parse_http_url(bad).expect_err("must fail");
            assert!(
                err.contains("http:// or https://") || err.contains("authority"),
                "{bad}: {err}"
            );
        }
        let tls = parse_http_url("https://cp.internal:8443/x").expect("https parses");
        assert!(tls.tls);
        assert_eq!(tls.authority, "cp.internal:8443");
    }

    #[test]
    fn parse_http_url_extracts_authority_and_path() {
        let url = parse_http_url("http://127.0.0.1:8090/v1/control-plane/placement?format=csv")
            .expect("url should parse");
        assert!(!url.tls);
        assert_eq!(url.authority, "127.0.0.1:8090");
        assert_eq!(url.path, "/v1/control-plane/placement?format=csv");
    }

    #[test]
    fn tls_server_name_is_the_host_without_port() {
        assert_eq!(authority_host("cp.internal:8443"), "cp.internal");
        assert_eq!(authority_host("cp.internal"), "cp.internal");
        assert_eq!(authority_host("[::1]:8443"), "::1");
        assert_eq!(authority_host("127.0.0.1:8090"), "127.0.0.1");
    }
}

#[cfg(test)]
mod hardening_tests {
    use super::*;
    use std::net::TcpListener;

    fn quick_options() -> PlacementSourceOptions {
        PlacementSourceOptions {
            connect_timeout: Duration::from_millis(500),
            read_timeout: Duration::from_millis(300),
            write_timeout: Duration::from_millis(300),
            ..PlacementSourceOptions::default()
        }
    }

    /// Spawn a one-shot server that reads the request head, hands the stream
    /// to `respond`, and returns the base URL plus a handle yielding the
    /// request head text.
    fn one_shot_server(
        respond: impl FnOnce(&mut TcpStream) + Send + 'static,
    ) -> (String, std::thread::JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                if stream.read(&mut byte).unwrap_or(0) == 0 {
                    break;
                }
                head.push(byte[0]);
            }
            respond(&mut stream);
            String::from_utf8_lossy(&head).to_string()
        });
        (format!("http://{addr}"), handle)
    }

    const CSV: &str = "tenant-a,0,3,node-a,leader,healthy\n";

    #[test]
    fn client_reads_by_content_length_without_waiting_for_close() {
        let (url, handle) = one_shot_server(|stream| {
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{}",
                CSV.len(),
                CSV
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.flush().unwrap();
            // Keep the connection open: the old read_to_end client hung here.
            std::thread::sleep(Duration::from_secs(3));
        });
        let started = Instant::now();
        let placements =
            load_shard_placements_from_control_plane_with_options(&url, &quick_options())
                .expect("response framed by Content-Length should parse");
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(placements[0].epoch, 3);
        drop(handle);
    }

    #[test]
    fn client_reads_until_close_without_content_length() {
        let (url, handle) = one_shot_server(|stream| {
            stream
                .write_all(format!("HTTP/1.1 200 OK\r\n\r\n{CSV}").as_bytes())
                .unwrap();
        });
        let placements =
            load_shard_placements_from_control_plane_with_options(&url, &quick_options()).unwrap();
        assert_eq!(placements.len(), 1);
        handle.join().unwrap();
    }

    #[test]
    fn client_times_out_on_unresponsive_server() {
        let (url, handle) = one_shot_server(|_stream| {
            std::thread::sleep(Duration::from_secs(3));
        });
        let started = Instant::now();
        let err = load_shard_placements_from_control_plane_with_options(&url, &quick_options())
            .expect_err("silent server must time out");
        assert!(started.elapsed() < Duration::from_secs(2), "{err}");
        assert!(err.contains("control-plane"));
        drop(handle);
    }

    #[test]
    fn client_enforces_response_size_cap() {
        let (url, handle) = one_shot_server(|stream| {
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n")
                .unwrap();
        });
        let mut options = quick_options();
        options.max_response_bytes = 1024;
        let err =
            load_shard_placements_from_control_plane_with_options(&url, &options).unwrap_err();
        assert!(err.contains("exceeds limit"), "{err}");
        handle.join().unwrap();
    }

    #[test]
    fn token_is_not_sent_in_clear_text_to_a_remote_host() {
        let options = PlacementSourceOptions {
            bearer_token: Some("s3cret".to_string()),
            ..quick_options()
        };
        let plain = parse_http_url("http://192.0.2.10:9/v1/placement").unwrap();
        let err = http_get(&plain, &options)
            .expect_err("plain http token to a remote host must be refused");
        assert!(
            err.contains("refusing to send the control-plane token"),
            "{err}"
        );
        // Over https the token is allowed to leave (here the connect fails).
        let tls = parse_http_url("https://192.0.2.10:9/v1/placement").unwrap();
        let err = http_get(&tls, &options).expect_err("nothing listens there");
        assert!(
            !err.contains("refusing to send the control-plane token"),
            "{err}"
        );
        for local in ["127.0.0.1:80", "localhost:80", "[::1]:80", "LOCALHOST"] {
            assert!(is_loopback_authority(local), "{local}");
        }
        for remote in [
            "192.0.2.10:80",
            "example.com:80",
            "[2001:db8::1]:80",
            "10.0.0.1",
        ] {
            assert!(!is_loopback_authority(remote), "{remote}");
        }
    }

    #[test]
    fn client_sends_bearer_token() {
        let (url, handle) = one_shot_server(|stream| {
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{CSV}",
                        CSV.len()
                    )
                    .as_bytes(),
                )
                .unwrap();
        });
        let mut options = quick_options();
        options.bearer_token = Some("s3cret".to_string());
        load_shard_placements_from_control_plane_with_options(&url, &options).unwrap();
        let request = handle.join().unwrap();
        assert!(
            request.contains("Authorization: Bearer s3cret\r\n"),
            "{request}"
        );
    }

    #[test]
    fn client_rejects_follower_control_plane_response() {
        let (url, handle) = one_shot_server(|stream| {
            stream
                .write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nX-Dash-Leader: false\r\nContent-Length: {}\r\n\r\n{CSV}",
                        CSV.len()
                    )
                    .as_bytes(),
                )
                .unwrap();
        });
        let err = load_shard_placements_from_control_plane_with_options(&url, &quick_options())
            .unwrap_err();
        assert!(err.contains("not the leader"), "{err}");
        handle.join().unwrap();
    }

    fn dead_control_plane_url() -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        format!("http://{addr}")
    }

    fn temp_placement_file(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "dash-router-{tag}-{}-{nanos}.csv",
            std::process::id()
        ));
        fs::write(&path, "tenant-a,0,1,node-stale,leader,healthy\n").unwrap();
        path
    }

    #[test]
    fn unreachable_control_plane_does_not_fall_back_to_stale_file_by_default() {
        let file = temp_placement_file("nofallback");
        let err = load_shard_placements_from_source_with_options(
            Some(&file),
            Some(&dead_control_plane_url()),
            &quick_options(),
        )
        .expect_err("must not silently use the stale file");
        assert!(err.contains("refusing to fall back"), "{err}");
    }

    #[test]
    fn explicit_allow_stale_placement_falls_back_to_file() {
        let file = temp_placement_file("allowstale");
        let mut options = quick_options();
        options.allow_stale_placement = true;
        let placements = load_shard_placements_from_source_with_options(
            Some(&file),
            Some(&dead_control_plane_url()),
            &options,
        )
        .unwrap();
        assert_eq!(placements[0].replicas[0].node_id, "node-stale");
    }

    #[test]
    fn file_only_source_still_works_without_control_plane() {
        let file = temp_placement_file("fileonly");
        let placements =
            load_shard_placements_from_source_with_options(Some(&file), None, &quick_options())
                .unwrap();
        assert_eq!(placements.len(), 1);
    }

    #[test]
    fn epoch_regression_is_rejected() {
        let mk = |epoch| {
            vec![ShardPlacement {
                tenant_id: "t".to_string(),
                shard_id: 1,
                epoch,
                replicas: vec![],
            }]
        };
        assert!(ensure_no_epoch_regression(&mk(5), &mk(5)).is_ok());
        assert!(ensure_no_epoch_regression(&mk(5), &mk(6)).is_ok());
        let err = ensure_no_epoch_regression(&mk(5), &mk(4)).unwrap_err();
        assert!(err.contains("epoch regression"));
    }

    #[test]
    fn route_to_shard_separates_tenant_and_entity() {
        // Without a separator ("ab","c") and ("a","bc") hash identically for
        // every shard count.
        let differs = (2..64u32).any(|count| {
            route_to_shard("ab", "c", count).shard_id != route_to_shard("a", "bc", count).shard_id
        });
        assert!(differs);
    }

    #[test]
    fn route_to_shard_placement_is_pinned_for_sample_keys() {
        // Changing the hash re-homes every entity. If this test fails the
        // change must be deliberate and come with a data migration plan.
        let sample = [
            ("tenant-a", "entity-x"),
            ("tenant-a", "entity-y"),
            ("tenant-b", "claim-1"),
            ("acme", "user:42"),
            ("", "empty-tenant"),
        ];
        let actual: Vec<u32> = sample
            .iter()
            .map(|(tenant, entity)| route_to_shard(tenant, entity, 16).shard_id)
            .collect();
        assert_eq!(actual, PINNED_SHARDS);
    }

    const PINNED_SHARDS: [u32; 5] = [4, 7, 13, 11, 0];

    #[test]
    fn route_with_replicas_never_repeats_a_shard() {
        let config = RouterConfig {
            shard_ids: vec![9, 3, 7, 1],
            virtual_nodes_per_shard: 64,
            replica_count: 4,
        };
        for i in 0..500 {
            let plan = route_with_replicas("tenant-a", &format!("entity-{i}"), &config);
            let mut shards = vec![plan.primary.shard_id];
            shards.extend(plan.replicas.iter().map(|replica| replica.shard_id));
            let unique: HashSet<u32> = shards.iter().copied().collect();
            assert_eq!(unique.len(), shards.len(), "duplicate shard in {shards:?}");
            assert_eq!(
                shards.len(),
                4,
                "all four shards should be used: {shards:?}"
            );
        }
    }

    #[test]
    fn ring_cache_returns_shared_ring_for_same_shard_set() {
        let a = RouterConfig {
            shard_ids: vec![21, 22, 23],
            virtual_nodes_per_shard: 8,
            replica_count: 1,
        };
        let b = RouterConfig {
            shard_ids: vec![23, 21, 22],
            ..a.clone()
        };
        assert!(Arc::ptr_eq(&cached_ring(&a), &cached_ring(&b)));
        let c = RouterConfig {
            shard_ids: vec![21, 22],
            ..a.clone()
        };
        assert!(!Arc::ptr_eq(&cached_ring(&a), &cached_ring(&c)));
    }
}
