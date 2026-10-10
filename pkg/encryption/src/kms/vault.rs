//! HashiCorp Vault / OpenBao Transit secrets engine as KEK provider.
//!
//! `wrap` is `POST /v1/<mount>/encrypt/<key>`, `unwrap` is
//! `POST /v1/<mount>/decrypt/<key>`. The DEK never leaves the process
//! unencrypted except inside the TLS connection to Vault, and the key never
//! leaves Vault. The ciphertext Vault returns (`vault:v<N>:...`) carries the
//! key version, so rotating the key in Vault (`vault write -f
//! <mount>/keys/<key>/rotate`) keeps every older file readable.
//!
//! Binding to the file: the plaintext sent to Vault is the DEK followed by
//! `SHA-256("dash-vault-wrap-v1" || len(key id) || key id || file id)`, and
//! `unwrap` checks that suffix. Vault authenticates its ciphertext, so the
//! wrapped DEK cannot be moved to another file. Transit's own
//! `associated_data` parameter is not used: older Vault releases ignore
//! unknown parameters, so a ciphertext written against one of them would
//! stop decrypting after an upgrade that starts honouring it.

use std::path::PathBuf;
use std::sync::Mutex;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use sha2::{Digest, Sha256};
use url::Url;
use zeroize::Zeroizing;

use super::{
    HttpClient, KmsHttpOptions, Reply, Step, UnwrapCache, ct_eq, read_secret_file,
    retryable_status, sanitize, transport_step, with_retries,
};
use crate::{DEK_LEN, EncryptionError, KekProvider, WrappedDek};

const BINDING_LABEL: &[u8] = b"dash-vault-wrap-v1";
const KEY_ID_PREFIX: &str = "vault:";

/// Settings of [`VaultTransitKekProvider`].
#[derive(Debug, Clone)]
pub struct VaultTransitConfig {
    /// Vault address, `https://vault.example:8200`.
    pub addr: String,
    /// File holding the Vault token; re-read when Vault answers 403.
    pub token_file: PathBuf,
    /// Mount path of the Transit engine (`transit`).
    pub mount: String,
    /// Name of the Transit key new files are wrapped with.
    pub key: String,
    /// Further Transit keys (same mount) that still unwrap.
    pub previous_keys: Vec<String>,
    /// Extra CA certificates (PEM) to trust for the Vault server.
    pub ca_file: Option<PathBuf>,
    /// Vault Enterprise / OpenBao namespace (`X-Vault-Namespace`).
    pub namespace: Option<String>,
    pub http: KmsHttpOptions,
    /// Allow plain http to a non-loopback address.
    pub allow_insecure_http: bool,
}

/// The id recorded in file headers for a Transit key: `vault:<mount>/<key>`.
pub fn vault_key_id(mount: &str, key: &str) -> String {
    format!("{KEY_ID_PREFIX}{mount}/{key}")
}

fn valid_mount(mount: &str) -> bool {
    !mount.is_empty()
        && !mount.starts_with('/')
        && !mount.ends_with('/')
        && mount
            .split('/')
            .all(|seg| !seg.is_empty() && seg != "." && seg != "..")
        && mount
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-' | b'/'))
}

fn valid_key_name(key: &str) -> bool {
    !key.is_empty()
        && key != "."
        && key != ".."
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

struct VaultKey {
    id: String,
    name: String,
}

/// KEKs held by a Vault Transit engine.
pub struct VaultTransitKekProvider {
    base: Url,
    mount: String,
    keys: Vec<VaultKey>,
    token_file: PathBuf,
    token: Mutex<Zeroizing<String>>,
    namespace: Option<String>,
    http: HttpClient,
    opts: KmsHttpOptions,
    cache: UnwrapCache,
}

impl std::fmt::Debug for VaultTransitKekProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultTransitKekProvider")
            .field("addr", &self.base.as_str())
            .field("active_key_id", &self.keys[0].id)
            .finish_non_exhaustive()
    }
}

impl VaultTransitKekProvider {
    /// Validates the settings and reads the token. Makes no request; see
    /// [`crate::check_provider`] for the start-up round trip.
    pub fn new(config: VaultTransitConfig) -> Result<Self, EncryptionError> {
        use crate::global::{
            VAULT_ADDR_ENV, VAULT_KEY_ENV, VAULT_MOUNT_ENV, VAULT_NAMESPACE_ENV,
            VAULT_PREVIOUS_KEYS_ENV, VAULT_TOKEN_FILE_ENV,
        };
        let base = super::parse_endpoint(&config.addr, VAULT_ADDR_ENV, config.allow_insecure_http)?;
        let mount = config.mount.trim().trim_matches('/').to_string();
        if !valid_mount(&mount) {
            return Err(EncryptionError::Config(format!(
                "{VAULT_MOUNT_ENV}: '{mount}' is not a valid mount path (letters, digits, '.', '_', '-', '/')"
            )));
        }
        let mut keys: Vec<VaultKey> = Vec::new();
        let names =
            std::iter::once(config.key.trim()).chain(config.previous_keys.iter().map(|k| k.trim()));
        for (idx, name) in names.enumerate() {
            let setting = if idx == 0 {
                VAULT_KEY_ENV
            } else {
                VAULT_PREVIOUS_KEYS_ENV
            };
            if name.is_empty() && idx > 0 {
                continue;
            }
            if !valid_key_name(name) {
                return Err(EncryptionError::Config(format!(
                    "{setting}: '{name}' is not a valid Transit key name (letters, digits, '.', '_', '-')"
                )));
            }
            let id = vault_key_id(&mount, name);
            if !crate::header::valid_key_id(&id) {
                return Err(EncryptionError::Config(format!(
                    "{setting}: key id '{id}' is longer than 128 characters; use a shorter mount path or key name"
                )));
            }
            if !keys.iter().any(|k| k.id == id) {
                keys.push(VaultKey {
                    id,
                    name: name.to_string(),
                });
            }
        }
        let namespace = config
            .namespace
            .as_deref()
            .map(str::trim)
            .filter(|n| !n.is_empty())
            .map(str::to_string);
        if let Some(ns) = &namespace
            && !ns.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(EncryptionError::Config(format!(
                "{VAULT_NAMESPACE_ENV}: the namespace must be visible ASCII"
            )));
        }
        let token = read_secret_file(&config.token_file, VAULT_TOKEN_FILE_ENV)?;
        let http = HttpClient::new(
            base.scheme() == "https",
            config.ca_file.as_deref(),
            &config.http,
        )
        .map_err(|e| e.context(crate::global::VAULT_CA_FILE_ENV))?;
        Ok(Self {
            base,
            mount,
            keys,
            token_file: config.token_file,
            token: Mutex::new(token),
            namespace,
            http,
            opts: config.http,
            cache: UnwrapCache::new(),
        })
    }

    fn endpoint(&self, op: &str, key: &str) -> String {
        let mut base = self.base.as_str().trim_end_matches('/').to_string();
        base.push_str(&format!("/v1/{}/{op}/{key}", self.mount));
        base
    }

    fn binding(key_id: &str, context: &[u8]) -> [u8; 32] {
        let mut h = Sha256::new();
        h.update(BINDING_LABEL);
        h.update([key_id.len() as u8]);
        h.update(key_id.as_bytes());
        h.update(context);
        h.finalize().into()
    }

    fn current_token(&self) -> Zeroizing<String> {
        self.token.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Re-reads the token file; true when it now holds a different token.
    fn reload_token(&self, used: &str) -> Result<bool, EncryptionError> {
        let fresh = read_secret_file(&self.token_file, crate::global::VAULT_TOKEN_FILE_ENV)?;
        let mut token = self.token.lock().unwrap_or_else(|e| e.into_inner());
        let changed = fresh.as_str() != used;
        *token = fresh;
        Ok(changed)
    }

    /// One Transit call with retries and token reload; returns the `data`
    /// object of the answer. `op` is `encrypt` or `decrypt`.
    fn call(
        &self,
        op: &str,
        key: &VaultKey,
        body: &Zeroizing<String>,
    ) -> Result<Zeroizing<String>, EncryptionError> {
        let url = self.endpoint(op, &key.name);
        let what = format!("Vault Transit {op} with key {} at {}", key.id, self.base);
        with_retries(&self.opts, &what, || {
            let token = self.current_token();
            let mut headers: Vec<(&str, &str)> = vec![
                ("X-Vault-Token", token.as_str()),
                ("Content-Type", "application/json"),
                ("Accept", "application/json"),
                ("X-Vault-Request", "true"),
            ];
            if let Some(ns) = &self.namespace {
                headers.push(("X-Vault-Namespace", ns.as_str()));
            }
            let secrets = [token.as_str()];
            let reply = match self.http.send(
                "POST",
                &url,
                &headers,
                Some(body.as_bytes()),
                self.opts.attempt_timeout,
                &secrets,
            ) {
                Ok(reply) => reply,
                Err(failure) => return transport_step(failure, &self.opts),
            };
            self.interpret(op, key, reply, &token)
        })
    }

    fn interpret(
        &self,
        op: &str,
        key: &VaultKey,
        reply: Reply,
        token: &str,
    ) -> Step<Zeroizing<String>> {
        let status = reply.status;
        if (200..300).contains(&status) {
            return match extract_data_field(
                &reply.body,
                if op == "encrypt" {
                    "ciphertext"
                } else {
                    "plaintext"
                },
            ) {
                Some(value) => Step::Done(value),
                None => Step::Fail(EncryptionError::Kms(format!(
                    "Vault Transit {op} with key {}: unexpected response (no data field)",
                    key.id
                ))),
            };
        }
        let detail = vault_errors(&reply.body, &[token]);
        if status == 403 {
            return match self.reload_token(token) {
                Ok(true) => Step::Again(format!("permission denied (HTTP 403): {detail}")),
                Ok(false) => Step::Fail(EncryptionError::Kms(format!(
                    "Vault Transit {op} with key {} at {}: permission denied (HTTP 403): {detail}; the token in {} was rejected (expired, revoked, or its policy lacks `update` on {}/{op}/{})",
                    key.id,
                    self.base,
                    self.token_file.display(),
                    self.mount,
                    key.name
                ))),
                Err(err) => Step::Fail(err),
            };
        }
        if retryable_status(status) {
            return Step::Retry {
                error: format!("HTTP {status}: {detail}"),
                retry_after: reply.retry_after,
            };
        }
        if op == "decrypt" && status == 400 {
            return Step::Fail(EncryptionError::Authentication(format!(
                "data key (Vault Transit decrypt with key {} refused the ciphertext: {detail})",
                key.id
            )));
        }
        Step::Fail(EncryptionError::Kms(format!(
            "Vault Transit {op} with key {} at {}: HTTP {status}: {detail}",
            key.id, self.base
        )))
    }
}

/// The `errors` list of a Vault error answer, sanitized.
fn vault_errors(body: &[u8], secrets: &[&str]) -> String {
    let parsed: Option<serde_json::Value> = serde_json::from_slice(body).ok();
    let joined = parsed
        .as_ref()
        .and_then(|v| v.get("errors"))
        .and_then(|e| e.as_array())
        .map(|list| {
            list.iter()
                .filter_map(|e| e.as_str())
                .collect::<Vec<_>>()
                .join("; ")
        });
    match joined {
        Some(text) if !text.is_empty() => sanitize(text.as_bytes(), secrets),
        _ if body.is_empty() => "(empty response)".to_string(),
        _ => sanitize(body, secrets),
    }
}

/// `data.<field>` of a Vault answer, as a string (cleared on drop).
fn extract_data_field(body: &[u8], field: &str) -> Option<Zeroizing<String>> {
    let mut value: serde_json::Value = serde_json::from_slice(body).ok()?;
    let out = value
        .get("data")
        .and_then(|d| d.get(field))
        .and_then(|f| f.as_str())
        .map(|s| Zeroizing::new(s.to_string()));
    // serde_json::Value does not zeroize; clear the string we know about.
    if let Some(serde_json::Value::String(s)) = value.get_mut("data").and_then(|d| d.get_mut(field))
    {
        zeroize::Zeroize::zeroize(s);
    }
    out
}

impl KekProvider for VaultTransitKekProvider {
    fn name(&self) -> &'static str {
        "vault"
    }

    fn active_key_id(&self) -> &str {
        &self.keys[0].id
    }

    fn key_ids(&self) -> Vec<String> {
        self.keys.iter().map(|k| k.id.clone()).collect()
    }

    fn wrap(&self, dek: &[u8; DEK_LEN], context: &[u8]) -> Result<WrappedDek, EncryptionError> {
        let key = &self.keys[0];
        let mut payload = Zeroizing::new(Vec::with_capacity(DEK_LEN + 32));
        payload.extend_from_slice(dek);
        payload.extend_from_slice(&Self::binding(&key.id, context));
        let encoded = Zeroizing::new(B64.encode(&*payload));
        let body = Zeroizing::new(format!("{{\"plaintext\":\"{}\"}}", encoded.as_str()));
        let ciphertext = self.call("encrypt", key, &body)?;
        if !ciphertext.starts_with("vault:v") || !ciphertext.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(EncryptionError::Kms(format!(
                "Vault Transit encrypt with key {}: unexpected ciphertext format",
                key.id
            )));
        }
        Ok(WrappedDek {
            key_id: key.id.clone(),
            bytes: ciphertext.as_bytes().to_vec(),
        })
    }

    fn unwrap(
        &self,
        key_id: &str,
        wrapped: &[u8],
        context: &[u8],
    ) -> Result<Zeroizing<[u8; DEK_LEN]>, EncryptionError> {
        let key = self.keys.iter().find(|k| k.id == key_id).ok_or_else(|| {
            EncryptionError::UnknownKey {
                what: "data key".to_string(),
                key_id: key_id.to_string(),
                configured: self.key_ids().join(", "),
            }
        })?;
        if let Some(dek) = self.cache.get(key_id, wrapped, context) {
            return Ok(dek);
        }
        let ciphertext = std::str::from_utf8(wrapped)
            .ok()
            .filter(|c| {
                c.starts_with("vault:v")
                    && c.bytes()
                        .all(|b| b.is_ascii_graphic() && b != b'"' && b != b'\\')
            })
            .ok_or_else(|| {
                EncryptionError::Format(
                    "wrapped data key is not a Vault Transit ciphertext".to_string(),
                )
            })?;
        let body = Zeroizing::new(format!("{{\"ciphertext\":\"{ciphertext}\"}}"));
        let encoded = self.call("decrypt", key, &body)?;
        let plain = Zeroizing::new(B64.decode(encoded.as_bytes()).map_err(|_| {
            EncryptionError::Kms(format!(
                "Vault Transit decrypt with key {}: plaintext is not base64",
                key.id
            ))
        })?);
        if plain.len() != DEK_LEN + 32 {
            return Err(EncryptionError::Authentication("data key".to_string()));
        }
        let (dek_bytes, binding) = plain.split_at(DEK_LEN);
        if !ct_eq(binding, &Self::binding(key_id, context)) {
            return Err(EncryptionError::Authentication("data key".to_string()));
        }
        let mut dek = Zeroizing::new([0u8; DEK_LEN]);
        dek.copy_from_slice(dek_bytes);
        self.cache.put(key_id, wrapped, context, &dek);
        Ok(dek)
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::*;
    use crate::kms::mock::{MockVault, write_token};

    fn config(vault: &MockVault, token_file: PathBuf) -> VaultTransitConfig {
        VaultTransitConfig {
            addr: vault.addr(),
            token_file,
            mount: "transit".to_string(),
            key: "dash".to_string(),
            previous_keys: vec![],
            ca_file: None,
            namespace: None,
            http: KmsHttpOptions {
                attempt_timeout: Duration::from_millis(500),
                max_retries: 2,
                backoff_base: Duration::from_millis(5),
                backoff_max: Duration::from_millis(20),
            },
            allow_insecure_http: false,
        }
    }

    #[test]
    fn round_trip_binds_the_file_and_survives_key_rotation() {
        let dir = tempfile::tempdir().unwrap();
        let vault = MockVault::start("s.good");
        let provider =
            VaultTransitKekProvider::new(config(&vault, write_token(dir.path(), "t", "s.good")))
                .unwrap();
        assert_eq!(provider.active_key_id(), "vault:transit/dash");
        let dek = [7u8; 32];
        let wrapped = provider.wrap(&dek, b"file-1").unwrap();
        assert_eq!(wrapped.key_id, "vault:transit/dash");
        assert!(wrapped.bytes.starts_with(b"vault:v1:"));
        assert!(crate::header::valid_key_id(&wrapped.key_id));

        // A fresh provider (empty cache) unwraps through Vault.
        let fresh =
            VaultTransitKekProvider::new(config(&vault, write_token(dir.path(), "t2", "s.good")))
                .unwrap();
        assert_eq!(
            *fresh
                .unwrap(&wrapped.key_id, &wrapped.bytes, b"file-1")
                .unwrap(),
            dek
        );
        // Wrong file id: rejected by the binding.
        let err = fresh
            .unwrap(&wrapped.key_id, &wrapped.bytes, b"file-2")
            .unwrap_err();
        assert!(matches!(err, EncryptionError::Authentication(_)), "{err}");
        // Damaged ciphertext: Vault refuses it.
        let mut bad = wrapped.bytes.clone();
        let last = bad.len() - 3;
        bad[last] = if bad[last] == b'A' { b'B' } else { b'A' };
        let err = fresh.unwrap(&wrapped.key_id, &bad, b"file-1").unwrap_err();
        assert!(matches!(err, EncryptionError::Authentication(_)), "{err}");
        // Unknown key id.
        assert!(matches!(
            fresh.unwrap("vault:transit/other", &wrapped.bytes, b"file-1"),
            Err(EncryptionError::UnknownKey { .. })
        ));

        // Rotate the key in Vault: new wraps use v2, v1 still unwraps.
        vault.rotate("dash");
        let newer = fresh.wrap(&dek, b"file-3").unwrap();
        assert!(newer.bytes.starts_with(b"vault:v2:"));
        let third =
            VaultTransitKekProvider::new(config(&vault, write_token(dir.path(), "t3", "s.good")))
                .unwrap();
        assert_eq!(
            *third
                .unwrap(&wrapped.key_id, &wrapped.bytes, b"file-1")
                .unwrap(),
            dek
        );
        assert_eq!(
            *third
                .unwrap(&newer.key_id, &newer.bytes, b"file-3")
                .unwrap(),
            dek
        );
    }

    #[test]
    fn unwraps_are_cached() {
        let dir = tempfile::tempdir().unwrap();
        let vault = MockVault::start("s.good");
        let provider =
            VaultTransitKekProvider::new(config(&vault, write_token(dir.path(), "t", "s.good")))
                .unwrap();
        let wrapped = provider.wrap(&[1u8; 32], b"f").unwrap();
        let before = vault.requests("decrypt");
        for _ in 0..3 {
            provider
                .unwrap(&wrapped.key_id, &wrapped.bytes, b"f")
                .unwrap();
        }
        assert_eq!(vault.requests("decrypt"), before + 1);
    }

    #[test]
    fn a_rotated_token_file_is_picked_up_after_a_403() {
        let dir = tempfile::tempdir().unwrap();
        let vault = MockVault::start("s.first");
        let token = write_token(dir.path(), "token", "s.first");
        let provider = VaultTransitKekProvider::new(config(&vault, token.clone())).unwrap();
        provider.wrap(&[1u8; 32], b"f").unwrap();

        // Vault now only accepts a new token; the file is rewritten.
        vault.set_token("s.second");
        write_token(dir.path(), "token", "s.second");
        let wrapped = provider.wrap(&[2u8; 32], b"g").unwrap();
        assert_eq!(
            *provider
                .unwrap(&wrapped.key_id, &wrapped.bytes, b"g")
                .unwrap(),
            [2u8; 32]
        );

        // The file still holds a rejected token: a clear error, no secret in it.
        vault.set_token("s.third");
        let err = provider.wrap(&[3u8; 32], b"h").unwrap_err().to_string();
        assert!(err.contains("403"), "{err}");
        assert!(err.contains(&token.display().to_string()), "{err}");
        assert!(
            !err.contains("s.second") && !err.contains("s.third"),
            "{err}"
        );
    }

    #[test]
    fn server_errors_are_retried_with_backoff_and_then_reported() {
        let dir = tempfile::tempdir().unwrap();
        let vault = MockVault::start("s.good");
        let provider =
            VaultTransitKekProvider::new(config(&vault, write_token(dir.path(), "t", "s.good")))
                .unwrap();
        vault.fail_next(2, 503);
        provider.wrap(&[1u8; 32], b"f").unwrap();
        assert_eq!(vault.requests("encrypt"), 3);

        vault.fail_next(10, 500);
        let err = provider.wrap(&[1u8; 32], b"f").unwrap_err().to_string();
        assert!(err.contains("HTTP 500"), "{err}");
        assert!(err.contains("gave up after 3 attempts"), "{err}");

        vault.fail_next(0, 0);
        vault.fail_next(1, 429);
        provider.wrap(&[1u8; 32], b"f").unwrap();
    }

    #[test]
    fn a_hanging_server_times_out() {
        let dir = tempfile::tempdir().unwrap();
        let vault = MockVault::start("s.good");
        let mut cfg = config(&vault, write_token(dir.path(), "t", "s.good"));
        cfg.http.attempt_timeout = Duration::from_millis(150);
        cfg.http.max_retries = 1;
        let provider = VaultTransitKekProvider::new(cfg).unwrap();
        vault.delay_next(5, Duration::from_millis(1500));
        let started = Instant::now();
        let err = provider.wrap(&[1u8; 32], b"f").unwrap_err().to_string();
        assert!(err.contains("no response within 150 ms"), "{err}");
        assert!(err.contains("gave up after 2 attempts"), "{err}");
        assert!(
            started.elapsed() < Duration::from_millis(1400),
            "{:?}",
            started.elapsed()
        );

        // One slow answer followed by a fast one succeeds.
        vault.delay_next(0, Duration::ZERO);
        vault.delay_next(1, Duration::from_millis(600));
        provider.wrap(&[1u8; 32], b"f").unwrap();
    }

    #[test]
    fn connection_refused_is_retried_and_reported() {
        let dir = tempfile::tempdir().unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let vault = MockVault::start("s.good");
        let mut cfg = config(&vault, write_token(dir.path(), "t", "s.good"));
        cfg.addr = addr;
        let provider = VaultTransitKekProvider::new(cfg).unwrap();
        let err = provider.wrap(&[1u8; 32], b"f").unwrap_err().to_string();
        assert!(err.contains("gave up after 3 attempts"), "{err}");
    }

    #[test]
    fn namespace_and_mount_are_sent() {
        let dir = tempfile::tempdir().unwrap();
        let vault = MockVault::start("s.good");
        let mut cfg = config(&vault, write_token(dir.path(), "t", "s.good"));
        cfg.namespace = Some("team-a".to_string());
        cfg.mount = "/kms/transit/".to_string();
        let provider = VaultTransitKekProvider::new(cfg).unwrap();
        assert_eq!(provider.active_key_id(), "vault:kms/transit/dash");
        provider.wrap(&[1u8; 32], b"f").unwrap();
        let last = vault.last_request().unwrap();
        assert_eq!(last.path, "/v1/kms/transit/encrypt/dash");
        assert_eq!(last.header("x-vault-namespace").as_deref(), Some("team-a"));
    }

    #[test]
    fn bad_settings_fail_with_clear_messages() {
        let dir = tempfile::tempdir().unwrap();
        let vault = MockVault::start("s.good");
        let token = write_token(dir.path(), "t", "s.good");
        let mut cfg = config(&vault, token.clone());
        cfg.mount = "a/../b".to_string();
        assert!(
            VaultTransitKekProvider::new(cfg)
                .unwrap_err()
                .to_string()
                .contains("MOUNT")
        );
        let mut cfg = config(&vault, token.clone());
        cfg.key = "has space".to_string();
        assert!(
            VaultTransitKekProvider::new(cfg)
                .unwrap_err()
                .to_string()
                .contains("KEY")
        );
        let mut cfg = config(&vault, token.clone());
        cfg.key = "k".repeat(130);
        assert!(
            VaultTransitKekProvider::new(cfg)
                .unwrap_err()
                .to_string()
                .contains("128")
        );
        let mut cfg = config(&vault, dir.path().join("missing"));
        cfg.key = "dash".to_string();
        assert!(
            VaultTransitKekProvider::new(cfg)
                .unwrap_err()
                .to_string()
                .contains("TOKEN_FILE")
        );
        let mut cfg = config(&vault, token.clone());
        cfg.addr = "http://vault.example:8200".to_string();
        assert!(
            VaultTransitKekProvider::new(cfg)
                .unwrap_err()
                .to_string()
                .contains("plain http")
        );
        let mut cfg = config(&vault, token);
        cfg.ca_file = Some(dir.path().join("missing-ca.pem"));
        assert!(
            VaultTransitKekProvider::new(cfg)
                .unwrap_err()
                .to_string()
                .contains("CA")
        );
    }

    #[test]
    fn previous_keys_unwrap_but_do_not_wrap() {
        let dir = tempfile::tempdir().unwrap();
        let vault = MockVault::start("s.good");
        let token = write_token(dir.path(), "t", "s.good");
        let mut old_cfg = config(&vault, token.clone());
        old_cfg.key = "old".to_string();
        let old = VaultTransitKekProvider::new(old_cfg).unwrap();
        let wrapped = old.wrap(&[4u8; 32], b"f").unwrap();
        let mut cfg = config(&vault, token);
        cfg.previous_keys = vec!["old".to_string(), " ".to_string()];
        let provider = VaultTransitKekProvider::new(cfg).unwrap();
        assert_eq!(
            provider.key_ids(),
            vec!["vault:transit/dash", "vault:transit/old"]
        );
        assert_eq!(
            *provider
                .unwrap(&wrapped.key_id, &wrapped.bytes, b"f")
                .unwrap(),
            [4u8; 32]
        );
        assert_eq!(
            provider.wrap(&[4u8; 32], b"f").unwrap().key_id,
            "vault:transit/dash"
        );
    }
}
