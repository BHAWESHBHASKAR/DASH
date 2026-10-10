//! The keyring in effect for this process (and, in tests, this thread).

use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use crate::kms::aws::{AwsKmsConfig, AwsKmsKekProvider};
use crate::kms::aws_creds::CredentialSource;
use crate::kms::chain::{ChainedKekProvider, check_provider};
use crate::kms::vault::{VaultTransitConfig, VaultTransitKekProvider};
use crate::{EncryptionError, KekProvider, Keyring, KmsHttpOptions, LocalKekProvider};

/// Path of the active key-encryption key file (local provider). Setting it
/// turns encryption at rest on.
pub const KEY_FILE_ENV: &str = "DASH_ENCRYPTION_KEY_FILE";
/// Comma-separated paths of retired key files that still decrypt.
pub const PREVIOUS_KEY_FILES_ENV: &str = "DASH_ENCRYPTION_PREVIOUS_KEY_FILES";
/// KEK provider: `local` (default), `vault` or `aws-kms`.
pub const PROVIDER_ENV: &str = "DASH_ENCRYPTION_PROVIDER";
/// Vault address (`https://vault.example:8200`).
pub const VAULT_ADDR_ENV: &str = "DASH_ENCRYPTION_VAULT_ADDR";
/// File holding the Vault token (re-read when Vault answers 403).
pub const VAULT_TOKEN_FILE_ENV: &str = "DASH_ENCRYPTION_VAULT_TOKEN_FILE";
/// Transit mount path (default `transit`).
pub const VAULT_MOUNT_ENV: &str = "DASH_ENCRYPTION_VAULT_MOUNT";
/// Transit key that wraps new files.
pub const VAULT_KEY_ENV: &str = "DASH_ENCRYPTION_VAULT_KEY";
/// Comma-separated Transit keys (same mount) that still unwrap.
pub const VAULT_PREVIOUS_KEYS_ENV: &str = "DASH_ENCRYPTION_VAULT_PREVIOUS_KEYS";
/// Extra CA certificates (PEM) for the Vault server.
pub const VAULT_CA_FILE_ENV: &str = "DASH_ENCRYPTION_VAULT_CA_FILE";
/// Vault Enterprise / OpenBao namespace.
pub const VAULT_NAMESPACE_ENV: &str = "DASH_ENCRYPTION_VAULT_NAMESPACE";
/// KMS key that wraps new files (key id, key ARN, alias name or alias ARN).
pub const AWS_KMS_KEY_ID_ENV: &str = "DASH_ENCRYPTION_AWS_KMS_KEY_ID";
/// Comma-separated KMS keys that still unwrap.
pub const AWS_KMS_PREVIOUS_KEY_IDS_ENV: &str = "DASH_ENCRYPTION_AWS_KMS_PREVIOUS_KEY_IDS";
/// AWS region of the key (falls back to `AWS_REGION`, `AWS_DEFAULT_REGION`,
/// then the key ARN).
pub const AWS_REGION_ENV: &str = "DASH_ENCRYPTION_AWS_REGION";
/// KMS endpoint override (falls back to `AWS_ENDPOINT_URL_KMS`).
pub const AWS_KMS_ENDPOINT_ENV: &str = "DASH_ENCRYPTION_AWS_KMS_ENDPOINT";
/// Timeout of one key manager request in milliseconds.
pub const KMS_TIMEOUT_MS_ENV: &str = "DASH_ENCRYPTION_KMS_TIMEOUT_MS";
/// Retries of a key manager request after the first attempt.
pub const KMS_MAX_RETRIES_ENV: &str = "DASH_ENCRYPTION_KMS_MAX_RETRIES";

static INSTALLED: RwLock<Option<Arc<Keyring>>> = RwLock::new(None);

thread_local! {
    static OVERRIDE: RefCell<Option<Option<Arc<Keyring>>>> = const { RefCell::new(None) };
}

/// Sets the process-wide keyring (`None`: encryption off). Services call
/// this once at startup, before opening any data file.
pub fn install(keyring: Option<Arc<Keyring>>) {
    *INSTALLED.write().unwrap_or_else(|e| e.into_inner()) = keyring;
}

/// The keyring in effect: this thread's override (see [`with_keyring`]) or
/// the installed one.
pub fn current() -> Option<Arc<Keyring>> {
    if let Some(over) = OVERRIDE.with(|o| o.borrow().clone()) {
        return over;
    }
    INSTALLED.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Runs `f` with `keyring` in effect on this thread only (for tests that
/// share a process). Objects that capture the keyring when they are created
/// (a WAL handle, a redb store) keep it on other threads.
pub fn with_keyring<R>(keyring: Option<Arc<Keyring>>, f: impl FnOnce() -> R) -> R {
    struct Restore(Option<Option<Arc<Keyring>>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            let previous = self.0.take();
            OVERRIDE.with(|o| *o.borrow_mut() = previous);
        }
    }
    let previous = OVERRIDE.with(|o| o.borrow_mut().replace(keyring));
    let _restore = Restore(previous);
    f()
}

/// Builds the keyring from raw setting values: `key_file` (empty or unset:
/// encryption off) and a comma-separated `previous` list.
pub fn keyring_from_values(
    key_file: Option<&str>,
    previous: Option<&str>,
) -> Result<Option<Arc<Keyring>>, EncryptionError> {
    let key_file = key_file.map(str::trim).filter(|v| !v.is_empty());
    let previous: Vec<PathBuf> = previous
        .unwrap_or("")
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
        .collect();
    let Some(key_file) = key_file else {
        if !previous.is_empty() {
            return Err(EncryptionError::Config(format!(
                "{PREVIOUS_KEY_FILES_ENV} is set but {KEY_FILE_ENV} is not; previous keys only decrypt and need an active key"
            )));
        }
        return Ok(None);
    };
    let provider = LocalKekProvider::from_files(&PathBuf::from(key_file), &previous)?;
    Ok(Some(Arc::new(Keyring::new(Arc::new(provider)))))
}

/// The keyring the environment describes (see [`keyring_from_lookup`]).
pub fn keyring_from_env() -> Result<Option<Arc<Keyring>>, EncryptionError> {
    keyring_from_lookup(&|name| std::env::var(name).ok())
}

fn value(lookup: &dyn Fn(&str) -> Option<String>, name: &str) -> Option<String> {
    lookup(name)
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn list(lookup: &dyn Fn(&str) -> Option<String>, name: &str) -> Vec<String> {
    lookup(name)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string)
        .collect()
}

fn number<T: std::str::FromStr + PartialOrd + std::fmt::Display>(
    lookup: &dyn Fn(&str) -> Option<String>,
    name: &str,
    default: T,
    min: T,
    max: T,
) -> Result<T, EncryptionError> {
    match value(lookup, name) {
        None => Ok(default),
        Some(raw) => match raw.parse::<T>() {
            Ok(v) if v >= min && v <= max => Ok(v),
            _ => Err(EncryptionError::Config(format!(
                "{name}: '{raw}' is not a number between {min} and {max}"
            ))),
        },
    }
}

/// Timeouts and retries for key manager calls from
/// `DASH_ENCRYPTION_KMS_TIMEOUT_MS` / `DASH_ENCRYPTION_KMS_MAX_RETRIES`.
fn kms_http_options(
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<KmsHttpOptions, EncryptionError> {
    let timeout_ms = number(
        lookup,
        KMS_TIMEOUT_MS_ENV,
        crate::kms::DEFAULT_TIMEOUT_MS,
        1,
        600_000,
    )?;
    let max_retries = number(
        lookup,
        KMS_MAX_RETRIES_ENV,
        crate::kms::DEFAULT_MAX_RETRIES,
        0,
        10,
    )?;
    Ok(KmsHttpOptions {
        attempt_timeout: Duration::from_millis(timeout_ms),
        max_retries,
        ..KmsHttpOptions::default()
    })
}

fn insecure_http_allowed(lookup: &dyn Fn(&str) -> Option<String>) -> bool {
    value(lookup, crate::kms::ALLOW_INSECURE_HTTP_ENV).as_deref() == Some("1")
}

/// Settings of the Vault Transit provider from `DASH_ENCRYPTION_VAULT_*`.
pub fn vault_config_from_lookup(
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<VaultTransitConfig, EncryptionError> {
    let required = |name: &str| {
        value(lookup, name).ok_or_else(|| {
            EncryptionError::Config(format!("{name} is required with {PROVIDER_ENV}=vault"))
        })
    };
    Ok(VaultTransitConfig {
        addr: required(VAULT_ADDR_ENV)?,
        token_file: PathBuf::from(required(VAULT_TOKEN_FILE_ENV)?),
        mount: value(lookup, VAULT_MOUNT_ENV).unwrap_or_else(|| "transit".to_string()),
        key: required(VAULT_KEY_ENV)?,
        previous_keys: list(lookup, VAULT_PREVIOUS_KEYS_ENV),
        ca_file: value(lookup, VAULT_CA_FILE_ENV).map(PathBuf::from),
        namespace: value(lookup, VAULT_NAMESPACE_ENV),
        http: kms_http_options(lookup)?,
        allow_insecure_http: insecure_http_allowed(lookup),
    })
}

/// Settings of the AWS KMS provider from `DASH_ENCRYPTION_AWS_*` and the
/// standard AWS variables (region, endpoint, credentials).
pub fn aws_kms_config_from_lookup(
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<AwsKmsConfig, EncryptionError> {
    let key_id = value(lookup, AWS_KMS_KEY_ID_ENV).ok_or_else(|| {
        EncryptionError::Config(format!(
            "{AWS_KMS_KEY_ID_ENV} is required with {PROVIDER_ENV}=aws-kms"
        ))
    })?;
    let region = value(lookup, AWS_REGION_ENV)
        .or_else(|| value(lookup, "AWS_REGION"))
        .or_else(|| value(lookup, "AWS_DEFAULT_REGION"))
        .or_else(|| crate::kms::aws::region_from_arn(&key_id).map(str::to_string))
        .ok_or_else(|| {
            EncryptionError::Config(format!(
                "no AWS region: set {AWS_REGION_ENV} (or AWS_REGION), or use a key ARN"
            ))
        })?;
    let endpoint =
        value(lookup, AWS_KMS_ENDPOINT_ENV).or_else(|| value(lookup, "AWS_ENDPOINT_URL_KMS"));
    let credentials = CredentialSource::from_lookup(lookup, &region)?;
    Ok(AwsKmsConfig {
        key_id,
        previous_key_ids: list(lookup, AWS_KMS_PREVIOUS_KEY_IDS_ENV),
        region,
        endpoint,
        credentials,
        http: kms_http_options(lookup)?,
        allow_insecure_http: insecure_http_allowed(lookup),
    })
}

/// Builds the keyring from settings looked up by name (the process
/// environment in [`keyring_from_env`]).
///
/// `DASH_ENCRYPTION_PROVIDER` picks the KEK provider:
///
/// * unset or `local`: key files. Encryption is on when
///   `DASH_ENCRYPTION_KEY_FILE` is set; `DASH_ENCRYPTION_PREVIOUS_KEY_FILES`
///   lists retired keys.
/// * `vault`: Vault / OpenBao Transit (`DASH_ENCRYPTION_VAULT_*`).
/// * `aws-kms`: AWS KMS (`DASH_ENCRYPTION_AWS_*`, standard AWS credentials).
///
/// With a key manager, `DASH_ENCRYPTION_KEY_FILE` must be unset and
/// `DASH_ENCRYPTION_PREVIOUS_KEY_FILES` keeps files under local keys
/// readable (migration with `wal-inspect rewrap`). A key manager provider is
/// checked with one wrap and unwrap before this returns, so a wrong address,
/// token, key or policy fails here.
pub fn keyring_from_lookup(
    lookup: &dyn Fn(&str) -> Option<String>,
) -> Result<Option<Arc<Keyring>>, EncryptionError> {
    let provider = value(lookup, PROVIDER_ENV)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let key_file = value(lookup, KEY_FILE_ENV);
    let previous = lookup(PREVIOUS_KEY_FILES_ENV);
    let kms_hint = if value(lookup, VAULT_ADDR_ENV).is_some() {
        Some(("vault", VAULT_ADDR_ENV))
    } else if value(lookup, AWS_KMS_KEY_ID_ENV).is_some() {
        Some(("aws-kms", AWS_KMS_KEY_ID_ENV))
    } else {
        None
    };
    let kms: Arc<dyn KekProvider> = match provider.as_str() {
        "" | "local" => {
            if provider.is_empty()
                && key_file.is_none()
                && let Some((name, setting)) = kms_hint
            {
                return Err(EncryptionError::Config(format!(
                    "{setting} is set but {PROVIDER_ENV} is not; set {PROVIDER_ENV}={name} to use it (encryption would otherwise stay off)"
                )));
            }
            if provider == "local" && key_file.is_none() {
                return Err(EncryptionError::Config(format!(
                    "{PROVIDER_ENV}=local needs {KEY_FILE_ENV}"
                )));
            }
            return keyring_from_values(key_file.as_deref(), previous.as_deref());
        }
        "vault" | "aws-kms" => {
            if key_file.is_some() {
                return Err(EncryptionError::Config(format!(
                    "{KEY_FILE_ENV} is set but {PROVIDER_ENV}={provider}: new files are wrapped by {provider}; to keep files under the local key readable while migrating, list that key file in {PREVIOUS_KEY_FILES_ENV} instead"
                )));
            }
            if provider == "vault" {
                Arc::new(VaultTransitKekProvider::new(vault_config_from_lookup(
                    lookup,
                )?)?)
            } else {
                Arc::new(AwsKmsKekProvider::new(aws_kms_config_from_lookup(lookup)?)?)
            }
        }
        other => {
            return Err(EncryptionError::Config(format!(
                "{PROVIDER_ENV}: unknown provider '{other}' (expected local, vault or aws-kms)"
            )));
        }
    };
    check_provider(kms.as_ref())?;
    let previous: Vec<PathBuf> = list(lookup, PREVIOUS_KEY_FILES_ENV)
        .into_iter()
        .map(PathBuf::from)
        .collect();
    let provider: Arc<dyn KekProvider> = match previous.split_first() {
        None => kms,
        Some((first, rest)) => {
            let local = LocalKekProvider::from_files(first, rest)?;
            Arc::new(ChainedKekProvider::new(kms, vec![Arc::new(local)]))
        }
    };
    Ok(Some(Arc::new(Keyring::new(provider))))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key_file(dir: &std::path::Path, name: &str, byte: u8) -> String {
        let path = dir.join(name);
        std::fs::write(&path, hex::encode([byte; 32])).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        path.display().to_string()
    }

    #[test]
    fn values_build_the_keyring() {
        let dir = tempfile::tempdir().unwrap();
        assert!(keyring_from_values(None, None).unwrap().is_none());
        assert!(keyring_from_values(Some("  "), Some("")).unwrap().is_none());
        let active = key_file(dir.path(), "a.key", 1);
        let old = key_file(dir.path(), "b.key", 2);
        let keyring = keyring_from_values(Some(&active), Some(&format!(" {old} ,")))
            .unwrap()
            .unwrap();
        assert_eq!(keyring.key_ids().len(), 2);
        assert_eq!(keyring.active_key_id(), crate::local_key_id(&[1; 32]));
        assert!(keyring_from_values(None, Some(&old)).is_err());
        assert!(keyring_from_values(Some("/does/not/exist"), None).is_err());
    }

    fn lookup_from(pairs: Vec<(&str, String)>) -> impl Fn(&str) -> Option<String> {
        let map: std::collections::HashMap<String, String> =
            pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn provider_selection_and_its_errors() {
        let dir = tempfile::tempdir().unwrap();
        let active = key_file(dir.path(), "a.key", 1);
        let err_of = |pairs: Vec<(&str, String)>| {
            keyring_from_lookup(&lookup_from(pairs))
                .unwrap_err()
                .to_string()
        };
        // Unset provider: local key file or off.
        assert!(keyring_from_lookup(&lookup_from(vec![])).unwrap().is_none());
        let keyring = keyring_from_lookup(&lookup_from(vec![(KEY_FILE_ENV, active.clone())]))
            .unwrap()
            .unwrap();
        assert_eq!(keyring.provider_name(), "local");
        let keyring = keyring_from_lookup(&lookup_from(vec![
            (PROVIDER_ENV, " Local ".to_string()),
            (KEY_FILE_ENV, active.clone()),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(keyring.provider_name(), "local");
        assert!(err_of(vec![(PROVIDER_ENV, "local".to_string())]).contains(KEY_FILE_ENV));
        // Key manager settings without a provider do not silently leave
        // encryption off.
        let err = err_of(vec![(VAULT_ADDR_ENV, "https://vault:8200".to_string())]);
        assert!(err.contains("DASH_ENCRYPTION_PROVIDER=vault"), "{err}");
        let err = err_of(vec![(AWS_KMS_KEY_ID_ENV, "alias/dash".to_string())]);
        assert!(err.contains("DASH_ENCRYPTION_PROVIDER=aws-kms"), "{err}");
        let err = err_of(vec![(PROVIDER_ENV, "gcp".to_string())]);
        assert!(err.contains("unknown provider 'gcp'"), "{err}");
        // A key file next to a key manager is ambiguous.
        let err = err_of(vec![
            (PROVIDER_ENV, "vault".to_string()),
            (KEY_FILE_ENV, active.clone()),
        ]);
        assert!(err.contains(PREVIOUS_KEY_FILES_ENV), "{err}");
        // Missing required settings are named.
        let err = err_of(vec![(PROVIDER_ENV, "vault".to_string())]);
        assert!(err.contains(VAULT_ADDR_ENV), "{err}");
        let err = err_of(vec![(PROVIDER_ENV, "aws-kms".to_string())]);
        assert!(err.contains(AWS_KMS_KEY_ID_ENV), "{err}");
        let err = err_of(vec![
            (PROVIDER_ENV, "aws-kms".to_string()),
            (AWS_KMS_KEY_ID_ENV, "alias/dash".to_string()),
        ]);
        assert!(err.contains("no AWS region"), "{err}");
        let err = err_of(vec![
            (PROVIDER_ENV, "vault".to_string()),
            (VAULT_ADDR_ENV, "https://vault:8200".to_string()),
            (VAULT_TOKEN_FILE_ENV, "/t".to_string()),
            (VAULT_KEY_ENV, "dash".to_string()),
            (KMS_TIMEOUT_MS_ENV, "soon".to_string()),
        ]);
        assert!(err.contains(KMS_TIMEOUT_MS_ENV), "{err}");
    }

    #[test]
    fn vault_keyring_is_checked_at_startup_and_keeps_local_keys_readable() {
        use crate::kms::mock::{MockVault, write_token};
        let dir = tempfile::tempdir().unwrap();
        let vault = MockVault::start("s.token");
        let old_key = key_file(dir.path(), "old.key", 5);
        let local = keyring_from_values(Some(&old_key), None).unwrap().unwrap();
        let old_file = local.new_file_key().unwrap();
        let token = write_token(dir.path(), "token", "s.token");
        let vault_addr = vault.addr();
        let settings = |token_file: &std::path::Path| {
            vec![
                (PROVIDER_ENV, "vault".to_string()),
                (VAULT_ADDR_ENV, vault_addr.clone()),
                (VAULT_TOKEN_FILE_ENV, token_file.display().to_string()),
                (VAULT_KEY_ENV, "dash".to_string()),
                (PREVIOUS_KEY_FILES_ENV, old_key.clone()),
                (KMS_MAX_RETRIES_ENV, "0".to_string()),
            ]
        };
        let keyring = keyring_from_lookup(&lookup_from(settings(&token)))
            .unwrap()
            .unwrap();
        // The start-up check wrapped and unwrapped once.
        assert_eq!(vault.requests("encrypt"), 1);
        assert_eq!(vault.requests("decrypt"), 1);
        assert_eq!(keyring.provider_name(), "vault");
        assert_eq!(keyring.active_key_id(), "vault:transit/dash");
        assert_eq!(keyring.key_ids().len(), 2);
        let opened = keyring
            .open_file_key(old_file.header(), "old file")
            .unwrap();
        assert_eq!(opened.dek(), old_file.dek());
        let moved = keyring.rewrap(old_file.header(), "old file").unwrap();
        assert_eq!(moved.key_id, "vault:transit/dash");
        assert_eq!(
            keyring.open_file_key(&moved, "old file").unwrap().dek(),
            old_file.dek()
        );

        // A rejected token stops start-up with a message naming the file.
        vault.set_token("s.other");
        let err = keyring_from_lookup(&lookup_from(settings(&token)))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("start-up check of the vault provider"),
            "{err}"
        );
        assert!(err.contains("403"), "{err}");
        assert!(
            !err.contains("s.token") && !err.contains("s.other"),
            "{err}"
        );
        // An unreachable Vault too.
        drop(vault);
        let err = keyring_from_lookup(&lookup_from(settings(&token)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("start-up check"), "{err}");
    }

    #[test]
    fn aws_kms_keyring_from_settings() {
        use crate::kms::mock::{MOCK_ACCESS_KEY, MOCK_SECRET_KEY, MockKms};
        let kms = MockKms::start(&["key-1"]);
        let keyring = keyring_from_lookup(&lookup_from(vec![
            (PROVIDER_ENV, "aws-kms".to_string()),
            (AWS_KMS_KEY_ID_ENV, "key-1".to_string()),
            ("AWS_REGION", "us-east-1".to_string()),
            ("AWS_ENDPOINT_URL_KMS", kms.addr()),
            ("AWS_ACCESS_KEY_ID", MOCK_ACCESS_KEY.to_string()),
            ("AWS_SECRET_ACCESS_KEY", MOCK_SECRET_KEY.to_string()),
        ]))
        .unwrap()
        .unwrap();
        assert_eq!(keyring.provider_name(), "aws-kms");
        assert_eq!(keyring.active_key_id(), "aws-kms:key-1");
        let key = keyring.new_file_key().unwrap();
        assert_eq!(
            keyring.open_file_key(key.header(), "f").unwrap().dek(),
            key.dek()
        );
        assert_eq!(kms.bad_signatures(), 0);
    }

    #[test]
    fn errors_point_at_the_right_setting_for_each_provider() {
        let header = |key_id: &str| crate::FileHeader {
            key_id: key_id.to_string(),
            file_id: [0; 16],
            wrapped: vec![1],
        };
        let missing = |key_id: &str| {
            crate::open_file_key(None, &header(key_id), "f")
                .unwrap_err()
                .to_string()
        };
        assert!(missing("vault:transit/dash").contains("DASH_ENCRYPTION_PROVIDER=vault"));
        assert!(missing("aws-kms:alias/x").contains("DASH_ENCRYPTION_PROVIDER=aws-kms"));
        assert!(missing("local-0011223344556677").contains(KEY_FILE_ENV));
        let keyring = Keyring::local([1u8; 32], &[]).unwrap();
        let unknown = |key_id: &str| {
            keyring
                .open_file_key(&header(key_id), "f")
                .unwrap_err()
                .to_string()
        };
        assert!(unknown("vault:transit/old").contains(VAULT_PREVIOUS_KEYS_ENV));
        assert!(unknown("aws-kms:old").contains(AWS_KMS_PREVIOUS_KEY_IDS_ENV));
        assert!(unknown("local-0011223344556677").contains(PREVIOUS_KEY_FILES_ENV));
    }

    #[test]
    fn thread_override_wins_and_is_restored() {
        let keyring = Arc::new(Keyring::local([1u8; 32], &[]).unwrap());
        assert!(current().is_none());
        with_keyring(Some(keyring.clone()), || {
            assert_eq!(current().unwrap().active_key_id(), keyring.active_key_id());
            with_keyring(None, || assert!(current().is_none()));
            assert!(current().is_some());
        });
        assert!(current().is_none());
    }
}
