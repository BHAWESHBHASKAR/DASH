//! The keyring in effect for this process (and, in tests, this thread).

use std::cell::RefCell;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use crate::{EncryptionError, Keyring, LocalKekProvider};

/// Path of the active key-encryption key file. Encryption at rest is on
/// exactly when this is set.
pub const KEY_FILE_ENV: &str = "DASH_ENCRYPTION_KEY_FILE";
/// Comma-separated paths of retired key files that still decrypt.
pub const PREVIOUS_KEY_FILES_ENV: &str = "DASH_ENCRYPTION_PREVIOUS_KEY_FILES";

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

/// [`keyring_from_values`] from `DASH_ENCRYPTION_KEY_FILE` and
/// `DASH_ENCRYPTION_PREVIOUS_KEY_FILES`.
pub fn keyring_from_env() -> Result<Option<Arc<Keyring>>, EncryptionError> {
    let key_file = std::env::var("DASH_ENCRYPTION_KEY_FILE").ok();
    let previous = std::env::var("DASH_ENCRYPTION_PREVIOUS_KEY_FILES").ok();
    keyring_from_values(key_file.as_deref(), previous.as_deref())
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
