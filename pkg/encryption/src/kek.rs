//! Key-encryption keys: the provider interface and the local-file provider.

use std::path::Path;

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, KeyInit, Payload},
};
use rand::RngCore;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::EncryptionError;

/// Length of a data-encryption key.
pub const DEK_LEN: usize = 32;
/// Length of a local key-encryption key.
pub const KEK_LEN: usize = 32;

const WRAP_LABEL: &[u8] = b"dash-dek-wrap-v1";
const WRAP_NONCE_LEN: usize = 12;

/// A DEK wrapped by the KEK `key_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrappedDek {
    pub key_id: String,
    pub bytes: Vec<u8>,
}

/// Source of key-encryption keys.
///
/// `wrap` encrypts a DEK under the active KEK and `unwrap` decrypts one
/// under the named KEK. `context` (the file id) must be authenticated by
/// both, so a wrapped DEK cannot be moved to another file. A cloud KMS
/// provider maps `wrap` / `unwrap` to the KMS Encrypt / Decrypt calls with
/// `context` as encryption context or additional authenticated data (see
/// ADR 0005, section 6). Both are called once per file, never per record.
pub trait KekProvider: Send + Sync {
    /// Short provider name (`local`).
    fn name(&self) -> &'static str;
    /// Id of the KEK new files are wrapped with.
    fn active_key_id(&self) -> &str;
    /// Every KEK id this provider can unwrap with, active first.
    fn key_ids(&self) -> Vec<String>;
    fn wrap(&self, dek: &[u8; DEK_LEN], context: &[u8]) -> Result<WrappedDek, EncryptionError>;
    fn unwrap(
        &self,
        key_id: &str,
        wrapped: &[u8],
        context: &[u8],
    ) -> Result<Zeroizing<[u8; DEK_LEN]>, EncryptionError>;
}

/// The id of a local key: `local-` and the first 8 bytes (hex) of
/// `SHA-256("dash-kek-id-v1" || key)`. Stable for a key, distinct across keys,
/// and it reveals nothing usable about the key.
pub fn local_key_id(key: &[u8; KEK_LEN]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"dash-kek-id-v1");
    hasher.update(key);
    format!("local-{}", hex::encode(&hasher.finalize()[..8]))
}

/// A fresh random 32-byte key (for tests and tooling; operators generate
/// keys with `openssl rand -hex 32`).
pub fn generate_key() -> Zeroizing<[u8; KEK_LEN]> {
    let mut key = Zeroizing::new([0u8; KEK_LEN]);
    rand::rngs::OsRng.fill_bytes(key.as_mut());
    key
}

/// Parses key material: 64 hex characters (surrounding whitespace allowed)
/// or exactly 32 raw bytes. An all-zero key is refused.
pub fn parse_key_material(raw: &[u8]) -> Result<Zeroizing<[u8; KEK_LEN]>, EncryptionError> {
    let mut key = Zeroizing::new([0u8; KEK_LEN]);
    let text = std::str::from_utf8(raw).ok().map(str::trim);
    match text {
        Some(t) if t.len() == 2 * KEK_LEN && t.bytes().all(|b| b.is_ascii_hexdigit()) => {
            let decoded = Zeroizing::new(
                hex::decode(t)
                    .map_err(|_| EncryptionError::Config("key is not valid hex".to_string()))?,
            );
            key.copy_from_slice(&decoded);
        }
        _ if raw.len() == KEK_LEN => key.copy_from_slice(raw),
        _ => {
            return Err(EncryptionError::Config(
                "key must be 64 hex characters or exactly 32 raw bytes (generate one with `openssl rand -hex 32`)"
                    .to_string(),
            ));
        }
    }
    if key.iter().all(|b| *b == 0) {
        return Err(EncryptionError::Config("key is all zeros".to_string()));
    }
    Ok(key)
}

/// Reads a key file. On Unix the file must be a regular file that grants
/// nothing to other users and is not group-writable (group read is tolerated
/// because Kubernetes adds it to Secret volumes when `fsGroup` is set).
pub fn read_key_file(path: &Path) -> Result<Zeroizing<[u8; KEK_LEN]>, EncryptionError> {
    let shown = path.display();
    let meta = std::fs::metadata(path)
        .map_err(|e| EncryptionError::Config(format!("key file {shown}: {e}")))?;
    if !meta.is_file() {
        return Err(EncryptionError::Config(format!(
            "key file {shown} is not a regular file"
        )));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = meta.permissions().mode() & 0o777;
        if mode & 0o027 != 0 {
            return Err(EncryptionError::Config(format!(
                "key file {shown} has mode {mode:03o}; it must not be accessible to other users or writable by the group (chmod 600 or 400)"
            )));
        }
    }
    if meta.len() > 4096 {
        return Err(EncryptionError::Config(format!(
            "key file {shown} is too large to be a key"
        )));
    }
    let raw = Zeroizing::new(
        std::fs::read(path)
            .map_err(|e| EncryptionError::Config(format!("key file {shown}: {e}")))?,
    );
    parse_key_material(&raw).map_err(|e| e.context(&format!("key file {shown}")))
}

struct LocalKek {
    id: String,
    cipher: Aes256Gcm,
}

impl LocalKek {
    fn new(key: &[u8; KEK_LEN]) -> Self {
        Self {
            id: local_key_id(key),
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key)),
        }
    }
}

fn wrap_aad(key_id: &str, context: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(WRAP_LABEL.len() + 1 + key_id.len() + context.len());
    aad.extend_from_slice(WRAP_LABEL);
    aad.push(key_id.len() as u8);
    aad.extend_from_slice(key_id.as_bytes());
    aad.extend_from_slice(context);
    aad
}

/// KEKs held in process memory, loaded from key files. One is active (wraps
/// new DEKs); previous keys only unwrap, which keeps files written before a
/// rotation readable.
pub struct LocalKekProvider {
    keys: Vec<LocalKek>,
}

impl LocalKekProvider {
    pub fn from_keys(
        active: [u8; KEK_LEN],
        previous: &[[u8; KEK_LEN]],
    ) -> Result<Self, EncryptionError> {
        let active = Zeroizing::new(active);
        let mut keys = vec![LocalKek::new(&active)];
        for key in previous {
            let key = Zeroizing::new(*key);
            let kek = LocalKek::new(&key);
            if !keys.iter().any(|k| k.id == kek.id) {
                keys.push(kek);
            }
        }
        Ok(Self { keys })
    }

    pub fn from_files(
        active: &Path,
        previous: &[std::path::PathBuf],
    ) -> Result<Self, EncryptionError> {
        let active = read_key_file(active)?;
        let mut old = Vec::with_capacity(previous.len());
        for path in previous {
            old.push(read_key_file(path)?);
        }
        let old: Vec<[u8; KEK_LEN]> = old.iter().map(|k| **k).collect();
        let provider = Self::from_keys(*active, &old);
        // `old` holds copies; clear them.
        let mut old = old;
        zeroize::Zeroize::zeroize(&mut old);
        provider
    }
}

impl KekProvider for LocalKekProvider {
    fn name(&self) -> &'static str {
        "local"
    }

    fn active_key_id(&self) -> &str {
        &self.keys[0].id
    }

    fn key_ids(&self) -> Vec<String> {
        self.keys.iter().map(|k| k.id.clone()).collect()
    }

    fn wrap(&self, dek: &[u8; DEK_LEN], context: &[u8]) -> Result<WrappedDek, EncryptionError> {
        let kek = &self.keys[0];
        // A fresh random nonce per wrap, straight from the OS generator.
        let nonce: [u8; WRAP_NONCE_LEN] = rand::Rng::r#gen(&mut rand::rngs::OsRng);
        let aad = wrap_aad(&kek.id, context);
        let sealed = kek
            .cipher
            .encrypt(
                Nonce::from_slice(&nonce),
                Payload {
                    msg: dek,
                    aad: &aad,
                },
            )
            .map_err(|_| EncryptionError::Format("wrapping the data key failed".to_string()))?;
        let mut bytes = nonce.to_vec();
        bytes.extend_from_slice(&sealed);
        Ok(WrappedDek {
            key_id: kek.id.clone(),
            bytes,
        })
    }

    fn unwrap(
        &self,
        key_id: &str,
        wrapped: &[u8],
        context: &[u8],
    ) -> Result<Zeroizing<[u8; DEK_LEN]>, EncryptionError> {
        let kek = self.keys.iter().find(|k| k.id == key_id).ok_or_else(|| {
            EncryptionError::UnknownKey {
                what: "data key".to_string(),
                key_id: key_id.to_string(),
                configured: self.key_ids().join(", "),
            }
        })?;
        if wrapped.len() != WRAP_NONCE_LEN + DEK_LEN + 16 {
            return Err(EncryptionError::Format(
                "wrapped data key has the wrong length".to_string(),
            ));
        }
        let (nonce, sealed) = wrapped.split_at(WRAP_NONCE_LEN);
        let aad = wrap_aad(key_id, context);
        let plain = Zeroizing::new(
            kek.cipher
                .decrypt(
                    Nonce::from_slice(nonce),
                    Payload {
                        msg: sealed,
                        aad: &aad,
                    },
                )
                .map_err(|_| EncryptionError::Authentication("data key".to_string()))?,
        );
        let mut dek = Zeroizing::new([0u8; DEK_LEN]);
        dek.copy_from_slice(&plain);
        Ok(dek)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_material_accepts_hex_and_raw_and_refuses_the_rest() {
        let hex_key = "ab".repeat(32);
        assert_eq!(*parse_key_material(hex_key.as_bytes()).unwrap(), [0xab; 32]);
        assert_eq!(
            *parse_key_material(format!("  {hex_key}\n").as_bytes()).unwrap(),
            [0xab; 32]
        );
        assert_eq!(*parse_key_material(&[5u8; 32]).unwrap(), [5u8; 32]);
        assert!(parse_key_material(b"short").is_err());
        assert!(parse_key_material("0".repeat(64).as_bytes()).is_err());
        assert!(parse_key_material(&[0u8; 32]).is_err());
        assert!(parse_key_material("zz".repeat(32).as_bytes()).is_err());
    }

    #[test]
    fn key_ids_are_stable_and_distinct() {
        assert_eq!(local_key_id(&[1; 32]), local_key_id(&[1; 32]));
        assert_ne!(local_key_id(&[1; 32]), local_key_id(&[2; 32]));
        assert!(local_key_id(&[1; 32]).starts_with("local-"));
        assert_eq!(local_key_id(&[1; 32]).len(), "local-".len() + 16);
    }

    #[cfg(unix)]
    #[test]
    fn key_file_permissions_are_checked() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dash.key");
        std::fs::write(&path, "cd".repeat(32)).unwrap();
        for (mode, ok) in [
            (0o600, true),
            (0o400, true),
            (0o440, true),
            (0o640, true),
            (0o644, false),
            (0o660, false),
            (0o604, false),
            (0o666, false),
        ] {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
            let result = read_key_file(&path);
            assert_eq!(result.is_ok(), ok, "mode {mode:o}: {result:?}");
            if let Err(err) = result {
                assert!(err.to_string().contains("chmod 600"), "{err}");
            }
        }
        assert!(read_key_file(&dir.path().join("missing")).is_err());
        assert!(read_key_file(dir.path()).is_err());
    }

    #[test]
    fn wrap_unwrap_round_trip_binds_the_context() {
        let provider = LocalKekProvider::from_keys([9u8; 32], &[]).unwrap();
        let dek = [3u8; 32];
        let wrapped = provider.wrap(&dek, b"file-1").unwrap();
        assert_eq!(wrapped.key_id, provider.active_key_id());
        let back = provider
            .unwrap(&wrapped.key_id, &wrapped.bytes, b"file-1")
            .unwrap();
        assert_eq!(*back, dek);
        assert!(
            provider
                .unwrap(&wrapped.key_id, &wrapped.bytes, b"file-2")
                .is_err()
        );
        assert!(
            provider
                .unwrap("local-0000000000000000", &wrapped.bytes, b"file-1")
                .is_err()
        );
    }
}
