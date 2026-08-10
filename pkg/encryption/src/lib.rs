use std::sync::Arc;

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{Aead, AeadCore, KeyInit, OsRng},
};
use base64::Engine;
use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    #[error("provider configuration error: {0}")]
    Config(String),
    #[error("encryption operation failed")]
    Encrypt,
    #[error("decryption operation failed")]
    Decrypt,
    #[error("ciphertext format error")]
    Format,
}

pub trait EncryptionProvider: Send + Sync {
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, EncryptionError>;
    fn decrypt(&self, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>, EncryptionError>;
    fn name(&self) -> &'static str;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Envelope {
    pub provider: String,
    pub key_id: Option<String>,
    pub nonce: String,
    pub ciphertext: String,
    pub aad_tag: Option<String>,
}

/// Load an encryption provider from environment variables.
///
/// Supported providers:
/// - `env`: `DASH_ENCRYPTION_MASTER_KEY` (hex or base64, 32 bytes)
/// - `none` / unset: pass-through no-op provider
pub fn provider_from_env() -> Result<Arc<dyn EncryptionProvider>, EncryptionError> {
    let provider = std::env::var("DASH_ENCRYPTION_PROVIDER")
        .or_else(|_| std::env::var("EME_ENCRYPTION_PROVIDER"))
        .unwrap_or_else(|_| "none".to_string());
    match provider.trim().to_ascii_lowercase().as_str() {
        "none" | "" => Ok(Arc::new(NoOpProvider)),
        "env" => {
            let raw = std::env::var("DASH_ENCRYPTION_MASTER_KEY")
                .or_else(|_| std::env::var("EME_ENCRYPTION_MASTER_KEY"))
                .map_err(|_| {
                    EncryptionError::Config("DASH_ENCRYPTION_MASTER_KEY is required".to_string())
                })?;
            let key = decode_master_key(&raw)?;
            Ok(Arc::new(EnvProvider::new(key)))
        }
        other => Err(EncryptionError::Config(format!(
            "unsupported encryption provider: {other}"
        ))),
    }
}

fn decode_master_key(raw: &str) -> Result<[u8; 32], EncryptionError> {
    let trimmed = raw.trim();
    let bytes = if trimmed.len() == 64 {
        hex::decode(trimmed)
            .map_err(|_| EncryptionError::Config("master key is not valid hex".to_string()))?
    } else if let Some(encoded) = trimmed.strip_prefix("base64:") {
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .map_err(|_| EncryptionError::Config("master key is not valid base64".to_string()))?
    } else {
        base64::engine::general_purpose::STANDARD
            .decode(trimmed)
            .map_err(|_| {
                EncryptionError::Config(
                    "master key must be 64-char hex, base64:, or base64".to_string(),
                )
            })?
    };
    let key: [u8; 32] = bytes
        .try_into()
        .map_err(|_| EncryptionError::Config("master key must be exactly 32 bytes".to_string()))?;
    Ok(key)
}

pub struct NoOpProvider;

impl EncryptionProvider for NoOpProvider {
    fn encrypt(&self, plaintext: &[u8], _aad: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        Ok(plaintext.to_vec())
    }

    fn decrypt(&self, ciphertext: &[u8], _aad: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        Ok(ciphertext.to_vec())
    }

    fn name(&self) -> &'static str {
        "none"
    }
}

pub struct EnvProvider {
    cipher: Aes256Gcm,
}

impl EnvProvider {
    pub fn new(master_key: [u8; 32]) -> Self {
        let key = Key::<Aes256Gcm>::from_slice(&master_key);
        Self {
            cipher: Aes256Gcm::new(key),
        }
    }
}

impl EncryptionProvider for EnvProvider {
    fn encrypt(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let payload = aes_gcm::aead::Payload {
            msg: plaintext,
            aad,
        };
        let ciphertext = self
            .cipher
            .encrypt(&nonce, payload)
            .map_err(|_| EncryptionError::Encrypt)?;
        let mut out = nonce.as_slice().to_vec();
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    fn decrypt(&self, ciphertext: &[u8], aad: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        if ciphertext.len() < 12 {
            return Err(EncryptionError::Format);
        }
        let (nonce_bytes, encrypted) = ciphertext.split_at(12);
        let nonce = Nonce::from_slice(nonce_bytes);
        let payload = aes_gcm::aead::Payload {
            msg: encrypted,
            aad,
        };
        self.cipher
            .decrypt(nonce, payload)
            .map_err(|_| EncryptionError::Decrypt)
    }

    fn name(&self) -> &'static str {
        "env"
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;

    use super::*;

    #[test]
    fn no_op_round_trip() {
        let provider = NoOpProvider;
        let plaintext = b"hello world";
        let encrypted = provider.encrypt(plaintext, b"tenant-a").unwrap();
        assert_eq!(encrypted, plaintext.to_vec());
        let decrypted = provider.decrypt(&encrypted, b"tenant-a").unwrap();
        assert_eq!(decrypted, plaintext.to_vec());
    }

    #[test]
    fn env_provider_round_trip() {
        let key = [0x42; 32];
        let provider = EnvProvider::new(key);
        let plaintext = b"sensitive tenant payload";
        let aad = b"tenant-42";
        let encrypted = provider.encrypt(plaintext, aad).unwrap();
        assert_ne!(encrypted, plaintext.to_vec());
        let decrypted = provider.decrypt(&encrypted, aad).unwrap();
        assert_eq!(decrypted, plaintext.to_vec());
    }

    #[test]
    fn env_provider_rejects_wrong_aad() {
        let key = [0x42; 32];
        let provider = EnvProvider::new(key);
        let plaintext = b"sensitive tenant payload";
        let encrypted = provider.encrypt(plaintext, b"tenant-42").unwrap();
        assert!(provider.decrypt(&encrypted, b"tenant-99").is_err());
    }

    #[test]
    fn decode_master_key_accepts_hex_and_base64() {
        let hex_key = "0".repeat(64);
        assert!(decode_master_key(&hex_key).is_ok());

        let b64 = base64::engine::general_purpose::STANDARD.encode([0u8; 32]);
        assert!(decode_master_key(&b64).is_ok());
        assert!(decode_master_key(&format!("base64:{b64}")).is_ok());
    }
}
