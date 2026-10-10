/// Why an encryption operation failed. Messages name the file and the key
/// id involved, never key material.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EncryptionError {
    /// Invalid configuration (key file missing, wrong size, too permissive).
    #[error("encryption configuration error: {0}")]
    Config(String),
    /// The data is encrypted but this process has no key configured.
    #[error(
        "{what} is encrypted (key id {key_id}) but no encryption key is configured; {} (see docs/operations/encryption.md)",
        enable_hint(.key_id)
    )]
    NotConfigured { what: String, key_id: String },
    /// The data is encrypted under a KEK this process does not have.
    #[error(
        "{what} is encrypted with key id {key_id}, which is not configured (configured key ids: {configured}); {}",
        previous_hint(.key_id)
    )]
    UnknownKey {
        what: String,
        key_id: String,
        configured: String,
    },
    /// Authentication failed: wrong key, or the bytes were changed.
    #[error("{0}: authentication failed (wrong key, or the data was modified or damaged)")]
    Authentication(String),
    /// The bytes are not in the expected layout.
    #[error("{0}")]
    Format(String),
    /// An I/O error while reading or writing encrypted data.
    #[error("{0}")]
    Io(String),
    /// A key manager (Vault, AWS KMS) call failed: unreachable, refused the
    /// credentials, or answered with an error. The message names the
    /// service, the operation and the key, never a token or key material.
    #[error("{0}")]
    Kms(String),
}

/// How to make `key_id` usable when nothing is configured.
fn enable_hint(key_id: &str) -> &'static str {
    if key_id.starts_with("vault:") {
        "set DASH_ENCRYPTION_PROVIDER=vault and the DASH_ENCRYPTION_VAULT_* settings for that key"
    } else if key_id.starts_with("aws-kms:") {
        "set DASH_ENCRYPTION_PROVIDER=aws-kms and DASH_ENCRYPTION_AWS_KMS_KEY_ID"
    } else {
        "set DASH_ENCRYPTION_KEY_FILE to the key file"
    }
}

/// Where a retired key that still has to decrypt is listed.
fn previous_hint(key_id: &str) -> &'static str {
    if key_id.starts_with("vault:") {
        "add that Transit key to DASH_ENCRYPTION_VAULT_PREVIOUS_KEYS (same mount)"
    } else if key_id.starts_with("aws-kms:") {
        "add that key to DASH_ENCRYPTION_AWS_KMS_PREVIOUS_KEY_IDS"
    } else {
        "add that key to DASH_ENCRYPTION_PREVIOUS_KEY_FILES"
    }
}

impl EncryptionError {
    /// Prefixes the message with `what` (the file or record concerned).
    pub fn context(self, what: &str) -> Self {
        match self {
            Self::Config(m) => Self::Config(format!("{what}: {m}")),
            Self::Authentication(m) => Self::Authentication(format!("{what}: {m}")),
            Self::Format(m) => Self::Format(format!("{what}: {m}")),
            Self::Io(m) => Self::Io(format!("{what}: {m}")),
            Self::Kms(m) => Self::Kms(format!("{what}: {m}")),
            other => other,
        }
    }
}

impl From<std::io::Error> for EncryptionError {
    fn from(err: std::io::Error) -> Self {
        // An error raised by this crate inside an io::Error keeps its text.
        Self::Io(err.to_string())
    }
}

impl From<EncryptionError> for std::io::Error {
    fn from(err: EncryptionError) -> Self {
        std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
    }
}
