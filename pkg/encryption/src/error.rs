/// Why an encryption operation failed. Messages name the file and the key
/// id involved, never key material.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EncryptionError {
    /// Invalid configuration (key file missing, wrong size, too permissive).
    #[error("encryption configuration error: {0}")]
    Config(String),
    /// The data is encrypted but this process has no key configured.
    #[error(
        "{what} is encrypted (key id {key_id}) but no encryption key is configured; set DASH_ENCRYPTION_KEY_FILE to the key file (see docs/operations/encryption.md)"
    )]
    NotConfigured { what: String, key_id: String },
    /// The data is encrypted under a KEK this process does not have.
    #[error(
        "{what} is encrypted with key id {key_id}, which is not configured (configured key ids: {configured}); add that key to DASH_ENCRYPTION_PREVIOUS_KEY_FILES"
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
}

impl EncryptionError {
    /// Prefixes the message with `what` (the file or record concerned).
    pub fn context(self, what: &str) -> Self {
        match self {
            Self::Config(m) => Self::Config(format!("{what}: {m}")),
            Self::Authentication(m) => Self::Authentication(format!("{what}: {m}")),
            Self::Format(m) => Self::Format(format!("{what}: {m}")),
            Self::Io(m) => Self::Io(format!("{what}: {m}")),
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
