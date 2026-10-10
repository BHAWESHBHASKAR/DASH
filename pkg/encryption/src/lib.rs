//! Envelope encryption for the files DASH writes (ADR 0005,
//! `docs/adr/0005-encryption-at-rest.md`).
//!
//! * A **key-encryption key** (KEK) comes from a [`KekProvider`]. The
//!   shipped provider, [`LocalKekProvider`], reads 32-byte keys from files
//!   (`DASH_ENCRYPTION_KEY_FILE`, `DASH_ENCRYPTION_PREVIOUS_KEY_FILES`).
//! * Every file gets a fresh random **data-encryption key** (DEK), stored in
//!   the file header wrapped by the KEK ([`FileHeader`], [`FileKey`]).
//! * Two framings use the DEK:
//!   - [`LineCipher`] (format A): line files such as the WAL. Every
//!     plaintext line becomes one encrypted line, so torn-tail detection,
//!     per-line errors and byte offsets of lines keep working.
//!   - [`SealedWriter`] / [`SealedReader`] / [`SealedFile`] (format B):
//!     files written once, sealed in fixed-size chunks, readable
//!     sequentially or by plaintext offset.
//! * [`RecordCipher`] seals individual records (format A lines, redb
//!   values) under per-session subkeys, so a nonce never repeats under a key.
//!
//! Which keyring is in effect is process state ([`install`], [`current`]);
//! services install it from the environment at startup
//! ([`keyring_from_env`]), tests can override it per thread
//! ([`with_keyring`]).

mod error;
mod global;
mod header;
mod kek;
mod lines;
mod reader;
mod record;
mod stream;

pub use error::EncryptionError;
pub use global::{
    KEY_FILE_ENV, PREVIOUS_KEY_FILES_ENV, current, install, keyring_from_env, keyring_from_values,
    with_keyring,
};
pub use header::{FILE_ID_LEN, FileHeader, FileKey};
pub use kek::{
    DEK_LEN, KEK_LEN, KekProvider, LocalKekProvider, WrappedDek, generate_key, local_key_id,
    parse_key_material, read_key_file,
};
pub use lines::{
    HEADER_LINE_PREFIX, LineCipher, RECORD_LINE_PREFIX, is_encrypted_line, is_header_line,
    parse_header_line, render_header_line,
};
pub use reader::{
    FileFormat, RewrapOutcome, detect_format, open_reader, read_all, rewrap_file, sniff_file,
};
pub use record::{LINE_LABEL, RECORD_OVERHEAD, REDB_LABEL, RecordCipher};
pub use stream::{
    DEFAULT_CHUNK_SIZE, SEAL_MAGIC, SealedFile, SealedReader, SealedWriter, open_sealed_bytes,
    read_seal_header, rewrap_sealed_header, seal_bytes,
};

use std::sync::Arc;

pub use zeroize::Zeroizing;

/// The keys a process encrypts and decrypts with: one KEK provider.
pub struct Keyring {
    provider: Arc<dyn KekProvider>,
}

impl std::fmt::Debug for Keyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Keyring")
            .field("provider", &self.provider.name())
            .field("active_key_id", &self.provider.active_key_id())
            .finish()
    }
}

impl Keyring {
    pub fn new(provider: Arc<dyn KekProvider>) -> Self {
        Self { provider }
    }

    /// A keyring over local keys: `active` encrypts, `previous` only decrypt.
    pub fn local(
        active: [u8; KEK_LEN],
        previous: &[[u8; KEK_LEN]],
    ) -> Result<Self, EncryptionError> {
        Ok(Self::new(Arc::new(LocalKekProvider::from_keys(
            active, previous,
        )?)))
    }

    pub fn provider_name(&self) -> &'static str {
        self.provider.name()
    }

    /// Id of the KEK that wraps the DEKs of new files.
    pub fn active_key_id(&self) -> &str {
        self.provider.active_key_id()
    }

    /// Ids of every KEK this keyring can unwrap with (active first).
    pub fn key_ids(&self) -> Vec<String> {
        self.provider.key_ids()
    }

    /// A fresh DEK and file id for a new file, wrapped by the active KEK.
    pub fn new_file_key(&self) -> Result<FileKey, EncryptionError> {
        let mut dek = Zeroizing::new([0u8; DEK_LEN]);
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, dek.as_mut());
        let mut file_id = [0u8; FILE_ID_LEN];
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, &mut file_id);
        let wrapped = self.provider.wrap(&dek, &file_id)?;
        Ok(FileKey::new(
            FileHeader {
                key_id: wrapped.key_id,
                file_id,
                wrapped: wrapped.bytes,
            },
            dek,
        ))
    }

    /// Unwraps the DEK of an existing file. `what` names the file in errors.
    pub fn open_file_key(
        &self,
        header: &FileHeader,
        what: &str,
    ) -> Result<FileKey, EncryptionError> {
        if !self.key_ids().iter().any(|id| id == &header.key_id) {
            return Err(EncryptionError::UnknownKey {
                what: what.to_string(),
                key_id: header.key_id.clone(),
                configured: self.key_ids().join(", "),
            });
        }
        let dek = self
            .provider
            .unwrap(&header.key_id, &header.wrapped, &header.file_id)
            .map_err(|err| err.context(what))?;
        Ok(FileKey::new(header.clone(), dek))
    }

    /// The same DEK (and file id) wrapped by the active KEK. Data encrypted
    /// under the old header stays valid under the new one.
    pub fn rewrap(&self, header: &FileHeader, what: &str) -> Result<FileHeader, EncryptionError> {
        let key = self.open_file_key(header, what)?;
        let wrapped = self.provider.wrap(key.dek(), &header.file_id)?;
        Ok(FileHeader {
            key_id: wrapped.key_id,
            file_id: header.file_id,
            wrapped: wrapped.bytes,
        })
    }
}

/// Unwraps `header` with `keyring`, or explains why it cannot (no keyring
/// configured: fail closed).
pub fn open_file_key(
    keyring: Option<&Keyring>,
    header: &FileHeader,
    what: &str,
) -> Result<FileKey, EncryptionError> {
    match keyring {
        Some(keyring) => keyring.open_file_key(header, what),
        None => Err(EncryptionError::NotConfigured {
            what: what.to_string(),
            key_id: header.key_id.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_keys_round_trip_and_rewrap_keeps_the_dek() {
        let old = Keyring::local([1u8; 32], &[]).unwrap();
        let key = old.new_file_key().unwrap();
        let reopened = old.open_file_key(key.header(), "test file").unwrap();
        assert_eq!(reopened.dek(), key.dek());

        let rotated = Keyring::local([2u8; 32], &[[1u8; 32]]).unwrap();
        let rewrapped = rotated.rewrap(key.header(), "test file").unwrap();
        assert_eq!(rewrapped.key_id, rotated.active_key_id());
        assert_eq!(rewrapped.file_id, key.header().file_id);
        let opened = rotated.open_file_key(&rewrapped, "test file").unwrap();
        assert_eq!(opened.dek(), key.dek());

        let new_only = Keyring::local([2u8; 32], &[]).unwrap();
        let err = new_only
            .open_file_key(key.header(), "test file")
            .unwrap_err();
        assert!(matches!(err, EncryptionError::UnknownKey { .. }), "{err}");
        assert!(err.to_string().contains(old.active_key_id()), "{err}");
        assert!(new_only.open_file_key(&rewrapped, "test file").is_ok());
    }

    #[test]
    fn missing_keyring_fails_closed_with_the_key_id() {
        let keyring = Keyring::local([7u8; 32], &[]).unwrap();
        let key = keyring.new_file_key().unwrap();
        let err = open_file_key(None, key.header(), "wal /x").unwrap_err();
        let text = err.to_string();
        assert!(text.contains("wal /x"), "{text}");
        assert!(text.contains(keyring.active_key_id()), "{text}");
        assert!(text.contains(KEY_FILE_ENV), "{text}");
    }

    #[test]
    fn tampered_wrapped_dek_is_rejected() {
        let keyring = Keyring::local([7u8; 32], &[]).unwrap();
        let key = keyring.new_file_key().unwrap();
        let mut header = key.header().clone();
        header.wrapped[20] ^= 1;
        assert!(matches!(
            keyring.open_file_key(&header, "f"),
            Err(EncryptionError::Authentication(_))
        ));
        let mut header = key.header().clone();
        header.file_id[0] ^= 1;
        assert!(matches!(
            keyring.open_file_key(&header, "f"),
            Err(EncryptionError::Authentication(_))
        ));
    }
}
