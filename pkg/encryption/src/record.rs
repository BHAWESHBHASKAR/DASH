//! Record sealing under per-session subkeys.
//!
//! A sealed record is `salt[16] | counter[8, BE] | ciphertext | tag[16]`.
//! The AES-256-GCM key is `HMAC-SHA256(DEK, "dash-record-subkey-v1" | salt)`
//! and the nonce is `0u32 | counter`. Every [`RecordCipher`] (one per open
//! file per process) draws a fresh random salt and counts from zero, and the
//! counter only ever increases, so a (subkey, nonce) pair is never used for
//! two different records: reusing one would need two sessions to draw the
//! same 128-bit salt.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{AeadInPlace, KeyInit},
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use zeroize::Zeroizing;

use crate::{DEK_LEN, EncryptionError, FILE_ID_LEN, FileKey};

const SALT_LEN: usize = 16;
const COUNTER_LEN: usize = 8;
const TAG_LEN: usize = 16;
const SUBKEY_LABEL: &[u8] = b"dash-record-subkey-v1";
/// Subkeys of other sessions kept for reading (cleared when exceeded).
const SUBKEY_CACHE_MAX: usize = 256;

/// Bytes a sealed record adds to its plaintext.
pub const RECORD_OVERHEAD: usize = SALT_LEN + COUNTER_LEN + TAG_LEN;
/// AAD label of format A lines.
pub const LINE_LABEL: &[u8] = b"dash-line-v1";
/// AAD label of redb values.
pub const REDB_LABEL: &[u8] = b"dash-redb-v1";

fn subkey(dek: &[u8; DEK_LEN], salt: &[u8; SALT_LEN]) -> Aes256Gcm {
    let mut mac =
        <Hmac<Sha256> as Mac>::new_from_slice(dek).expect("HMAC accepts keys of any length");
    mac.update(SUBKEY_LABEL);
    mac.update(salt);
    let key = Zeroizing::new(<[u8; 32]>::from(mac.finalize().into_bytes()));
    Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key.as_ref()))
}

fn nonce_for(counter: u64) -> [u8; 12] {
    let mut nonce = [0u8; 12];
    nonce[4..].copy_from_slice(&counter.to_be_bytes());
    nonce
}

/// Seals and opens records of one file (see the module docs).
pub struct RecordCipher {
    dek: Zeroizing<[u8; DEK_LEN]>,
    file_id: [u8; FILE_ID_LEN],
    label: &'static [u8],
    salt: [u8; SALT_LEN],
    session: Aes256Gcm,
    counter: AtomicU64,
    readers: Mutex<HashMap<[u8; SALT_LEN], Aes256Gcm>>,
}

impl RecordCipher {
    /// A cipher for the records of the file `key`, with AAD label `label`
    /// ([`LINE_LABEL`], [`REDB_LABEL`]).
    pub fn new(key: &FileKey, label: &'static [u8]) -> Self {
        let dek = Zeroizing::new(*key.dek());
        // A fresh random salt per writing session, straight from the OS
        // generator; the session subkey is derived from it.
        let salt: [u8; SALT_LEN] = rand::Rng::r#gen(&mut rand::rngs::OsRng);
        let session = subkey(&dek, &salt);
        Self {
            dek,
            file_id: *key.file_id(),
            label,
            salt,
            session,
            counter: AtomicU64::new(0),
            readers: Mutex::new(HashMap::new()),
        }
    }

    fn aad(&self, extra: &[u8]) -> Vec<u8> {
        let mut aad = Vec::with_capacity(self.label.len() + FILE_ID_LEN + extra.len());
        aad.extend_from_slice(self.label);
        aad.extend_from_slice(&self.file_id);
        aad.extend_from_slice(extra);
        aad
    }

    /// Seals `plaintext`; `extra_aad` is authenticated but not stored.
    pub fn seal(&self, plaintext: &[u8], extra_aad: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        let counter = self.counter.fetch_add(1, Ordering::Relaxed);
        if counter == u64::MAX {
            return Err(EncryptionError::Format(
                "record counter exhausted; reopen the file".to_string(),
            ));
        }
        let mut out = Vec::with_capacity(plaintext.len() + RECORD_OVERHEAD);
        out.extend_from_slice(&self.salt);
        out.extend_from_slice(&counter.to_be_bytes());
        out.extend_from_slice(plaintext);
        let aad = self.aad(extra_aad);
        let tag = self
            .session
            .encrypt_in_place_detached(
                Nonce::from_slice(&nonce_for(counter)),
                &aad,
                &mut out[SALT_LEN + COUNTER_LEN..],
            )
            .map_err(|_| EncryptionError::Format("record encryption failed".to_string()))?;
        out.extend_from_slice(&tag);
        Ok(out)
    }

    /// Opens a record sealed by any session of this file.
    pub fn open(&self, sealed: &[u8], extra_aad: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.open_owned(sealed.to_vec(), extra_aad)
    }

    /// [`RecordCipher::open`] decrypting inside `sealed` (no copy).
    pub fn open_owned(
        &self,
        mut sealed: Vec<u8>,
        extra_aad: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        if sealed.len() < RECORD_OVERHEAD {
            return Err(EncryptionError::Format(
                "encrypted record is too short".to_string(),
            ));
        }
        // The writer's session salt, stored at the start of the record.
        let salt: [u8; SALT_LEN] = sealed[..SALT_LEN]
            .try_into()
            .expect("length checked against RECORD_OVERHEAD");
        let mut counter = [0u8; COUNTER_LEN];
        counter.copy_from_slice(&sealed[SALT_LEN..SALT_LEN + COUNTER_LEN]);
        let counter = u64::from_be_bytes(counter);
        let body_end = sealed.len() - TAG_LEN;
        let tag = *aes_gcm::Tag::from_slice(&sealed[body_end..]);
        let aad = self.aad(extra_aad);
        let cipher = if salt == self.salt {
            self.session.clone()
        } else {
            let mut readers = self.readers.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(cipher) = readers.get(&salt) {
                cipher.clone()
            } else {
                if readers.len() >= SUBKEY_CACHE_MAX {
                    readers.clear();
                }
                let cipher = subkey(&self.dek, &salt);
                readers.insert(salt, cipher.clone());
                cipher
            }
        };
        cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(&nonce_for(counter)),
                &aad,
                &mut sealed[SALT_LEN + COUNTER_LEN..body_end],
                &tag,
            )
            .map_err(|_| EncryptionError::Authentication("encrypted record".to_string()))?;
        sealed.truncate(body_end);
        sealed.drain(..SALT_LEN + COUNTER_LEN);
        Ok(sealed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Keyring;

    #[test]
    fn records_round_trip_across_sessions_and_detect_tampering() {
        let keyring = Keyring::local([5u8; 32], &[]).unwrap();
        let key = keyring.new_file_key().unwrap();
        let writer = RecordCipher::new(&key, LINE_LABEL);
        let a = writer.seal(b"first record", b"").unwrap();
        let b = writer.seal(b"first record", b"").unwrap();
        assert_ne!(a, b, "same plaintext, different counter");
        assert_eq!(a.len(), b"first record".len() + RECORD_OVERHEAD);

        let reader = RecordCipher::new(&key, LINE_LABEL);
        assert_eq!(reader.open(&a, b"").unwrap(), b"first record");
        assert_eq!(writer.open(&b, b"").unwrap(), b"first record");

        for i in 0..a.len() {
            let mut bad = a.clone();
            bad[i] ^= 0x01;
            assert!(reader.open(&bad, b"").is_err(), "flip at {i}");
        }
        assert!(reader.open(&a[..a.len() - 1], b"").is_err());
        assert!(reader.open(&a, b"other aad").is_err());
        let other_label = RecordCipher::new(&key, REDB_LABEL);
        assert!(other_label.open(&a, b"").is_err());
        let other_file = RecordCipher::new(&keyring.new_file_key().unwrap(), LINE_LABEL);
        assert!(other_file.open(&a, b"").is_err());
    }

    #[test]
    fn sessions_use_distinct_salts() {
        let keyring = Keyring::local([5u8; 32], &[]).unwrap();
        let key = keyring.new_file_key().unwrap();
        let a = RecordCipher::new(&key, LINE_LABEL).seal(b"x", b"").unwrap();
        let b = RecordCipher::new(&key, LINE_LABEL).seal(b"x", b"").unwrap();
        assert_ne!(a[..SALT_LEN], b[..SALT_LEN]);
        assert_eq!(a[SALT_LEN..SALT_LEN + 8], b[SALT_LEN..SALT_LEN + 8]);
    }
}
