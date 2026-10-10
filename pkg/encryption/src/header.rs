//! The per-file key header: KEK id, file id and wrapped DEK.

use zeroize::Zeroizing;

use crate::{DEK_LEN, EncryptionError};

/// Length of the random file id every record and chunk authenticates.
pub const FILE_ID_LEN: usize = 16;

const HEADER_VERSION: u8 = 1;
const KEY_ID_MAX: usize = 128;
const WRAPPED_MAX: usize = 4096;

/// What a file stores about its key. Encoded as
/// `u8 version | u8 key id length | key id | file id | u16 LE wrapped length | wrapped`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    pub key_id: String,
    pub file_id: [u8; FILE_ID_LEN],
    pub wrapped: Vec<u8>,
}

fn valid_key_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= KEY_ID_MAX
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b':' | b'-' | b'/'))
}

impl FileHeader {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(4 + self.key_id.len() + FILE_ID_LEN + self.wrapped.len());
        out.push(HEADER_VERSION);
        out.push(self.key_id.len() as u8);
        out.extend_from_slice(self.key_id.as_bytes());
        out.extend_from_slice(&self.file_id);
        out.extend_from_slice(&(self.wrapped.len() as u16).to_le_bytes());
        out.extend_from_slice(&self.wrapped);
        out
    }

    /// Decodes a header from the start of `bytes`; returns it and the number
    /// of bytes it occupies.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize), EncryptionError> {
        let bad = |what: &str| EncryptionError::Format(format!("encryption header: {what}"));
        let version = *bytes.first().ok_or_else(|| bad("empty"))?;
        if version != HEADER_VERSION {
            return Err(bad(&format!(
                "version {version}, this build reads {HEADER_VERSION}"
            )));
        }
        let id_len = *bytes.get(1).ok_or_else(|| bad("truncated"))? as usize;
        let mut at = 2;
        let id_bytes = bytes.get(at..at + id_len).ok_or_else(|| bad("truncated"))?;
        let key_id = std::str::from_utf8(id_bytes)
            .ok()
            .filter(|id| valid_key_id(id))
            .ok_or_else(|| bad("invalid key id"))?
            .to_string();
        at += id_len;
        let mut file_id = [0u8; FILE_ID_LEN];
        file_id.copy_from_slice(
            bytes
                .get(at..at + FILE_ID_LEN)
                .ok_or_else(|| bad("truncated"))?,
        );
        at += FILE_ID_LEN;
        let len_bytes = bytes.get(at..at + 2).ok_or_else(|| bad("truncated"))?;
        let wrapped_len = u16::from_le_bytes([len_bytes[0], len_bytes[1]]) as usize;
        at += 2;
        if wrapped_len == 0 || wrapped_len > WRAPPED_MAX {
            return Err(bad("invalid wrapped key length"));
        }
        let wrapped = bytes
            .get(at..at + wrapped_len)
            .ok_or_else(|| bad("truncated"))?
            .to_vec();
        at += wrapped_len;
        Ok((
            Self {
                key_id,
                file_id,
                wrapped,
            },
            at,
        ))
    }
}

/// A file's header together with its unwrapped DEK (cleared on drop).
pub struct FileKey {
    header: FileHeader,
    dek: Zeroizing<[u8; DEK_LEN]>,
}

impl std::fmt::Debug for FileKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FileKey")
            .field("key_id", &self.header.key_id)
            .field("file_id", &hex::encode(self.header.file_id))
            .finish_non_exhaustive()
    }
}

impl FileKey {
    pub(crate) fn new(header: FileHeader, dek: Zeroizing<[u8; DEK_LEN]>) -> Self {
        Self { header, dek }
    }

    pub fn header(&self) -> &FileHeader {
        &self.header
    }

    pub fn key_id(&self) -> &str {
        &self.header.key_id
    }

    pub fn file_id(&self) -> &[u8; FILE_ID_LEN] {
        &self.header.file_id
    }

    pub(crate) fn dek(&self) -> &[u8; DEK_LEN] {
        &self.dek
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_round_trips_and_rejects_damage() {
        let header = FileHeader {
            key_id: "local-0123456789abcdef".to_string(),
            file_id: [4u8; FILE_ID_LEN],
            wrapped: vec![1, 2, 3, 4],
        };
        let mut bytes = header.encode();
        bytes.extend_from_slice(b"trailing");
        let (back, used) = FileHeader::decode(&bytes).unwrap();
        assert_eq!(back, header);
        assert_eq!(used, bytes.len() - b"trailing".len());
        for cut in 0..used {
            assert!(FileHeader::decode(&bytes[..cut]).is_err(), "cut at {cut}");
        }
        let mut bad = header.encode();
        bad[0] = 9;
        assert!(FileHeader::decode(&bad).is_err());
        let mut bad_id = header.clone();
        bad_id.key_id = "has space".to_string();
        assert!(FileHeader::decode(&bad_id.encode()).is_err());
    }
}
