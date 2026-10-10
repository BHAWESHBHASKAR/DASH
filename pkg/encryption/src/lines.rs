//! Format A: encrypted line files.
//!
//! ```text
//! ~DASHENC1 <base64 FileHeader>
//! ~E1 <base64 sealed record>          one per plaintext line
//! ```
//!
//! Base64 is the standard alphabet without padding, so an encrypted line
//! never contains a tab, a newline or a carriage return.

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;

use crate::{
    EncryptionError, FileHeader, FileKey, Keyring, LINE_LABEL, RecordCipher, open_file_key,
};

/// First characters of the header line of an encrypted line file.
pub const HEADER_LINE_PREFIX: &str = "~DASHENC1 ";
/// First characters of every encrypted record line.
pub const RECORD_LINE_PREFIX: &str = "~E1 ";

/// `true` when `line` looks like the header line of an encrypted line file
/// (it may still be torn or invalid).
pub fn is_header_line(line: &[u8]) -> bool {
    line.starts_with(HEADER_LINE_PREFIX.as_bytes())
}

/// `true` when `line` looks like an encrypted record line.
pub fn is_encrypted_line(line: &[u8]) -> bool {
    line.starts_with(RECORD_LINE_PREFIX.as_bytes())
}

pub fn render_header_line(header: &FileHeader) -> String {
    format!(
        "{HEADER_LINE_PREFIX}{}",
        STANDARD_NO_PAD.encode(header.encode())
    )
}

/// Parses a header line (without its newline).
pub fn parse_header_line(line: &str) -> Result<FileHeader, EncryptionError> {
    let line = line.strip_suffix('\r').unwrap_or(line);
    let encoded = line
        .strip_prefix(HEADER_LINE_PREFIX)
        .ok_or_else(|| EncryptionError::Format("not an encryption header line".to_string()))?;
    let bytes = STANDARD_NO_PAD
        .decode(encoded.trim_end())
        .map_err(|_| EncryptionError::Format("encryption header line is not base64".to_string()))?;
    let (header, used) = FileHeader::decode(&bytes)?;
    if used != bytes.len() {
        return Err(EncryptionError::Format(
            "encryption header line has trailing bytes".to_string(),
        ));
    }
    Ok(header)
}

/// Encrypts and decrypts the lines of one line file.
pub struct LineCipher {
    header_line: String,
    key_id: String,
    records: RecordCipher,
}

impl std::fmt::Debug for LineCipher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LineCipher")
            .field("key_id", &self.key_id)
            .finish_non_exhaustive()
    }
}

impl LineCipher {
    /// A cipher for a new file: fresh DEK wrapped by the active KEK.
    pub fn create(keyring: &Keyring) -> Result<Self, EncryptionError> {
        Ok(Self::from_key(&keyring.new_file_key()?))
    }

    pub fn from_key(key: &FileKey) -> Self {
        Self {
            header_line: render_header_line(key.header()),
            key_id: key.key_id().to_string(),
            records: RecordCipher::new(key, LINE_LABEL),
        }
    }

    /// The cipher of an existing file from its header line. Without a
    /// keyring this fails closed ([`EncryptionError::NotConfigured`]).
    pub fn from_header_line(
        keyring: Option<&Keyring>,
        line: &str,
        what: &str,
    ) -> Result<Self, EncryptionError> {
        let header = parse_header_line(line).map_err(|e| e.context(what))?;
        let key = open_file_key(keyring, &header, what)?;
        Ok(Self {
            header_line: line.strip_suffix('\r').unwrap_or(line).to_string(),
            key_id: key.key_id().to_string(),
            records: RecordCipher::new(&key, LINE_LABEL),
        })
    }

    /// The header line (without newline) to write as the file's first line.
    pub fn header_line(&self) -> &str {
        &self.header_line
    }

    /// Id of the KEK that wraps this file's DEK.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// The encrypted form of one plaintext line (which must not contain a
    /// newline).
    pub fn encrypt_line(&self, plaintext: &str) -> String {
        let sealed = self
            .records
            .seal(plaintext.as_bytes(), b"")
            .expect("AES-GCM encryption of a line cannot fail");
        let mut out =
            String::with_capacity(RECORD_LINE_PREFIX.len() + sealed.len().div_ceil(3) * 4 + 4);
        out.push_str(RECORD_LINE_PREFIX);
        STANDARD_NO_PAD.encode_string(&sealed, &mut out);
        out
    }

    /// Decrypts one physical line (without its newline). `Ok(None)` for this
    /// file's header line and for blank lines; an error for anything that is
    /// not an authentic record of this file.
    pub fn decrypt_line(&self, line: &[u8]) -> Result<Option<String>, EncryptionError> {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        if line.iter().all(u8::is_ascii_whitespace) {
            return Ok(None);
        }
        if line == self.header_line.as_bytes() {
            return Ok(None);
        }
        let Some(encoded) = line.strip_prefix(RECORD_LINE_PREFIX.as_bytes()) else {
            return Err(EncryptionError::Format(
                if is_header_line(line) {
                    "a second, different encryption header inside the file"
                } else {
                    "unencrypted line inside an encrypted file"
                }
                .to_string(),
            ));
        };
        let sealed = STANDARD_NO_PAD.decode(encoded).map_err(|_| {
            EncryptionError::Format("encrypted line is not valid base64".to_string())
        })?;
        let plain = self
            .records
            .open(&sealed, b"")
            .map_err(|_| EncryptionError::Authentication("encrypted line".to_string()))?;
        String::from_utf8(plain)
            .map(Some)
            .map_err(|_| EncryptionError::Format("decrypted line is not UTF-8".to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lines_round_trip_and_reject_damage() {
        let keyring = Keyring::local([8u8; 32], &[]).unwrap();
        let writer = LineCipher::create(&keyring).unwrap();
        let line = writer.encrypt_line("C2\tclaim-1\ttenant\ttext with ünïcode\tcrc=00000000");
        assert!(line.starts_with(RECORD_LINE_PREFIX));
        assert!(!line.contains(['\n', '\t', '\r']));
        assert!(!line.contains("claim-1"));

        let reader =
            LineCipher::from_header_line(Some(&keyring), writer.header_line(), "wal").unwrap();
        assert_eq!(
            reader.decrypt_line(line.as_bytes()).unwrap().unwrap(),
            "C2\tclaim-1\ttenant\ttext with ünïcode\tcrc=00000000"
        );
        assert_eq!(
            reader
                .decrypt_line(writer.header_line().as_bytes())
                .unwrap(),
            None
        );
        assert_eq!(reader.decrypt_line(b"   ").unwrap(), None);
        // Torn: every strict prefix fails.
        for cut in 0..line.len() {
            assert!(
                reader.decrypt_line(&line.as_bytes()[..cut]).is_err()
                    || line.as_bytes()[..cut].iter().all(u8::is_ascii_whitespace),
                "prefix of {cut} bytes decoded"
            );
        }
        // Flip any base64 character.
        let mut bytes = line.clone().into_bytes();
        let i = bytes.len() - 10;
        bytes[i] = if bytes[i] == b'A' { b'B' } else { b'A' };
        assert!(matches!(
            reader.decrypt_line(&bytes),
            Err(EncryptionError::Authentication(_))
        ));
        assert!(reader.decrypt_line(b"C2\tplain").is_err());
        let other = LineCipher::create(&keyring).unwrap();
        assert!(reader.decrypt_line(other.header_line().as_bytes()).is_err());
    }

    #[test]
    fn header_line_needs_a_key() {
        let keyring = Keyring::local([8u8; 32], &[]).unwrap();
        let writer = LineCipher::create(&keyring).unwrap();
        let err = LineCipher::from_header_line(None, writer.header_line(), "wal x").unwrap_err();
        assert!(matches!(err, EncryptionError::NotConfigured { .. }));
        let wrong = Keyring::local([9u8; 32], &[]).unwrap();
        let err =
            LineCipher::from_header_line(Some(&wrong), writer.header_line(), "wal x").unwrap_err();
        assert!(matches!(err, EncryptionError::UnknownKey { .. }));
        let torn = &writer.header_line()[..writer.header_line().len() - 5];
        assert!(LineCipher::from_header_line(Some(&keyring), torn, "wal x").is_err());
    }
}
