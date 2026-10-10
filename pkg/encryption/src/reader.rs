//! Format detection, transparent plaintext readers and header rewrapping.

use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::{
    EncryptionError, FileHeader, HEADER_LINE_PREFIX, Keyring, LineCipher, SEAL_MAGIC, SealedReader,
    parse_header_line, read_seal_header, render_header_line, rewrap_sealed_header,
};

/// How a file is stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileFormat {
    /// Not encrypted (or empty).
    Plain,
    /// Format A: an encrypted line file.
    Lines(FileHeader),
    /// Format B: a sealed file.
    Sealed(FileHeader),
}

impl FileFormat {
    pub fn is_encrypted(&self) -> bool {
        !matches!(self, Self::Plain)
    }

    /// KEK id of an encrypted file.
    pub fn key_id(&self) -> Option<&str> {
        match self {
            Self::Plain => None,
            Self::Lines(h) | Self::Sealed(h) => Some(&h.key_id),
        }
    }
}

/// Classifies a file from its first bytes (at least the whole first line of
/// a line file, or the prefix and header of a sealed file). An encrypted
/// header that does not parse is an error.
pub fn detect_format(first: &[u8]) -> Result<FileFormat, EncryptionError> {
    if first.starts_with(SEAL_MAGIC) {
        let (header, _, _) = read_seal_header(&mut &first[..])?;
        return Ok(FileFormat::Sealed(header));
    }
    if first.starts_with(HEADER_LINE_PREFIX.as_bytes()) {
        let line = first
            .split(|b| *b == b'\n')
            .next()
            .expect("split yields at least one item");
        let line = std::str::from_utf8(line)
            .map_err(|_| EncryptionError::Format("encryption header is not UTF-8".to_string()))?;
        return Ok(FileFormat::Lines(parse_header_line(line)?));
    }
    Ok(FileFormat::Plain)
}

/// [`detect_format`] for the file at `path`. A missing file is an error.
pub fn sniff_file(path: &Path) -> Result<FileFormat, EncryptionError> {
    let mut file = File::open(path)?;
    let mut first = Vec::with_capacity(4096);
    (&mut file).take(16 * 1024).read_to_end(&mut first)?;
    detect_format(&first).map_err(|e| e.context(&path.display().to_string()))
}

/// Yields the plaintext of an encrypted line file, one `line\n` per line.
struct LineReader {
    inner: BufReader<File>,
    cipher: LineCipher,
    what: String,
    raw: Vec<u8>,
    out: Vec<u8>,
    pos: usize,
    line_no: usize,
}

impl Read for LineReader {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.pos == self.out.len() {
            self.raw.clear();
            self.out.clear();
            self.pos = 0;
            let n = self.inner.read_until(b'\n', &mut self.raw)?;
            if n == 0 {
                return Ok(0);
            }
            self.line_no += 1;
            let terminated = self.raw.last() == Some(&b'\n');
            let body = if terminated {
                &self.raw[..n - 1]
            } else {
                &self.raw[..]
            };
            let decoded = self.cipher.decrypt_line(body).map_err(|e| {
                io::Error::from(e.context(&format!("{} line {}", self.what, self.line_no)))
            })?;
            if let Some(text) = decoded {
                self.out.extend_from_slice(text.as_bytes());
                if terminated {
                    self.out.push(b'\n');
                }
            }
        }
        let n = buf.len().min(self.out.len() - self.pos);
        buf[..n].copy_from_slice(&self.out[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Opens `path` for reading its plaintext, whatever its format: plain files
/// as they are, line files and sealed files decrypted (failing closed when
/// no keyring is configured). Damage is reported as an
/// `io::ErrorKind::InvalidData` error while reading.
pub fn open_reader(
    path: &Path,
    keyring: Option<&Keyring>,
) -> Result<Box<dyn BufRead + Send>, EncryptionError> {
    let what = path.display().to_string();
    let format = sniff_file(path)?;
    let file = File::open(path)?;
    match format {
        FileFormat::Plain => Ok(Box::new(BufReader::new(file))),
        FileFormat::Sealed(_) => Ok(Box::new(BufReader::with_capacity(
            64 * 1024,
            SealedReader::new(BufReader::new(file), keyring, &what)?,
        ))),
        FileFormat::Lines(_) => {
            let mut inner = BufReader::new(file);
            let mut first = String::new();
            inner.read_line(&mut first)?;
            let header = first.trim_end_matches('\n');
            let cipher = LineCipher::from_header_line(keyring, header, &what)?;
            Ok(Box::new(BufReader::new(LineReader {
                inner,
                cipher,
                what,
                raw: Vec::new(),
                out: Vec::new(),
                pos: 0,
                line_no: 1,
            })))
        }
    }
}

/// The whole plaintext of `path` (see [`open_reader`]).
pub fn read_all(path: &Path, keyring: Option<&Keyring>) -> Result<Vec<u8>, EncryptionError> {
    let mut reader = open_reader(path, keyring)?;
    let mut out = Vec::new();
    reader.read_to_end(&mut out)?;
    Ok(out)
}

/// Result of [`rewrap_file`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RewrapOutcome {
    /// The file is not encrypted.
    Plain,
    /// The file already uses the active KEK.
    Current,
    /// The header now wraps the DEK with the active KEK (was `from`).
    Rewrapped { from: String },
}

fn replace_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".rewrap.tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    #[cfg(unix)]
    if let Some(dir) = path.parent() {
        let dir = if dir.as_os_str().is_empty() {
            Path::new(".")
        } else {
            dir
        };
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// Rewraps the DEK of an encrypted file with the active KEK (offline: the
/// file must not be open for writing). Only the header changes; records and
/// chunks are not re-encrypted. The file is replaced atomically.
pub fn rewrap_file(keyring: &Keyring, path: &Path) -> Result<RewrapOutcome, EncryptionError> {
    let what = path.display().to_string();
    let format = sniff_file(path)?;
    match format {
        FileFormat::Plain => Ok(RewrapOutcome::Plain),
        FileFormat::Sealed(header) => {
            let bytes = std::fs::read(path)?;
            match rewrap_sealed_header(keyring, &bytes, &what)? {
                None => Ok(RewrapOutcome::Current),
                Some(out) => {
                    replace_file(path, &out)?;
                    Ok(RewrapOutcome::Rewrapped {
                        from: header.key_id,
                    })
                }
            }
        }
        FileFormat::Lines(header) => {
            if header.key_id == keyring.active_key_id() {
                return Ok(RewrapOutcome::Current);
            }
            let bytes = std::fs::read(path)?;
            let rest = match bytes.iter().position(|b| *b == b'\n') {
                Some(pos) => &bytes[pos..],
                None => &[][..],
            };
            let rewrapped = keyring.rewrap(&header, &what)?;
            let mut out = render_header_line(&rewrapped).into_bytes();
            if rest.is_empty() {
                out.push(b'\n');
            } else {
                out.extend_from_slice(rest);
            }
            replace_file(path, &out)?;
            Ok(RewrapOutcome::Rewrapped {
                from: header.key_id,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SealedWriter, seal_bytes};

    #[test]
    fn readers_decode_every_format() {
        let dir = tempfile::tempdir().unwrap();
        let keyring = Keyring::local([3u8; 32], &[]).unwrap();

        let plain = dir.path().join("plain");
        std::fs::write(&plain, b"a\nb\n").unwrap();
        assert_eq!(sniff_file(&plain).unwrap(), FileFormat::Plain);
        assert_eq!(read_all(&plain, None).unwrap(), b"a\nb\n");

        let sealed = dir.path().join("sealed");
        std::fs::write(&sealed, seal_bytes(&keyring, b"sealed body").unwrap()).unwrap();
        assert!(matches!(
            sniff_file(&sealed).unwrap(),
            FileFormat::Sealed(_)
        ));
        assert_eq!(read_all(&sealed, Some(&keyring)).unwrap(), b"sealed body");
        assert!(matches!(
            read_all(&sealed, None),
            Err(EncryptionError::NotConfigured { .. })
        ));

        let lines = dir.path().join("lines");
        let cipher = LineCipher::create(&keyring).unwrap();
        let mut text = format!("{}\n", cipher.header_line());
        text.push_str(&format!("{}\n", cipher.encrypt_line("first")));
        text.push_str(&format!("{}\n", cipher.encrypt_line("second")));
        std::fs::write(&lines, &text).unwrap();
        assert!(matches!(sniff_file(&lines).unwrap(), FileFormat::Lines(_)));
        assert_eq!(
            read_all(&lines, Some(&keyring)).unwrap(),
            b"first\nsecond\n"
        );

        let mut damaged = text.clone().into_bytes();
        let at = damaged.len() - 5;
        damaged[at] = if damaged[at] == b'Q' { b'R' } else { b'Q' };
        std::fs::write(&lines, &damaged).unwrap();
        let err = read_all(&lines, Some(&keyring)).unwrap_err().to_string();
        assert!(err.contains("line 3"), "{err}");
    }

    #[test]
    fn rewrap_file_moves_both_formats_to_the_active_key() {
        let dir = tempfile::tempdir().unwrap();
        let old = Keyring::local([3u8; 32], &[]).unwrap();
        let new = Keyring::local([4u8; 32], &[[3u8; 32]]).unwrap();
        let new_only = Keyring::local([4u8; 32], &[]).unwrap();

        let sealed = dir.path().join("sealed");
        let mut writer = SealedWriter::new(Vec::new(), &old).unwrap();
        writer.write_all(b"payload").unwrap();
        std::fs::write(&sealed, writer.finish().unwrap()).unwrap();

        let lines = dir.path().join("lines");
        let cipher = LineCipher::create(&old).unwrap();
        std::fs::write(
            &lines,
            format!(
                "{}\n{}\n",
                cipher.header_line(),
                cipher.encrypt_line("record")
            ),
        )
        .unwrap();

        for path in [&sealed, &lines] {
            assert!(read_all(path, Some(&new_only)).is_err());
            assert_eq!(
                rewrap_file(&new, path).unwrap(),
                RewrapOutcome::Rewrapped {
                    from: old.active_key_id().to_string()
                }
            );
            assert_eq!(rewrap_file(&new, path).unwrap(), RewrapOutcome::Current);
            assert!(read_all(path, Some(&new_only)).is_ok());
        }
        assert_eq!(read_all(&lines, Some(&new_only)).unwrap(), b"record\n");
        let plain = dir.path().join("plain");
        std::fs::write(&plain, b"x").unwrap();
        assert_eq!(rewrap_file(&new, &plain).unwrap(), RewrapOutcome::Plain);
    }
}
