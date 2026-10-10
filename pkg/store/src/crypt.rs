//! Encryption at rest for the store's files (ADR 0005).
//!
//! Line files (WAL, snapshot, closed generation, quarantine, follower part
//! file) use format A ([`LineCodec`]); files written once (vector index,
//! replication export) use format B (sealed). Which keyring applies is
//! captured when a file handle is created (`encryption::current()`).

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use encryption::{EncryptionError, Keyring, LineCipher};

use crate::StoreError;


/// The keyring a store handle captured (`None`: encryption off).
pub(crate) type KeyringRef = Option<Arc<Keyring>>;

pub(crate) fn current_keyring() -> KeyringRef {
    encryption::current()
}

pub(crate) fn enc_err(err: EncryptionError) -> StoreError {
    match err {
        EncryptionError::Authentication(m) => StoreError::Parse(format!(
            "{m}: authentication failed (wrong key, or the data was modified or damaged)"
        )),
        EncryptionError::Format(m) => StoreError::Parse(m),
        other => StoreError::Io(other.to_string()),
    }
}

/// How the lines of one line file are stored.
#[derive(Clone, Debug, Default)]
pub(crate) enum LineCodec {
    #[default]
    Plain,
    Encrypted(Arc<LineCipher>),
}

impl LineCodec {
    /// The codec for a new file: encrypted with a fresh DEK when a keyring is
    /// configured.
    pub(crate) fn create(keyring: Option<&Arc<Keyring>>) -> Result<Self, StoreError> {
        match keyring {
            Some(keyring) => Ok(Self::Encrypted(Arc::new(
                LineCipher::create(keyring).map_err(enc_err)?,
            ))),
            None => Ok(Self::Plain),
        }
    }

    pub(crate) fn is_encrypted(&self) -> bool {
        matches!(self, Self::Encrypted(_))
    }

    pub(crate) fn key_id(&self) -> Option<&str> {
        match self {
            Self::Plain => None,
            Self::Encrypted(c) => Some(c.key_id()),
        }
    }

    /// The header line to write first in a new file, if any.
    pub(crate) fn header_line(&self) -> Option<&str> {
        match self {
            Self::Plain => None,
            Self::Encrypted(c) => Some(c.header_line()),
        }
    }

    /// Text of one physical line (without its newline). An empty string for
    /// a blank line or the encryption header line; `Err(reason)` for a line
    /// that is not valid in this file (invalid UTF-8, or a line that fails
    /// decryption).
    pub(crate) fn decode(&self, body: &[u8]) -> Result<String, String> {
        match self {
            Self::Plain => {
                let body = body.strip_suffix(b"\r").unwrap_or(body);
                std::str::from_utf8(body)
                    .map(str::to_string)
                    .map_err(|_| "invalid UTF-8".to_string())
            }
            Self::Encrypted(c) => match c.decrypt_line(body) {
                Ok(Some(text)) => Ok(text),
                Ok(None) => Ok(String::new()),
                Err(EncryptionError::Authentication(_)) => Err(
                    "authentication failed (wrong key, or the line was modified or damaged)"
                        .to_string(),
                ),
                Err(err) => Err(err.to_string()),
            },
        }
    }

    /// The stored form of a plaintext line (no newline).
    pub(crate) fn encode<'a>(&self, line: &'a str) -> std::borrow::Cow<'a, str> {
        match self {
            Self::Plain => std::borrow::Cow::Borrowed(line),
            Self::Encrypted(c) => std::borrow::Cow::Owned(c.encrypt_line(line)),
        }
    }

    /// Appends the stored form of `line` and a newline to `out`.
    pub(crate) fn push_line(&self, out: &mut String, line: &str) {
        out.push_str(&self.encode(line));
        out.push('\n');
    }
}

/// Result of looking at the first line of an existing line file.
pub(crate) enum Detected {
    Plain,
    Encrypted(LineCodec),
    /// The file holds only an unterminated encryption header that does not
    /// parse: a crash while the file was being created.
    TornHeader,
}

fn first_line(reader: &mut impl BufRead) -> std::io::Result<(Vec<u8>, bool)> {
    let mut line = Vec::new();
    reader.take(64 * 1024).read_until(b'\n', &mut line)?;
    let terminated = line.last() == Some(&b'\n');
    if terminated {
        line.pop();
    }
    Ok((line, terminated))
}

fn detect_from_first_line(
    line: &[u8],
    terminated: bool,
    keyring: Option<&Arc<Keyring>>,
    what: &str,
) -> Result<Detected, StoreError> {
    if !encryption::is_header_line(line) {
        return Ok(Detected::Plain);
    }
    let text = std::str::from_utf8(line).map_err(|_| {
        StoreError::Parse(format!("{what}: encryption header line is not UTF-8"))
    })?;
    if !terminated && encryption::parse_header_line(text).is_err() {
        return Ok(Detected::TornHeader);
    }
    let cipher =
        LineCipher::from_header_line(keyring.map(Arc::as_ref), text, what).map_err(enc_err)?;
    Ok(Detected::Encrypted(LineCodec::Encrypted(Arc::new(cipher))))
}

/// Looks at the first line of the line file `path` (which must exist).
pub(crate) fn detect_line_file(
    path: &Path,
    keyring: Option<&Arc<Keyring>>,
) -> Result<Detected, StoreError> {
    let mut reader = BufReader::new(File::open(path)?);
    let (line, terminated) = first_line(&mut reader)?;
    detect_from_first_line(&line, terminated, keyring, &path.display().to_string())
}

/// [`detect_line_file`] on an open handle; the handle is left at offset 0.
pub(crate) fn detect_open_line_file(
    file: &mut File,
    keyring: Option<&Arc<Keyring>>,
    what: &str,
) -> Result<Detected, StoreError> {
    file.seek(SeekFrom::Start(0))?;
    let (line, terminated) = first_line(&mut BufReader::new(&mut *file))?;
    file.seek(SeekFrom::Start(0))?;
    detect_from_first_line(&line, terminated, keyring, what)
}

/// The codec of an existing line file, failing on a torn header (only the
/// live WAL may have one, and it handles that case itself).
pub(crate) fn line_codec_for(
    path: &Path,
    keyring: Option<&Arc<Keyring>>,
) -> Result<LineCodec, StoreError> {
    match detect_line_file(path, keyring)? {
        Detected::Plain => Ok(LineCodec::Plain),
        Detected::Encrypted(codec) => Ok(codec),
        Detected::TornHeader => Err(StoreError::Parse(format!(
            "{}: the encryption header is incomplete",
            path.display()
        ))),
    }
}

/// Writes `lines` as a complete line file through `file` (header line
/// first when encrypted). Does not sync.
pub(crate) fn write_line_file<'a>(
    file: &mut File,
    codec: &LineCodec,
    lines: impl IntoIterator<Item = &'a str>,
) -> Result<(), StoreError> {
    let mut out = std::io::BufWriter::with_capacity(1 << 20, file);
    if let Some(header) = codec.header_line() {
        out.write_all(header.as_bytes())?;
        out.write_all(b"\n")?;
    }
    for line in lines {
        out.write_all(codec.encode(line).as_bytes())?;
        out.write_all(b"\n")?;
    }
    out.flush()?;
    Ok(())
}

/// Opens `path` for appending lines with `codec`, writing the header line
/// first when the file is empty. Returns the handle.
pub(crate) fn open_line_file_for_append(path: &Path, codec: &LineCodec) -> Result<File, StoreError> {
    let mut file = OpenOptions::new().create(true).append(true).open(path)?;
    if let Some(header) = codec.header_line()
        && file.metadata()?.len() == 0
    {
        file.write_all(header.as_bytes())?;
        file.write_all(b"\n")?;
    }
    Ok(file)
}

/// A keyring captured by a value that must stay `Clone + PartialEq + Debug`
/// (equality compares the keyring identity).
#[derive(Clone, Default)]
pub(crate) struct CapturedKeyring(pub(crate) KeyringRef);

impl CapturedKeyring {
    pub(crate) fn current() -> Self {
        Self(current_keyring())
    }

    pub(crate) fn get(&self) -> Option<&Keyring> {
        self.0.as_deref()
    }
}

impl PartialEq for CapturedKeyring {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (None, None) => true,
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }
}

impl Eq for CapturedKeyring {}

impl std::fmt::Debug for CapturedKeyring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            None => f.write_str("CapturedKeyring(None)"),
            Some(k) => write!(f, "CapturedKeyring({})", k.active_key_id()),
        }
    }
}

/// Plaintext reader for a file in any format (plain, line or sealed).
pub(crate) fn open_plain(
    path: &Path,
    keyring: Option<&Keyring>,
) -> Result<Box<dyn BufRead + Send>, StoreError> {
    encryption::open_reader(path, keyring).map_err(enc_err)
}

/// Writes plaintext either straight through or sealed (format B).
pub(crate) enum SealSink<W: Write> {
    Plain(W),
    Sealed(encryption::SealedWriter<W>),
}

impl<W: Write> SealSink<W> {
    pub(crate) fn new(inner: W, keyring: Option<&Keyring>) -> Result<Self, StoreError> {
        match keyring {
            Some(keyring) => Ok(Self::Sealed(
                encryption::SealedWriter::new(inner, keyring).map_err(enc_err)?,
            )),
            None => Ok(Self::Plain(inner)),
        }
    }

    /// Seals the last chunk (when sealing) and returns the inner writer.
    pub(crate) fn finish(self) -> std::io::Result<W> {
        match self {
            Self::Plain(inner) => Ok(inner),
            Self::Sealed(writer) => writer.finish(),
        }
    }
}

impl<W: Write> Write for SealSink<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(w) => w.write(buf),
            Self::Sealed(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(w) => w.flush(),
            Self::Sealed(w) => w.flush(),
        }
    }
}

fn is_sealed(file: &mut File) -> std::io::Result<bool> {
    let mut magic = [0u8; 8];
    let mut got = 0;
    while got < magic.len() {
        let n = file.read(&mut magic[got..])?;
        if n == 0 {
            break;
        }
        got += n;
    }
    Ok(got == magic.len() && &magic == encryption::SEAL_MAGIC)
}

/// Random access to the plaintext of a plain or sealed file.
pub(crate) enum PlainFile {
    Plain { file: File, len: u64 },
    Sealed(Box<encryption::SealedFile>),
}

impl PlainFile {
    pub(crate) fn open(path: &Path, keyring: Option<&Keyring>) -> Result<Self, StoreError> {
        let mut file = File::open(path)?;
        if is_sealed(&mut file)? {
            drop(file);
            return Ok(Self::Sealed(Box::new(
                encryption::SealedFile::open(path, keyring).map_err(enc_err)?,
            )));
        }
        let len = file.metadata()?.len();
        Ok(Self::Plain { file, len })
    }

    /// Plaintext length of `path` without reading its data.
    pub(crate) fn plain_len(path: &Path) -> Result<u64, StoreError> {
        let mut file = File::open(path)?;
        if is_sealed(&mut file)? {
            return encryption::sealed_plain_len(path).map_err(enc_err);
        }
        Ok(file.metadata()?.len())
    }

    pub(crate) fn len(&self) -> u64 {
        match self {
            Self::Plain { len, .. } => *len,
            Self::Sealed(f) => f.plain_len(),
        }
    }

    /// Reads up to `out.len()` bytes at `offset`; fewer only at the end.
    pub(crate) fn read_at(&mut self, offset: u64, out: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain { file, len } => {
                if offset >= *len {
                    return Ok(0);
                }
                file.seek(SeekFrom::Start(offset))?;
                let want = ((*len - offset) as usize).min(out.len());
                file.read_exact(&mut out[..want])?;
                Ok(want)
            }
            Self::Sealed(f) => f.read_at(offset, out),
        }
    }
}
