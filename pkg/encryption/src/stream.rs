//! Format B: sealed files, written once.
//!
//! ```text
//! "DASHSEAL" | u8 version | u32 LE chunk size | u16 LE header length | FileHeader
//! chunk 0 | chunk 1 | ... | chunk n           chunk = ciphertext | tag[16]
//! ```
//!
//! Chunks are sealed with AES-256-GCM under the file's DEK, nonce
//! `chunk index (u64 BE) | last flag (u32 BE)`, AAD `"dash-stream-v1" | file id`.
//! Every chunk but the last holds exactly `chunk size` plaintext bytes; an
//! empty file is a single empty last chunk. Each file has a fresh DEK and is
//! written front to back once, so a nonce never repeats under a key.
//! Truncating, reordering, dropping or appending chunks fails authentication.

use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

use aes_gcm::{
    Aes256Gcm, Key, Nonce,
    aead::{AeadInPlace, KeyInit},
};

use crate::{EncryptionError, FILE_ID_LEN, FileHeader, FileKey, Keyring, open_file_key};

/// Magic bytes of a sealed file.
pub const SEAL_MAGIC: &[u8; 8] = b"DASHSEAL";
/// Plaintext bytes per chunk of files written by this build.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

const SEAL_VERSION: u8 = 1;
const PREFIX_LEN: usize = 8 + 1 + 4 + 2;
const TAG_LEN: usize = 16;
const STREAM_LABEL: &[u8] = b"dash-stream-v1";
const CHUNK_SIZE_MAX: usize = 16 * 1024 * 1024;
const HEADER_MAX: usize = 8 * 1024;

struct ChunkCipher {
    cipher: Aes256Gcm,
    aad: Vec<u8>,
}

impl ChunkCipher {
    fn new(key: &FileKey) -> Self {
        let mut aad = STREAM_LABEL.to_vec();
        aad.extend_from_slice(key.file_id());
        Self {
            cipher: Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key.dek())),
            aad,
        }
    }

    fn nonce(index: u64, last: bool) -> [u8; 12] {
        let mut nonce = [0u8; 12];
        nonce[..8].copy_from_slice(&index.to_be_bytes());
        nonce[8..].copy_from_slice(&u32::from(last).to_be_bytes());
        nonce
    }

    fn seal(&self, index: u64, last: bool, plain: &[u8], out: &mut Vec<u8>) -> io::Result<()> {
        let start = out.len();
        out.extend_from_slice(plain);
        let tag = self
            .cipher
            .encrypt_in_place_detached(
                Nonce::from_slice(&Self::nonce(index, last)),
                &self.aad,
                &mut out[start..],
            )
            .map_err(|_| io::Error::other("chunk encryption failed"))?;
        out.extend_from_slice(&tag);
        Ok(())
    }

    fn open(&self, index: u64, last: bool, sealed: &[u8], what: &str) -> io::Result<Vec<u8>> {
        if sealed.len() < TAG_LEN {
            return Err(
                EncryptionError::Format(format!("{what}: sealed chunk is truncated")).into(),
            );
        }
        let (body, tag) = sealed.split_at(sealed.len() - TAG_LEN);
        let mut plain = body.to_vec();
        self.cipher
            .decrypt_in_place_detached(
                Nonce::from_slice(&Self::nonce(index, last)),
                &self.aad,
                &mut plain,
                aes_gcm::Tag::from_slice(tag),
            )
            .map_err(|_| EncryptionError::Authentication(format!("{what}, chunk {index}")))?;
        Ok(plain)
    }
}

fn prefix_bytes(header: &FileHeader, chunk_size: usize) -> Vec<u8> {
    let encoded = header.encode();
    let mut out = Vec::with_capacity(PREFIX_LEN + encoded.len());
    out.extend_from_slice(SEAL_MAGIC);
    out.push(SEAL_VERSION);
    out.extend_from_slice(&(chunk_size as u32).to_le_bytes());
    out.extend_from_slice(&(encoded.len() as u16).to_le_bytes());
    out.extend_from_slice(&encoded);
    out
}

/// Reads the prefix and header of a sealed file. Returns the header, the
/// chunk size and the offset of the first chunk.
pub fn read_seal_header(
    reader: &mut impl Read,
) -> Result<(FileHeader, usize, u64), EncryptionError> {
    let mut prefix = [0u8; PREFIX_LEN];
    reader
        .read_exact(&mut prefix)
        .map_err(|_| EncryptionError::Format("sealed file is truncated".to_string()))?;
    if &prefix[..8] != SEAL_MAGIC {
        return Err(EncryptionError::Format("not a sealed file".to_string()));
    }
    if prefix[8] != SEAL_VERSION {
        return Err(EncryptionError::Format(format!(
            "sealed file version {}, this build reads {SEAL_VERSION}",
            prefix[8]
        )));
    }
    let chunk_size = u32::from_le_bytes([prefix[9], prefix[10], prefix[11], prefix[12]]) as usize;
    if chunk_size == 0 || chunk_size > CHUNK_SIZE_MAX {
        return Err(EncryptionError::Format(
            "sealed file has an invalid chunk size".to_string(),
        ));
    }
    let header_len = u16::from_le_bytes([prefix[13], prefix[14]]) as usize;
    if header_len == 0 || header_len > HEADER_MAX {
        return Err(EncryptionError::Format(
            "sealed file has an invalid header length".to_string(),
        ));
    }
    let mut header = vec![0u8; header_len];
    reader
        .read_exact(&mut header)
        .map_err(|_| EncryptionError::Format("sealed file is truncated".to_string()))?;
    let (decoded, used) = FileHeader::decode(&header)?;
    if used != header_len {
        return Err(EncryptionError::Format(
            "sealed file header has trailing bytes".to_string(),
        ));
    }
    Ok((decoded, chunk_size, (PREFIX_LEN + header_len) as u64))
}

/// Streams plaintext into a sealed file. Call [`SealedWriter::finish`]; a
/// writer dropped without it leaves a file that fails to open (no last
/// chunk), which is what an interrupted write must look like.
pub struct SealedWriter<W: Write> {
    inner: Option<W>,
    cipher: ChunkCipher,
    chunk_size: usize,
    buf: Vec<u8>,
    out: Vec<u8>,
    index: u64,
    plain_len: u64,
}

impl<W: Write> SealedWriter<W> {
    /// Writes the header (fresh DEK wrapped by the active KEK) to `inner`.
    pub fn new(inner: W, keyring: &Keyring) -> Result<Self, EncryptionError> {
        Self::with_chunk_size(inner, keyring, DEFAULT_CHUNK_SIZE)
    }

    pub fn with_chunk_size(
        mut inner: W,
        keyring: &Keyring,
        chunk_size: usize,
    ) -> Result<Self, EncryptionError> {
        assert!(chunk_size > 0 && chunk_size <= CHUNK_SIZE_MAX);
        let key = keyring.new_file_key()?;
        inner.write_all(&prefix_bytes(key.header(), chunk_size))?;
        Ok(Self {
            inner: Some(inner),
            cipher: ChunkCipher::new(&key),
            chunk_size,
            buf: Vec::with_capacity(chunk_size + 1),
            out: Vec::with_capacity(chunk_size + TAG_LEN),
            index: 0,
            plain_len: 0,
        })
    }

    /// Plaintext bytes written so far.
    pub fn plain_len(&self) -> u64 {
        self.plain_len
    }

    fn emit(&mut self, last: bool) -> io::Result<()> {
        let take = self.buf.len().min(self.chunk_size);
        self.out.clear();
        self.cipher
            .seal(self.index, last, &self.buf[..take], &mut self.out)?;
        self.inner
            .as_mut()
            .expect("writer used after finish")
            .write_all(&self.out)?;
        self.buf.drain(..take);
        self.index += 1;
        Ok(())
    }

    /// Seals the last chunk and returns the inner writer (not flushed).
    pub fn finish(mut self) -> io::Result<W> {
        self.emit(true)?;
        Ok(self.inner.take().expect("finish called once"))
    }
}

impl<W: Write> Write for SealedWriter<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buf.extend_from_slice(data);
        self.plain_len += data.len() as u64;
        // A full chunk is sealed only once a byte follows it: until then it
        // may be the last one.
        while self.buf.len() > self.chunk_size {
            self.emit(false)?;
        }
        Ok(data.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.inner.as_mut() {
            Some(inner) => inner.flush(),
            None => Ok(()),
        }
    }
}

/// Reads a sealed file front to back, yielding plaintext. Errors (as
/// `io::ErrorKind::InvalidData`) on any damaged, missing or extra chunk.
pub struct SealedReader<R: Read> {
    inner: R,
    cipher: ChunkCipher,
    chunk_size: usize,
    what: String,
    key_id: String,
    raw: Vec<u8>,
    plain: Vec<u8>,
    pos: usize,
    index: u64,
    done: bool,
}

impl<R: Read> SealedReader<R> {
    /// Reads the header from `inner` and unwraps the DEK.
    pub fn new(
        mut inner: R,
        keyring: Option<&Keyring>,
        what: &str,
    ) -> Result<Self, EncryptionError> {
        let (header, chunk_size, _) = read_seal_header(&mut inner).map_err(|e| e.context(what))?;
        let key = open_file_key(keyring, &header, what)?;
        Ok(Self {
            inner,
            cipher: ChunkCipher::new(&key),
            chunk_size,
            what: what.to_string(),
            key_id: header.key_id.clone(),
            raw: Vec::with_capacity(chunk_size + TAG_LEN + 1),
            plain: Vec::new(),
            pos: 0,
            index: 0,
            done: false,
        })
    }

    /// Id of the KEK that wraps this file's DEK.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    fn next_chunk(&mut self) -> io::Result<()> {
        let want = self.chunk_size + TAG_LEN + 1;
        while self.raw.len() < want {
            let start = self.raw.len();
            self.raw.resize(want, 0);
            let n = self.inner.read(&mut self.raw[start..])?;
            self.raw.truncate(start + n);
            if n == 0 {
                break;
            }
        }
        let last = self.raw.len() <= self.chunk_size + TAG_LEN;
        let take = self.raw.len().min(self.chunk_size + TAG_LEN);
        if take == 0 {
            return Err(EncryptionError::Format(format!(
                "{}: sealed file ends without its last chunk",
                self.what
            ))
            .into());
        }
        self.plain = self
            .cipher
            .open(self.index, last, &self.raw[..take], &self.what)?;
        self.raw.drain(..take);
        self.pos = 0;
        self.index += 1;
        self.done = last;
        Ok(())
    }
}

impl<R: Read> Read for SealedReader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        while self.pos == self.plain.len() {
            if self.done {
                return Ok(0);
            }
            self.next_chunk()?;
        }
        let n = out.len().min(self.plain.len() - self.pos);
        out[..n].copy_from_slice(&self.plain[self.pos..self.pos + n]);
        self.pos += n;
        Ok(n)
    }
}

/// Random access to the plaintext of a sealed file.
pub struct SealedFile {
    file: File,
    cipher: ChunkCipher,
    chunk_size: usize,
    data_start: u64,
    chunks: u64,
    plain_len: u64,
    key_id: String,
    what: String,
    cached: Option<(u64, Vec<u8>)>,
}

impl SealedFile {
    pub fn open(path: &Path, keyring: Option<&Keyring>) -> Result<Self, EncryptionError> {
        let what = path.display().to_string();
        let mut file = File::open(path)?;
        let (header, chunk_size, data_start) =
            read_seal_header(&mut file).map_err(|e| e.context(&what))?;
        let key = open_file_key(keyring, &header, &what)?;
        let total = file.metadata()?.len();
        let body = total.saturating_sub(data_start);
        let stride = (chunk_size + TAG_LEN) as u64;
        let full = body / stride;
        let rem = body % stride;
        let (chunks, plain_len) = if rem == 0 {
            (full, full * chunk_size as u64)
        } else if rem >= TAG_LEN as u64 {
            (full + 1, full * chunk_size as u64 + rem - TAG_LEN as u64)
        } else {
            return Err(EncryptionError::Format(format!(
                "{what}: sealed file has a truncated chunk"
            )));
        };
        if chunks == 0 {
            return Err(EncryptionError::Format(format!(
                "{what}: sealed file has no chunks"
            )));
        }
        let mut sealed = Self {
            file,
            cipher: ChunkCipher::new(&key),
            chunk_size,
            data_start,
            chunks,
            plain_len,
            key_id: header.key_id,
            what,
            cached: None,
        };
        // Authenticate the last chunk now: a truncated file (cut at a chunk
        // boundary) fails here, not halfway through a read.
        sealed.chunk(chunks - 1)?;
        Ok(sealed)
    }

    /// Plaintext length.
    pub fn plain_len(&self) -> u64 {
        self.plain_len
    }

    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    fn chunk(&mut self, index: u64) -> io::Result<&[u8]> {
        if self.cached.as_ref().map(|(i, _)| *i) != Some(index) {
            let stride = (self.chunk_size + TAG_LEN) as u64;
            let start = self.data_start + index * stride;
            let len = if index + 1 == self.chunks {
                (self.plain_len - index * self.chunk_size as u64) as usize + TAG_LEN
            } else {
                stride as usize
            };
            let mut raw = vec![0u8; len];
            self.file.seek(SeekFrom::Start(start))?;
            self.file.read_exact(&mut raw)?;
            let plain = self
                .cipher
                .open(index, index + 1 == self.chunks, &raw, &self.what)?;
            self.cached = Some((index, plain));
        }
        Ok(&self.cached.as_ref().expect("cached above").1)
    }

    /// Fills `out` from plaintext offset `offset`; returns the bytes read
    /// (fewer than `out.len()` only at the end).
    pub fn read_at(&mut self, offset: u64, out: &mut [u8]) -> io::Result<usize> {
        let mut done = 0usize;
        while done < out.len() {
            let at = offset + done as u64;
            if at >= self.plain_len {
                break;
            }
            let index = at / self.chunk_size as u64;
            let within = (at % self.chunk_size as u64) as usize;
            let chunk = self.chunk(index)?;
            let n = (chunk.len() - within).min(out.len() - done);
            out[done..done + n].copy_from_slice(&chunk[within..within + n]);
            done += n;
        }
        Ok(done)
    }
}

/// Seals `plain` into the bytes of a sealed file.
pub fn seal_bytes(keyring: &Keyring, plain: &[u8]) -> Result<Vec<u8>, EncryptionError> {
    let mut writer = SealedWriter::new(Vec::with_capacity(plain.len() + 256), keyring)?;
    writer.write_all(plain)?;
    Ok(writer.finish()?)
}

/// Opens the bytes of a sealed file.
pub fn open_sealed_bytes(
    keyring: Option<&Keyring>,
    bytes: &[u8],
    what: &str,
) -> Result<Vec<u8>, EncryptionError> {
    let mut reader = SealedReader::new(bytes, keyring, what)?;
    let mut out = Vec::with_capacity(bytes.len());
    reader
        .read_to_end(&mut out)
        .map_err(|e| EncryptionError::Format(e.to_string()))?;
    Ok(out)
}

/// Replaces the header of the sealed file `bytes` by one wrapping the same
/// DEK with the active KEK. `None` when it already uses the active KEK.
pub fn rewrap_sealed_header(
    keyring: &Keyring,
    bytes: &[u8],
    what: &str,
) -> Result<Option<Vec<u8>>, EncryptionError> {
    let mut reader = bytes;
    let (header, chunk_size, data_start) =
        read_seal_header(&mut reader).map_err(|e| e.context(what))?;
    if header.key_id == keyring.active_key_id() {
        return Ok(None);
    }
    let rewrapped = keyring.rewrap(&header, what)?;
    debug_assert_eq!(rewrapped.file_id.len(), FILE_ID_LEN);
    let mut out = prefix_bytes(&rewrapped, chunk_size);
    out.extend_from_slice(&bytes[data_start as usize..]);
    Ok(Some(out))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyring() -> Keyring {
        Keyring::local([6u8; 32], &[]).unwrap()
    }

    fn seal_with(keyring: &Keyring, plain: &[u8], chunk: usize) -> Vec<u8> {
        let mut writer = SealedWriter::with_chunk_size(Vec::new(), keyring, chunk).unwrap();
        // Uneven writes.
        for piece in plain.chunks(7) {
            writer.write_all(piece).unwrap();
        }
        writer.finish().unwrap()
    }

    #[test]
    fn sealed_files_round_trip_at_every_size() {
        let keyring = keyring();
        for len in [0usize, 1, 15, 16, 17, 31, 32, 33, 100] {
            let plain: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
            let sealed = seal_with(&keyring, &plain, 16);
            let back = {
                let mut r = SealedReader::new(&sealed[..], Some(&keyring), "t").unwrap();
                let mut out = Vec::new();
                r.read_to_end(&mut out).unwrap();
                out
            };
            assert_eq!(back, plain, "len {len}");

            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("f");
            std::fs::write(&path, &sealed).unwrap();
            let mut file = SealedFile::open(&path, Some(&keyring)).unwrap();
            assert_eq!(file.plain_len(), len as u64);
            for start in 0..=len {
                let mut out = vec![0u8; 20];
                let n = file.read_at(start as u64, &mut out).unwrap();
                let want = &plain[start..(start + 20).min(len)];
                assert_eq!(&out[..n], want, "len {len} start {start}");
            }
        }
    }

    #[test]
    fn any_damage_is_an_error_not_garbage() {
        let keyring = keyring();
        let plain: Vec<u8> = (0..70u8).collect();
        let sealed = seal_with(&keyring, &plain, 16);
        let read = |bytes: &[u8]| -> Result<Vec<u8>, String> {
            let mut r = SealedReader::new(bytes, Some(&keyring), "t").map_err(|e| e.to_string())?;
            let mut out = Vec::new();
            r.read_to_end(&mut out).map_err(|e| e.to_string())?;
            Ok(out)
        };
        for i in 0..sealed.len() {
            let mut bad = sealed.clone();
            bad[i] ^= 0x40;
            assert!(read(&bad).is_err(), "flip at {i} went unnoticed");
        }
        for cut in 0..sealed.len() {
            assert!(read(&sealed[..cut]).is_err(), "truncation at {cut}");
        }
        let mut extended = sealed.clone();
        extended.extend_from_slice(&[0u8; 32]);
        assert!(read(&extended).is_err());
        // Dropping a whole middle chunk.
        let (_, _, start) = read_seal_header(&mut &sealed[..]).unwrap();
        let start = start as usize;
        let mut dropped = sealed[..start].to_vec();
        dropped.extend_from_slice(&sealed[start + 32..]);
        assert!(read(&dropped).is_err());
    }

    #[test]
    fn writer_dropped_without_finish_leaves_an_unreadable_file() {
        let keyring = keyring();
        let mut writer = SealedWriter::with_chunk_size(Vec::new(), &keyring, 16).unwrap();
        writer.write_all(&[1u8; 40]).unwrap();
        // Simulate an interrupted write: take what was emitted so far.
        let partial = writer.inner.take().unwrap();
        assert!(open_sealed_bytes(Some(&keyring), &partial, "t").is_err());
    }

    #[test]
    fn rewrap_changes_only_the_header() {
        let old = keyring();
        let sealed = seal_bytes(&old, b"hello sealed world").unwrap();
        let new = Keyring::local([7u8; 32], &[[6u8; 32]]).unwrap();
        let rewrapped = rewrap_sealed_header(&new, &sealed, "t").unwrap().unwrap();
        assert!(
            rewrap_sealed_header(&new, &rewrapped, "t")
                .unwrap()
                .is_none()
        );
        let only_new = Keyring::local([7u8; 32], &[]).unwrap();
        assert_eq!(
            open_sealed_bytes(Some(&only_new), &rewrapped, "t").unwrap(),
            b"hello sealed world"
        );
        assert!(open_sealed_bytes(Some(&only_new), &sealed, "t").is_err());
        assert!(matches!(
            open_sealed_bytes(None, &sealed, "t"),
            Err(EncryptionError::NotConfigured { .. })
        ));
    }
}
