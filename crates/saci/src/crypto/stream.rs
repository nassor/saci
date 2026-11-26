//! Chunked authenticated stream encryption and decryption for byte-stream codecs.

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use crate::crypto::envelope::Cipher;
use crate::error::SaciError;

/// Stream magic header.
const STREAM_MAGIC: [u8; 4] = *b"SACS";
/// Stream format version.
const STREAM_VERSION: u8 = 1;
/// Default plaintext chunk size (64 KiB).
const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;
/// Maximum allowed chunk frame size (64 MiB) to guard against corrupt length prefixes.
const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

/// Streaming AEAD writer that frames plaintext into indexed, authenticated chunks.
///
/// Each chunk binds `(header, chunk_index: u64, is_final: u8)` into its AEAD
/// AAD so header tampering, chunk reordering, duplication, or silent stream
/// truncation is detected on decryption.
pub struct EncryptingWriter<W: Write + Send> {
    inner: W,
    cipher: Arc<Cipher>,
    chunk_size: usize,
    header: [u8; 9],
    buf: Vec<u8>,
    chunk_index: u64,
    header_written: bool,
    finished: bool,
}

impl<W: Write + Send> EncryptingWriter<W> {
    /// Wrap `inner` with chunked AEAD encryption using the default 64 KiB chunk size.
    pub fn new(inner: W, cipher: Arc<Cipher>) -> Self {
        Self::with_chunk_size(inner, cipher, DEFAULT_CHUNK_SIZE)
    }

    /// Wrap `inner` with an explicit plaintext `chunk_size` (clamped to `1..=MAX_FRAME_LEN`).
    pub fn with_chunk_size(inner: W, cipher: Arc<Cipher>, chunk_size: usize) -> Self {
        let chunk_size = chunk_size.clamp(1, MAX_FRAME_LEN);
        let mut header = [0u8; 9];
        header[..4].copy_from_slice(&STREAM_MAGIC);
        header[4] = STREAM_VERSION;
        let cap = u32::try_from(chunk_size).unwrap_or(MAX_FRAME_LEN as u32);
        header[5..9].copy_from_slice(&cap.to_be_bytes());
        Self {
            inner,
            cipher,
            chunk_size,
            header,
            buf: Vec::with_capacity(chunk_size),
            chunk_index: 0,
            header_written: false,
            finished: false,
        }
    }

    fn ensure_header(&mut self) -> std::io::Result<()> {
        if self.header_written {
            return Ok(());
        }
        self.inner.write_all(&self.header)?;
        self.header_written = true;
        Ok(())
    }

    fn write_chunk(&mut self, data: &[u8], is_final: bool) -> std::io::Result<()> {
        self.ensure_header()?;
        let aad = chunk_aad(&self.header, self.chunk_index, is_final);
        let env = self
            .cipher
            .encrypt(data, &aad)
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        let frame_len = u32::try_from(env.len())
            .map_err(|_| std::io::Error::other("encrypted chunk exceeds u32 length"))?;
        self.inner.write_all(&frame_len.to_be_bytes())?;
        self.inner.write_all(&env)?;
        self.chunk_index = self
            .chunk_index
            .checked_add(1)
            .ok_or_else(|| std::io::Error::other("encrypted stream chunk index overflow"))?;
        Ok(())
    }

    /// Flush any buffered plaintext as a non-final encrypted chunk and flush `inner`.
    pub(crate) fn flush_chunk(&mut self) -> std::io::Result<()> {
        if self.finished {
            return Ok(());
        }
        if !self.buf.is_empty() {
            let chunk = std::mem::take(&mut self.buf);
            self.write_chunk(&chunk, false)?;
        }
        self.inner.flush()
    }

    /// Seal the final authenticated chunk and flush the underlying writer.
    ///
    /// # Errors
    ///
    /// Returns [`SaciError::Generic`] if writing or sealing fails.
    pub fn finish(mut self) -> Result<W, SaciError> {
        self.finish_in_place()
            .map_err(|e| SaciError::generic(format!("crypto: finishing encrypted stream: {e}")))?;
        Ok(self.inner)
    }

    pub(crate) fn finish_in_place(&mut self) -> std::io::Result<()> {
        if self.finished {
            return Ok(());
        }
        let tail = std::mem::take(&mut self.buf);
        self.write_chunk(&tail, true)?;
        self.finished = true;
        self.inner.flush()
    }
}

impl<W: Write + Send> Write for EncryptingWriter<W> {
    fn write(&mut self, mut src: &[u8]) -> std::io::Result<usize> {
        if self.finished {
            return Err(std::io::Error::other(
                "cannot write to an already-finished EncryptingWriter",
            ));
        }
        let total = src.len();
        while !src.is_empty() {
            let space = self.chunk_size - self.buf.len();
            let take = space.min(src.len());
            self.buf.extend_from_slice(&src[..take]);
            src = &src[take..];
            if self.buf.len() == self.chunk_size {
                let full = std::mem::take(&mut self.buf);
                self.write_chunk(&full, false)?;
            }
        }
        Ok(total)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.flush_chunk()
    }
}

fn chunk_aad(header: &[u8; 9], chunk_index: u64, is_final: bool) -> [u8; 29] {
    let mut aad = [0u8; 29];
    aad[..11].copy_from_slice(b"saci:chunk:");
    aad[11..20].copy_from_slice(header);
    aad[20..28].copy_from_slice(&chunk_index.to_be_bytes());
    aad[28] = u8::from(is_final);
    aad
}

/// Decrypt a stream from `src` into `dst`, checking header, chunk order, and finality.
pub(crate) fn decrypt_stream<R: Read, W: Write>(
    src: &mut R,
    dst: &mut W,
    cipher: &Cipher,
) -> Result<(), SaciError> {
    let mut hdr = [0u8; 9];
    src.read_exact(&mut hdr)
        .map_err(|e| SaciError::generic(format!("crypto: reading encrypted stream header: {e}")))?;
    if hdr[..4] != STREAM_MAGIC {
        return Err(SaciError::generic(
            "crypto: invalid encrypted stream magic header",
        ));
    }
    if hdr[4] != STREAM_VERSION {
        return Err(SaciError::generic(format!(
            "crypto: unsupported encrypted stream version {}",
            hdr[4]
        )));
    }
    let chunk_size = u32::from_be_bytes([hdr[5], hdr[6], hdr[7], hdr[8]]) as usize;
    if chunk_size == 0 || chunk_size > MAX_FRAME_LEN {
        return Err(SaciError::generic(format!(
            "crypto: invalid encrypted stream chunk size {chunk_size}"
        )));
    }

    let mut chunk_index = 0u64;
    let mut saw_final = false;
    loop {
        let mut first = [0u8; 1];
        loop {
            match src.read(&mut first) {
                Ok(0) => {
                    if !saw_final {
                        return Err(SaciError::generic(
                            "crypto: encrypted stream truncated before final chunk",
                        ));
                    }
                    dst.flush().map_err(|e| {
                        SaciError::generic(format!("crypto: flushing decrypted stream bytes: {e}"))
                    })?;
                    return Ok(());
                }
                Ok(_) => {
                    if saw_final {
                        return Err(SaciError::generic(
                            "crypto: unexpected trailing data after final encrypted chunk",
                        ));
                    }
                    break;
                }
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => {
                    return Err(SaciError::generic(format!(
                        "crypto: reading encrypted chunk length: {e}"
                    )));
                }
            }
        }
        let mut len_buf = [first[0], 0, 0, 0];
        src.read_exact(&mut len_buf[1..]).map_err(|e| {
            SaciError::generic(format!(
                "crypto: truncated encrypted chunk length prefix: {e}"
            ))
        })?;

        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 || len > MAX_FRAME_LEN {
            return Err(SaciError::generic(format!(
                "crypto: invalid encrypted chunk frame length {len}"
            )));
        }
        let mut env = vec![0u8; len];
        src.read_exact(&mut env).map_err(|e| {
            SaciError::generic(format!("crypto: reading encrypted chunk body: {e}"))
        })?;

        // Probe non-final first; if authentication fails, try final chunk AAD.
        let aad_non_final = chunk_aad(&hdr, chunk_index, false);
        let plain = match cipher.decrypt(&env, &aad_non_final) {
            Ok(plain) => plain,
            Err(_) => {
                let aad_final = chunk_aad(&hdr, chunk_index, true);
                let plain = cipher.decrypt(&env, &aad_final).map_err(|_| {
                    SaciError::generic(format!(
                        "crypto: chunk {chunk_index} failed AEAD authentication"
                    ))
                })?;
                saw_final = true;
                plain
            }
        };

        dst.write_all(&plain).map_err(|e| {
            SaciError::generic(format!("crypto: writing decrypted stream bytes: {e}"))
        })?;
        chunk_index = chunk_index
            .checked_add(1)
            .ok_or_else(|| SaciError::generic("crypto: encrypted stream chunk index overflow"))?;
    }
}

/// Decrypt a [`crate::codec::ReadInput`] while preserving seekability (`Memory` or unlinked temp `File`).
#[cfg(feature = "codec-api")]
pub(crate) fn decrypt_read_input(
    mut input: crate::codec::ReadInput,
    cipher: &Cipher,
) -> Result<crate::codec::ReadInput, SaciError> {
    match &mut input {
        crate::codec::ReadInput::Memory(_) => {
            let mut plain = Vec::new();
            decrypt_stream(&mut input, &mut plain, cipher)?;
            Ok(crate::codec::ReadInput::from(plain))
        }
        crate::codec::ReadInput::File(_) => {
            let mut tmp = tempfile::tempfile().map_err(|e| {
                SaciError::generic(format!("crypto: cannot create decryption temp file: {e}"))
            })?;
            decrypt_stream(&mut input, &mut tmp, cipher)?;
            tmp.seek(SeekFrom::Start(0)).map_err(|e| {
                SaciError::generic(format!("crypto: cannot rewind decrypted temp file: {e}"))
            })?;
            Ok(crate::codec::ReadInput::File(tmp))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use super::*;
    use crate::crypto::keys::{KeyMaterial, StaticKeyProvider};

    fn test_cipher() -> Arc<Cipher> {
        let provider =
            Arc::new(StaticKeyProvider::single("k1", KeyMaterial::generate().unwrap()).unwrap());
        Arc::new(Cipher::aes_256_gcm(provider))
    }

    #[test]
    fn stream_round_trip_empty_exact_and_multi_chunk() {
        let cipher = test_cipher();
        for payload in [
            Vec::new(),
            vec![42u8; 64],
            vec![7u8; 1024],
            (0..200_000u32).map(|i| (i % 251) as u8).collect(),
        ] {
            let mut writer = EncryptingWriter::with_chunk_size(Vec::new(), cipher.clone(), 1024);
            writer.write_all(&payload).unwrap();
            let encrypted = writer.finish().unwrap();

            let mut out = Vec::new();
            decrypt_stream(&mut Cursor::new(&encrypted), &mut out, &cipher).unwrap();
            assert_eq!(out, payload);
        }
    }

    #[test]
    fn truncated_and_reordered_streams_are_rejected() {
        let cipher = test_cipher();
        let mut writer = EncryptingWriter::with_chunk_size(Vec::new(), cipher.clone(), 16);
        writer.write_all(&[9u8; 40]).unwrap();
        let encrypted = writer.finish().unwrap();

        // Chop off at every prefix shorter than the full stream -> must fail.
        for cutoff in 0..encrypted.len() {
            let mut out = Vec::new();
            assert!(
                decrypt_stream(&mut Cursor::new(&encrypted[..cutoff]), &mut out, &cipher).is_err(),
                "truncation at byte {cutoff} must fail"
            );
        }

        // Flip any byte of the 9-byte stream header (magic, version, chunk_size).
        for pos in 0..9 {
            let mut bad = encrypted.clone();
            bad[pos] ^= 0x01;
            let mut out = Vec::new();
            assert!(
                decrypt_stream(&mut Cursor::new(&bad), &mut out, &cipher).is_err(),
                "header flip at byte {pos} must fail"
            );
        }

        // Trailing 1, 2, 3, and 4+ bytes after the final chunk must all fail.
        for trailing in [vec![0u8], vec![1, 2], vec![1, 2, 3], vec![1, 2, 3, 4]] {
            let mut with_tail = encrypted.clone();
            with_tail.extend_from_slice(&trailing);
            let mut out = Vec::new();
            assert!(
                decrypt_stream(&mut Cursor::new(&with_tail), &mut out, &cipher).is_err(),
                "trailing {} byte(s) must fail",
                trailing.len()
            );
        }

        // Chunk reordering / duplication / omission detection.
        // Extract frames starting after the 9-byte header.
        let mut frames = Vec::new();
        let mut pos = 9usize;
        while pos < encrypted.len() {
            let flen = u32::from_be_bytes(encrypted[pos..pos + 4].try_into().unwrap()) as usize;
            frames.push(encrypted[pos..pos + 4 + flen].to_vec());
            pos += 4 + flen;
        }
        assert!(frames.len() >= 3);

        // Swap chunks 0 and 1.
        let mut swapped = encrypted[..9].to_vec();
        swapped.extend_from_slice(&frames[1]);
        swapped.extend_from_slice(&frames[0]);
        for f in &frames[2..] {
            swapped.extend_from_slice(f);
        }
        assert!(decrypt_stream(&mut Cursor::new(&swapped), &mut Vec::new(), &cipher).is_err());

        // Duplicate chunk 0.
        let mut dup = encrypted[..9].to_vec();
        dup.extend_from_slice(&frames[0]);
        for f in &frames {
            dup.extend_from_slice(f);
        }
        assert!(decrypt_stream(&mut Cursor::new(&dup), &mut Vec::new(), &cipher).is_err());

        // Drop middle chunk 1.
        let mut dropped = encrypted[..9].to_vec();
        dropped.extend_from_slice(&frames[0]);
        for f in &frames[2..] {
            dropped.extend_from_slice(f);
        }
        assert!(decrypt_stream(&mut Cursor::new(&dropped), &mut Vec::new(), &cipher).is_err());
    }

    #[test]
    fn chunk_boundaries_flush_and_post_finish_write_refusal() {
        let cipher = test_cipher();
        let chunk_size = 64usize;
        for len in [
            0usize,
            1,
            chunk_size - 1,
            chunk_size,
            chunk_size + 1,
            2 * chunk_size + 7,
        ] {
            let payload: Vec<u8> = (0..len).map(|i| (i % 253) as u8).collect();
            let mut writer =
                EncryptingWriter::with_chunk_size(Vec::new(), cipher.clone(), chunk_size);
            if !payload.is_empty() {
                let mid = payload.len() / 2;
                writer.write_all(&payload[..mid]).unwrap();
                writer.flush().unwrap();
                writer.write_all(&payload[mid..]).unwrap();
            }
            writer.finish_in_place().unwrap();
            assert!(
                writer.write_all(b"after-finish").is_err(),
                "writes after finish must fail"
            );
            let encrypted = writer.finish().unwrap();
            let mut out = Vec::new();
            decrypt_stream(&mut Cursor::new(&encrypted), &mut out, &cipher).unwrap();
            assert_eq!(out, payload);
        }
    }
}
