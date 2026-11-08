//! Seekable attachment bytes stored only in authenticated SQLCipher pages.

use super::storage;
use anyhow::{Context, Result, ensure};
use rusqlite::{Connection, OptionalExtension, params};
use sha2::{Digest, Sha256};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::Path;

pub(super) const CHUNK_BYTES: usize = 64 * 1024;
pub(super) const INDEX_NAME: &str = ".wassup-media-index.sqlite";
const APPLICATION_ID: i64 = 0x574d4131;

pub(super) struct ChunkFile {
    connection: Connection,
    position: u64,
    length: u64,
    writable: bool,
}

impl ChunkFile {
    pub(super) fn create(path: &Path, key: &[u8; 32]) -> Result<Self> {
        let connection = storage::open(path, key)?;
        connection.execute_batch(
            "PRAGMA journal_mode=DELETE;
             PRAGMA application_id=1464680753;
             CREATE TABLE media_metadata (singleton INTEGER PRIMARY KEY CHECK(singleton=1),
               length INTEGER NOT NULL CHECK(length>=0), sha256 BLOB);
             INSERT INTO media_metadata VALUES(1,0,NULL);
             CREATE TABLE media_chunks (chunk INTEGER PRIMARY KEY CHECK(chunk>=0), bytes BLOB NOT NULL);
             BEGIN IMMEDIATE;",
        )?;
        Ok(Self {
            connection,
            position: 0,
            length: 0,
            writable: true,
        })
    }

    pub(super) fn open(path: &Path, key: &[u8; 32]) -> Result<Self> {
        let connection = storage::read_only(path, key)?;
        let application: i64 =
            connection.pragma_query_value(None, "application_id", |r| r.get(0))?;
        ensure!(
            application == APPLICATION_ID,
            "Invalid encrypted attachment format"
        );
        let (length, hash): (i64, Vec<u8>) = connection.query_row(
            "SELECT length,sha256 FROM media_metadata WHERE singleton=1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let length = u64::try_from(length).context("Invalid attachment length")?;
        ensure!(hash.len() == 32, "Incomplete encrypted attachment");
        Ok(Self {
            connection,
            position: 0,
            length,
            writable: false,
        })
    }

    pub(super) fn len(&self) -> u64 {
        self.length
    }

    pub(super) fn truncate(&mut self, length: u64) -> io::Result<()> {
        self.require_writable()?;
        if length < self.length {
            let first_removed =
                i64::try_from(length.div_ceil(CHUNK_BYTES as u64)).map_err(io_error)?;
            self.connection
                .execute("DELETE FROM media_chunks WHERE chunk>=?", [first_removed])
                .map_err(io_error)?;
            let tail = (length % CHUNK_BYTES as u64) as usize;
            if tail != 0 {
                let index = length / CHUNK_BYTES as u64;
                if let Some(mut bytes) = self.chunk(index)? {
                    bytes[tail..].fill(0);
                    self.put_chunk(index, &bytes)?;
                }
            }
        }
        self.length = length;
        Ok(())
    }

    fn require_writable(&self) -> io::Result<()> {
        if self.writable {
            Ok(())
        } else {
            Err(io::Error::other("Attachment is read-only"))
        }
    }

    fn chunk(&self, index: u64) -> io::Result<Option<Vec<u8>>> {
        let index = i64::try_from(index).map_err(io_error)?;
        let value: Option<Vec<u8>> = self
            .connection
            .query_row(
                "SELECT bytes FROM media_chunks WHERE chunk=?",
                [index],
                |r| r.get(0),
            )
            .optional()
            .map_err(io_error)?;
        if value
            .as_ref()
            .is_some_and(|bytes| bytes.len() != CHUNK_BYTES)
        {
            return Err(io::Error::other("Invalid encrypted attachment chunk"));
        }
        Ok(value)
    }

    fn put_chunk(&self, index: u64, bytes: &[u8]) -> io::Result<()> {
        let index = i64::try_from(index).map_err(io_error)?;
        self.connection
            .execute(
                "INSERT INTO media_chunks(chunk,bytes) VALUES(?,?)
             ON CONFLICT(chunk) DO UPDATE SET bytes=excluded.bytes",
                params![index, bytes],
            )
            .map_err(io_error)?;
        Ok(())
    }

    pub(super) fn digest(&mut self) -> Result<[u8; 32]> {
        let position = self.position;
        self.position = 0;
        let result = digest_reader(self);
        self.position = position;
        result
    }

    /// Read all authenticated pages, then verify the committed logical bytes.
    pub(super) fn verify(&mut self) -> Result<[u8; 32]> {
        let expected = self.expected_digest()?;
        let actual = self.digest()?;
        ensure!(
            expected == actual,
            "Encrypted attachment integrity check failed"
        );
        Ok(actual)
    }

    pub(super) fn expected_digest(&self) -> Result<[u8; 32]> {
        ensure!(!self.writable, "Attachment has not been committed");
        let expected: Vec<u8> = self.connection.query_row(
            "SELECT sha256 FROM media_metadata WHERE singleton=1",
            [],
            |r| r.get(0),
        )?;
        expected
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid attachment digest"))
    }

    pub(super) fn commit(mut self) -> Result<(u64, [u8; 32])> {
        self.require_writable()?;
        let hash = self.digest()?;
        self.connection.execute(
            "UPDATE media_metadata SET length=?,sha256=? WHERE singleton=1",
            params![i64::try_from(self.length)?, hash.as_slice()],
        )?;
        self.connection.execute_batch("COMMIT")?;
        self.writable = false;
        let length = self.length;
        self.connection.close().map_err(|(_, error)| error)?;
        Ok((length, hash))
    }
}

impl Read for ChunkFile {
    fn read(&mut self, target: &mut [u8]) -> io::Result<usize> {
        let count = (self.length.saturating_sub(self.position)).min(target.len() as u64) as usize;
        let mut done = 0;
        while done < count {
            let index = self.position / CHUNK_BYTES as u64;
            let offset = (self.position % CHUNK_BYTES as u64) as usize;
            let take = (CHUNK_BYTES - offset).min(count - done);
            if let Some(bytes) = self.chunk(index)? {
                target[done..done + take].copy_from_slice(&bytes[offset..offset + take]);
            } else {
                target[done..done + take].fill(0);
            }
            done += take;
            self.position += take as u64;
        }
        Ok(done)
    }
}

impl Write for ChunkFile {
    fn write(&mut self, source: &[u8]) -> io::Result<usize> {
        self.require_writable()?;
        self.position
            .checked_add(source.len() as u64)
            .filter(|end| *end <= i64::MAX as u64)
            .ok_or_else(|| io::Error::other("Attachment length overflow"))?;
        let mut done = 0;
        while done < source.len() {
            let index = self.position / CHUNK_BYTES as u64;
            let offset = (self.position % CHUNK_BYTES as u64) as usize;
            let take = (CHUNK_BYTES - offset).min(source.len() - done);
            let mut bytes = if offset == 0 && take == CHUNK_BYTES {
                vec![0; CHUNK_BYTES]
            } else {
                self.chunk(index)?.unwrap_or_else(|| vec![0; CHUNK_BYTES])
            };
            bytes[offset..offset + take].copy_from_slice(&source[done..done + take]);
            self.put_chunk(index, &bytes)?;
            self.position += take as u64;
            self.length = self.length.max(self.position);
            done += take;
        }
        Ok(done)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for ChunkFile {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let position = match from {
            SeekFrom::Start(position) => i128::from(position),
            SeekFrom::Current(offset) => i128::from(self.position) + i128::from(offset),
            SeekFrom::End(offset) => i128::from(self.length) + i128::from(offset),
        };
        self.position = u64::try_from(position)
            .ok()
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "Invalid attachment seek")
            })?;
        Ok(self.position)
    }
}

pub(super) fn digest_reader(reader: &mut impl Read) -> Result<[u8; 32]> {
    let mut hash = Sha256::new();
    let mut buffer = [0; CHUNK_BYTES];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        hash.update(&buffer[..count]);
    }
    Ok(hash.finalize().into())
}

pub(super) fn io_error(error: impl std::fmt::Display) -> io::Error {
    io::Error::other(error.to_string())
}

/// A keyed registry removes ambiguity between legacy bytes and wrong-key or
/// damaged ciphertext. Entries are durable before replacing/publishing bytes.
pub(super) struct Registry(Connection);

pub(super) enum Entry {
    Unregistered,
    Encrypted,
    Legacy { length: u64, hash: Vec<u8> },
}

impl Registry {
    pub(super) fn open(directory: &Path, key: &[u8; 32]) -> Result<Self> {
        let path = directory.join(INDEX_NAME);
        super::reject_symlink(&path)?;
        if !path.exists() {
            let file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            super::private_file(&path)?;
            file.sync_all()?;
            std::fs::File::open(directory)?.sync_all()?;
        }
        let connection = storage::open(&path, key)?;
        connection.execute_batch(
            "PRAGMA journal_mode=DELETE;
             CREATE TABLE IF NOT EXISTS media_files (
               name BLOB PRIMARY KEY, legacy_length INTEGER, legacy_hash BLOB,
               CHECK((legacy_length IS NULL)=(legacy_hash IS NULL)));
             CREATE TABLE IF NOT EXISTS media_staging (name BLOB PRIMARY KEY);",
        )?;
        super::private_file(&path)?;
        Ok(Self(connection))
    }

    pub(super) fn register_staging(&self, path: &Path) -> Result<()> {
        self.0
            .execute("INSERT INTO media_staging(name) VALUES(?)", [name(path)?])?;
        Ok(())
    }

    pub(super) fn is_staging(&self, path: &Path) -> Result<bool> {
        Ok(self.0.query_row(
            "SELECT EXISTS(SELECT 1 FROM media_staging WHERE name=?)",
            [name(path)?],
            |row| row.get(0),
        )?)
    }

    pub(super) fn forget_staging(&self, path: &Path) -> Result<()> {
        self.0
            .execute("DELETE FROM media_staging WHERE name=?", [name(path)?])?;
        Ok(())
    }

    pub(super) fn entry(&self, path: &Path) -> Result<Entry> {
        let result: Option<(Option<i64>, Option<Vec<u8>>)> = self
            .0
            .query_row(
                "SELECT legacy_length,legacy_hash FROM media_files WHERE name=?",
                [name(path)?],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        match result {
            None => Ok(Entry::Unregistered),
            Some((None, None)) => Ok(Entry::Encrypted),
            Some((Some(length), Some(hash))) if hash.len() == 32 => Ok(Entry::Legacy {
                length: u64::try_from(length).context("Invalid migration length")?,
                hash,
            }),
            _ => anyhow::bail!("Invalid attachment migration state"),
        }
    }

    pub(super) fn register(&self, path: &Path) -> Result<()> {
        self.0.execute(
            "INSERT OR IGNORE INTO media_files(name) VALUES(?)",
            [name(path)?],
        )?;
        Ok(())
    }

    pub(super) fn legacy(&self, path: &Path, length: u64, hash: &[u8]) -> Result<()> {
        self.0.execute(
            "INSERT INTO media_files(name,legacy_length,legacy_hash) VALUES(?,?,?)",
            params![name(path)?, i64::try_from(length)?, hash],
        )?;
        Ok(())
    }

    pub(super) fn migrated(&self, path: &Path) -> Result<()> {
        self.0.execute(
            "UPDATE media_files SET legacy_length=NULL,legacy_hash=NULL WHERE name=?",
            [name(path)?],
        )?;
        Ok(())
    }
}

fn name(path: &Path) -> Result<&[u8]> {
    Ok(path
        .file_name()
        .context("Missing attachment filename")?
        .as_encoded_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;
    const KEY: [u8; 32] = [91; 32];

    #[test]
    fn chunked_seek_overwrite_truncate_and_sparse_extension() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().canonicalize().unwrap().join("fixture.bin");
        let mut file = ChunkFile::create(&path, &KEY).unwrap();
        file.write_all(&vec![b'a'; CHUNK_BYTES + 10]).unwrap();
        file.seek(SeekFrom::Start((CHUNK_BYTES - 1) as u64))
            .unwrap();
        file.write_all(b"XYZ").unwrap();
        file.truncate((CHUNK_BYTES + 1) as u64).unwrap();
        file.truncate((CHUNK_BYTES + 4) as u64).unwrap();
        file.seek(SeekFrom::End(-6)).unwrap();
        let mut tail = Vec::new();
        file.read_to_end(&mut tail).unwrap();
        assert_eq!(tail, b"aXY\0\0\0");
        let (_, hash) = file.commit().unwrap();
        let mut saved = ChunkFile::open(&path, &KEY).unwrap();
        assert_eq!(saved.verify().unwrap(), hash);
        assert!(saved.write_all(b"no").is_err());
        assert!(ChunkFile::open(&path, &[12; 32]).is_err());
        let bytes = std::fs::read(path).unwrap();
        assert!(!bytes.windows(128).any(|part| part == [b'a'; 128]));
        assert!(!bytes.starts_with(b"SQLite format 3"));
    }

    #[test]
    fn uncommitted_container_is_never_a_complete_attachment() {
        let root = tempfile::tempdir().unwrap();
        let path = root.path().canonicalize().unwrap().join("partial.bin");
        {
            let mut file = ChunkFile::create(&path, &KEY).unwrap();
            file.write_all(b"synthetic secret").unwrap();
        }
        assert!(ChunkFile::open(&path, &KEY).is_err());
    }
}
