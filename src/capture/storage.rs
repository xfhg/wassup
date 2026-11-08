//! SQLCipher storage with keys owned by permanent capture namespaces.
//!
//! A namespace is the account directory's stable digest, not its path or the
//! reusable application account slot. Its key survives unlinking and moves.

use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use keyring_core::api::CredentialStoreApi;
use rusqlite::{Connection, OpenFlags};
use zeroize::Zeroizing;

pub(super) type Key = Arc<Zeroizing<[u8; 32]>>;

const SERVICE: &str = "local.wassup.capture";
const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";
static KEY_CREATION: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn key_identity(namespace: &str) -> Result<String> {
    ensure!(
        namespace.len() == 64 && namespace.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "Invalid permanent archive namespace"
    );
    Ok(format!("capture-{}", namespace.to_ascii_lowercase()))
}

/// Unit tests never read or write the real operating-system keyring.
#[cfg(test)]
pub(super) fn key_for(_account_root: &Path, namespace: &str) -> Result<Key> {
    use sha2::{Digest, Sha256};
    let identity = key_identity(namespace)?;
    let mut digest = Sha256::new();
    digest.update(b"Wassup synthetic capture key; tests only\0");
    digest.update(identity.as_bytes());
    Ok(Arc::new(Zeroizing::new(digest.finalize().into())))
}

#[cfg(not(test))]
pub(super) fn key_for(account_root: &Path, namespace: &str) -> Result<Key> {
    let identity = key_identity(namespace)?;
    #[cfg(target_os = "linux")]
    let store = zbus_secret_service_keyring_store::Store::new();
    #[cfg(target_os = "macos")]
    let store = apple_native_keyring_store::keychain::Store::new();
    #[cfg(windows)]
    let store = windows_native_keyring_store::Store::new();
    let store = store.map_err(|_| anyhow::anyhow!("Unlock the OS keyring and restart Wassup"))?;
    let entry = store
        .build(SERVICE, &identity, None)
        .map_err(|_| anyhow::anyhow!("Could not open the permanent archive key"))?;
    key_from_entry(account_root, &entry)
}

fn key_from_entry(account_root: &Path, entry: &keyring_core::Entry) -> Result<Key> {
    let _creation = KEY_CREATION
        .lock()
        .map_err(|_| anyhow::anyhow!("Permanent archive key access is unavailable"))?;
    match entry.get_secret() {
        Ok(secret) => {
            let secret = Zeroizing::new(secret);
            ensure!(secret.len() == 32, "The permanent archive key is invalid");
            let mut key = Zeroizing::new([0; 32]);
            key.copy_from_slice(&secret);
            Ok(Arc::new(key))
        }
        Err(keyring_core::Error::NoEntry) => {
            ensure!(
                !encrypted_content_exists(account_root)?,
                "The permanent archive key is missing. Restore its original keyring key; the archive has not been changed"
            );
            let mut key = Zeroizing::new([0; 32]);
            getrandom::fill(key.as_mut()).context("Could not generate a permanent archive key")?;
            entry
                .set_secret(key.as_ref())
                .map_err(|_| anyhow::anyhow!("Could not save the permanent archive key"))?;
            let saved = Zeroizing::new(
                entry
                    .get_secret()
                    .map_err(|_| anyhow::anyhow!("Could not verify the permanent archive key"))?,
            );
            ensure!(
                saved.as_slice() == key.as_ref(),
                "The OS keyring did not retain the permanent archive key"
            );
            Ok(Arc::new(key))
        }
        Err(_) => anyhow::bail!("Unlock the OS keyring and restart Wassup"),
    }
}

/// Refuse replacement keys even when an encrypted journal was moved aside.
fn encrypted_content_exists(account_root: &Path) -> Result<bool> {
    let journal = account_root.join("journal.sqlite3");
    for name in ["journal.sqlite3", "journal.sqlite3.encrypting"] {
        let path = account_root.join(name);
        if !plaintext_or_missing(&path)? {
            return Ok(true);
        }
    }
    // With no authoritative database, orphan sidecars cannot be classified
    // as plaintext safely. Never initialize over potential retained pages.
    if !journal.try_exists()? || fs::metadata(&journal)?.len() == 0 {
        for suffix in ["-wal", "-shm", "-journal"] {
            match fs::symlink_metadata(account_root.join(format!("journal.sqlite3{suffix}"))) {
                Ok(_) => return Ok(true),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    let media = account_root.join("media");
    match fs::symlink_metadata(&media) {
        Ok(metadata) => ensure!(
            metadata.is_dir() && !metadata.file_type().is_symlink(),
            "Permanent archive media path is not a real directory"
        ),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    }
    for entry in fs::read_dir(media)? {
        let entry = entry?;
        ensure!(
            !entry.file_type()?.is_symlink(),
            "Permanent archive media paths cannot be symbolic links"
        );
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // The media writer durably creates this encrypted manifest before
        // replacing any legacy file, whose original extension is retained.
        if name == ".wassup-media-index.sqlite"
            || name.starts_with(".wassup-media-index.sqlite-")
            || name.starts_with(".wassup-media-index.sqlite.")
            || name.starts_with(".wassup-encrypted-")
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn plaintext_or_missing(path: &Path) -> Result<bool> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(true),
        Err(error) => return Err(error.into()),
    };
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "Permanent archive database must be a regular file"
    );
    if metadata.len() == 0 {
        return Ok(true);
    }
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW);
    }
    let mut file = options.open(path)?;
    let mut header = [0; 16];
    // A truncated or unknown file is never grounds to generate a new key.
    file.read_exact(&mut header)?;
    Ok(&header == SQLITE_HEADER)
}

/// Canonicalize trusted ancestors such as macOS's /var alias, never the leaf.
fn checked_path(path: &Path) -> Result<PathBuf> {
    super::reject_symlink(path)?;
    let parent = path
        .parent()
        .context("Permanent archive path has no parent")?;
    let name = path
        .file_name()
        .context("Permanent archive path has no filename")?;
    let parent_metadata = fs::symlink_metadata(parent)?;
    ensure!(
        parent_metadata.is_dir() && !parent_metadata.file_type().is_symlink(),
        "Permanent archive parent must be a real directory"
    );
    Ok(parent.canonicalize()?.join(name))
}

/// Apply the key before any schema access, then enforce capture's durability.
pub(super) fn unlock(connection: Connection, key: &[u8; 32]) -> Result<Connection> {
    let connection = crate::archive::encryption::unlock(connection, key)?;
    connection.busy_timeout(Duration::from_secs(5))?;
    connection.execute_batch(
        "PRAGMA temp_store=MEMORY;
         PRAGMA synchronous=FULL;
         PRAGMA fullfsync=ON;
         PRAGMA checkpoint_fullfsync=ON;",
    )?;
    Ok(connection)
}

pub(super) fn read_only(path: &Path, key: &[u8; 32]) -> Result<Connection> {
    let path = checked_path(path)?;
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
    )?;
    let connection = unlock(connection, key)?;
    connection.pragma_update(None, "query_only", true)?;
    Ok(connection)
}

/// Migrate plaintext through a verified encrypted copy; never rekey in place.
pub(super) fn open(path: &Path, key: &[u8; 32]) -> Result<Connection> {
    let path = checked_path(path)?;
    let mut staging_name = path.as_os_str().to_owned();
    staging_name.push(".encrypting");
    let staging = PathBuf::from(staging_name);
    super::reject_symlink(&staging)?;
    let exists = path.try_exists()?;
    if !exists || fs::metadata(&path)?.len() == 0 {
        ensure!(
            !staging.try_exists()?,
            "A permanent archive staging file needs recovery; no database was created"
        );
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = path.as_os_str().to_owned();
            sidecar.push(suffix);
            match fs::symlink_metadata(PathBuf::from(sidecar)) {
                Ok(_) => anyhow::bail!(
                    "Permanent archive sidecars need recovery; no database was created"
                ),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }
    }
    if !exists {
        create_private_file(&path)?;
    } else if fs::metadata(&path)?.len() > 0 && plaintext_or_missing(&path)? {
        let source = Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        source.busy_timeout(Duration::from_secs(5))?;
        source.execute_batch(
            "PRAGMA temp_store=MEMORY;
             PRAGMA synchronous=FULL;
             PRAGMA fullfsync=ON;
             PRAGMA checkpoint_fullfsync=ON;",
        )?;
        // Fold in committed WAL pages before replacing the main database.
        let mode: String = source.query_row("PRAGMA journal_mode=DELETE", [], |row| row.get(0))?;
        ensure!(
            mode == "delete",
            "Close other archive readers before encryption"
        );
        if staging.try_exists()? {
            fs::remove_file(&staging)?;
        }
        create_private_file(&staging)?;
        source.execute(
            "ATTACH DATABASE ?1 AS encrypted KEY ?2",
            rusqlite::params![
                staging
                    .to_str()
                    .context("Permanent archive path is not UTF-8")?,
                &*crate::archive::encryption::key_literal(key)
            ],
        )?;
        source.execute_batch("PRAGMA encrypted.synchronous=FULL;")?;
        source.query_row("SELECT sqlcipher_export('encrypted')", [], |_| Ok(()))?;
        for pragma in ["user_version", "application_id"] {
            let value: i64 = source.pragma_query_value(None, pragma, |row| row.get(0))?;
            source.pragma_update(Some("encrypted"), pragma, value)?;
        }
        source.execute_batch("DETACH DATABASE encrypted")?;
        let verified = read_only(&staging, key)?;
        let integrity: String =
            verified.query_row("PRAGMA integrity_check", [], |row| row.get(0))?;
        ensure!(
            integrity == "ok",
            "Encrypted permanent archive failed its integrity check"
        );
        let original_counts = table_counts(&source)?;
        ensure!(
            table_counts(&verified)? == original_counts,
            "Encrypted permanent archive row counts do not match the original"
        );
        drop(verified);
        drop(source);
        durable_file(&OpenOptions::new().write(true).open(&staging)?)?;
        fs::rename(&staging, &path)
            .context("Could not replace the permanent archive with its encrypted copy")?;
        super::sync_directory(path.parent().expect("checked parent"))?;
        if let (Some(events), Some(media)) = (
            original_counts.get("events"),
            original_counts.get("media_jobs"),
        ) {
            log::info!(
                "Permanent archive migration verified: {events} records, {media} attachment jobs"
            );
        }
    }
    super::private_file(&path)?;
    let connection = unlock(
        Connection::open_with_flags(
            &path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?,
        key,
    )?;
    // A verified authoritative database makes an interrupted staging copy disposable.
    if staging.try_exists()? {
        fs::remove_file(staging)?;
        super::sync_directory(path.parent().expect("checked parent"))?;
    }
    Ok(connection)
}

/// Compare metadata counts only; no message fields enter logs or diagnostics.
fn table_counts(connection: &Connection) -> Result<std::collections::BTreeMap<String, i64>> {
    let names = connection
        .prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT GLOB 'sqlite_*' ORDER BY name",
        )?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    names
        .into_iter()
        .map(|name| {
            let quoted = name.replace('"', "\"\"");
            let count =
                connection.query_row(&format!("SELECT COUNT(*) FROM \"{quoted}\""), [], |row| {
                    row.get(0)
                })?;
            Ok((name, count))
        })
        .collect()
}

fn create_private_file(path: &Path) -> Result<()> {
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600).custom_flags(libc::O_NOFOLLOW);
    }
    durable_file(&options.open(path)?)?;
    super::sync_directory(path.parent().context("Missing database parent")?)
}

fn durable_file(file: &File) -> Result<()> {
    file.sync_all()?;
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == -1 {
            return Err(std::io::Error::last_os_error().into());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    const NAMESPACE: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn namespace_keys_survive_folder_moves_without_using_the_real_keyring() {
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let original = key_for(first.path(), NAMESPACE).unwrap();
        assert_eq!(**original, **key_for(second.path(), NAMESPACE).unwrap());
        assert_ne!(
            **original,
            **key_for(second.path(), &"b".repeat(64)).unwrap()
        );
        assert!(key_for(first.path(), "not-a-namespace").is_err());
    }

    #[test]
    fn missing_or_invalid_keys_never_replace_encrypted_capture_keys() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("journal.sqlite3");
        let store = keyring_core::mock::Store::new().unwrap();
        let entry = store
            .build(SERVICE, &key_identity(NAMESPACE).unwrap(), None)
            .unwrap();
        let key = key_from_entry(directory.path(), &entry).unwrap();
        assert_eq!(entry.get_secret().unwrap(), key.as_slice());
        assert_eq!(**key, **key_from_entry(directory.path(), &entry).unwrap());
        let connection = open(&path, &key).unwrap();
        connection
            .execute_batch(
                "CREATE TABLE fixture(value TEXT); INSERT INTO fixture VALUES ('retained');",
            )
            .unwrap();
        drop(connection);
        let before = fs::read(&path).unwrap();
        entry.delete_credential().unwrap();
        assert!(key_from_entry(directory.path(), &entry).is_err());
        assert!(matches!(
            entry.get_secret(),
            Err(keyring_core::Error::NoEntry)
        ));
        assert_eq!(fs::read(&path).unwrap(), before);
        entry.set_secret(&[1; 12]).unwrap();
        assert!(key_from_entry(directory.path(), &entry).is_err());
        assert_eq!(entry.get_secret().unwrap(), [1; 12]);
        assert_eq!(fs::read(path).unwrap(), before);
    }

    #[test]
    fn encrypted_media_prevents_replacement_when_the_journal_is_missing() {
        let directory = tempfile::tempdir().unwrap();
        fs::create_dir(directory.path().join("media")).unwrap();
        fs::write(
            directory.path().join("media/.wassup-media-index.sqlite"),
            b"encrypted fixture",
        )
        .unwrap();
        let store = keyring_core::mock::Store::new().unwrap();
        let entry = store
            .build(SERVICE, &key_identity(NAMESPACE).unwrap(), None)
            .unwrap();
        assert!(key_from_entry(directory.path(), &entry).is_err());
        assert!(matches!(
            entry.get_secret(),
            Err(keyring_core::Error::NoEntry)
        ));
    }

    #[test]
    fn orphaned_database_pages_are_never_replaced_by_a_new_key_or_empty_database() {
        for name in ["journal.sqlite3-wal", "journal.sqlite3.encrypting"] {
            let directory = tempfile::tempdir().unwrap();
            let orphan = directory.path().join(name);
            fs::write(&orphan, b"retained encrypted database pages").unwrap();
            let store = keyring_core::mock::Store::new().unwrap();
            let entry = store
                .build(SERVICE, &key_identity(NAMESPACE).unwrap(), None)
                .unwrap();
            assert!(key_from_entry(directory.path(), &entry).is_err());
            assert!(matches!(
                entry.get_secret(),
                Err(keyring_core::Error::NoEntry)
            ));
            let path = directory.path().join("journal.sqlite3");
            assert!(open(&path, &[1; 32]).is_err());
            assert!(!path.exists());
            fs::write(&path, []).unwrap();
            assert!(open(&path, &[1; 32]).is_err());
            assert_eq!(fs::metadata(path).unwrap().len(), 0);
            assert_eq!(
                fs::read(orphan).unwrap(),
                b"retained encrypted database pages"
            );
        }
    }

    #[test]
    fn plaintext_wal_migration_retains_rows_schema_and_immutable_triggers() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("journal.sqlite3");
        let source = Connection::open(&path).unwrap();
        source.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA user_version=4; CREATE TABLE events(id INTEGER PRIMARY KEY,value TEXT); INSERT INTO events VALUES (1,'retained plaintext fixture'); CREATE TRIGGER immutable_events BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'immutable'); END;").unwrap();
        source
            .execute_batch(
                r#"CREATE TABLE media_jobs(id INTEGER); INSERT INTO media_jobs VALUES(1),(2);
               CREATE TABLE dirty_pages(version INTEGER); INSERT INTO dirty_pages VALUES(3);
               CREATE TABLE "other""table"(value INTEGER); INSERT INTO "other""table" VALUES(4);"#,
            )
            .unwrap();
        source
            .set_db_config(
                rusqlite::config::DbConfig::SQLITE_DBCONFIG_NO_CKPT_ON_CLOSE,
                true,
            )
            .unwrap();
        drop(source);
        assert!(directory.path().join("journal.sqlite3-wal").exists());
        let connection = open(&path, &[7; 32]).unwrap();
        assert_eq!(
            connection
                .query_row("SELECT value FROM events", [], |row| row
                    .get::<_, String>(0))
                .unwrap(),
            "retained plaintext fixture"
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "user_version", |row| row.get::<_, i64>(0))
                .unwrap(),
            4
        );
        assert!(connection.execute("DELETE FROM events", []).is_err());
        assert_eq!(
            table_counts(&connection).unwrap(),
            [
                ("dirty_pages".to_owned(), 1),
                ("events".to_owned(), 1),
                ("media_jobs".to_owned(), 2),
                ("other\"table".to_owned(), 1),
            ]
            .into_iter()
            .collect()
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "synchronous", |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        assert_eq!(
            connection
                .pragma_query_value(None, "temp_store", |row| row.get::<_, i64>(0))
                .unwrap(),
            2
        );
        drop(connection);
        assert!(!plaintext_or_missing(&path).unwrap());
        assert!(
            !fs::read(&path)
                .unwrap()
                .windows(26)
                .any(|bytes| bytes == b"retained plaintext fixture")
        );
        assert!(read_only(&path, &[8; 32]).is_err());
        let reader = read_only(&path, &[7; 32]).unwrap();
        assert_eq!(
            reader
                .query_row("SELECT count(*) FROM events", [], |row| row
                    .get::<_, i64>(0))
                .unwrap(),
            1
        );
    }

    #[cfg(unix)]
    #[test]
    fn symlink_database_and_staging_targets_are_refused() {
        use std::os::unix::fs::symlink;
        let directory = tempfile::tempdir().unwrap();
        let original = directory.path().join("original");
        fs::write(&original, b"untouched").unwrap();
        let path = directory.path().join("journal.sqlite3");
        symlink(&original, &path).unwrap();
        assert!(open(&path, &[1; 32]).is_err());
        assert!(read_only(&path, &[1; 32]).is_err());
        fs::remove_file(&path).unwrap();
        symlink(&original, path.with_extension("sqlite3.encrypting")).unwrap();
        assert!(open(&path, &[1; 32]).is_err());
        assert!(!path.exists());
        assert_eq!(fs::read(original).unwrap(), b"untouched");
    }
}
