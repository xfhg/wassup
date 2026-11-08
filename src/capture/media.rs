//! Durable attachment capture, independent of the mutable chat archive.
//!
//! The receive hook only queues jobs. Two workers stream immutable descriptors
//! to encrypted temporary containers, authenticate them, and publish complete files.

use super::{
    Capture, MediaJob,
    media_store::{ChunkFile, Entry, INDEX_NAME, Registry, io_error},
};
use anyhow::{Context, Result, ensure};
use sha2::{Digest, Sha256};
use std::fs::{self, File};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, watch};
use tokio::task::JoinHandle;
use whatsapp_rust::Client;
use whatsapp_rust::download::DownloadLimits;
use whatsapp_rust::prelude::{MessageExt, wa};
use whatsapp_rust::wacore::download::{DownloadWriter, Downloadable};
use whatsapp_rust::waproto::buffa::Message as _;

pub const MAX_FILE_BYTES: u64 = 2_000_000_000;
const FREE_RESERVE: u64 = 1_000_000_000;
const MAX_ATTEMPTS: u32 = 5;
const OVERSIZED: &str = "Attachment exceeds the 2 GB capture limit";
const LOW_SPACE: &str = "Attachment capture paused: less than 1 GB free space would remain";
const CANCELLED: &str = "Attachment capture paused";
static DISK_WRITE: std::sync::Mutex<()> = std::sync::Mutex::new(());
static MEDIA_SLOTS: std::sync::LazyLock<Arc<Semaphore>> =
    std::sync::LazyLock::new(|| Arc::new(Semaphore::new(2)));

/// Dropping a connection's handle stops its workers. Jobs remain on disk.
pub struct MediaWorker {
    cancelled: Arc<AtomicBool>,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl MediaWorker {
    pub async fn stop(mut self) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.stop.send(true);
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
    }
}

impl Drop for MediaWorker {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
        let _ = self.stop.send(true);
        // Do not abort: workers must durably return their claimed jobs to paused.
    }
}

pub fn start(capture: Arc<Capture>, client: Arc<Client>) -> MediaWorker {
    let cancelled = Arc::new(AtomicBool::new(false));
    let (stop, receiver) = watch::channel(false);
    let cancel = cancelled.clone();
    let tasks = vec![tokio::spawn(async move {
        let directory = capture.root().join("media");
        let key = Arc::new(zeroize::Zeroizing::new(*capture.key()));
        // Only this archive's private staging files are disposable. Published
        // files survive even when a crash prevented the saved-state update.
        let _ = tokio::task::spawn_blocking(move || discard_staging(&directory, &key)).await;
        tokio::join!(
            run(
                capture.clone(),
                client.clone(),
                cancel.clone(),
                receiver.clone()
            ),
            run(capture, client, cancel, receiver),
        );
    })];
    MediaWorker {
        cancelled,
        stop,
        tasks,
    }
}

async fn run(
    capture: Arc<Capture>,
    client: Arc<Client>,
    cancelled: Arc<AtomicBool>,
    mut stop: watch::Receiver<bool>,
) {
    while !cancelled.load(Ordering::Acquire) {
        if !client.is_socket_connected() {
            tokio::select! {
                _ = stop.changed() => break,
                _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
            }
        }
        // Shared across accounts and reconnecting clients. The writer owns the
        // permit during HTTP work, including a cancelled blocking operation.
        let permit = tokio::select! {
            _ = stop.changed() => break,
            permit = MEDIA_SLOTS.clone().acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => break,
            },
        };
        let store = capture.clone();
        let job = tokio::task::spawn_blocking(move || store.claim_media_job()).await;
        let job = match job {
            Ok(Ok(Some(job))) => job,
            result => {
                drop(permit);
                if !matches!(result, Ok(Ok(None))) {
                    capture.write_failed();
                }
                tokio::select! {
                    _ = stop.changed() => break,
                    _ = tokio::time::sleep(Duration::from_secs(1)) => continue,
                }
            }
        };
        let mut outcome = if cancelled.load(Ordering::Acquire) {
            Outcome::Paused(CANCELLED)
        } else {
            tokio::select! {
                _ = stop.changed() => Outcome::Paused(CANCELLED),
                result = download(&capture, &client, &job, cancelled.clone(), permit) => result,
            }
        };
        if matches!(outcome, Outcome::Failed(_)) && !client.is_socket_connected() {
            outcome = Outcome::Paused("Attachment capture paused while disconnected");
        }
        loop {
            let store = capture.clone();
            let job = job.clone();
            let result = outcome.clone();
            let saved = tokio::task::spawn_blocking(move || finish(&store, &job, result)).await;
            if matches!(saved, Ok(Ok(()))) {
                break;
            }
            capture.write_failed();
            // Keep the completion in memory while retrying its durable update.
            // On shutdown, restart recovery reclaims the persisted job.
            if cancelled.load(Ordering::Acquire) {
                break;
            }
            tokio::select! {
                _ = stop.changed() => break,
                _ = tokio::time::sleep(Duration::from_secs(1)) => {},
            }
        }
    }
}

fn finish(store: &Capture, job: &MediaJob, outcome: Outcome) -> Result<()> {
    let now = crate::util::now();
    match outcome {
        Outcome::Saved(path) => store.finish_media_job(job.id, "saved", Some(&path), None, None),
        Outcome::Oversized => {
            store.finish_media_job(job.id, "oversized", None, Some(OVERSIZED), None)
        }
        Outcome::Paused(reason) => {
            store.finish_media_job(job.id, "paused", None, Some(reason), Some(now + 60))
        }
        Outcome::Failed(reason) => {
            let retry = retry_delay(job.attempts).map(|delay| now + delay);
            store.finish_media_job(job.id, "failed", None, Some(reason), retry)
        }
    }
}

fn retry_delay(attempts: u32) -> Option<i64> {
    if attempts >= MAX_ATTEMPTS {
        return None;
    }
    Some([5, 30, 120, 600][attempts.saturating_sub(1).min(3) as usize])
}

#[derive(Clone)]
enum Outcome {
    Saved(PathBuf),
    Oversized,
    Paused(&'static str),
    Failed(&'static str),
}

struct Descriptor {
    media: Box<dyn Downloadable>,
    extension: &'static str,
}

/// Used while committing a receipt and its durable media job together.
pub fn has_media(raw: &[u8]) -> bool {
    wa::Message::decode_from_slice(raw)
        .ok()
        .and_then(|message| descriptor(&message))
        .is_some()
}

fn descriptor(message: &wa::Message) -> Option<Descriptor> {
    if message.is_view_once() {
        return None;
    }
    let base = message.get_base_message();
    if let Some(edited) = base
        .protocol_message
        .as_option()
        .and_then(|p| p.edited_message.as_option())
    {
        return descriptor(edited);
    }
    let (media, mime): (Box<dyn Downloadable>, &str) =
        if let Some(m) = base.image_message.as_option() {
            (
                Box::new(m.clone()),
                m.mimetype.as_deref().unwrap_or("image/jpeg"),
            )
        } else if let Some(m) = base
            .video_message
            .as_option()
            .or(base.ptv_message.as_option())
        {
            (
                Box::new(m.clone()),
                m.mimetype.as_deref().unwrap_or("video/mp4"),
            )
        } else if let Some(m) = base.audio_message.as_option() {
            (
                Box::new(m.clone()),
                m.mimetype.as_deref().unwrap_or("audio/ogg"),
            )
        } else if let Some(m) = base.document_message.as_option() {
            (
                Box::new(m.clone()),
                m.mimetype.as_deref().unwrap_or("application/octet-stream"),
            )
        } else if let Some(m) = base.sticker_message.as_option() {
            (
                Box::new(m.clone()),
                m.mimetype.as_deref().unwrap_or("image/webp"),
            )
        } else {
            let m = base
                .lottie_sticker_message
                .as_option()
                .and_then(|wrapper| wrapper.message.as_option())
                .and_then(|inner| inner.sticker_message.as_option())?;
            return Some(Descriptor {
                media: Box::new(m.clone()),
                extension: "was",
            });
        };
    Some(Descriptor {
        media,
        extension: extension(mime),
    })
}

fn extension(mime: &str) -> &'static str {
    match mime.split(';').next().unwrap_or("").trim() {
        "image/jpeg" => "jpg",
        "image/png" => "png",
        "image/webp" => "webp",
        "image/gif" => "gif",
        "image/heic" => "heic",
        "video/mp4" => "mp4",
        "video/quicktime" => "mov",
        "audio/ogg" => "ogg",
        "audio/mpeg" => "mp3",
        "audio/mp4" => "m4a",
        "audio/aac" => "aac",
        "audio/wav" | "audio/x-wav" => "wav",
        "application/pdf" => "pdf",
        "text/plain" => "txt",
        "text/csv" => "csv",
        "application/zip" => "zip",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "docx",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "xlsx",
        "application/vnd.openxmlformats-officedocument.presentationml.presentation" => "pptx",
        _ => "bin",
    }
}

async fn download(
    capture: &Capture,
    client: &Arc<Client>,
    job: &MediaJob,
    cancelled: Arc<AtomicBool>,
    permit: OwnedSemaphorePermit,
) -> Outcome {
    let Some(item) = wa::Message::decode_from_slice(&job.raw)
        .ok()
        .and_then(|m| descriptor(&m))
    else {
        return Outcome::Failed("Attachment descriptor is unavailable or excluded");
    };
    let size = item.media.file_length();
    if size.is_some_and(|n| n > MAX_FILE_BYTES) {
        return Outcome::Oversized;
    }
    let relative = PathBuf::from("media").join(format!("{}.{ext}", job.id, ext = item.extension));
    let target = capture.root().join(&relative);
    let expected_hash = item.media.file_sha256().map(<[u8]>::to_vec);
    let published = target.clone();
    let key = Arc::new(zeroize::Zeroizing::new(*capture.key()));
    let recovery_key = key.clone();
    match tokio::task::spawn_blocking(move || {
        recover_published(&published, &recovery_key, expected_hash.as_deref(), size)
    })
    .await
    {
        Ok(Ok(true)) => return Outcome::Saved(relative),
        Ok(Ok(false)) => {}
        _ => return Outcome::Failed("Existing attachment file failed integrity verification"),
    }
    let root = capture.root().to_owned();
    let cancel = cancelled.clone();
    let prepared = tokio::task::spawn_blocking(move || {
        let mut writer = DiskWriter::new(&root, &key, size, cancel)?;
        writer.transfer_permit = Some(permit);
        Ok::<_, io::Error>(writer)
    })
    .await;
    let writer = match prepared {
        Ok(Ok(writer)) => writer,
        Ok(Err(error)) if error.to_string() == LOW_SPACE => return Outcome::Paused(LOW_SPACE),
        _ => return Outcome::Failed("Cannot create an attachment file in the capture folder"),
    };
    // Budgets include failed CDN attempts. The writer independently caps the
    // final file; bounded SDK mode refuses a buffered HTTP implementation.
    let limits = DownloadLimits::new(3 * (MAX_FILE_BYTES + 1_048_576), 3 * MAX_FILE_BYTES);
    let deadline = Duration::from_secs(120 + size.unwrap_or(MAX_FILE_BYTES) / (128 * 1024));
    let result = tokio::time::timeout(
        deadline,
        client.download_to_writer(&*item.media, writer, limits),
    )
    .await;
    let writer = match result {
        Ok(Ok(writer)) => writer,
        Ok(Err(error)) => {
            // Never persist SDK error text: it can contain signed CDN URLs.
            if cancelled.load(Ordering::Acquire) {
                return Outcome::Paused(CANCELLED);
            }
            if has_cause(&error, LOW_SPACE) {
                return Outcome::Paused(LOW_SPACE);
            }
            if has_cause(&error, OVERSIZED) {
                return Outcome::Oversized;
            }
            return Outcome::Failed("Attachment download or integrity verification failed");
        }
        Err(_) => return Outcome::Failed("Attachment download timed out"),
    };
    match tokio::task::spawn_blocking(move || writer.publish(&target)).await {
        Ok(Ok(())) => Outcome::Saved(relative),
        _ => Outcome::Failed("Cannot durably publish the completed attachment"),
    }
}

/// Recover the publish-before-status crash window without needing the CDN.
fn recover_published(
    target: &Path,
    key: &[u8; 32],
    expected_hash: Option<&[u8]>,
    size: Option<u64>,
) -> Result<bool> {
    let metadata = match fs::symlink_metadata(target) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    let parent = target.parent().context("Missing attachment directory")?;
    let directory = fs::symlink_metadata(parent)?;
    ensure!(
        directory.is_dir() && !directory.file_type().is_symlink(),
        "Invalid attachment directory"
    );
    ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "Invalid attachment file"
    );
    let mut encrypted = ChunkFile::open(target, key)?;
    ensure!(
        encrypted.len() <= MAX_FILE_BYTES && size.is_none_or(|size| encrypted.len() == size),
        "Attachment length mismatch"
    );
    let Some(expected) = expected_hash.filter(|hash| hash.len() == 32) else {
        return Ok(false);
    };
    ensure!(
        encrypted.verify()?.as_slice() == expected,
        "Attachment hash mismatch"
    );
    durable_file(&File::open(target)?)?;
    File::open(parent)?.sync_all()?;
    Ok(true)
}

fn has_cause(error: &(dyn std::error::Error + 'static), marker: &str) -> bool {
    let mut source = Some(error);
    while let Some(error) = source {
        if error.to_string().contains(marker) {
            return true;
        }
        source = error.source();
    }
    false
}

fn discard_staging(directory: &Path, key: &[u8; 32]) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(());
    }
    let registry = Registry::open(directory, key).map_err(io_error)?;
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let pid = name
            .strip_prefix(".wassup-encrypted-")
            .and_then(|s| s.split('-').next())
            .and_then(|s| s.parse::<u32>().ok());
        let path = entry.path();
        if pid.is_some_and(|pid| !process_running(pid))
            && entry.file_type()?.is_file()
            && registry.is_staging(&path).map_err(io_error)?
            && matches!(
                registry.entry(&path).map_err(io_error)?,
                Entry::Unregistered
            )
        {
            // Ownership of this exact random name was recorded while empty,
            // before encrypted writes. Prefix matching alone is insufficient.
            fs::remove_file(&path)?;
            File::open(directory)?.sync_all()?;
            registry.forget_staging(&path).map_err(io_error)?;
        }
    }
    Ok(())
}

#[cfg(unix)]
fn process_running(pid: u32) -> bool {
    if pid == 0 || pid > i32::MAX as u32 {
        return true;
    }
    (unsafe { libc::kill(pid as libc::pid_t, 0) == 0 })
        || io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
}

#[cfg(not(unix))]
fn process_running(_: u32) -> bool {
    true
}

/// Call on a blocking task, before uploading/sending an ordinary attachment.
/// The returned file is owned by the capture archive and survives source edits.
pub fn copy_outgoing(capture: &Capture, source: &Path) -> Result<PathBuf> {
    copy_to(capture.root(), capture.key(), source)
}

fn copy_to(root: &Path, key: &[u8; 32], source: &Path) -> Result<PathBuf> {
    let mut input = File::open(source).context("Cannot open outgoing attachment")?;
    let metadata = input.metadata()?;
    ensure!(
        metadata.is_file(),
        "Outgoing attachment must be a regular file"
    );
    ensure!(metadata.len() <= MAX_FILE_BYTES, OVERSIZED);
    let mut writer = DiskWriter::new(
        root,
        key,
        Some(metadata.len()),
        Arc::new(AtomicBool::new(false)),
    )?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = input.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        writer.write_all(&buffer[..read])?;
        digest.update(&buffer[..read]);
    }
    let suffix = source
        .extension()
        .and_then(|s| s.to_str())
        .filter(|s| !s.is_empty() && s.len() <= 12 && s.bytes().all(|b| b.is_ascii_alphanumeric()))
        .unwrap_or("bin")
        .to_ascii_lowercase();
    let hash: String = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let target = root.join("media").join(format!("out-{hash}.{suffix}"));
    writer.publish(&target)?;
    Ok(target)
}

struct DiskWriter {
    // Close SQLite (and its encrypted rollback journal) before removing the file.
    chunks: Option<ChunkFile>,
    file: tempfile::NamedTempFile,
    directory: PathBuf,
    key: super::storage::Key,
    cancelled: Arc<AtomicBool>,
    transfer_permit: Option<OwnedSemaphorePermit>,
}

impl DiskWriter {
    fn new(
        root: &Path,
        key: &[u8; 32],
        expected: Option<u64>,
        cancelled: Arc<AtomicBool>,
    ) -> io::Result<Self> {
        if expected.is_some_and(|size| size > MAX_FILE_BYTES) {
            return Err(io::Error::other(OVERSIZED));
        }
        let directory = root.join("media");
        super::private_dir(&directory).map_err(io_error)?;
        let directory = fs::canonicalize(directory)?;
        // The registry is a durable marker for missing-key detection and crash
        // recovery. It must precede the first encrypted staging file.
        Registry::open(&directory, key).map_err(io_error)?;
        reserve_space(&directory, encrypted_budget(expected.unwrap_or(1_048_576)))?;
        let file = tempfile::Builder::new()
            .prefix(&format!(".wassup-encrypted-{}-", std::process::id()))
            .tempfile_in(&directory)?;
        let registry = Registry::open(&directory, key).map_err(io_error)?;
        registry.register_staging(file.path()).map_err(io_error)?;
        registry
            .register_staging(&rollback_path(file.path()))
            .map_err(io_error)?;
        let chunks = ChunkFile::create(file.path(), key).map_err(io_error)?;
        Ok(Self {
            chunks: Some(chunks),
            file,
            directory,
            key: Arc::new(zeroize::Zeroizing::new(*key)),
            cancelled,
            transfer_permit: None,
        })
    }

    fn complete(&mut self) -> Result<(u64, [u8; 32])> {
        ensure!(!self.cancelled.load(Ordering::Acquire), CANCELLED);
        let expected = self
            .chunks
            .take()
            .context("Attachment already completed")?
            .commit()?;
        durable_file(self.file.as_file())?;
        let mut check = ChunkFile::open(self.file.path(), &self.key)?;
        ensure!(
            check.len() == expected.0 && check.verify()? == expected.1,
            "Completed attachment failed verification"
        );
        Ok(expected)
    }

    fn publish(mut self, target: &Path) -> Result<()> {
        let (length, hash) = self.complete()?;
        ensure!(!self.cancelled.load(Ordering::Acquire), CANCELLED);
        let registry = Registry::open(&self.directory, &self.key)?;
        registry.register(target)?;
        let staging = self.file.path().to_owned();
        match self.file.persist_noclobber(target) {
            Ok(file) => durable_file(&file)?,
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                // Verify decrypted bytes: SQLCipher uses a fresh random salt,
                // so identical attachments deliberately have different files.
                let metadata = fs::symlink_metadata(target)?;
                ensure!(
                    metadata.is_file() && !metadata.file_type().is_symlink(),
                    "Attachment destination is not a regular file"
                );
                let mut existing = ChunkFile::open(target, &self.key)?;
                ensure!(
                    existing.len() == length && existing.verify()? == hash,
                    "Attachment destination conflicts with saved bytes"
                );
                durable_file(&File::open(target)?)?;
            }
            Err(error) => return Err(error.error.into()),
        }
        File::open(&self.directory)?.sync_all()?;
        registry.forget_staging(&staging)?;
        registry.forget_staging(&rollback_path(&staging))?;
        Ok(())
    }
}

impl Write for DiskWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::other(CANCELLED));
        }
        let chunks = self
            .chunks
            .as_mut()
            .ok_or_else(|| io::Error::other("Attachment completed"))?;
        let end = chunks
            .stream_position()?
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other(OVERSIZED))?;
        if end > MAX_FILE_BYTES {
            return Err(io::Error::other(OVERSIZED));
        }
        let _disk = DISK_WRITE
            .lock()
            .map_err(|_| io::Error::other("Capture disk guard failed"))?;
        // Include encrypted pages, B-tree overhead and rollback pages. This is
        // checked on every write, including rewrites during SDK retry.
        reserve_space(&self.directory, encrypted_budget(bytes.len() as u64))?;
        chunks.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl Seek for DiskWriter {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.chunks
            .as_mut()
            .ok_or_else(|| io::Error::other("Attachment completed"))?
            .seek(position)
    }
}

impl DownloadWriter for DiskWriter {
    fn truncate(&mut self, len: u64) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::other(CANCELLED));
        }
        if len > MAX_FILE_BYTES {
            return Err(io::Error::other(OVERSIZED));
        }
        self.chunks
            .as_mut()
            .ok_or_else(|| io::Error::other("Attachment completed"))?
            .truncate(len)
    }
}

fn encrypted_budget(bytes: u64) -> u64 {
    bytes.saturating_mul(3).saturating_add(262_144)
}

fn rollback_path(path: &Path) -> PathBuf {
    let mut sidecar = path.as_os_str().to_owned();
    sidecar.push("-journal");
    PathBuf::from(sidecar)
}

fn hash_file(path: &Path) -> Result<[u8; 32]> {
    super::media_store::digest_reader(&mut File::open(path)?)
}

/// Produce plaintext only for an explicitly requested export destination.
pub fn export_file(capture: &Capture, source: &Path, destination: &Path) -> Result<()> {
    export_file_cancellable(capture, source, destination, &AtomicBool::new(false))
}

pub fn export_file_cancellable(
    capture: &Capture,
    source: &Path,
    destination: &Path,
    cancelled: &AtomicBool,
) -> Result<()> {
    let relative = super::relative_media_path(capture.root(), source)?;
    let source = capture.root().join(relative);
    export_to(&source, capture.key(), destination, cancelled)
}

fn export_to(
    source: &Path,
    key: &[u8; 32],
    destination: &Path,
    cancelled: &AtomicBool,
) -> Result<()> {
    ensure!(
        !cancelled.load(Ordering::Acquire),
        "Attachment export cancelled"
    );
    super::reject_symlink(source)?;
    super::reject_symlink(destination)?;
    ensure!(
        !destination.try_exists()?,
        "Export destination already exists"
    );
    let parent = destination
        .parent()
        .context("Export destination has no directory")?;
    let metadata = fs::symlink_metadata(parent)?;
    ensure!(
        metadata.is_dir() && !metadata.file_type().is_symlink(),
        "Invalid export directory"
    );
    let mut input = ChunkFile::open(source, key)?;
    ensure!(input.len() <= MAX_FILE_BYTES, OVERSIZED);
    reserve_space(parent, input.len())?;
    let expected = input.expected_digest()?;
    let mut output = tempfile::Builder::new()
        .prefix(".wassup-export-")
        .tempfile_in(parent)?;
    let mut hash = Sha256::new();
    let mut buffer = [0; 64 * 1024];
    loop {
        ensure!(
            !cancelled.load(Ordering::Acquire),
            "Attachment export cancelled"
        );
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let _disk = DISK_WRITE
            .lock()
            .map_err(|_| anyhow::anyhow!("Capture disk guard failed"))?;
        reserve_space(parent, count as u64)?;
        output.write_all(&buffer[..count])?;
        hash.update(&buffer[..count]);
    }
    ensure!(
        hash.finalize().as_slice() == expected,
        "Attachment export integrity check failed"
    );
    output.flush()?;
    durable_file(output.as_file())?;
    ensure!(
        !cancelled.load(Ordering::Acquire),
        "Attachment export cancelled"
    );
    output
        .persist_noclobber(destination)
        .map_err(|error| error.error)?;
    File::open(parent)?.sync_all()?;
    Ok(())
}

/// Upgrade legacy published attachments before capture workers start. A keyed
/// manifest records the exact old bytes before their verified atomic replacement.
pub fn migrate_existing(capture: &Capture) -> Result<()> {
    migrate_directory(capture.root(), capture.key())
}

fn migrate_directory(root: &Path, key: &[u8; 32]) -> Result<()> {
    let directory = root.join("media");
    super::private_dir(&directory)?;
    let directory = fs::canonicalize(directory)?;
    let registry = Registry::open(&directory, key)?;
    // Do not recurse into, modify or delete unexpected directories/symlinks.
    // Failing closed is preferable to claiming their contents are encrypted.
    for item in fs::read_dir(&directory)? {
        let item = item?;
        let name = item.file_name();
        let name = name.to_string_lossy();
        if name == INDEX_NAME
            || ["-journal", "-wal", "-shm"]
                .iter()
                .any(|suffix| name == format!("{INDEX_NAME}{suffix}"))
        {
            continue;
        }
        let kind = item.file_type()?;
        ensure!(
            kind.is_file() && !kind.is_symlink(),
            "Unexpected entry in attachment directory"
        );
        if name.starts_with(".wassup-encrypted-")
            && registry.is_staging(&item.path())?
            && matches!(registry.entry(&item.path())?, Entry::Unregistered)
        {
            // Only this version's encrypted, unpublished staging is disposable.
            continue;
        }
        let legacy_pid = name
            .strip_prefix(".incoming-")
            .and_then(|rest| rest.split('-').next())
            .and_then(|value| value.parse::<u32>().ok());
        ensure!(
            !legacy_pid.is_some_and(process_running),
            "A legacy attachment download is still active"
        );
        let path = item.path();
        super::private_file(&path)?;
        match registry.entry(&path)? {
            Entry::Encrypted => {
                // Pages are authenticated when read. The committed registry
                // makes rehashing every large attachment on reconnect unnecessary.
                ChunkFile::open(&path, key)?;
                continue;
            }
            Entry::Legacy { length, hash } => {
                // A crash after replacement but before this marker update is
                // recovered by authenticated reads, never by filename guesses.
                if let Ok(mut file) = ChunkFile::open(&path, key) {
                    ensure!(
                        file.len() == length && file.verify()?.as_slice() == hash,
                        "Migrated attachment differs from the original"
                    );
                    registry.migrated(&path)?;
                    continue;
                }
                ensure!(
                    fs::metadata(&path)?.len() == length && hash_file(&path)?.as_slice() == hash,
                    "Pending attachment migration no longer matches its original"
                );
                migrate_one(root, key, &path, length, &hash)?;
            }
            Entry::Unregistered => {
                // A separately restored attachment can outlive its registry.
                // Authenticate a complete existing container before treating
                // unknown bytes as a legacy plaintext file.
                if let Ok(mut existing) = ChunkFile::open(&path, key) {
                    existing.verify()?;
                    registry.register(&path)?;
                    continue;
                }
                let length = item.metadata()?.len();
                ensure!(length <= MAX_FILE_BYTES, OVERSIZED);
                let hash = hash_file(&path)?;
                registry.legacy(&path, length, &hash)?;
                migrate_one(root, key, &path, length, &hash)?;
            }
        }
        registry.migrated(&path)?;
    }
    Ok(())
}

fn migrate_one(
    root: &Path,
    key: &[u8; 32],
    source: &Path,
    length: u64,
    expected: &[u8],
) -> Result<()> {
    ensure!(length <= MAX_FILE_BYTES, OVERSIZED);
    let mut writer = DiskWriter::new(root, key, Some(length), Arc::new(AtomicBool::new(false)))?;
    let mut input = File::open(source)?;
    let mut buffer = [0; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        writer.write_all(&buffer[..count])?;
    }
    let (actual_length, actual_hash) = writer.complete()?;
    ensure!(
        actual_length == length && actual_hash.as_slice() == expected,
        "Original attachment changed during encryption"
    );
    // Verify the source again immediately before replacement; no original is
    // removed before its encrypted successor is complete and authenticated.
    super::reject_symlink(source)?;
    ensure!(
        fs::metadata(source)?.len() == length && hash_file(source)?.as_slice() == expected,
        "Original attachment changed during encryption"
    );
    let staging = writer.file.path().to_owned();
    writer.file.persist(source).map_err(|error| error.error)?;
    File::open(&writer.directory)?.sync_all()?;
    let registry = Registry::open(&writer.directory, key)?;
    registry.forget_staging(&staging)?;
    registry.forget_staging(&rollback_path(&staging))?;
    Ok(())
}

fn durable_file(file: &File) -> io::Result<()> {
    file.sync_all()?;
    #[cfg(target_os = "macos")]
    {
        use std::os::fd::AsRawFd;
        // fsync alone need not flush a drive's volatile cache on macOS.
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) } == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

fn reserve_space(path: &Path, additional: u64) -> io::Result<()> {
    check_reserve(available_space(path)?, additional)
}

fn check_reserve(available: u64, additional: u64) -> io::Result<()> {
    if available < FREE_RESERVE.saturating_add(additional) {
        return Err(io::Error::other(LOW_SPACE));
    }
    Ok(())
}

#[cfg(unix)]
fn available_space(path: &Path) -> io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|_| io::Error::other("Invalid capture directory"))?;
    let mut stat = std::mem::MaybeUninit::<libc::statvfs>::uninit();
    if unsafe { libc::statvfs(path.as_ptr(), stat.as_mut_ptr()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let stat = unsafe { stat.assume_init() };
    // libc uses different integer widths on Unix targets. Widen first, then
    // saturate the product without truncating either platform's counters.
    let bytes = u128::from(stat.f_bavail) * u128::from(stat.f_frsize);
    Ok(u64::try_from(bytes).unwrap_or(u64::MAX))
}

#[cfg(not(unix))]
fn available_space(_: &Path) -> io::Result<u64> {
    Err(io::Error::other(
        "Capture free-space checks are not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use whatsapp_rust::prelude::MessageField;
    const KEY: [u8; 32] = [37; 32];

    #[test]
    fn ordinary_media_is_queued_but_view_once_is_not() {
        let mut message = wa::Message::default();
        let mut image = wa::message::ImageMessage::default();
        image.mimetype = Some("image/jpeg".into());
        message.image_message = MessageField::some(image);
        assert!(has_media(&message.encode_to_vec()));
        let mut wrapper = wa::message::FutureProofMessage::default();
        wrapper.message = MessageField::some(message);
        let mut once = wa::Message::default();
        once.view_once_message_v2 = MessageField::some(wrapper);
        assert!(!has_media(&once.encode_to_vec()));
        assert!(!has_media(b"invalid"));
    }

    #[test]
    fn retries_are_bounded_and_increase() {
        assert_eq!(
            (1..=5).map(retry_delay).collect::<Vec<_>>(),
            vec![Some(5), Some(30), Some(120), Some(600), None]
        );
    }

    #[test]
    fn free_space_guard_preserves_a_gigabyte_and_refuses_unknown_capacity() {
        assert!(check_reserve(FREE_RESERVE + 50, 51).is_err());
        assert!(check_reserve(FREE_RESERVE + 50, 50).is_ok());
        assert!(check_reserve(0, 0).is_err());
        assert!(check_reserve(FREE_RESERVE, u64::MAX).is_err());
    }

    #[tokio::test]
    async fn a_cancelled_transfers_writer_retains_its_global_slot_until_drop() {
        let first = MEDIA_SLOTS.clone().acquire_owned().await.unwrap();
        let second = MEDIA_SLOTS.clone().acquire_owned().await.unwrap();
        let root = tempfile::tempdir().unwrap();
        let mut writer =
            DiskWriter::new(root.path(), &KEY, Some(1), Arc::new(AtomicBool::new(true))).unwrap();
        writer.transfer_permit = Some(first);
        assert!(MEDIA_SLOTS.clone().try_acquire_owned().is_err());
        assert!(writer.write(b"x").is_err());
        assert!(MEDIA_SLOTS.clone().try_acquire_owned().is_err());
        drop(writer);
        assert!(MEDIA_SLOTS.clone().try_acquire_owned().is_ok());
        drop(second);
    }

    #[test]
    fn copy_preserves_bytes_and_reuses_only_identical_files() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("fixture.txt");
        fs::write(&source, b"synthetic attachment").unwrap();
        let saved = copy_to(root.path(), &KEY, &source).unwrap();
        let mut decoded = ChunkFile::open(&saved, &KEY).unwrap();
        let mut bytes = Vec::new();
        decoded.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"synthetic attachment");
        assert!(
            !fs::read(&saved)
                .unwrap()
                .windows(20)
                .any(|part| part == b"synthetic attachment")
        );
        drop(decoded);
        assert_eq!(copy_to(root.path(), &KEY, &source).unwrap(), saved);
        fs::write(&saved, b"different bytes").unwrap();
        assert!(copy_to(root.path(), &KEY, &source).is_err());
    }

    #[test]
    fn published_file_recovers_after_a_crash_only_when_descriptor_matches() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("fixture.txt");
        fs::write(&source, b"synthetic attachment").unwrap();
        let target = copy_to(root.path(), &KEY, &source).unwrap();
        let hash = ChunkFile::open(&target, &KEY).unwrap().verify().unwrap();
        assert!(recover_published(&target, &KEY, Some(&hash), Some(20)).unwrap());
        assert!(!recover_published(&target, &KEY, None, Some(20)).unwrap());
        assert!(recover_published(&target, &KEY, Some(&hash), Some(19)).is_err());
        assert!(recover_published(&target, &KEY, Some(&[0; 32]), Some(20)).is_err());
    }

    #[test]
    fn writer_refuses_oversize_cancellation_and_symlink_directory() {
        let root = tempfile::tempdir().unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut writer = DiskWriter::new(root.path(), &KEY, Some(1), cancelled.clone()).unwrap();
        writer.seek(SeekFrom::Start(MAX_FILE_BYTES)).unwrap();
        assert!(writer.write(b"x").is_err());
        writer.seek(SeekFrom::Start(0)).unwrap();
        cancelled.store(true, Ordering::Release);
        assert!(writer.write(b"x").is_err());
        assert!(writer.truncate(MAX_FILE_BYTES + 1).is_err());
        #[cfg(unix)]
        {
            let another = tempfile::tempdir().unwrap();
            std::os::unix::fs::symlink(root.path(), another.path().join("media")).unwrap();
            assert!(DiskWriter::new(another.path(), &KEY, None, cancelled).is_err());
        }
    }

    #[test]
    fn explicit_export_survives_removed_outgoing_source_and_cancels_without_plaintext() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("original.txt");
        fs::write(&source, b"synthetic outgoing fixture").unwrap();
        let saved = copy_to(root.path(), &KEY, &source).unwrap();
        fs::remove_file(&source).unwrap();
        let output = root.path().join("requested.txt");
        let cancelled = AtomicBool::new(true);
        assert!(export_to(&saved, &KEY, &output, &cancelled).is_err());
        assert!(!output.exists());
        cancelled.store(false, Ordering::Release);
        export_to(&saved, &KEY, &output, &cancelled).unwrap();
        assert_eq!(fs::read(&output).unwrap(), b"synthetic outgoing fixture");
        assert!(export_to(&saved, &KEY, &output, &cancelled).is_err());
        assert!(ChunkFile::open(&saved, &[0; 32]).is_err());
    }

    #[test]
    fn legacy_migration_preserves_unknown_files_and_recovers_both_replace_windows() {
        let root = tempfile::tempdir().unwrap();
        let media = root.path().join("media");
        fs::create_dir(&media).unwrap();
        let first = media.join("12.jpg");
        let orphan = media.join("orphan.unknown");
        fs::write(&first, b"synthetic existing attachment").unwrap();
        fs::write(&orphan, b"synthetic unregistered orphan").unwrap();
        // Crash before replacement: the registry knows the exact legacy bytes.
        let registry = Registry::open(&media, &KEY).unwrap();
        let hash = hash_file(&first).unwrap();
        let length = fs::metadata(&first).unwrap().len();
        registry.legacy(&first, length, &hash).unwrap();
        drop(registry);
        migrate_directory(root.path(), &KEY).unwrap();
        assert_eq!(
            ChunkFile::open(&first, &KEY).unwrap().verify().unwrap(),
            hash
        );
        let mut decoded = ChunkFile::open(&orphan, &KEY).unwrap();
        let mut bytes = Vec::new();
        decoded.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"synthetic unregistered orphan");
        drop(decoded);
        // Crash after replacement: restore the still-pending registry state.
        let registry = Registry::open(&media, &KEY).unwrap();
        let interrupted = media.join("after-replacement.bin");
        fs::write(&interrupted, b"synthetic existing attachment").unwrap();
        registry.legacy(&interrupted, length, &hash).unwrap();
        migrate_one(root.path(), &KEY, &interrupted, length, &hash).unwrap();
        drop(registry);
        let ciphertext = fs::read(&interrupted).unwrap();
        migrate_directory(root.path(), &KEY).unwrap();
        assert_eq!(fs::read(&interrupted).unwrap(), ciphertext);
        assert!(migrate_directory(root.path(), &[0; 32]).is_err());
        assert_eq!(fs::read(&interrupted).unwrap(), ciphertext);
    }

    #[test]
    fn migration_refuses_changed_original_and_keeps_it_intact() {
        let root = tempfile::tempdir().unwrap();
        let media = root.path().join("media");
        fs::create_dir(&media).unwrap();
        let source = media.join("fixture.bin");
        fs::write(&source, b"original").unwrap();
        let registry = Registry::open(&media, &KEY).unwrap();
        registry
            .legacy(&source, 8, &hash_file(&source).unwrap())
            .unwrap();
        fs::write(&source, b"modified").unwrap();
        assert!(migrate_directory(root.path(), &KEY).is_err());
        assert_eq!(fs::read(&source).unwrap(), b"modified");
    }

    #[test]
    fn staging_prefix_never_authorizes_deleting_unknown_plaintext() {
        let root = tempfile::tempdir().unwrap();
        let media = root.path().join("media");
        fs::create_dir(&media).unwrap();
        // This PID cannot exist on supported desktop platforms.
        let source = media.join(".wassup-encrypted-2147483647-unknown.txt");
        fs::write(&source, b"synthetic unknown orphan").unwrap();
        discard_staging(&media, &KEY).unwrap();
        assert_eq!(fs::read(&source).unwrap(), b"synthetic unknown orphan");
        migrate_directory(root.path(), &KEY).unwrap();
        discard_staging(&media, &KEY).unwrap();
        let mut saved = ChunkFile::open(&source, &KEY).unwrap();
        let mut bytes = Vec::new();
        saved.read_to_end(&mut bytes).unwrap();
        assert_eq!(bytes, b"synthetic unknown orphan");
    }

    #[test]
    fn restored_encrypted_attachment_without_registry_is_not_double_encrypted() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("fixture.txt");
        fs::write(&source, b"synthetic restored attachment").unwrap();
        let saved = copy_to(root.path(), &KEY, &source).unwrap();
        let ciphertext = fs::read(&saved).unwrap();
        fs::remove_file(root.path().join("media").join(INDEX_NAME)).unwrap();
        migrate_directory(root.path(), &KEY).unwrap();
        assert_eq!(fs::read(&saved).unwrap(), ciphertext);
        let output = root.path().join("exported.txt");
        export_to(&saved, &KEY, &output, &AtomicBool::new(false)).unwrap();
        assert_eq!(fs::read(output).unwrap(), b"synthetic restored attachment");
    }
}
