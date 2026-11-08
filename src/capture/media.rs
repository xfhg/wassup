//! Durable attachment capture, independent of the mutable chat archive.
//!
//! The receive hook only queues jobs. Two workers stream immutable descriptors
//! to private temporary files, authenticate them, and publish complete files.

use super::{Capture, MediaJob};
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
        // Only this archive's private staging files are disposable. Published
        // files survive even when a crash prevented the saved-state update.
        let _ = tokio::task::spawn_blocking(move || discard_staging(&directory)).await;
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
    match tokio::task::spawn_blocking(move || {
        recover_published(&published, expected_hash.as_deref(), size)
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
        let mut writer = DiskWriter::new(&root, size, cancel)?;
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
    ensure!(
        metadata.len() <= MAX_FILE_BYTES && size.is_none_or(|size| metadata.len() == size),
        "Attachment length mismatch"
    );
    let Some(expected) = expected_hash.filter(|hash| hash.len() == 32) else {
        return Ok(false);
    };
    ensure!(
        hash_file(target)?.as_slice() == expected,
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

fn discard_staging(directory: &Path) -> io::Result<()> {
    let metadata = match fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(error) => return Err(error),
    };
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Ok(());
    }
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let pid = name
            .strip_prefix(".incoming-")
            .and_then(|s| s.split('-').next())
            .and_then(|s| s.parse::<u32>().ok());
        if pid.is_some_and(|pid| !process_running(pid)) && entry.file_type()?.is_file() {
            fs::remove_file(entry.path())?;
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
    copy_to(capture.root(), source)
}

fn copy_to(root: &Path, source: &Path) -> Result<PathBuf> {
    let mut input = File::open(source).context("Cannot open outgoing attachment")?;
    let metadata = input.metadata()?;
    ensure!(
        metadata.is_file(),
        "Outgoing attachment must be a regular file"
    );
    ensure!(metadata.len() <= MAX_FILE_BYTES, OVERSIZED);
    let mut writer = DiskWriter::new(root, Some(metadata.len()), Arc::new(AtomicBool::new(false)))?;
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
    file: tempfile::NamedTempFile,
    directory: PathBuf,
    cancelled: Arc<AtomicBool>,
    transfer_permit: Option<OwnedSemaphorePermit>,
}

impl DiskWriter {
    fn new(root: &Path, expected: Option<u64>, cancelled: Arc<AtomicBool>) -> io::Result<Self> {
        let directory = root.join("media");
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        let metadata = fs::symlink_metadata(&directory)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(io::Error::other(
                "Capture media directory is not a real directory",
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        }
        reserve_space(&directory, expected.unwrap_or(1_048_576))?;
        let file = tempfile::Builder::new()
            .prefix(&format!(".incoming-{}-", std::process::id()))
            .tempfile_in(&directory)?;
        Ok(Self {
            file,
            directory,
            cancelled,
            transfer_permit: None,
        })
    }

    fn publish(mut self, target: &Path) -> Result<()> {
        ensure!(!self.cancelled.load(Ordering::Acquire), CANCELLED);
        self.file.flush()?;
        durable_file(self.file.as_file())?;
        match self.file.persist_noclobber(target) {
            Ok(file) => {
                durable_file(&file)?;
            }
            Err(error) if error.error.kind() == io::ErrorKind::AlreadyExists => {
                // A crash may publish bytes before the journal marks success.
                // Never silently adopt or overwrite a different existing file.
                let metadata = fs::symlink_metadata(target)?;
                ensure!(
                    metadata.is_file() && !metadata.file_type().is_symlink(),
                    "Attachment destination is not a regular file"
                );
                ensure!(
                    hash_file(error.file.path())? == hash_file(target)?,
                    "Attachment destination conflicts with saved bytes"
                );
                durable_file(&File::open(target)?)?;
            }
            Err(error) => return Err(error.error.into()),
        }
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }
}

impl Write for DiskWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.cancelled.load(Ordering::Acquire) {
            return Err(io::Error::other(CANCELLED));
        }
        let end = self
            .file
            .stream_position()?
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| io::Error::other(OVERSIZED))?;
        if end > MAX_FILE_BYTES {
            return Err(io::Error::other(OVERSIZED));
        }
        let _disk = DISK_WRITE
            .lock()
            .map_err(|_| io::Error::other("Capture disk guard failed"))?;
        reserve_space(&self.directory, bytes.len() as u64)?;
        self.file.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

impl Seek for DiskWriter {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.file.seek(position)
    }
}

impl DownloadWriter for DiskWriter {
    fn truncate(&mut self, len: u64) -> io::Result<()> {
        if len > MAX_FILE_BYTES {
            return Err(io::Error::other(OVERSIZED));
        }
        self.file.as_file_mut().set_len(len)
    }
}

fn hash_file(path: &Path) -> Result<[u8; 32]> {
    let mut file = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hash.update(&buffer[..read]);
    }
    Ok(hash.finalize().into())
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
            DiskWriter::new(root.path(), Some(1), Arc::new(AtomicBool::new(true))).unwrap();
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
        let saved = copy_to(root.path(), &source).unwrap();
        assert_eq!(fs::read(&saved).unwrap(), b"synthetic attachment");
        assert_eq!(copy_to(root.path(), &source).unwrap(), saved);
        fs::write(&saved, b"different bytes").unwrap();
        assert!(copy_to(root.path(), &source).is_err());
    }

    #[test]
    fn published_file_recovers_after_a_crash_only_when_descriptor_matches() {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("fixture.txt");
        fs::write(&source, b"synthetic attachment").unwrap();
        let target = copy_to(root.path(), &source).unwrap();
        let hash = hash_file(&target).unwrap();
        assert!(recover_published(&target, Some(&hash), Some(20)).unwrap());
        assert!(!recover_published(&target, None, Some(20)).unwrap());
        assert!(recover_published(&target, Some(&hash), Some(19)).is_err());
        assert!(recover_published(&target, Some(&[0; 32]), Some(20)).is_err());
    }

    #[test]
    fn writer_refuses_oversize_cancellation_and_symlink_directory() {
        let root = tempfile::tempdir().unwrap();
        let cancelled = Arc::new(AtomicBool::new(false));
        let mut writer = DiskWriter::new(root.path(), Some(1), cancelled.clone()).unwrap();
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
            assert!(DiskWriter::new(another.path(), None, cancelled).is_err());
        }
    }
}
