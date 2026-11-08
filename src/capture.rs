//! Independent, append-only message capture for Wassup.
//!
//! The protocol durability hook commits the complete batch before allowing an
//! acknowledgement. Readable copies are created only by an explicit export.

mod export;
pub mod media;
mod media_store;
mod storage;
#[cfg(test)]
mod tests;
pub mod view;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use sha2::{Digest, Sha256};
use whatsapp_rust::InboundDurabilityHook;
use whatsapp_rust::prelude::{Client, MessageExt, wa};
use whatsapp_rust::types::events::InboundMessage;
use whatsapp_rust::types::message::MessageInfo;
use whatsapp_rust::waproto::buffa::Message as _;

/// One immutable observation. `event_id` identifies an event, not its target.
#[derive(Clone, Debug, Default)]
pub struct CaptureEvent {
    pub event_id: String,
    pub chat: String,
    pub sender: String,
    pub message_id: String,
    pub target_id: Option<String>,
    pub kind: String,
    pub text: String,
    pub timestamp: i64,
    pub direction: String,
    pub raw: Vec<u8>,
    pub source: String,
}

/// A cheap snapshot for the UI. No private message contents appear here.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CaptureStatus {
    pub paused: bool,
    pub error: Option<String>,
    pub markdown_error: Option<String>,
    pub last_saved_at: Option<i64>,
    pub queued: u64,
    pub downloading: u64,
    pub saved: u64,
    pub failed: u64,
    pub oversized: u64,
    pub root: PathBuf,
}

#[derive(Clone, Debug)]
pub struct MediaJob {
    pub id: i64,
    pub raw: Vec<u8>,
    pub attempts: u32,
}

struct Inner {
    root: PathBuf,
    connection: Mutex<Connection>,
    status: Mutex<CaptureStatus>,
    key: storage::Key,
}

#[derive(Clone)]
pub struct Capture {
    inner: Arc<Inner>,
}

/// Deliberately outside application state, caches, and cloud-backed Documents.
pub fn default_root() -> PathBuf {
    directories::BaseDirs::new()
        .map(|dirs| dirs.home_dir().join("Wassup"))
        .unwrap_or_else(|| PathBuf::from("Wassup"))
}

impl Capture {
    /// `account` is a persistent random identity, never a reusable roster slot.
    /// Call on a worker, not the UI thread.
    pub fn open(root: &Path, account: &str) -> Result<Arc<Self>> {
        if account.is_empty() {
            bail!("Missing capture account identity");
        }
        private_dir(root)?;
        // macOS exposes its temporary directory through /var -> /private/var.
        // Reject a user-selected symlink leaf, then resolve trusted ancestors
        // before SQLite's NOFOLLOW checks the entire pathname.
        let root = fs::canonicalize(root)?;
        let accounts = root.join("accounts");
        private_dir(&accounts)?;
        let root = accounts.join(hex_digest(account.as_bytes()));
        private_dir(&root)?;
        private_dir(&root.join("media"))?;

        let db = root.join("journal.sqlite3");
        reject_symlink(&db)?;
        // NOFOLLOW closes the check/open race for the database itself.
        let namespace = root
            .file_name()
            .and_then(|name| name.to_str())
            .context("Invalid capture namespace")?;
        let key = storage::key_for(&root, namespace)?;
        let mut connection = storage::open(&db, &key)?;
        private_file(&db)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        connection.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=FULL;
             PRAGMA fullfsync=ON;
             PRAGMA checkpoint_fullfsync=ON;
             PRAGMA foreign_keys=ON;
             CREATE TABLE IF NOT EXISTS events (
               sequence INTEGER PRIMARY KEY AUTOINCREMENT,
               dedup TEXT NOT NULL UNIQUE,
               event_id TEXT NOT NULL, chat TEXT NOT NULL, sender TEXT NOT NULL,
               message_id TEXT NOT NULL, target_id TEXT, kind TEXT NOT NULL,
               text TEXT NOT NULL, sent_at INTEGER NOT NULL, observed_at INTEGER NOT NULL,
               direction TEXT NOT NULL, raw BLOB NOT NULL, source TEXT NOT NULL,
               month TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS events_page ON events(chat,month,sequence);
             CREATE INDEX IF NOT EXISTS events_observed ON events(observed_at);
             CREATE INDEX IF NOT EXISTS events_message ON events(chat,message_id,kind,sequence);
             CREATE INDEX IF NOT EXISTS events_target ON events(chat,target_id,sequence);
             CREATE TABLE IF NOT EXISTS media_jobs (
               id INTEGER PRIMARY KEY AUTOINCREMENT,
               event_sequence INTEGER NOT NULL UNIQUE REFERENCES events(sequence),
               raw BLOB NOT NULL, status TEXT NOT NULL DEFAULT 'queued',
               attempts INTEGER NOT NULL DEFAULT 0, path TEXT, error TEXT,
               retry_at INTEGER
             );
             CREATE INDEX IF NOT EXISTS media_state ON media_jobs(status,retry_at,id);
             CREATE TABLE IF NOT EXISTS dirty_pages (
               chat TEXT NOT NULL, month TEXT NOT NULL, version INTEGER NOT NULL,
               PRIMARY KEY(chat,month)
             );
             CREATE TRIGGER IF NOT EXISTS immutable_events_update
               BEFORE UPDATE ON events BEGIN SELECT RAISE(ABORT,'Immutable capture event'); END;
             CREATE TRIGGER IF NOT EXISTS immutable_events_delete
               BEFORE DELETE ON events BEGIN SELECT RAISE(ABORT,'Immutable capture event'); END;",
        )?;
        // A previous process may have stopped with transfers in flight.
        let tx = connection.transaction()?;
        tx.execute(
            "UPDATE media_jobs SET status='queued',retry_at=0 WHERE status='downloading'",
            [],
        )?;
        tx.commit()?;
        for suffix in ["-wal", "-shm"] {
            let path = db.with_file_name(format!("journal.sqlite3{suffix}"));
            if path.exists() {
                private_file(&path)?;
            }
        }
        sync_directory(&root)?;
        let capture = Arc::new(Self {
            inner: Arc::new(Inner {
                root: root.clone(),
                connection: Mutex::new(connection),
                status: Mutex::new(CaptureStatus {
                    root,
                    ..Default::default()
                }),
                key,
            }),
        });
        capture.refresh_status()?;
        // Fail closed until every pre-existing attachment has been encrypted.
        // The journal migration is independently durable and can be resumed.
        media::migrate_existing(&capture)?;
        capture.retire_legacy_markdown()?;
        if let Some(root) = capture.root().parent().and_then(Path::parent) {
            write_archive_readme(root)?;
        }
        Ok(capture)
    }

    /// Account-specific storage, independent of Wassup's app profile and caches.
    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    pub fn status(&self) -> CaptureStatus {
        self.inner
            .status
            .lock()
            .map(|s| s.clone())
            .unwrap_or_else(|_| CaptureStatus {
                paused: true,
                error: Some("Capture state is unavailable. Restart Wassup.".to_owned()),
                root: self.root().to_owned(),
                ..Default::default()
            })
    }

    pub fn record_event(&self, event: CaptureEvent) -> Result<()> {
        self.record_entries(&[(event, None)])
    }

    /// Event-only recovery paths do not get the SDK's pre-ACK guarantee.
    pub fn record_incoming(
        &self,
        message: &wa::Message,
        info: &MessageInfo,
        source: &str,
    ) -> Result<()> {
        match incoming_event(message, info, source) {
            Some(event) => self.record_event(event),
            None => Ok(()),
        }
    }

    /// Preserve local attachment bytes before callers send or clear a cache.
    pub fn record_outgoing(&self, row: &crate::model::Message, raw: &[u8]) -> Result<()> {
        if !allowed_chat(&row.chat) {
            return Ok(());
        }
        let message =
            wa::Message::decode_from_slice(raw).context("Invalid outgoing capture payload")?;
        if message.is_view_once() {
            return Ok(());
        }
        let saved = row
            .content
            .media()
            .and_then(|m| m.path.as_deref())
            .map(|path| media::copy_outgoing(self, path))
            .transpose();
        let saved = match saved {
            Ok(saved) => saved,
            Err(error) => {
                self.write_failed();
                return Err(error);
            }
        };
        self.record_entries(&[(
            CaptureEvent {
                event_id: row.id.clone(),
                chat: row.chat.clone(),
                sender: row.sender.clone(),
                message_id: row.id.clone(),
                kind: "outgoing_pending".to_owned(),
                text: content_text(&row.content),
                timestamp: row.timestamp,
                direction: "outgoing".to_owned(),
                raw: raw.to_vec(),
                source: "local_send".to_owned(),
                ..Default::default()
            },
            saved,
        )])
    }

    fn record_entries(&self, entries: &[(CaptureEvent, Option<PathBuf>)]) -> Result<()> {
        let result = (|| {
            let mut connection = self
                .inner
                .connection
                .lock()
                .map_err(|_| anyhow!("Capture lock failed"))?;
            let tx = connection.transaction()?;
            for (event, saved) in entries {
                if !allowed_chat(&event.chat) {
                    continue;
                }
                insert_event(&tx, event, saved.as_deref(), self.root())?;
            }
            tx.commit()?;
            Ok::<_, anyhow::Error>(())
        })();
        if result.is_err() {
            self.write_failed();
            return result;
        }
        if let Ok(mut status) = self.inner.status.lock() {
            status.paused = false;
            status.error = None;
        }
        // A status refresh failure must not turn a durable commit into an
        // unacknowledged batch. The journal is already committed.
        let _ = self.refresh_status();
        Ok(())
    }

    fn write_failed(&self) {
        if let Ok(mut status) = self.inner.status.lock() {
            status.paused = true;
            status.error =
                Some("Capture paused. Check free space and archive folder access.".to_owned());
        }
    }

    pub fn claim_media_job(&self) -> Result<Option<MediaJob>> {
        let mut connection = self
            .inner
            .connection
            .lock()
            .map_err(|_| anyhow!("Capture lock failed"))?;
        let tx = connection.transaction()?;
        let mut job = tx
            .query_row(
                "SELECT id,raw,attempts FROM media_jobs
             WHERE status IN ('queued','failed','paused')
               AND (status='queued' OR retry_at IS NOT NULL)
               AND COALESCE(retry_at,0)<=?1 ORDER BY id LIMIT 1",
                [now()],
                |row| {
                    Ok(MediaJob {
                        id: row.get(0)?,
                        raw: row.get(1)?,
                        attempts: row.get(2)?,
                    })
                },
            )
            .optional()?;
        if let Some(job) = &mut job {
            tx.execute(
                "UPDATE media_jobs SET status='downloading',attempts=attempts+1 WHERE id=?1",
                [job.id],
            )?;
            job.attempts += 1;
            mark_media_dirty(&tx, job.id)?;
        }
        tx.commit()?;
        drop(connection);
        self.refresh_status()?;
        Ok(job)
    }

    pub fn finish_media_job(
        &self,
        id: i64,
        status: &str,
        path: Option<&Path>,
        error: Option<&str>,
        retry_at: Option<i64>,
    ) -> Result<()> {
        if !matches!(
            status,
            "queued" | "downloading" | "saved" | "failed" | "oversized" | "paused"
        ) {
            bail!("Invalid attachment state");
        }
        let path = path
            .map(|p| relative_media_path(self.root(), p))
            .transpose()?;
        if status == "saved" && path.is_none() {
            bail!("Saved attachment needs a permanent path");
        }
        let mut connection = self
            .inner
            .connection
            .lock()
            .map_err(|_| anyhow!("Capture lock failed"))?;
        let tx = connection.transaction()?;
        if tx.execute(
            "UPDATE media_jobs SET status=?2,path=?3,error=?4,retry_at=?5,
                attempts=CASE WHEN ?2='paused' THEN MAX(attempts-1,0) ELSE attempts END WHERE id=?1",
            params![id,status,path,error,retry_at],
        )? != 1 {
            bail!("Unknown attachment job");
        }
        mark_media_dirty(&tx, id)?;
        tx.commit()?;
        drop(connection);
        self.refresh_status()?;
        if let Ok(mut snapshot) = self.inner.status.lock() {
            snapshot.paused = false;
            snapshot.error = None;
        }
        Ok(())
    }

    fn refresh_status(&self) -> Result<()> {
        let connection = self
            .inner
            .connection
            .lock()
            .map_err(|_| anyhow!("Capture lock failed"))?;
        let last =
            connection.query_row("SELECT MAX(observed_at) FROM events", [], |row| row.get(0))?;
        let mut counts = [0_u64; 5];
        let mut statement =
            connection.prepare("SELECT status,COUNT(*) FROM media_jobs GROUP BY status")?;
        for row in statement.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))? {
            let (state, count) = row?;
            let count = u64::try_from(count)?;
            match state.as_str() {
                "queued" | "paused" => counts[0] += count,
                "downloading" => counts[1] += count,
                "saved" => counts[2] += count,
                "failed" => counts[3] += count,
                "oversized" => counts[4] += count,
                _ => {}
            }
        }
        let mut status = self
            .inner
            .status
            .lock()
            .map_err(|_| anyhow!("Capture status lock failed"))?;
        status.last_saved_at = last;
        [
            status.queued,
            status.downloading,
            status.saved,
            status.failed,
            status.oversized,
        ] = counts;
        Ok(())
    }

    pub(super) fn key(&self) -> &[u8; 32] {
        &self.inner.key
    }

    pub(crate) fn reader(&self) -> Result<Connection> {
        storage::read_only(&self.root().join("journal.sqlite3"), self.key())
    }

    /// Preserve prior generated exports inside the encrypted journal before
    /// removing plaintext copies. This also protects any edits made to them.
    fn retire_legacy_markdown(&self) -> Result<()> {
        let mut connection = self
            .inner
            .connection
            .lock()
            .map_err(|_| anyhow!("Capture lock failed"))?;
        connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS legacy_exports (
            path TEXT NOT NULL, chunk INTEGER NOT NULL, bytes BLOB NOT NULL,
            PRIMARY KEY(path,chunk));",
        )?;
        let mut files = Vec::new();
        let chats = self.root().join("chats");
        if chats.exists() {
            reject_symlink(&chats)?;
            for entry in fs::read_dir(&chats)? {
                let entry = entry?;
                let name = entry.file_name();
                let name = name.to_str().context("Invalid legacy export filename")?;
                let valid = name.is_ascii()
                    && name.len() == 75
                    && name[..64].bytes().all(|c| c.is_ascii_hexdigit())
                    && name.as_bytes()[64] == b'-'
                    && name.ends_with(".md")
                    && name[65..69].bytes().all(|c| c.is_ascii_digit())
                    && name.as_bytes()[69] == b'-'
                    && name[70..72].bytes().all(|c| c.is_ascii_digit());
                if !valid || !entry.file_type()?.is_file() {
                    bail!("Unexpected file in legacy chat exports; migration paused");
                }
                files.push(entry.path());
            }
        }
        let index = self.root().join("index.md");
        if index.exists() {
            files.push(index);
        }
        for path in files {
            reject_symlink(&path)?;
            let relative = path.strip_prefix(self.root())?.to_string_lossy();
            let mut input = fs::File::open(&path)?;
            let original_metadata = input.metadata()?;
            let tx = connection.transaction()?;
            // Re-running after a crash replaces only this encrypted backup.
            tx.execute("DELETE FROM legacy_exports WHERE path=?1", [&*relative])?;
            let mut buffer = [0u8; 64 * 1024];
            let mut chunk = 0_i64;
            let mut digest = Sha256::new();
            loop {
                use std::io::Read;
                let len = input.read(&mut buffer)?;
                if len == 0 {
                    break;
                }
                digest.update(&buffer[..len]);
                tx.execute(
                    "INSERT INTO legacy_exports(path,chunk,bytes) VALUES(?1,?2,?3)",
                    params![&*relative, chunk, &buffer[..len]],
                )?;
                chunk += 1;
            }
            tx.commit()?;
            let mut verified = Sha256::new();
            let mut statement = connection
                .prepare("SELECT bytes FROM legacy_exports WHERE path=?1 ORDER BY chunk")?;
            for bytes in statement.query_map([&*relative], |row| row.get::<_, Vec<u8>>(0))? {
                verified.update(bytes?);
            }
            let expected = digest.finalize();
            anyhow::ensure!(
                expected == verified.finalize(),
                "Legacy export verification failed"
            );
            reject_symlink(&path)?;
            let current_metadata = fs::metadata(&path)?;
            anyhow::ensure!(
                original_metadata.len() == current_metadata.len()
                    && original_metadata.modified()? == current_metadata.modified()?,
                "A legacy transcript changed during migration; retry after closing its editor"
            );
            let mut current = fs::File::open(&path)?;
            let mut current_digest = Sha256::new();
            loop {
                use std::io::Read;
                let len = current.read(&mut buffer)?;
                if len == 0 {
                    break;
                }
                current_digest.update(&buffer[..len]);
            }
            anyhow::ensure!(
                expected == current_digest.finalize(),
                "A legacy transcript changed during migration"
            );
            fs::remove_file(&path)?;
            sync_directory(path.parent().context("Missing legacy export directory")?)?;
        }
        if chats.exists() {
            fs::remove_dir(&chats)?;
            sync_directory(self.root())?;
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl InboundDurabilityHook for Capture {
    async fn on_messages(&self, _client: Arc<Client>, batch: &[InboundMessage]) -> Result<()> {
        let events = batch
            .iter()
            .filter_map(|item| incoming_event(&item.message, &item.info, "durable_inbound"))
            .map(|event| (event, None))
            .collect::<Vec<_>>();
        let capture = self.clone();
        tokio::task::spawn_blocking(move || capture.record_entries(&events))
            .await
            .context("Capture task failed")?
    }
}

fn incoming_event(message: &wa::Message, info: &MessageInfo, source: &str) -> Option<CaptureEvent> {
    let original_chat = info.source.chat.to_non_ad_string();
    // Received list broadcasts are delivered into the owner's direct chat.
    // Status broadcasts and channels remain outside capture's scope.
    let chat = if original_chat.ends_with("@broadcast")
        && original_chat != "status@broadcast"
        && !info.source.is_from_me
    {
        info.source
            .broadcast_list_owner
            .as_ref()
            .unwrap_or(&info.source.sender)
            .to_non_ad_string()
    } else {
        original_chat
    };
    if !allowed_chat(&chat) || message.is_view_once() {
        return None;
    }
    let base = message.get_base_message();
    let (kind, target, text) = if let Some(protocol) = base.protocol_message.as_option() {
        use wa::message::protocol_message::Type;
        let target = protocol.key.as_option()?.id.clone()?;
        match protocol.r#type {
            Some(Type::REVOKE) => (
                "deleted",
                Some(target),
                "Sender requested deletion. The saved original is retained.".to_owned(),
            ),
            Some(Type::MESSAGE_EDIT) => {
                let edited = protocol.edited_message.as_option()?;
                if edited.is_view_once() {
                    return None;
                }
                ("edited", Some(target), message_text(edited))
            }
            _ => return None,
        }
    } else {
        ("message", None, message_text(message))
    };
    Some(CaptureEvent {
        event_id: info.id.to_string(),
        chat,
        sender: info.source.sender.to_non_ad_string(),
        message_id: info.id.to_string(),
        target_id: target,
        kind: kind.to_owned(),
        text,
        timestamp: info.timestamp.timestamp(),
        direction: if info.source.is_from_me {
            "outgoing"
        } else {
            "incoming"
        }
        .to_owned(),
        raw: message.encode_to_vec(),
        source: source.to_owned(),
    })
}

fn message_text(message: &wa::Message) -> String {
    let base = message.get_base_message();
    if let Some(text) = base.text_content() {
        return text.to_owned();
    }
    if let Some(image) = base.image_message.as_option() {
        return image
            .caption
            .clone()
            .unwrap_or_else(|| "[Photo]".to_owned());
    }
    if let Some(video) = base
        .video_message
        .as_option()
        .or(base.ptv_message.as_option())
    {
        return video
            .caption
            .clone()
            .unwrap_or_else(|| "[Video]".to_owned());
    }
    if let Some(document) = base.document_message.as_option() {
        return [
            document.file_name.as_deref().unwrap_or("Document"),
            document.caption.as_deref().unwrap_or(""),
        ]
        .join("\n");
    }
    if base.audio_message.is_set() {
        return "[Audio or voice note]".to_owned();
    }
    if base.sticker_message.is_set() {
        return "[Sticker]".to_owned();
    }
    if let Some(reaction) = base.reaction_message.as_option() {
        return format!("[Reaction] {}", reaction.text.as_deref().unwrap_or(""));
    }
    if let Some(contact) = base.contact_message.as_option() {
        return [
            contact.display_name.as_deref().unwrap_or("Contact"),
            contact.vcard.as_deref().unwrap_or(""),
        ]
        .join("\n");
    }
    "[Message payload retained in journal]".to_owned()
}

/// Preserve captions and multiline bodies that the chat-list summary omits.
pub fn content_text(content: &crate::model::Content) -> String {
    use crate::model::Content;
    match content {
        Content::Document {
            file_name, caption, ..
        } => [file_name.as_str(), caption.as_deref().unwrap_or("")].join("\n"),
        Content::Contact {
            display_name,
            vcard,
        } => [display_name.as_str(), vcard.as_str()].join("\n"),
        _ => content.full_summary(),
    }
}

fn insert_event(
    tx: &Transaction<'_>,
    event: &CaptureEvent,
    saved: Option<&Path>,
    root: &Path,
) -> Result<()> {
    // Raw fingerprints distinguish valid edits while the envelope identity
    // prevents equal-text revisions from collapsing across separate events.
    let mut digest = Sha256::new();
    for part in [
        &event.chat,
        &event.sender,
        &event.message_id,
        &event.event_id,
        &event.kind,
    ] {
        digest.update((part.len() as u64).to_be_bytes());
        digest.update(part.as_bytes());
    }
    digest.update(&event.raw);
    digest.update(event.text.as_bytes());
    digest.update(event.target_id.as_deref().unwrap_or("").as_bytes());
    digest.update(event.timestamp.to_be_bytes());
    let dedup = hex_bytes(&digest.finalize());
    let observed = now();
    let month = month(event.timestamp);
    let inserted=tx.execute("INSERT OR IGNORE INTO events(dedup,event_id,chat,sender,message_id,target_id,kind,text,sent_at,observed_at,direction,raw,source,month)
      VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14)",
      params![dedup,event.event_id,event.chat,event.sender,event.message_id,event.target_id,event.kind,event.text,event.timestamp,observed,event.direction,event.raw,event.source,month])?;
    if inserted == 0 {
        return Ok(());
    }
    let sequence = tx.last_insert_rowid();
    if let Some(saved) = saved {
        let path = relative_media_path(root, saved)?;
        tx.execute(
            "INSERT INTO media_jobs(event_sequence,raw,status,path) VALUES(?1,?2,'saved',?3)",
            params![sequence, event.raw, path],
        )?;
    } else if media::has_media(&event.raw) {
        tx.execute(
            "INSERT INTO media_jobs(event_sequence,raw,status) VALUES(?1,?2,'queued')",
            params![sequence, event.raw],
        )?;
    }
    tx.execute("INSERT INTO dirty_pages(chat,month,version) VALUES(?1,?2,1) ON CONFLICT(chat,month) DO UPDATE SET version=version+1",params![event.chat,month])?;
    Ok(())
}

fn mark_media_dirty(tx: &Transaction<'_>, id: i64) -> Result<()> {
    tx.execute("INSERT INTO dirty_pages(chat,month,version) SELECT e.chat,e.month,1 FROM events e JOIN media_jobs m ON m.event_sequence=e.sequence WHERE m.id=?1
      ON CONFLICT(chat,month) DO UPDATE SET version=version+1",[id])?;
    Ok(())
}

fn relative_media_path(root: &Path, path: &Path) -> Result<String> {
    let relative = if path.is_absolute() {
        path.strip_prefix(root)?
    } else {
        path
    };
    if !relative.starts_with("media")
        || relative
            .components()
            .any(|p| !matches!(p, std::path::Component::Normal(_)))
    {
        bail!("Attachment path must be inside archive media");
    }
    Ok(relative.to_string_lossy().replace('\\', "/"))
}

fn allowed_chat(chat: &str) -> bool {
    chat.split_once('@').is_some_and(|(user, server)| {
        !user.is_empty() && matches!(server, "s.whatsapp.net" | "lid" | "g.us")
    })
}
fn now() -> i64 {
    jiff::Timestamp::now().as_second()
}
fn month(timestamp: i64) -> String {
    jiff::Timestamp::from_second(timestamp)
        .unwrap_or_else(|_| jiff::Timestamp::now())
        .to_zoned(jiff::tz::TimeZone::UTC)
        .strftime("%Y-%m")
        .to_string()
}
fn stamp(timestamp: i64) -> String {
    jiff::Timestamp::from_second(timestamp)
        .map(|t| t.to_string())
        .unwrap_or_else(|_| format!("{timestamp} (invalid timestamp)"))
}
fn hex_digest(bytes: &[u8]) -> String {
    hex_bytes(&Sha256::digest(bytes))
}
fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn escape_markdown(value: &str) -> String {
    let mut result = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '&' => result.push_str("&amp;"),
            '<' => result.push_str("&lt;"),
            '>' => result.push_str("&gt;"),
            '\\' | '`' | '*' | '_' | '[' | ']' | '#' => {
                result.push('\\');
                result.push(c);
            }
            _ => result.push(c),
        }
    }
    result
}
fn reject_symlink(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            bail!("Archive paths cannot be symbolic links")
        }
        Ok(_) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
pub(super) fn private_dir(path: &Path) -> Result<()> {
    reject_symlink(path)?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut created = false;
    if !path.exists() {
        // Persist each newly created ancestor before putting durable data in
        // it. Syncing only the final account directory leaves its parent entry
        // vulnerable to a power failure after the message acknowledgement.
        if !parent.exists() {
            private_dir(parent)?;
        }
        let builder = fs::DirBuilder::new();
        #[cfg(unix)]
        let builder = {
            use std::os::unix::fs::DirBuilderExt;
            let mut builder = builder;
            builder.mode(0o700);
            builder
        };
        match builder.create(path) {
            Ok(()) => created = true,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                reject_symlink(path)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    if !path.is_dir() {
        bail!("Archive directory path is not a directory");
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    if created {
        sync_directory(path)?;
        sync_directory(parent)?;
    }
    Ok(())
}
fn private_file(path: &Path) -> Result<()> {
    reject_symlink(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}
fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
    reject_symlink(path)?;
    let parent = path.parent().context("Missing archive parent")?;
    private_dir(parent)?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    private_file(temporary.path())?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary.persist(path)?;
    sync_directory(parent)
}

fn write_archive_readme(root: &Path) -> Result<()> {
    let text = "# Wassup encrypted archive\n\nMessages and attachments in this folder are encrypted. Wassup unlocks them with a separate permanent-archive key in the macOS Keychain. Read deleted messages inside Wassup. Use Export transcript, Save attachment, or Export archive when you want a readable copy outside the app. Exports are unencrypted.\n\nBack up this entire folder with Wassup closed. Preserve the matching Keychain keys: copying this folder alone is not enough to recover it on another Mac. Removing an account from Wassup does not delete its permanent archive or key. Do not remove database sidecar files while Wassup is running.\n";
    atomic_write(&root.join("WASSUP-README.md"), text.as_bytes())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}
struct RenderRecord {
    message_id: String,
    target: Option<String>,
    kind: String,
    text: String,
    sent_at: i64,
    observed_at: i64,
    sender: String,
    direction: String,
    source: String,
    media_status: Option<String>,
    media_path: Option<String>,
    media_error: Option<String>,
}
