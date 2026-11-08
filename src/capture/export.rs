//! Explicit plaintext exports. Capture itself never generates readable files.

use super::*;
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};

fn check_cancelled(cancelled: &AtomicBool) -> Result<()> {
    anyhow::ensure!(
        !cancelled.load(Ordering::Acquire),
        "Archive export cancelled"
    );
    Ok(())
}

impl Capture {
    pub fn export_readable(&self, destination: &Path, chats: &[String]) -> Result<()> {
        self.export_readable_cancellable(destination, chats, &AtomicBool::new(false))
    }

    /// Builds a private export next to the chosen destination, then publishes
    /// it without overwriting anything. Only the caller's unlocked chats enter.
    pub fn export_readable_cancellable(
        &self,
        destination: &Path,
        chats: &[String],
        cancelled: &AtomicBool,
    ) -> Result<()> {
        check_cancelled(cancelled)?;
        anyhow::ensure!(!chats.is_empty(), "No unlocked chats selected for export");
        let parent = destination
            .parent()
            .context("Missing export parent")?
            .canonicalize()?;
        let archive_root = self
            .root()
            .parent()
            .and_then(Path::parent)
            .context("Missing archive parent")?;
        anyhow::ensure!(
            !parent.starts_with(archive_root),
            "Choose a folder outside the encrypted archive"
        );
        reject_symlink(destination)?;
        anyhow::ensure!(!destination.exists(), "Export destination already exists");
        let temporary = tempfile::Builder::new()
            .prefix(".Wassup-export-")
            .tempdir_in(&parent)?;
        let output = temporary.path();
        private_dir(output)?;
        private_dir(&output.join("chats"))?;
        private_dir(&output.join("media"))?;
        let mut reader = self.reader()?;
        let snapshot = reader.transaction()?;
        let reader = &snapshot;
        let pages = {
            let mut statement = reader
                .prepare("SELECT DISTINCT chat,month FROM events ORDER BY chat,month DESC")?;
            statement
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
                .into_iter()
                .filter(|(chat, _)| chats.contains(chat))
                .collect::<Vec<_>>()
        };
        let mut exported = HashSet::new();
        for (chat, month) in &pages {
            check_cancelled(cancelled)?;
            let records = {
                let mut statement=reader.prepare(
                    "SELECT e.message_id,e.target_id,e.kind,e.text,e.sent_at,e.observed_at,e.sender,e.direction,e.source,
                      m.status,m.path,m.error FROM events e LEFT JOIN media_jobs m ON m.event_sequence=e.sequence
                      WHERE e.chat=?1 AND e.month=?2 ORDER BY e.sequence")?;
                statement
                    .query_map(params![chat, month], |row| {
                        Ok(RenderRecord {
                            message_id: row.get(0)?,
                            target: row.get(1)?,
                            kind: row.get(2)?,
                            text: row.get(3)?,
                            sent_at: row.get(4)?,
                            observed_at: row.get(5)?,
                            sender: row.get(6)?,
                            direction: row.get(7)?,
                            source: row.get(8)?,
                            media_status: row.get(9)?,
                            media_path: row.get(10)?,
                            media_error: row.get(11)?,
                        })
                    })?
                    .collect::<std::result::Result<Vec<_>, _>>()?
            };
            let mut markdown = format!(
                "# {}\n\nMonth: {month} (UTC)\n\nOriginal observations are retained below. This file is generated from Wassup's journal.\n\n",
                escape_markdown(chat)
            );
            let pending_ids = records
                .iter()
                .filter(|record| record.kind == "outgoing_pending")
                .map(|record| record.message_id.clone())
                .collect::<HashSet<_>>();
            let mut own_messages = HashSet::new();
            for record in records {
                // An echo proves observation but should not print our original
                // text twice. Edits are never collapsed by their text.
                if record.direction == "outgoing"
                    && matches!(record.kind.as_str(), "message" | "outgoing_pending")
                {
                    if record.kind == "message" && pending_ids.contains(&record.message_id) {
                        continue;
                    }
                    if !own_messages.insert(record.message_id.clone()) {
                        continue;
                    }
                }
                markdown.push_str(&format!(
                    "## {} | {}\n\n",
                    stamp(record.sent_at),
                    escape_markdown(&record.kind)
                ));
                markdown.push_str(&format!(
                    "Sender: {}  \nMessage: {}  \nReceived: {}  \nSource: {}\n\n",
                    escape_markdown(&record.sender),
                    escape_markdown(&record.message_id),
                    stamp(record.observed_at),
                    escape_markdown(&record.source)
                ));
                if let Some(target) = record.target {
                    markdown.push_str(&format!("Target message: {}\n\n", escape_markdown(&target)));
                }
                for line in record.text.split('\n') {
                    markdown.push_str(&format!("> {}\n", escape_markdown(line)));
                }
                markdown.push('\n');
                if let Some(state) = record.media_status {
                    markdown.push_str(&format!("Attachment: **{}**", escape_markdown(&state)));
                    if let Some(path) = record.media_path {
                        let relative = relative_media_path(self.root(), Path::new(&path))?;
                        if state == "saved" && exported.insert(relative.clone()) {
                            self.export_attachment_cancellable(
                                &self.root().join(&relative),
                                &output.join(&relative),
                                cancelled,
                            )?;
                        }
                        markdown
                            .push_str(&format!(" [Open file](../{})", path.replace(' ', "%20")));
                    }
                    if let Some(error) = record.media_error {
                        markdown.push_str(&format!(" ({})", escape_markdown(&error)));
                    }
                    markdown.push_str("\n\n");
                }
                markdown.push_str("---\n\n");
            }
            let file = output
                .join("chats")
                .join(format!("{}-{month}.md", hex_digest(chat.as_bytes())));
            atomic_write(&file, markdown.as_bytes())?;
        }
        let mut index = "# Wassup chat archive\n\nSelect a conversation and month. Chat identifiers are the original WhatsApp addresses. All times use UTC.\n\n".to_owned();
        for (chat, month) in &pages {
            index.push_str(&format!(
                "- [{} · {}](chats/{}-{}.md)\n",
                escape_markdown(chat),
                month,
                hex_digest(chat.as_bytes()),
                month
            ));
        }
        if index.ends_with("\n\n") {
            index.push_str("No messages have been captured for this account yet.\n");
        }
        atomic_write(&output.join("index.md"), index.as_bytes())?;
        atomic_write(&output.join("README.md"), b"# Wassup export\n\nThis is an unencrypted copy created on request. Keep it private. Open index.md to read the exported chats. This copy does not update automatically.\n")?;
        check_cancelled(cancelled)?;
        sync_directory(output)?;
        check_cancelled(cancelled)?;
        publish_directory(output, destination)?;
        sync_directory(&parent)?;
        Ok(())
    }

    pub fn export_attachment(&self, source: &Path, destination: &Path) -> Result<()> {
        self.export_attachment_cancellable(source, destination, &AtomicBool::new(false))
    }

    pub fn export_attachment_cancellable(
        &self,
        source: &Path,
        destination: &Path,
        cancelled: &AtomicBool,
    ) -> Result<()> {
        check_cancelled(cancelled)?;
        let relative = relative_media_path(self.root(), source)?;
        let canonical = self.root().join(relative).canonicalize()?;
        anyhow::ensure!(
            canonical.starts_with(self.root().join("media")),
            "Attachment is outside the archive"
        );
        let parent = destination
            .parent()
            .context("Missing export parent")?
            .canonicalize()?;
        let archive_root = self
            .root()
            .parent()
            .and_then(Path::parent)
            .context("Missing archive parent")?;
        anyhow::ensure!(
            !parent.starts_with(archive_root),
            "Choose a folder outside the encrypted archive"
        );
        media::export_file_cancellable(self, &canonical, destination, cancelled)
    }

    /// Bind a saved file to the source chat; a path alone does not grant export.
    pub fn attachment_in_chats(&self, source: &Path, chats: &[String]) -> Result<bool> {
        let relative = relative_media_path(self.root(), source)?;
        let reader = self.reader()?;
        for chat in chats {
            let present: bool = reader.query_row(
                "SELECT EXISTS(SELECT 1 FROM media_jobs m JOIN events e ON e.sequence=m.event_sequence
                 WHERE m.status='saved' AND m.path=?1 AND e.chat=?2)",
                params![relative, chat], |row| row.get(0))?;
            if present {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

/// rename with exclusive publication: never replace even an empty folder.
#[cfg(target_os = "macos")]
fn publish_directory(source: &Path, destination: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(source.as_os_str().as_bytes())?;
    let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())?;
    let result =
        unsafe { libc::renamex_np(source.as_ptr(), destination.as_ptr(), libc::RENAME_EXCL) };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn publish_directory(source: &Path, destination: &Path) -> Result<()> {
    use std::os::unix::ffi::OsStrExt;
    let source = std::ffi::CString::new(source.as_os_str().as_bytes())?;
    let destination = std::ffi::CString::new(destination.as_os_str().as_bytes())?;
    let result = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            source.as_ptr(),
            libc::AT_FDCWD,
            destination.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if result != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

#[cfg(windows)]
fn publish_directory(source: &Path, destination: &Path) -> Result<()> {
    fs::rename(source, destination)?;
    Ok(())
}
