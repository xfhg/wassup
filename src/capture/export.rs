//! Explicit plaintext exports. Capture itself never generates readable files.

use super::*;
use crate::backend::RetainedMessage;
use crate::capture::view::{SavedAttachment, SavedObservation};
use std::collections::HashSet;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicBool, Ordering};

fn check_cancelled(cancelled: &AtomicBool) -> Result<()> {
    anyhow::ensure!(
        !cancelled.load(Ordering::Acquire),
        "Archive export cancelled"
    );
    Ok(())
}

impl Capture {
    /// Writes the shared deleted-chat projection, including its complete
    /// histories, into one plaintext transcript and its selected attachments.
    /// Callers obtain rows through the same privacy-filtered projection as the
    /// reader; this renderer does not broaden that selection.
    pub(crate) fn export_deleted_readable_cancellable(
        &self,
        destination: &Path,
        title: &str,
        rows: &[RetainedMessage],
        cancelled: &AtomicBool,
    ) -> Result<()> {
        check_cancelled(cancelled)?;
        anyhow::ensure!(
            rows.iter().all(|row| !row.message.saved.history_truncated),
            "Complete revision history is required for export"
        );
        if let Some(first) = rows.first() {
            anyhow::ensure!(
                rows.iter()
                    .all(|row| row.message.canonical_chat == first.message.canonical_chat),
                "A deleted-chat export must contain one source chat"
            );
        }
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
        private_dir(&output.join("media"))?;
        let transcript_path = output.join("chat.md");
        let transcript = fs::File::create_new(&transcript_path)?;
        private_file(&transcript_path)?;
        let mut transcript = BufWriter::new(transcript);
        writeln!(transcript, "# {}\n", one_line(title))?;
        writeln!(
            transcript,
            "Saved deletions and their surrounding context. Times are UTC.\n\nThis is a plaintext copy created on request. Keep it private.\n"
        )?;
        let mut exported_files = HashSet::new();
        let mut exported_messages = HashSet::new();
        if rows.is_empty() {
            writeln!(
                transcript,
                "No saved deletions were available in this chat.\n"
            )?;
        }
        for row in rows {
            check_cancelled(cancelled)?;
            let message = &row.message;
            let saved = &message.saved;
            if !exported_messages.insert((message.canonical_chat.clone(), saved.message_id.clone()))
            {
                continue;
            }
            if row.gap_before {
                writeln!(
                    transcript,
                    "---\n\n*Messages omitted between saved context windows.*\n"
                )?;
            }
            if row.missing_before {
                writeln!(transcript, "*No earlier saved message.*\n")?;
            }
            let role = if message.reason.is_empty() {
                "Context"
            } else {
                "Deleted"
            };
            writeln!(
                transcript,
                "## {} · {} · {}\n",
                stamp(saved.sent_at),
                one_line(&message.sender_name),
                role
            )?;
            if !message.reason.is_empty() {
                writeln!(transcript, "{}\n", one_line(&message.reason))?;
            }
            write_quoted(&mut transcript, &saved.text)?;
            self.write_deleted_attachments(
                &mut transcript,
                output,
                &saved.attachments,
                &mut exported_files,
                cancelled,
            )?;
            for observation in &saved.history {
                check_cancelled(cancelled)?;
                write_observation(&mut transcript, observation)?;
                self.write_deleted_attachments(
                    &mut transcript,
                    output,
                    &observation.attachments,
                    &mut exported_files,
                    cancelled,
                )?;
            }
            if saved.observed_at != saved.sent_at {
                writeln!(transcript, "Saved: {}\n", stamp(saved.observed_at))?;
            }
            if row.missing_after {
                writeln!(transcript, "*No later saved message yet.*\n")?;
            }
        }
        transcript.flush()?;
        transcript.get_ref().sync_all()?;
        drop(transcript);
        check_cancelled(cancelled)?;
        sync_directory(&output.join("media"))?;
        sync_directory(output)?;
        check_cancelled(cancelled)?;
        publish_directory(output, destination)?;
        sync_directory(&parent)?;
        Ok(())
    }

    fn write_deleted_attachments(
        &self,
        transcript: &mut impl Write,
        output: &Path,
        attachments: &[SavedAttachment],
        exported: &mut HashSet<String>,
        cancelled: &AtomicBool,
    ) -> Result<()> {
        for attachment in attachments {
            check_cancelled(cancelled)?;
            write!(
                transcript,
                "Attachment: **{}**",
                one_line(&attachment.status)
            )?;
            if attachment.status == "saved"
                && let Some(path) = &attachment.path
            {
                let relative = relative_media_path(self.root(), path)?;
                if exported.insert(relative.clone()) {
                    self.export_attachment_cancellable(path, &output.join(&relative), cancelled)?;
                }
                write!(
                    transcript,
                    " [Open attachment]({})",
                    markdown_link_path(&relative)
                )?;
            }
            if let Some(error) = &attachment.error {
                write!(transcript, " ({})", one_line(error))?;
            }
            writeln!(transcript, "\n")?;
        }
        Ok(())
    }

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

fn one_line(value: &str) -> String {
    escape_markdown(&value.split_whitespace().collect::<Vec<_>>().join(" "))
}

fn write_quoted(output: &mut impl Write, text: &str) -> Result<()> {
    if !text.is_empty() {
        for line in text.lines() {
            writeln!(output, "> {}", escape_markdown(line))?;
        }
        writeln!(output)?;
    }
    Ok(())
}

fn write_observation(output: &mut impl Write, observation: &SavedObservation) -> Result<()> {
    let label = match observation.kind.as_str() {
        "edited" => "Edit received",
        "deleted" => "Deletion received",
        "edit_requested" => "Edit requested",
        "edit_accepted" => "Edit accepted",
        "edit_uncertain" => "Edit outcome uncertain",
        "revoke_requested" => "Deletion requested",
        "revoke_accepted" => "Deletion accepted",
        "revoke_uncertain" => "Deletion outcome uncertain",
        "send_accepted" => "Send accepted",
        "send_failed" => "Send failed",
        "send_uncertain" => "Send outcome uncertain",
        _ => &observation.kind,
    };
    writeln!(
        output,
        "### {} · {}\n",
        one_line(label),
        stamp(observation.observed_at)
    )?;
    write_quoted(output, &observation.text)
}

fn markdown_link_path(path: &str) -> String {
    let mut encoded = String::new();
    for byte in path.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'-' | b'_' | b'.' | b'~') {
            encoded.push(char::from(byte));
        } else {
            encoded.push_str(&format!("%{byte:02X}"));
        }
    }
    encoded
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

#[cfg(test)]
mod deleted_export_tests {
    use super::*;
    use crate::backend::DeletedMessage;
    use crate::capture::view::SavedMessage;

    fn row(id: &str, deleted: bool) -> RetainedMessage {
        RetainedMessage {
            message: DeletedMessage {
                sender_id: "fixture@s.whatsapp.net".into(),
                canonical_chat: "fixture@g.us".into(),
                chat_name: "Synthetic group".into(),
                sender_name: "Synthetic sender".into(),
                reason: if deleted {
                    "Deleted for me".into()
                } else {
                    String::new()
                },
                saved: SavedMessage {
                    sequence: 1,
                    chat: "fixture@g.us".into(),
                    message_id: id.into(),
                    sender: "fixture@s.whatsapp.net".into(),
                    text: format!("Synthetic body {id}"),
                    sent_at: 100,
                    observed_at: 101,
                    outgoing: false,
                    observed_on_whatsapp: true,
                    send_accepted: false,
                    deletion_observed: deleted,
                    source: "synthetic".into(),
                    markdown_path: PathBuf::new(),
                    attachments: Vec::new(),
                    history: Vec::new(),
                    history_truncated: false,
                },
            },
            anchor: deleted,
            gap_before: false,
            missing_before: false,
            missing_after: false,
        }
    }

    #[test]
    fn deleted_chat_export_writes_one_transcript_with_context_and_complete_history() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(&directory.path().join("archive"), "export-fixture").unwrap();
        let mut before = row("before", false);
        before.missing_before = true;
        let mut deleted = row("deleted", true);
        deleted.message.saved.history = (0..300)
            .map(|index| SavedObservation {
                sequence: index + 2,
                kind: "edited".into(),
                text: format!("Synthetic revision {index}"),
                sender: "fixture@s.whatsapp.net".into(),
                sent_at: 102 + index,
                observed_at: 103 + index,
                source: "synthetic".into(),
                attachments: Vec::new(),
            })
            .collect();
        let mut after = row("after", false);
        after.gap_before = true;
        after.missing_after = true;
        let destination = directory.path().join("export");
        capture
            .export_deleted_readable_cancellable(
                &destination,
                "Synthetic group",
                &[before, deleted.clone(), deleted, after],
                &AtomicBool::new(false),
            )
            .unwrap();
        let transcript = fs::read_to_string(destination.join("chat.md")).unwrap();
        assert!(transcript.starts_with("# Synthetic group\n"));
        assert_eq!(transcript.matches("Synthetic body deleted").count(), 1);
        assert_eq!(transcript.matches(" · Context\n").count(), 2);
        assert_eq!(transcript.matches(" · Deleted\n").count(), 1);
        assert!(transcript.contains("Synthetic revision 299"));
        assert!(transcript.contains("No earlier saved message"));
        assert!(transcript.contains("No later saved message yet"));
        assert!(transcript.contains("Messages omitted between saved context windows"));
        assert_eq!(destination.read_dir().unwrap().count(), 2);
        assert!(!capture.root().join("chat.md").exists());
    }

    #[test]
    fn deleted_chat_export_deduplicates_and_decrypts_selected_attachments() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(&directory.path().join("archive"), "media-fixture").unwrap();
        let source = directory.path().join("source.txt");
        fs::write(&source, b"synthetic plaintext attachment").unwrap();
        let saved = media::copy_outgoing(&capture, &source).unwrap();
        fs::remove_file(&source).unwrap();
        let attachment = SavedAttachment {
            status: "saved".into(),
            path: Some(saved.clone()),
            error: None,
        };
        let mut first = row("first", true);
        let mut second = row("second", false);
        first.message.saved.attachments.push(attachment.clone());
        second.message.saved.attachments.push(attachment);
        let destination = directory.path().join("export");
        capture
            .export_deleted_readable_cancellable(
                &destination,
                "Group",
                &[first, second],
                &AtomicBool::new(false),
            )
            .unwrap();
        let relative = relative_media_path(capture.root(), &saved).unwrap();
        assert_eq!(
            fs::read(destination.join(&relative)).unwrap(),
            b"synthetic plaintext attachment"
        );
        assert_eq!(destination.join("media").read_dir().unwrap().count(), 1);
        assert_eq!(
            fs::read_to_string(destination.join("chat.md"))
                .unwrap()
                .matches("Open attachment")
                .count(),
            2
        );
    }

    #[test]
    fn deleted_chat_export_fails_closed_without_partial_publication() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(&directory.path().join("archive"), "fail-fixture").unwrap();
        let destination = directory.path().join("export");
        assert!(
            capture
                .export_deleted_readable_cancellable(
                    &destination,
                    "Group",
                    &[row("first", true)],
                    &AtomicBool::new(true)
                )
                .is_err()
        );
        assert!(!destination.exists());
        let mut truncated = row("truncated", true);
        truncated.message.saved.history_truncated = true;
        assert!(
            capture
                .export_deleted_readable_cancellable(
                    &destination,
                    "Group",
                    &[truncated],
                    &AtomicBool::new(false)
                )
                .is_err()
        );
        assert!(!destination.exists());
        let mut other_chat = row("other", true);
        other_chat.message.canonical_chat = "another@g.us".into();
        assert!(
            capture
                .export_deleted_readable_cancellable(
                    &destination,
                    "Group",
                    &[row("first", true), other_chat],
                    &AtomicBool::new(false)
                )
                .is_err()
        );
        assert!(!destination.exists());
        fs::create_dir(&destination).unwrap();
        fs::write(destination.join("existing.txt"), b"keep me").unwrap();
        assert!(
            capture
                .export_deleted_readable_cancellable(
                    &destination,
                    "Group",
                    &[row("first", true)],
                    &AtomicBool::new(false)
                )
                .is_err()
        );
        assert_eq!(
            fs::read(destination.join("existing.txt")).unwrap(),
            b"keep me"
        );
        assert_eq!(destination.read_dir().unwrap().count(), 1);
        assert!(
            capture
                .export_deleted_readable_cancellable(
                    &capture.root().join("export"),
                    "Group",
                    &[row("first", true)],
                    &AtomicBool::new(false)
                )
                .is_err()
        );
        assert!(!capture.root().join("export").exists());
        assert!(!directory.path().read_dir().unwrap().any(|item| {
            item.unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with(".Wassup-export-")
        }));
    }
}
