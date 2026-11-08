//! Explicit plaintext exports from the encrypted permanent archive.

use super::*;
use crate::backend::CaptureExportSelection;
use std::sync::atomic::{AtomicBool, Ordering};

#[derive(Default)]
pub(super) struct State {
    job: Option<Job>,
}

struct Job {
    request: u64,
    selection: CaptureExportSelection,
    chats: Vec<String>,
    cancelled: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for State {
    fn drop(&mut self) {
        if let Some(job) = &self.job {
            job.cancelled.store(true, Ordering::Release);
        }
    }
}

impl Worker {
    /// The protocol can report a successful sync even when our local write failed.
    /// Withhold permanent data until a fresh recovery persists its lock updates.
    pub(super) fn capture_lock_write_failed(&mut self) {
        self.deleted_context.cancel();
        self.privacy_confirmed = false;
        self.privacy_write_failed = true;
        self.privacy_snapshot = true;
        self.privacy_retry = Instant::now();
        self.recheck_capture_export();
        self.emit(Event::DeletedUnavailable);
    }

    pub(super) async fn stop_capture_export(&mut self) {
        let Some(job) = self.capture_export.job.take() else {
            return;
        };
        job.cancelled.store(true, Ordering::Release);
        let mut result = Ok(None);
        if let Some(mut task) = job.task {
            match tokio::time::timeout(Duration::from_secs(1), &mut task).await {
                Ok(Ok(())) => {}
                Ok(Err(_)) => result = Err("The export stopped unexpectedly".into()),
                Err(_) => {
                    task.abort();
                    result = Err("Export cancellation is waiting for storage to respond".into());
                }
            }
        }
        self.emit(Event::CaptureExportFinished {
            request: job.request,
            result,
        });
    }

    pub(super) fn cancel_capture_export_for(&self, chat: &str) {
        let canonical = self.canonical_str(chat);
        if let Some(job) = &self.capture_export.job
            && job.chats.iter().any(|id| id == chat || id == &canonical)
        {
            job.cancelled.store(true, Ordering::Release);
        }
    }

    /// Snapshot and identity updates can change locks without a single-chat event.
    pub(super) fn recheck_capture_export(&self) {
        if let Some(job) = &self.capture_export.job
            && self.capture_export_chats(&job.selection).is_err()
        {
            job.cancelled.store(true, Ordering::Release);
        }
    }

    /// An alias with a known lock blocks the complete chat, including its old identifiers.
    fn capture_export_chats(
        &self,
        selection: &CaptureExportSelection,
    ) -> anyhow::Result<Vec<String>> {
        anyhow::ensure!(
            self.chat_export.allowed && self.privacy_ready && self.privacy_confirmed,
            "Export is not available while locked or waiting for privacy settings"
        );
        let requested = match selection {
            CaptureExportSelection::Chats(chats) => chats.as_slice(),
            CaptureExportSelection::DeletedChat(chat) => std::slice::from_ref(chat),
            CaptureExportSelection::Attachment { chat, .. } => std::slice::from_ref(chat),
        };
        anyhow::ensure!(
            !requested.is_empty(),
            "No unlocked chats are available for export"
        );
        let mut allowed = Vec::new();
        for chat in requested {
            let canonical = self.canonical_str(chat);
            let mut aliases = vec![chat.clone(), canonical.clone()];
            if let Some(pn) = canonical.strip_suffix("@s.whatsapp.net") {
                aliases.extend(
                    self.lid_to_pn
                        .iter()
                        .filter(|(_, mapped)| mapped.as_str() == pn)
                        .map(|(lid, _)| format!("{lid}@lid")),
                );
            }
            if self.is_me(&canonical) {
                aliases.extend(self.me_pn.iter().cloned());
                aliases.extend(self.me_lid.iter().cloned());
            }
            let mut known = false;
            for alias in &aliases {
                if let Some(chat) = self.archive.chat(alias)? {
                    anyhow::ensure!(!chat.locked, "A selected chat is locked");
                    known = true;
                }
            }
            anyhow::ensure!(known, "A selected chat is no longer available");
            allowed.extend(aliases);
        }
        allowed.sort_unstable();
        allowed.dedup();
        Ok(allowed)
    }

    /// No decrypted content is created while this native picker is open.
    pub(super) async fn begin_capture_export(
        &mut self,
        request: u64,
        selection: CaptureExportSelection,
    ) {
        self.stop_capture_export().await;
        let chats = match self.capture_export_chats(&selection) {
            Ok(chats) if self.capture.is_some() => chats,
            _ => {
                self.emit(Event::CaptureExportFinished {
                    request,
                    result: Err("Only available, unlocked chats can be exported".into()),
                });
                return;
            }
        };
        if let CaptureExportSelection::Attachment { path, .. } = &selection
            && !self
                .capture
                .as_ref()
                .is_some_and(|capture| capture.attachment_in_chats(path, &chats).unwrap_or(false))
        {
            self.emit(Event::CaptureExportFinished {
                request,
                result: Err("This saved attachment is not available for export".into()),
            });
            return;
        }
        let picker_selection = selection.clone();
        self.capture_export.job = Some(Job {
            request,
            selection,
            chats,
            cancelled: Arc::new(AtomicBool::new(false)),
            task: None,
        });
        let commands = self.commands.clone();
        tokio::task::spawn_blocking(move || {
            let target = match picker_selection {
                CaptureExportSelection::Chats(_) => rfd::FileDialog::new()
                    .set_title("Choose a folder for a plaintext archive export")
                    .pick_folder(),
                CaptureExportSelection::DeletedChat(_) => rfd::FileDialog::new()
                    .set_title("Export chat deletions and context as plaintext")
                    .pick_folder(),
                CaptureExportSelection::Attachment { path, .. } => rfd::FileDialog::new()
                    .set_title("Save a plaintext attachment copy")
                    .set_file_name(
                        path.file_name()
                            .and_then(|name| name.to_str())
                            .unwrap_or("attachment"),
                    )
                    .save_file(),
            };
            let _ = commands.send(Command::CaptureExportTarget { request, target });
        });
    }

    /// Recheck current permissions after the picker and before starting the decrypting writer.
    pub(super) fn capture_export_target(&mut self, request: u64, target: Option<PathBuf>) {
        let Some(job) = self
            .capture_export
            .job
            .as_ref()
            .filter(|job| job.request == request && job.task.is_none())
        else {
            return;
        };
        if job.cancelled.load(Ordering::Acquire) || target.is_none() {
            self.capture_export_finished(request, Ok(None));
            return;
        }
        let selection = job.selection.clone();
        let cancelled = job.cancelled.clone();
        let chats = match self.capture_export_chats(&selection) {
            Ok(chats) => chats,
            Err(_) => {
                self.capture_export_finished(
                    request,
                    Err("Export cancelled because a chat is locked or unavailable".into()),
                );
                return;
            }
        };
        let Some(capture) = self.capture.clone() else {
            self.capture_export_finished(
                request,
                Err("The encrypted archive is unavailable".into()),
            );
            return;
        };
        if let CaptureExportSelection::Attachment { path, .. } = &selection
            && !capture.attachment_in_chats(path, &chats).unwrap_or(false)
        {
            self.capture_export_finished(
                request,
                Err("This saved attachment is not available for export".into()),
            );
            return;
        }
        let target = target.expect("checked destination");
        let encrypted_root = capture
            .root()
            .parent()
            .and_then(Path::parent)
            .unwrap_or(capture.root())
            .to_path_buf();
        let roots = [self.dirs.state.clone(), self.dirs.cache.clone()];
        let archive_path = self.dirs.archive_db();
        let identity = self.deleted_identity();
        let syncing = self.syncing;
        let commands = self.commands.clone();
        let job = self.capture_export.job.as_mut().expect("current export");
        job.chats = chats.clone();
        job.task = Some(tokio::task::spawn_blocking(move || {
            let outcome =
                (|| -> anyhow::Result<PathBuf> {
                    anyhow::ensure!(!cancelled.load(Ordering::Acquire), "Export cancelled");
                    let parent = match &selection {
                        CaptureExportSelection::Chats(_)
                        | CaptureExportSelection::DeletedChat(_) => target.as_path(),
                        CaptureExportSelection::Attachment { .. } => target
                            .parent()
                            .ok_or_else(|| anyhow::anyhow!("Choose a folder"))?,
                    }
                    .canonicalize()?;
                    anyhow::ensure!(
                        !parent.starts_with(&encrypted_root),
                        "Choose a folder outside the encrypted archive"
                    );
                    for root in &roots {
                        let root = root.parent().and_then(Path::parent).unwrap_or(root);
                        if let Ok(root) = root.canonicalize() {
                            anyhow::ensure!(
                                !parent.starts_with(root),
                                "Choose a folder outside application data"
                            );
                        }
                    }
                    anyhow::ensure!(!cancelled.load(Ordering::Acquire), "Export cancelled");
                    let destination = match &selection {
                        CaptureExportSelection::Chats(_)
                        | CaptureExportSelection::DeletedChat(_) => parent.join(format!(
                            "Wassup export-{}-{:08x}",
                            crate::util::now(),
                            rand::random::<u32>()
                        )),
                        CaptureExportSelection::Attachment { .. } => parent.join(
                            target
                                .file_name()
                                .ok_or_else(|| anyhow::anyhow!("Choose a file name"))?,
                        ),
                    };
                    match selection {
                        CaptureExportSelection::Chats(_) => {
                            capture.export_readable_cancellable(&destination, &chats, &cancelled)?
                        }
                        CaptureExportSelection::DeletedChat(chat) => {
                            let archive = Archive::export_reader(&archive_path)?;
                            let _snapshot = archive.read_snapshot()?;
                            let rows = super::deleted::project_deleted_chat(
                                &capture,
                                &archive,
                                &identity,
                                &chat,
                                super::deleted::ProjectionOptions {
                                    anchors: None,
                                    syncing,
                                    now: crate::util::now(),
                                    full_history: true,
                                    cancelled: &cancelled,
                                },
                            )?;
                            let title = rows
                                .first()
                                .map(|row| row.message.chat_name.as_str())
                                .unwrap_or("Saved chat deletions");
                            capture.export_deleted_readable_cancellable(
                                &destination,
                                title,
                                &rows,
                                &cancelled,
                            )?;
                        }
                        CaptureExportSelection::Attachment { path, .. } => capture
                            .export_attachment_cancellable(&path, &destination, &cancelled)?,
                    }
                    Ok(destination)
                })();
            let result = match outcome {
                Ok(path) => Ok(Some(path)),
                Err(_) if cancelled.load(Ordering::Acquire) => Ok(None),
                // Storage errors may contain private paths or journal data. Do not log them.
                Err(_) => Err("Could not export. Choose a new destination outside application data and check free space".into()),
            };
            let _ = commands.send(Command::CaptureExportFinished { request, result });
        }));
    }

    pub(super) fn capture_export_finished(
        &mut self,
        request: u64,
        result: Result<Option<PathBuf>, String>,
    ) {
        if self
            .capture_export
            .job
            .as_ref()
            .is_some_and(|job| job.request == request)
        {
            self.capture_export.job = None;
            self.emit(Event::CaptureExportFinished { request, result });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::receipt_tests::{PEER, own_message, worker};
    use super::*;

    fn job(request: u64, chat: &str) -> Job {
        Job {
            request,
            selection: CaptureExportSelection::Chats(vec![chat.into()]),
            chats: vec![chat.into()],
            cancelled: Arc::new(AtomicBool::new(false)),
            task: None,
        }
    }

    #[test]
    fn deleted_chat_export_uses_all_chat_deletions_and_shared_context() {
        let (worker, _, _, _) = worker();
        let chat = "500@g.us";
        worker.archive.ensure_chat(chat, "Synthetic group").unwrap();
        let capture = worker.capture.as_ref().unwrap();
        for index in 0..114 {
            let id = format!("message-{index}");
            let text = format!("Synthetic message {index}.");
            let sender = if index % 2 == 0 {
                "111@s.whatsapp.net"
            } else {
                "222@s.whatsapp.net"
            };
            capture
                .record_event(crate::capture::CaptureEvent {
                    event_id: id.clone(),
                    chat: chat.into(),
                    sender: sender.into(),
                    message_id: id.clone(),
                    kind: "message".into(),
                    text: text.clone(),
                    timestamp: 1000 + index,
                    direction: "incoming".into(),
                    source: "synthetic".into(),
                    raw: wa::Message::text(text).encode_to_vec(),
                    ..Default::default()
                })
                .unwrap();
            if (2..112).contains(&index) {
                worker.archive.delete_message(chat, &id).unwrap();
            } else {
                let mut message = own_message(&id, 1000 + index);
                message.chat = chat.into();
                message.sender = sender.into();
                message.from_me = false;
                worker.archive.insert_message(&message, None).unwrap();
            }
        }
        for index in 0..300 {
            capture
                .record_event(crate::capture::CaptureEvent {
                    event_id: format!("edit-{index}"),
                    chat: chat.into(),
                    message_id: format!("edit-{index}"),
                    target_id: Some("message-2".into()),
                    sender: "111@s.whatsapp.net".into(),
                    kind: "edited".into(),
                    text: format!("Synthetic revision {index}."),
                    timestamp: 2000 + index,
                    direction: "incoming".into(),
                    source: "synthetic".into(),
                    ..Default::default()
                })
                .unwrap();
        }
        let cancelled = AtomicBool::new(false);
        let rows = super::super::deleted::project_deleted_chat(
            capture,
            &worker.archive,
            &worker.deleted_identity(),
            chat,
            super::super::deleted::ProjectionOptions {
                anchors: None,
                syncing: false,
                now: crate::util::now(),
                full_history: true,
                cancelled: &cancelled,
            },
        )
        .unwrap();
        assert_eq!(rows.iter().filter(|row| row.anchor).count(), 110);
        assert_eq!(rows.len(), 112);
        assert_eq!(rows.first().unwrap().message.saved.message_id, "message-1");
        assert_eq!(rows.last().unwrap().message.saved.message_id, "message-112");
        let edited = rows
            .iter()
            .find(|row| row.message.saved.message_id == "message-2")
            .unwrap();
        assert_eq!(edited.message.saved.history.len(), 300);
        assert!(!edited.message.saved.history_truncated);
        let output = tempfile::tempdir().unwrap();
        let destination = output.path().join("export");
        capture
            .export_deleted_readable_cancellable(&destination, "Synthetic group", &rows, &cancelled)
            .unwrap();
        let transcript = std::fs::read_to_string(destination.join("chat.md")).unwrap();
        assert!(!transcript.contains("Synthetic message 0."));
        assert!(!transcript.contains("Synthetic message 113."));
        assert!(transcript.contains("Synthetic message 1."));
        assert!(transcript.contains("Synthetic message 112."));
        assert!(transcript.contains("Synthetic revision 299."));
        assert_eq!(transcript.matches(" · Deleted\n").count(), 110);
        assert_eq!(transcript.matches(" · Context\n").count(), 2);
    }

    #[test]
    fn deleted_chat_export_checks_alias_locks_and_cancels_before_writing() {
        let (mut worker, events, _, _) = worker();
        worker.chat_export.allowed = true;
        let selection = CaptureExportSelection::DeletedChat(PEER.into());
        assert!(worker.capture_export_chats(&selection).is_err());
        worker.archive.ensure_chat(PEER, "Synthetic chat").unwrap();
        assert!(worker.capture_export_chats(&selection).is_ok());
        worker
            .lid_to_pn
            .insert("123".into(), PEER.split('@').next().unwrap().into());
        worker
            .archive
            .ensure_chat("123@lid", "Synthetic alias")
            .unwrap();
        let mut pending = job(30, PEER);
        pending.selection = selection.clone();
        worker.capture_export.job = Some(pending);
        worker.archive.set_locked("123@lid", true).unwrap();
        assert!(worker.capture_export_chats(&selection).is_err());
        worker.recheck_capture_export();
        let output = tempfile::tempdir().unwrap();
        worker.capture_export_target(30, Some(output.path().to_owned()));
        assert_eq!(output.path().read_dir().unwrap().count(), 0);
        assert!(worker.capture_export.job.is_none());
        assert!(events.try_iter().any(|event| matches!(
            event,
            Event::CaptureExportFinished {
                request: 30,
                result: Ok(None)
            }
        )));
    }

    #[test]
    fn unknown_locked_and_locked_alias_chats_cannot_be_exported() {
        let (mut worker, _, _, _) = worker();
        worker.chat_export.allowed = true;
        worker.privacy_ready = true;
        let selection = CaptureExportSelection::Chats(vec![PEER.into()]);
        assert!(worker.capture_export_chats(&selection).is_err());
        worker.archive.ensure_chat(PEER, "Synthetic chat").unwrap();
        assert!(worker.capture_export_chats(&selection).is_ok());
        worker
            .lid_to_pn
            .insert("123".into(), PEER.split('@').next().unwrap().into());
        worker
            .archive
            .ensure_chat("123@lid", "Synthetic alias")
            .unwrap();
        worker.archive.set_locked("123@lid", true).unwrap();
        assert!(worker.capture_export_chats(&selection).is_err());
        worker.archive.set_locked("123@lid", false).unwrap();
        assert!(
            worker
                .capture_export_chats(&selection)
                .unwrap()
                .contains(&"123@lid".to_owned())
        );
        worker.privacy_ready = false;
        assert!(worker.capture_export_chats(&selection).is_err());
        worker.privacy_ready = true;
        assert!(
            worker
                .capture_export_chats(&CaptureExportSelection::Chats(Vec::new()))
                .is_err()
        );
        worker.chat_export.allowed = false;
        assert!(worker.capture_export_chats(&selection).is_err());
    }

    #[test]
    fn cancelling_a_picker_creates_no_output_and_ignores_late_results() {
        let (mut worker, events, _, _) = worker();
        worker.capture_export.job = Some(job(7, PEER));
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("not-created");
        worker.cancel_capture_export_for(PEER);
        worker.capture_export_target(7, Some(target.clone()));
        assert!(worker.capture_export.job.is_none());
        assert!(matches!(
            events.try_recv().unwrap(),
            Event::CaptureExportFinished {
                request: 7,
                result: Ok(None)
            }
        ));
        worker.capture_export_target(7, Some(target.clone()));
        assert!(events.try_recv().is_err());
        assert!(!target.exists());
    }

    #[test]
    fn a_chat_lock_after_the_picker_opens_prevents_export() {
        let (mut worker, events, _, _) = worker();
        worker.chat_export.allowed = true;
        worker.privacy_ready = true;
        worker.archive.ensure_chat(PEER, "Synthetic chat").unwrap();
        worker.capture_export.job = Some(job(9, PEER));
        worker.archive.set_locked(PEER, true).unwrap();
        let directory = tempfile::tempdir().unwrap();
        worker.capture_export_target(9, Some(directory.path().to_owned()));
        assert!(worker.capture_export.job.is_none());
        assert!(matches!(
            events.try_recv().unwrap(),
            Event::CaptureExportFinished {
                request: 9,
                result: Err(_)
            }
        ));
        assert_eq!(directory.path().read_dir().unwrap().count(), 0);
    }

    #[test]
    fn a_bulk_privacy_update_cancels_an_active_export() {
        let (mut worker, _, _, _) = worker();
        worker.chat_export.allowed = true;
        worker.privacy_ready = true;
        worker.archive.ensure_chat(PEER, "Synthetic chat").unwrap();
        worker.capture_export.job = Some(job(11, PEER));
        let cancelled = worker
            .capture_export
            .job
            .as_ref()
            .unwrap()
            .cancelled
            .clone();
        worker.recheck_capture_export();
        assert!(!cancelled.load(Ordering::Acquire));
        worker.archive.set_locked(PEER, true).unwrap();
        worker.emit_chats();
        assert!(cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn unconfirmed_fallback_blocks_exports_and_cancels_an_existing_export() {
        let (mut worker, _, _, _) = worker();
        worker.chat_export.allowed = true;
        worker.privacy_ready = true;
        worker.archive.ensure_chat(PEER, "Synthetic chat").unwrap();
        let selection = CaptureExportSelection::Chats(vec![PEER.into()]);
        assert!(worker.capture_export_chats(&selection).is_ok());
        worker.capture_export.job = Some(job(12, PEER));
        let cancelled = worker
            .capture_export
            .job
            .as_ref()
            .unwrap()
            .cancelled
            .clone();
        worker.privacy_confirmed = false;
        assert!(worker.privacy_ready, "ordinary chats remain available");
        assert!(worker.capture_export_chats(&selection).is_err());
        worker.emit_chats();
        assert!(cancelled.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn failed_phone_and_local_lock_writes_withhold_permanent_data() {
        for from_phone in [false, true] {
            let (mut worker, events, _, _) = worker();
            worker.chat_export.allowed = true;
            worker.privacy_ready = true;
            worker.archive.ensure_chat(PEER, "Synthetic chat").unwrap();
            worker.capture_export.job = Some(job(15, PEER));
            let cancelled = worker
                .capture_export
                .job
                .as_ref()
                .unwrap()
                .cancelled
                .clone();
            worker.archive.execute_for_tests("PRAGMA query_only=ON");
            if from_phone {
                let lock = wa_events::LockChatUpdate::builder()
                    .jid(PEER.parse().unwrap())
                    .timestamp(whatsapp_rust::wacore::time::now_utc())
                    .from_full_sync(true)
                    .action(Box::new({
                        let mut action = wa::sync_action_value::LockChatAction::default();
                        action.locked = Some(true);
                        action
                    }))
                    .build();
                worker
                    .handle_wa_event(Arc::new(wa_events::Event::LockChatUpdate(lock)))
                    .await;
            } else {
                worker
                    .handle_command(Command::SetLocked(PEER.into(), true))
                    .await;
            }
            assert!(
                !worker.archive.chat(PEER).unwrap().unwrap().locked,
                "the database rejected the write"
            );
            assert!(
                cancelled.load(Ordering::Acquire),
                "cancellation precedes persistence"
            );
            assert!(worker.privacy_ready, "ordinary chat behavior is unchanged");
            assert!(!worker.privacy_confirmed && worker.privacy_write_failed);
            assert!(
                worker.privacy_snapshot,
                "retry must replay the lost mutation"
            );
            assert!(
                events
                    .try_iter()
                    .any(|event| matches!(event, Event::DeletedUnavailable))
            );
            let selection = CaptureExportSelection::Chats(vec![PEER.into()]);
            assert!(worker.capture_export_chats(&selection).is_err());
            worker.load_deleted(16, None);
            assert!(events.try_iter().any(|event| matches!(event,
                Event::DeletedLoaded { request: 16, messages, error: Some(_), .. }
                    if messages.is_empty())));
            worker.preferences_recovered(worker.privacy_generation, true, true);
            assert!(
                !worker.privacy_confirmed,
                "network success cannot override failed local writes"
            );
            worker.archive.execute_for_tests("PRAGMA query_only=OFF");
            // Model a fresh recovery's reset and successful persisted replay.
            worker.privacy_write_failed = false;
            worker.archive.set_locked(PEER, true).unwrap();
            worker.preferences_recovered(worker.privacy_generation, true, true);
            assert!(worker.privacy_confirmed);
            assert!(
                worker.capture_export_chats(&selection).is_err(),
                "the recovered lock is enforced"
            );
        }
    }
}
