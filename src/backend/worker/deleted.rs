//! Compare permanent originals with this account's mutable chat archive.
//!
//! App-state delete-for-me and chat-clear updates do not carry a revoke
//! message. Their durable removal barriers are stronger evidence than a
//! missing row. Missing rows alone never identify who removed a message.

use super::*;
use crate::backend::{DeletedMessage, RetainedMessage};
use crate::capture::view::SavedMessage;
use std::sync::atomic::{AtomicBool, Ordering};

const CANDIDATES_PER_PAGE: usize = 100;
const CANDIDATE_PAGES_PER_REQUEST: usize = 10;
const DELETED_PER_PAGE: usize = 100;
const MATERIALIZATION_GRACE_SECONDS: i64 = 5;
const CONTEXT_ERROR: &str = "Could not load saved context. Check chat privacy and try again";

#[derive(Default)]
pub(super) struct State {
    generation: u64,
    job: Option<ContextJob>,
}

struct ContextJob {
    request: u64,
    generation: u64,
    privacy_generation: u64,
    capture_root: PathBuf,
    identity: DeletedIdentity,
    chats: Vec<String>,
    cancelled: Arc<AtomicBool>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl State {
    pub(super) fn cancel(&mut self) {
        if let Some(job) = self.job.take() {
            // Aborting the async waiter does not stop its blocking task.
            // Projection queries also observe this shared cancellation flag.
            job.cancelled.store(true, Ordering::Release);
            if let Some(task) = job.task {
                task.abort();
            }
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        self.cancel();
    }
}

impl Worker {
    pub(super) fn load_deleted(&self, request: u64, before: Option<i64>) {
        let result = (|| -> anyhow::Result<_> {
            anyhow::ensure!(
                self.privacy_ready && self.privacy_confirmed,
                "Waiting for chat privacy settings before showing saved messages"
            );
            let capture = self
                .capture
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("The permanent archive is not available"))?;
            let now = crate::util::now();
            let identity = self.deleted_identity();
            let mut readers = HashMap::new();
            let mut messages = Vec::new();
            let mut cursor = before;
            // Ordinary messages and protocol controls can fill many capture
            // pages without one deletion. Page the filtered result, while
            // yielding to the worker after a bounded amount of scanning. An
            // empty response with a cursor asks the UI to continue scanning.
            for _ in 0..CANDIDATE_PAGES_PER_REQUEST {
                let page = capture.saved_messages(cursor, CANDIDATES_PER_PAGE)?;
                let mut candidates = page.messages.into_iter().peekable();
                while let Some(saved) = candidates.next() {
                    // A protocol echo under a PN/LID alias is one original.
                    // Keep its first capture cursor and merge its evidence.
                    let canonical = identity.canonical(&saved.chat);
                    let aliases = identity.aliases(&saved.chat);
                    let saved = if aliases.len() > 1 {
                        let reader = match readers.entry(canonical) {
                            std::collections::hash_map::Entry::Occupied(entry) => entry.into_mut(),
                            std::collections::hash_map::Entry::Vacant(entry) => {
                                entry.insert(capture.saved_chat_reader(&aliases)?)
                            }
                        };
                        let Some(merged) = reader.original(&saved.message_id)? else {
                            continue;
                        };
                        if merged.sequence != saved.sequence {
                            continue;
                        }
                        merged
                    } else {
                        saved
                    };
                    if let Some(message) =
                        classify_saved(&self.archive, &identity, saved, self.syncing, now)?
                            .filter(|row| !row.reason.is_empty())
                    {
                        let sequence = message.saved.sequence;
                        messages.push(message);
                        if messages.len() == DELETED_PER_PAGE {
                            // A full result can stop inside a candidate page.
                            // Resume after the last returned original, not at
                            // that page's end, so no older result is skipped.
                            let next = (candidates.peek().is_some() || page.next_before.is_some())
                                .then_some(sequence);
                            return Ok((messages, next));
                        }
                    }
                }
                cursor = page.next_before;
                if cursor.is_none() {
                    break;
                }
            }
            Ok((messages, cursor))
        })();

        let (messages, next_before, error) = match result {
            Ok((messages, next_before)) => (messages, next_before, None),
            Err(_) => {
                let message = if !self.privacy_ready || !self.privacy_confirmed {
                    "Waiting for chat privacy settings before showing saved messages"
                } else if self.capture.is_none() {
                    "The permanent archive is not available"
                } else {
                    // Database errors may include paths or journal data. Keep
                    // both this response and the logs free of private details.
                    "Could not read saved messages. Try again"
                };
                (Vec::new(), None, Some(message.to_owned()))
            }
        };
        self.emit(Event::DeletedLoaded {
            request,
            messages,
            next_before,
            error,
        });
    }

    pub(super) fn deleted_identity(&self) -> DeletedIdentity {
        DeletedIdentity {
            lid_to_pn: self.lid_to_pn.clone(),
            me_pn: self.me_pn.clone(),
            me_lid: self.me_lid.clone(),
        }
    }

    pub(super) fn load_deleted_context(&mut self, request: u64, anchors: Vec<i64>) {
        self.deleted_context.cancel();
        let prepared = (|| -> anyhow::Result<_> {
            anyhow::ensure!(
                self.privacy_ready && self.privacy_confirmed,
                "Privacy is unconfirmed"
            );
            let capture = self
                .capture
                .clone()
                .ok_or_else(|| anyhow::anyhow!("Archive is unavailable"))?;
            let identity = self.deleted_identity();
            let mut chats = BTreeMap::<String, Vec<i64>>::new();
            // One indexed metadata read, without hydrating bodies or histories,
            // binds requested anchors to their account-owned source chats.
            for (sequence, chat) in capture.saved_original_locations(&anchors)? {
                chats
                    .entry(identity.canonical(&chat))
                    .or_default()
                    .push(sequence);
            }
            for chat in chats.keys() {
                ensure_chat_available(&self.archive, &identity.aliases(chat))?;
            }
            Ok((capture, identity, chats))
        })();
        let (capture, identity, chats) = match prepared {
            Ok(prepared) => prepared,
            Err(_) => {
                self.emit(Event::DeletedContextLoaded {
                    request,
                    rows: Vec::new(),
                    error: Some(CONTEXT_ERROR.into()),
                });
                return;
            }
        };
        if chats.is_empty() {
            self.emit(Event::DeletedContextLoaded {
                request,
                rows: Vec::new(),
                error: None,
            });
            return;
        }
        self.deleted_context.generation = self.deleted_context.generation.wrapping_add(1);
        let generation = self.deleted_context.generation;
        let cancelled = Arc::new(AtomicBool::new(false));
        self.deleted_context.job = Some(ContextJob {
            request,
            generation,
            privacy_generation: self.privacy_generation,
            capture_root: capture.root().to_path_buf(),
            identity: identity.clone(),
            chats: chats.keys().cloned().collect(),
            cancelled: cancelled.clone(),
            task: None,
        });
        let archive_path = self.dirs.archive_db();
        let syncing = self.syncing;
        let now = crate::util::now();
        let commands = self.commands.clone();
        let task = tokio::spawn(async move {
            let projected =
                tokio::task::spawn_blocking(move || -> anyhow::Result<Vec<RetainedMessage>> {
                    anyhow::ensure!(!cancelled.load(Ordering::Acquire), "Projection cancelled");
                    let archive = Archive::export_reader(&archive_path)?;
                    let _snapshot = archive.read_snapshot()?;
                    let mut rows = Vec::new();
                    for (chat, anchors) in chats {
                        rows.extend(project_deleted_chat(
                            &capture,
                            &archive,
                            &identity,
                            &chat,
                            ProjectionOptions {
                                anchors: Some(&anchors),
                                syncing,
                                now,
                                full_history: false,
                                cancelled: &cancelled,
                            },
                        )?);
                    }
                    Ok(rows)
                })
                .await;
            let (rows, failed) = match projected {
                Ok(Ok(rows)) => (rows, false),
                _ => (Vec::new(), true),
            };
            let _ = commands.send(Command::DeletedContextReady {
                request,
                generation,
                rows,
                failed,
            });
        });
        self.deleted_context
            .job
            .as_mut()
            .expect("current context job")
            .task = Some(task);
    }

    /// A background snapshot never grants access. Current account ownership,
    /// identity mappings and every alias's privacy state are checked again.
    pub(super) fn deleted_context_ready(
        &mut self,
        request: u64,
        generation: u64,
        mut rows: Vec<RetainedMessage>,
        failed: bool,
    ) {
        let Some(job) = self
            .deleted_context
            .job
            .as_ref()
            .filter(|job| job.request == request && job.generation == generation)
        else {
            return;
        };
        let identity = self.deleted_identity();
        let authorized = !failed
            && !job.cancelled.load(Ordering::Acquire)
            && self.privacy_ready
            && self.privacy_confirmed
            && job.privacy_generation == self.privacy_generation
            && self
                .capture
                .as_ref()
                .is_some_and(|capture| capture.root() == job.capture_root.as_path())
            && identity == job.identity
            && job
                .chats
                .iter()
                .all(|chat| ensure_chat_available(&self.archive, &identity.aliases(chat)).is_ok())
            && rows.iter().all(|row| {
                job.chats.contains(&row.message.canonical_chat)
                    && identity.canonical(&row.message.saved.chat) == row.message.canonical_chat
            });
        self.deleted_context.job.take();
        let error = if authorized {
            None
        } else {
            rows.clear();
            Some(CONTEXT_ERROR.into())
        };
        self.emit(Event::DeletedContextLoaded {
            request,
            rows,
            error,
        });
    }

    pub(super) fn load_deleted_history(
        &self,
        request: u64,
        chat: String,
        message: String,
        after: i64,
    ) {
        let result = (|| -> anyhow::Result<_> {
            anyhow::ensure!(
                self.privacy_ready && self.privacy_confirmed,
                "Privacy is unconfirmed"
            );
            let capture = self
                .capture
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("Archive is unavailable"))?;
            let identity = self.deleted_identity();
            let aliases = identity.aliases(&chat);
            ensure_chat_available(&self.archive, &aliases)?;
            let reader = capture.saved_chat_reader(&aliases)?;
            let saved = reader
                .original(&message)?
                .ok_or_else(|| anyhow::anyhow!("Original is unavailable"))?;
            anyhow::ensure!(
                crate::capture::view::accepted_original(&saved),
                "Original was not sent"
            );
            reader.history(&message, after, 256)
        })();
        let (observations, has_more, error) = match result {
            Ok(page) => (page.observations, page.has_more, None),
            Err(_) => (
                Vec::new(),
                false,
                Some("Could not load saved revisions. Check chat privacy and try again".into()),
            ),
        };
        self.emit(Event::DeletedHistoryLoaded {
            request,
            chat,
            message,
            after,
            observations,
            has_more,
            error,
        });
    }

    #[cfg(test)]
    fn classify_deleted(
        &self,
        saved: SavedMessage,
        now: i64,
    ) -> anyhow::Result<Option<DeletedMessage>> {
        if !self.privacy_ready || !self.privacy_confirmed {
            return Ok(None);
        }
        Ok(classify_saved(
            &self.archive,
            &self.deleted_identity(),
            saved,
            self.syncing,
            now,
        )?
        .filter(|row| !row.reason.is_empty()))
    }
}

/// Immutable identity inputs shared by UI projections and export tasks.
#[derive(Clone, PartialEq, Eq)]
pub(super) struct DeletedIdentity {
    lid_to_pn: HashMap<String, String>,
    me_pn: Option<String>,
    me_lid: Option<String>,
}

impl DeletedIdentity {
    fn me(&self) -> String {
        self.me_pn
            .clone()
            .or_else(|| self.me_lid.clone())
            .filter(|id| !id.trim().is_empty())
            .unwrap_or_else(|| "__self__".into())
    }

    pub(super) fn canonical(&self, id: &str) -> String {
        let Ok(jid) = id.parse::<Jid>() else {
            return id.to_owned();
        };
        let id = if jid.is_lid()
            && let Some(pn) = self.lid_to_pn.get(jid.user_base())
        {
            format!("{pn}@s.whatsapp.net")
        } else {
            jid.to_non_ad_string()
        };
        if self.me_pn.as_deref() == Some(&id) || self.me_lid.as_deref() == Some(&id) {
            self.me()
        } else {
            id
        }
    }

    pub(super) fn aliases(&self, chat: &str) -> Vec<String> {
        let canonical = self.canonical(chat);
        let mut aliases = vec![chat.to_owned(), canonical.clone()];
        if let Some(pn) = canonical.strip_suffix("@s.whatsapp.net") {
            aliases.extend(
                self.lid_to_pn
                    .iter()
                    .filter(|(_, mapped)| mapped.as_str() == pn)
                    .map(|(lid, _)| format!("{lid}@lid")),
            );
        }
        if canonical == self.me() {
            aliases.extend(self.me_pn.iter().cloned());
            aliases.extend(self.me_lid.iter().cloned());
        }
        aliases.sort_unstable();
        aliases.dedup();
        aliases
    }
}

fn ensure_chat_available(archive: &Archive, aliases: &[String]) -> anyhow::Result<()> {
    let mut known = false;
    for alias in aliases {
        if let Some(chat) = archive.chat(alias)? {
            anyhow::ensure!(!chat.locked, "Chat is locked");
            known = true;
        }
    }
    anyhow::ensure!(known, "Chat privacy metadata is unavailable");
    Ok(())
}

/// `reason` is empty for ordinary context; `None` is never safe to expose.
fn classify_saved(
    archive: &Archive,
    identity: &DeletedIdentity,
    saved: SavedMessage,
    syncing: bool,
    now: i64,
) -> anyhow::Result<Option<DeletedMessage>> {
    let canonical_chat = identity.canonical(&saved.chat);
    let aliases = identity.aliases(&saved.chat);
    let mut chat_name = None;
    let mut present = false;
    let mut revoked = false;
    let mut removed = false;
    let mut cleared = false;
    let mut sender_name = None;
    for alias in &aliases {
        if let Some(chat) = archive.chat(alias)? {
            if chat.locked {
                return Ok(None);
            }
            if chat_name.is_none() || alias == &canonical_chat {
                chat_name = Some(chat.name);
            }
        }
        if let Some(message) = archive.message(alias, &saved.message_id)? {
            present = true;
            revoked |= matches!(message.content, Content::Revoked);
            sender_name = sender_name.or(message.sender_name);
        }
        removed |= archive.message_removed(alias, &saved.message_id)?;
        cleared |= archive
            .removal_point(alias)?
            .is_some_and(|through| saved.sent_at <= through);
    }
    let Some(chat_name) = chat_name else {
        return Ok(None);
    };
    if !crate::capture::view::accepted_original(&saved) {
        return Ok(None);
    }
    let reason = if saved.deletion_observed {
        "Deleted for everyone"
    } else if removed {
        "Deleted for me"
    } else if cleared {
        "Removed when this chat was cleared or deleted"
    } else if revoked {
        "Shown as deleted in this chat"
    } else if !present
        && !syncing
        && now.saturating_sub(saved.observed_at) >= MATERIALIZATION_GRACE_SECONDS
    {
        "No longer in chat"
    } else {
        ""
    };
    let sender_id = if saved.outgoing {
        identity.me()
    } else {
        identity.canonical(&saved.sender)
    };
    let sender_name = if saved.outgoing {
        "You".to_owned()
    } else {
        sender_name
            .or(archive
                .contact(&sender_id)?
                .and_then(|contact| contact.label()))
            .or_else(|| crate::model::phone_of(&sender_id).map(crate::util::phone))
            .unwrap_or_else(|| fallback_name(&saved.sender))
    };
    Ok(Some(DeletedMessage {
        saved,
        canonical_chat,
        chat_name,
        sender_id,
        sender_name,
        reason: reason.into(),
    }))
}

/// The UI and export use exactly the same anchors, classification and context.
/// `anchors=None` exports every saved deletion, regardless of UI filters/pages.
pub(super) struct ProjectionOptions<'a> {
    pub anchors: Option<&'a [i64]>,
    pub syncing: bool,
    pub now: i64,
    pub full_history: bool,
    pub cancelled: &'a AtomicBool,
}

pub(super) fn project_deleted_chat(
    capture: &crate::capture::Capture,
    archive: &Archive,
    identity: &DeletedIdentity,
    chat: &str,
    options: ProjectionOptions<'_>,
) -> anyhow::Result<Vec<RetainedMessage>> {
    let ProjectionOptions {
        anchors,
        syncing,
        now,
        full_history,
        cancelled,
    } = options;
    let check = || -> anyhow::Result<()> {
        anyhow::ensure!(!cancelled.load(Ordering::Acquire), "Projection cancelled");
        Ok(())
    };
    check()?;
    let aliases = identity.aliases(chat);
    ensure_chat_available(archive, &aliases)?;
    let reader = capture.saved_chat_reader(&aliases)?;
    let mut originals = BTreeMap::<i64, SavedMessage>::new();
    if let Some(anchors) = anchors {
        for &sequence in anchors {
            check()?;
            if let Some(saved) = reader.original_at(sequence)? {
                originals.insert(saved.sequence, saved);
            }
        }
    } else {
        let mut cursor = None;
        loop {
            check()?;
            let page = reader.originals_cancellable(cursor, 100, cancelled)?;
            for saved in page.messages {
                if let Some(row) = classify_saved(archive, identity, saved, syncing, now)?
                    && !row.reason.is_empty()
                {
                    originals.insert(row.saved.sequence, row.saved);
                }
            }
            cursor = page.next_after;
            if cursor.is_none() {
                break;
            }
        }
    }
    let mut selected = BTreeMap::<i64, RetainedMessage>::new();
    let mut anchor_ids = HashSet::new();
    for (_, saved) in originals {
        check()?;
        let Some(anchor) = classify_saved(archive, identity, saved, syncing, now)?
            .filter(|row| !row.reason.is_empty())
        else {
            continue;
        };
        let (before, after) = reader.neighbors_cancellable(&anchor.saved, cancelled)?;
        let missing_before = before.is_none();
        let missing_after = after.is_none();
        anchor_ids.insert(anchor.saved.sequence);
        for neighbor in [before, after].into_iter().flatten() {
            if let Some(message) = classify_saved(archive, identity, neighbor, syncing, now)? {
                selected
                    .entry(message.saved.sequence)
                    .or_insert(RetainedMessage {
                        message,
                        anchor: false,
                        gap_before: false,
                        missing_before: false,
                        missing_after: false,
                    });
            }
        }
        selected.insert(
            anchor.saved.sequence,
            RetainedMessage {
                message: anchor,
                anchor: true,
                gap_before: false,
                missing_before,
                missing_after,
            },
        );
    }
    let mut rows = selected.into_values().collect::<Vec<_>>();
    rows.sort_by_key(|row| (row.message.saved.sent_at, row.message.saved.sequence));
    let mut previous: Option<SavedMessage> = None;
    for row in &mut rows {
        check()?;
        row.anchor = anchor_ids.contains(&row.message.saved.sequence);
        if let Some(previous) = previous.as_ref() {
            row.gap_before = reader
                .neighbors_cancellable(previous, cancelled)?
                .1
                .is_none_or(|next| next.sequence != row.message.saved.sequence);
        }
        previous = Some(row.message.saved.clone());
        if full_history {
            while row.message.saved.history_truncated {
                check()?;
                let after = row
                    .message
                    .saved
                    .history
                    .last()
                    .map_or(0, |event| event.sequence);
                let page = reader.history(&row.message.saved.message_id, after, 256)?;
                row.message.saved.history.extend(page.observations);
                row.message.saved.history_truncated = page.has_more;
            }
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::super::receipt_tests::{PEER, own_message, worker};
    use super::*;

    fn original(id: &str) -> SavedMessage {
        SavedMessage {
            sequence: 1,
            chat: PEER.into(),
            message_id: id.into(),
            sender: PEER.into(),
            text: "Synthetic retained original".into(),
            sent_at: 100,
            observed_at: 100,
            outgoing: false,
            source: "incoming_event".into(),
            observed_on_whatsapp: true,
            send_accepted: false,
            deletion_observed: false,
            markdown_path: PathBuf::new(),
            attachments: Vec::new(),
            history: Vec::new(),
            history_truncated: false,
        }
    }

    fn known_chat(worker: &Worker) {
        worker.archive.ensure_chat(PEER, "Synthetic chat").unwrap();
    }

    fn context_job(worker: &Worker, request: u64, generation: u64) -> ContextJob {
        ContextJob {
            request,
            generation,
            privacy_generation: worker.privacy_generation,
            capture_root: worker.capture.as_ref().unwrap().root().to_path_buf(),
            identity: worker.deleted_identity(),
            chats: vec![PEER.into()],
            cancelled: Arc::new(AtomicBool::new(false)),
            task: None,
        }
    }

    fn context_row(worker: &Worker) -> RetainedMessage {
        RetainedMessage {
            message: worker
                .classify_deleted(original("saved"), 110)
                .unwrap()
                .unwrap(),
            anchor: true,
            gap_before: false,
            missing_before: false,
            missing_after: false,
        }
    }

    #[tokio::test]
    async fn context_background_reader_dispatches_saved_neighbors() {
        let (mut worker, events, mut commands, _) = worker();
        let path = worker.dirs.archive_db();
        // Archive's test platform store is an in-memory mock. Both this write
        // connection and the production background reader unlock the same key.
        worker.archive = Archive::open(&path).unwrap();
        known_chat(&worker);
        timed_original(&worker, "before", 100, false, PEER);
        let anchor = timed_original(&worker, "deleted", 101, true, PEER);
        timed_original(&worker, "after", 102, false, PEER);
        assert_ne!(
            std::fs::read(&path).unwrap().get(..16),
            Some(b"SQLite format 3\0".as_slice())
        );

        worker.load_deleted_context(42, vec![anchor]);
        // The account worker returns before the independent projection runs.
        assert!(worker.deleted_context.job.is_some());
        assert!(events.try_recv().is_err());
        assert!(worker.archive.chat(PEER).unwrap().is_some());

        let command = tokio::time::timeout(std::time::Duration::from_secs(10), commands.recv())
            .await
            .expect("background projection completes")
            .expect("background projection sends a result");
        assert!(matches!(
            &command,
            Command::DeletedContextReady {
                request: 42,
                failed: false,
                rows,
                ..
            } if rows.len() == 3
        ));
        worker.handle_command(command).await;
        let Event::DeletedContextLoaded {
            request: 42,
            rows,
            error: None,
        } = events.try_recv().unwrap()
        else {
            panic!("expected authorized context result");
        };
        assert_eq!(
            rows.iter()
                .map(|row| row.message.saved.message_id.as_str())
                .collect::<Vec<_>>(),
            ["before", "deleted", "after"]
        );
        assert_eq!(rows.iter().filter(|row| row.anchor).count(), 1);
        assert!(rows[0].message.reason.is_empty());
        assert!(!rows[1].message.reason.is_empty());
        assert!(rows[2].message.reason.is_empty());
        assert!(worker.deleted_context.job.is_none());
    }

    #[test]
    fn context_completion_rejects_stale_request_and_job_generations() {
        let (mut worker, events, _, _) = worker();
        known_chat(&worker);
        worker.deleted_context.job = Some(context_job(&worker, 4, 8));
        let row = context_row(&worker);
        for (request, generation) in [(3, 7), (4, 7), (3, 8)] {
            worker.deleted_context_ready(request, generation, vec![row.clone()], false);
            assert!(events.try_recv().is_err());
            assert!(worker.deleted_context.job.is_some());
        }
        worker.deleted_context_ready(4, 8, vec![row], false);
        assert!(worker.deleted_context.job.is_none());
        assert!(
            matches!(events.try_recv().unwrap(), Event::DeletedContextLoaded {
            request: 4, rows, error: None
        } if rows.len() == 1)
        );
    }

    #[test]
    fn context_completion_rechecks_privacy_identity_and_capture_namespace() {
        for changed in 0..7 {
            let (mut worker, events, _, _) = worker();
            known_chat(&worker);
            worker
                .lid_to_pn
                .insert("123".into(), PEER.split('@').next().unwrap().into());
            worker
                .archive
                .ensure_chat("123@lid", "Synthetic alias")
                .unwrap();
            worker.deleted_context.job = Some(context_job(&worker, 4, 8));
            let mut row = context_row(&worker);
            match changed {
                0 => worker.privacy_confirmed = false,
                1 => worker.privacy_generation = worker.privacy_generation.wrapping_add(1),
                2 => worker.archive.set_locked("123@lid", true).unwrap(),
                3 => {
                    worker.capture = Some(
                        crate::capture::Capture::open(&worker.capture_root, "replacement").unwrap(),
                    )
                }
                4 => {
                    worker.lid_to_pn.insert("456".into(), "999".into());
                }
                5 => row.message.canonical_chat = "different@g.us".into(),
                _ => worker.capture = None,
            }
            worker.deleted_context_ready(4, 8, vec![row], false);
            assert!(worker.deleted_context.job.is_none());
            assert!(
                matches!(events.try_recv().unwrap(), Event::DeletedContextLoaded {
                request: 4, rows, error: Some(_)
            } if rows.is_empty())
            );
        }
    }

    #[test]
    fn context_request_replacement_and_state_drop_cancel_blocking_work() {
        let (mut worker, events, _, _) = worker();
        known_chat(&worker);
        let job = context_job(&worker, 4, 8);
        let cancelled = job.cancelled.clone();
        worker.deleted_context.job = Some(job);
        worker.privacy_confirmed = false;
        worker.load_deleted_context(5, vec![1]);
        assert!(cancelled.load(Ordering::Acquire));
        assert!(worker.deleted_context.job.is_none());
        assert!(
            matches!(events.try_recv().unwrap(), Event::DeletedContextLoaded {
            request: 5, rows, error: Some(_)
        } if rows.is_empty())
        );
        worker.privacy_confirmed = true;
        let row = context_row(&worker);
        worker.deleted_context_ready(4, 8, vec![row], false);
        assert!(events.try_recv().is_err());
        let job = context_job(&worker, 6, 9);
        let cancelled = job.cancelled.clone();
        let state = State {
            generation: 9,
            job: Some(job),
        };
        drop(state);
        assert!(cancelled.load(Ordering::Acquire));
    }

    #[test]
    fn context_background_failure_does_not_publish_rows() {
        let (mut worker, events, _, _) = worker();
        known_chat(&worker);
        worker.deleted_context.job = Some(context_job(&worker, 4, 8));
        worker.deleted_context_ready(4, 8, vec![context_row(&worker)], true);
        assert!(
            matches!(events.try_recv().unwrap(), Event::DeletedContextLoaded {
            request: 4, rows, error: Some(_)
        } if rows.is_empty())
        );
    }

    fn capture_original(worker: &Worker, id: &str, deleted: bool, visible: bool) {
        worker
            .capture
            .as_ref()
            .unwrap()
            .record_event(crate::capture::CaptureEvent {
                event_id: format!("fixture:{id}"),
                chat: PEER.into(),
                message_id: id.into(),
                sender: PEER.into(),
                kind: "message".into(),
                text: "Synthetic captured original".into(),
                timestamp: 100,
                source: "incoming_event".into(),
                direction: "incoming".into(),
                raw: if visible {
                    wa::Message::text("Synthetic captured original").encode_to_vec()
                } else {
                    wa::Message::default().encode_to_vec()
                },
                ..Default::default()
            })
            .unwrap();
        if deleted {
            worker.archive.delete_message(PEER, id).unwrap();
        } else {
            worker
                .archive
                .insert_message(&own_message(id, 100), None)
                .unwrap();
        }
    }

    fn deleted_response(
        events: &std::sync::mpsc::Receiver<Event>,
        expected_request: u64,
    ) -> (Vec<DeletedMessage>, Option<i64>) {
        events
            .try_iter()
            .find_map(|event| match event {
                Event::DeletedLoaded {
                    request,
                    messages,
                    next_before,
                    error,
                } if request == expected_request => {
                    assert!(error.is_none(), "synthetic saved-message read failed");
                    Some((messages, next_before))
                }
                _ => None,
            })
            .expect("deleted page response")
    }

    fn timed_original(
        worker: &Worker,
        id: &str,
        timestamp: i64,
        deleted: bool,
        sender: &str,
    ) -> i64 {
        let capture = worker.capture.as_ref().unwrap();
        capture
            .record_event(crate::capture::CaptureEvent {
                event_id: format!("timed:{id}"),
                chat: PEER.into(),
                message_id: id.into(),
                sender: sender.into(),
                kind: "message".into(),
                text: id.into(),
                timestamp,
                source: "incoming_event".into(),
                direction: "incoming".into(),
                raw: wa::Message::text(id).encode_to_vec(),
                ..Default::default()
            })
            .unwrap();
        if deleted {
            worker.archive.delete_message(PEER, id).unwrap();
        } else {
            worker
                .archive
                .insert_message(&own_message(id, timestamp), None)
                .unwrap();
        }
        capture.saved_messages(None, 1).unwrap().messages[0].sequence
    }

    fn projected(
        worker: &Worker,
        anchors: Option<&[i64]>,
        full_history: bool,
    ) -> Vec<RetainedMessage> {
        project_deleted_chat(
            worker.capture.as_ref().unwrap(),
            &worker.archive,
            &worker.deleted_identity(),
            PEER,
            ProjectionOptions {
                anchors,
                syncing: worker.syncing,
                now: crate::util::now(),
                full_history,
                cancelled: &AtomicBool::new(false),
            },
        )
        .unwrap()
    }

    #[test]
    fn deleted_anchor_pages_merge_alias_echoes_without_inflating_counts() {
        let (mut worker, events, _, _) = worker();
        known_chat(&worker);
        let lid = "123@lid";
        worker
            .lid_to_pn
            .insert("123".into(), PEER.split('@').next().unwrap().into());
        capture_original(&worker, "aliased", true, true);
        for n in 0..100 {
            capture_original(&worker, &format!("other-{n}"), true, true);
        }
        worker
            .capture
            .as_ref()
            .unwrap()
            .record_event(crate::capture::CaptureEvent {
                event_id: "alias-echo".into(),
                chat: lid.into(),
                message_id: "aliased".into(),
                sender: lid.into(),
                kind: "message".into(),
                text: "Same original".into(),
                timestamp: 100,
                source: "incoming_event".into(),
                direction: "incoming".into(),
                raw: wa::Message::text("Same original").encode_to_vec(),
                ..Default::default()
            })
            .unwrap();
        worker.load_deleted(1, None);
        let (first, cursor) = deleted_response(&events, 1);
        assert_eq!(first.len(), 100);
        assert_eq!(cursor, Some(2));
        assert!(first.iter().all(|row| row.saved.message_id != "aliased"));
        worker.load_deleted(2, cursor);
        let (second, next) = deleted_response(&events, 2);
        assert_eq!(second.len(), 1);
        assert!(next.is_none());
        assert_eq!(second[0].saved.message_id, "aliased");
        assert_eq!(second[0].saved.sequence, 1);
    }

    #[test]
    fn context_uses_same_chat_chronology_and_deduplicates_overlapping_windows() {
        let (worker, _, _, _) = worker();
        known_chat(&worker);
        timed_original(&worker, "after", 40, false, PEER);
        let second = timed_original(&worker, "second-deleted", 30, true, "222@s.whatsapp.net");
        timed_original(&worker, "before", 10, false, PEER);
        let first = timed_original(&worker, "first-deleted", 20, true, PEER);
        let rows = projected(&worker, Some(&[first, second]), false);
        assert_eq!(
            rows.iter()
                .map(|r| r.message.saved.message_id.as_str())
                .collect::<Vec<_>>(),
            ["before", "first-deleted", "second-deleted", "after"]
        );
        assert_eq!(rows.iter().filter(|r| r.anchor).count(), 2);
        assert!(rows[0].message.reason.is_empty() && rows[3].message.reason.is_empty());
        assert!(!rows[1].message.reason.is_empty() && !rows[2].message.reason.is_empty());
        assert!(rows.iter().all(|r| !r.gap_before));
    }

    #[test]
    fn deleted_neighbor_keeps_its_status_without_recursively_expanding_sender_context() {
        let (worker, _, _, _) = worker();
        known_chat(&worker);
        timed_original(&worker, "outside-before", 10, false, PEER);
        timed_original(
            &worker,
            "other-sender-deleted",
            20,
            true,
            "222@s.whatsapp.net",
        );
        let anchor = timed_original(&worker, "anchor", 30, true, PEER);
        timed_original(&worker, "after", 40, false, PEER);
        let rows = projected(&worker, Some(&[anchor]), false);
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].message.saved.message_id, "other-sender-deleted");
        assert!(!rows[0].anchor && !rows[0].message.reason.is_empty());
        assert_eq!(rows.iter().filter(|r| r.anchor).count(), 1);
    }

    #[test]
    fn separate_context_windows_mark_gaps_and_boundaries_without_extra_messages() {
        let (worker, _, _, _) = worker();
        known_chat(&worker);
        for n in 0..7 {
            timed_original(
                &worker,
                &format!("message-{n}"),
                n * 10,
                n == 1 || n == 5,
                PEER,
            );
        }
        let rows = projected(&worker, None, false);
        assert_eq!(
            rows.iter()
                .map(|r| r.message.saved.message_id.as_str())
                .collect::<Vec<_>>(),
            [
                "message-0",
                "message-1",
                "message-2",
                "message-4",
                "message-5",
                "message-6"
            ]
        );
        assert!(rows[3].gap_before);
        assert_eq!(rows.iter().filter(|r| r.gap_before).count(), 1);
        let first = timed_original(&worker, "earliest-deleted", -10, true, PEER);
        let rows = projected(&worker, Some(&[first]), false);
        assert!(rows[0].missing_before);
        assert!(!rows[0].missing_after);
    }

    #[test]
    fn context_refresh_adds_later_messages_and_reclassifies_deleted_neighbors() {
        let (worker, _, _, _) = worker();
        known_chat(&worker);
        let anchor = timed_original(&worker, "anchor", 20, true, PEER);
        let rows = projected(&worker, Some(&[anchor]), false);
        assert!(rows[0].missing_after);
        timed_original(&worker, "later", 30, false, "222@s.whatsapp.net");
        let rows = projected(&worker, Some(&[anchor]), false);
        assert_eq!(rows.len(), 2);
        assert!(!rows[0].missing_after);
        assert!(rows[1].message.reason.is_empty());
        worker.archive.delete_message(PEER, "later").unwrap();
        assert_eq!(
            projected(&worker, Some(&[anchor]), false)[1].message.reason,
            "Deleted for me"
        );
    }

    #[test]
    fn context_and_revision_reads_fail_closed_for_locked_aliases_and_unknown_privacy() {
        let (mut worker, events, _, _) = worker();
        known_chat(&worker);
        let anchor = timed_original(&worker, "anchor", 20, true, PEER);
        worker
            .lid_to_pn
            .insert("123".into(), PEER.split('@').next().unwrap().into());
        worker.archive.ensure_chat("123@lid", "Alias").unwrap();
        worker.archive.set_locked("123@lid", true).unwrap();
        worker.load_deleted_context(77, vec![anchor]);
        assert!(events.try_iter().any(|event|matches!(event,Event::DeletedContextLoaded{request:77,rows,error:Some(_)} if rows.is_empty())));
        worker.load_deleted_history(78, PEER.into(), "anchor".into(), 0);
        assert!(events.try_iter().any(|event|matches!(event,Event::DeletedHistoryLoaded{request:78,observations,error:Some(_),..} if observations.is_empty())));
        worker.archive.set_locked("123@lid", false).unwrap();
        worker.privacy_confirmed = false;
        worker.load_deleted_context(79, vec![anchor]);
        assert!(events.try_iter().any(|event|matches!(event,Event::DeletedContextLoaded{request:79,rows,error:Some(_)} if rows.is_empty())));
    }

    #[test]
    fn full_chat_projection_ignores_ui_anchor_pages_and_reads_complete_revision_history() {
        let (worker, _, _, _) = worker();
        known_chat(&worker);
        timed_original(&worker, "anchor", 20, true, PEER);
        for n in 0..260 {
            worker
                .capture
                .as_ref()
                .unwrap()
                .record_event(crate::capture::CaptureEvent {
                    event_id: format!("edit-{n}"),
                    chat: PEER.into(),
                    message_id: format!("edit-{n}"),
                    target_id: Some("anchor".into()),
                    kind: "edited".into(),
                    text: "Repeat".into(),
                    timestamp: 21,
                    source: "fixture".into(),
                    ..Default::default()
                })
                .unwrap();
        }
        assert!(
            projected(&worker, None, false)[0]
                .message
                .saved
                .history_truncated
        );
        let full = projected(&worker, None, true);
        assert_eq!(full[0].message.saved.history.len(), 260);
        assert!(!full[0].message.saved.history_truncated);
        let cancelled = AtomicBool::new(true);
        assert!(
            project_deleted_chat(
                worker.capture.as_ref().unwrap(),
                &worker.archive,
                &worker.deleted_identity(),
                PEER,
                ProjectionOptions {
                    anchors: None,
                    syncing: false,
                    now: crate::util::now(),
                    full_history: true,
                    cancelled: &cancelled
                }
            )
            .is_err()
        );
    }

    #[test]
    fn deleted_page_scans_past_recent_normal_messages() {
        let (worker, events, _, _) = worker();
        known_chat(&worker);
        capture_original(&worker, "older-deletion", true, true);
        for index in 0..205 {
            capture_original(&worker, &format!("normal-{index}"), false, true);
        }

        worker.load_deleted(1, None);
        let (messages, next) = deleted_response(&events, 1);
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].saved.message_id, "older-deletion");
        assert_eq!(next, None);
    }

    #[test]
    fn deleted_page_limit_resumes_inside_candidate_page_without_skipping_results() {
        let (worker, events, _, _) = worker();
        known_chat(&worker);
        for index in 0..110 {
            capture_original(&worker, &format!("deleted-{index}"), true, true);
        }
        for index in 0..125 {
            capture_original(&worker, &format!("normal-{index}"), false, true);
        }

        worker.load_deleted(1, None);
        let (first, cursor) = deleted_response(&events, 1);
        assert_eq!(first.len(), DELETED_PER_PAGE);
        assert_eq!(cursor, first.last().map(|row| row.saved.sequence));
        worker.load_deleted(2, cursor);
        let (second, next) = deleted_response(&events, 2);
        assert_eq!(second.len(), 10);
        assert_eq!(next, None);
        let actual = first
            .into_iter()
            .chain(second)
            .map(|row| row.saved.message_id)
            .collect::<Vec<_>>();
        let expected = (0..110)
            .rev()
            .map(|index| format!("deleted-{index}"))
            .collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn deleted_scan_budget_returns_progress_through_invisible_candidate_pages() {
        let (worker, events, _, _) = worker();
        known_chat(&worker);
        capture_original(&worker, "older-deletion", true, true);
        for index in 0..CANDIDATES_PER_PAGE * CANDIDATE_PAGES_PER_REQUEST {
            capture_original(&worker, &format!("control-{index}"), false, false);
        }

        worker.load_deleted(1, None);
        let (first, cursor) = deleted_response(&events, 1);
        assert!(first.is_empty());
        assert_eq!(cursor, Some(2), "bounded work still makes cursor progress");
        worker.load_deleted(2, cursor);
        let (second, next) = deleted_response(&events, 2);
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].saved.message_id, "older-deletion");
        assert_eq!(next, None);
    }

    #[test]
    fn accepted_delete_for_me_does_not_need_a_revoke_observation() {
        let (worker, _, _, _) = worker();
        known_chat(&worker);
        worker.archive.delete_message(PEER, "gone").unwrap();
        let found = worker
            .classify_deleted(original("gone"), 100)
            .unwrap()
            .unwrap();
        assert_eq!(found.reason, "Deleted for me");
        assert!(found.saved.history.is_empty());
    }

    #[test]
    fn chat_clear_uses_its_boundary_and_keeps_newer_arrivals_out() {
        let (worker, _, _, _) = worker();
        known_chat(&worker);
        worker
            .archive
            .remove_chat_through(PEER, 100, false)
            .unwrap();
        let found = worker
            .classify_deleted(original("old"), 100)
            .unwrap()
            .unwrap();
        assert_eq!(
            found.reason,
            "Removed when this chat was cleared or deleted"
        );
        let mut newer = original("new");
        newer.sent_at = 101;
        assert!(worker.classify_deleted(newer, 100).unwrap().is_none());
    }

    #[test]
    fn optimistic_revoke_does_not_claim_server_acceptance() {
        let (worker, _, _, _) = worker();
        known_chat(&worker);
        let mut row = own_message("revoked", 100);
        row.content = Content::Revoked;
        worker.archive.insert_message(&row, None).unwrap();
        let found = worker
            .classify_deleted(original("revoked"), 110)
            .unwrap()
            .unwrap();
        assert_eq!(found.reason, "Shown as deleted in this chat");
        let mut saved = original("revoked");
        saved.deletion_observed = true;
        assert_eq!(
            worker.classify_deleted(saved, 110).unwrap().unwrap().reason,
            "Deleted for everyone"
        );
    }

    #[test]
    fn absence_waits_for_ingestion_and_does_not_name_a_cause() {
        let (mut worker, _, _, _) = worker();
        known_chat(&worker);
        for now in [90, 100, 104] {
            assert!(
                worker
                    .classify_deleted(original("missing"), now)
                    .unwrap()
                    .is_none()
            );
        }
        assert_eq!(
            worker
                .classify_deleted(original("missing"), 105)
                .unwrap()
                .unwrap()
                .reason,
            "No longer in chat"
        );
        worker.syncing = true;
        assert!(
            worker
                .classify_deleted(original("missing"), 200)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_prepared_send_needs_acceptance_or_a_phone_echo() {
        let (worker, _, _, _) = worker();
        known_chat(&worker);
        let mut saved = original("send");
        saved.source = "local_send".into();
        saved.outgoing = true;
        saved.observed_on_whatsapp = false;
        assert!(
            worker
                .classify_deleted(saved.clone(), 110)
                .unwrap()
                .is_none()
        );
        saved.send_accepted = true;
        assert!(
            worker
                .classify_deleted(saved.clone(), 110)
                .unwrap()
                .is_some()
        );
        saved.send_accepted = false;
        saved.observed_on_whatsapp = true;
        assert!(worker.classify_deleted(saved, 110).unwrap().is_some());
    }

    #[test]
    fn aliases_prevent_false_absence_and_preserve_locks() {
        let (mut worker, _, _, _) = worker();
        known_chat(&worker);
        let lid = "123@lid";
        worker
            .lid_to_pn
            .insert("123".into(), PEER.split('@').next().unwrap().into());
        worker
            .archive
            .insert_message(&own_message("present", 100), None)
            .unwrap();
        let mut saved = original("present");
        saved.chat = lid.into();
        assert!(
            worker
                .classify_deleted(saved.clone(), 110)
                .unwrap()
                .is_none()
        );
        worker.archive.delete_message(PEER, "present").unwrap();
        let found = worker
            .classify_deleted(saved.clone(), 110)
            .unwrap()
            .unwrap();
        assert_eq!(found.canonical_chat, PEER);
        assert_eq!(found.saved.chat, lid);
        worker.archive.ensure_chat(lid, "Synthetic alias").unwrap();
        worker.archive.set_locked(lid, true).unwrap();
        assert!(worker.classify_deleted(saved, 110).unwrap().is_none());
    }

    #[test]
    fn one_sender_has_the_same_key_across_pn_lid_and_different_groups() {
        let (mut worker, _, _, _) = worker();
        known_chat(&worker);
        worker
            .lid_to_pn
            .insert("123".into(), PEER.split('@').next().unwrap().into());
        let mut keys = Vec::new();
        for (index, (chat, sender)) in [(PEER, PEER), ("111@g.us", "123@lid"), ("222@g.us", PEER)]
            .into_iter()
            .enumerate()
        {
            worker.archive.ensure_chat(chat, "Synthetic chat").unwrap();
            let mut saved = original(&format!("sender-{index}"));
            saved.chat = chat.into();
            saved.sender = sender.into();
            let found = worker.classify_deleted(saved, 110).unwrap().unwrap();
            assert_eq!(found.canonical_chat, chat);
            keys.push(found.sender_id);
        }
        assert_eq!(keys, [PEER, PEER, PEER]);
    }

    #[test]
    fn all_outgoing_copies_use_our_identity_and_a_stable_unknown_fallback() {
        let (mut worker, _, _, _) = worker();
        known_chat(&worker);
        let own_id = worker.me();
        for raw_sender in [own_id.as_str(), "123@lid"] {
            let mut saved = original("own");
            saved.outgoing = true;
            saved.sender = raw_sender.into();
            let found = worker.classify_deleted(saved, 110).unwrap().unwrap();
            assert_eq!(found.sender_id, own_id);
            assert_eq!(found.sender_name, "You");
        }
        worker.me_lid = None;
        for unavailable in [None, Some(String::new())] {
            worker.me_pn = unavailable;
            let mut saved = original("own-unknown");
            saved.outgoing = true;
            let found = worker.classify_deleted(saved, 110).unwrap().unwrap();
            assert_eq!(found.sender_id, "__self__");
        }
    }

    #[test]
    fn missing_chat_metadata_and_unknown_privacy_fail_closed() {
        let (mut worker, events, _, _) = worker();
        assert!(
            worker
                .classify_deleted(original("missing"), 110)
                .unwrap()
                .is_none()
        );
        known_chat(&worker);
        worker.archive.set_locked(PEER, true).unwrap();
        worker.archive.remove_chat_through(PEER, 100, true).unwrap();
        assert!(
            worker
                .classify_deleted(original("missing"), 110)
                .unwrap()
                .is_none()
        );
        worker.privacy_ready = false;
        worker.load_deleted(7, None);
        assert!(events.try_iter().any(|event| matches!(event,
            Event::DeletedLoaded { request: 7, messages, next_before: None, error: Some(_) }
                if messages.is_empty())));
    }

    #[test]
    fn normal_chat_fallback_does_not_expose_permanent_originals() {
        let (mut worker, events, _, _) = worker();
        known_chat(&worker);
        worker.privacy_ready = false;
        worker.privacy_confirmed = false;
        worker.reveal_unconfirmed();
        assert!(
            worker.privacy_ready,
            "normal chats keep the upstream fallback"
        );
        assert!(!worker.privacy_confirmed);
        assert!(
            events
                .try_iter()
                .any(|event| matches!(event, Event::Chats(_)))
        );
        assert!(
            worker
                .classify_deleted(original("saved"), 110)
                .unwrap()
                .is_none()
        );
        worker.load_deleted(23, None);
        assert!(events.try_iter().any(|event| matches!(event,
            Event::DeletedLoaded { request: 23, messages, next_before: None, error: Some(error) }
                if messages.is_empty() && error.starts_with("Waiting for chat privacy settings"))));
    }
}
