//! Read-only projections for the in-app retained-message browser.
//!
//! Deletion for oneself is often an app-state removal, not a journal event.
//! Callers must compare these originals with the current chat archive rather
//! than treating the presence or absence of a revoke event as authoritative.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Result;
use rusqlite::{Connection, OpenFlags, OptionalExtension, Row, params};
use whatsapp_rust::prelude::wa;
use whatsapp_rust::waproto::buffa::Message as _;

use super::{Capture, hex_digest, reject_symlink, relative_media_path};

const PAGE_LIMIT: usize = 100;
const HISTORY_LIMIT: usize = 256;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SavedMessagePage {
    pub messages: Vec<SavedMessage>,
    /// Pass this cursor unchanged to request older originals. None means end.
    pub next_before: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedMessage {
    /// First original observation, stable even if a preferred local send is
    /// observed after its phone echo. This is the descending pagination key.
    pub sequence: i64,
    pub chat: String,
    pub message_id: String,
    pub sender: String,
    pub text: String,
    pub sent_at: i64,
    pub observed_at: i64,
    pub outgoing: bool,
    /// A protocol message/echo was received, even if a local send outcome was
    /// lost during shutdown. A local pending record alone is not proof of send.
    pub observed_on_whatsapp: bool,
    /// These evidence flags inspect every target event, including observations
    /// beyond the bounded history preview.
    pub send_accepted: bool,
    pub deletion_observed: bool,
    pub source: String,
    /// A derived file; it may not exist yet if Markdown generation is stale.
    pub markdown_path: PathBuf,
    pub attachments: Vec<SavedAttachment>,
    /// Immutable target observations in received order. Includes edits,
    /// deletion notices, and local send/edit/revoke requests and outcomes.
    /// Equal text is deliberately not collapsed.
    pub history: Vec<SavedObservation>,
    /// Very long histories are bounded in this list; the journal and Markdown
    /// retain all observations. The first 256 observations are included.
    pub history_truncated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedObservation {
    pub sequence: i64,
    pub kind: String,
    pub text: String,
    pub sender: String,
    pub sent_at: i64,
    pub observed_at: i64,
    pub source: String,
    pub attachments: Vec<SavedAttachment>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedAttachment {
    /// queued, downloading, saved, paused, failed, or oversized.
    pub status: String,
    /// Absolute path confined to this account's permanent media directory.
    pub path: Option<PathBuf>,
    pub error: Option<String>,
}

impl Capture {
    /// Read at most 100 unique originals, newest capture first.
    ///
    /// This synchronous API belongs on a worker or spawn_blocking task. Its
    /// independent read-only WAL snapshot never takes the capture writer lock.
    /// The mutable chat database is deliberately not consulted here.
    pub fn saved_messages(&self, before: Option<i64>, limit: usize) -> Result<SavedMessagePage> {
        let limit = limit.clamp(1, PAGE_LIMIT);
        let path = self.root().join("journal.sqlite3");
        reject_symlink(&path)?;
        let mut reader = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NOFOLLOW,
        )?;
        reader.busy_timeout(Duration::from_secs(2))?;
        reader.pragma_update(None, "query_only", true)?;
        let snapshot = reader.transaction()?;

        // Select the first original for each (chat,message_id), not the last
        // echo. Keyset pagination remains stable when a new echo or edit arrives.
        let candidates = {
            let mut statement = snapshot.prepare(
                "SELECT e.sequence,e.chat,e.message_id FROM events e
                 WHERE (?1 IS NULL OR e.sequence < ?1)
                   AND e.kind IN ('message','outgoing_pending')
                   AND NOT EXISTS (
                     SELECT 1 FROM events prior
                     WHERE prior.chat=e.chat AND prior.message_id=e.message_id
                       AND prior.kind IN ('message','outgoing_pending')
                       AND prior.sequence<e.sequence)
                 ORDER BY e.sequence DESC LIMIT ?2",
            )?;
            statement
                .query_map(params![before, (limit + 1) as i64], |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?
        };
        let more = candidates.len() > limit;
        // Pagination describes scanned originals, including excluded controls.
        // A page containing only reactions must still advance to older text.
        let next_before = more.then(|| candidates[limit - 1].0);
        let mut messages = Vec::with_capacity(candidates.len().min(limit));
        for (sequence, chat, message_id) in candidates.into_iter().take(limit) {
            let (original, raw) = snapshot.query_row(
                "SELECT sequence,kind,text,sender,sent_at,observed_at,source,direction,month,raw
                 FROM events WHERE chat=?1 AND message_id=?2
                   AND kind IN ('message','outgoing_pending')
                 ORDER BY (kind='outgoing_pending') DESC,sequence ASC LIMIT 1",
                params![chat, message_id],
                |row| Ok((read_event(row)?, row.get::<_, Vec<u8>>(9)?)),
            )?;
            let visible = wa::Message::decode_from_slice(&raw)
                .ok()
                .is_some_and(|message| crate::backend::retained_original_is_visible(&message));
            if !visible {
                continue;
            }
            let (observed_on_whatsapp, send_accepted, deletion_observed) = snapshot.query_row(
                "SELECT EXISTS(SELECT 1 FROM events
                   WHERE chat=?1 AND message_id=?2 AND kind='message'),
                 EXISTS(SELECT 1 FROM events
                   WHERE chat=?1 AND target_id=?2 AND kind='send_accepted'),
                 EXISTS(SELECT 1 FROM events
                   WHERE chat=?1 AND target_id=?2 AND kind IN ('deleted','revoke_accepted'))",
                params![chat, message_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
            // Prefer the durable local copy, then a completed echo download.
            // There is one ordinary attachment descriptor per WhatsApp message.
            let attachments = snapshot
                .query_row(
                    "SELECT m.status,m.path,m.error FROM media_jobs m
                     JOIN events e ON e.sequence=m.event_sequence
                     WHERE e.chat=?1 AND e.message_id=?2
                       AND e.kind IN ('message','outgoing_pending')
                     ORDER BY (m.status='saved') DESC,
                              (e.kind='outgoing_pending') DESC,e.sequence ASC LIMIT 1",
                    params![chat, message_id],
                    |row| read_attachment(row, self.root()),
                )
                .optional()?
                .into_iter()
                .collect();
            let mut history = {
                let mut statement = snapshot.prepare(
                    "SELECT sequence,kind,text,sender,sent_at,observed_at,source,direction,month
                     FROM events WHERE chat=?1 AND target_id=?2
                     ORDER BY sequence ASC LIMIT ?3",
                )?;
                statement
                    .query_map(
                        params![chat, message_id, (HISTORY_LIMIT + 1) as i64],
                        read_event,
                    )?
                    .collect::<rusqlite::Result<Vec<_>>>()?
            };
            let history_truncated = history.len() > HISTORY_LIMIT;
            history.truncate(HISTORY_LIMIT);
            let history = history
                .into_iter()
                .map(|event| {
                    let attachments = snapshot
                        .query_row(
                            "SELECT status,path,error FROM media_jobs WHERE event_sequence=?1",
                            [event.sequence],
                            |row| read_attachment(row, self.root()),
                        )
                        .optional()?
                        .into_iter()
                        .collect();
                    Ok(SavedObservation {
                        sequence: event.sequence,
                        kind: event.kind,
                        text: event.text,
                        sender: event.sender,
                        sent_at: event.sent_at,
                        observed_at: event.observed_at,
                        source: event.source,
                        attachments,
                    })
                })
                .collect::<rusqlite::Result<Vec<_>>>()?;
            messages.push(SavedMessage {
                sequence,
                chat: chat.clone(),
                message_id,
                sender: original.sender,
                text: original.text,
                sent_at: original.sent_at,
                observed_at: original.observed_at,
                outgoing: original.direction == "outgoing",
                observed_on_whatsapp,
                send_accepted,
                deletion_observed,
                source: original.source,
                markdown_path: self.root().join("chats").join(format!(
                    "{}-{}.md",
                    hex_digest(chat.as_bytes()),
                    original.month
                )),
                attachments,
                history,
                history_truncated,
            });
        }
        snapshot.commit()?;
        Ok(SavedMessagePage {
            messages,
            next_before,
        })
    }
}

struct EventRow {
    sequence: i64,
    kind: String,
    text: String,
    sender: String,
    sent_at: i64,
    observed_at: i64,
    source: String,
    direction: String,
    month: String,
}

fn read_event(row: &Row<'_>) -> rusqlite::Result<EventRow> {
    Ok(EventRow {
        sequence: row.get(0)?,
        kind: row.get(1)?,
        text: row.get(2)?,
        sender: row.get(3)?,
        sent_at: row.get(4)?,
        observed_at: row.get(5)?,
        source: row.get(6)?,
        direction: row.get(7)?,
        month: row.get(8)?,
    })
}

fn read_attachment(row: &Row<'_>, root: &Path) -> rusqlite::Result<SavedAttachment> {
    let stored_path: Option<String> = row.get(1)?;
    // The journal writer validates these paths. Repeat the lexical check when
    // exposing an open-file action, so a manually damaged row cannot escape.
    let path = stored_path
        .as_deref()
        .and_then(|path| relative_media_path(root, Path::new(path)).ok())
        .map(|path| root.join(path));
    Ok(SavedAttachment {
        status: row.get(0)?,
        path,
        error: row.get(2)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::capture::CaptureEvent;
    use whatsapp_rust::prelude::MessageBuilderExt;
    use whatsapp_rust::prelude::MessageField;

    const CHAT: &str = "15550001111@s.whatsapp.net";

    fn event(id: &str, kind: &str, text: &str, target: Option<&str>) -> CaptureEvent {
        CaptureEvent {
            event_id: id.to_owned(),
            chat: CHAT.to_owned(),
            sender: CHAT.to_owned(),
            message_id: id.to_owned(),
            target_id: target.map(str::to_owned),
            kind: kind.to_owned(),
            text: text.to_owned(),
            raw: wa::Message::text(text).encode_to_vec(),
            timestamp: 1_790_000_000,
            direction: "incoming".to_owned(),
            source: "fixture".to_owned(),
        }
    }

    #[test]
    fn originals_prefer_local_sends_and_keep_repeated_edits_and_early_deletions() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        capture
            .record_event(event("delete", "deleted", "Deleted", Some("original")))
            .unwrap();
        let mut echo = event("original", "message", "Phone caption", None);
        echo.direction = "outgoing".into();
        capture.record_event(echo).unwrap();
        let mut local = event(
            "original",
            "outgoing_pending",
            "Full original\nsecond line",
            None,
        );
        local.direction = "outgoing".into();
        local.source = "local_send".into();
        capture.record_event(local).unwrap();
        for (id, text) in [("edit-1", "B"), ("edit-2", "A"), ("edit-3", "A")] {
            capture
                .record_event(event(id, "edited", text, Some("original")))
                .unwrap();
        }
        let mut other_chat = event("other-edit", "edited", "Different chat", Some("original"));
        other_chat.chat = "15550002222@s.whatsapp.net".into();
        capture.record_event(other_chat).unwrap();
        let page = capture.saved_messages(None, 10).unwrap();
        assert_eq!(page.messages.len(), 1);
        let saved = &page.messages[0];
        assert_eq!(saved.text, "Full original\nsecond line");
        assert_eq!(
            saved.sequence, 2,
            "cursor uses the first original, not the selected echo"
        );
        assert!(saved.outgoing);
        assert!(saved.observed_on_whatsapp);
        assert!(saved.deletion_observed);
        assert!(!saved.send_accepted);
        assert_eq!(saved.source, "local_send");
        assert_eq!(
            saved
                .history
                .iter()
                .map(|item| item.text.as_str())
                .collect::<Vec<_>>(),
            ["Deleted", "B", "A", "A"]
        );
        assert_eq!(saved.history[0].kind, "deleted");
        assert!(!saved.history_truncated);
        assert_eq!(
            saved.markdown_path,
            capture.root().join("chats").join(format!(
                "{}-{}.md",
                hex_digest(CHAT.as_bytes()),
                crate::capture::month(1_790_000_000)
            ))
        );
        capture
            .record_event(event(
                "pending-only",
                "outgoing_pending",
                "Not sent yet",
                None,
            ))
            .unwrap();
        assert!(!capture.saved_messages(None, 1).unwrap().messages[0].observed_on_whatsapp);
    }

    #[test]
    fn pagination_is_stable_when_new_messages_and_late_echoes_arrive() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        for id in ["old", "middle", "new"] {
            capture
                .record_event(event(id, "message", id, None))
                .unwrap();
        }
        let first = capture.saved_messages(None, 2).unwrap();
        assert_eq!(
            first
                .messages
                .iter()
                .map(|item| item.message_id.as_str())
                .collect::<Vec<_>>(),
            ["new", "middle"]
        );
        let cursor = first.next_before.unwrap();
        capture
            .record_event(event("old", "outgoing_pending", "preferred old copy", None))
            .unwrap();
        capture
            .record_event(event("newest", "message", "newest", None))
            .unwrap();
        let second = capture.saved_messages(Some(cursor), 2).unwrap();
        assert_eq!(second.messages.len(), 1);
        assert_eq!(second.messages[0].message_id, "old");
        assert_eq!(second.messages[0].text, "preferred old copy");
        assert!(second.next_before.is_none());
        assert!(
            capture
                .saved_messages(Some(1), 2)
                .unwrap()
                .messages
                .is_empty()
        );
    }

    #[test]
    fn saved_attachment_paths_states_and_account_boundaries_are_preserved() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        let other = Capture::open(directory.path(), "two").unwrap();
        other
            .record_event(event("original", "message", "Other account", None))
            .unwrap();
        let path = capture.root().join("media/fixture.txt");
        std::fs::write(&path, "fixture").unwrap();
        capture
            .record_entries(&[(
                event("original", "message", "Attachment", None),
                Some(path.clone()),
            )])
            .unwrap();
        let edit_path = capture.root().join("media/edited.txt");
        std::fs::write(&edit_path, "edited fixture").unwrap();
        capture
            .record_entries(&[(
                event("edit", "edited", "Edited attachment", Some("original")),
                Some(edit_path.clone()),
            )])
            .unwrap();
        let page = capture.saved_messages(None, 5).unwrap();
        assert_eq!(page.messages.len(), 1);
        assert_eq!(page.messages[0].text, "Attachment");
        assert_eq!(
            page.messages[0].attachments,
            [SavedAttachment {
                status: "saved".into(),
                path: Some(path),
                error: None
            }]
        );
        assert_eq!(
            page.messages[0].history[0].attachments[0].path.as_ref(),
            Some(&edit_path)
        );
        // Damaged mutable job metadata must not expose an external open action.
        capture
            .inner
            .connection
            .lock()
            .unwrap()
            .execute("UPDATE media_jobs SET path='../../outside'", [])
            .unwrap();
        assert!(
            capture.saved_messages(None, 5).unwrap().messages[0].attachments[0]
                .path
                .is_none()
        );
    }

    #[test]
    fn page_and_history_limits_are_explicit_without_losing_journal_events() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        let mut entries = (0..101)
            .map(|number| {
                (
                    event(&format!("original-{number}"), "message", "Original", None),
                    None,
                )
            })
            .collect::<Vec<_>>();
        entries.extend((0..257).map(|number| {
            (
                event(
                    &format!("edit-{number}"),
                    "edited",
                    "Repeated text",
                    Some("original-100"),
                ),
                None,
            )
        }));
        entries.push((
            event("sent", "send_accepted", "", Some("original-100")),
            None,
        ));
        entries.push((event("deleted", "deleted", "", Some("original-100")), None));
        capture.record_entries(&entries).unwrap();
        let page = capture.saved_messages(None, usize::MAX).unwrap();
        assert_eq!(page.messages.len(), PAGE_LIMIT);
        assert!(page.next_before.is_some());
        assert_eq!(page.messages[0].history.len(), HISTORY_LIMIT);
        assert!(page.messages[0].history_truncated);
        assert!(page.messages[0].send_accepted);
        assert!(page.messages[0].deletion_observed);
        assert!(
            page.messages[0]
                .history
                .iter()
                .all(|event| event.kind == "edited")
        );
        let count: i64 = capture
            .inner
            .connection
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 360, "view reads never alter the immutable journal");
        assert_eq!(capture.saved_messages(None, 0).unwrap().messages.len(), 1);
    }

    #[test]
    fn controls_are_not_deleted_originals_but_remain_in_the_journal() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        let mut reaction = wa::Message::default();
        reaction.reaction_message = MessageField::some(wa::message::ReactionMessage::default());
        let mut vote = wa::Message::default();
        vote.poll_update_message = MessageField::some(wa::message::PollUpdateMessage::default());
        let mut pin = wa::Message::default();
        pin.pin_in_chat_message = MessageField::some(wa::message::PinInChatMessage::default());
        let mut poll = wa::Message::default();
        let mut creation = wa::message::PollCreationMessage::default();
        creation.name = Some("Which day?".into());
        poll.poll_creation_message = MessageField::some(creation);
        let mut invitation = wa::Message::default();
        invitation.group_invite_message =
            MessageField::some(wa::message::GroupInviteMessage::default());
        for (id, message) in [
            (
                "literal-prefix",
                wa::Message::text("[Reaction] This is ordinary text"),
            ),
            ("poll", poll),
            ("unsupported-but-visible", invitation),
            ("reaction", reaction),
            ("vote", vote),
            ("pin", pin),
        ] {
            let mut saved = event(id, "message", id, None);
            saved.raw = message.encode_to_vec();
            capture.record_event(saved).unwrap();
        }
        // The newest page contains only controls. It still has a cursor and
        // must not panic, terminate history early, or manufacture deleted rows.
        let controls = capture.saved_messages(None, 3).unwrap();
        assert!(controls.messages.is_empty());
        assert!(controls.next_before.is_some());
        let originals = capture.saved_messages(controls.next_before, 3).unwrap();
        assert_eq!(
            originals
                .messages
                .iter()
                .map(|row| row.message_id.as_str())
                .collect::<Vec<_>>(),
            ["unsupported-but-visible", "poll", "literal-prefix"]
        );
        assert!(originals.next_before.is_none());
        let count: i64 = capture
            .inner
            .connection
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM events", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 6);
    }
}
