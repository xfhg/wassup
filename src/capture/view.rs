//! Read-only projections for the in-app retained-message browser.
//!
//! Deletion for oneself is often an app-state removal, not a journal event.
//! Callers must compare these originals with the current chat archive rather
//! than treating the presence or absence of a revoke event as authoritative.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, Row, params};
use whatsapp_rust::prelude::wa;
use whatsapp_rust::waproto::buffa::Message as _;

use super::{Capture, hex_digest, relative_media_path};

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
    /// Legacy transcript locator. No plaintext file is generated automatically.
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
        let mut reader = self.reader()?;
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
            if let Some(saved) = hydrate(&snapshot, self.root(), &[chat], &message_id)? {
                debug_assert_eq!(saved.sequence, sequence);
                messages.push(saved);
            }
        }
        snapshot.commit()?;
        Ok(SavedMessagePage {
            messages,
            next_before,
        })
    }
}

/// A bounded continuation through one original's immutable observations.
#[derive(Clone, Debug, Default)]
pub struct SavedHistoryPage {
    pub observations: Vec<SavedObservation>,
    pub has_more: bool,
}

/// Chronological originals, with a cursor even when a batch contained controls.
pub(crate) struct ChronologicalPage {
    pub messages: Vec<SavedMessage>,
    pub next_after: Option<(i64, i64)>,
}

/// One keyed snapshot shared by every original, neighbor and revision in a view.
pub(crate) struct SavedChatReader {
    connection: Connection,
    root: PathBuf,
    aliases: Vec<String>,
}

impl Capture {
    pub(crate) fn saved_chat_reader(&self, aliases: &[String]) -> Result<SavedChatReader> {
        anyhow::ensure!(!aliases.is_empty(), "Missing chat identity");
        let connection = self.reader()?;
        connection.busy_timeout(Duration::from_secs(2))?;
        connection.execute_batch("PRAGMA query_only=ON; BEGIN")?;
        Ok(SavedChatReader {
            connection,
            root: self.root().to_path_buf(),
            aliases: aliases.to_vec(),
        })
    }

    /// Resolve requested anchors in one keyed query before opening chat views.
    pub(crate) fn saved_original_locations(&self, sequences: &[i64]) -> Result<Vec<(i64, String)>> {
        let reader = self.reader()?;
        let sequences = serde_json::to_string(sequences)?;
        let mut statement=reader.prepare(
            "SELECT sequence,chat FROM events WHERE sequence IN (SELECT value FROM json_each(?1)) AND kind IN ('message','outgoing_pending') ORDER BY sequence"
        )?;
        Ok(statement
            .query_map([sequences], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?)
    }

    pub fn saved_history(
        &self,
        aliases: &[String],
        message: &str,
        after: i64,
        limit: usize,
    ) -> Result<SavedHistoryPage> {
        self.saved_chat_reader(aliases)?
            .history(message, after, limit)
    }
}

impl SavedChatReader {
    pub(crate) fn original(&self, message: &str) -> Result<Option<SavedMessage>> {
        hydrate(&self.connection, &self.root, &self.aliases, message)
    }

    pub(crate) fn original_at(&self, sequence: i64) -> Result<Option<SavedMessage>> {
        let aliases = serde_json::to_string(&self.aliases)?;
        let message: Option<String> = self.connection.query_row(
            "SELECT message_id FROM events WHERE sequence=?1 AND chat IN (SELECT value FROM json_each(?2)) AND kind IN ('message','outgoing_pending')",
            params![sequence,aliases], |row| row.get(0),
        ).optional()?;
        message
            .map(|id| self.original(&id))
            .transpose()
            .map(Option::flatten)
    }

    pub(crate) fn history(
        &self,
        message: &str,
        after: i64,
        limit: usize,
    ) -> Result<SavedHistoryPage> {
        read_history(
            &self.connection,
            &self.root,
            &self.aliases,
            message,
            after,
            limit,
        )
    }

    #[cfg(test)]
    pub(crate) fn originals(
        &self,
        after: Option<(i64, i64)>,
        limit: usize,
    ) -> Result<ChronologicalPage> {
        self.originals_cancellable(after, limit, &AtomicBool::new(false))
    }

    pub(crate) fn originals_cancellable(
        &self,
        after: Option<(i64, i64)>,
        limit: usize,
        cancelled: &AtomicBool,
    ) -> Result<ChronologicalPage> {
        self.page(after, limit, false, cancelled)
    }

    fn page(
        &self,
        boundary: Option<(i64, i64)>,
        limit: usize,
        backwards: bool,
        cancelled: &AtomicBool,
    ) -> Result<ChronologicalPage> {
        check_cancelled(cancelled)?;
        let limit = limit.clamp(1, PAGE_LIMIT);
        let aliases = serde_json::to_string(&self.aliases)?;
        let comparison = if backwards { "<" } else { ">" };
        let order = if backwards { "DESC" } else { "ASC" };
        let sql = format!(
            "SELECT e.sent_at,e.sequence,e.message_id FROM events e
             WHERE e.chat IN (SELECT value FROM json_each(?1))
               AND e.kind IN ('message','outgoing_pending')
               AND NOT EXISTS (SELECT 1 FROM events prior
                   WHERE prior.chat IN (SELECT value FROM json_each(?1))
                     AND prior.message_id=e.message_id
                     AND prior.kind IN ('message','outgoing_pending') AND prior.sequence<e.sequence)
               AND (?2 IS NULL OR (e.sent_at,e.sequence) {comparison} (?2,?3))
             ORDER BY e.sent_at {order},e.sequence {order} LIMIT ?4"
        );
        let mut statement = self.connection.prepare(&sql)?;
        let rows = statement
            .query_map(
                params![
                    aliases,
                    boundary.map(|p| p.0),
                    boundary.map(|p| p.1),
                    (limit + 1) as i64
                ],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        let next_after = (rows.len() > limit).then(|| (rows[limit - 1].0, rows[limit - 1].1));
        let mut messages = Vec::new();
        for (_, _, id) in rows.into_iter().take(limit) {
            check_cancelled(cancelled)?;
            if let Some(saved) = self.original(&id)?
                && accepted_original(&saved)
            {
                messages.push(saved);
            }
        }
        Ok(ChronologicalPage {
            messages,
            next_after,
        })
    }

    #[cfg(test)]
    pub(crate) fn neighbors(
        &self,
        original: &SavedMessage,
    ) -> Result<(Option<SavedMessage>, Option<SavedMessage>)> {
        self.neighbors_cancellable(original, &AtomicBool::new(false))
    }

    pub(crate) fn neighbors_cancellable(
        &self,
        original: &SavedMessage,
        cancelled: &AtomicBool,
    ) -> Result<(Option<SavedMessage>, Option<SavedMessage>)> {
        Ok((
            self.neighbor(original, true, cancelled)?,
            self.neighbor(original, false, cancelled)?,
        ))
    }

    fn neighbor(
        &self,
        original: &SavedMessage,
        backwards: bool,
        cancelled: &AtomicBool,
    ) -> Result<Option<SavedMessage>> {
        let mut cursor = Some((original.sent_at, original.sequence));
        loop {
            check_cancelled(cancelled)?;
            let page = self.page(cursor, 1, backwards, cancelled)?;
            if let Some(message) = page.messages.into_iter().next() {
                return Ok(Some(message));
            }
            let Some(next) = page.next_after else {
                return Ok(None);
            };
            cursor = Some(next);
        }
    }
}

fn check_cancelled(cancelled: &AtomicBool) -> Result<()> {
    anyhow::ensure!(
        !cancelled.load(Ordering::Acquire),
        "Saved message read cancelled"
    );
    Ok(())
}

pub(crate) fn accepted_original(saved: &SavedMessage) -> bool {
    saved.source != "local_send" || saved.send_accepted || saved.observed_on_whatsapp
}

fn hydrate(
    connection: &Connection,
    root: &Path,
    aliases: &[String],
    message: &str,
) -> Result<Option<SavedMessage>> {
    let json = serde_json::to_string(aliases)?;
    let first: Option<(i64,i64)> = connection.query_row(
        "SELECT sequence,sent_at FROM events WHERE chat IN (SELECT value FROM json_each(?1)) AND message_id=?2 AND kind IN ('message','outgoing_pending') ORDER BY sequence LIMIT 1",
        params![json,message], |row| Ok((row.get(0)?,row.get(1)?)),
    ).optional()?;
    let Some((sequence, sent_at)) = first else {
        return Ok(None);
    };
    let (original,raw,chat)=connection.query_row(
        "SELECT sequence,kind,text,sender,sent_at,observed_at,source,direction,month,raw,chat FROM events
         WHERE chat IN (SELECT value FROM json_each(?1)) AND message_id=?2 AND kind IN ('message','outgoing_pending')
         ORDER BY (kind='outgoing_pending') DESC,sequence ASC LIMIT 1",
        params![json,message], |row| Ok((read_event(row)?,row.get::<_,Vec<u8>>(9)?,row.get::<_,String>(10)?)),
    )?;
    let visible = wa::Message::decode_from_slice(&raw)
        .ok()
        .is_some_and(|message| crate::backend::retained_original_is_visible(&message));
    if !visible {
        return Ok(None);
    }
    let (observed_on_whatsapp,send_accepted,deletion_observed)=connection.query_row(
        "SELECT EXISTS(SELECT 1 FROM events WHERE chat IN (SELECT value FROM json_each(?1)) AND message_id=?2 AND kind='message'),
         EXISTS(SELECT 1 FROM events WHERE chat IN (SELECT value FROM json_each(?1)) AND target_id=?2 AND kind='send_accepted'),
         EXISTS(SELECT 1 FROM events WHERE chat IN (SELECT value FROM json_each(?1)) AND target_id=?2 AND kind IN ('deleted','revoke_accepted'))",
        params![json,message], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?)),
    )?;
    let attachments=connection.query_row(
        "SELECT m.status,m.path,m.error FROM media_jobs m JOIN events e ON e.sequence=m.event_sequence
         WHERE e.chat IN (SELECT value FROM json_each(?1)) AND e.message_id=?2 AND e.kind IN ('message','outgoing_pending')
         ORDER BY (m.status='saved') DESC,(e.kind='outgoing_pending') DESC,e.sequence ASC LIMIT 1",
        params![json,message], |row| read_attachment(row,root),
    ).optional()?.into_iter().collect();
    let history = read_history(connection, root, aliases, message, 0, HISTORY_LIMIT)?;
    Ok(Some(SavedMessage {
        sequence,
        chat: chat.clone(),
        message_id: message.to_owned(),
        sender: original.sender,
        text: original.text,
        sent_at,
        observed_at: original.observed_at,
        outgoing: original.direction == "outgoing",
        observed_on_whatsapp,
        send_accepted,
        deletion_observed,
        source: original.source,
        markdown_path: root.join("chats").join(format!(
            "{}-{}.md",
            hex_digest(chat.as_bytes()),
            original.month
        )),
        attachments,
        history: history.observations,
        history_truncated: history.has_more,
    }))
}

fn read_history(
    connection: &Connection,
    root: &Path,
    aliases: &[String],
    message: &str,
    after: i64,
    limit: usize,
) -> Result<SavedHistoryPage> {
    let limit = limit.clamp(1, HISTORY_LIMIT);
    let aliases = serde_json::to_string(aliases)?;
    let mut statement=connection.prepare(
        "SELECT sequence,kind,text,sender,sent_at,observed_at,source,direction,month FROM events
         WHERE chat IN (SELECT value FROM json_each(?1)) AND target_id=?2 AND sequence>?3 ORDER BY sequence ASC LIMIT ?4"
    )?;
    let mut events = statement
        .query_map(
            params![aliases, message, after, (limit + 1) as i64],
            read_event,
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let has_more = events.len() > limit;
    events.truncate(limit);
    let observations = events
        .into_iter()
        .map(|event| {
            let attachments = connection
                .query_row(
                    "SELECT status,path,error FROM media_jobs WHERE event_sequence=?1",
                    [event.sequence],
                    |row| read_attachment(row, root),
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
    Ok(SavedHistoryPage {
        observations,
        has_more,
    })
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
    fn chronological_neighbors_merge_aliases_and_skip_controls_and_unsent_attempts() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        let lid = "123@lid";
        let mut entries = Vec::new();
        for (id, timestamp, chat) in [
            ("after", 30, CHAT),
            ("anchor", 20, CHAT),
            ("before", 10, lid),
        ] {
            let mut original = event(id, "message", id, None);
            original.timestamp = timestamp;
            original.chat = chat.into();
            entries.push((original, None));
        }
        let mut alias_echo = event("anchor", "message", "Alias echo", None);
        alias_echo.chat = lid.into();
        alias_echo.timestamp = 21;
        entries.push((alias_echo, None));
        let mut control = event("control", "message", "control", None);
        control.timestamp = 19;
        control.raw = wa::Message::default().encode_to_vec();
        entries.push((control, None));
        let mut unsent = event("unsent", "outgoing_pending", "Not sent", None);
        unsent.timestamp = 22;
        unsent.source = "local_send".into();
        unsent.direction = "outgoing".into();
        entries.push((unsent, None));
        capture.record_entries(&entries).unwrap();
        let aliases = vec![CHAT.to_owned(), lid.to_owned()];
        let reader = capture.saved_chat_reader(&aliases).unwrap();
        let anchor = reader.original("anchor").unwrap().unwrap();
        assert_eq!((anchor.sequence, anchor.sent_at), (2, 20));
        let (before, after) = reader.neighbors(&anchor).unwrap();
        assert_eq!(before.unwrap().message_id, "before");
        assert_eq!(after.unwrap().message_id, "after");
        let page = reader.originals(None, 100).unwrap();
        assert_eq!(
            page.messages
                .iter()
                .map(|m| m.message_id.as_str())
                .collect::<Vec<_>>(),
            ["before", "anchor", "after"]
        );
        assert!(page.next_after.is_none());
    }

    #[test]
    fn context_snapshot_is_stable_and_a_new_snapshot_sees_late_context() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        let mut anchor = event("anchor", "message", "Original", None);
        anchor.timestamp = 20;
        capture.record_event(anchor).unwrap();
        let aliases = vec![CHAT.to_owned()];
        let reader = capture.saved_chat_reader(&aliases).unwrap();
        let original = reader.original("anchor").unwrap().unwrap();
        assert!(reader.neighbors(&original).unwrap().1.is_none());
        let mut late = event("later", "message", "Late arrival", None);
        late.timestamp = 21;
        capture.record_event(late).unwrap();
        assert!(
            reader.neighbors(&original).unwrap().1.is_none(),
            "one view/export has one snapshot"
        );
        assert_eq!(
            capture
                .saved_chat_reader(&aliases)
                .unwrap()
                .neighbors(&original)
                .unwrap()
                .1
                .unwrap()
                .message_id,
            "later"
        );
    }

    #[test]
    fn history_pages_keep_equal_text_and_alias_observations_in_sequence_order() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        let lid = "123@lid";
        capture
            .record_event(event("original", "message", "Original", None))
            .unwrap();
        let entries = (0..300)
            .map(|n| {
                let mut edit = event(&format!("edit-{n}"), "edited", "Repeated", Some("original"));
                if n % 2 == 1 {
                    edit.chat = lid.into();
                }
                (edit, None)
            })
            .collect::<Vec<_>>();
        capture.record_entries(&entries).unwrap();
        let aliases = vec![CHAT.to_owned(), lid.to_owned()];
        let reader = capture.saved_chat_reader(&aliases).unwrap();
        let saved = reader.original("original").unwrap().unwrap();
        assert_eq!(saved.history.len(), 256);
        assert!(saved.history_truncated);
        let page = capture
            .saved_history(
                &aliases,
                "original",
                saved.history.last().unwrap().sequence,
                256,
            )
            .unwrap();
        assert_eq!(page.observations.len(), 44);
        assert!(!page.has_more);
        assert!(page.observations.iter().all(|e| e.text == "Repeated"));
        assert_eq!(page.observations.last().unwrap().sequence, 301);
    }

    #[test]
    fn cancelled_context_reads_stop_before_scanning_or_hydrating() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        capture
            .record_event(event("anchor", "message", "Original", None))
            .unwrap();
        let reader = capture.saved_chat_reader(&[CHAT.to_owned()]).unwrap();
        let original = reader.original("anchor").unwrap().unwrap();
        let cancelled = AtomicBool::new(true);
        assert!(reader.neighbors_cancellable(&original, &cancelled).is_err());
        assert!(reader.originals_cancellable(None, 100, &cancelled).is_err());
        assert_eq!(
            capture
                .saved_original_locations(&[original.sequence, original.sequence, 999])
                .unwrap(),
            [(original.sequence, CHAT.to_owned())]
        );
    }

    #[test]
    fn equal_timestamp_originals_have_stable_first_capture_order() {
        let directory = tempfile::tempdir().unwrap();
        let capture = Capture::open(directory.path(), "one").unwrap();
        for id in ["first", "second", "third"] {
            capture
                .record_event(event(id, "message", id, None))
                .unwrap();
        }
        let reader = capture.saved_chat_reader(&[CHAT.to_owned()]).unwrap();
        let original = reader.original("second").unwrap().unwrap();
        let (before, after) = reader.neighbors(&original).unwrap();
        assert_eq!(before.unwrap().message_id, "first");
        assert_eq!(after.unwrap().message_id, "third");
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
