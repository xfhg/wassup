//! Account-scoped context and history loading for the retained-message reader.

use super::*;
use crate::backend::RetainedMessage;
use crate::capture::view::SavedObservation;
use crate::model::DeletedGrouping;

impl App {
    pub(super) fn remap_deleted_identity(&mut self, from: &str, to: &str) {
        if from == to {
            return;
        }
        let focused = self
            .deleted_messages
            .iter()
            .find(|row| Some(row.saved.sequence) == self.deleted_focused_message)
            .map(|row| {
                (
                    if row.canonical_chat == from {
                        to.to_owned()
                    } else {
                        row.canonical_chat.clone()
                    },
                    row.saved.message_id.clone(),
                )
            });
        for row in &mut self.deleted_messages {
            if row.canonical_chat == from {
                row.canonical_chat = to.to_owned();
            }
            if row.sender_id == from {
                row.sender_id = to.to_owned();
            }
        }
        if self.deleted_selected_chat.as_deref() == Some(from) {
            self.deleted_selected_chat = Some(to.to_owned());
        }
        if self.deleted_selected_sender.as_deref() == Some(from) {
            self.deleted_selected_sender = Some(to.to_owned());
        }
        // Alias echoes share one canonical original, including when their raw
        // capture observations landed on different list pages.
        self.deleted_messages.sort_by_key(|row| row.saved.sequence);
        let mut seen = HashSet::new();
        self.deleted_messages
            .retain(|row| seen.insert((row.canonical_chat.clone(), row.saved.message_id.clone())));
        self.deleted_messages
            .sort_by_key(|row| std::cmp::Reverse(row.saved.sequence));
        if let Some((chat, message)) = focused {
            self.deleted_focused_message = self
                .deleted_messages
                .iter()
                .find(|row| row.canonical_chat == chat && row.saved.message_id == message)
                .map(|row| row.saved.sequence);
        }
        self.clear_deleted_context();
    }

    pub(super) fn clear_deleted_context(&mut self) {
        self.deleted_context_request = self.deleted_context_request.wrapping_add(1);
        self.deleted_context.clear();
        self.deleted_context_anchors.clear();
        self.deleted_context_scope = None;
        self.deleted_context_loading = false;
        self.deleted_context_error = None;
        self.deleted_context_due = None;
        self.deleted_history_loading.clear();
        self.deleted_history_errors.clear();
    }

    pub(super) fn set_deleted_grouping(&mut self, grouping: DeletedGrouping) {
        if grouping == self.deleted_grouping {
            return;
        }
        let anchors = crate::ui::deleted::selected_anchor_sequences(self);
        let focus = self
            .deleted_messages
            .iter()
            .filter(|row| anchors.contains(&row.saved.sequence))
            .max_by_key(|row| {
                (
                    Some(row.saved.sequence) == self.deleted_focused_message,
                    row.saved.sequence,
                )
            })
            .map(|row| {
                let sender = if row.saved.outgoing {
                    "wassup:own-messages".to_owned()
                } else if row.sender_id.trim().is_empty() {
                    format!(
                        "wassup:unknown-sender:{}:{}",
                        row.canonical_chat, row.saved.message_id
                    )
                } else {
                    row.sender_id.clone()
                };
                (row.canonical_chat.clone(), sender)
            });
        if let Some((chat, sender)) = focus {
            match grouping {
                DeletedGrouping::Chat => self.deleted_selected_chat = Some(chat),
                DeletedGrouping::Sender => self.deleted_selected_sender = Some(sender),
            }
        }
        self.deleted_grouping = grouping;
        self.clear_deleted_context();
    }

    pub(super) fn refresh_deleted_context(&mut self) {
        let scope = crate::ui::deleted::selected_context_scope(self);
        let mut anchors = crate::ui::deleted::selected_anchor_sequences(self);
        anchors.sort_unstable();
        anchors.dedup();
        let changed = anchors != self.deleted_context_anchors;
        let scope_changed = scope != self.deleted_context_scope;
        if anchors.is_empty() {
            if changed || scope_changed || !self.deleted_context.is_empty() {
                self.clear_deleted_context();
            }
            return;
        }
        if !changed
            && !scope_changed
            && (self.deleted_context_loading
                || !self.deleted_history_loading.is_empty()
                || self
                    .deleted_context_due
                    .is_some_and(|due| due > Instant::now()))
        {
            return;
        }
        if scope_changed {
            self.clear_deleted_context();
        }
        // New deletions and older pages within this chat/sender must not discard
        // revisions the reader has already opened. New request IDs still reject
        // in-flight results, and the view filters cached rows to the new anchors.
        self.deleted_context_scope = scope;
        self.deleted_context_anchors = anchors.clone();
        self.deleted_context_request = self.deleted_context_request.wrapping_add(1);
        self.deleted_context_loading = true;
        self.deleted_context_error = None;
        self.deleted_history_loading.clear();
        self.deleted_history_errors.clear();
        self.backend.send(Command::LoadDeletedContext {
            request: self.deleted_context_request,
            anchors,
        });
    }

    pub(super) fn apply_deleted_context(
        &mut self,
        request: u64,
        mut rows: Vec<RetainedMessage>,
        error: Option<String>,
    ) {
        if request != self.deleted_context_request {
            return;
        }
        self.deleted_context_loading = false;
        self.deleted_context_due = Some(Instant::now() + Duration::from_secs(5));
        self.deleted_context_error = error;
        if self.deleted_context_error.is_some() {
            self.deleted_context.clear();
            self.deleted_history_loading.clear();
            return;
        }
        // Keep already read revisions while refreshing current message state.
        // The new truncation flag triggers another bounded page from the last
        // loaded revision, including edits that arrived after the previous read.
        let mut refresh_history = Vec::new();
        for row in &mut rows {
            if let Some(previous) = self.deleted_context.iter().find(|previous| {
                previous.message.canonical_chat == row.message.canonical_chat
                    && previous.message.saved.message_id == row.message.saved.message_id
            }) {
                let history = &mut row.message.saved.history;
                let first_page_end = history.last().map_or(0, |event| event.sequence);
                if row.message.saved.history_truncated
                    && previous
                        .message
                        .saved
                        .history
                        .last()
                        .is_some_and(|event| event.sequence > first_page_end)
                {
                    refresh_history.push((
                        row.message.canonical_chat.clone(),
                        row.message.saved.message_id.clone(),
                        first_page_end,
                    ));
                }
                history.extend(
                    previous
                        .message
                        .saved
                        .history
                        .iter()
                        .filter(|event| event.sequence > first_page_end)
                        .cloned(),
                );
            }
        }
        // Refresh the selected anchors even when list pagination or search has
        // paused the global list refresh. The shared projection can reconcile
        // aliases or discover that a formerly absent message is present again.
        let requested = self
            .deleted_context_anchors
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let refreshed = rows
            .iter()
            .filter(|row| row.anchor)
            .map(|row| {
                (
                    (
                        row.message.canonical_chat.clone(),
                        row.message.saved.message_id.clone(),
                    ),
                    row.message.clone(),
                )
            })
            .collect::<HashMap<_, _>>();
        self.deleted_messages.retain_mut(|row| {
            if !requested.contains(&row.saved.sequence) {
                return true;
            }
            if let Some(current) =
                refreshed.get(&(row.canonical_chat.clone(), row.saved.message_id.clone()))
            {
                *row = current.clone();
                true
            } else {
                false
            }
        });
        self.deleted_context_anchors = rows
            .iter()
            .filter(|row| row.anchor)
            .map(|row| row.message.saved.sequence)
            .collect();
        self.deleted_context_anchors.sort_unstable();
        self.deleted_context_anchors.dedup();
        self.deleted_context = rows;
        // Attachment states are mutable even though their revision text is
        // immutable. Re-read the already visible tail in bounded pages without
        // collapsing it or moving the reader's viewport.
        if !self.app_lock.is_locked() {
            for (chat, message, after) in refresh_history {
                self.send_deleted_history(chat, message, after);
            }
        }
    }

    pub(super) fn request_deleted_history(&mut self, chat: String, message: String, after: i64) {
        if self.app_lock.is_locked()
            || self.deleted_context_loading
            || !self.chat(&chat).is_some_and(|chat| !chat.locked)
            || self
                .deleted_history_loading
                .iter()
                .any(|(pending_chat, pending_message, _)| {
                    pending_chat == &chat && pending_message == &message
                })
            || !self.deleted_context.iter().any(|row| {
                row.message.canonical_chat == chat
                    && row.message.saved.message_id == message
                    && row.message.saved.history_truncated
                    && row
                        .message
                        .saved
                        .history
                        .last()
                        .map_or(0, |event| event.sequence)
                        == after
            })
        {
            return;
        }
        // A retry also refreshes any cached attachment states in older pages.
        let after = if self
            .deleted_history_errors
            .remove(&(chat.clone(), message.clone()))
            .is_some()
        {
            0
        } else {
            after
        };
        self.send_deleted_history(chat, message, after);
    }

    fn send_deleted_history(&mut self, chat: String, message: String, after: i64) {
        let key = (chat.clone(), message.clone(), after);
        self.deleted_history_loading.insert(key);
        self.backend.send(Command::LoadDeletedHistory {
            request: self.deleted_context_request,
            chat,
            message,
            after,
        });
    }

    pub(super) fn apply_deleted_history(
        &mut self,
        request: u64,
        key: (String, String, i64),
        observations: Vec<SavedObservation>,
        has_more: bool,
        error: Option<String>,
    ) {
        if request != self.deleted_context_request || !self.deleted_history_loading.remove(&key) {
            return;
        }
        if self.deleted_history_loading.is_empty() {
            // Leave time for the visible sentinel to request new revisions;
            // continuous arrivals must not monopolize the reader with tail
            // revalidation. Explicit refresh and selection changes can bypass.
            self.deleted_context_due = Some(Instant::now() + Duration::from_secs(5));
        }
        let (chat, message, after) = key;
        if let Some(error) = error {
            self.deleted_history_errors.insert((chat, message), error);
            return;
        }
        if has_more && !observations.iter().any(|event| event.sequence > after) {
            self.deleted_history_errors.insert(
                (chat, message),
                "Could not load more revisions. Refresh to retry.".into(),
            );
            return;
        }
        let mut refresh_next = None;
        if let Some(row) = self.deleted_context.iter_mut().find(|row| {
            row.message.canonical_chat == chat && row.message.saved.message_id == message
        }) {
            let saved = &mut row.message.saved;
            let loaded_end = saved.history.last().map_or(0, |event| event.sequence);
            let next = observations.last().map_or(after, |event| event.sequence);
            for observation in observations {
                match saved
                    .history
                    .binary_search_by_key(&observation.sequence, |event| event.sequence)
                {
                    Ok(index) => saved.history[index] = observation,
                    Err(index) => saved.history.insert(index, observation),
                }
            }
            saved.history_truncated = has_more;
            if has_more && after < loaded_end && next < loaded_end {
                refresh_next = Some(next);
            }
        }
        if let Some(next) = refresh_next {
            self.send_deleted_history(chat, message, next);
        }
    }
}
