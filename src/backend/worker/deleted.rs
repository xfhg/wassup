//! Compare permanent originals with this account's mutable chat archive.
//!
//! App-state delete-for-me and chat-clear updates do not carry a revoke
//! message. Their durable removal barriers are stronger evidence than a
//! missing row. Missing rows alone never identify who removed a message.

use super::*;
use crate::backend::DeletedMessage;
use crate::capture::view::SavedMessage;

const CANDIDATES_PER_PAGE: usize = 100;
const CANDIDATE_PAGES_PER_REQUEST: usize = 10;
const DELETED_PER_PAGE: usize = 100;
const MATERIALIZATION_GRACE_SECONDS: i64 = 5;

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
                    if let Some(message) = self.classify_deleted(saved, now)? {
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

    fn classify_deleted(
        &self,
        saved: SavedMessage,
        now: i64,
    ) -> anyhow::Result<Option<DeletedMessage>> {
        if !self.privacy_ready || !self.privacy_confirmed {
            return Ok(None);
        }
        let canonical_chat = self.canonical_str(&saved.chat);
        let aliases = self.deleted_chat_aliases(&saved.chat, &canonical_chat);
        let mut chat_name = None;
        let mut present = false;
        let mut revoked = false;
        let mut removed = false;
        let mut cleared = false;
        let mut sender_name = None;
        for alias in &aliases {
            if let Some(chat) = self.archive.chat(alias)? {
                if chat.locked {
                    return Ok(None);
                }
                if chat_name.is_none() || alias == &canonical_chat {
                    chat_name = Some(chat.name);
                }
            }
            if let Some(message) = self.archive.message(alias, &saved.message_id)? {
                present = true;
                revoked |= matches!(message.content, Content::Revoked);
                sender_name = sender_name.or(message.sender_name);
            }
            removed |= self.archive.message_removed(alias, &saved.message_id)?;
            cleared |= self
                .archive
                .removal_point(alias)?
                .is_some_and(|through| saved.sent_at <= through);
        }
        // A fully removed chat loses its lock flag in the mutable database.
        // Do not assume that such a chat was unlocked, even with a tombstone.
        let Some(chat_name) = chat_name else {
            return Ok(None);
        };
        // Preparing a local send saves the original before network dispatch.
        // It is not a deleted WhatsApp message unless acceptance or a phone
        // echo is independently known. These flags cover untruncated history.
        if saved.source == "local_send" && !saved.send_accepted && !saved.observed_on_whatsapp {
            return Ok(None);
        }
        let reason = if saved.deletion_observed {
            "Deleted for everyone"
        } else if removed {
            "Deleted for me"
        } else if cleared {
            "Removed when this chat was cleared or deleted"
        } else if revoked {
            // Local revoke currently changes this row optimistically, before
            // the server result. The row alone is not proof of acceptance.
            "Shown as deleted in this chat"
        } else if !present
            && !self.syncing
            && now.saturating_sub(saved.observed_at) >= MATERIALIZATION_GRACE_SECONDS
        {
            // The durable receive hook runs before mutable ingestion. Suppress
            // recent arrivals and active history sync to avoid that normal gap.
            "No longer in chat"
        } else {
            return Ok(None);
        };
        let sender_id = if saved.outgoing {
            let me = self.me();
            if me.trim().is_empty() || me == "me" {
                "__self__".to_owned()
            } else {
                me
            }
        } else {
            self.canonical_str(&saved.sender)
        };
        let sender_name = if saved.outgoing {
            "You".to_owned()
        } else {
            sender_name
                .or_else(|| self.name_for(&sender_id))
                .unwrap_or_else(|| fallback_name(&saved.sender))
        };
        Ok(Some(DeletedMessage {
            saved,
            canonical_chat,
            chat_name,
            sender_id,
            sender_name,
            reason: reason.to_owned(),
        }))
    }

    fn deleted_chat_aliases(&self, original: &str, canonical: &str) -> Vec<String> {
        let mut aliases = vec![original.to_owned(), canonical.to_owned()];
        if let Some(pn) = canonical.strip_suffix("@s.whatsapp.net") {
            aliases.extend(
                self.lid_to_pn
                    .iter()
                    .filter(|(_, mapped)| mapped.as_str() == pn)
                    .map(|(lid, _)| format!("{lid}@lid")),
            );
        }
        // Our PN/LID pair can be known before the general mapping table.
        if self.is_me(canonical) {
            aliases.extend(self.me_pn.iter().cloned());
            aliases.extend(self.me_lid.iter().cloned());
        }
        aliases.sort_unstable();
        aliases.dedup();
        aliases
    }
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
