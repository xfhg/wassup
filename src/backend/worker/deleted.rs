//! Compare permanent originals with this account's mutable chat archive.
//!
//! App-state delete-for-me and chat-clear updates do not carry a revoke
//! message. Their durable removal barriers are stronger evidence than a
//! missing row. Missing rows alone never identify who removed a message.

use super::*;
use crate::backend::DeletedMessage;
use crate::capture::view::SavedMessage;

const CANDIDATES_PER_PAGE: usize = 100;
const MATERIALIZATION_GRACE_SECONDS: i64 = 5;

impl Worker {
    pub(super) fn load_deleted(&self, request: u64, before: Option<i64>) {
        let result = (|| -> anyhow::Result<_> {
            anyhow::ensure!(
                self.privacy_ready,
                "Waiting for chat privacy settings before showing saved messages"
            );
            let capture = self
                .capture
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("The permanent archive is not available"))?;
            let page = capture.saved_messages(before, CANDIDATES_PER_PAGE)?;
            let now = crate::util::now();
            let mut messages = Vec::new();
            for saved in page.messages {
                if let Some(message) = self.classify_deleted(saved, now)? {
                    messages.push(message);
                }
            }
            Ok((messages, page.next_before))
        })();

        let (messages, next_before, error) = match result {
            Ok((messages, next_before)) => (messages, next_before, None),
            Err(_) => {
                let message = if !self.privacy_ready {
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
        if !self.privacy_ready {
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
}
