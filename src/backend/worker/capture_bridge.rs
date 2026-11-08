//! Connects the independent receipt journal to this account's lifecycle.

use super::*;
use crate::capture::{Capture, CaptureEvent, CaptureStatus};

impl Worker {
    pub(super) async fn prepare_capture(&mut self) -> bool {
        if self.capture.is_none() {
            let identity = (|| -> anyhow::Result<String> {
                if let Some(id) = self.archive.meta(crate::archive::CAPTURE_ACCOUNT_KEY)? {
                    anyhow::ensure!(
                        id.len() == 32 && id.bytes().all(|c| c.is_ascii_hexdigit()),
                        "Invalid permanent archive identity"
                    );
                    return Ok(id);
                }
                let mut bytes = [0u8; 16];
                getrandom::fill(&mut bytes)
                    .map_err(|_| anyhow::anyhow!("Cannot create archive identity"))?;
                let id: String = bytes.iter().map(|byte| format!("{byte:02x}")).collect();
                self.archive
                    .set_meta_durable(crate::archive::CAPTURE_ACCOUNT_KEY, &id)?;
                Ok(id)
            })();
            let opened = match identity {
                Ok(id) => tokio::task::spawn_blocking(move || {
                    Capture::open(&crate::capture::default_root(), &id)
                })
                .await
                .map_err(anyhow::Error::from)
                .and_then(|result| result),
                Err(error) => Err(error),
            };
            match opened {
                Ok(capture) => self.capture = Some(capture),
                Err(_) => {
                    self.emit(Event::CaptureStatus(CaptureStatus {
                        paused: true,
                        error: Some("Cannot open the permanent archive. Check its folder permissions and free space, then reconnect.".into()),
                        root: crate::capture::default_root(),
                        ..Default::default()
                    }));
                    self.set_status(LinkStatus::Failed(
                        "Capture paused: the permanent archive is unavailable".into(),
                    ));
                    return false;
                }
            }
        }
        let capture = self.capture.as_ref().expect("capture initialized").clone();
        self.emit(Event::CaptureStatus(capture.status()));
        if let Some(monitor) = self.capture_monitor.take() {
            monitor.abort();
        }
        let events = self.events.clone();
        let waker = self.waker.clone();
        self.capture_monitor = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            let mut previous = None;
            loop {
                interval.tick().await;
                let status = capture.status();
                if previous.as_ref() != Some(&status) {
                    previous = Some(status.clone());
                    if events.send(Event::CaptureStatus(status)).is_err() {
                        break;
                    }
                    waker.wake();
                }
            }
        }));
        true
    }

    pub(super) fn capture_outgoing(&self, message: &Message, raw: &[u8]) -> bool {
        let result = self
            .capture
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Capture is unavailable"))
            .and_then(|capture| capture.record_outgoing(message, raw));
        if result.is_err() {
            self.capture_failure();
            false
        } else {
            true
        }
    }

    pub(super) fn capture_failure(&self) {
        if let Some(capture) = &self.capture {
            self.emit(Event::CaptureStatus(capture.status()));
        }
        self.emit(Event::Error("Capture paused: the permanent archive could not save this operation. Check free space and folder permissions.".into()));
    }

    pub(super) fn capture_action(
        &self,
        kind: &str,
        chat: &str,
        id: &str,
        text: String,
        raw: Vec<u8>,
    ) -> bool {
        let Some(capture) = &self.capture else {
            self.capture_failure();
            return false;
        };
        let result = capture.record_event(action_event(kind, chat, id, text, raw));
        if result.is_err() {
            self.capture_failure();
        }
        result.is_ok()
    }
}

pub(super) fn action_event(
    kind: &str,
    chat: &str,
    id: &str,
    text: String,
    raw: Vec<u8>,
) -> CaptureEvent {
    CaptureEvent {
        event_id: format!("{kind}:{id}:{}", jiff::Timestamp::now().as_nanosecond()),
        chat: chat.to_owned(),
        message_id: id.to_owned(),
        target_id: Some(id.to_owned()),
        kind: kind.to_owned(),
        text,
        raw,
        timestamp: crate::util::now(),
        direction: "outgoing".into(),
        source: "local_action".into(),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::super::receipt_tests::{own_message, worker};
    use super::*;

    #[test]
    fn an_unavailable_capture_prevents_outgoing_storage() {
        let (mut worker, events, _, _) = worker();
        worker.capture = None;
        let mut row = own_message("not-sent", 1700000000);
        row.status = Delivery::Pending;
        let chat = row.chat.clone();
        let raw = wa::Message::text("Do not lose this").encode_to_vec();
        assert!(!worker.store_message(row, Some(raw), None));
        assert!(worker.archive.message(&chat, "not-sent").unwrap().is_none());
        assert!(
            events
                .try_iter()
                .any(|event| matches!(event, Event::Error(_)))
        );
    }

    #[test]
    fn a_mutable_chat_deletion_does_not_remove_the_permanent_original() {
        let (mut worker, _, _, _) = worker();
        let mut row = own_message("retained", 1700000000);
        row.status = Delivery::Pending;
        let chat = row.chat.clone();
        assert!(worker.store_message(row, Some(wa::Message::text("hi").encode_to_vec()), None));
        worker
            .archive
            .set_content(&chat, "retained", &Content::Revoked, false)
            .unwrap();
        worker.archive.clear().unwrap();
        let journal = rusqlite::Connection::open(
            worker
                .capture
                .as_ref()
                .unwrap()
                .root()
                .join("journal.sqlite3"),
        )
        .unwrap();
        let text: String = journal
            .query_row(
                "SELECT text FROM events WHERE message_id='retained'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(text, "hi");
    }

    #[tokio::test]
    async fn an_old_prepared_attachment_cannot_enter_a_new_account() {
        let (mut worker, events, _, _) = worker();
        worker.privacy_generation = 10;
        let row = own_message("old-upload", 1700000000);
        let chat = row.chat.clone();
        worker
            .handle_command(Command::Outbound {
                generation: 9,
                chat: chat.clone(),
                row: Box::new(row),
                raw: wa::Message::text("old upload").encode_to_vec(),
                turn: None,
            })
            .await;
        assert!(
            worker
                .archive
                .message(&chat, "old-upload")
                .unwrap()
                .is_none()
        );
        assert!(events.try_iter().next().is_none());
    }

    #[test]
    fn separate_identical_action_attempts_keep_distinct_ids() {
        let first = action_event(
            "edit_requested",
            "1@s.whatsapp.net",
            "target",
            "A".into(),
            vec![],
        );
        let second = action_event(
            "edit_requested",
            "1@s.whatsapp.net",
            "target",
            "A".into(),
            vec![],
        );
        assert_ne!(first.event_id, second.event_id);
        assert_eq!(first.target_id, second.target_id);
    }

    #[tokio::test]
    async fn committed_batches_materialize_without_a_second_capture_write() {
        for committed in [true, false] {
            let (mut worker, events, _, _) = worker();
            let chat = own_message("fixture", 1_700_000_000).chat;
            // A captured batch remains valid if storage becomes unavailable
            // between its durable hook and the worker's materializer.
            worker.capture = None;
            let info = wa_events::InboundMessage::builder()
                .message(Arc::new(wa::Message::text("Already committed")))
                .info(Arc::new(MessageInfo {
                    id: "received".into(),
                    source: MessageSource {
                        chat: chat.parse().unwrap(),
                        sender: chat.parse().unwrap(),
                        ..Default::default()
                    },
                    timestamp: whatsapp_rust::wacore::time::from_secs(1_700_000_000).unwrap(),
                    ..Default::default()
                }))
                .build();
            let batch = wa_events::MessageBatch::builder()
                .messages(Arc::from([info]))
                .origin(wa_events::BatchOrigin::Live)
                .hook_committed(committed)
                .build();
            worker
                .handle_wa_event(Arc::new(wa_events::Event::Messages(batch)))
                .await;
            assert_eq!(
                worker.archive.message(&chat, "received").unwrap().is_some(),
                committed,
                "event-only recovery still requires a successful capture"
            );
            assert_eq!(
                events
                    .try_iter()
                    .any(|event| matches!(event, Event::Error(_))),
                !committed,
            );
        }
    }
}
