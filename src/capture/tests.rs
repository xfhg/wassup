use super::*;
use whatsapp_rust::prelude::MessageField;
use whatsapp_rust::types::message::MessageSource;

const CHAT: &str = "15550001111@s.whatsapp.net";

#[test]
fn the_default_archive_is_wassup_in_the_user_home() {
    let expected = directories::BaseDirs::new()
        .map(|dirs| dirs.home_dir().join("Wassup"))
        .unwrap_or_else(|| PathBuf::from("Wassup"));
    assert_eq!(default_root(), expected);
}

#[test]
fn renaming_the_archive_preserves_records_links_and_pending_media() {
    use wa::message::protocol_message::Type;

    let fixture = tempfile::tempdir().unwrap();
    let old_root = fixture.path().join("Previous Archive");
    let new_root = fixture.path().join("Wassup");
    let capture = Capture::open(&old_root, "relocation-account").unwrap();
    capture
        .record_incoming(&text("Original text"), &info("original"), "durable_inbound")
        .unwrap();
    capture
        .record_incoming(
            &protocol(Type::MESSAGE_EDIT, "original", Some("Revised text")),
            &info("revision"),
            "durable_inbound",
        )
        .unwrap();
    capture
        .record_incoming(
            &protocol(Type::REVOKE, "original", None),
            &info("deletion"),
            "durable_inbound",
        )
        .unwrap();

    let mut image = wa::message::ImageMessage::default();
    image.mimetype = Some("image/jpeg".to_owned());
    image.file_length = Some(7);
    let mut body = wa::Message::default();
    body.image_message = MessageField::some(image);
    capture
        .record_incoming(&body, &info("saved-photo"), "durable_inbound")
        .unwrap();
    let saved_job = capture.claim_media_job().unwrap().unwrap();
    let saved_path = capture.root().join("media/fixture.jpg");
    fs::write(&saved_path, b"fixture").unwrap();
    capture
        .finish_media_job(saved_job.id, "saved", Some(&saved_path), None, None)
        .unwrap();
    capture
        .record_incoming(&body, &info("pending-photo"), "durable_inbound")
        .unwrap();
    let pending_job = capture.claim_media_job().unwrap().unwrap();
    assert!(!capture.root().join("chats").exists());
    let original_events = events(&capture);
    let account_folder = capture.root().file_name().unwrap().to_owned();
    let readme = fs::read(old_root.join("WASSUP-README.md")).unwrap();

    // Like quitting the app before the real move, release all journal handles.
    let closed = Arc::downgrade(&capture.inner);
    drop(capture);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while closed.strong_count() > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "fixture journal did not close"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    fs::rename(&old_root, &new_root).unwrap();
    let reopened = Capture::open(&new_root, "relocation-account").unwrap();
    assert_eq!(reopened.root().file_name().unwrap(), account_folder);
    assert_eq!(events(&reopened), original_events);
    assert_eq!(reopened.status().saved, 1);
    assert_eq!(reopened.status().queued, 1);
    assert_eq!(fs::read(new_root.join("WASSUP-README.md")).unwrap(), readme);

    let page = reopened.saved_messages(None, 10).unwrap();
    let original = page
        .messages
        .iter()
        .find(|row| row.message_id == "original")
        .unwrap();
    assert_eq!(original.text, "Original text");
    assert_eq!(original.history.len(), 2);
    assert!(original.deletion_observed);
    let photo = page
        .messages
        .iter()
        .find(|row| row.message_id == "saved-photo")
        .unwrap();
    let relocated_path = reopened.root().join("media/fixture.jpg");
    assert_eq!(photo.attachments[0].path.as_ref(), Some(&relocated_path));
    assert_ne!(fs::read(&relocated_path).unwrap(), b"fixture");
    let exported = fixture.path().join("exported");
    reopened
        .export_readable(&exported, &[CHAT.to_owned()])
        .unwrap();
    assert_eq!(
        fs::read(exported.join("media/fixture.jpg")).unwrap(),
        b"fixture"
    );
    assert!(photo.markdown_path.starts_with(reopened.root()));
    assert!(!photo.markdown_path.exists());
    let markdown = fs::read_to_string(
        exported.join(photo.markdown_path.strip_prefix(reopened.root()).unwrap()),
    )
    .unwrap();
    assert!(markdown.contains("[Open file](../media/fixture.jpg)"));
    assert!(!markdown.contains(old_root.to_string_lossy().as_ref()));
    let persisted_path: String = reopened
        .inner
        .connection
        .lock()
        .unwrap()
        .query_row(
            "SELECT path FROM media_jobs WHERE id=?1",
            [saved_job.id],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(persisted_path, "media/fixture.jpg");
    let resumed = reopened.claim_media_job().unwrap().unwrap();
    assert_eq!(resumed.id, pending_job.id);
    assert_eq!(resumed.raw, pending_job.raw);
    assert!(!old_root.exists());
}

fn info(id: &str) -> MessageInfo {
    MessageInfo {
        id: id.into(),
        source: MessageSource {
            chat: CHAT.parse().unwrap(),
            sender: CHAT.parse().unwrap(),
            ..Default::default()
        },
        timestamp: whatsapp_rust::wacore::time::from_secs(1_790_000_000).unwrap(),
        ..Default::default()
    }
}

fn text(value: &str) -> wa::Message {
    let mut message = wa::Message::default();
    message.conversation = Some(value.to_owned());
    message
}

fn protocol(
    kind: wa::message::protocol_message::Type,
    target: &str,
    body: Option<&str>,
) -> wa::Message {
    let mut protocol = wa::message::ProtocolMessage::default();
    protocol.r#type = Some(kind);
    let mut key = wa::MessageKey::default();
    key.id = Some(target.to_owned());
    protocol.key = MessageField::some(key);
    if let Some(body) = body {
        protocol.edited_message = MessageField::some(text(body));
    }
    let mut message = wa::Message::default();
    message.protocol_message = MessageField::some(protocol);
    message
}

fn events(capture: &Capture) -> Vec<(String, String, Vec<u8>)> {
    let connection = capture.inner.connection.lock().unwrap();
    let mut statement = connection
        .prepare("SELECT kind,text,raw FROM events ORDER BY sequence")
        .unwrap();
    statement
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<std::result::Result<Vec<_>, _>>()
        .unwrap()
}

#[test]
fn originals_repeated_edits_and_revoke_survive_and_replay_is_idempotent() {
    use wa::message::protocol_message::Type;
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    let original = text("A\nSecond line");
    capture
        .record_incoming(&original, &info("original"), "durable_inbound")
        .unwrap();
    capture
        .record_incoming(&original, &info("original"), "recovered_event")
        .unwrap();
    capture
        .record_incoming(
            &protocol(Type::MESSAGE_EDIT, "original", Some("B")),
            &info("edit-one"),
            "durable_inbound",
        )
        .unwrap();
    capture
        .record_incoming(
            &protocol(Type::MESSAGE_EDIT, "original", Some("A\nSecond line")),
            &info("edit-two"),
            "durable_inbound",
        )
        .unwrap();
    capture
        .record_incoming(
            &protocol(Type::REVOKE, "original", None),
            &info("revoke"),
            "durable_inbound",
        )
        .unwrap();
    let records = events(&capture);
    assert_eq!(records.len(), 4);
    assert_eq!(
        records[0],
        (
            "message".to_owned(),
            "A\nSecond line".to_owned(),
            original.encode_to_vec()
        )
    );
    assert_eq!(records[1].1, "B");
    assert_eq!(records[2].1, "A\nSecond line");
    assert_eq!(records[3].0, "deleted");
    let exports = tempfile::tempdir().unwrap();
    let exported = exports.path().join("readable");
    capture
        .export_readable(&exported, &[CHAT.to_owned()])
        .unwrap();
    let page = fs::read_dir(exported.join("chats"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let readable = fs::read_to_string(page).unwrap();
    assert!(readable.contains("> Second line"));
    assert!(readable.contains("> B"));
    assert!(readable.contains("Sender requested deletion"));
    assert_eq!(readable.matches("## ").count(), 4);
}

#[test]
fn revoke_before_original_and_equal_text_distinct_edits_are_retained() {
    use wa::message::protocol_message::Type;
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    capture
        .record_incoming(
            &protocol(Type::REVOKE, "original", None),
            &info("revoke"),
            "durable_inbound",
        )
        .unwrap();
    capture
        .record_incoming(&text("original"), &info("original"), "durable_inbound")
        .unwrap();
    let edit = protocol(Type::MESSAGE_EDIT, "original", Some("same text"));
    capture
        .record_incoming(&edit, &info("edit-one"), "durable_inbound")
        .unwrap();
    capture
        .record_incoming(&edit, &info("edit-two"), "durable_inbound")
        .unwrap();
    assert_eq!(events(&capture).len(), 4);
}

#[test]
fn transaction_failure_rolls_back_entire_batch_and_sets_paused() {
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    let first = incoming_event(&text("one"), &info("one"), "durable_inbound").unwrap();
    let second = incoming_event(&text("two"), &info("two"), "durable_inbound").unwrap();
    // Invalid permanent attachment location fails after the second INSERT.
    let failed = capture.record_entries(&[
        (first.clone(), None),
        (second, Some(temp.path().join("outside"))),
    ]);
    assert!(failed.is_err());
    assert!(events(&capture).is_empty());
    assert!(capture.status().paused);
    assert!(capture.status().last_saved_at.is_none());
    capture.record_event(first).unwrap();
    assert!(!capture.status().paused);
    assert!(capture.status().last_saved_at.is_some());
}

#[test]
fn storage_failure_never_reports_success() {
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    capture
        .inner
        .connection
        .lock()
        .unwrap()
        .execute_batch("PRAGMA query_only=ON")
        .unwrap();
    assert!(
        capture
            .record_incoming(&text("not saved"), &info("one"), "durable_inbound")
            .is_err()
    );
    assert!(capture.status().paused);
    assert!(capture.status().last_saved_at.is_none());
    assert!(events(&capture).is_empty());
}

#[test]
fn account_and_group_sender_identity_prevent_dedup_collisions() {
    let temp = tempfile::tempdir().unwrap();
    let one = Capture::open(temp.path(), "account-one").unwrap();
    let two = Capture::open(temp.path(), "account-two").unwrap();
    let mut first = info("same-envelope-id");
    first.source.chat = "123456789@g.us".parse().unwrap();
    let mut second = first.clone();
    second.source.sender = "15550002222@s.whatsapp.net".parse().unwrap();
    one.record_incoming(&text("same"), &first, "durable_inbound")
        .unwrap();
    one.record_incoming(&text("same"), &second, "durable_inbound")
        .unwrap();
    two.record_incoming(&text("same"), &first, "durable_inbound")
        .unwrap();
    assert_eq!(events(&one).len(), 2);
    assert_eq!(events(&two).len(), 1);
    assert_ne!(one.root(), two.root());
}

#[test]
fn view_once_channels_and_status_are_excluded() {
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    let mut image = wa::message::ImageMessage::default();
    image.view_once = Some(true);
    let mut body = wa::Message::default();
    body.image_message = MessageField::some(image);
    let mut wrapper = wa::message::FutureProofMessage::default();
    wrapper.message = MessageField::some(body);
    let mut once = wa::Message::default();
    once.view_once_message_v2 = MessageField::some(wrapper);
    capture
        .record_incoming(&once, &info("once"), "durable_inbound")
        .unwrap();
    for chat in ["status@broadcast", "123456789@newsletter"] {
        let mut metadata = info("excluded");
        metadata.source.chat = chat.parse().unwrap();
        capture
            .record_incoming(&text("excluded"), &metadata, "durable_inbound")
            .unwrap();
    }
    assert!(events(&capture).is_empty());
}

#[test]
fn immutable_events_cannot_be_updated_or_deleted() {
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    capture
        .record_incoming(&text("retained"), &info("one"), "durable_inbound")
        .unwrap();
    let connection = capture.inner.connection.lock().unwrap();
    assert!(
        connection
            .execute("UPDATE events SET text='replacement'", [])
            .is_err()
    );
    assert!(connection.execute("DELETE FROM events", []).is_err());
}

#[test]
fn media_queue_survives_restart_and_pauses_do_not_consume_attempts() {
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    let mut image = wa::message::ImageMessage::default();
    image.mimetype = Some("image/jpeg".to_owned());
    image.file_length = Some(10);
    let mut body = wa::Message::default();
    body.image_message = MessageField::some(image);
    capture
        .record_incoming(&body, &info("photo"), "durable_inbound")
        .unwrap();
    assert_eq!(capture.status().queued, 1);
    let job = capture.claim_media_job().unwrap().unwrap();
    assert_eq!(job.attempts, 1);
    assert_eq!(job.raw, body.encode_to_vec());
    drop(capture);
    let reopened = Capture::open(temp.path(), "account-one").unwrap();
    let job = reopened.claim_media_job().unwrap().unwrap();
    assert_eq!(job.id, 1);
    reopened
        .finish_media_job(
            job.id,
            "paused",
            None,
            Some("Connection unavailable"),
            Some(0),
        )
        .unwrap();
    let retried = reopened.claim_media_job().unwrap().unwrap();
    assert_eq!(retried.attempts, job.attempts);
    reopened
        .finish_media_job(job.id, "failed", None, Some("Unavailable"), None)
        .unwrap();
    assert!(reopened.claim_media_job().unwrap().is_none());
    assert_eq!(reopened.status().failed, 1);
}

#[tokio::test]
async fn durable_small_message_commit_stays_under_one_second() {
    use whatsapp_rust::{prelude::Bot, store::SqliteStore};
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(&temp.path().join("external"), "latency-fixture").unwrap();
    let store = SqliteStore::open(temp.path().join("session.db").to_str().unwrap())
        .await
        .unwrap();
    let bot = Bot::builder()
        .with_backend(store)
        .with_inbound_durability_hook_arc(capture.clone())
        .build()
        .await
        .unwrap();
    let mut longest = Duration::ZERO;
    let mut total = Duration::ZERO;
    for index in 0..20 {
        let message = InboundMessage::builder()
            .message(Arc::new(text("Synthetic latency fixture")))
            .info(Arc::new(info(&format!("latency-{index}"))))
            .build();
        let started = std::time::Instant::now();
        capture.on_messages(bot.client(), &[message]).await.unwrap();
        let elapsed = started.elapsed();
        longest = longest.max(elapsed);
        total += elapsed;
    }
    eprintln!(
        "Wassup capture fixture: 20 hook-to-commit calls, total={total:?}, mean={:?}, max={longest:?}",
        total / 20
    );
    assert!(
        longest < Duration::from_secs(1),
        "Longest fixture commit: {longest:?}"
    );
}

#[test]
fn incoming_broadcast_list_is_captured_under_its_direct_chat_owner() {
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    let mut metadata = info("list-message");
    metadata.source.chat = "123456789@broadcast".parse().unwrap();
    metadata.source.broadcast_list_owner = Some("15550003333@s.whatsapp.net".parse().unwrap());
    capture
        .record_incoming(&text("List delivery"), &metadata, "durable_inbound")
        .unwrap();
    let chat: String = capture
        .inner
        .connection
        .lock()
        .unwrap()
        .query_row("SELECT chat FROM events", [], |row| row.get(0))
        .unwrap();
    assert_eq!(chat, "15550003333@s.whatsapp.net");
    metadata.source.chat = "status@broadcast".parse().unwrap();
    capture
        .record_incoming(
            &text("Status must stay excluded"),
            &metadata,
            "durable_inbound",
        )
        .unwrap();
    assert_eq!(events(&capture).len(), 1);
}

#[cfg(unix)]
#[test]
fn archive_files_are_private_and_symlink_destinations_are_rejected() {
    use std::os::unix::fs::{PermissionsExt, symlink};
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    assert_eq!(
        fs::metadata(capture.root()).unwrap().permissions().mode() & 0o777,
        0o700
    );
    assert_eq!(
        fs::metadata(capture.root().join("journal.sqlite3"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let link = temp.path().join("redirected");
    symlink(capture.root(), &link).unwrap();
    assert!(Capture::open(&link, "account-two").is_err());
}

#[tokio::test]
async fn sdk_hook_waits_for_durable_batch_and_returns_errors() {
    use whatsapp_rust::{prelude::Bot, store::SqliteStore};
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(&temp.path().join("external"), "account-one").unwrap();
    let store = SqliteStore::open(temp.path().join("session.db").to_str().unwrap())
        .await
        .unwrap();
    // Constructing validates the SDK durability backend. Never run/connect it.
    let bot = Bot::builder()
        .with_backend(store)
        .with_inbound_durability_hook_arc(capture.clone())
        .build()
        .await
        .unwrap();
    let first = InboundMessage::builder()
        .message(Arc::new(text("first")))
        .info(Arc::new(info("first")))
        .build();
    let second = InboundMessage::builder()
        .message(Arc::new(text("second")))
        .info(Arc::new(info("second")))
        .build();
    capture
        .on_messages(bot.client(), &[first.clone(), second])
        .await
        .unwrap();
    assert_eq!(events(&capture).len(), 2);
    // Replaying the whole committed batch is harmless.
    capture.on_messages(bot.client(), &[first]).await.unwrap();
    assert_eq!(events(&capture).len(), 2);
    capture
        .inner
        .connection
        .lock()
        .unwrap()
        .execute_batch("PRAGMA query_only=ON")
        .unwrap();
    let failed = InboundMessage::builder()
        .message(Arc::new(text("not committed")))
        .info(Arc::new(info("failed")))
        .build();
    assert!(capture.on_messages(bot.client(), &[failed]).await.is_err());
    assert_eq!(events(&capture).len(), 2);
    assert!(capture.status().paused);
}

#[test]
fn own_message_echo_uses_local_original_and_does_not_hide_edits() {
    let temp = tempfile::tempdir().unwrap();
    let capture = Capture::open(temp.path(), "account-one").unwrap();
    // Echo can race ahead of the local record; captions can differ in formatting.
    let mut own_info = info("photo-id");
    own_info.source.is_from_me = true;
    capture
        .record_incoming(&text("caption"), &own_info, "durable_inbound")
        .unwrap();
    capture
        .record_event(CaptureEvent {
            event_id: "photo-id".to_owned(),
            chat: CHAT.to_owned(),
            sender: CHAT.to_owned(),
            message_id: "photo-id".to_owned(),
            kind: "outgoing_pending".to_owned(),
            text: "Photo: caption".to_owned(),
            timestamp: own_info.timestamp.timestamp(),
            direction: "outgoing".to_owned(),
            ..Default::default()
        })
        .unwrap();
    let exports = tempfile::tempdir().unwrap();
    let exported = exports.path().join("readable");
    capture
        .export_readable(&exported, &[CHAT.to_owned()])
        .unwrap();
    let page = fs::read_dir(exported.join("chats"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let rendered = fs::read_to_string(page).unwrap();
    assert_eq!(rendered.matches("## ").count(), 1);
    assert!(rendered.contains("Photo: caption"));
    assert_eq!(
        events(&capture).len(),
        2,
        "journal preserves both observations"
    );
}

#[test]
fn capture_is_encrypted_and_readable_only_when_explicitly_exported() {
    let archive = tempfile::tempdir().unwrap();
    let capture = Capture::open(archive.path(), "encrypted-fixture").unwrap();
    let marker = "PRIVATE_SYNTHETIC_MARKER_54ee";
    capture
        .record_incoming(&text(marker), &info("secret"), "durable_inbound")
        .unwrap();
    assert!(!capture.root().join("chats").exists());
    assert!(!capture.root().join("index.md").exists());
    // The in-app projection remains directly available without an export.
    assert_eq!(
        capture.saved_messages(None, 10).unwrap().messages[0].text,
        marker
    );
    let plain = Connection::open(capture.root().join("journal.sqlite3")).unwrap();
    assert!(
        plain
            .query_row("SELECT COUNT(*) FROM events", [], |row| row
                .get::<_, i64>(0))
            .is_err()
    );
    for name in ["journal.sqlite3", "journal.sqlite3-wal"] {
        let bytes = fs::read(capture.root().join(name)).unwrap();
        assert!(
            !bytes
                .windows(marker.len())
                .any(|bytes| bytes == marker.as_bytes())
        );
    }
    let exports = tempfile::tempdir().unwrap();
    let destination = exports.path().join("requested");
    capture
        .export_readable(&destination, &[CHAT.to_owned()])
        .unwrap();
    let page = fs::read_dir(destination.join("chats"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    assert!(
        fs::read_to_string(page)
            .unwrap()
            .contains(&escape_markdown(marker))
    );
    assert!(!capture.root().join("chats").exists());
}

#[test]
fn export_is_scoped_cancelable_and_never_overwrites_a_destination() {
    use std::sync::atomic::AtomicBool;
    let archive = tempfile::tempdir().unwrap();
    let capture = Capture::open(archive.path(), "export-fixture").unwrap();
    capture
        .record_incoming(&text("selected"), &info("one"), "durable_inbound")
        .unwrap();
    let mut other = info("two");
    other.source.chat = "15550002222@s.whatsapp.net".parse().unwrap();
    capture
        .record_incoming(&text("unselected"), &other, "durable_inbound")
        .unwrap();
    let exports = tempfile::tempdir().unwrap();
    let destination = exports.path().join("selected");
    assert!(
        capture
            .export_readable_cancellable(&destination, &[CHAT.to_owned()], &AtomicBool::new(true))
            .is_err()
    );
    assert!(!destination.exists());
    assert_eq!(fs::read_dir(exports.path()).unwrap().count(), 0);
    capture
        .export_readable(&destination, &[CHAT.to_owned()])
        .unwrap();
    assert_eq!(fs::read_dir(destination.join("chats")).unwrap().count(), 1);
    fs::write(destination.join("keep.txt"), "keep").unwrap();
    assert!(
        capture
            .export_readable(&destination, &[CHAT.to_owned()])
            .is_err()
    );
    assert_eq!(
        fs::read_to_string(destination.join("keep.txt")).unwrap(),
        "keep"
    );
    assert!(
        capture
            .export_readable(&archive.path().join("plaintext"), &[CHAT.to_owned()])
            .is_err()
    );
    assert!(
        capture
            .export_readable(&exports.path().join("all"), &[])
            .is_err()
    );
}

#[test]
fn legacy_readable_files_are_preserved_encrypted_then_removed() {
    let archive = tempfile::tempdir().unwrap();
    let capture = Capture::open(archive.path(), "legacy-fixture").unwrap();
    capture
        .record_incoming(&text("original"), &info("one"), "durable_inbound")
        .unwrap();
    let root = capture.root().to_owned();
    drop(capture);
    fs::create_dir(root.join("chats")).unwrap();
    let name = format!("chats/{}-2026-11.md", hex_digest(CHAT.as_bytes()));
    let edited = b"A prior generated transcript with a personal annotation";
    fs::write(root.join(&name), edited).unwrap();
    fs::write(root.join("index.md"), b"old index").unwrap();
    let reopened = Capture::open(archive.path(), "legacy-fixture").unwrap();
    assert!(!root.join("chats").exists());
    assert!(!root.join("index.md").exists());
    let saved: Vec<u8> = reopened
        .reader()
        .unwrap()
        .query_row(
            "SELECT bytes FROM legacy_exports WHERE path=?1 AND chunk=0",
            [&name],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(saved, edited);
    assert_eq!(
        reopened.saved_messages(None, 10).unwrap().messages[0].text,
        "original"
    );
    drop(reopened);
    let again = Capture::open(archive.path(), "legacy-fixture").unwrap();
    assert_eq!(
        again
            .reader()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM legacy_exports", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        2
    );
}
