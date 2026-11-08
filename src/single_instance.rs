//! One running Wassup per user, in its independent profile runtime directory.
//! No fixed port or legacy application protocol is probed or served.

use std::path::Path;
use std::sync::{Arc, Mutex};

const NAME: &str = "wassup";
const OK: &str = "ok";
#[cfg(test)]
const PREFIX: &str = "wassup:";

pub enum Outcome {
    /// This process owns the instance guard.
    Only(Guard),
    /// Another instance is running and received the request.
    Surfaced,
    /// Another instance is running but could not take the request (an older
    /// copy that does not know the verb), so it was asked to show its window
    /// instead.
    Outdated,
    /// Another instance holds the lock but did not answer.
    Unanswered,
}

/// Request from another launch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlCommand {
    /// Shows or creates the window.
    Show,
    /// Reload local theme files without opening the window.
    ReloadThemes,
    /// Quits, as the tray's Quit does: from the launcher's Quit action.
    Quit,
    /// Confirms an instance is running and changes nothing.
    Ping,
    /// Opens the chat a link named, showing the window as it does.
    OpenChat(crate::target::Request),
    /// Opens the invite dialog for the group a link named.
    OpenInvite(String),
}

type Queue = Arc<Mutex<Vec<ControlCommand>>>;

/// Marks this process as the running instance until it exits.
pub struct Guard {
    /// Requests queued by later launches.
    commands: Queue,
    /// Held until the process exits.
    _claim: fastframe_instance::Guard,
}

impl Guard {
    /// Shared request queue drained by the app.
    pub fn commands(&self) -> Queue {
        Arc::clone(&self.commands)
    }
}

/// Wassup's slot: its runtime directory, with the prefix older copies use.
fn slot(dir: &Path) -> fastframe_instance::Slot {
    fastframe_instance::Slot::at(dir, NAME)
}

/// Becomes the running instance, or hands `verb` to the one already running.
///
/// Whatever the running copy makes of `verb`, a launch that finds one never
/// runs beside it on the same archive and linked device: a copy too old to
/// know the verb is asked to show its window instead.
pub fn acquire(dir: &Path, waker: &crate::backend::Waker, verb: &str) -> Outcome {
    let commands = Queue::default();
    let claim = match claim(dir, verb, &commands, waker) {
        fastframe_instance::Claim::First(claim) => claim,
        fastframe_instance::Claim::Running(_) => return Outcome::Surfaced,
        // The copy declined the verb, or ignored it as copies from before
        // `fastframe-instance` ignore what they do not know: it holds the
        // slot either way, so it is at most brought forward.
        fastframe_instance::Claim::Unanswered | fastframe_instance::Claim::Declined => {
            return fall_back(verb, || matches!(send(dir, "show"), Sent::Taken));
        }
    };
    Outcome::Only(Guard {
        commands,
        _claim: claim,
    })
}

/// What a launch reports when the copy holding the slot did not take `verb`.
/// A request beyond showing the window is retried as `show`, which every
/// copy answers; `show` and `ping` themselves are not, as their answer is
/// already the one being waited for.
fn fall_back(verb: &str, show: impl FnOnce() -> bool) -> Outcome {
    if matches!(verb, "show" | "ping") {
        return Outcome::Unanswered;
    }
    // One more try, without waiting for a copy that may still be starting:
    // the first request already waited for it.
    if show() {
        Outcome::Outdated
    } else {
        Outcome::Unanswered
    }
}

/// Takes the slot, queueing what later launches ask for, or hands `verb` to
/// the copy that holds it.
fn claim(
    dir: &Path,
    verb: &str,
    commands: &Queue,
    waker: &crate::backend::Waker,
) -> fastframe_instance::Claim {
    let (commands, waker) = (Arc::clone(commands), waker.clone());
    slot(dir).claim(verb, move |request| {
        let command = parse(request)?;
        commands
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .push(command);
        waker.wake();
        Some(OK.to_owned())
    })
}

/// What became of a request [`send`] made of the running copy.
#[derive(Debug)]
pub enum Sent {
    /// The running copy took the request.
    Taken,
    /// No copy is running.
    NotRunning,
    /// A Wassup copy holds the slot but does not understand the request.
    Older,
    /// A copy holds the slot but could not be asked.
    Failed(std::io::Error),
}

/// Sends one request to the running copy, without becoming it when there is
/// none: for a command-line action that only makes sense while Wassup runs.
pub fn send(dir: &Path, verb: &str) -> Sent {
    sent(slot(dir).send(verb).map(drop))
}

/// Reads the result without contacting any other application identity.
fn sent(result: std::io::Result<()>) -> Sent {
    use std::io::ErrorKind;
    match result {
        Ok(()) => Sent::Taken,
        Err(error) => match error.kind() {
            ErrorKind::NotFound | ErrorKind::ConnectionRefused => Sent::NotRunning,
            // Only Wassup listens in the slot, so a refusal or a silent
            // close comes from a copy older than the request.
            ErrorKind::PermissionDenied | ErrorKind::InvalidData | ErrorKind::UnexpectedEof => {
                Sent::Older
            }
            _ => Sent::Failed(error),
        },
    }
}

/// The verbs another launch may send. Anything else is declined.
///
/// `open` carries the whole request, so it is read and held to the same rule
/// [`crate::target::parse`] applies rather than being trusted as it arrives.
fn parse(verb: &str) -> Option<ControlCommand> {
    if let Some(request) = verb.strip_prefix("open ") {
        return crate::target::Request::from_verb(request).map(ControlCommand::OpenChat);
    }
    if let Some(code) = verb.strip_prefix("invite ") {
        // Held to the rule a clicked link meets, not trusted as it arrives.
        let url = format!("https://chat.whatsapp.com/{code}");
        return crate::safety::group_invite_code(&url).map(ControlCommand::OpenInvite);
    }
    match verb {
        "show" => Some(ControlCommand::Show),
        "reload-themes" => Some(ControlCommand::ReloadThemes),
        "quit" => Some(ControlCommand::Quit),
        "ping" => Some(ControlCommand::Ping),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};

    #[test]
    fn only_our_own_verbs_are_understood() {
        assert_eq!(parse("show"), Some(ControlCommand::Show));
        assert_eq!(parse("ping"), Some(ControlCommand::Ping));
        assert_eq!(parse("reload-themes"), Some(ControlCommand::ReloadThemes));
        assert_eq!(
            parse("open 20123456789@s.whatsapp.net"),
            Some(ControlCommand::OpenChat(crate::target::Request {
                chat: "20123456789@s.whatsapp.net".into(),
                text: None
            }))
        );
        assert_eq!(
            parse("open 20123456789@s.whatsapp.net\tHello%20there"),
            Some(ControlCommand::OpenChat(crate::target::Request {
                chat: "20123456789@s.whatsapp.net".into(),
                text: Some("Hello there".into())
            }))
        );
        assert_eq!(
            parse("invite AbCdEf1234567890XyZ"),
            Some(ControlCommand::OpenInvite("AbCdEf1234567890XyZ".into()))
        );
        assert_eq!(parse("invite ../evil"), None);
        assert_eq!(parse("invite short"), None);
        // A chat this copy would not have opened is declined, not acted on.
        assert_eq!(parse("open 20123456789@lid"), None);
        assert_eq!(parse("open"), None);
        assert_eq!(parse("quit"), Some(ControlCommand::Quit));
        assert_eq!(parse("GET / HTTP/1.1"), None);
        assert_eq!(parse("frobnicate"), None);
        assert_eq!(parse(""), None);
    }

    const INVITE: &str = "invite AbCdEf1234567890XyZ";

    /// What a copy holding the slot was asked for.
    type Seen = Arc<Mutex<Vec<String>>>;

    /// A copy from before `invite` declines it. The launch must not start
    /// beside it, so the copy is asked to show its window instead.
    #[test]
    fn a_copy_that_declines_the_verb_is_brought_forward() {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join("runtime");
        let seen = Seen::default();
        let record = Arc::clone(&seen);
        let fastframe_instance::Claim::First(older) = slot(&dir).claim("show", move |request| {
            record.lock().unwrap().push(request.to_owned());
            (request == "show").then(|| OK.to_owned())
        }) else {
            panic!("the older copy takes the slot");
        };
        let outcome = acquire(&dir, &crate::backend::Waker::default(), INVITE);
        assert!(matches!(outcome, Outcome::Outdated));
        assert_eq!(
            *seen.lock().unwrap(),
            vec![INVITE.to_owned(), "show".to_owned()]
        );
        drop(older);
    }

    /// A copy that closes the connection on a verb it does not know, without
    /// a reply, is still running: it is asked to show its window too.
    #[cfg(unix)]
    #[test]
    fn a_copy_that_ignores_the_verb_is_brought_forward() {
        use std::io::BufRead;
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join("runtime");
        std::fs::create_dir_all(&dir).unwrap();
        let lock = std::fs::File::create(dir.join("instance.lock")).unwrap();
        lock.try_lock().expect("the older copy's lock");
        let listener = std::os::unix::net::UnixListener::bind(dir.join("instance.sock")).unwrap();
        let seen = Seen::default();
        let record = Arc::clone(&seen);
        std::thread::spawn(move || {
            for stream in listener.incoming().flatten() {
                let mut line = String::new();
                let mut reader = std::io::BufReader::new(&stream);
                if reader.read_line(&mut line).is_err() {
                    continue;
                }
                let line = line.trim_end().to_owned();
                if line == "wassup:show" {
                    let _ = (&stream).write_all(b"wassup:ok\n");
                }
                record.lock().unwrap().push(line);
            }
        });
        let outcome = acquire(&dir, &crate::backend::Waker::default(), INVITE);
        assert!(matches!(outcome, Outcome::Outdated));
        let seen = seen.lock().unwrap();
        assert_eq!(seen.last().map(String::as_str), Some("wassup:show"));
        assert!(seen.iter().any(|line| line == &format!("{PREFIX}{INVITE}")));
        drop(lock);
    }

    /// Showing the window is the fallback, so a copy that does not take
    /// `show` or `ping` is not asked again, and one that answers nothing at
    /// all is reported as before.
    #[test]
    fn only_requests_beyond_the_window_fall_back_to_showing_it() {
        let never = || -> bool { panic!("not asked again") };
        assert!(matches!(fall_back("show", never), Outcome::Unanswered));
        assert!(matches!(fall_back("ping", never), Outcome::Unanswered));
        assert!(matches!(fall_back(INVITE, || true), Outcome::Outdated));
        assert!(matches!(fall_back(INVITE, || false), Outcome::Unanswered));
        assert!(matches!(
            fall_back("open 20123456789@s.whatsapp.net", || true),
            Outcome::Outdated
        ));
    }

    /// Sends `line` the way a copy from before fastframe-instance does, with
    /// `token` first on Windows, and returns the raw reply.
    fn raw_request(dir: &Path, line: &str, token: Option<&str>) -> String {
        #[cfg(unix)]
        let mut stream = {
            let _ = token;
            std::os::unix::net::UnixStream::connect(dir.join("instance.sock")).unwrap()
        };
        #[cfg(not(unix))]
        let mut stream = {
            let key = std::fs::read_to_string(dir.join("instance.key")).unwrap();
            let mut key = key.lines();
            let port: u16 = key.next().unwrap().parse().unwrap();
            let written = key.next().unwrap().to_owned();
            let mut stream =
                std::net::TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, port)).unwrap();
            let token = token.map_or(written, str::to_owned);
            stream.write_all(format!("{token}\n").as_bytes()).unwrap();
            stream
        };
        stream.write_all(format!("{line}\n").as_bytes()).unwrap();
        let mut reply = String::new();
        let _ = stream.read_to_string(&mut reply);
        reply
    }

    /// A second launch reaches the first, whether it is this version or one
    /// from before fastframe-instance, which writes and expects the same
    /// lines: `wassup:show` in, `wassup:ok` out. Themes reload without a
    /// window. Unknown verbs are declined, and on Windows a wrong token gets
    /// no reply.
    #[test]
    fn a_second_launch_reaches_the_queue_on_the_old_wire() {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join("runtime");
        let waker = crate::backend::Waker::default();
        let commands = Queue::default();
        let fastframe_instance::Claim::First(first) = claim(&dir, "show", &commands, &waker) else {
            panic!("the first launch takes the slot");
        };

        // A launch of this version.
        let second = claim(&dir, "show", &Queue::default(), &waker);
        assert!(matches!(second, fastframe_instance::Claim::Running(reply) if reply == OK));
        assert!(matches!(send(&dir, "reload-themes"), Sent::Taken));
        assert!(matches!(send(&dir, "quit"), Sent::Taken));
        assert!(
            matches!(send(&dir, "frobnicate"), Sent::Older),
            "a verb this copy does not know is declined"
        );
        assert!(matches!(
            claim(&dir, "frobnicate", &Queue::default(), &waker),
            fastframe_instance::Claim::Declined
        ));

        // A launch of an older version.
        assert_eq!(raw_request(&dir, "wassup:show", None), "wassup:ok\n");
        assert_eq!(
            raw_request(&dir, "wassup:frobnicate", None),
            "wassup!declined\n",
            "which an older copy reads as no answer"
        );
        #[cfg(not(unix))]
        assert_eq!(
            raw_request(&dir, "wassup:show", Some(&"0".repeat(64))),
            "",
            "a wrong token is refused"
        );

        assert_eq!(
            *commands.lock().unwrap(),
            vec![
                ControlCommand::Show,
                ControlCommand::ReloadThemes,
                ControlCommand::Quit,
                ControlCommand::Show,
            ]
        );
        drop(first);
    }

    /// `wassup quit` against a copy from before the verb: one that declines
    /// it is left running and reported as older, and nothing new starts.
    #[test]
    fn a_copy_that_declines_quit_is_reported_as_older() {
        let home = tempfile::tempdir().unwrap();
        let dir = home.path().join("runtime");
        let asked = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&asked);
        // The handler of a copy that knows only `show` and `ping`.
        let fastframe_instance::Claim::First(older) = slot(&dir).claim("show", move |request| {
            seen.lock().unwrap().push(request.to_owned());
            matches!(request, "show" | "ping").then(|| OK.to_owned())
        }) else {
            panic!("the older copy takes the slot");
        };
        assert!(matches!(send(&dir, "quit"), Sent::Older));
        assert_eq!(*asked.lock().unwrap(), vec!["quit".to_owned()]);
        drop(older);
    }

    /// Copies from before declining existed close the connection without a
    /// word when they do not know a verb.
    #[cfg(unix)]
    #[test]
    fn a_copy_that_ignores_quit_is_reported_as_older() {
        let home = tempfile::tempdir().unwrap();
        let socket = home.path().join("instance.sock");
        let listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
        let older = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut line = [0u8; 64];
            let _ = stream.read(&mut line);
        });
        assert!(matches!(send(home.path(), "quit"), Sent::Older));
        older.join().unwrap();
    }

    /// Missing private state means no Wassup copy runs. Other application
    /// endpoints are never consulted.
    #[test]
    fn a_request_reads_who_answered() {
        use std::io::{Error, ErrorKind};
        let none = || Error::from(ErrorKind::NotFound);
        assert!(matches!(sent(Ok(())), Sent::Taken));
        assert!(matches!(sent(Err(none())), Sent::NotRunning));
        assert!(matches!(
            sent(Err(ErrorKind::ConnectionRefused.into())),
            Sent::NotRunning
        ));
        assert!(matches!(
            sent(Err(ErrorKind::PermissionDenied.into())),
            Sent::Older
        ));
        assert!(matches!(
            sent(Err(ErrorKind::TimedOut.into())),
            Sent::Failed(_)
        ));
    }
}
