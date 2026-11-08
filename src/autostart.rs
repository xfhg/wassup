//! Starting Wassup in the tray when the person logs in, through
//! fastframe-autostart.
//!
//! The platform's own login-item record is the only state: Settings reads it
//! back rather than keeping a copy that could disagree with it. Every read and
//! change may block (`SMAppService`, the registry, the Flatpak portal, which can
//! ask the person first), so each runs on a thread of its own.

use std::sync::mpsc::{Receiver, TryRecvError};

use fastframe_autostart::{Autostart, Start, State};

fn autostart() -> Autostart {
    Autostart::new("wassup", "Wassup")
}

/// What a finished read or change found.
#[derive(Debug)]
pub struct Outcome {
    /// The login item as the platform has it now, or why it could not be
    /// changed.
    pub state: Result<State, String>,
    /// Whether the person asked for the change, rather than a read at start.
    pub changed: bool,
    /// The requested value, or `None` for a status read.
    pub requested: Option<bool>,
}

/// The one read or change of the login item in flight.
#[derive(Default)]
pub struct LoginItem {
    job: Option<Receiver<Outcome>>,
}

impl LoginItem {
    /// Reads only this application bundle's login item.
    pub fn read(&mut self, wake: impl Fn() + Send + 'static) {
        self.spawn(
            move || Outcome {
                state: Ok(autostart().state()),
                changed: false,
                requested: None,
            },
            wake,
        );
    }

    /// Starts Wassup in the tray at login, or not at all.
    pub fn set(&mut self, enabled: bool, wake: impl Fn() + Send + 'static) {
        self.spawn(
            move || Outcome {
                state: autostart()
                    .set(enabled.then_some(Start::Hidden))
                    .map_err(|error| error.to_string()),
                changed: true,
                requested: Some(enabled),
            },
            wake,
        );
    }

    fn spawn(
        &mut self,
        work: impl FnOnce() -> Outcome + Send + 'static,
        wake: impl Fn() + Send + 'static,
    ) {
        let (sender, receiver) = std::sync::mpsc::channel();
        let spawned = std::thread::Builder::new()
            .name("login-item".into())
            .spawn(move || {
                let _ = sender.send(work());
                wake();
            });
        match spawned {
            Ok(_) => self.job = Some(receiver),
            Err(error) => log::warn!("could not read the login item: {error}"),
        }
    }

    /// The finished read or change, if there is one.
    pub fn poll(&mut self) -> Option<Outcome> {
        match self.job.as_ref()?.try_recv() {
            Ok(outcome) => {
                self.job = None;
                Some(outcome)
            }
            Err(TryRecvError::Empty) => None,
            Err(TryRecvError::Disconnected) => {
                self.job = None;
                None
            }
        }
    }
}

/// Whether Settings offers the switch, and where it stands: `None` where this
/// installation cannot start at login.
pub fn switch(state: State) -> Option<bool> {
    match state {
        State::Unsupported => None,
        State::Off => Some(false),
        State::On(_) | State::NeedsApproval(_) => Some(true),
    }
}

/// One initial registration attempt, while respecting a later explicit disable.
#[derive(Debug, PartialEq, Eq)]
pub enum InitialSetup {
    Enable,
    Remember,
    Wait,
}

pub fn initial_setup(complete: bool, state: State, requested: Option<bool>) -> InitialSetup {
    if complete {
        return InitialSetup::Wait;
    }
    match state {
        State::On(_) | State::NeedsApproval(_) => InitialSetup::Remember,
        // A confirmed explicit disable is also a completed user choice.
        State::Off if requested == Some(false) => InitialSetup::Remember,
        State::Off if requested.is_none() => InitialSetup::Enable,
        // Unsupported/unconfirmed registration is retried at a later launch.
        State::Off | State::Unsupported => InitialSetup::Wait,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_switch_shows_only_where_zapfast_can_start_at_login() {
        assert_eq!(switch(State::Unsupported), None);
        assert_eq!(switch(State::Off), Some(false));
        assert_eq!(switch(State::On(Start::Hidden)), Some(true));
        assert_eq!(switch(State::NeedsApproval(Start::Hidden)), Some(true));
        assert_eq!(switch(State::On(Start::Window)), Some(true));
    }

    #[test]
    fn first_launch_registers_once_and_respects_a_confirmed_disable() {
        assert_eq!(initial_setup(false, State::Off, None), InitialSetup::Enable);
        assert_eq!(
            initial_setup(false, State::On(Start::Hidden), Some(true)),
            InitialSetup::Remember
        );
        assert_eq!(
            initial_setup(false, State::NeedsApproval(Start::Hidden), Some(true)),
            InitialSetup::Remember
        );
        assert_eq!(
            initial_setup(false, State::Unsupported, None),
            InitialSetup::Wait
        );
        assert_eq!(
            initial_setup(false, State::Off, Some(true)),
            InitialSetup::Wait
        );
        assert_eq!(
            initial_setup(false, State::Off, Some(false)),
            InitialSetup::Remember
        );
        assert_eq!(initial_setup(true, State::Off, None), InitialSetup::Wait);
    }
}
