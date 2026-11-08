//! Updates are disabled in this private build so an upstream release cannot
//! replace its recorder or change its independent application identity.

pub use fastframe_update::{
    CHECK_INTERVAL, DownloadState, Installation, Kind, Prepared, Release, Source, Unsupported,
    Updater,
};

/// Checked by both UI entry points and the backend's updater factory.
pub const ENABLED: bool = false;
pub const DISABLED_REASON: &str =
    "Automatic updates are disabled in Wassup. Install a reviewed private build to update it.";

/// Every backend update command uses this factory. Refuse before constructing
/// a transport, inspecting an installation, or starting an update helper.
pub fn updater() -> anyhow::Result<Updater> {
    anyhow::bail!(DISABLED_REASON)
}

#[cfg(test)]
mod tests {
    #[test]
    fn private_build_cannot_construct_an_updater() {
        let error = match super::updater() {
            Ok(_) => panic!("updater must be disabled"),
            Err(error) => error,
        };
        assert_eq!(error.to_string(), super::DISABLED_REASON);
    }
}
