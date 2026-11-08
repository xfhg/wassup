//! Wassup rejects upstream update-helper arguments before touching any profile.

use std::path::Path;
use std::process::{Command, Output};

/// Runs the built executable with every per-user location inside `home`.
fn wassup(home: &Path, arguments: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_wassup"));
    command.args(arguments).env_remove("RUST_LOG");
    for variable in [
        "HOME",
        "USERPROFILE",
        "APPDATA",
        "LOCALAPPDATA",
        "XDG_CONFIG_HOME",
        "XDG_DATA_HOME",
        "XDG_STATE_HOME",
        "XDG_CACHE_HOME",
        "XDG_RUNTIME_DIR",
    ] {
        command.env(variable, home);
    }
    command.output().expect("the Wassup executable runs")
}

fn entries(directory: &Path) -> Vec<String> {
    std::fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .collect()
}

#[test]
fn updater_arguments_are_rejected_without_touching_the_profile() {
    for argument in ["--apply-update", "--update-receipt", "--update-error"] {
        let home = tempfile::tempdir().unwrap();
        let output = wassup(home.path(), &[argument, "fixture"]);
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert_eq!(output.status.code(), Some(2), "stderr: {stderr}");
        assert!(stderr.contains("unexpected argument"), "stderr: {stderr}");
        assert_eq!(entries(home.path()), Vec::<String>::new());
    }
}

#[test]
fn version_identifies_the_private_executable_without_touching_the_profile() {
    let home = tempfile::tempdir().unwrap();
    let output = wassup(home.path(), &["--version"]);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "stderr: {stderr}");
    assert_eq!(
        stdout.trim(),
        format!("wassup {}", env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(entries(home.path()), Vec::<String>::new());
}
