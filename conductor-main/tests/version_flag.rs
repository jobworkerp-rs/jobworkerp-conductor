//! The release workflow verifies each published binary through `--version`,
//! so its output and its lack of side effects are part of the release contract.

use std::process::{Command, Output};

use tempfile::TempDir;

fn run(workspace: &TempDir, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_conductor-main"))
        .args(args)
        .current_dir(workspace.path())
        .output()
        .expect("run conductor-main")
}

fn entries(workspace: &TempDir) -> Vec<String> {
    let mut names: Vec<String> = std::fs::read_dir(workspace.path())
        .expect("read workspace")
        .map(|entry| {
            entry
                .expect("workspace entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();
    names
}

#[test]
fn version_flag_prints_the_package_version() {
    let workspace = TempDir::new().expect("create workspace");

    let output = run(&workspace, &["--version"]);

    assert!(output.status.success(), "--version must succeed");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("conductor-main {}\n", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn version_flag_does_not_create_log_files() {
    let workspace = TempDir::new().expect("create workspace");
    // The settings a deployment would use: logs go to files under LOG_FILE_DIR.
    std::fs::write(
        workspace.path().join(".env"),
        "LOG_FILE_DIR=log/\nLOG_USE_STDOUT=false\n",
    )
    .expect("write .env");

    let output = run(&workspace, &["--version"]);

    assert!(output.status.success(), "--version must succeed");
    assert_eq!(
        entries(&workspace),
        vec![".env".to_string()],
        "--version must exit before logging is initialized"
    );
}

#[test]
fn unknown_flag_is_a_usage_error() {
    let workspace = TempDir::new().expect("create workspace");

    let output = run(&workspace, &["--no-such-flag"]);

    assert!(!output.status.success(), "an unknown flag must fail");
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("Usage:"),
        "an unknown flag must report usage"
    );
}
