//! End-to-end tests that drive the real `conclave` binary.
//!
//! Each test spawns `env!("CARGO_BIN_EXE_conclave")` inside a `tempfile::TempDir`, so the
//! process runs against a hermetic working directory. In M0 this asserts the binary launches
//! and advertises its command surface; later milestones stand up real `serve` and `bridge`
//! processes here with staggered ports and fixture key dirs (DESIGN.md §17).

// Tests relax `unwrap_used` (house convention; DESIGN.md §22).
#![allow(clippy::unwrap_used)]

use std::process::Command;

use tempfile::TempDir;

/// Path to the freshly-built `conclave` binary, injected by Cargo at compile time.
const CONCLAVE_BIN: &str = env!("CARGO_BIN_EXE_conclave");

#[test]
fn e2e_help_advertises_the_command_surface() {
    let workdir = TempDir::new().unwrap();

    let output = Command::new(CONCLAVE_BIN)
        .arg("--help")
        .current_dir(workdir.path())
        .output()
        .expect("failed to spawn `conclave --help`");

    assert!(output.status.success(), "`--help` exited non-zero: {:?}", output.status);

    let stdout = String::from_utf8(output.stdout).unwrap();
    for verb in ["serve", "bridge", "register", "machine", "join", "perm", "key"] {
        assert!(stdout.contains(verb), "help output is missing the `{verb}` subcommand");
    }
}

#[test]
fn e2e_unimplemented_command_fails_cleanly() {
    let workdir = TempDir::new().unwrap();

    let output = Command::new(CONCLAVE_BIN).arg("key").current_dir(workdir.path()).output().expect("failed to spawn `conclave key`");

    // M0 stub: the command parses but the verb is unimplemented, so it exits non-zero with a
    // clear message rather than a panic or a silent success.
    assert!(!output.status.success(), "expected `key` to fail in the M0 scaffold");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("not yet implemented"), "stderr is missing the unimplemented notice: {stderr}");
}
