//! Docker-backed integration scenarios for postgres-cli V2.
//!
//! These tests are ignored by default because they require a running Postgres instance.
//! Run with:
//!   cargo test --test integration_docker -- --ignored

use std::process::Command;

fn run(command: &[&str]) -> std::io::Result<std::process::Output> {
    Command::new(command[0]).args(&command[1..]).output()
}

#[test]
#[ignore]
fn query_read_mode_blocks_write() {
    let output = run(&[
        "cargo",
        "run",
        "--",
        "--project-root",
        ".",
        "--target",
        "local-read",
        "query",
        "--sql",
        "INSERT INTO users(id) VALUES (1)",
    ])
    .expect("run cargo");

    assert!(!output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("READ_MODE_BLOCKED") || stdout.contains("TARGET_WRITE_DISABLED"));
}

#[test]
#[ignore]
fn config_validate_smoke() {
    let output = run(&["cargo", "run", "--", "config", "validate"]).expect("run cargo");

    // Validation may fail depending on local env vars, but it must emit structured output.
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("\"version\""));
}
