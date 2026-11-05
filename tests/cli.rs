//! End-to-end tests: run the binary on fixture dumps.

use std::io::{ErrorKind, Write};
use std::process::{Command, Output, Stdio};

const PASSTHROUGH: &[u8] = include_bytes!("fixtures/passthrough.sql");
const HMAC_RULES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/hmac_rules.toml");
const DP_RULES: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/dp_rules.toml");

/// Runs ghostdump with `args`, the secret (if any) in the environment, and
/// `stdin` as input.
fn run(args: &[&str], secret: Option<&str>, stdin: &[u8]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ghostdump"));
    command
        .args(args)
        .env_remove("GHOSTDUMP_SECRET")
        .env_remove("GHOSTDUMP_LOG")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(secret) = secret {
        command.env("GHOSTDUMP_SECRET", secret);
    }

    let mut child = command.spawn().expect("the binary runs");
    let mut input = child.stdin.take().expect("stdin is piped");
    // The process may exit before reading its input (a rejected secret).
    if let Err(e) = input.write_all(stdin)
        && e.kind() != ErrorKind::BrokenPipe
    {
        panic!("could not write the input: {e}");
    }
    drop(input);
    child.wait_with_output().expect("the binary finishes")
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

#[test]
fn stdout_carries_only_the_dump() {
    let output = run(&[], None, PASSTHROUGH);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(PASSTHROUGH)
    );
    assert!(stderr(&output).contains("Rows processed"));
}

#[test]
fn hmac_rules_require_a_secret() {
    let dump = b"INSERT INTO users (id) VALUES (1);\n";
    let output = run(&["-c", HMAC_RULES], None, dump);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("no secret was given"));
    assert!(output.stdout.is_empty());
}

#[test]
fn blank_secret_is_rejected() {
    let dump = b"INSERT INTO users (id) VALUES (1);\n";
    for secret in ["", "   "] {
        let output = run(&["-c", HMAC_RULES], Some(secret), dump);

        assert!(!output.status.success(), "secret {secret:?} was accepted");
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn short_secret_only_warns() {
    let dump = b"INSERT INTO users (id) VALUES (1);\n";
    let output = run(&["-c", HMAC_RULES], Some("short"), dump);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(stderr(&output).contains("shorter than 16 bytes"));
}

#[test]
fn truncated_data_block_fails_without_writing_the_partial_row() {
    let dump = b"INSERT INTO t (a) VALUES ('first'), ('sec";
    let output = run(&[], None, dump);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("middle of a data row"));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("sec"));
}

#[test]
fn verbose_logging_does_not_touch_stdout() {
    let quiet = run(&[], None, PASSTHROUGH);
    let verbose = run(&["-v"], None, PASSTHROUGH);

    assert!(verbose.status.success());
    assert_eq!(quiet.stdout, verbose.stdout);
    assert!(stderr(&verbose).contains("debug:"));
}

#[test]
fn log_level_can_be_overridden() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ghostdump"));
    let output = command
        .env("GHOSTDUMP_LOG", "warn")
        .stdin(Stdio::null())
        .output()
        .expect("the binary runs");

    assert!(output.status.success());
    assert!(!stderr(&output).contains("Starting GhostDump"));
}

#[test]
fn logs_never_contain_column_values() {
    let dump = b"INSERT INTO users (salary) VALUES ('Batman-Secret-123');\n";
    for flags in [&[][..], &["-v"][..]] {
        let args: Vec<&str> = ["-c", DP_RULES].iter().chain(flags).copied().collect();
        let output = run(&args, None, dump);

        assert!(output.status.success(), "stderr: {}", stderr(&output));
        assert!(!stderr(&output).contains("Batman-Secret-123"));
    }
}
