//! End-to-end tests: run the binary on fixture dumps.

use std::io::{ErrorKind, Write};
use std::process::{Command, Output, Stdio};

const PASSTHROUGH: &[u8] = include_bytes!("fixtures/passthrough.sql");
const PG_DATA: &[u8] = include_bytes!("fixtures/pg_data.sql");
const HMAC_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/hmac_rules.toml"
);
const MYSQL_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/mysql_rules.toml"
);
const INVALID_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/invalid_rules.toml"
);
const PG_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/pg_data_rules.toml"
);
const GOLDEN_SECRET: &str = "golden-test-secret-0123456789";
const CONSISTENT_RULES: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/consistent_rules.toml"
);
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

#[test]
fn data_blocks_are_copied_exactly_without_rules() {
    let output = run(&[], None, PG_DATA);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(PG_DATA)
    );
}

#[test]
fn mysqldump_with_column_names_is_masked() {
    let dump = include_bytes!("fixtures/mysql_complete_insert.sql");
    let output = run(&["-c", MYSQL_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(
        stdout.contains("(1,'Masked O\\'Name',NULL),(2,'Masked O\\'Name',NULL);"),
        "{stdout}"
    );
    assert!(!stdout.contains("ana@example.com"));
    assert!(stdout.contains("UNLOCK TABLES;"));
}

#[test]
fn mysqldump_default_format_is_refused() {
    let dump = include_bytes!("fixtures/mysql_default.sql");
    let output = run(&["-c", MYSQL_RULES], None, dump);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("--complete-insert"));
    assert!(
        !stdout.contains("Ana") && !stdout.contains("example.com"),
        "{stdout}"
    );
}

#[test]
fn invalid_rules_stop_before_reading_data() {
    let dump = b"INSERT INTO users (salary) VALUES (10);\n";
    let output = run(&["-c", INVALID_RULES], None, dump);

    assert!(!output.status.success());
    assert!(stderr(&output).contains("table `users`, column `salary`"));
    assert!(output.stdout.is_empty());
}

#[test]
fn non_numeric_dp_values_are_counted_once() {
    let dump = b"INSERT INTO users (salary) VALUES ('a'), ('b'), (30);\n";
    let output = run(&["-c", DP_RULES], None, dump);
    let stderr = stderr(&output);

    assert!(output.status.success(), "stderr: {stderr}");
    assert_eq!(stderr.matches("were not numbers").count(), 1, "{stderr}");
    assert!(stderr.contains("2 value(s)"), "{stderr}");
}

/// Regenerate after an intended change (a new `fake` version, for example):
///
/// ```sh
/// GHOSTDUMP_SECRET=golden-test-secret-0123456789 ghostdump --seed 42 \
///     -c tests/fixtures/pg_data_rules.toml -i tests/fixtures/pg_data.sql \
///     > tests/fixtures/pg_data_masked.sql
/// ```
#[test]
fn seeded_run_matches_the_golden_output() {
    let expected = include_bytes!("fixtures/pg_data_masked.sql");
    let output = run(
        &["-c", PG_RULES, "--seed", "42"],
        Some(GOLDEN_SECRET),
        PG_DATA,
    );

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert!(stderr(&output).contains("--seed makes every random choice reproducible"));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(expected)
    );
}

#[test]
fn noise_is_reproducible_only_with_a_seed() {
    let dump = b"INSERT INTO users (salary) VALUES (10), (20), (30);\n";
    let seeded = |seed: &str| run(&["-c", DP_RULES, "--seed", seed], None, dump).stdout;
    let unseeded = || run(&["-c", DP_RULES], None, dump).stdout;

    assert_eq!(seeded("7"), seeded("7"));
    assert_ne!(seeded("7"), seeded("8"));
    assert_ne!(unseeded(), unseeded());
}

#[test]
fn progress_flag_keeps_stdout_clean() {
    let output = run(&["-p"], None, PG_DATA);

    assert!(output.status.success(), "stderr: {}", stderr(&output));
    assert_eq!(output.stdout, PG_DATA);
}

#[test]
fn consistent_noise_repeats_across_runs_and_rows() {
    let dump = b"INSERT INTO users (salary) VALUES (10), (20), (10);\n";
    let first = run(&["-c", CONSISTENT_RULES], Some(GOLDEN_SECRET), dump);
    let second = run(&["-c", CONSISTENT_RULES], Some(GOLDEN_SECRET), dump);

    assert!(first.status.success(), "stderr: {}", stderr(&first));
    assert_eq!(first.stdout, second.stdout);
    let text = String::from_utf8_lossy(&first.stdout);
    let values: Vec<&str> = text
        .trim_end()
        .trim_end_matches(");")
        .split("VALUES (")
        .nth(1)
        .unwrap()
        .split("), (")
        .collect();
    assert_eq!(values[0], values[2], "{text}");
    assert_ne!(values[0], values[1], "{text}");

    let without_secret = run(&["-c", CONSISTENT_RULES], None, dump);
    assert!(!without_secret.status.success());
}

#[test]
fn startup_summary_states_the_privacy_budget() {
    let output = run(&["-c", PG_RULES], Some(GOLDEN_SECRET), PG_DATA);
    let stderr = stderr(&output);

    assert!(output.status.success(), "stderr: {stderr}");
    assert!(
        stderr.contains("privacy: table `users`: ε = 1.0000 per row (salary ε = 1.0000)"),
        "{stderr}"
    );
    assert!(stderr.contains("pseudonymized, no DP: email"), "{stderr}");
}
